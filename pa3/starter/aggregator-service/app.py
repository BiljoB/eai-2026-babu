"""
Aggregator Service
==================
Your job: implement the AGGREGATOR pattern on top of the connection
handling and consume loop already wired up below.

- Collect item results from orders.results, grouped by orderId.
- Completion condition: every item of the order has reported a result.
- Timeout: if the order has been sitting incomplete for too long (one
  worker crashed, or was never running), emit a PARTIAL result instead of
  waiting forever. A hung order is a worse outcome than an honest partial
  answer.
- Duplicate results (the same item redelivered, e.g. after a requeue) must
  not be double-counted.
- Two orders in flight at once must never have their results mixed up.

Consumes from: orders.results
Publishes to:  orders.complete
"""

import json
import pika
import os
import threading
import time


in_flight = {}  # orderId -> {"results": {itemIndex: result_dict}, "totalItems": int, "lastActivity": float}
lock = threading.Lock()

IDLE_TIMEOUT_SECONDS = float(os.environ.get('AGGREGATOR_IDLE_TIMEOUT_SECONDS', '5'))
SWEEP_INTERVAL_SECONDS = 1.0


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment variable for host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


def publish_completion(message):
    """Publish a single message to orders.complete. Called with the lock
    already released -- do not hold `lock` while doing network I/O."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.complete', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='orders.complete',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Persistent
    )
    connection.close()


def aggregate_result(ch, method, properties, body):
    """Handle one message from orders.results."""
    result = json.loads(body)
    order_id = result['orderId']
    item_index = result['itemIndex']
    total_items = result['totalItems']

    completion_message = None

    with lock:
        order = in_flight.setdefault(order_id, {
            'results': {},
            'totalItems': total_items,
            'lastActivity': time.time(),
        })

        # Duplicate protection: keyed by itemIndex, so a redelivered
        # result for an index we've already seen is a no-op.
        if item_index not in order['results']:
            order['results'][item_index] = result

        order['lastActivity'] = time.time()

        if len(order['results']) >= order['totalItems']:
            completion_message = {
                'orderId': order_id,
                'correlationId': result.get('correlationId', order_id),
                'status': 'complete',
                'totalItems': order['totalItems'],
                'receivedItems': len(order['results']),
                'itemResults': list(order['results'].values()),
                'missingItemIndexes': [],
            }
            del in_flight[order_id]

    if completion_message is not None:
        publish_completion(completion_message)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    """Runs forever in a background thread. Emits a partial completion for
    any order that has gone quiet for longer than IDLE_TIMEOUT_SECONDS."""
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)

        completions_to_publish = []
        now = time.time()

        with lock:
            timed_out_order_ids = [
                order_id for order_id, order in in_flight.items()
                if now - order['lastActivity'] > IDLE_TIMEOUT_SECONDS
            ]

            for order_id in timed_out_order_ids:
                order = in_flight.pop(order_id)
                received_indexes = set(order['results'].keys())
                missing_indexes = [
                    i for i in range(order['totalItems'])
                    if i not in received_indexes
                ]
                completions_to_publish.append({
                    'orderId': order_id,
                    'correlationId': order_id,
                    'status': 'partial',
                    'totalItems': order['totalItems'],
                    'receivedItems': len(order['results']),
                    'itemResults': list(order['results'].values()),
                    'missingItemIndexes': missing_indexes,
                })

        for message in completions_to_publish:
            publish_completion(message)


def main():
    """Main entry point: connect to RabbitMQ, start the timeout sweeper,
    and start consuming results."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue='orders.results', durable=True)
    channel.queue_declare(queue='orders.complete', durable=True)

    channel.basic_qos(prefetch_count=1)

    sweeper = threading.Thread(target=sweep_timeouts, daemon=True)
    sweeper.start()

    channel.basic_consume(queue='orders.results', on_message_callback=aggregate_result)

    print('[Aggregator] Waiting for results...')
    channel.start_consuming()


if __name__ == '__main__':
    main()