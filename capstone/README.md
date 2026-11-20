# Capstone (PA8): order processing system

An order arrives as web JSON, mobile JSON or b2b XML. Intake normalizes it and publishes `order.accepted`. The orchestrator runs the saga (payment, inventory, shipping, notification) and compensates on failure. The notifier tells the customer when an order could not be completed. The observer shows the full trace of an order and serves metrics and alerts.

## How to run

Requirements: Docker Compose 2.20 or later, Node 20, at least 4 GB of memory for Docker.

    docker compose up -d --build --wait
    npm --prefix tests ci
    npm --prefix tests run test:stage

Stop and reset everything (this deletes all stores):

    docker compose down -v

## Services

| Service | Host port | Role |
|---|---|---|
| intake | 8800 | Normalizer, idempotency, order status, DLQ list and replay |
| observer | 8801 | trace, /ops/metrics, alerts |
| orchestrator | 8802 | saga consumer, compensation, stuck sagas |
| notifier | 8803 | failure notices from outcome events |

RabbitMQ management UI: http://localhost:15680 (user `eai`, password `eai-capstone`).

## SAGA_ENGINE

`handrolled` (default). The orchestrator runs the saga itself.
`temporal` is not implemented. See below.

## Known limitations

- (Write what is true for you. Examples:)
- Hardening (step 6): list anything you did not finish or test, such as stuck detection, operator compensation, runtime reconnect, or concurrency.
- The lag alert thresholds are guesses (saga_consumer_lag = N, notifier_consumer_lag = N).
- No automated tests beyond the public tests.

## Documents

See `docs/`: architecture, orchestration BPMN, patterns, failure analysis, runbook, agent readiness, AI usage, and ADRs.

## Advanced component

Not implemented.

The Temporal mode (SAGA_ENGINE=temporal) was not built. `docs/adr/adr-011.md` is left as the template.