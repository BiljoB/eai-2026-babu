# Architecture decision records

<!--
The index of your ADRs (README §6, R-6.8). ADR-000 to ADR-006 are the ones
you wrote in PA1–PA7: they stay in their own folders, and the links below
reach them from here. ADR-007 to ADR-011 are the capstone's, in this folder.

Fill in each title and status. Keep every link working: the stage 6 tests
check that each one resolves. If a PA ADR was superseded by a capstone one
(PA6's HTTP-started saga, for example), say so in its status.

ADR-011 is for the advanced component only. Without it, set its status to
"not written", and leave the file as it is.
-->

| # | Title | Status | Link |
|---|---|---|---|
| 000 | (Explicit encoding, string-safe amounts, and visible rejection for legacy order ingestion) · PA1 | accepted | [adr-000.md](https://github.com/BiljoB/eai-2026-babu/blob/pa1/pa1/docs/adr-000.md) |
| 001 | (Durable Queue, Publisher Confirms, Manual Acknowledgements, and Correlation ID Propagation) · PA2 | accepted | [adr-001.md](https://github.com/BiljoB/eai-2026-babu/blob/pa2/pa2/docs/adr-001.md) |
| 002 | (Correlation, timeout, and deduplication strategy for the order aggregator) · PA3 | accepted | [adr-002.md](https://github.com/BiljoB/eai-2026-babu/blob/pa3/pa3/docs/adr-002.md) |
| 003 | (Transformation to the Canonical Order Model) · PA4 | accepted | [adr-003.md](https://github.com/BiljoB/eai-2026-babu/blob/pa4/pa4/docs/adr-003.md) |
| 004 | (Header-carried correlation id, file-backed idempotency, and return-exchange replay) · PA5 | accepted | [adr-004.md](https://github.com/BiljoB/eai-2026-babu/blob/pa5/pa5/docs/adr-004.md) |
| 005 | (Contract-first checkout with a sequential, compensating saga orchestrator) · PA6 | accepted | [adr-005.md](https://github.com/BiljoB/eai-2026-babu/blob/pa6/pa6/docs/adr-005.md) |
| 006 | (Run the checkout saga on Temporal) · PA7 | accepted | [adr-006.md](https://github.com/BiljoB/eai-2026-babu/blob/pa7/pa7/docs/adr-006.md) |
| 007 | (Starting the saga from an event on saga.queue) | accepted | [adr-007.md](adr-007.md) |
| 008 | (Three dedup layers: client key, business key, provider keys) | accepted | [adr-008.md](adr-008.md) |
| 009 | (Wire tap for hop records, with causation carried in headers) | accepted | [adr-009.md](adr-009.md) |
| 010 | (Stuck sagas: detect by owner, never resume, compensate on operator command) | accepted | [adr-010.md](adr-010.md) |
| 011 | (title) | not written | [adr-011.md](adr-011.md) |
