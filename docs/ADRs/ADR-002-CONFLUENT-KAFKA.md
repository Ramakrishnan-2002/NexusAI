# ADR-002: Adoption of confluent-kafka (librdkafka) over pure-Python Clients

## Status
**ACCEPTED**

## Context
Initial prototypes used `aiokafka`. Under high-throughput workloads, pure-Python Kafka clients suffer from Python GIL contention during batch serialization, lack native C-level buffering, and exhibit socket lifecycle fragility across separate event loop boundaries during worker rebalancing or test isolation.

## Decision
Migrate the entire platform to `confluent-kafka` (backed by the native C `librdkafka` engine).
- **Producer:** Non-blocking `producer.produce()` enqueues into C memory (`linger.ms: 5`), with `producer.poll(0)` dispatching delivery callbacks on loop ticks.
- **Consumer:** Polling is delegated to OS worker threads via `await asyncio.to_thread(consumer.poll, 1.0)`, releasing the Python GIL during network waits and keeping the main asyncio event loop unblocked.

## Alternatives Considered
1. **aiokafka:** Pure Python asyncio client. Rejected due to GIL bottlenecks during heavy serialization and event loop lifecycle fragility.
2. **kafka-python:** Legacy synchronous client. Rejected due to lack of active maintenance and lower throughput.

## Consequences
- **Positive:** High-throughput native C buffering, enterprise protocol parity, and reliable socket lifecycle management.
- **Tradeoff:** Requires thread-isolated polling (`asyncio.to_thread`) to prevent blocking Python's single-threaded event loop.
