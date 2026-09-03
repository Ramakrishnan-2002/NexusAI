# ADR-004: Separate Consistency Domains (PostgreSQL vs. Redis)

## Status
**ACCEPTED**

## Context
WikiPulse persists canonical relational entities (`articles`, `editors`, `edits`) while simultaneously tracking high-frequency rolling sliding-window velocity metrics (1m, 5m, 15m) in Redis. Dual-writing across PostgreSQL and Redis cannot be executed as a single distributed atomic transaction.

## Decision
Explicitly separate the consistency models:
1. **PostgreSQL:** Authoritative **system of record** backed by ACID transactions and disk Write-Ahead Logging (WAL).
2. **Redis:** **Transient derived aggregation state** managed via in-memory Sorted Sets (`ZADD`, `ZREMRANGEBYSCORE`, `ZCARD`).
3. Execution order: PostgreSQL transaction commits first; Redis is updated second; Kafka offset is committed last. If Redis blips, `ActivityCounterService` degrades to an in-memory fallback without rolling back the primary PostgreSQL transaction.

## Alternatives Considered
1. **PostgreSQL-Only Queries:** Running rolling window `COUNT(*)` SQL queries on every write. Rejected due to severe database lock contention and CPU bottlenecks.
2. **Distributed 2PC / Saga Pattern:** Unnecessary complexity for transient velocity counters that can be reconstructed from historical edit logs.

## Consequences
- **Positive:** Sub-millisecond velocity scoring (<0.5ms) without relational table locks; primary database writes never fail due to cache outages.
- **Tradeoff:** Redis state could temporarily diverge during extreme crash replays before rolling window expiration.
