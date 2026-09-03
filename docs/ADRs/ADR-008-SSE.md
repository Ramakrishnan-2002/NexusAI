# ADR-008: Server-Sent Events (SSE) for Real-Time Dashboard Broadcasting

## Status
**ACCEPTED**

## Context
The frontend live dashboard requires real-time updates as knowledge spikes and AI summaries are detected. The communication is unidirectional (server to client).

## Decision
Use **Server-Sent Events (SSE)** over HTTP (`GET /api/v1/stream/live`) rather than WebSockets.
- Each connected client is allocated a bounded `asyncio.Queue(maxsize=100)` to prevent memory exhaustion from slow clients.
- Transmits 15-second heartbeat ping comments (`: ping - <timestamp>\n\n`) to detect silent client disconnects.

## Alternatives Considered
1. **WebSockets:** Full-duplex protocol. Adds unnecessary connection handshake overhead and complex connection state management for unidirectional streaming.
2. **Short Polling:** Repeated HTTP polling every second. Causes massive database query amplification and excessive network overhead.

## Consequences
- **Positive:** Operates over standard HTTP/2, works through standard corporate firewalls, and includes browser-native auto-reconnection.
- **Tradeoff:** In-process `asyncio.Queue` fanout does not scale across multiple API pods without an external pub/sub layer (e.g. Redis Pub/Sub).
