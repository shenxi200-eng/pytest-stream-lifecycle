# Changelog

## 0.1.0a1 — 2026-11-12

First experimental alpha; the public API may change.

- Real loopback HTTP/1.1 ASGI server fixture on the test's asyncio event loop.
- Bounded SSE and line readers, including POST requests and explicit disconnects.
- Lifecycle observations requiring explicit acknowledgement after cleanup.
- Buffering, detached-upstream and repeated-session examples.
- Failure diagnostics preserve primary errors and nested cleanup exceptions.
- Cross-platform regression tests for Linux, macOS and Windows.

Limits: asyncio and explicitly instrumented resources only. Client closure is
not proof of upstream cleanup, and a local marker does not prove remote work or
billing stopped. Event-loop-blocking or cancellation-resistant apps need process
isolation for a hard stop.
