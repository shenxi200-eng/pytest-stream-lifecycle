# Changelog

## 0.1.0a2 — 2026-11-12

- Preserve failed-startup finalizer exceptions, causes, and notes without replacing
  startup timeouts or caller cancellation; retain automatic lifespan fallback.
- Stop request admission before forced cleanup and include Uvicorn's already
  scheduled request tasks in the bounded, server-owned shutdown set.
- Coordinate pending stream reads and overlapping closes through one cleanup
  operation; preserve primary failures through repeated caller cancellation.
- Accept case-insensitive SSE media types while preserving their parameters.
- Report recorded lifecycle failures even when the exception is falsy.

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
