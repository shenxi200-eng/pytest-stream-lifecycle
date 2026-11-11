# API guide

The pytest plugin exposes two function-scoped fixtures. Use them within one
asyncio event loop per test, either through `asyncio.run()` or your own async
pytest integration.

## Server

```python
async with stream_server(app, startup_timeout=5.0, shutdown_timeout=5.0, lifespan="on") as server:
    print(server.url)  # http://127.0.0.1:<ephemeral-port>
```

`stream_server` is the `live_server` factory, also exported for non-pytest use.
It runs on the test's event loop, retains its port-zero socket, and does not
install signal handlers or reconfigure global logging. `lifespan` accepts
`"on"`, `"auto"`, or `"off"`; choose `"off"` for an app without lifespan support.
Startup and shutdown failures raise `ServerError`. Deadlines require cooperative
async code; they cannot preempt code that blocks the event loop.

## Stream readers

```python
async with server.sse("/events", method="POST", json={"prompt": "hello"}) as stream:
    event = await stream.next(timeout=2.0)
    assert event.data

async with server.lines("/jsonl", headers={"X-Test": "yes"}) as stream:
    line = await stream.next()
```

- `sse()` yields a reader of `httpx_sse.ServerSentEvent` objects; `lines()` yields
  strings, without decoding JSON or filtering empty lines.
- `timeout=2.0` is the default for connection/response headers and each read.
  `next(timeout=...)` overrides one read. It is not a whole-session deadline.
  Timeouts must be positive and finite.
- Paths must start with a single `/`; absolute URLs and `//host` paths are
  rejected. Request options such as `json`, `params`, and `headers` are passed
  to HTTPX. Environment proxy settings are ignored (`trust_env=False`). Redirects
  are not followed by default; callers opting in to redirects are responsible
  for the destination.
- HTTP errors and invalid SSE content types fail the context entry. Read timeout,
  read error, and EOF before a requested event/line raise `LifecycleError`.
  EOF is an error for `next()`, not an end-of-stream assertion API.
- A timed-out reader is closed, not reusable. Keep one reader context open for
  multiple `next()` calls. Concurrent reads on the same reader are rejected.
- `await stream.aclose()` is idempotent. Context exit also closes the reader.
  Client closure is deliberately separate from server-side cleanup assertions.

## Lifecycle probe

```python
with stream_probe.track("upstream") as observed:
    try:
        await use_resource()
    finally:
        await close_resource()
        observed.mark_closed()

await stream_probe.wait_closed("upstream", timeout=2.0)
```

`track(name)` is a synchronous context manager for instrumentation **inside async
code**. Entering it records a distinct resource instance; multiple instances may
share a label. Put the acknowledgement after successful cleanup, not unconditionally
in an outer `finally`. A second cancellation can interrupt awaited cleanup.
The probe neither shields cleanup nor cancels application tasks.

| Operation | Meaning |
| --- | --- |
| `track(name)` | Record one resource lifetime; name must be a nonempty string. |
| `observed.mark_closed()` | Acknowledge cleanup while the observation is still active. |
| `assert_active(name)` | Require at least one currently active instance. |
| `await wait_started(name, timeout=2.0)` | Wait for at least one observed instance; it may already have exited. |
| `await wait_closed(*names, timeout=2.0)` | Require each label to have been observed, and all its observed instances to have exited with cleanup acknowledged and no non-cancellation error. |
| `started_count(name)` | Number of entered instances for this label, including closed ones. |
| `active_labels` | A `frozenset` of labels with currently active instances. |

Unknown labels wait until observed or timeout; an empty `wait_closed()` call
fails. An exited instance without acknowledgement or with a recorded error fails
immediately. `CancelledError` and `GeneratorExit` propagate but are not recorded
as cleanup failures by themselves; an acknowledgement is still required.
Failures use `LifecycleError`, an `AssertionError` subclass.

Observations are retained for the probe's lifetime, so an earlier failure is not
forgotten when another instance with the same name closes successfully. A probe
binds to its first running event loop and rejects reuse from another loop. It
can be constructed outside that loop. `LifecycleProbe` is exported for use
without pytest.

A successful check describes instances observed **so far**, not resources that
might start later. Synchronize application start points explicitly. Check both
producer and upstream labels while the server is still running, before test
teardown has a chance to cancel them.
