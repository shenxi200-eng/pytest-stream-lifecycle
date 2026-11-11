# pytest-stream-lifecycle

Test streaming delivery and disconnect cleanup over real loopback HTTP.

**Experimental. Python 3.11+, asyncio. Not published to PyPI yet.**
No GPU, model API credentials, or external service is needed.

## What it catches

A final response-body assertion can pass even when every token was buffered.
Closing a client can also leave a producer or upstream task running. This plugin
provides:

- `stream_server`: an ASGI app served by Uvicorn on a retained local socket.
- `server.sse()` / `server.lines()`: bounded incremental reads over real HTTP/1.1.
- `stream_probe`: explicit resource observations and cleanup acknowledgements.

It is not a load tester, LLM evaluator, SSE parser, or general leak detector.
The SSE parser is [httpx-sse](https://github.com/florimondmanca/httpx-sse).

## Install

Install the current source in your test environment:

```sh
python -m pip install 'git+https://github.com/shenxi200-eng/pytest-stream-lifecycle.git'
```

For the example below, also install `fastapi` and `sse-starlette`. These are
example/development dependencies, not plugin runtime requirements. Installation
registers the pytest fixtures automatically; pytest-asyncio is not required.

## Test delivery, then disconnect

Save as `test_stream.py` and run `python -m pytest -q test_stream.py`:

```python
import asyncio

from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse


def test_disconnect(stream_server, stream_probe):
    async def scenario():
        app = FastAPI()
        release = asyncio.Event()

        async def tokens():
            with stream_probe.track("producer") as observed:
                try:
                    yield {"event": "token", "data": "first"}
                    await release.wait()
                    yield {"event": "token", "data": "second"}
                finally:
                    # This synthetic producer owns no external resource.
                    # In real code, await cleanup BEFORE marking it closed.
                    observed.mark_closed()

        @app.get("/events")
        async def events():
            # Listens for disconnects even while tokens() is waiting.
            return EventSourceResponse(tokens())

        async with stream_server(app) as server:
            async with server.sse("/events") as stream:
                assert (await stream.next()).data == "first"
                stream_probe.assert_active("producer")
            # Assert while the server is STILL RUNNING: its shutdown must
            # not hide an application that ignores client disconnects.
            await stream_probe.wait_closed("producer", timeout=2)
            assert not release.is_set()

    asyncio.run(scenario())
```

The first event must arrive before the release gate opens. Exiting the stream
context disconnects the client; it does **not** assert server cleanup. The
separate probe assertion checks that an observed producer exited and explicitly
acknowledged cleanup without a non-cancellation error.

For a real upstream, instrument its lifetime separately. Call `mark_closed()`
only after the cleanup you intend to test has completed successfully. Leaving
`track()` without that acknowledgement fails `wait_closed()`, even if the task
has already exited. The marker is application-supplied evidence, not an
independent guarantee that a remote provider stopped work or billing.

## Executable comparisons

```sh
git clone https://github.com/shenxi200-eng/pytest-stream-lifecycle.git
cd pytest-stream-lifecycle
python -m venv .venv
# Activate .venv using your shell's activation command.
python -m pip install -e '.[dev]'
python -m pytest examples/test_contracts.py -v
python -m pytest -q
```

The examples compare a buffered implementation that passes a body-only test
with a streaming contract that detects it. They also cover incremental delivery,
20 sequential sessions, and a detached upstream that remains alive after the
producer closes. Negative examples use `pytest.raises`: a passing test means
the defect was **detected**, not that the intentionally broken app is correct.

## Boundaries

- Same-loop asyncio and loopback HTTP/1.1 only: not Trio, HTTP/2, TLS,
  WebSockets, or browser/proxy emulation.
- Resource observations are explicit. Unknown resources do not count as clean;
  uninstrumented tasks are outside the probe's coverage.
- Cleanup checks should run before server teardown. Server shutdown can itself
  cancel request tasks and otherwise hide a broken disconnect handler.
- An in-process server cannot forcibly kill code that blocks the event loop or
  continually suppresses cancellation. Such apps need process isolation.
- A local cleanup marker does not prove remote cancellation, GPU work cessation,
  or a provider's billing state.

See the [API guide](docs/api.md), [design and related work](docs/design.md), and
[contribution guide](CONTRIBUTING.md). Development uses AI-assisted implementation
and review; generated code and claims must be checked rather than treated as
verification evidence. AI-assisted contributions have the same testing and review
requirements as other contributions.
