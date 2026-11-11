# pytest-stream-lifecycle

Real-network lifecycle tests for Python streaming APIs.

**Experimental, under active construction.** This first milestone supplies a
loopback ASGI server fixture. Streaming and disconnect assertions are the next
milestone, not features of this initial scaffold. Not published to PyPI yet.

## Why

A response can look correct after it has been fully buffered. A client can close
while a server-side producer keeps running. Testing either requires more than
asserting the final response text.

The intended scope is small: observable progressive delivery, disconnect cleanup,
and repeated-session checks for explicitly instrumented resources. It is **not**
a load tester, LLM evaluator, general leak detector, or a replacement SSE parser.

## Install from source

Requires Python 3.11 or newer. No GPU or model API credentials are needed.

```sh
git clone https://github.com/shenxi200-eng/pytest-stream-lifecycle.git
cd pytest-stream-lifecycle
python -m venv .venv
# Activate .venv using your shell's activation command.
python -m pip install -e '.[dev]'
python -m pytest -q
```

## First milestone: real loopback HTTP

The pytest plugin registers the `stream_server` fixture automatically after
installation. It runs Uvicorn on a retained ephemeral socket, on the **same
asyncio event loop** as the test. No pytest-asyncio dependency is required:

```python
import asyncio

import httpx
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route


async def hello(request):
    return PlainTextResponse("hello")


app = Starlette(routes=[Route("/", hello)])


def test_over_tcp(stream_server):
    async def scenario():
        async with stream_server(app) as server:
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.get(server.url + "/")
                assert response.text == "hello"

    asyncio.run(scenario())
```

Starlette is an example/development dependency, not a runtime requirement. The
fixture accepts ASGI apps, including FastAPI apps. Real HTTP/1.1 is used; there
is no in-process HTTPX transport, external listener, or outbound service needed.

## Boundaries

- asyncio only; not Trio, HTTP/2, TLS, WebSockets, or browser/proxy emulation.
- Client disconnect is not proof that upstream work stopped. Later assertions
  will require explicit test-side observation rather than guesswork.
- An in-process server cannot forcibly kill code that blocks the event loop or
  continually suppresses cancellation. Such apps need process isolation.
- These tests cannot infer GPU work, remote billing, or uninstrumented task leaks.

See [design and related work](docs/design.md). Contributions, including
AI-assisted ones, are welcome under the same testing and review requirements.
Development of this project uses AI-assisted implementation and review; generated
code and claims must be checked rather than treated as evidence.
