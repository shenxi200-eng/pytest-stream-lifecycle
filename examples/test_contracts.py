"""Executable comparisons: expected failures are caught, not hidden.

Run with: python -m pytest examples/test_contracts.py -v
All services and synthetic upstreams are local. No model API is used.
"""

import asyncio
from contextlib import aclosing

import httpx
import pytest
from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse

from pytest_stream_lifecycle import LifecycleError


def streaming_app(probe, release, *, buffered=False):
    app = FastAPI()

    async def upstream():
        with probe.track("upstream") as observed:
            try:
                yield "first"
                await release.wait()
                yield "second"
            finally:
                # This synthetic source owns no remote connection. Real code
                # must acknowledge only AFTER its awaited cleanup succeeds.
                observed.mark_closed()

    async def body():
        with probe.track("producer") as observed:
            try:
                async with aclosing(upstream()) as source:
                    if buffered:
                        chunks = [chunk async for chunk in source]
                        for chunk in chunks:
                            yield {"event": "token", "data": chunk}
                    else:
                        async for chunk in source:
                            yield {"event": "token", "data": chunk}
            finally:
                observed.mark_closed()

    @app.get("/events")
    async def events():
        # This response explicitly listens for disconnects. A response that
        # only notices a disconnect on its next send may not cancel a source
        # blocked waiting for its next token.
        return EventSourceResponse(body())

    return app


def test_final_body_assertion_misses_buffering(stream_server, stream_probe):
    async def scenario():
        release = asyncio.Event()
        release.set()  # Let even the buffered implementation finish.
        app = streaming_app(stream_probe, release, buffered=True)
        async with stream_server(app) as server:
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.get(server.url + "/events")
            assert response.status_code == 200
            assert "data: first" in response.text
            assert "data: second" in response.text
            await stream_probe.wait_closed("producer", "upstream", timeout=2)

    asyncio.run(scenario())


def test_contract_detects_buffering(stream_server, stream_probe):
    async def scenario():
        release = asyncio.Event()
        app = streaming_app(stream_probe, release, buffered=True)
        async with stream_server(app) as server:
            async with server.sse("/events", timeout=2) as stream:
                with pytest.raises(LifecycleError, match="(?i)timed out|timeout"):
                    await stream.next(timeout=0.1)
            # The producer was never allowed to finish normally. The helper
            # closed its socket after timeout; the application must clean up.
            assert not release.is_set()
            await stream_probe.wait_closed("producer", "upstream", timeout=2)

    asyncio.run(scenario())


def test_incremental_delivery_and_disconnect_cleanup(stream_server, stream_probe):
    async def scenario():
        release = asyncio.Event()
        app = streaming_app(stream_probe, release)
        async with stream_server(app) as server:
            async with server.sse("/events", timeout=2) as stream:
                event = await stream.next()
                assert event.event == "token"
                assert event.data == "first"
                stream_probe.assert_active("producer")
                stream_probe.assert_active("upstream")
            # Assert before stopping the server: server teardown must not make
            # a broken disconnect implementation appear correct.
            await stream_probe.wait_closed("producer", "upstream", timeout=2)
            assert not release.is_set()
            assert stream_probe.active_labels == frozenset()

    asyncio.run(scenario())


def test_twenty_sessions_leave_no_observed_resource_open(stream_server, stream_probe):
    async def scenario():
        app = streaming_app(stream_probe, asyncio.Event())
        async with stream_server(app) as server:
            for _ in range(20):
                async with server.sse("/events", timeout=2) as stream:
                    assert (await stream.next()).data == "first"
                await stream_probe.wait_closed("producer", "upstream", timeout=2)
            assert stream_probe.started_count("producer") == 20
            assert stream_probe.started_count("upstream") == 20
            assert stream_probe.active_labels == frozenset()

    asyncio.run(scenario())


def test_detached_upstream_is_detected_before_fixture_cleanup(stream_server, stream_probe):
    async def scenario():
        release = asyncio.Event()
        detached = []
        app = FastAPI()

        async def upstream():
            with stream_probe.track("upstream") as observed:
                try:
                    await release.wait()
                finally:
                    observed.mark_closed()

        async def body():
            with stream_probe.track("producer") as observed:
                try:
                    detached.append(asyncio.create_task(upstream()))
                    await stream_probe.wait_started("upstream", timeout=2)
                    yield {"data": "first"}
                    await release.wait()
                finally:
                    # Intentionally broken: the detached upstream is neither
                    # cancelled nor joined when the consumer disconnects.
                    observed.mark_closed()

        @app.get("/events")
        async def events():
            return EventSourceResponse(body())

        async with stream_server(app) as server:
            try:
                async with server.sse("/events", timeout=2) as stream:
                    assert (await stream.next()).data == "first"
                await stream_probe.wait_closed("producer", timeout=2)
                with pytest.raises(LifecycleError, match="upstream"):
                    await stream_probe.wait_closed("upstream", timeout=0.1)
                assert not detached[0].done()
            finally:
                # Repair only the synthetic fixture, AFTER detecting its leak.
                release.set()
                await asyncio.wait_for(asyncio.gather(*detached), timeout=2)

    asyncio.run(scenario())
