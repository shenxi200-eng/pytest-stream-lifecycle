import asyncio

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, StreamingResponse
from starlette.routing import Route

from pytest_stream_lifecycle import LifecycleError, LifecycleProbe, live_server


def run(coro):
    return asyncio.run(coro)


@pytest.mark.parametrize("kind", ["sse", "lines"])
def test_first_chunk_is_live_before_producer_completion(stream_server, stream_probe, kind):
    async def scenario():
        release = asyncio.Event()
        producing = asyncio.Event()

        async def body():
            with stream_probe.track("producer") as observed:
                try:
                    yield b"data: first\ndata: second\n\n" if kind == "sse" else b'{"part":1}\n'
                    producing.set()
                    await release.wait()
                finally:
                    observed.mark_closed()

        async def endpoint(request):
            assert await request.json() == {"message": "hello"}
            assert request.headers["x-demo"] == "yes"
            media = "text/event-stream" if kind == "sse" else "application/x-ndjson"
            return StreamingResponse(body(), media_type=media)

        app = Starlette(routes=[Route("/events", endpoint, methods=["POST"])])
        async with stream_server(app) as server:
            async with getattr(server, kind)(
                "/events", method="POST", json={"message": "hello"}, headers={"X-Demo": "yes"}
            ) as stream:
                first = await stream.next()
                assert first.data == "first\nsecond" if kind == "sse" else first == '{"part":1}'
                await asyncio.wait_for(producing.wait(), 1)
                stream_probe.assert_active("producer")
                assert not release.is_set()
                release.set()
            await stream_probe.wait_closed("producer")

    run(scenario())


def test_buffered_response_has_headers_but_next_times_out():
    async def scenario():
        release = asyncio.Event()
        producer_done = asyncio.Event()

        async def body():
            await release.wait()
            yield b"data: eventually\n\n"
            producer_done.set()

        async def endpoint(request):
            return StreamingResponse(body(), media_type="text/event-stream")

        app = Starlette(routes=[Route("/", endpoint)])
        async with live_server(app) as server:
            async with server.sse("/", timeout=0.1) as stream:
                with pytest.raises(LifecycleError, match="timed out"):
                    await stream.next()
                with pytest.raises(LifecycleError, match="closed"):
                    await stream.next()
                await stream.aclose()
            release.set()
            # A separately consumed response demonstrates the final body alone can pass.
            async with httpx.AsyncClient(trust_env=False) as client:
                assert (await client.get(server.url)).text == "data: eventually\n\n"
            assert producer_done.is_set()

    run(scenario())


def disconnect_app(probe, *, detached=False, background=None, release=None):
    async def upstream():
        with probe.track("upstream") as observed:
            try:
                await release.wait()
            finally:
                observed.mark_closed()

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                else:
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        with probe.track("producer") as observed:
            upstream_task = asyncio.create_task(upstream())
            if background is not None:
                background.append(upstream_task)
            try:
                await send(
                    {
                        "type": "http.response.start",
                        "status": 200,
                        "headers": [(b"content-type", b"text/event-stream")],
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": b"data: one\n\ndata: two\n\n",
                        "more_body": True,
                    }
                )
                while (await receive())["type"] != "http.disconnect":
                    pass
            finally:
                if not detached:
                    release.set()
                    await upstream_task
                observed.mark_closed()

    return app


def test_disconnect_closes_producer_and_upstream_while_server_running():
    async def scenario():
        probe = LifecycleProbe()
        release = asyncio.Event()
        async with live_server(disconnect_app(probe, release=release)) as server:
            async with server.sse("/") as stream:
                assert (await stream.next()).data == "one"
                assert (await stream.next()).data == "two"
                await probe.wait_started("upstream")
                probe.assert_active("producer")
                probe.assert_active("upstream")
            await probe.wait_closed("producer", "upstream", timeout=1)
            assert probe.active_labels == frozenset()

    run(scenario())


def test_detached_leak_fails_before_explicit_test_cleanup():
    async def scenario():
        probe = LifecycleProbe()
        release = asyncio.Event()
        background = []
        async with live_server(
            disconnect_app(probe, detached=True, background=background, release=release)
        ) as server:
            try:
                async with server.sse("/") as stream:
                    assert (await stream.next()).data == "one"
                    await probe.wait_started("upstream")
                await probe.wait_closed("producer")
                with pytest.raises(LifecycleError, match="active"):
                    await probe.wait_closed("upstream", timeout=0.05)
                probe.assert_active("upstream")
            finally:
                release.set()
                await asyncio.gather(*background)
            await probe.wait_closed("upstream")

    run(scenario())


def test_repeated_sessions_track_each_observed_instance():
    async def scenario():
        probe = LifecycleProbe()
        release = asyncio.Event()
        release.set()
        async with live_server(disconnect_app(probe, release=release)) as server:
            for count in range(1, 21):
                async with server.sse("/") as stream:
                    assert (await stream.next()).data == "one"
                await probe.wait_closed("producer", "upstream")
                assert probe.started_count("producer") == count
                assert probe.started_count("upstream") == count
                assert not probe.active_labels

    run(scenario())


def test_status_content_type_eof_paths_and_close():
    async def endpoint(request):
        if request.url.path == "/bad":
            return PlainTextResponse("bad", status_code=503)
        if request.url.path == "/wrong":
            return PlainTextResponse("data: no\n\n")
        return PlainTextResponse("hello\n")

    async def scenario():
        app = Starlette(routes=[Route("/{path:path}", endpoint)])
        async with live_server(app) as server:
            with pytest.raises(LifecycleError, match="503"):
                async with server.sse("/bad"):
                    pass
            with pytest.raises(LifecycleError, match="content-type"):
                async with server.sse("/wrong"):
                    pass
            for path in ("https://example.com", "//example.com", "plain"):
                with pytest.raises(ValueError, match="relative"):
                    async with server.lines(path):
                        pass
            async with server.lines("/ok") as stream:
                assert await stream.next() == "hello"
                with pytest.raises(LifecycleError, match="EOF"):
                    await stream.next()
                await stream.aclose()
                await stream.aclose()

    run(scenario())


def test_connection_failure_and_body_exception():
    async def scenario():
        import socket

        from pytest_stream_lifecycle.server import ServerHandle

        # Reserve a local port without listening: no third-party service can
        # answer it, and platforms may report refusal or let connect time out.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
            handle = ServerHandle(f"http://127.0.0.1:{port}")
            started = asyncio.get_running_loop().time()
            with pytest.raises(
                LifecycleError, match="request failed|connection timed out"
            ) as caught:
                async with handle.lines("/", timeout=0.2):
                    pass
            assert isinstance(caught.value.__cause__, (httpx.ConnectError, TimeoutError))
            assert asyncio.get_running_loop().time() - started < 2

        async def endpoint(request):
            return PlainTextResponse("hi")

        async with live_server(Starlette(routes=[Route("/", endpoint)])) as server:
            with pytest.raises(ValueError, match="body issue"):
                async with server.lines("/") as stream:
                    assert await stream.next() == "hi"
                    raise ValueError("body issue")
            with pytest.raises(LifecycleError, match="closed"):
                await stream.next()

    run(scenario())


def test_connect_header_timeout_and_read_cancellation():
    async def scenario():
        release = asyncio.Event()
        started = asyncio.Event()

        async def slow_header(request):
            await release.wait()
            return PlainTextResponse("late")

        async def body():
            yield b"data: first\n\n"
            started.set()
            await release.wait()

        async def event_endpoint(request):
            return StreamingResponse(body(), media_type="text/event-stream")

        app = Starlette(routes=[Route("/slow", slow_header), Route("/events", event_endpoint)])
        async with live_server(app) as server:
            try:
                with pytest.raises(LifecycleError, match="connection timed out"):
                    async with server.lines("/slow", timeout=0.1):
                        pass
                async with server.sse("/events") as stream:
                    assert (await stream.next()).data == "first"
                    await asyncio.wait_for(started.wait(), timeout=1)
                    waiter = asyncio.create_task(stream.next())
                    await asyncio.sleep(0)
                    waiter.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await waiter
                    with pytest.raises(LifecycleError, match="closed"):
                        await stream.next()
            finally:
                release.set()

    run(scenario())


def test_body_exception_remains_primary_when_stream_cleanup_fails(monkeypatch):
    from pytest_stream_lifecycle.streams import Stream

    original = Stream.aclose

    async def failing_close(self):
        await original(self)
        raise RuntimeError("close failure detail")

    async def endpoint(request):
        return PlainTextResponse("hello\n")

    async def scenario():
        app = Starlette(routes=[Route("/", endpoint)])
        async with live_server(app) as server:
            monkeypatch.setattr(Stream, "aclose", failing_close)
            with pytest.raises(ValueError, match="body detail") as caught:
                async with server.lines("/") as stream:
                    assert await stream.next() == "hello"
                    raise ValueError("body detail")
            assert any("close failure detail" in note for note in caught.value.__notes__)

    run(scenario())


def test_read_failure_preserves_all_socket_free_cleanup_failures():
    from pytest_stream_lifecycle.streams import Stream

    def failure(message):
        error = RuntimeError(message)
        error.add_note(f"{message} note")
        error.__cause__ = ValueError(f"{message} cause")
        return error

    class Iterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise RuntimeError("read error")

        async def aclose(self):
            raise failure("iterator close")

    class ResponseContext:
        async def __aexit__(self, *args):
            raise failure("response close")

    class Client:
        async def aclose(self):
            raise failure("client close")

    async def scenario():
        stream = Stream(Client(), ResponseContext(), None, Iterator(), 1, "fake")
        with pytest.raises(LifecycleError, match="read error") as caught:
            await stream.next()
        notes = " ".join(caught.value.__notes__)
        for label in ("iterator close", "response close", "client close"):
            assert label in notes
            assert f"{label} note" in notes
            assert f"{label} cause" in notes

    run(scenario())


@pytest.mark.parametrize("body_failure", [False, True])
def test_context_preserves_all_socket_free_cleanup_failures(monkeypatch, body_failure):
    import pytest_stream_lifecycle.streams as streams_module

    class Response:
        def __init__(self):
            self.headers = {"content-type": "text/event-stream" if body_failure else "text/plain"}

        def raise_for_status(self):
            pass

    class ResponseContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *args):
            error = RuntimeError("setup response close")
            error.add_note("response cleanup note")
            raise error from ValueError("response cleanup cause")

    class Client:
        def __init__(self, **kwargs):
            pass

        def stream(self, *args, **kwargs):
            return ResponseContext()

        async def aclose(self):
            error = RuntimeError("setup client close")
            error.add_note("client cleanup note")
            raise error from ValueError("client cleanup cause")

    async def scenario():
        monkeypatch.setattr(streams_module.httpx, "AsyncClient", Client)
        error_type = ValueError if body_failure else LifecycleError
        message = "body detail" if body_failure else "invalid content-type"
        with pytest.raises(error_type, match=message) as caught:
            async with streams_module.open_stream("http://127.0.0.1", "/", kind="sse"):
                raise ValueError("body detail")
        notes = " ".join(caught.value.__notes__)
        for detail in (
            "setup response close",
            "response cleanup note",
            "response cleanup cause",
            "setup client close",
            "client cleanup note",
            "client cleanup cause",
        ):
            assert detail in notes

    run(scenario())


def test_grouped_stream_cleanup_preserves_nested_evidence_and_body(monkeypatch):
    from pytest_stream_lifecycle.streams import Stream

    original = Stream.aclose

    async def failing_close(self):
        await original(self)
        child = RuntimeError("group child failure")
        child.add_note("group child note")
        child.__cause__ = ValueError("group child cause")
        cyclic = RuntimeError("cyclic child")
        cyclic.__context__ = cyclic
        group = ExceptionGroup(
            "outer cleanup group",
            [ExceptionGroup("inner cleanup group", [child, cyclic]), child],
        )
        raise group

    async def endpoint(request):
        return PlainTextResponse("hello\n")

    async def scenario():
        app = Starlette(routes=[Route("/", endpoint)])
        async with live_server(app) as server:
            monkeypatch.setattr(Stream, "aclose", failing_close)
            with pytest.raises(ValueError, match="original body") as caught:
                async with server.lines("/"):
                    raise ValueError("original body")
            notes = " ".join(caught.value.__notes__)
            for detail in (
                "outer cleanup group",
                "inner cleanup group",
                "group child failure",
                "group child note",
                "group child cause",
                "cyclic child",
            ):
                assert detail in notes
            assert (
                caught.value.__notes__.count(
                    "Stream cleanup failure: RuntimeError('group child failure')"
                )
                == 1
            )

    run(scenario())


def test_injected_connect_error_preserves_cause_and_closes_client(monkeypatch):
    import pytest_stream_lifecycle.streams as streams_module

    instances = []
    cause = httpx.ConnectError("injected connection refusal")

    class RequestContext:
        async def __aenter__(self):
            raise cause

        async def __aexit__(self, *args):
            raise AssertionError("an unentered request must not be exited")

    class Client:
        def __init__(self, **kwargs):
            self.closed = False
            instances.append(self)

        def stream(self, *args, **kwargs):
            return RequestContext()

        async def aclose(self):
            self.closed = True

    async def scenario():
        monkeypatch.setattr(streams_module.httpx, "AsyncClient", Client)
        with pytest.raises(LifecycleError, match="request failed") as caught:
            async with streams_module.open_stream("http://127.0.0.1", "/", kind="lines"):
                pass
        assert caught.value.__cause__ is cause
        assert len(instances) == 1 and instances[0].closed

    run(scenario())
