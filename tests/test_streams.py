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


@pytest.mark.parametrize(
    "content_type",
    [
        "text/event-stream",
        "Text/Event-Stream",
        "text/event-stream; charset=UTF-8",
        "Text/Event-Stream; charset=UTF-8; custom=Preserved",
    ],
)
def test_sse_content_type_is_case_insensitive_and_preserves_parameters(content_type):
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", content_type.encode("ascii"))],
            }
        )
        await send({"type": "http.response.body", "body": b"data: first\n\n"})

    async def scenario():
        async with live_server(app, lifespan="off") as server, server.sse("/") as stream:
            assert (await stream.next()).data == "first"
            assert (
                stream._response.headers["content-type"].partition(";")[1:]
                == content_type.partition(";")[1:]
            )

    run(scenario())


@pytest.mark.parametrize("iterator_kind", ["async-generator", "sync-returned-awaitable"])
def test_close_during_pending_read_cancels_only_owned_operation(iterator_kind):
    from pytest_stream_lifecycle.streams import Stream

    async def scenario():
        entered = asyncio.Event()
        reader_stopped = asyncio.Event()
        release_caller = asyncio.Event()
        operations = set()
        steps = []

        async def read_operation():
            operations.add(asyncio.current_task())
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                steps.append("read finalizer")

        async def generator():
            await read_operation()
            yield "late"

        class Awaitable:
            def __await__(self):
                return read_operation().__await__()

        class Iterator:
            def __anext__(self):
                return Awaitable()

            async def aclose(self):
                steps.append("iterator close")

        class ResponseContext:
            async def __aexit__(self, *args):
                steps.append("response close")

        class Client:
            async def aclose(self):
                steps.append("client close")

        iterator = generator() if iterator_kind == "async-generator" else Iterator()
        stream = Stream(Client(), ResponseContext(), None, iterator, 5, "pending")

        async def caller():
            try:
                await stream.next()
            except asyncio.CancelledError:
                reader_stopped.set()
            # The caller has work unrelated to the owned read. Closing the
            # stream must not cancel this whole task or its later work.
            await release_caller.wait()
            return "caller survived"

        caller_task = asyncio.create_task(caller())
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            await asyncio.wait_for(stream.aclose(), timeout=1)
            await asyncio.wait_for(reader_stopped.wait(), timeout=1)
            assert caller_task.cancelling() == 0 and not caller_task.done()
            assert all(task.done() for task in operations)
            expected = ["read finalizer"]
            if iterator_kind != "async-generator":
                expected.append("iterator close")
            assert steps == [*expected, "response close", "client close"]
            await stream.aclose()
            assert steps == [*expected, "response close", "client close"]
            release_caller.set()
            assert await asyncio.wait_for(caller_task, timeout=1) == "caller survived"
            with pytest.raises(LifecycleError, match="closed"):
                await stream.next()
        finally:
            release_caller.set()
            if not caller_task.done():
                caller_task.cancel()
            await asyncio.gather(caller_task, return_exceptions=True)
            await asyncio.gather(stream.aclose(), return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_overlapping_closes_wait_for_one_cleanup(cleanup_fails):
    from pytest_stream_lifecycle.streams import Stream

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        second_entered = asyncio.Event()
        steps = []
        cleanup_error = RuntimeError("shared cleanup failure")

        class Iterator:
            async def aclose(self):
                steps.append("iterator close")
                entered.set()
                await release.wait()
                if cleanup_fails:
                    raise cleanup_error

        class ResponseContext:
            async def __aexit__(self, *args):
                steps.append("response close")

        class Client:
            async def aclose(self):
                steps.append("client close")

        stream = Stream(Client(), ResponseContext(), None, Iterator(), 1, "overlap")

        async def second_close():
            second_entered.set()
            await stream.aclose()

        closers = [asyncio.create_task(stream.aclose())]
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            closers.append(asyncio.create_task(second_close()))
            await asyncio.wait_for(second_entered.wait(), timeout=1)
            assert not any(task.done() for task in closers)
            release.set()
            outcomes = await asyncio.wait_for(
                asyncio.gather(*closers, return_exceptions=True), timeout=1
            )
            assert outcomes == ([cleanup_error, cleanup_error] if cleanup_fails else [None, None])
            assert steps == ["iterator close", "response close", "client close"]
        finally:
            release.set()
            await asyncio.gather(*closers, return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_repeated_cancellation_during_close_waits_for_cleanup(cleanup_fails):
    from pytest_stream_lifecycle.streams import Stream

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()
        cleanup_tasks = set()
        steps = []

        class Iterator:
            async def aclose(self):
                cleanup_tasks.add(asyncio.current_task())
                entered.set()
                await release.wait()
                steps.append("iterator close")
                if cleanup_fails:
                    error = RuntimeError("cancelled close finalizer")
                    error.add_note("cancelled close note")
                    raise error from ValueError("cancelled close cause")

        class ResponseContext:
            async def __aexit__(self, *args):
                steps.append("response close")

        class Client:
            async def aclose(self):
                steps.append("client close")
                finished.set()

        stream = Stream(Client(), ResponseContext(), None, Iterator(), 1, "cancelled")
        close_task = asyncio.create_task(stream.aclose())
        unrelated = asyncio.create_task(asyncio.Event().wait())
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            close_task.cancel("first close cancellation")
            await asyncio.sleep(0)  # Deliver the first cancellation before the next one.
            close_task.cancel("second close cancellation")
            await asyncio.sleep(0)
            assert not close_task.done() and not finished.is_set()
            release.set()
            with pytest.raises(asyncio.CancelledError, match="first close cancellation") as caught:
                await asyncio.wait_for(close_task, timeout=1)
            assert finished.is_set()
            assert steps == ["iterator close", "response close", "client close"]
            if cleanup_fails:
                notes = " ".join(caught.value.__notes__)
                for detail in (
                    "cancelled close finalizer",
                    "cancelled close note",
                    "cancelled close cause",
                ):
                    assert detail in notes
            assert cleanup_tasks and all(task.done() for task in cleanup_tasks)
            assert not unrelated.done()
        finally:
            release.set()
            unrelated.cancel()
            await asyncio.gather(close_task, unrelated, return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("primary_kind", ["timeout", "cancellation"])
def test_pending_read_finalizer_preserves_primary_under_repeated_cancellation(primary_kind):
    from pytest_stream_lifecycle.streams import Stream

    async def scenario():
        entered = asyncio.Event()
        finalizing = asyncio.Event()
        release = asyncio.Event()
        operations = set()
        steps = []

        async def iterator():
            operations.add(asyncio.current_task())
            entered.set()
            try:
                await asyncio.Event().wait()
                yield "late"
            finally:
                finalizing.set()
                await release.wait()
                steps.append("read finalizer")
                error = RuntimeError("pending read finalizer")
                error.add_note("pending read note")
                raise error from ValueError("pending read cause")

        class ResponseContext:
            async def __aexit__(self, *args):
                steps.append("response close")

        class Client:
            async def aclose(self):
                steps.append("client close")

        stream = Stream(Client(), ResponseContext(), None, iterator(), 0.05, "read")
        reader = asyncio.create_task(stream.next())
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            if primary_kind == "cancellation":
                reader.cancel("original read cancellation")
            await asyncio.wait_for(finalizing.wait(), timeout=1)
            reader.cancel("later read cancellation")
            await asyncio.sleep(0)
            reader.cancel("last read cancellation")
            await asyncio.sleep(0)
            assert not reader.done()
            release.set()
            expected = LifecycleError if primary_kind == "timeout" else asyncio.CancelledError
            message = "timed out" if primary_kind == "timeout" else "original read cancellation"
            with pytest.raises(expected, match=message) as caught:
                await asyncio.wait_for(reader, timeout=1)
            notes = " ".join(caught.value.__notes__)
            for detail in ("pending read finalizer", "pending read note", "pending read cause"):
                assert detail in notes
            assert steps == ["read finalizer", "response close", "client close"]
            assert operations and all(task.done() for task in operations)
        finally:
            release.set()
            if not reader.done():
                reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
            await asyncio.gather(*operations, return_exceptions=True)
            await asyncio.gather(stream.aclose(), return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("cancel_caller", [False, True])
def test_close_cancellation_at_cleanup_completion_preserves_primary(cancel_caller):
    from pytest_stream_lifecycle.streams import Stream

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        steps = []
        cleanup_error = asyncio.CancelledError("cleanup cancelled itself")

        class Iterator:
            async def aclose(self):
                entered.set()
                await release.wait()
                raise cleanup_error

        class ResponseContext:
            async def __aexit__(self, *args):
                steps.append("response close")

        class Client:
            async def aclose(self):
                steps.append("client close")

        stream = Stream(Client(), ResponseContext(), None, Iterator(), 1, "completion")
        closer = asyncio.create_task(stream.aclose())
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            # Queue cleanup completion first, then inject caller cancellation
            # before either task resumes. Both outcomes are ready together.
            release.set()
            if cancel_caller:
                closer.cancel("caller cancellation")
            message = "caller cancellation" if cancel_caller else "cleanup cancelled itself"
            with pytest.raises(asyncio.CancelledError, match=message) as caught:
                await asyncio.wait_for(closer, timeout=1)
            assert steps == ["response close", "client close"]
            if cancel_caller:
                assert any("cleanup cancelled itself" in note for note in caught.value.__notes__)
            with pytest.raises(
                asyncio.CancelledError, match="cleanup cancelled itself"
            ) as repeated:
                await stream.aclose()
            assert repeated.value is cleanup_error
        finally:
            release.set()
            await asyncio.gather(closer, return_exceptions=True)

    run(scenario())


def test_context_body_failure_joins_pending_real_socket_read():
    async def scenario():
        probe = LifecycleProbe()
        release = asyncio.Event()
        reader = None
        failure = ValueError("body interrupted a pending read")
        async with live_server(disconnect_app(probe, release=release)) as server:
            try:
                with pytest.raises(ValueError, match="body interrupted") as caught:
                    async with server.sse("/") as stream:
                        assert (await stream.next()).data == "one"
                        assert (await stream.next()).data == "two"
                        reader = asyncio.create_task(stream.next())

                        async def iterator_running():
                            while not stream._iterator.ag_running:
                                assert not reader.done()
                                await asyncio.sleep(0)

                        await asyncio.wait_for(iterator_running(), timeout=1)
                        raise failure
                assert caught.value is failure
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(reader, timeout=1)
                assert reader.cancelling() == 0
                assert stream._read_task.done() and stream._close_task.done()
                # Check instrumented cleanup before server teardown can hide it.
                await probe.wait_closed("producer", "upstream", timeout=1)
            finally:
                release.set()
                if reader is not None:
                    if not reader.done():
                        reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("cancel_caller", [False, True])
def test_iterator_cancellation_preserves_diagnostics_and_caller_priority(cancel_caller):
    from pytest_stream_lifecycle.streams import Stream

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        steps = []
        cause = ValueError("iterator cancellation cause")
        diagnostic = asyncio.CancelledError("iterator cancellation detail")
        diagnostic.add_note("iterator cancellation note")
        diagnostic.__cause__ = cause

        class Iterator:
            async def __anext__(self):
                entered.set()
                await release.wait()
                raise diagnostic

            async def aclose(self):
                steps.append("iterator close")

        class ResponseContext:
            async def __aexit__(self, *args):
                steps.append("response close")

        class Client:
            async def aclose(self):
                steps.append("client close")

        stream = Stream(Client(), ResponseContext(), None, Iterator(), 5, "diagnostic")

        async def caller():
            try:
                await stream.next()
            except asyncio.CancelledError as exc:
                return exc
            pytest.fail("iterator cancellation was not raised")

        reader = asyncio.create_task(caller())
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            release.set()
            if cancel_caller:
                # Queue the iterator's diagnostic failure before the cancelled
                # caller resumes; the caller's cancellation remains primary.
                reader.cancel("caller cancellation detail")
            error = await asyncio.wait_for(reader, timeout=1)
            if cancel_caller:
                assert str(error) == "caller cancellation detail"
                notes = " ".join(getattr(error, "__notes__", ()))
                for detail in (
                    "iterator cancellation detail",
                    "iterator cancellation cause",
                    "iterator cancellation note",
                ):
                    assert detail in notes
            else:
                assert error is diagnostic
                assert error.__cause__ is cause
                assert "iterator cancellation note" in error.__notes__
            assert steps == ["iterator close", "response close", "client close"]
            assert stream._read_task.done() and stream._close_task.done()
        finally:
            release.set()
            if not reader.done():
                reader.cancel()
            await asyncio.gather(reader, stream.aclose(), return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("replace_cancellation", [False, True])
def test_pending_read_diagnostic_cancellation_is_not_success(replace_cancellation):
    from pytest_stream_lifecycle.streams import Stream

    async def scenario():
        entered = asyncio.Event()
        diagnostics = []
        cause = ValueError("read finalizer cancellation cause")
        steps = []

        async def iterator():
            entered.set()
            try:
                await asyncio.Event().wait()
                yield "late"
            except asyncio.CancelledError as exc:
                error = (
                    asyncio.CancelledError("read finalizer cancellation")
                    if replace_cancellation
                    else exc
                )
                error.add_note("read finalizer cancellation note")
                diagnostics.append(error)
                raise error from cause
            finally:
                steps.append("read finalizer")

        class ResponseContext:
            async def __aexit__(self, *args):
                steps.append("response close")

        class Client:
            async def aclose(self):
                steps.append("client close")

        stream = Stream(Client(), ResponseContext(), None, iterator(), 5, "finalizer")

        async def caller():
            try:
                await stream.next()
            except asyncio.CancelledError as exc:
                return exc
            pytest.fail("pending read was not interrupted")

        reader = asyncio.create_task(caller())
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            with pytest.raises(asyncio.CancelledError) as caught:
                await stream.aclose()
            assert caught.value is diagnostics[0]
            assert caught.value.__cause__ is cause
            assert "read finalizer cancellation note" in caught.value.__notes__
            assert await asyncio.wait_for(reader, timeout=1) is diagnostics[0]
            with pytest.raises(asyncio.CancelledError) as repeated:
                await stream.aclose()
            assert repeated.value is diagnostics[0]
            assert steps == ["read finalizer", "response close", "client close"]
            assert stream._read_task.done() and stream._close_task.done()
        finally:
            if not reader.done():
                reader.cancel()
            await asyncio.gather(reader, stream.aclose(), return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("outcome", ["success", "failure", "close"])
def test_read_completion_race_preserves_caller_cancellation(outcome):
    from pytest_stream_lifecycle.streams import Stream

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        steps = []
        cause = ValueError("completed read cause")
        failure = RuntimeError("completed read failure")
        failure.add_note("completed read note")

        class Iterator:
            async def __anext__(self):
                entered.set()
                try:
                    await release.wait()
                    if outcome == "failure":
                        raise failure from cause
                    if outcome == "close":
                        await asyncio.Event().wait()
                    return "completed read value"
                finally:
                    steps.append("read finalizer")

            async def aclose(self):
                steps.append("iterator close")

        class ResponseContext:
            async def __aexit__(self, *args):
                steps.append("response close")

        class Client:
            async def aclose(self):
                steps.append("client close")

        stream = Stream(Client(), ResponseContext(), None, Iterator(), 5, "completion race")

        async def caller():
            try:
                return await stream.next()
            except (asyncio.CancelledError, LifecycleError) as exc:
                return exc

        reader = asyncio.create_task(caller())
        unrelated = asyncio.create_task(asyncio.Event().wait())
        closer = None
        try:
            async with asyncio.timeout(1):
                await entered.wait()
            # Shield's callback runs first and completes its future. Cancel the
            # caller next, before it resumes: Python 3.11 wait_for loses this
            # cancellation when it sees that completed future.
            stream._read_task.add_done_callback(
                lambda _: reader.cancel("caller completion-race cancellation")
            )
            release.set()
            if outcome == "close":
                closer = asyncio.create_task(stream.aclose())
            async with asyncio.timeout(1):
                error = await reader
                if closer is not None:
                    await closer
            assert isinstance(error, asyncio.CancelledError)
            assert str(error) == "caller completion-race cancellation"
            if outcome == "failure":
                notes = " ".join(getattr(error, "__notes__", ()))
                for detail in (
                    "completed read failure",
                    "completed read cause",
                    "completed read note",
                ):
                    assert detail in notes
            assert steps == ["read finalizer", "iterator close", "response close", "client close"]
            assert stream._read_task.done() and stream._close_task.done()
            assert not unrelated.done() and unrelated.cancelling() == 0
        finally:
            release.set()
            tasks = [reader, unrelated]
            if closer is not None:
                tasks.append(closer)
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, stream.aclose(), return_exceptions=True)

    run(scenario())
