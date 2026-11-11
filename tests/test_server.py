import asyncio
import math
import subprocess
import sys
import textwrap
from contextlib import asynccontextmanager

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from pytest_stream_lifecycle import ServerError, live_server


def run(coro):
    return asyncio.run(coro)


def app_with_events(events):
    @asynccontextmanager
    async def lifespan(app):
        events.append("startup")
        yield
        events.append("shutdown")

    async def homepage(request):
        return PlainTextResponse("real HTTP")

    return Starlette(routes=[Route("/", homepage)], lifespan=lifespan)


async def assert_port_closed(url):
    port = int(url.rsplit(":", 1)[1])
    with pytest.raises(OSError):
        await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout=1)


def test_fixture_real_http_and_lifespan(stream_server):
    async def scenario():
        events = []
        async with stream_server(app_with_events(events)) as server:
            assert events == ["startup"]
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.get(server.url)
                assert response.status_code == 200
                assert response.text == "real HTTP"
                assert response.http_version == "HTTP/1.1"
        assert events == ["startup", "shutdown"]
        await assert_port_closed(server.url)

    run(scenario())


def test_two_servers():
    async def scenario():
        async with (
            live_server(app_with_events([])) as first,
            live_server(app_with_events([])) as second,
        ):
            assert first.url != second.url
            async with httpx.AsyncClient(trust_env=False) as client:
                assert (await client.get(first.url)).text == "real HTTP"
                assert (await client.get(second.url)).text == "real HTTP"
        await assert_port_closed(first.url)
        await assert_port_closed(second.url)

    run(scenario())


def test_body_exception_closes_server():
    async def scenario():
        events = []
        with pytest.raises(RuntimeError, match="body failure"):
            async with (
                live_server(app_with_events(events)) as server,
                httpx.AsyncClient(trust_env=False) as client,
            ):
                assert (await client.get(server.url)).status_code == 200
                raise RuntimeError("body failure")
        assert events == ["startup", "shutdown"]
        await assert_port_closed(server.url)

    run(scenario())


def test_startup_failure_and_timeout_do_not_leak_owned_tasks():
    async def fail(scope, receive, send):
        await receive()
        await send({"type": "lifespan.startup.failed", "message": "failure"})

    async def hang(scope, receive, send):
        await receive()
        await asyncio.Event().wait()

    async def scenario():
        unrelated = asyncio.create_task(asyncio.Event().wait())
        try:
            for app, expected in ((fail, "startup"), (hang, "startup timed out")):
                before = asyncio.get_running_loop().time()
                with pytest.raises(ServerError, match=expected):
                    async with live_server(app, startup_timeout=0.1, shutdown_timeout=0.1):
                        pass
                assert asyncio.get_running_loop().time() - before < 1.0
                await asyncio.sleep(0.01)
                assert not [
                    t
                    for t in asyncio.all_tasks()
                    if t is not asyncio.current_task() and t is not unrelated and not t.done()
                ]
                assert not unrelated.cancelled()
        finally:
            unrelated.cancel()
            await asyncio.gather(unrelated, return_exceptions=True)

    run(scenario())


def test_lifespan_off():
    async def app(scope, receive, send):
        assert scope["type"] == "http"
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"off"})

    async def scenario():
        async with (
            live_server(app, lifespan="off") as server,
            httpx.AsyncClient(trust_env=False) as client,
        ):
            assert (await client.get(server.url)).text == "off"

    run(scenario())


@pytest.mark.parametrize("value", [0, -1, math.inf, -math.inf, math.nan])
@pytest.mark.parametrize("option", ["startup_timeout", "shutdown_timeout"])
def test_timeout_validation(value, option):
    async def scenario():
        with pytest.raises(ValueError, match=option):
            async with live_server(app_with_events([]), **{option: value}):
                pass

    run(scenario())


def test_invalid_lifespan():
    async def scenario():
        with pytest.raises(ValueError, match="lifespan"):
            async with live_server(app_with_events([]), lifespan="bad"):
                pass

    run(scenario())


def test_installed_entrypoint_discovery(tmp_path):
    (tmp_path / "test_discovery.py").write_text(
        textwrap.dedent("""
        def test_discovery(stream_server, stream_probe):
            assert callable(stream_server)
            assert stream_probe.started_count
    """)
    )
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", str(tmp_path)],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_shutdown_timeout_is_bounded_and_does_not_cancel_unrelated_task():
    async def scenario():
        shutdown_entered = asyncio.Event()

        async def app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            shutdown_entered.set()
            await asyncio.Event().wait()

        unrelated = asyncio.create_task(asyncio.Event().wait())
        try:
            with pytest.raises(ServerError, match="shutdown timed out"):
                async with live_server(app, shutdown_timeout=0.5) as server:
                    pass
            assert shutdown_entered.is_set()
            await asyncio.sleep(0.01)
            await assert_port_closed(server.url)
            assert not unrelated.done()
            assert not [
                t
                for t in asyncio.all_tasks()
                if t is not asyncio.current_task() and t is not unrelated and not t.done()
            ]
        finally:
            unrelated.cancel()
            await asyncio.gather(unrelated, return_exceptions=True)

    run(scenario())


def test_active_stream_is_cleaned_and_signals_are_unchanged():
    import signal

    from starlette.responses import StreamingResponse

    async def scenario():
        stopped = asyncio.Event()
        active = asyncio.Event()
        unrelated = asyncio.create_task(asyncio.Event().wait())
        original = signal.getsignal(signal.SIGINT)

        async def body():
            try:
                yield b"started\n"
                active.set()
                await asyncio.Event().wait()
            finally:
                stopped.set()

        async def endpoint(request):
            return StreamingResponse(body(), media_type="text/plain")

        app = Starlette(routes=[Route("/", endpoint)])
        try:
            async with httpx.AsyncClient(trust_env=False, timeout=None) as client:
                async with live_server(app, shutdown_timeout=1) as server:
                    stream = client.stream("GET", server.url)
                    response = await stream.__aenter__()
                    chunks = response.aiter_bytes()
                    assert await asyncio.wait_for(anext(chunks), timeout=1) == b"started\n"
                    await asyncio.wait_for(active.wait(), timeout=1)
                    assert not stopped.is_set()
                    assert signal.getsignal(signal.SIGINT) == original
                assert stopped.is_set()
                await stream.__aexit__(None, None, None)
            assert not unrelated.done()
            assert signal.getsignal(signal.SIGINT) == original
            await assert_port_closed(server.url)
        finally:
            unrelated.cancel()
            await asyncio.gather(unrelated, return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("failure_kind", ["message", "exception"])
@pytest.mark.parametrize("body_fails", [False, True])
def test_lifespan_shutdown_failures_are_reported(failure_kind, body_fails):
    async def app(scope, receive, send):
        await receive()
        await send({"type": "lifespan.startup.complete"})
        await receive()
        if failure_kind == "message":
            await send({"type": "lifespan.shutdown.failed", "message": "shutdown detail"})
        else:
            raise RuntimeError("shutdown exception detail")

    async def scenario():
        if body_fails:
            with pytest.raises(ValueError, match="body detail") as caught:
                async with live_server(app):
                    raise ValueError("body detail")
            detail = "shutdown detail" if failure_kind == "message" else "shutdown exception detail"
            assert any(detail in note for note in caught.value.__notes__)
        else:
            with pytest.raises(ServerError, match="lifespan") as caught:
                async with live_server(app):
                    pass
            if failure_kind == "exception":
                assert isinstance(caught.value.__cause__, RuntimeError)
                assert str(caught.value.__cause__) == "shutdown exception detail"
            else:
                assert "shutdown detail" in str(caught.value)

    run(scenario())


def test_logging_configuration_is_preserved_during_nested_servers():
    import logging

    loggers = [
        logging.getLogger(name) for name in ("", "uvicorn.access", "uvicorn.error", "uvicorn.asgi")
    ]
    access = logging.getLogger("uvicorn.access")
    handler = logging.NullHandler()
    access.addHandler(handler)

    def snapshot():
        return [(tuple(logger.handlers), logger.level, logger.propagate) for logger in loggers]

    before = snapshot()
    before_level_names = logging.getLevelNamesMapping()

    async def scenario():
        assert snapshot() == before
        async with live_server(app_with_events([])):
            assert snapshot() == before
            assert logging.getLevelNamesMapping() == before_level_names
            async with live_server(app_with_events([])):
                assert snapshot() == before
            assert snapshot() == before
        assert snapshot() == before
        assert logging.getLevelNamesMapping() == before_level_names

    try:
        run(scenario())
    finally:
        access.removeHandler(handler)


def test_repeated_cancellation_during_shutdown_waits_for_cleanup():
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            entered.set()
            await release.wait()
            await send({"type": "lifespan.shutdown.complete"})
            finished.set()

        async def host():
            async with live_server(app, shutdown_timeout=2) as server:
                return server.url

        host_task = asyncio.create_task(host())
        try:
            await asyncio.wait_for(entered.wait(), timeout=3)
            host_task.cancel("first")
            await asyncio.sleep(0)
            host_task.cancel("second")
            await asyncio.sleep(0)
            assert not host_task.done()
            release.set()
            with pytest.raises(asyncio.CancelledError, match="first"):
                await asyncio.wait_for(host_task, timeout=1)
            assert finished.is_set()
        finally:
            release.set()
            if not host_task.done():
                host_task.cancel()
            await asyncio.gather(host_task, return_exceptions=True)

    run(scenario())


def test_forced_shutdown_closes_idle_accepted_connection_only(monkeypatch):
    import pytest_stream_lifecycle.server as server_module

    instances = []

    class BlockedServer(server_module._Server):
        def __init__(self, config):
            super().__init__(config)
            instances.append(self)

        async def main_loop(self):
            await asyncio.Event().wait()

    monkeypatch.setattr(server_module, "_Server", BlockedServer)

    async def scenario():
        def unrelated_handler(reader, writer):
            writer.write(b"alive")
            writer.close()

        unrelated = await asyncio.start_server(unrelated_handler, "127.0.0.1", 0)
        other_port = unrelated.sockets[0].getsockname()[1]
        writer = None
        try:
            with pytest.raises(ServerError, match="shutdown timed out"):
                async with live_server(app_with_events([]), shutdown_timeout=0.15) as server:
                    port = int(server.url.rsplit(":", 1)[1])
                    reader, writer = await asyncio.open_connection("127.0.0.1", port)

                    async def wait_accepted():
                        while not instances[0].server_state.connections:
                            await asyncio.sleep(0)

                    await asyncio.wait_for(wait_accepted(), timeout=1)
            try:
                remaining = await asyncio.wait_for(reader.read(), timeout=1)
            except ConnectionError:
                # A forced transport close can be EOF or a reset across OSes.
                remaining = b""
            assert remaining == b""
            other_reader, other_writer = await asyncio.open_connection("127.0.0.1", other_port)
            assert await asyncio.wait_for(other_reader.read(5), timeout=1) == b"alive"
            other_writer.close()
            await other_writer.wait_closed()
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except ConnectionError:
                    pass  # A reset is an expected forced-close outcome.
            unrelated.close()
            await unrelated.wait_closed()

    run(scenario())


@pytest.mark.parametrize("body_fails", [False, True])
@pytest.mark.parametrize("grouped", [False, True])
def test_finalizer_exception_is_reported_on_forced_shutdown(body_fails, grouped):
    async def scenario():
        entered = asyncio.Event()
        loop_errors = []
        loop = asyncio.get_running_loop()
        old_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: loop_errors.append(context))

        async def app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                try:
                    raise ValueError("shutdown root cause")
                except ValueError as cause:
                    error = RuntimeError("shutdown finalizer exploded")
                    error.add_note("shutdown finalizer note")
                    error.__cause__ = cause
                    if grouped:
                        raise ExceptionGroup("shutdown group", [ExceptionGroup("inner", [error])])
                    raise error

        try:
            expected = ValueError if body_fails else ServerError
            message = "body failure" if body_fails else "shutdown timed out"
            with pytest.raises(expected, match=message) as caught:
                async with live_server(app, shutdown_timeout=0.5):
                    if body_fails:
                        raise ValueError("body failure")
            assert entered.is_set()
            notes = " ".join(caught.value.__notes__)
            assert "shutdown finalizer exploded" in notes
            assert "shutdown finalizer note" in notes
            assert "shutdown root cause" in notes
            await asyncio.sleep(0)
            assert not any(
                "never retrieved" in context.get("message", "") for context in loop_errors
            )
        finally:
            loop.set_exception_handler(old_handler)

    run(scenario())


def test_cancelled_server_task_exception_is_retrieved(monkeypatch):
    import pytest_stream_lifecycle.server as server_module

    async def scenario():
        entered = asyncio.Event()
        loop_errors = []
        loop = asyncio.get_running_loop()
        old_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: loop_errors.append(context))

        class FailingServer(server_module._Server):
            async def main_loop(self):
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    raise RuntimeError("server finalizer exploded")

        monkeypatch.setattr(server_module, "_Server", FailingServer)
        try:
            with pytest.raises(ServerError, match="shutdown timed out") as caught:
                async with live_server(app_with_events([]), shutdown_timeout=0.15):
                    await asyncio.wait_for(entered.wait(), timeout=1)
            assert any("server finalizer exploded" in note for note in caught.value.__notes__)
            await asyncio.sleep(0)
            assert not any(
                "never retrieved" in context.get("message", "") for context in loop_errors
            )
        finally:
            loop.set_exception_handler(old_handler)

    run(scenario())


@pytest.mark.parametrize("failure_kind", ["message", "exception"])
@pytest.mark.parametrize("lifespan", ["on", "auto"])
def test_startup_failure_preserves_detail_and_cause(failure_kind, lifespan):
    async def app(scope, receive, send):
        await receive()
        if failure_kind == "message":
            await send({"type": "lifespan.startup.failed", "message": "startup detail"})
        else:
            raise RuntimeError("startup exception detail")

    async def scenario():
        if failure_kind == "exception" and lifespan == "auto":
            # Uvicorn treats unsupported lifespan as a valid auto fallback.
            async with live_server(app, lifespan=lifespan):
                pass
        else:
            with pytest.raises(ServerError, match="startup.*detail") as caught:
                async with live_server(app, lifespan=lifespan):
                    pass
            if failure_kind == "exception":
                assert isinstance(caught.value.__cause__, RuntimeError)
                assert str(caught.value.__cause__) == "startup exception detail"

    run(scenario())


def test_auto_lifespan_fallback_still_serves_http():
    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            raise RuntimeError("unsupported")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"fallback"})

    async def scenario():
        async with (
            live_server(app, lifespan="auto") as server,
            httpx.AsyncClient(trust_env=False) as client,
        ):
            assert (await client.get(server.url)).text == "fallback"

    run(scenario())


@pytest.mark.parametrize("primary_kind", ["body", "cancellation"])
def test_primary_exception_preserves_server_task_failure_detail(monkeypatch, primary_kind):
    import pytest_stream_lifecycle.server as server_module

    async def scenario():
        ready = asyncio.Event()
        release_server = asyncio.Event()
        release_body = asyncio.Event()

        class FailingServer(server_module._Server):
            async def main_loop(self):
                await release_server.wait()
                raise RuntimeError("actual server failure detail")

        monkeypatch.setattr(server_module, "_Server", FailingServer)

        async def host():
            async with live_server(app_with_events([]), shutdown_timeout=1):
                ready.set()
                await release_body.wait()
                raise ValueError("body detail")

        host_task = asyncio.create_task(host())
        await asyncio.wait_for(ready.wait(), timeout=1)
        if primary_kind == "body":
            release_body.set()
        else:
            host_task.cancel()
        await asyncio.sleep(0)
        release_server.set()
        expected = ValueError if primary_kind == "body" else asyncio.CancelledError
        with pytest.raises(expected) as caught:
            await asyncio.wait_for(host_task, timeout=2)
        assert any("actual server failure detail" in note for note in caught.value.__notes__)

    run(scenario())


def test_shutdown_budget_allows_poll_and_scheduler_overhead(monkeypatch):
    from starlette.responses import StreamingResponse

    import pytest_stream_lifecycle.server as server_module

    async def scenario():
        active = asyncio.Event()
        cleaned = asyncio.Event()

        class DelayedServer(server_module._Server):
            async def main_loop(self):
                while not self.should_exit:
                    await asyncio.sleep(0.01)
                # Model the main-loop polling and scheduling overhead.
                await asyncio.sleep(0.1)

            async def shutdown(self, sockets=None):
                await asyncio.sleep(0.05)
                await super().shutdown(sockets=sockets)

        monkeypatch.setattr(server_module, "_Server", DelayedServer)

        async def body():
            try:
                yield b"start\n"
                active.set()
                await asyncio.Event().wait()
            finally:
                cleaned.set()

        async def endpoint(request):
            return StreamingResponse(body())

        app = Starlette(routes=[Route("/", endpoint)])
        async with httpx.AsyncClient(trust_env=False, timeout=None) as client:
            async with live_server(app, shutdown_timeout=1) as server:
                response_context = client.stream("GET", server.url)
                response = await response_context.__aenter__()
                chunks = response.aiter_bytes()
                assert await asyncio.wait_for(anext(chunks), 1) == b"start\n"
                await asyncio.wait_for(active.wait(), 1)
                started = asyncio.get_running_loop().time()
            assert cleaned.is_set()
            assert asyncio.get_running_loop().time() - started < 1.5
            await response_context.__aexit__(None, None, None)

    run(scenario())


@pytest.mark.parametrize("server_fails", [False, True])
def test_stop_preserves_all_owned_task_failures(server_fails):
    import socket
    from types import SimpleNamespace

    from pytest_stream_lifecycle.server import _stop

    async def scenario():
        async def failing_task(message):
            try:
                raise ValueError(f"{message} cause")
            except ValueError as cause:
                error = RuntimeError(message)
                error.add_note(f"{message} note")
                raise error from cause

        owned = [asyncio.create_task(failing_task(f"owned {i}")) for i in (1, 2)]
        await asyncio.wait(owned)
        errors = [task.exception() for task in owned]
        assert all(errors)
        if server_fails:
            server_task = asyncio.create_task(failing_task("server failure"))
        else:
            server_task = asyncio.create_task(asyncio.sleep(0))
        await asyncio.wait({server_task})
        server = SimpleNamespace(
            should_exit=False,
            force_exit=False,
            servers=(),
            server_state=SimpleNamespace(connections=set()),
        )
        with socket.socket() as sock, pytest.raises(ServerError) as caught:
            await _stop(server, server_task, sock, set(), 1, errors)
        error = caught.value
        if server_fails:
            assert isinstance(error.__cause__, RuntimeError)
            assert str(error.__cause__) == "server failure"
        else:
            assert error.__cause__ is errors[0]
        details = f"{error!r} {error.__cause__!r} {' '.join(error.__notes__)}"
        if server_fails:
            for detail in ("owned 1", "owned 1 note", "owned 1 cause"):
                assert detail in details
        for detail in ("owned 2", "owned 2 note", "owned 2 cause"):
            assert detail in details

    run(scenario())
