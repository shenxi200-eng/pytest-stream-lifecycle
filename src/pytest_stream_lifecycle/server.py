"""In-process, asyncio-only Uvicorn server on a reserved loopback socket.

Cancellation-resistant application code cannot be forcibly killed in process.
Timeouts bound our waits; any tasks that ignore cancellation may continue running.
"""

from __future__ import annotations

import asyncio
import math
import socket
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any, Literal

import uvicorn

from ._errors import add_exception_notes

ASGIApp = Callable[..., Any]
Lifespan = Literal["on", "off", "auto"]


class ServerError(RuntimeError):
    """The server failed to start or stop within its allotted time."""


@dataclass(frozen=True, slots=True)
class ServerHandle:
    """Address of a running HTTP server."""

    url: str

    def sse(self, path: str, *, method: str = "GET", timeout: float = 2.0, **request_kwargs: Any):
        """Open a bounded SSE reader over the server's real HTTP socket."""
        from .streams import open_stream

        return open_stream(
            self.url, path, kind="sse", method=method, timeout=timeout, **request_kwargs
        )

    def lines(self, path: str, *, method: str = "GET", timeout: float = 2.0, **request_kwargs: Any):
        """Open a bounded line reader over the server's real HTTP socket."""
        from .streams import open_stream

        return open_stream(
            self.url, path, kind="lines", method=method, timeout=timeout, **request_kwargs
        )


class _Config(uvicorn.Config):
    def configure_logging(self) -> None:
        # Uvicorn's default method changes global logging even when
        # log_config=None (and removes access handlers when access_log=False).
        pass


class _Server(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        # Embedded server must never replace the test process's signal handlers.
        yield


def _timeout(name: str, value: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive finite number") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return number


async def _wait_until_started(server: _Server, task: asyncio.Task[None], timeout: float) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not server.started:
        if task.done():
            try:
                task.result()
            except BaseException as exc:
                raise ServerError("server failed during startup") from exc
            raise ServerError("server exited before startup completed")
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise ServerError(f"server startup timed out after {timeout:g}s")
        await asyncio.wait({task}, timeout=min(0.01, remaining))


async def _stop(
    server: _Server,
    task: asyncio.Task[None],
    sock: socket.socket,
    app_tasks: set[asyncio.Task[Any]],
    timeout: float,
    task_errors: list[BaseException],
) -> None:
    def close_connections() -> None:
        # Stop admission before taking the forced-cleanup task snapshot.
        for listener in getattr(server, "servers", ()):
            listener.close()
        for connection in tuple(server.server_state.connections):
            transport = getattr(connection, "transport", None)
            if transport is not None:
                transport.abort()
        sock.close()

    def owned_tasks() -> set[asyncio.Task[Any]]:
        # Uvicorn registers request tasks before tracked_app starts running.
        requests = getattr(server.server_state, "tasks", ())
        return {owned for owned in {*app_tasks, *requests, task} if not owned.done()}

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    server.should_exit = True
    timed_out = False
    # Reserve part of the deadline for cooperative cancellation and its cleanup.
    grace = timeout * 0.8
    grace_deadline = loop.time() + grace
    try:
        done, pending = await asyncio.wait({task}, timeout=grace)
        if pending:
            timed_out = True
        # Serving has ended or exhausted its grace period. Close admission
        # before draining application work, including startup failure paths.
        server.force_exit = True
        close_connections()
        owned = owned_tasks()
        if owned and not timed_out:
            _, owned = await asyncio.wait(owned, timeout=max(0, grace_deadline - loop.time()))
            timed_out = bool(owned)
        if owned:
            owned = owned_tasks()
            for owned_task in owned:
                owned_task.cancel()
            completed, _ = await asyncio.wait(owned, timeout=max(0, deadline - loop.time()))
            for completed_task in completed:
                if not completed_task.cancelled():
                    error = completed_task.exception()
                    if error is not None and error not in task_errors:
                        task_errors.append(error)
        server_error = task.exception() if done and not task.cancelled() else None
        if server_error is not None:
            error = ServerError("server failed while running or shutting down")
            for secondary in task_errors:
                add_exception_notes(error, "Owned task cleanup failure", secondary)
            raise error from server_error
        if timed_out:
            error = ServerError(f"server shutdown timed out after {timeout:g}s")
            for secondary in task_errors:
                add_exception_notes(error, "Owned task cleanup failure", secondary)
            raise error
        if task_errors:
            error = ServerError(f"owned task cleanup failed: {task_errors[0]!r}")
            for secondary in task_errors[1:]:
                add_exception_notes(error, "Additional owned task failure", secondary)
            raise error from task_errors[0]
    finally:
        # Uvicorn's asyncio.Server normally owns the supplied socket. If its
        # serve task was cancelled mid-shutdown, close its listener explicitly
        # before closing the original socket, and never touch other servers.
        close_connections()


@asynccontextmanager
async def live_server(
    app: ASGIApp,
    *,
    startup_timeout: float = 5.0,
    shutdown_timeout: float = 5.0,
    lifespan: Lifespan = "on",
) -> AsyncIterator[ServerHandle]:
    """Serve ``app`` on a reserved 127.0.0.1 port within this event loop.

    Startup and shutdown waits are bounded. Cancellation-resistant application
    code can outlive those waits; process isolation is required for a hard kill.
    """
    startup_timeout = _timeout("startup_timeout", startup_timeout)
    shutdown_timeout = _timeout("shutdown_timeout", shutdown_timeout)
    if lifespan not in ("on", "off", "auto"):
        raise ValueError("lifespan must be 'on', 'off', or 'auto'")
    app_tasks: set[asyncio.Task[Any]] = set()
    task_errors: list[BaseException] = []
    lifespan_started = False
    startup_succeeded = False
    startup_failure: BaseException | str | None = None
    startup_evidence: list[BaseException | str] = []
    reported_startup_failure: BaseException | str | None = None
    lifespan_failure: BaseException | str | None = None

    async def tracked_app(scope, receive, send):
        nonlocal lifespan_started, lifespan_failure, startup_failure
        current = asyncio.current_task()
        if current is not None:
            app_tasks.add(current)

            def task_done(done):
                app_tasks.discard(done)
                if not done.cancelled():
                    error = done.exception()
                    if error is not None and error not in task_errors:
                        task_errors.append(error)

            current.add_done_callback(task_done)
        if scope["type"] != "lifespan":
            if server is not None and server.force_exit:
                # A previously accepted connection may have callbacks queued
                # when its listener closes. Do not enter user code afterward.
                await send({"type": "http.response.start", "status": 503, "headers": []})
                await send({"type": "http.response.body", "body": b""})
                return
            try:
                await app(scope, receive, send)
            except BaseException as exc:
                if (
                    current is not None
                    and current.cancelling()
                    and not isinstance(exc, asyncio.CancelledError)
                ):
                    task_errors.append(exc)
                raise
            return

        async def tracked_send(message):
            nonlocal lifespan_started, lifespan_failure, startup_failure
            if message["type"] == "lifespan.startup.complete":
                lifespan_started = True
            elif message["type"] == "lifespan.startup.failed":
                startup_failure = message.get("message") or "application reported startup failure"
                startup_evidence.append(startup_failure)
            elif message["type"] == "lifespan.shutdown.failed":
                lifespan_failure = message.get("message") or "application reported shutdown failure"
            await send(message)

        try:
            await app(scope, receive, tracked_send)
        except BaseException as exc:
            plain_cancellation = (
                type(exc) is asyncio.CancelledError
                and not exc.args
                and exc.__cause__ is None
                and exc.__context__ is None
                and not getattr(exc, "__notes__", ())
            )
            if not plain_cancellation:
                if lifespan_started:
                    lifespan_failure = exc
                else:
                    startup_evidence.append(exc)
                    if startup_failure is None:
                        startup_failure = exc
            raise

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    task: asyncio.Task[None] | None = None
    server: _Server | None = None
    failure: BaseException | None = None
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen(socket.SOMAXCONN)
        sock.setblocking(False)
        port = sock.getsockname()[1]
        config = _Config(
            tracked_app,
            lifespan=lifespan,
            log_config=None,
            access_log=False,
            http="h11",
            ws="none",
            # Uvicorn also polls in main_loop and sleeps before waiting for
            # active connections. Reserve fixed time for those phases and
            # scheduler jitter, in addition to our cancellation reserve.
            timeout_graceful_shutdown=min(
                shutdown_timeout * 0.4, max(0.0, shutdown_timeout * 0.8 - 0.3)
            ),
        )
        server = _Server(config)

        async def serve() -> None:
            try:
                await server.serve(sockets=[sock])
            except SystemExit as exc:
                # Recent Uvicorn versions call sys.exit on failed startup.
                # A BaseException escaping a task stops the host event loop.
                raise ServerError("Uvicorn exited during startup") from exc

        task = asyncio.create_task(serve(), name="stream-lifecycle-server")
        try:
            await _wait_until_started(server, task, startup_timeout)
        except ServerError:
            if startup_failure is not None:
                reported_startup_failure = startup_failure
                detail = f"application lifespan startup failed: {startup_failure}"
                if isinstance(startup_failure, BaseException):
                    raise ServerError(detail) from startup_failure
                raise ServerError(detail) from None
            raise
        startup_succeeded = True
        yield ServerHandle(url=f"http://127.0.0.1:{port}")
    except BaseException as exc:
        failure = exc
        raise
    finally:
        if task is not None and server is not None:

            async def cleanup() -> None:
                cleanup_error: BaseException | None = None
                try:
                    await _stop(server, task, sock, app_tasks, shutdown_timeout, task_errors)
                except Exception as exc:  # noqa: BLE001 - preserve cleanup failure
                    cleanup_error = exc
                # Collect after _stop even when it fails: finalizers can record
                # additional startup messages or exceptions during cancellation.
                # Ignore unsupported-lifespan evidence only after successful
                # entry, not merely because Uvicorn starts during cleanup.
                failures = []
                if not startup_succeeded:
                    failures.extend(
                        ("application lifespan startup failed", evidence)
                        for evidence in startup_evidence
                        if evidence is not reported_startup_failure
                    )
                if lifespan_failure is not None:
                    failures.append(("application lifespan failed after startup", lifespan_failure))
                for label, evidence in failures:
                    detail = f"{label}: {evidence}"
                    if cleanup_error is not None:
                        if isinstance(evidence, BaseException):
                            add_exception_notes(cleanup_error, detail, evidence)
                        else:
                            cleanup_error.add_note(detail)
                    else:
                        cleanup_error = ServerError(detail)
                        if isinstance(evidence, BaseException):
                            cleanup_error.__cause__ = evidence
                if cleanup_error is not None:
                    raise cleanup_error

            cleanup_task = asyncio.create_task(cleanup(), name="stream-lifecycle-cleanup")
            interrupted: asyncio.CancelledError | None = None
            while not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError as exc:
                    if interrupted is None:
                        interrupted = exc
                except Exception:  # noqa: BLE001 - retrieve the cleanup task error below
                    break
            cleanup_error = None if cleanup_task.cancelled() else cleanup_task.exception()
            primary = failure if failure is not None else interrupted
            if cleanup_error is not None:
                if primary is None:
                    raise cleanup_error
                add_exception_notes(primary, "Server cleanup failure", cleanup_error)
            if failure is None and interrupted is not None:
                raise interrupted
        else:
            sock.close()
