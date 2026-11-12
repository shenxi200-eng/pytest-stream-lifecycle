"""Bounded real-network line and server-sent-event readers."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from typing import Any

import httpx
from httpx_sse import EventSource

from ._errors import add_exception_notes
from .probe import LifecycleError, _positive_timeout


async def _close_steps(*steps) -> None:
    error: BaseException | None = None
    for close in steps:
        try:
            await close()
        except BaseException as exc:  # noqa: BLE001 - attempt every owned cleanup step
            if error is None:
                error = exc
            else:
                add_exception_notes(error, "Additional stream cleanup failure", exc)
    if error is not None:
        raise error


class Stream:
    """An open response; closing it does not assert application cleanup."""

    def __init__(self, client, response_context, response, iterator, timeout: float, label: str):
        self._client = client
        self._response_context = response_context
        self._response = response
        self._iterator = iterator
        self._timeout = timeout
        self._label = label
        self._closed = False
        self._reading = False
        self._read_task: asyncio.Task[tuple[bool, Any]] | None = None
        self._read_error: BaseException | None = None
        self._read_cancel_token = object()
        self._close_task: asyncio.Task[BaseException | None] | None = None

    async def next(self, *, timeout: float | None = None) -> Any:
        if self._closed:
            raise LifecycleError(f"{self._label} stream is closed")
        if self._reading:
            raise LifecycleError(f"{self._label} stream already has a pending read")
        duration = self._timeout if timeout is None else _positive_timeout(timeout)
        self._reading = True
        self._read_error = None

        async def read() -> tuple[bool, Any]:
            # __anext__ may return any awaitable, not necessarily a coroutine.
            try:
                return True, await anext(self._iterator)
            except BaseException as exc:  # noqa: BLE001 - retain iterator cancellation unchanged
                # Shield would otherwise replace CancelledError with a blank
                # surrogate. Keep the original outcome for reader and cleanup.
                return False, exc

        self._read_task = asyncio.create_task(read(), name="stream-lifecycle-read")
        try:
            try:
                async with asyncio.timeout(duration):
                    succeeded, result = await asyncio.shield(self._read_task)
                if not succeeded:
                    if self._intentional_read_cancel(result):
                        raise asyncio.CancelledError()
                    self._read_error = result
                    raise result
                return result
            except TimeoutError as exc:
                self._read_error = exc
                error = LifecycleError(f"{self._label} stream timed out after {duration:g}s")
                await self._close_with_primary(error)
                raise error from exc
            except StopAsyncIteration as exc:
                self._read_error = exc
                error = LifecycleError(f"{self._label} stream ended unexpectedly (EOF)")
                await self._close_with_primary(error)
                raise error from exc
            except asyncio.CancelledError as exc:
                await self._close_with_primary(exc)
                raise
            except Exception as exc:
                self._read_error = exc
                error = LifecycleError(f"{self._label} stream read failed: {exc}")
                await self._close_with_primary(error)
                raise error from exc
        finally:
            self._reading = False

    async def _close_with_primary(self, primary: BaseException) -> None:
        try:
            await self.aclose()
        except BaseException as exc:  # noqa: BLE001 - preserve the primary failure
            add_exception_notes(primary, "Stream cleanup failure", exc)

    async def _finish_read(self) -> None:
        read_task = self._read_task
        reported_error = self._read_error
        if read_task is None:
            return
        if not read_task.done():
            read_task.cancel(self._read_cancel_token)
        try:
            succeeded, result = await read_task
        except asyncio.CancelledError as exc:
            # A task cancelled before its wrapper first runs has no outcome.
            if not self._intentional_read_cancel(exc):
                raise
        else:
            if (
                not succeeded
                and result is not reported_error
                and not self._intentional_read_cancel(result)
            ):
                raise result

    def _intentional_read_cancel(self, error: BaseException) -> bool:
        # Ignore only our unmodified cancellation. A finalizer can replace it
        # or attach evidence to it; either case must remain a reported failure.
        return (
            type(error) is asyncio.CancelledError
            and len(error.args) == 1
            and error.args[0] is self._read_cancel_token
            and error.__cause__ is None
            and error.__context__ is None
            and not getattr(error, "__notes__", ())
        )

    async def _cleanup(self) -> BaseException | None:
        try:
            await _close_steps(
                self._finish_read,
                self._iterator.aclose,
                partial(self._response_context.__aexit__, None, None, None),
                self._client.aclose,
            )
        except BaseException as exc:  # noqa: BLE001 - share even cleanup cancellation unchanged
            # A cancelled Task loses its original CancelledError after result()
            # retrieves it once. Return the outcome so every closer retains it.
            return exc
        return None

    async def aclose(self) -> None:
        current = asyncio.current_task()
        if current is self._read_task:
            raise LifecycleError(
                f"{self._label} stream close must run outside its iterator operation"
            )
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._cleanup(), name="stream-lifecycle-close")
        if current is self._close_task:
            return  # A cleanup step may re-enter close; never await ourselves.

        interrupted: asyncio.CancelledError | None = None
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError as exc:
                if interrupted is None:
                    interrupted = exc
        cleanup_error = self._close_task.result()
        if cleanup_error is not None:
            if interrupted is None:
                raise cleanup_error
            add_exception_notes(interrupted, "Stream cleanup failure", cleanup_error)
        if interrupted is not None:
            raise interrupted


@asynccontextmanager
async def open_stream(
    base_url: str,
    path: str,
    *,
    kind: str,
    method: str = "GET",
    timeout: float = 2.0,
    **request_kwargs: Any,
) -> AsyncIterator[Stream]:
    duration = _positive_timeout(timeout)
    if not isinstance(path, str) or not path.startswith("/") or path.startswith("//"):
        raise ValueError("path must be a relative path starting with a single '/'")
    if kind == "sse":
        headers = httpx.Headers(request_kwargs.pop("headers", {}))
        headers["Accept"] = "text/event-stream"
        headers["Cache-Control"] = "no-store"
        request_kwargs["headers"] = headers
    client = httpx.AsyncClient(trust_env=False, timeout=None)
    response_context = client.stream(method, base_url + path, **request_kwargs)
    response = None
    stream = None
    failure: BaseException | None = None
    try:
        try:
            response = await asyncio.wait_for(response_context.__aenter__(), timeout=duration)
            response.raise_for_status()
            if kind == "sse":
                token, separator, parameters = response.headers.get("content-type", "").partition(
                    ";"
                )
                media_type = token.strip().lower()
                if media_type != "text/event-stream":
                    raise LifecycleError(f"SSE stream has invalid content-type: {media_type!r}")
                response.headers["content-type"] = media_type + separator + parameters
                iterator = EventSource(response).aiter_sse()
            else:
                iterator = response.aiter_lines()
            stream = Stream(
                client, response_context, response, iterator, duration, f"{kind.upper()} {path}"
            )
        except TimeoutError as exc:
            raise LifecycleError(
                f"{kind.upper()} {path} connection timed out after {duration:g}s"
            ) from exc
        except httpx.HTTPError as exc:
            raise LifecycleError(f"{kind.upper()} {path} request failed: {exc}") from exc
        yield stream
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            if stream is not None:
                await stream.aclose()
            else:
                steps = []
                if response is not None:
                    steps.append(partial(response_context.__aexit__, None, None, None))
                steps.append(client.aclose)
                await _close_steps(*steps)
        except BaseException as exc:
            if failure is None:
                raise
            add_exception_notes(failure, "Stream cleanup failure", exc)
