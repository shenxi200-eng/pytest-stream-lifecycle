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

    async def next(self, *, timeout: float | None = None) -> Any:
        if self._closed:
            raise LifecycleError(f"{self._label} stream is closed")
        if self._reading:
            raise LifecycleError(f"{self._label} stream already has a pending read")
        duration = self._timeout if timeout is None else _positive_timeout(timeout)
        self._reading = True
        try:
            try:
                return await asyncio.wait_for(anext(self._iterator), timeout=duration)
            except TimeoutError as exc:
                error = LifecycleError(f"{self._label} stream timed out after {duration:g}s")
                await self._close_with_primary(error)
                raise error from exc
            except StopAsyncIteration as exc:
                error = LifecycleError(f"{self._label} stream ended unexpectedly (EOF)")
                await self._close_with_primary(error)
                raise error from exc
            except asyncio.CancelledError as exc:
                await self._close_with_primary(exc)
                raise
            except Exception as exc:
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

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await _close_steps(
            self._iterator.aclose,
            partial(self._response_context.__aexit__, None, None, None),
            self._client.aclose,
        )


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
                media_type = (
                    response.headers.get("content-type", "").partition(";")[0].strip().lower()
                )
                if media_type != "text/event-stream":
                    raise LifecycleError(f"SSE stream has invalid content-type: {media_type!r}")
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
