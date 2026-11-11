"""Explicit observations of application-owned resource lifetimes."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from types import TracebackType
from typing import Self


class LifecycleError(AssertionError):
    """An observed lifecycle or streaming expectation was not met."""


def _positive_timeout(timeout: float) -> float:
    import math

    try:
        value = float(timeout)
    except (TypeError, ValueError) as exc:
        raise ValueError("timeout must be a positive finite number") from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError("timeout must be a positive finite number")
    return value


class _Observation:
    def __init__(self, probe: LifecycleProbe, name: str):
        self._probe = probe
        self.name = name
        self.active = False
        self.acknowledged = False
        self.failure: BaseException | None = None
        self._entered = False

    def __enter__(self) -> Self:
        self._probe._check_loop()
        if self._entered:
            raise RuntimeError("an observation cannot be entered more than once")
        self._entered = self.active = True
        self._probe._entries[self.name].append(self)
        self._probe._changed()
        return self

    def mark_closed(self) -> None:
        """Acknowledge that this instance's awaited cleanup has completed."""
        self._probe._check_loop()
        if not self.active:
            raise RuntimeError("mark_closed() requires an active observation")
        self.acknowledged = True
        self._probe._changed()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._probe._check_loop()
        self.active = False
        if exc is not None and not isinstance(exc, (asyncio.CancelledError, GeneratorExit)):
            self.failure = exc
        self._probe._changed()


class LifecycleProbe:
    """Record explicitly instrumented resource instances on one asyncio loop.

    A cleanup acknowledgement is supplied by the application, not proof of
    remote-provider cancellation. The probe never cancels application tasks.
    """

    def __init__(self) -> None:
        self._entries: dict[str, list[_Observation]] = defaultdict(list)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event: asyncio.Event | None = None

    def _check_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
            self._event = asyncio.Event()
        elif self._loop is not loop:
            raise RuntimeError("LifecycleProbe cannot be reused across asyncio event loops")

    def _changed(self) -> None:
        assert self._event is not None
        self._event.set()
        self._event = asyncio.Event()

    def track(self, name: str) -> _Observation:
        if not isinstance(name, str) or not name:
            raise ValueError("resource name must be a nonempty string")
        return _Observation(self, name)

    @property
    def active_labels(self) -> frozenset[str]:
        self._check_loop()
        return frozenset(
            name for name, entries in self._entries.items() if any(e.active for e in entries)
        )

    def started_count(self, name: str) -> int:
        self._check_loop()
        return len(self._entries.get(name, ()))

    def assert_active(self, name: str) -> None:
        self._check_loop()
        if name not in self.active_labels:
            state = "not observed" if not self.started_count(name) else "not active"
            raise LifecycleError(f"resource {name!r} is {state}")

    async def wait_started(self, name: str, *, timeout: float = 2.0) -> None:
        self._check_loop()
        deadline = asyncio.get_running_loop().time() + _positive_timeout(timeout)
        while not self.started_count(name):
            await self._wait_change(deadline, f"resource {name!r} was not observed")

    async def _wait_change(self, deadline: float, diagnostic: str) -> None:
        assert self._event is not None
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise LifecycleError(f"timed out: {diagnostic}")
        try:
            await asyncio.wait_for(self._event.wait(), timeout=remaining)
        except TimeoutError as exc:
            raise LifecycleError(f"timed out: {diagnostic}") from exc

    async def wait_closed(self, *names: str, timeout: float = 2.0) -> None:
        self._check_loop()
        if not names:
            raise LifecycleError("wait_closed requires at least one resource name")
        deadline = asyncio.get_running_loop().time() + _positive_timeout(timeout)
        while True:
            unknown = [name for name in names if not self._entries.get(name)]
            active = [name for name in names if any(e.active for e in self._entries.get(name, ()))]
            failed = [e for name in names for e in self._entries.get(name, ()) if e.failure]
            unacknowledged = [
                name
                for name in names
                if any(not e.active and not e.acknowledged for e in self._entries.get(name, ()))
            ]
            if failed:
                raise LifecycleError(
                    f"resource {failed[0].name!r} exited with error: {failed[0].failure!r}"
                ) from failed[0].failure
            if unacknowledged:
                raise LifecycleError(
                    f"resource cleanup unacknowledged: {', '.join(unacknowledged)}"
                )
            if not unknown and not active:
                return
            await self._wait_change(deadline, f"unknown: {unknown}; active: {active}")
