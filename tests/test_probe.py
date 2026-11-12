import asyncio

import pytest

from pytest_stream_lifecycle import LifecycleError, LifecycleProbe


def run(coro):
    return asyncio.run(coro)


def test_fixture_observes_repeated_instances(stream_probe):
    async def scenario():
        assert stream_probe.active_labels == frozenset()
        with stream_probe.track("producer") as first, stream_probe.track("producer") as second:
            assert stream_probe.started_count("producer") == 2
            assert stream_probe.active_labels == frozenset({"producer"})
            stream_probe.assert_active("producer")
            first.mark_closed()
            second.mark_closed()
        await stream_probe.wait_closed("producer")
        with pytest.raises(LifecycleError, match="not active"):
            stream_probe.assert_active("producer")

    run(scenario())


def test_unknown_unacknowledged_and_error_states():
    async def scenario():
        probe = LifecycleProbe()
        with pytest.raises(LifecycleError, match="not observed"):
            probe.assert_active("missing")
        with pytest.raises(LifecycleError, match="not observed"):
            await probe.wait_started("missing", timeout=0.01)
        with pytest.raises(LifecycleError, match="unknown"):
            await probe.wait_closed("missing", timeout=0.01)
        with pytest.raises(LifecycleError, match="at least one"):
            await probe.wait_closed()
        with probe.track("unacknowledged"):
            pass
        with pytest.raises(LifecycleError, match="unacknowledged"):
            await probe.wait_closed("unacknowledged")
        with pytest.raises(ValueError, match="broken"), probe.track("failed") as observed:
            observed.mark_closed()
            raise ValueError("broken")
        with pytest.raises(LifecycleError, match="broken") as caught:
            await probe.wait_closed("failed")
        assert isinstance(caught.value.__cause__, ValueError)

    run(scenario())


def test_timeout_and_waiter_cancellation_never_cancel_producer():
    async def scenario():
        probe = LifecycleProbe()
        release = asyncio.Event()

        async def producer():
            with probe.track("producer") as observed:
                try:
                    await release.wait()
                finally:
                    observed.mark_closed()

        task = asyncio.create_task(producer())
        await probe.wait_started("producer")
        with pytest.raises(LifecycleError, match="active"):
            await probe.wait_closed("producer", timeout=0.01)
        waiter = asyncio.create_task(probe.wait_closed("producer"))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not task.done()
        release.set()
        await task
        await probe.wait_closed("producer")

    run(scenario())


def test_interrupted_cleanup_without_acknowledgement():
    async def scenario():
        probe = LifecycleProbe()
        cleanup_started = asyncio.Event()

        async def producer():
            with probe.track("producer") as observed:
                try:
                    await asyncio.Event().wait()
                finally:
                    cleanup_started.set()
                    await asyncio.Event().wait()
                    observed.mark_closed()

        task = asyncio.create_task(producer())
        await probe.wait_started("producer")
        task.cancel()
        await cleanup_started.wait()
        probe.assert_active("producer")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(LifecycleError, match="unacknowledged"):
            await probe.wait_closed("producer")

    run(scenario())


def test_failed_awaited_finalizer_is_error_not_acknowledgement():
    async def scenario():
        probe = LifecycleProbe()

        async def fail_cleanup():
            await asyncio.sleep(0)
            raise RuntimeError("cleanup exploded")

        with (
            pytest.raises(RuntimeError, match="cleanup exploded"),
            probe.track("resource") as observed,
        ):
            try:
                pass
            finally:
                await fail_cleanup()
                observed.mark_closed()
        with pytest.raises(LifecycleError, match="cleanup exploded"):
            await probe.wait_closed("resource")

    run(scenario())


def test_cross_loop_usage_fails_clearly():
    probe = LifecycleProbe()

    async def use():
        return probe.active_labels

    run(use())
    with pytest.raises(RuntimeError, match="across asyncio event loops"):
        run(use())


def test_generator_exit_with_acknowledged_cleanup():
    async def scenario():
        probe = LifecycleProbe()

        async def producer():
            with probe.track("producer") as observed:
                try:
                    yield "first"
                    yield "second"
                finally:
                    await asyncio.sleep(0)
                    observed.mark_closed()

        generator = producer()
        assert await anext(generator) == "first"
        probe.assert_active("producer")
        await generator.aclose()
        await probe.wait_closed("producer")

    run(scenario())


def test_falsy_failure_is_retained_after_cleanup_acknowledgement():
    class FalsyFailure(RuntimeError):
        def __bool__(self):
            return False

    async def scenario():
        probe = LifecycleProbe()
        failure = FalsyFailure("acknowledged but failed")
        with pytest.raises(FalsyFailure), probe.track("resource") as observed:
            observed.mark_closed()
            raise failure
        with pytest.raises(LifecycleError, match="acknowledged but failed") as caught:
            await probe.wait_closed("resource")
        assert caught.value.__cause__ is failure

    run(scenario())
