"""Pytest fixture registration."""

import pytest

from .probe import LifecycleProbe
from .server import live_server


@pytest.fixture
def stream_server():
    """Return a factory for async context managers serving ASGI applications."""
    return live_server


@pytest.fixture
def stream_probe():
    """Return a new explicit application-lifecycle probe."""
    return LifecycleProbe()
