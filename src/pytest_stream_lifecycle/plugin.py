"""Pytest fixture registration."""

import pytest

from .server import live_server


@pytest.fixture
def stream_server():
    """Return a factory for async context managers serving ASGI applications."""
    return live_server
