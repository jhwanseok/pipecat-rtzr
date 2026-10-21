import pytest
from aiohttp.test_utils import TestServer

from .mock_rtzr import MockRTZR


@pytest.fixture
async def mock_rtzr():
    mock = MockRTZR()
    server = TestServer(mock.app())
    await server.start_server()
    mock.api_base = f"http://127.0.0.1:{server.port}"
    yield mock
    await server.close()
