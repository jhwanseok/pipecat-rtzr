"""End-to-end tests against the in-process mock RTZR server (no credentials needed)."""

import asyncio

import pytest
from pipecat.frames.frames import InterimTranscriptionFrame, TranscriptionFrame

from pipecat_rtzr import RTZRAuth, RTZRAuthError, RTZRSTTService


def make_service(mock, secret="good-secret") -> RTZRSTTService:
    svc = RTZRSTTService(
        client_id="id", client_secret=secret, api_base=mock.api_base, sample_rate=16000
    )
    svc._sample_rate = 16000
    return svc


async def wait_for(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


@pytest.fixture
def collect(monkeypatch):
    def attach(svc):
        frames, errors = [], []

        async def push_frame(frame, direction=None):
            frames.append(frame)

        async def push_error(error_msg, **kwargs):
            errors.append(error_msg)

        async def noop(*a, **k):
            return None

        monkeypatch.setattr(svc, "push_frame", push_frame)
        monkeypatch.setattr(svc, "push_error", push_error)
        monkeypatch.setattr(svc, "emit_stt_usage_metrics", noop)
        return frames, errors

    return attach


async def test_auth_token_is_cached_and_refreshed(mock_rtzr):
    auth = RTZRAuth("id", "good-secret", api_base=mock_rtzr.api_base)
    assert await auth.get_token() == "token-1"
    assert await auth.get_token() == "token-1"
    assert len(mock_rtzr.auth_requests) == 1
    assert mock_rtzr.auth_requests[0] == {"client_id": "id", "client_secret": "good-secret"}

    # A token that expires within the refresh margin is replaced.
    mock_rtzr.token_lifetime_s = 3600
    auth._expire_at = 0
    assert await auth.get_token() == "token-2"


async def test_auth_failure_raises(mock_rtzr):
    auth = RTZRAuth("id", "wrong", api_base=mock_rtzr.api_base)
    with pytest.raises(RTZRAuthError) as err:
        await auth.get_token()
    assert err.value.status == 401


async def test_stream_audio_interim_and_final(mock_rtzr, collect):
    svc = make_service(mock_rtzr)
    frames, errors = collect(svc)

    await svc._connect_websocket()
    assert svc._websocket is not None and not errors
    receiver = asyncio.create_task(svc._receive_messages())

    hs = mock_rtzr.handshakes[0]
    assert hs["authorization"] == "Bearer token-1"
    assert hs["query"]["sample_rate"] == "16000"
    assert hs["query"]["model_name"] == "sommers_ko"

    for _ in range(4):
        async for _ in svc.run_stt(b"\x00\x00" * 160):
            pass
    await wait_for(lambda: any(isinstance(f, InterimTranscriptionFrame) for f in frames))
    assert mock_rtzr.audio_bytes == 4 * 320

    svc.request_finalize()
    await svc._websocket.send('{"type": "Finalize"}')
    await wait_for(lambda: any(isinstance(f, TranscriptionFrame) for f in frames))
    final = next(f for f in frames if isinstance(f, TranscriptionFrame))
    assert final.text == "안녕하세요"

    await svc._send_eos()
    await asyncio.wait_for(receiver, 3)
    assert "EOS" in mock_rtzr.text_messages
    await svc._disconnect_websocket()


async def test_handshake_401_refreshes_token_and_retries(mock_rtzr, collect):
    mock_rtzr.reject_first_n_handshakes = 1
    svc = make_service(mock_rtzr)
    _, errors = collect(svc)

    await svc._connect_websocket()

    assert svc._websocket is not None and not errors
    assert len(mock_rtzr.handshakes) == 2
    assert mock_rtzr.tokens_issued == 2
    assert mock_rtzr.handshakes[1]["authorization"] == "Bearer token-2"
    await svc._disconnect_websocket()


@pytest.mark.parametrize("status,expected", [(429, "A0001"), (400, "H0001"), (500, "E500")])
async def test_handshake_errors_are_reported(mock_rtzr, collect, status, expected):
    mock_rtzr.reject_handshake_status = status
    svc = make_service(mock_rtzr)
    _, errors = collect(svc)

    await svc._connect_websocket()

    assert svc._websocket is None
    assert len(errors) == 1 and expected in errors[0]


async def test_bad_credentials_are_reported(mock_rtzr, collect):
    svc = make_service(mock_rtzr, secret="wrong")
    _, errors = collect(svc)

    await svc._connect_websocket()

    assert svc._websocket is None
    assert len(errors) == 1 and "authentication failed" in errors[0]


async def test_unsupported_sample_rate_is_reported(mock_rtzr, collect):
    svc = make_service(mock_rtzr)
    svc._sample_rate = 4000
    _, errors = collect(svc)

    await svc._connect_websocket()

    assert svc._websocket is None
    assert errors and "8-48 kHz" in errors[0]
    assert not mock_rtzr.handshakes
