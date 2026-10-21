from urllib.parse import parse_qs, urlparse

import pytest
from pipecat.frames.frames import InterimTranscriptionFrame, TranscriptionFrame
from pipecat.transcriptions.language import Language

from pipecat_rtzr import RTZRSTTService
from pipecat_rtzr.stt import format_keywords, language_to_rtzr_language


def make_service(**kwargs) -> RTZRSTTService:
    svc = RTZRSTTService(client_id="id", client_secret="secret", sample_rate=8000, **kwargs)
    svc._sample_rate = 8000
    return svc


def query_of(svc: RTZRSTTService) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlparse(svc._stream_url()).query).items()}


def test_format_keywords_plain_and_boosted():
    assert (
        format_keywords(["리턴제로", ("부스팅", 3.5), ("제외", -1)])
        == "리턴제로,부스팅:3.5,제외:-1.0"
    )


@pytest.mark.parametrize(
    "bad",
    [[""], ["가" * 21], [("단어", 5.1)], [("단어", -5.1)], ["단어"] * 101],
)
def test_format_keywords_rejects_invalid(bad):
    with pytest.raises(ValueError):
        format_keywords(bad)


def test_language_mapping():
    assert language_to_rtzr_language(Language.KO) == "ko"
    assert language_to_rtzr_language(Language.FR) is None


def test_requires_credentials(monkeypatch):
    monkeypatch.delenv("RTZR_CLIENT_ID", raising=False)
    monkeypatch.delenv("RTZR_CLIENT_SECRET", raising=False)
    with pytest.raises(ValueError):
        RTZRSTTService()


def test_credentials_from_env(monkeypatch):
    monkeypatch.setenv("RTZR_CLIENT_ID", "a")
    monkeypatch.setenv("RTZR_CLIENT_SECRET", "b")
    RTZRSTTService()


def test_default_stream_url():
    svc = make_service()
    parsed = urlparse(svc._stream_url())
    assert parsed.scheme == "wss"
    assert parsed.netloc == "openapi.vito.ai"
    assert parsed.path == "/v1/transcribe:streaming"
    assert query_of(svc) == {
        "model_name": "sommers_ko",
        "sample_rate": "8000",
        "encoding": "LINEAR16",
        "domain": "CALL",
        "use_itn": "true",
        "use_disfluency_filter": "false",
        "use_profanity_filter": "false",
        "use_punctuation": "false",
    }


def test_stream_url_with_options():
    svc = make_service(
        settings=RTZRSTTService.Settings(
            keywords=[("리턴제로", 3.0)], epd_time=0.5, use_punctuation=True, domain="MEETING"
        )
    )
    q = query_of(svc)
    assert q["keywords"] == "리턴제로:3.0"
    assert q["epd_time"] == "0.5"
    assert q["use_punctuation"] == "true"
    assert q["domain"] == "MEETING"
    assert "language" not in q


def test_language_only_sent_for_whisper():
    svc = make_service(settings=RTZRSTTService.Settings(model="whisper", language=Language.EN))
    assert query_of(svc)["language"] == "en"


async def test_handle_message_interim_and_final(monkeypatch):
    svc = make_service()
    pushed = []

    async def capture(frame, direction=None):
        pushed.append(frame)

    async def noop(*a, **k):
        return None

    monkeypatch.setattr(svc, "push_frame", capture)
    monkeypatch.setattr(svc, "emit_stt_usage_metrics", noop)

    await svc._handle_message({"final": False, "alternatives": [{"text": "안녕"}]})
    await svc._handle_message({"final": True, "alternatives": [{"text": " 안녕하세요 "}]})
    await svc._handle_message({"final": True, "alternatives": [{"text": ""}]})
    await svc._handle_message({"final": True, "alternatives": []})

    assert [type(f) for f in pushed] == [InterimTranscriptionFrame, TranscriptionFrame]
    assert pushed[0].text == "안녕"
    assert pushed[1].text == "안녕하세요"
    assert pushed[1].language == Language.KO


async def test_handle_message_error(monkeypatch):
    svc = make_service()
    errors = []

    async def capture(error_msg, **kwargs):
        errors.append(error_msg)

    monkeypatch.setattr(svc, "push_error", capture)
    await svc._handle_message({"error": "boom"})
    assert errors == ["RTZR error: boom"]
