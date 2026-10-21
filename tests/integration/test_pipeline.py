"""Run the service inside a real Pipecat pipeline against the mock RTZR server."""

from pipecat.frames.frames import (
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.tests.utils import SleepFrame, run_test

from pipecat_rtzr import RTZRSTTService


async def test_pipeline_emits_interim_then_final(mock_rtzr):
    stt = RTZRSTTService(
        client_id="id", client_secret="good-secret", api_base=mock_rtzr.api_base, sample_rate=16000
    )
    audio = InputAudioRawFrame(audio=b"\x00\x00" * 320, sample_rate=16000, num_channels=1)

    down, _ = await run_test(
        stt,
        frames_to_send=[
            audio,
            audio,
            SleepFrame(sleep=0.3),
            VADUserStoppedSpeakingFrame(stop_secs=0.2),
            SleepFrame(sleep=0.5),
        ],
    )

    kinds = [type(f) for f in down]
    assert InterimTranscriptionFrame in kinds
    finals = [f for f in down if isinstance(f, TranscriptionFrame)]
    assert [f.text for f in finals] == ["안녕하세요"]
    assert mock_rtzr.audio_bytes >= 2 * 640
    assert '{"type": "Finalize"}' in mock_rtzr.text_messages
    # run_test ends the pipeline with an EndFrame, which sends EOS before closing.
    assert "EOS" in mock_rtzr.text_messages
