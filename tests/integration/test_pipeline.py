"""Run the service inside a real Pipecat pipeline against the mock RTZR server."""

import asyncio

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


async def test_eos_flushes_open_utterance(mock_rtzr):
    """Without a VAD stop, the final for an open utterance still arrives after EOS."""
    stt = RTZRSTTService(
        client_id="id", client_secret="good-secret", api_base=mock_rtzr.api_base, sample_rate=16000
    )
    audio = InputAudioRawFrame(audio=b"\x00\x00" * 320, sample_rate=16000, num_channels=1)

    down, _ = await run_test(stt, frames_to_send=[audio, audio, SleepFrame(sleep=0.3)])

    finals = [f for f in down if isinstance(f, TranscriptionFrame)]
    assert [f.text for f in finals] == ["안녕하세요"]
    assert '{"type": "Finalize"}' not in mock_rtzr.text_messages


async def test_eos_does_not_wait_without_open_utterance(mock_rtzr):
    mock_rtzr.interim_every_n_chunks = 10_000  # never produces an interim
    stt = RTZRSTTService(
        client_id="id",
        client_secret="good-secret",
        api_base=mock_rtzr.api_base,
        sample_rate=16000,
        eos_flush_timeout=30,
    )
    audio = InputAudioRawFrame(audio=b"\x00\x00" * 320, sample_rate=16000, num_channels=1)

    # Would hang for 30 s if the service waited although nothing was open.
    await asyncio.wait_for(run_test(stt, frames_to_send=[audio, SleepFrame(sleep=0.2)]), 10)
