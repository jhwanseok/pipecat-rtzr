"""Run RTZRSTTService end to end against a local mock of the RTZR API (no credentials).

python example_mock.py
"""

import asyncio

from aiohttp.test_utils import TestServer
from pipecat.frames.frames import (
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.tests.utils import SleepFrame, run_test
from tests.mock_rtzr import MockRTZR

from pipecat_rtzr import RTZRSTTService


async def main() -> None:
    mock = MockRTZR()
    server = TestServer(mock.app())
    await server.start_server()
    try:
        stt = RTZRSTTService(
            client_id="demo",
            client_secret="good-secret",
            api_base=f"http://127.0.0.1:{server.port}",
            sample_rate=16000,
        )
        chunk = InputAudioRawFrame(audio=b"\x00\x00" * 320, sample_rate=16000, num_channels=1)
        frames, _ = await run_test(
            stt,
            frames_to_send=[
                chunk,
                chunk,
                SleepFrame(sleep=0.3),
                VADUserStoppedSpeakingFrame(stop_secs=0.2),
                SleepFrame(sleep=0.5),
            ],
        )
        for frame in frames:
            if isinstance(frame, InterimTranscriptionFrame):
                print("interim:", frame.text)
            elif isinstance(frame, TranscriptionFrame):
                print("final:  ", frame.text)
    finally:
        await server.close()


if __name__ == "__main__":
    asyncio.run(main())
