"""Check pipecat-rtzr against the live RTZR API with short WAV files.

Needs RTZR_CLIENT_ID and RTZR_CLIENT_SECRET in the environment. Credentials are never printed.
Audio is billed by duration, so use short clips.

    python scripts/live_check.py token
    python scripts/live_check.py stream clip.wav [cut_seconds]   # LIVE_NO_FINALIZE=1 skips Finalize
    python scripts/live_check.py reconnect clip.wav     # drops the socket mid-stream
"""

import asyncio
import hashlib
import os
import sys
import time
import wave

from pipecat.frames.frames import (
    EndFrame,
    ErrorFrame,
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from pipecat_rtzr import RTZRAuth, RTZRSTTService


def short(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()[:8]


async def check_token() -> None:
    auth = RTZRAuth(os.environ["RTZR_CLIENT_ID"], os.environ["RTZR_CLIENT_SECRET"])
    first = await auth.get_token()
    print(f"token ok (fingerprint {short(first)}), expires in {auth._expire_at - time.time():.0f}s")
    again = await auth.get_token()
    print("second call served from cache:", again == first)
    auth.invalidate()
    new = await auth.get_token()
    print(
        f"after invalidate: new request ok (fingerprint {short(new)}), same token: {new == first}"
    )


class Sink(FrameProcessor):
    def __init__(self, t0: float):
        super().__init__()
        self.t0 = t0
        self.events: list[tuple[float, str, str]] = []

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        now = time.monotonic() - self.t0
        if isinstance(frame, InterimTranscriptionFrame):
            self.events.append((now, "interim", frame.text))
        elif isinstance(frame, TranscriptionFrame):
            self.events.append((now, "FINAL", frame.text))
        elif isinstance(frame, ErrorFrame):
            self.events.append((now, "error", frame.error))
        await self.push_frame(frame, direction)


def read_wav(path: str) -> tuple[bytes, int]:
    with wave.open(path, "rb") as w:
        assert w.getnchannels() == 1 and w.getsampwidth() == 2, "need 16-bit mono PCM"
        return w.readframes(w.getnframes()), w.getframerate()


async def stream(
    path: str, drop_socket_after: float | None = None, cut_seconds: float | None = None
) -> None:
    pcm, rate = read_wav(path)
    if cut_seconds:
        pcm = pcm[: int(cut_seconds * rate) * 2]
    stt = RTZRSTTService(
        settings=RTZRSTTService.Settings(use_punctuation=True),
        vad_force_finalize=not os.environ.get("LIVE_NO_FINALIZE"),
    )
    t0 = time.monotonic()
    sink = Sink(t0)

    raw: list[tuple[float, dict]] = []
    original = stt._handle_message

    async def spy(data):
        raw.append((time.monotonic() - t0, {k: v for k, v in data.items() if k != "alternatives"}))
        await original(data)

    stt._handle_message = spy  # type: ignore[method-assign]

    task = PipelineTask(
        Pipeline([stt, sink]),
        params=PipelineParams(audio_in_sample_rate=rate, enable_metrics=False),
        cancel_on_idle_timeout=False,
    )

    chunk_ms = 20
    step = rate * 2 * chunk_ms // 1000

    async def feed() -> None:
        await asyncio.sleep(1.0)  # let the service connect
        started = time.monotonic()
        dropped = False
        for i in range(0, len(pcm), step):
            await task.queue_frame(
                InputAudioRawFrame(audio=pcm[i : i + step], sample_rate=rate, num_channels=1)
            )
            if drop_socket_after and not dropped and time.monotonic() - started > drop_socket_after:
                dropped = True
                print(f"[{time.monotonic() - t0:5.2f}s] dropping the websocket")
                await stt._websocket.close()
            await asyncio.sleep(chunk_ms / 1000)
        print(f"[{time.monotonic() - t0:5.2f}s] audio sent, sending VAD stop")
        if os.environ.get("LIVE_NO_FINALIZE"):
            await asyncio.sleep(0.2)  # EOS-only: shut down right after the audio
        else:
            await task.queue_frame(VADUserStoppedSpeakingFrame(stop_secs=0.2))
            await asyncio.sleep(2.5)
        await task.queue_frame(EndFrame())

    feeder = asyncio.create_task(feed())
    await PipelineRunner(handle_sigint=False).run(task)
    await feeder

    print("--- raw message fields (without text) ---")
    for t, d in raw:
        print(f"[{t:5.2f}s] {d}")
    print("--- frames ---")
    for t, kind, text in sink.events:
        print(f"[{t:5.2f}s] {kind:7s} {text}")


async def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "token"
    if mode == "token":
        await check_token()
    elif mode == "stream":
        await stream(sys.argv[2], cut_seconds=float(sys.argv[3]) if len(sys.argv) > 3 else None)
    elif mode == "reconnect":
        await stream(sys.argv[2], drop_socket_after=1.5)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    asyncio.run(main())
