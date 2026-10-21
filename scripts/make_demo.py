"""Make the demo video for the Pipecat docs PR.

Streams a short synthesized Korean script through RTZRSTTService against the live RTZR API at
real-time speed, records when each interim and final transcript arrives, and renders that
timeline as an mp4 with the audio.

    python scripts/make_demo.py synth            # Windows only: speech with the Korean SAPI voice
    python scripts/make_demo.py run              # needs RTZR_CLIENT_ID / RTZR_CLIENT_SECRET
    python scripts/make_demo.py render           # needs ffmpeg (or `pip install imageio-ffmpeg`)
    python scripts/make_demo.py preview 12.5     # one PNG at media time 12.5 s, no ffmpeg needed

Files go to --out (default: ./demo-out). Credentials are read from the environment by the
service and never printed or written.
"""

import argparse
import asyncio
import json
import math
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import wave
from pathlib import Path

# (text, cut_seconds, heard): cut_seconds stops the audio mid-sentence, so the server has no
# end-point to find and only Finalize can close the utterance; `heard` is what was actually played
SCRIPT = [
    ("안녕하세요. 리턴제로 음성 인식 데모입니다.", None, None),
    ("내일 오후 세 시에 상담 예약을 하고 싶어요.", None, None),
    ("전화번호는 공일공 일이삼사 오육칠팔 입니다.", None, None),
    ("통화가 중간에 끊겨도 발화를 확정합니다.", 2.3, "통화가 중간에 끊겨도 발화를 확"),
]
INTRO_S = 5.0
OUTRO_S = 7.0
TAIL_S = 1.5
GAP_S = 0.8  # silence after each sentence
LEAD_S = 0.6  # silence before the first one
VAD_STOP_AFTER_S = 0.2  # a VAD with stop_secs=0.2 reports the end of speech this long after it

W, H, FPS = 1280, 720, 15
FONT_REG = "C:/Windows/Fonts/malgun.ttf"
FONT_BOLD = "C:/Windows/Fonts/malgunbd.ttf"

BG = (18, 20, 26)
PANEL = (28, 31, 40)
TEXT = (226, 229, 236)
DIM = (130, 136, 150)
ACCENT = (96, 165, 250)
INTERIM = (250, 204, 21)
FINAL = (74, 222, 128)


# --------------------------------------------------------------------------- audio


def _read(path: Path) -> tuple[bytes, int]:
    with wave.open(str(path), "rb") as w:
        assert w.getnchannels() == 1 and w.getsampwidth() == 2
        return w.readframes(w.getnframes()), w.getframerate()


def _write(path: Path, pcm: bytes, rate: int) -> None:
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(pcm)


def _speech_end(clip: bytes, rate: int) -> float:
    """Seconds into the clip where speech ends (trailing silence trimmed)."""
    n = len(clip) // 2
    samples = struct.unpack(f"<{n}h", clip)
    win = rate // 50
    rms = [
        math.sqrt(sum(x * x for x in samples[i : i + win]) / win)
        for i in range(0, n - win + 1, win)
    ]
    peak = max(rms) or 1.0
    last = max((i for i, v in enumerate(rms) if v > 0.04 * peak), default=len(rms) - 1)
    return (last + 1) * win / rate


def synth(out: Path) -> None:
    """Synthesize SCRIPT with the Windows Korean voice (Microsoft Heami) at 8 kHz and 16 kHz."""
    out.mkdir(parents=True, exist_ok=True)
    ps = out / "synth.ps1"
    lines = [
        "Add-Type -AssemblyName System.Speech",
        "$s = New-Object System.Speech.Synthesis.SpeechSynthesizer",
        "$s.SelectVoice('Microsoft Heami Desktop')",
    ]
    for i, (text, _cut, _heard) in enumerate(SCRIPT):
        for hz in (8000, 16000):
            target = str((out / f"s{i}-{hz}.wav").resolve())
            lines += [
                f"$f = New-Object System.Speech.AudioFormat.SpeechAudioFormatInfo({hz}, "
                "[System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen, "
                "[System.Speech.AudioFormat.AudioChannel]::Mono)",
                f"$s.SetOutputToWaveFile('{target}', $f)",
                f"$s.Speak('{text}')",
                "$s.SetOutputToNull()",
            ]
    ps.write_text("\n".join(lines), encoding="utf-8-sig")  # BOM keeps the Korean intact
    subprocess.run(
        ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ps)], check=True
    )
    ps.unlink()

    timeline = []
    for hz in (8000, 16000):
        pcm = b"\x00\x00" * int(LEAD_S * hz)
        cursor = LEAD_S
        for i in range(len(SCRIPT)):
            clip, rate = _read(out / f"s{i}-{hz}.wav")
            assert rate == hz
            cut = SCRIPT[i][1]
            if cut:
                clip = clip[: int(cut * hz) * 2]
            if hz == 8000:
                timeline.append(
                    {
                        "text": SCRIPT[i][0],
                        "start": cursor,
                        "end": cursor + _speech_end(clip, hz)
                        if not cut
                        else cursor + len(clip) / 2 / hz,
                        "clip_end": cursor + len(clip) / 2 / hz,
                        "cut": bool(cut),
                    }
                )
            pcm += clip + b"\x00\x00" * int(GAP_S * hz)
            cursor += len(clip) / 2 / hz + GAP_S
        _write(out / f"demo-{hz}.wav", pcm, hz)
    (out / "script.json").write_text(json.dumps(timeline, ensure_ascii=False, indent=1), "utf-8")
    for p in out.glob("s?-*.wav"):
        p.unlink()
    print(f"wrote demo-8000.wav, demo-16000.wav ({cursor:.1f} s), script.json")


# --------------------------------------------------------------------------- live run


async def run(out: Path) -> None:
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

    from pipecat_rtzr import RTZRSTTService

    timeline = json.loads((out / "script.json").read_text("utf-8"))
    pcm, rate = _read(out / "demo-8000.wav")
    state = {"t0": None}
    events: list[dict] = []

    def now() -> float:
        return time.monotonic() - state["t0"] if state["t0"] else 0.0

    class Sink(FrameProcessor):
        async def process_frame(self, frame, direction: FrameDirection):
            await super().process_frame(frame, direction)
            if state["t0"] is not None:
                if isinstance(frame, InterimTranscriptionFrame):
                    events.append({"t": now(), "kind": "interim", "text": frame.text})
                elif isinstance(frame, TranscriptionFrame):
                    events.append({"t": now(), "kind": "final", "text": frame.text})
                elif isinstance(frame, ErrorFrame):
                    events.append({"t": now(), "kind": "error", "text": "error"})
            await self.push_frame(frame, direction)

    stt = RTZRSTTService(settings=RTZRSTTService.Settings(use_punctuation=True))
    task = PipelineTask(
        Pipeline([stt, Sink()]),
        params=PipelineParams(audio_in_sample_rate=rate, enable_metrics=False),
        cancel_on_idle_timeout=False,
    )

    chunk_s = 0.02
    step = int(rate * chunk_s) * 2
    stops = sorted(s["clip_end"] + VAD_STOP_AFTER_S for s in timeline)

    async def feed() -> None:
        await asyncio.sleep(1.5)  # let the service connect
        state["t0"] = time.monotonic()
        n = 0
        pending = list(stops)
        while n * step < len(pcm):
            media_t = n * chunk_s
            await task.queue_frame(
                InputAudioRawFrame(
                    audio=pcm[n * step : (n + 1) * step], sample_rate=rate, num_channels=1
                )
            )
            while pending and media_t >= pending[0]:
                events.append({"t": now(), "kind": "vad_stop", "text": "VAD stop, Finalize sent"})
                await task.queue_frame(VADUserStoppedSpeakingFrame(stop_secs=0.2))
                pending.pop(0)
            n += 1
            # pace to real time against the wall clock, so sleeps do not drift
            await asyncio.sleep(max(0.0, state["t0"] + n * chunk_s - time.monotonic()))
        await asyncio.sleep(1.5)
        await task.queue_frame(EndFrame())

    feeder = asyncio.create_task(feed())
    await PipelineRunner(handle_sigint=False).run(task)
    await feeder

    (out / "events.json").write_text(json.dumps(events, ensure_ascii=False, indent=1), "utf-8")
    finals = [e for e in events if e["kind"] == "final"]
    print(f"{len(events)} events, {len(finals)} finals; wrote events.json")
    for e in events:
        print(f"[{e['t']:6.2f}s] {e['kind']:8s} {e['text']}")


# --------------------------------------------------------------------------- rendering


def _load():
    from PIL import Image, ImageDraw, ImageFont  # noqa: F401

    return Image, ImageDraw, ImageFont


def _envelope(path: Path, bins_per_s: int = 30) -> list[float]:
    pcm, rate = _read(path)
    n = len(pcm) // 2
    samples = struct.unpack(f"<{n}h", pcm)
    size = rate // bins_per_s
    env = []
    for i in range(0, n, size):
        chunk = samples[i : i + size]
        env.append(math.sqrt(sum(s * s for s in chunk) / max(len(chunk), 1)) / 32768)
    peak = max(env) or 1.0
    return [e / peak for e in env]


class Renderer:
    def __init__(self, out: Path):
        self.Image, self.ImageDraw, self.ImageFont = _load()
        self.timeline = json.loads((out / "script.json").read_text("utf-8"))
        self.events = json.loads((out / "events.json").read_text("utf-8"))
        self.env = _envelope(out / "demo-16000.wav")
        self.duration = len(self.env) / 30
        f = self.ImageFont.truetype
        self.f_title = f(FONT_BOLD, 30)
        self.f_sub = f(FONT_REG, 20)
        self.f_big = f(FONT_BOLD, 34)
        self.f_mid = f(FONT_REG, 26)
        self.f_small = f(FONT_REG, 20)
        # latency of the last final of each sentence, measured from the end of its speech
        self.latency: list[int | None] = []
        for i, seg in enumerate(self.timeline):
            finals = [e for e in self.events if e["kind"] == "final" and self._in_window(i, e["t"])]
            if seg["cut"]:
                # no end-point for the server to find: measure from the VAD stop that sent Finalize
                stops = [
                    e["t"]
                    for e in self.events
                    if e["kind"] == "vad_stop" and self._in_window(i, e["t"])
                ]
                ref = stops[-1] if stops else seg["clip_end"] + VAD_STOP_AFTER_S
            else:
                ref = seg["end"]
            self.latency.append(round((finals[-1]["t"] - ref) * 1000) if finals else None)
        self.content_end = max([e["t"] for e in self.events] + [self.duration]) + TAIL_S
        self.t_end = INTRO_S + self.content_end + OUTRO_S

    def _in_window(self, i: int, t: float) -> bool:
        lo = self.timeline[i]["start"] - 0.1
        hi = self.timeline[i + 1]["start"] - 0.1 if i + 1 < len(self.timeline) else 1e9
        return lo <= t < hi

    def _sentence_state(self, i: int, t: float):
        """Committed text (all finals so far) and the interim text after the last final."""
        committed: list[str] = []
        interim = ""
        for e in self.events:
            if e["t"] > t or not self._in_window(i, e["t"]):
                continue
            if e["kind"] == "interim":
                interim = e["text"]
            elif e["kind"] == "final":
                committed.append(e["text"])
                interim = ""
        return " ".join(committed), interim

    def card(self, title: str, lines: list[str], code: str | None = None):
        img = self.Image.new("RGB", (W, H), BG)
        d = self.ImageDraw.Draw(img)
        d.text((80, 150), title, font=self.f_title, fill=TEXT)
        y = 230
        for line in lines:
            d.text((80, y), line, font=self.f_mid, fill=DIM if line.startswith("(") else TEXT)
            y += 46
        if code:
            d.rounded_rectangle((80, y + 20, W - 80, y + 100), 12, fill=PANEL)
            d.text((110, y + 40), code, font=self.f_mid, fill=ACCENT)
        return img

    def frame(self, t: float):
        if t < INTRO_S:
            return self.card(
                "pipecat-rtzr",
                [
                    "Streaming Korean speech-to-text for Pipecat",
                    "RTZR VITO live API  ·  recorded in real time",
                    "(Synthesized Korean speech, Windows SAPI voice. No credentials shown.)",
                ],
            )
        if t >= INTRO_S + self.content_end:
            return self.card(
                "Install",
                [
                    "RTZRSTTService extends WebsocketSTTService.",
                    "Interim and final transcripts, Finalize on VAD stop, EOS on shutdown,",
                    "token refresh and automatic reconnect.",
                    "github.com/jhwanseok/pipecat-rtzr",
                ],
                code="uv add pipecat-rtzr",
            )
        return self.main(t - INTRO_S)

    def main(self, t: float):
        Image, ImageDraw = self.Image, self.ImageDraw
        img = Image.new("RGB", (W, H), BG)
        d = ImageDraw.Draw(img)

        d.text((48, 30), "pipecat-rtzr", font=self.f_title, fill=TEXT)
        d.text(
            (48, 74),
            "Streaming Korean STT for Pipecat  ·  RTZR VITO live API  ·  "
            "8 kHz, domain=CALL, model=sommers_ko",
            font=self.f_sub,
            fill=DIM,
        )

        # waveform + playhead
        x0, x1, y0, y1 = 48, W - 48, 118, 188
        d.rounded_rectangle((x0, y0, x1, y1), 10, fill=PANEL)
        n = len(self.env)
        bw = (x1 - x0 - 24) / n
        mid = (y0 + y1) / 2
        for i, a in enumerate(self.env):
            x = x0 + 12 + i * bw
            h = max(2, a * 26)
            played = i / 30 <= t
            d.rectangle(
                (x, mid - h, x + max(bw - 1, 1), mid + h), fill=ACCENT if played else (60, 66, 82)
            )

        # sentences
        y = 204
        for i, seg in enumerate(self.timeline):
            speaking = seg["start"] <= t <= seg["end"]
            committed, interim = self._sentence_state(i, t)
            box_h = 118
            d.rounded_rectangle(
                (48, y, W - 48, y + box_h),
                12,
                fill=PANEL,
                outline=ACCENT if speaking else None,
                width=2,
            )
            d.text((68, y + 12), f"{i + 1}.  원문", font=self.f_small, fill=DIM)
            shown = SCRIPT[i][2] or seg["text"]
            fill = TEXT if speaking or t > seg["end"] else DIM
            d.text((170, y + 6), shown + ("…" if seg["cut"] else ""), font=self.f_mid, fill=fill)
            if seg["cut"]:
                x = 170 + d.textlength(shown + "…  ", font=self.f_mid)
                d.text(
                    (x, y + 12),
                    "[음성 끊김 · audio cut mid-word]",
                    font=self.f_small,
                    fill=(251, 146, 60) if t > seg["end"] - 0.3 else DIM,
                )

            d.text((68, y + 46), "인식", font=self.f_small, fill=DIM)
            if committed:
                d.text((170, y + 40), committed, font=self.f_mid, fill=FINAL)
            if interim:
                x = 170 + (d.textlength(committed + " ", font=self.f_mid) if committed else 0)
                d.text((x, y + 40), interim, font=self.f_mid, fill=INTERIM)
            if not committed and not interim:
                d.text((170, y + 46), "…", font=self.f_mid, fill=DIM)
            lat = self.latency[i]
            if interim:
                d.text((170, y + 82), "interim (updating)", font=self.f_small, fill=INTERIM)
            elif committed and lat is not None:
                if seg["cut"]:
                    note = (
                        f"final by Finalize  +{lat} ms after VAD stop "
                        "(server end-point not reached)"
                    )
                else:
                    note = f"final by server end-point  +{lat} ms after speech end"
                d.text((170, y + 82), note, font=self.f_small, fill=FINAL)
            y += box_h + 10

        # footer
        stop_times = [e["t"] for e in self.events if e["kind"] == "vad_stop"]
        if any(0 <= t - s < 0.8 for s in stop_times):
            d.text(
                (48, H - 44),
                "VAD reported end of speech  →  Finalize sent",
                font=self.f_small,
                fill=ACCENT,
            )
        d.text((W - 150, 36), f"{t:5.1f} s", font=self.f_small, fill=DIM)
        return img


def _ffmpeg() -> str:
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        raise SystemExit(
            "ffmpeg not found. Install it, or run: pip install imageio-ffmpeg"
        ) from None


def render(out: Path) -> None:
    r = Renderer(out)
    target = out / "pipecat-rtzr-demo.mp4"
    total = int(r.t_end * FPS)
    with tempfile.TemporaryDirectory() as tmp:
        audio = Path(tmp) / "audio.wav"
        pcm, rate = _read(out / "demo-16000.wav")
        pad = b"\x00\x00" * int(rate * (r.t_end - len(pcm) / 2 / rate))
        _write(audio, pcm + pad, rate)
        cmd = [
            _ffmpeg(), "-y", "-loglevel", "error",
            "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{W}x{H}", "-r", str(FPS), "-i", "-",
            "-i", str(audio),
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "27", "-preset", "medium",
            "-c:a", "aac", "-b:a", "96k", "-shortest", "-movflags", "+faststart", str(target),
        ]  # fmt: skip
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        for i in range(total):
            proc.stdin.write(r.frame(i / FPS).tobytes())
        proc.stdin.close()
        if proc.wait() != 0:
            raise SystemExit("ffmpeg failed")
    print(f"wrote {target} ({target.stat().st_size / 1e6:.1f} MB, {r.t_end:.1f} s)")


def preview(out: Path, t: float) -> None:
    r = Renderer(out)
    path = out / f"preview-{t:.1f}.png"
    r.frame(t).save(path)
    print(f"wrote {path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("step", choices=["synth", "run", "render", "preview"])
    ap.add_argument("time", nargs="?", type=float, default=0.0)
    ap.add_argument("--out", type=Path, default=Path("demo-out"))
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    if args.step == "synth":
        synth(args.out)
    elif args.step == "run":
        asyncio.run(run(args.out))
    elif args.step == "render":
        render(args.out)
    else:
        preview(args.out, args.time)


if __name__ == "__main__":
    main()
