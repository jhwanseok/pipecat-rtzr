"""RTZR (Return Zero) VITO streaming speech-to-text service for Pipecat."""

import asyncio
import json
import os
from collections.abc import AsyncGenerator, Iterable
from dataclasses import dataclass, field, replace
from typing import Any
from urllib.parse import urlencode

from loguru import logger
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    ErrorFrame,
    Frame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessorSetup
from pipecat.services.settings import NOT_GIVEN, NotGiven, STTSettings
from pipecat.services.stt_service import WebsocketSTTService
from pipecat.transcriptions.language import Language
from pipecat.utils.time import time_now_iso8601
from pipecat.utils.tracing.service_decorators import traced_stt
from websockets.exceptions import InvalidStatus
from websockets.protocol import State

from .auth import DEFAULT_API_BASE, RTZRAuth, RTZRAuthError

DEFAULT_MODEL = "sommers_ko"
STREAMING_PATH = "/v1/transcribe:streaming"

FINALIZE_MESSAGE = json.dumps({"type": "Finalize"})
EOS_MESSAGE = "EOS"

_MAX_KEYWORDS = 100
_MAX_KEYWORD_CHARS = 20
_MIN_BOOST, _MAX_BOOST = -5.0, 5.0

# RTZR accepts 8-48 kHz raw audio. 8 kHz is the native telephony rate.
_MIN_SAMPLE_RATE, _MAX_SAMPLE_RATE = 8000, 48000


def language_to_rtzr_language(language: Language) -> str | None:
    """Convert a Pipecat Language to the RTZR ``language`` value used by the whisper model.

    Args:
        language: The Pipecat language.

    Returns:
        The RTZR language code, or ``None`` when RTZR does not support it.
    """
    mapping = {Language.KO: "ko", Language.JA: "ja", Language.EN: "en"}
    return mapping.get(language)


def format_keywords(keywords: Iterable[str | tuple[str, float]]) -> str:
    """Format keyword-boosting entries as the comma-separated RTZR ``keywords`` value.

    Each entry is a word, or a ``(word, boost)`` tuple with a boost from -5.0 to 5.0
    (the RTZR default boost is 2.0). At most 100 entries of up to 20 characters each.

    Args:
        keywords: Words or ``(word, boost)`` tuples.

    Returns:
        The value for the ``keywords`` query parameter.

    Raises:
        ValueError: If an entry is empty, too long, or has an out-of-range boost.
    """
    items = list(keywords)
    if len(items) > _MAX_KEYWORDS:
        raise ValueError(f"RTZR keyword boosting supports at most {_MAX_KEYWORDS} keywords")
    parts: list[str] = []
    for item in items:
        word, boost = item if isinstance(item, tuple) else (item, None)
        word = word.strip()
        if not word:
            raise ValueError("RTZR keywords must be non-empty")
        if len(word) > _MAX_KEYWORD_CHARS:
            raise ValueError(f"RTZR keywords must be at most {_MAX_KEYWORD_CHARS} characters")
        if boost is None:
            parts.append(word)
            continue
        if not _MIN_BOOST <= float(boost) <= _MAX_BOOST:
            raise ValueError(f"RTZR keyword boost must be between {_MIN_BOOST} and {_MAX_BOOST}")
        parts.append(f"{word}:{float(boost)}")
    return ",".join(parts)


@dataclass
class RTZRSTTSettings(STTSettings):
    """Runtime-updatable settings for :class:`RTZRSTTService`.

    Changing a setting reconnects the stream, since RTZR reads them from the
    connection URL.

    Parameters:
        domain: ``"CALL"`` (telephony, default) or ``"MEETING"``.
        use_itn: Apply inverse text normalization (numbers, dates).
        use_disfluency_filter: Drop filler words.
        use_profanity_filter: Mask profanity.
        use_punctuation: Add punctuation to transcripts.
        keywords: Words (or ``(word, boost)`` tuples) to boost. Only for ``sommers_ko``,
            or ``whisper`` with Korean.
        epd_time: End-point detection silence, in seconds.
        noise_threshold: Noise gate threshold.
        active_threshold: Speech activity threshold.

    ``model`` is ``"sommers_ko"``, ``"sommers_ja"`` or ``"whisper"``. ``language`` is only
    sent for ``"whisper"``.
    """

    domain: str | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)
    use_itn: bool | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)
    use_disfluency_filter: bool | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)
    use_profanity_filter: bool | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)
    use_punctuation: bool | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)
    keywords: list[str | tuple[str, float]] | None | NotGiven = field(
        default_factory=lambda: NOT_GIVEN
    )
    epd_time: float | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)
    noise_threshold: float | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)
    active_threshold: float | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)


class RTZRSTTService(WebsocketSTTService):
    """Streaming speech-to-text with the RTZR VITO WebSocket API.

    Audio is sent as raw 16-bit PCM. Partial results arrive as
    :class:`~pipecat.frames.frames.InterimTranscriptionFrame` and completed utterances
    as :class:`~pipecat.frames.frames.TranscriptionFrame`. When the local VAD reports that
    the user stopped speaking, a ``Finalize`` message makes RTZR close the utterance
    immediately instead of waiting for its own end-point detection.

    API reference: https://developers.rtzr.ai/docs/en/stt-streaming/websocket/
    """

    Settings = RTZRSTTSettings
    _settings: RTZRSTTSettings

    def __init__(
        self,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        api_base: str = DEFAULT_API_BASE,
        sample_rate: int | None = None,
        vad_force_finalize: bool = True,
        settings: RTZRSTTSettings | None = None,
        ttfs_p99_latency: float | None = None,
        **kwargs,
    ):
        """Initialize the RTZR STT service.

        Args:
            client_id: RTZR client ID. Defaults to the ``RTZR_CLIENT_ID`` environment variable.
            client_secret: RTZR client secret. Defaults to ``RTZR_CLIENT_SECRET``.
            api_base: Base URL of the RTZR OpenAPI. The WebSocket URL is derived from it.
            sample_rate: Input sample rate (8000-48000). Taken from the pipeline when omitted.
            vad_force_finalize: Send ``Finalize`` when the VAD reports the end of speech.
            settings: Runtime-updatable settings.
            ttfs_p99_latency: P99 time from end of speech to the final transcript, in seconds.
                Measure it for your deployment with the Pipecat STT benchmark.
            **kwargs: Passed to :class:`~pipecat.services.stt_service.WebsocketSTTService`.
        """
        client_id = client_id or os.environ.get("RTZR_CLIENT_ID")
        client_secret = client_secret or os.environ.get("RTZR_CLIENT_SECRET")
        if not (client_id and client_secret):
            raise ValueError(
                "RTZR credentials are required: pass client_id and client_secret, "
                "or set RTZR_CLIENT_ID and RTZR_CLIENT_SECRET"
            )

        default_settings = self.Settings(
            model=DEFAULT_MODEL,
            language=Language.KO,
            domain="CALL",
            use_itn=True,
            use_disfluency_filter=False,
            use_profanity_filter=False,
            use_punctuation=False,
            keywords=None,
            epd_time=None,
            noise_threshold=None,
            active_threshold=None,
        )
        if settings is not None:
            default_settings.apply_update(replace(settings))

        kwargs.setdefault("keepalive_timeout", 10)
        super().__init__(
            sample_rate=sample_rate,
            ttfs_p99_latency=ttfs_p99_latency,
            settings=default_settings,
            **kwargs,
        )

        self._auth = RTZRAuth(client_id, client_secret, api_base=api_base)
        self._vad_force_finalize = vad_force_finalize
        self._receive_task: asyncio.Task | None = None

    def can_generate_metrics(self) -> bool:
        """Whether this service reports processing metrics.

        Returns:
            True.
        """
        return True

    def language_to_service_language(self, language: Language) -> str | None:
        """Convert a Pipecat Language to the RTZR language value.

        Args:
            language: The Pipecat language.

        Returns:
            The RTZR language code, or ``None`` when unsupported.
        """
        return language_to_rtzr_language(language)

    async def _update_settings(self, delta: RTZRSTTSettings) -> dict[str, Any]:  # type: ignore[override]
        """Apply a settings delta and reconnect, since RTZR reads settings from the URL.

        Args:
            delta: A settings delta.

        Returns:
            Mapping of changed field names to their previous values.
        """
        changed = await super()._update_settings(delta)
        if changed:
            await self._request_reconnect()
        return changed

    async def setup(self, setup: FrameProcessorSetup):  # type: ignore[override]
        """Set up the service and open the connection.

        Args:
            setup: Processor setup parameters.
        """
        await super().setup(setup)
        await self._connect()

    async def stop(self, frame: EndFrame):
        """Send ``EOS`` so RTZR flushes the last utterance, then disconnect.

        Args:
            frame: The end frame.
        """
        await self._send_eos()
        await super().stop(frame)

    async def cancel(self, frame: CancelFrame):
        """Disconnect immediately, without ``EOS``.

        Args:
            frame: The cancel frame.
        """
        await super().cancel(frame)
        await self._disconnect()

    async def run_stt(self, audio: bytes) -> AsyncGenerator[Frame | None, None]:
        """Send audio to RTZR. Transcripts come back through the receive loop.

        Args:
            audio: Raw 16-bit mono PCM.

        Yields:
            None, always.
        """
        if self._websocket and self._websocket.state is State.OPEN:
            try:
                await self._websocket.send(audio)
            except Exception as e:
                logger.warning(f"{self}: failed to send audio: {e}")
        yield None

    @traced_stt
    async def _handle_transcription(
        self, transcript: str, is_final: bool, language: Language | None = None
    ):
        """Record a transcript for tracing."""
        pass

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Finalize the utterance when the local VAD reports the end of speech.

        Args:
            frame: The frame to process.
            direction: Direction of the frame.
        """
        await super().process_frame(frame, direction)
        if isinstance(frame, VADUserStoppedSpeakingFrame) and self._vad_force_finalize:
            if self._websocket and self._websocket.state is State.OPEN:
                self.request_finalize()
                await self._websocket.send(FINALIZE_MESSAGE)

    async def _send_eos(self):
        if self._websocket and self._websocket.state is State.OPEN:
            try:
                await self._websocket.send(EOS_MESSAGE)
            except Exception as e:
                logger.warning(f"{self}: failed to send EOS: {e}")

    def _stream_url(self) -> str:
        s = self._settings
        params: dict[str, str] = {
            "model_name": str(s.model),
            "sample_rate": str(self.sample_rate),
            "encoding": "LINEAR16",
        }

        def put_bool(key: str, value: Any) -> None:
            if isinstance(value, bool):
                params[key] = "true" if value else "false"

        if isinstance(s.domain, str):
            params["domain"] = s.domain
        put_bool("use_itn", s.use_itn)
        put_bool("use_disfluency_filter", s.use_disfluency_filter)
        put_bool("use_profanity_filter", s.use_profanity_filter)
        put_bool("use_punctuation", s.use_punctuation)
        for key in ("epd_time", "noise_threshold", "active_threshold"):
            value = getattr(s, key)
            if isinstance(value, (int, float)):
                params[key] = str(value)
        if s.keywords:
            params["keywords"] = format_keywords(s.keywords)
        if s.model == "whisper":
            language = s.language
            if isinstance(language, Language):
                language = language_to_rtzr_language(language)
            if isinstance(language, str):
                params["language"] = language

        ws_base = self._auth.api_base.replace("https://", "wss://", 1).replace(
            "http://", "ws://", 1
        )
        return f"{ws_base}{STREAMING_PATH}?{urlencode(params)}"

    async def _connect(self):
        await self._connect_websocket()
        await super()._connect()
        if not self._receive_task:
            self._receive_task = self.create_task(self._receive_task_handler(self._report_error))

    async def _disconnect(self):
        await super()._disconnect()
        if self._receive_task:
            await self.cancel_task(self._receive_task)
            self._receive_task = None
        await self._disconnect_websocket()

    async def _connect_websocket(self):
        if self._websocket and self._websocket.state is State.OPEN:
            return
        if not _MIN_SAMPLE_RATE <= (self.sample_rate or _MIN_SAMPLE_RATE) <= _MAX_SAMPLE_RATE:
            await self.push_error(
                error_msg=f"RTZR supports 8-48 kHz audio, got {self.sample_rate} Hz"
            )
            return

        try:
            # A rejected handshake with 401 usually means the cached token expired early.
            for attempt in (1, 2):
                token = await self._auth.get_token()
                try:
                    logger.debug("Connecting to RTZR STT")
                    self._websocket = await self._websocket_connect(
                        self._stream_url(),
                        additional_headers={"Authorization": f"Bearer {token}"},
                    )
                    break
                except InvalidStatus as e:
                    status = e.response.status_code
                    if status == 401 and attempt == 1:
                        self._auth.invalidate()
                        continue
                    raise RTZRAuthError(_describe_handshake_error(status), status) from e
            await self._call_event_handler("on_connected")
            logger.debug("Connected to RTZR STT")
        except Exception as e:
            self._websocket = None
            await self.push_error(error_msg=f"Unable to connect to RTZR: {e}", exception=e)

    async def _disconnect_websocket(self):
        try:
            if self._websocket:
                logger.debug("Disconnecting from RTZR STT")
                await self._websocket.close()
        except Exception as e:
            await self.push_error(error_msg=f"Error closing RTZR websocket: {e}", exception=e)
        finally:
            self._websocket = None
            await self._call_event_handler("on_disconnected")

    def _get_websocket(self):
        if self._websocket:
            return self._websocket
        raise Exception("Websocket not connected")

    async def _receive_messages(self):
        async for message in self._get_websocket():
            try:
                data = json.loads(message)
            except (TypeError, json.JSONDecodeError):
                logger.warning(f"{self}: ignoring non-JSON message")
                continue
            await self._handle_message(data)

    async def _handle_message(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return
        if data.get("error") or data.get("type") == "error":
            detail = data.get("error") or data.get("message")
            await self.push_error(error_msg=f"RTZR error: {detail}")
            return

        alternatives = data.get("alternatives") or []
        text = (alternatives[0].get("text") if alternatives else "") or ""
        text = text.strip()
        if not text:
            return

        language = self._result_language()
        if data.get("final"):
            self.confirm_finalize()
            await self.emit_stt_usage_metrics()
            await self.push_frame(
                TranscriptionFrame(
                    text=text,
                    user_id=self._user_id,
                    timestamp=time_now_iso8601(),
                    language=language,
                    result=data,
                )
            )
            await self._handle_transcription(text, is_final=True, language=language)
        else:
            await self.push_frame(
                InterimTranscriptionFrame(
                    text=text,
                    user_id=self._user_id,
                    timestamp=time_now_iso8601(),
                    language=language,
                    result=data,
                )
            )

    def _result_language(self) -> Language | None:
        language = self._settings.language
        if isinstance(language, Language):
            return language
        if isinstance(language, str):
            try:
                return Language(language)
            except ValueError:
                return None
        return None

    async def _send_keepalive(self, silence: bytes):
        """Keep the connection alive with silent audio (RTZR drops idle sockets after 60 s).

        Args:
            silence: Silent 16-bit mono PCM.
        """
        if self._websocket is None:
            return
        await self._websocket.send(silence)
        self._record_stt_audio_usage(silence)

    async def _report_error(self, error: ErrorFrame, force_treat_as_permanent: bool = False):
        await self._call_event_handler("on_connection_error", error.error)


def _describe_handshake_error(status: int) -> str:
    reasons = {
        400: "invalid parameters (H0001)",
        401: "unauthorized (H0002)",
        429: "usage exceeded (A0001) or too many concurrent streams",
        500: "RTZR server error (E500)",
    }
    return f"RTZR rejected the WebSocket handshake: HTTP {status}, {reasons.get(status, 'unknown')}"
