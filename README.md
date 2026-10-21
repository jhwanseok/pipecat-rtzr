# pipecat-rtzr

Streaming Korean speech-to-text for [Pipecat](https://github.com/pipecat-ai/pipecat), using the
[RTZR (Return Zero) VITO](https://developers.rtzr.ai/docs/en/) WebSocket API.

> Status: **pre-release (0.1.0.dev0)**. Tested against an in-process mock of the RTZR API and
> Pipecat 1.13.0. It has **not yet been run against the live RTZR service**.

RTZR's `sommers_ko` model is tuned for Korean telephone audio (8 kHz), which makes it a good fit
for call-center voice agents.

## Installation

```bash
pip install pipecat-rtzr
```

## Prerequisites

- An RTZR account. Create a client in the [RTZR Developers Console](https://developers.rtzr.ai/)
  to get a `client_id` and `client_secret` (new accounts include free minutes).
- Set the credentials as environment variables:

```bash
export RTZR_CLIENT_ID=...
export RTZR_CLIENT_SECRET=...
```

## Usage

```python
from pipecat_rtzr import RTZRSTTService

stt = RTZRSTTService(
    settings=RTZRSTTService.Settings(
        domain="CALL",
        use_punctuation=True,
        keywords=[("리턴제로", 3.0), "상담원"],
    ),
)

pipeline = Pipeline([transport.input(), stt, user_aggregator, llm, tts, transport.output()])
```

See [`example.py`](example.py) for a complete bot, and
[`example_mock.py`](example_mock.py) for a run that needs no credentials.

### Behavior

- Audio is streamed as 16-bit mono PCM (`LINEAR16`, 8 to 48 kHz).
- Partial results become `InterimTranscriptionFrame`s; completed utterances become
  `TranscriptionFrame`s.
- When the VAD reports the end of speech, a `Finalize` message closes the utterance right away
  (`vad_force_finalize=True`, the default).
- The access token is cached and refreshed before it expires. A rejected handshake (HTTP 401)
  fetches a new token and retries once.
- Idle connections are kept alive with silent audio (RTZR closes sockets after 60 s without audio).
- On shutdown the service sends `EOS` so the last utterance is flushed.
- Changing a setting at runtime reconnects the stream.

### Settings

| Setting | Default | Notes |
|---|---|---|
| `model` | `sommers_ko` | `sommers_ko`, `sommers_ja`, `whisper` |
| `language` | `Language.KO` | sent only for `whisper` |
| `domain` | `CALL` | `CALL` or `MEETING` |
| `use_itn` | `True` | inverse text normalization |
| `use_disfluency_filter` | `False` | |
| `use_profanity_filter` | `False` | |
| `use_punctuation` | `False` | |
| `keywords` | `None` | up to 100 words of 20 characters; boost -5.0 to 5.0; `sommers_ko` only |
| `epd_time`, `noise_threshold`, `active_threshold` | server default | end-point and activity tuning |

Streaming concurrency is limited by your RTZR plan (5 channels on the free tier).

## Development

```bash
uv venv && uv pip install -e . pytest pytest-asyncio ruff
pytest          # unit tests and mock-server integration tests, no credentials needed
ruff check . && ruff format --check .
```

## Compatibility

Tested with Pipecat 1.13.0 and Python 3.12.

## License

BSD 2-Clause.
