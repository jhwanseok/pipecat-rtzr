# Changelog

## 0.1.0 (2026-10-22)
- First release: streaming Korean speech-to-text for Pipecat over the RTZR VITO WebSocket API.
- Interim and final transcripts, `Finalize` on VAD stop, `EOS` flush on shutdown.
- Access-token caching and refresh, automatic reconnect, keepalive, keyword boosting.
- Checked against the live RTZR API with short clips; mock-server tests run without credentials.
