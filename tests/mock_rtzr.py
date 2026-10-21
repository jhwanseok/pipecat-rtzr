"""A small in-process mock of the RTZR OpenAPI (auth endpoint + streaming WebSocket).

It lets the test-suite and the example run without RTZR credentials. The behavior follows
https://developers.rtzr.ai/docs/en/stt-streaming/websocket/ :

- ``POST /v1/authenticate`` returns ``{"access_token", "expire_at"}``.
- ``GET /v1/transcribe:streaming`` upgrades to a WebSocket and needs ``Authorization: Bearer``.
- Binary frames are audio; ``{"type": "Finalize"}`` closes the current utterance; ``EOS`` ends
  the session.
- Responses are ``{"seq", "start_at", "duration", "final", "alternatives": [{"text"}]}``.
"""

import json
import time
from dataclasses import dataclass, field

from aiohttp import WSMsgType, web


@dataclass
class MockRTZR:
    """State and recorded traffic of the mock server."""

    valid_token: str = "token-1"
    token_lifetime_s: float = 3600.0
    reject_handshake_status: int | None = None
    reject_first_n_handshakes: int = 0
    interim_text: str = "안녕"
    final_text: str = "안녕하세요"
    interim_every_n_chunks: int = 2

    auth_requests: list[dict] = field(default_factory=list)
    handshakes: list[dict] = field(default_factory=list)
    audio_bytes: int = 0
    text_messages: list[str] = field(default_factory=list)
    tokens_issued: int = 0
    flush_on_eos: bool = True
    _chunks: int = 0
    _open: bool = False

    async def authenticate(self, request: web.Request) -> web.Response:
        form = await request.post()
        self.auth_requests.append(dict(form))
        if form.get("client_secret") != "good-secret":
            return web.json_response({"code": "H0002", "msg": "Unauthorized"}, status=401)
        self.tokens_issued += 1
        return web.json_response(
            {
                "access_token": f"token-{self.tokens_issued}",
                "expire_at": time.time() + self.token_lifetime_s,
            }
        )

    async def stream(self, request: web.Request) -> web.StreamResponse:
        self.handshakes.append(
            {"query": dict(request.query), "authorization": request.headers.get("Authorization")}
        )
        if self.reject_first_n_handshakes > 0:
            self.reject_first_n_handshakes -= 1
            return web.json_response({"code": "H0002", "msg": "Unauthorized"}, status=401)
        if self.reject_handshake_status:
            return web.json_response({"code": "E"}, status=self.reject_handshake_status)
        if request.headers.get("Authorization") != f"Bearer token-{self.tokens_issued}":
            return web.json_response({"code": "H0002", "msg": "Unauthorized"}, status=401)

        ws = web.WebSocketResponse()
        await ws.prepare(request)
        seq = 0
        async for msg in ws:
            if msg.type == WSMsgType.BINARY:
                self.audio_bytes += len(msg.data)
                self._chunks += 1
                if self._chunks % self.interim_every_n_chunks == 0:
                    self._open = True
                    await ws.send_str(self._payload(seq, False, self.interim_text))
            elif msg.type == WSMsgType.TEXT:
                self.text_messages.append(msg.data)
                if msg.data == "EOS":
                    if self._open and self.flush_on_eos:
                        await ws.send_str(self._payload(seq, True, self.final_text))
                        self._open = False
                    await ws.close()
                    break
                try:
                    command = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue
                if command.get("type") == "Finalize":
                    self._open = False
                    await ws.send_str(self._payload(seq, True, self.final_text))
                    seq += 1
        return ws

    @staticmethod
    def _payload(seq: int, final: bool, text: str) -> str:
        return json.dumps(
            {
                "seq": seq,
                "start_at": 0,
                "duration": 1200 if final else 0,
                "final": final,
                "alternatives": [{"text": text, "confidence": 0.9}],
            },
            ensure_ascii=False,
        )

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/v1/authenticate", self.authenticate)
        app.router.add_get("/v1/transcribe:streaming", self.stream)
        return app
