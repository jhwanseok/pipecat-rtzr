"""Access-token handling for the RTZR OpenAPI."""

import asyncio
import time

import aiohttp

DEFAULT_API_BASE = "https://openapi.vito.ai"

# Refresh this many seconds before the token's stated expiry.
_REFRESH_MARGIN_S = 300.0


class RTZRAuthError(Exception):
    """Raised when an access token cannot be obtained."""

    def __init__(self, message: str, status: int | None = None):
        """Create the error.

        Args:
            message: Human-readable description.
            status: HTTP status of the failed request, if any.
        """
        super().__init__(message)
        self.status = status


class RTZRAuth:
    """Fetches and caches an RTZR access token.

    The token is requested with ``POST {api_base}/v1/authenticate`` and the response
    carries ``access_token`` and ``expire_at`` (epoch seconds). It is refreshed shortly
    before it expires, and can be dropped with :meth:`invalidate` after a 401.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        api_base: str = DEFAULT_API_BASE,
        session: aiohttp.ClientSession | None = None,
    ):
        """Create the token provider.

        Args:
            client_id: Client ID from the RTZR Developers Console.
            client_secret: Client secret from the RTZR Developers Console.
            api_base: Base URL of the RTZR OpenAPI.
            session: Optional shared HTTP session. One is created per call if omitted.
        """
        self._client_id = client_id
        self._client_secret = client_secret
        self._api_base = api_base.rstrip("/")
        self._session = session
        self._token: str | None = None
        self._expire_at = 0.0
        self._lock = asyncio.Lock()

    @property
    def api_base(self) -> str:
        """Base URL of the RTZR OpenAPI."""
        return self._api_base

    def invalidate(self) -> None:
        """Forget the cached token so the next call fetches a new one."""
        self._token = None
        self._expire_at = 0.0

    async def get_token(self) -> str:
        """Return a valid access token, fetching a new one when needed."""
        async with self._lock:
            if self._token and time.time() < self._expire_at - _REFRESH_MARGIN_S:
                return self._token
            await self._fetch()
            assert self._token is not None
            return self._token

    async def _fetch(self) -> None:
        url = f"{self._api_base}/v1/authenticate"
        form = {"client_id": self._client_id, "client_secret": self._client_secret}
        owns = self._session is None
        session = self._session or aiohttp.ClientSession()
        try:
            async with session.post(url, data=form) as resp:
                if resp.status != 200:
                    raise RTZRAuthError(
                        f"RTZR authentication failed (HTTP {resp.status})", resp.status
                    )
                body = await resp.json()
        except aiohttp.ClientError as e:
            raise RTZRAuthError(f"RTZR authentication request failed: {e}") from e
        finally:
            if owns:
                await session.close()

        token = body.get("access_token")
        expire_at = body.get("expire_at")
        if not isinstance(token, str) or not isinstance(expire_at, (int, float)):
            raise RTZRAuthError("RTZR authentication returned an unexpected payload")
        self._token = token
        self._expire_at = float(expire_at)
