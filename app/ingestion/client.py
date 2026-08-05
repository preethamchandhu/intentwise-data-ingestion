import asyncio
import logging
from typing import Any, Dict, Optional, Tuple

import httpx

from app.schemas.ingestion import AuthConfig, AuthType

logger = logging.getLogger(__name__)


class IngestionClient:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        timeout: float = 30.0
    ):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout

    def _prepare_headers_and_params(
        self,
        custom_headers: Dict[str, str],
        custom_params: Dict[str, Any],
        auth: AuthConfig
    ) -> Tuple[Dict[str, str], Dict[str, Any]]:
        headers = {"User-Agent": "Intentwise-Generic-Ingestion/1.0"}
        headers.update(custom_headers)
        params = dict(custom_params)

        if auth and auth.type != AuthType.NONE:
            if not auth.value:
                raise ValueError(f"auth.type is '{auth.type.value}' but auth.value was not provided")
            if auth.type == AuthType.BEARER:
                headers["Authorization"] = f"Bearer {auth.value}"
            elif auth.type == AuthType.API_KEY_HEADER:
                if not auth.key:
                    raise ValueError("auth.type is 'api_key_header' but auth.key was not provided")
                headers[auth.key] = auth.value
            elif auth.type == AuthType.API_KEY_QUERY:
                if not auth.key:
                    raise ValueError("auth.type is 'api_key_query' but auth.key was not provided")
                params[auth.key] = auth.value

        return headers, params

    async def fetch_page(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        auth: Optional[AuthConfig] = None
    ) -> Tuple[Any, Dict[str, str]]:
        """Fetch one page with retry/backoff. Returns (parsed_json, response_headers)."""
        req_headers, req_params = self._prepare_headers_and_params(
            headers or {}, params or {}, auth or AuthConfig()
        )

        last_exception = None
        for attempt in range(1, self.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=req_headers,
                        params=req_params
                    )

                if response.status_code in (429, 500, 502, 503, 504):
                    retry_after = response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after and retry_after.isdigit() else (self.backoff_factor * (2 ** (attempt - 1)))
                    logger.warning(
                        f"attempt {attempt}/{self.max_retries} got {response.status_code} for {url}, retrying in {delay}s"
                    )
                    await asyncio.sleep(delay)
                    continue

                response.raise_for_status()
                json_data = response.json()
                resp_headers = dict(response.headers)
                return json_data, resp_headers

            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_exception = exc
                delay = self.backoff_factor * (2 ** (attempt - 1))
                logger.warning(f"attempt {attempt}/{self.max_retries} failed for {url}: {exc}, retrying in {delay}s")
                if attempt < self.max_retries:
                    await asyncio.sleep(delay)

        raise RuntimeError(f"failed to fetch {url} after {self.max_retries} attempts: {last_exception}")
