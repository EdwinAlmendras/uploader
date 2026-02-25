"""HTTP adapter for uploader API operations."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import httpx


class HTTPAPIClient:
    """
    HTTP client adapter for API calls.

    Implements IAPIClient protocol.
    """

    def __init__(self, base_url: str, timeout: int = 60):
        self._base_url = base_url
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    async def post(self, endpoint: str, json: Dict) -> Any:
        if not self._client:
            raise RuntimeError("HTTPAPIClient not initialized. Use 'async with' context.")

        max_retries = 3
        last_exception = None

        for attempt in range(max_retries):
            try:
                response = await self._client.post(endpoint, json=json)

                if response.status_code >= 500 and attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue

                if response.status_code >= 400:
                    try:
                        error_detail = response.json()
                    except Exception:
                        error_detail = response.text
                    raise RuntimeError(
                        f"API error {response.status_code} on POST {endpoint}: {error_detail}"
                    )

                return response
            except (httpx.RequestError, httpx.TimeoutException) as exc:
                last_exception = exc
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                raise

        if last_exception:
            raise last_exception
        raise RuntimeError(f"Failed to POST {endpoint} after {max_retries} attempts")

    async def get(self, endpoint: str) -> Any:
        if not self._client:
            raise RuntimeError("HTTPAPIClient not initialized. Use 'async with' context.")

        max_retries = 3
        last_exception = None

        for attempt in range(max_retries):
            try:
                response = await self._client.get(endpoint)

                if response.status_code >= 500 and attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue

                if response.status_code >= 400:
                    try:
                        error_detail = response.json()
                    except Exception:
                        error_detail = response.text
                    raise RuntimeError(
                        f"API error {response.status_code} on GET {endpoint}: {error_detail}"
                    )

                return response
            except (httpx.RequestError, httpx.TimeoutException) as exc:
                last_exception = exc
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                raise

        if last_exception:
            raise last_exception
        raise RuntimeError(f"Failed to GET {endpoint} after {max_retries} attempts")
