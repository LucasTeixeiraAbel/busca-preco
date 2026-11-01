"""Small Telegram Bot API client that avoids logging token-bearing URLs."""
from __future__ import annotations

import logging
from typing import Any

import httpx

# Bot API tokens are part of request paths; HTTPX info logs would expose them.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


async def call(client: httpx.AsyncClient, token: str, method: str, payload: dict[str, Any]) -> Any:
    try:
        response = await client.post(f"https://api.telegram.org/bot{token}/{method}", json=payload)
        response.raise_for_status()
        result = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise RuntimeError(f"Telegram API {method} request failed") from None
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API {method} returned error {result.get('error_code', 'unknown')}")
    return result.get("result")
