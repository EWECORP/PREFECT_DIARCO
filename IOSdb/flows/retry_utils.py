from __future__ import annotations

from typing import Any

from IOSdb.clients.api_client import get_api_client
from IOSdb.config.settings import load_settings


def get_token() -> str:
    return get_api_client().get_token()


def send_batch(batch: Any, token: str | None = None, url: str | None = None) -> None:
    del token
    client = get_api_client()
    payload = batch if isinstance(batch, list) else [batch]
    if not url:
        url = load_settings().api.stock_url
    client.post_json(url, payload)
