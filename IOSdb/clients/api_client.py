from __future__ import annotations

import threading
from typing import Any, Optional

import requests

from IOSdb.config.settings import IOSApiSettings, load_settings


class IOSApiClient:
    def __init__(self, settings: IOSApiSettings) -> None:
        self._settings = settings
        self._session = requests.Session()
        self._token: Optional[str] = None
        self._lock = threading.Lock()

    def get_token(self, force_refresh: bool = False) -> str:
        with self._lock:
            if self._token and not force_refresh:
                return self._token

            response = self._session.post(
                self._settings.login_url,
                json={
                    "username": self._settings.username,
                    "company": self._settings.company,
                    "password": self._settings.password,
                },
                timeout=self._settings.login_timeout_seconds,
            )
            response.raise_for_status()

            token = response.json().get("token_api", "").strip()
            if not token:
                raise RuntimeError("La autenticacion IOS no devolvio token_api")

            self._token = token
            return token

    def post_json(
        self,
        url: str,
        payload: list[dict[str, Any]],
        timeout_seconds: Optional[int] = None,
    ) -> None:
        token = self.get_token()
        headers = {"Authorization": f"Bearer {token}"}

        response = self._session.post(
            url,
            json=payload,
            headers=headers,
            timeout=timeout_seconds or self._settings.request_timeout_seconds,
        )

        if response.status_code == 401:
            token = self.get_token(force_refresh=True)
            headers["Authorization"] = f"Bearer {token}"
            response = self._session.post(
                url,
                json=payload,
                headers=headers,
                timeout=timeout_seconds or self._settings.request_timeout_seconds,
            )

        response.raise_for_status()


_client: Optional[IOSApiClient] = None
_client_lock = threading.Lock()


def get_api_client() -> IOSApiClient:
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = IOSApiClient(load_settings().api)
    return _client
