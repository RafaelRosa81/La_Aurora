"""WebSocket client helpers for telemetry dashboard."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable

from websocket import (
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
    create_connection,
)

logger = logging.getLogger("la_aurora_telemetry")
if logger.level == logging.NOTSET:
    logger.setLevel(logging.INFO)


def _redact(value: str | None) -> str | None:
    if value is None:
        return None
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


class TelemetryWSClient:
    """Robust websocket client for telemetry dashboard."""

    def __init__(
        self,
        ws_url: str,
        auth_cookie: str | None = None,
        auth_header: str | None = None,
        extra_headers: dict | None = None,
        connect_timeout: int = 20,
    ) -> None:
        self.ws_url = ws_url
        self.auth_cookie = auth_cookie
        self.auth_header = auth_header
        self.extra_headers = extra_headers or {}
        self.connect_timeout = connect_timeout
        self._socket = None

    def _build_headers(self) -> list[str]:
        headers: list[str] = []
        if self.auth_cookie:
            headers.append(f"Cookie: {self.auth_cookie}")
        if self.auth_header:
            headers.append(f"Authorization: {self.auth_header}")
        for key, value in self.extra_headers.items():
            headers.append(f"{key}: {value}")
        return headers

    def connect(self) -> None:
        """Open the websocket connection with retries."""
        if self._socket:
            return

        retries = 3
        backoffs = [1, 2, 4]
        headers = self._build_headers()
        auth_cookie_hint = _redact(self.auth_cookie)
        auth_header_hint = _redact(self.auth_header)

        for attempt in range(retries):
            try:
                logger.info(
                    "Connecting to telemetry websocket (attempt %s). auth_cookie=%s auth_header=%s",
                    attempt + 1,
                    auth_cookie_hint,
                    auth_header_hint,
                )
                self._socket = create_connection(
                    self.ws_url,
                    header=headers,
                    timeout=self.connect_timeout,
                )
                logger.info("Telemetry websocket connected.")
                return
            except Exception as exc:  # noqa: BLE001 - surface retry details
                logger.warning("Websocket connection failed: %s", exc)
                if attempt < retries - 1:
                    time.sleep(backoffs[attempt])

        raise ConnectionError("Unable to connect to telemetry websocket after retries.")

    def close(self) -> None:
        """Close the websocket connection safely."""
        if not self._socket:
            return
        try:
            self._socket.close()
        finally:
            self._socket = None

    def send_json(self, obj: dict) -> None:
        if not self._socket:
            raise ConnectionError("Websocket is not connected.")
        payload = json.dumps(obj)
        self._socket.send(payload)

    def recv_json(self, timeout: int = 30) -> dict | None:
        if not self._socket:
            raise ConnectionError("Websocket is not connected.")
        self._socket.settimeout(timeout)
        while True:
            try:
                raw = self._socket.recv()
            except WebSocketTimeoutException:
                return None
            except WebSocketConnectionClosedException:
                return None
            if raw is None:
                return None
            try:
                return json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue

    def request_response(
        self,
        send_obj: dict,
        expect_predicate: Callable[[dict], bool],
        timeout: int = 30,
    ) -> list[dict]:
        if not self._socket:
            raise ConnectionError("Websocket is not connected.")
        messages: list[dict] = []
        self.send_json(send_obj)
        deadline = time.monotonic() + timeout
        while True:
            remaining = max(0, int(deadline - time.monotonic()))
            if remaining == 0:
                return messages
            message = self.recv_json(timeout=remaining)
            if message is None:
                return messages
            messages.append(message)
            if expect_predicate(message):
                return messages


if __name__ == "__main__":
    ws_url = os.getenv("WS_URL", "")
    auth_cookie = os.getenv("AUTH_COOKIE")
    auth_header = os.getenv("AUTH_HEADER")

    client = TelemetryWSClient(
        ws_url=ws_url,
        auth_cookie=auth_cookie,
        auth_header=auth_header,
    )
    client.connect()
    print("Connected OK")
    client.close()
