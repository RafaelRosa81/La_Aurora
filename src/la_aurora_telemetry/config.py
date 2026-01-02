"""Configuration loader for telemetry exports."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from dateutil.tz import gettz


@dataclass(frozen=True)
class TelemetryConfig:
    ws_url: str
    auth_cookie: str | None
    auth_header: str | None
    output_dir: Path
    timezone: str

    @classmethod
    def from_env(cls) -> "TelemetryConfig":
        ws_url = os.environ.get("WS_URL", "").strip()
        auth_cookie = os.environ.get("AUTH_COOKIE")
        auth_header = os.environ.get("AUTH_HEADER")
        output_dir = Path(os.environ.get("OUTPUT_DIR", "output")).expanduser()
        timezone = os.environ.get("TIMEZONE", "UTC")

        if not ws_url:
            raise ValueError("WS_URL is required; set it in the environment or .env file.")

        if gettz(timezone) is None:
            raise ValueError(
                "TIMEZONE is invalid; provide an IANA timezone like America/Montevideo."
            )

        return cls(
            ws_url=ws_url,
            auth_cookie=auth_cookie,
            auth_header=auth_header,
            output_dir=output_dir,
            timezone=timezone,
        )
