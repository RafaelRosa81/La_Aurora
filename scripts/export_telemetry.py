#!/usr/bin/env python3
"""Export telemetry data from the La Aurora websocket."""

from __future__ import annotations

import sys

from dotenv import load_dotenv

from la_aurora_telemetry import TelemetryConfig, export_telemetry


def main() -> int:
    load_dotenv()
    try:
        config = TelemetryConfig.from_env()
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    output_path = export_telemetry(config)
    print(f"Telemetry exported to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
