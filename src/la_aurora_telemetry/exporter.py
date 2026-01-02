"""Telemetry export helpers."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from dateutil.tz import gettz
import pandas as pd

from .config import TelemetryConfig


def export_telemetry(config: TelemetryConfig) -> Path:
    """Create a CSV file placeholder for telemetry data.

    This is a minimal export that timestamps the run. Extend this function to
    connect to the websocket, stream data, and build a dataframe.
    """

    config.output_dir.mkdir(parents=True, exist_ok=True)
    timezone = gettz(config.timezone)
    timestamp = datetime.now(tz=timezone).isoformat()

    data = [{"timestamp": timestamp, "ws_url": config.ws_url}]
    frame = pd.DataFrame(data)

    output_path = config.output_dir / f"telemetry_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    frame.to_csv(output_path, index=False)

    return output_path
