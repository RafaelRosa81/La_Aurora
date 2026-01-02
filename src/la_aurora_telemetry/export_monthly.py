import json
import os
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from la_aurora_telemetry.time_utils import (
    month_range,
    parse_date,
    to_epoch_ms,
)


def export_timeseries_monthly(
    client,
    asset_id: str,
    asset_label: str,
    keys: Iterable[str],
    start_date: str,
    end_date: str,
    out_dir: Path,
    interval_ms: int = 60_000,  # 1 minuto
    timezone: str = "America/Montevideo",
    limit: int = 100_000,
):
    """
    Exporta series temporales mes a mes para un asset y guarda CSVs.

    client: TelemetryWSClient ya conectado
    asset_id: ID del asset en ThingsBoard
    asset_label: nombre legible (para carpetas)
    keys: lista de keys de telemetría
    start_date, end_date: strings YYYY-MM-DD
    out_dir: carpeta base de salida
    """

    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    asset_dir = out_dir / asset_label
    asset_dir.mkdir(parents=True, exist_ok=True)

    for month_start, month_end in month_range(start_dt, end_dt):
        start_ms = to_epoch_ms(month_start)
        end_ms = to_epoch_ms(month_end)

        month_tag = f"{month_start.year}_{month_start.month:02d}"
        out_csv = asset_dir / f"{month_tag}.csv"

        print(f"[INFO] {asset_label} → {month_tag}")

        cmd = {
            "cmdId": 1,
            "entityType": "ASSET",
            "entityId": asset_id,
            "keys": ",".join(keys),
            "startTs": start_ms,
            "endTs": end_ms,
            "interval": interval_ms,
            "limit": limit,
            "agg": "NONE",
            "timeZoneId": timezone,
        }

        send_obj = {
            "cmds": [
                {
                    "type": "ENTITY_DATA",
                    "tsCmd": cmd,
                }
            ]
        }

        rows = []

        def done_predicate(msg):
            return msg.get("cmdId") == 1 and "data" in msg

        messages = client.request_response(
            send_obj,
            expect_predicate=done_predicate,
            timeout=60,
        )

        for msg in messages:
            data = msg.get("data", {})
            for key, points in data.items():
                for ts, value in points:
                    rows.append(
                        {
                            "timestamp_ms": ts,
                            "key": key,
                            "value": value,
                        }
                    )

        if not rows:
            print(f"[WARN] Sin datos para {asset_label} {month_tag}")
            continue

        df = pd.DataFrame(rows)
        df["datetime"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        df.to_csv(out_csv, index=False)

        print(f"[OK] Guardado {out_csv}")


