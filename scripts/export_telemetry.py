import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

from la_aurora_telemetry.ws_client import TelemetryWSClient
from la_aurora_telemetry.export_monthly import export_timeseries_monthly


def read_assets_csv(csv_path: Path):
    """
    Lee CSV con columnas:
    asset_label,asset_id
    """
    assets = []
    with open(csv_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.lower().startswith("asset_label"):
                continue
            label, asset_id = line.split(",", 1)
            assets.append((label.strip(), asset_id.strip()))
    return assets


def main():
    parser = argparse.ArgumentParser(description="Export telemetry to monthly CSVs")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2025-12-31")
    parser.add_argument("--assets-estanques", default="config/assets_estanques.csv")
    parser.add_argument("--assets-bombas", default="config/assets_bombas.csv")
    parser.add_argument("--keys-estanques", default="nivelPorcentual,nivelEstanque")
    parser.add_argument("--keys-bombas", default="")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--interval-ms", type=int, default=60_000)

    args = parser.parse_args()

    load_dotenv()

    ws_url = os.getenv("WS_URL")
    auth_cookie = os.getenv("AUTH_COOKIE")
    auth_header = os.getenv("AUTH_HEADER")

    if not ws_url:
        raise RuntimeError("WS_URL no está definido en .env")

    out_base = Path(args.output_dir)
    out_estanques = out_base / "estanques"
    out_bombas = out_base / "bombas"

    client = TelemetryWSClient(
        ws_url=ws_url,
        auth_cookie=auth_cookie,
        auth_header=auth_header,
    )

    client.connect()

    try:
        # ---- ESTANQUES ----
        estanques = read_assets_csv(Path(args.assets_estanques))
        keys_est = [k.strip() for k in args.keys_estanques.split(",") if k.strip()]

        for label, asset_id in estanques:
            export_timeseries_monthly(
                client=client,
                asset_id=asset_id,
                asset_label=label,
                keys=keys_est,
                start_date=args.start_date,
                end_date=args.end_date,
                out_dir=out_estanques,
                interval_ms=args.interval_ms,
            )

        # ---- BOMBAS ----
        bombas = read_assets_csv(Path(args.assets_bombas))
        keys_bomb = [k.strip() for k in args.keys_bombas.split(",") if k.strip()]

        if not keys_bomb:
            print("[WARN] No se definieron keys para bombas, se omite exportación.")
        else:
            for label, asset_id in bombas:
                export_timeseries_monthly(
                    client=client,
                    asset_id=asset_id,
                    asset_label=label,
                    keys=keys_bomb,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    out_dir=out_bombas,
                    interval_ms=args.interval_ms,
                )

    finally:
        client.close()


if __name__ == "__main__":
    main()
