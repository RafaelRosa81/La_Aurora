# scripts/export_monthly_rest.py
import argparse
import os
import csv
import calendar
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # Python 3.9+

import requests
from dotenv import load_dotenv


ASSET_TYPES = {
    "estanques": "La Aurora - Estanques",
    "bombas": "La Aurora - Bombas",
}

KEYS = {
    "estanques": ["nivelPorcentual", "nivelEstanque"],
    "bombas": ["estadoOn", "timeOn"],
}


def sanitize(s: str) -> str:
    """Seguro para Windows."""
    s = (s or "").strip()
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]+', "_", s)
    s = re.sub(r"\s+", " ", s)
    return s


def ms(dt: datetime) -> int:
    """
    Convierte datetime timezone-aware a epoch ms (UTC).
    Importante: dt DEBE tener tzinfo.
    """
    if dt.tzinfo is None:
        raise ValueError("ms() requiere datetime con tzinfo (timezone-aware).")
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def month_ranges(start_ym: str, end_ym: str, tz_local: ZoneInfo):
    """
    Rangos mensuales definidos en tz_local (ej. America/Santiago).
    Retorna datetimes timezone-aware en tz_local.
    """
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        last_day = calendar.monthrange(y, m)[1]

        # 00:00:00 del primer día EN HORA LOCAL
        start = datetime(y, m, 1, 0, 0, 0, tzinfo=tz_local)

        # 23:59:59 del último día EN HORA LOCAL
        end = datetime(y, m, last_day, 23, 59, 59, tzinfo=tz_local)

        yield y, m, start, end

        m += 1
        if m == 13:
            m = 1
            y += 1


def get_customer_id(base: str, headers: dict) -> str:
    me = requests.get(f"{base}/api/auth/user", headers=headers, timeout=30)
    me.raise_for_status()
    return me.json()["customerId"]["id"]


def list_assets(base: str, headers: dict, customer_id: str, asset_type: str, page_size=100):
    assets = []
    page = 0
    while True:
        url = f"{base}/api/customer/{customer_id}/assets"
        params = {"pageSize": page_size, "page": page, "type": asset_type}
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        assets.extend(data.get("data", []))
        if not data.get("hasNext"):
            break
        page += 1
    return assets


def fetch_timeseries(base: str, headers: dict, asset_id: str, keys: list[str], start_ts: int, end_ts: int):
    url = f"{base}/api/plugins/telemetry/ASSET/{asset_id}/values/timeseries"
    params = {
        "keys": ",".join(keys),
        "startTs": start_ts,
        "endTs": end_ts,
        "agg": "NONE",
        "limit": 50000,
    }
    r = requests.get(url, headers=headers, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def write_csv(path, asset, y, m, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Normalizamos: una fila por timestamp (ts en ms UTC), columnas por key
    rows = {}
    for k, series in payload.items():
        for p in series:
            ts = p.get("ts")
            rows.setdefault(ts, {})[k] = p.get("value")

    fieldnames = ["asset_id", "asset_name", "asset_label", "year", "month", "ts"] + sorted(payload.keys())

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for ts in sorted(rows.keys()):
            row = {
                "asset_id": asset["id"]["id"],
                "asset_name": asset.get("name", ""),
                "asset_label": asset.get("label", ""),
                "year": y,
                "month": m,
                "ts": ts,
            }
            row.update(rows[ts])
            w.writerow(row)

def file_exists_and_nonempty(path: str) -> bool:
    """True si el archivo existe y tiene contenido."""
    try:
        return os.path.isfile(path) and os.path.getsize(path) > 0
    except OSError:
        return False


def parse_only_list(s: str | None):
    if not s:
        return None
    parts = [p.strip() for p in s.split(",")]
    return {p for p in parts if p}


def asset_matches(asset: dict, only_set: set[str] | None) -> bool:
    if not only_set:
        return True
    name = (asset.get("name") or "").strip()
    label = (asset.get("label") or "").strip()
    return (name in only_set) or (label in only_set)


def parse_args():
    p = argparse.ArgumentParser(
        description="Export mensual a CSV (REST) para estanques y bombas (rangos definidos en zona horaria local).",
    )
    p.add_argument("--start-ym", required=True, help="Inicio YYYY-MM (ej: 2024-01)")
    p.add_argument("--end-ym", required=True, help="Fin YYYY-MM (ej: 2025-12)")
    p.add_argument("--outdir", default="output/monthly", help="Directorio de salida")
    p.add_argument(
        "--groups",
        nargs="+",
        choices=list(ASSET_TYPES.keys()),
        default=list(ASSET_TYPES.keys()),
        help="Qué exportar",
    )
    p.add_argument(
        "--only",
        default=None,
        help='Lista separada por comas de assets a exportar (match por label o name). Ej: "3A - 3B,Principal"',
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Salta meses ya descargados (CSV existente y no vacío).",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Reescribe archivos existentes (ignora --resume).",
    )
    return p.parse_args()


def main():
    # CLI/help NO toca red: solo parsea args y sale si corresponde
    args = parse_args()

    # Cargar .env y variables
    load_dotenv()

    base = os.environ["TB_BASE_URL"].rstrip("/")
    token = os.environ["TB_TOKEN"].strip()
    headers = {"X-Authorization": f"Bearer {token}"}

    # Zona horaria local para definir los rangos (Chile por defecto)
    tz_name = os.environ.get("TB_TIMEZONE", "America/Santiago")
    tz_local = ZoneInfo(tz_name)
    print(f"Using timezone: {tz_name}")

    only_set = parse_only_list(args.only)

    # customerId 1 vez
    customer_id = get_customer_id(base, headers)

    for group in args.groups:
        a_type = ASSET_TYPES[group]
        keys = KEYS[group]

        assets = list_assets(base, headers, customer_id, a_type)
        print(f"{group}: {len(assets)} assets")

        for asset in assets:
            if not asset_matches(asset, only_set):
                continue

            asset_id = asset["id"]["id"]
            label = asset.get("label") or asset.get("name") or asset_id
            safe_label = sanitize(label)
            '''
            for y, m, start_dt, end_dt in month_ranges(args.start_ym, args.end_ym, tz_local):
                payload = fetch_timeseries(base, headers, asset_id, keys, ms(start_dt), ms(end_dt))
                out = os.path.join(args.outdir, group, safe_label, f"{safe_label}_{y:04d}-{m:02d}.csv")
                write_csv(out, asset, y, m, payload)
                print("OK", out)
            '''
            for y, m, start_dt, end_dt in month_ranges(args.start_ym, args.end_ym, tz_local):
                out = os.path.join(
                    args.outdir,
                    group,
                    safe_label,
                    f"{safe_label}_{y:04d}-{m:02d}.csv",
                )

                if args.resume and (not args.overwrite) and file_exists_and_nonempty(out):
                    print("SKIP", out)
                    continue

                payload = fetch_timeseries(
                    base,
                    headers,
                    asset_id,
                    keys,
                    ms(start_dt),
                    ms(end_dt),
                )
                write_csv(out, asset, y, m, payload)
                print("OK", out)


if __name__ == "__main__":
    main()
