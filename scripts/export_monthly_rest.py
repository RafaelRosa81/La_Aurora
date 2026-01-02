# scripts/export_monthly_rest.py
import os
import csv
import calendar
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = os.environ["TB_BASE_URL"].rstrip("/")
TOKEN = os.environ["TB_TOKEN"].strip()
TZ = os.environ.get("TB_TIMEZONE", "America/Montevideo")

HEADERS = {"X-Authorization": f"Bearer {TOKEN}"}

ASSET_TYPES = {
    "estanques": "La Aurora - Estanques",
    "bombas": "La Aurora - Bombas",
}

# Ajustá estas keys a tus series reales (las vemos rápido en la UI o con un endpoint)
KEYS = {
    "estanques": ["nivelPorcentual", "nivelEstanque"],      # ejemplo
    "bombas":    ["estadoOn", "timeOn"],   # ejemplo
}

OUTDIR = "output/monthly"

def ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def month_ranges(start_ym: str, end_ym: str):
    # start_ym / end_ym formato "YYYY-MM"
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        last_day = calendar.monthrange(y, m)[1]
        start = datetime(y, m, 1, 0, 0, 0)
        end = datetime(y, m, last_day, 23, 59, 59)
        yield y, m, start, end
        m += 1
        if m == 13:
            m = 1
            y += 1

def list_assets(asset_type: str, page_size=100):
    # 1) obtener customerId
    me = requests.get(f"{BASE}/api/auth/user", headers=HEADERS, timeout=30)
    me.raise_for_status()
    customer_id = me.json()["customerId"]["id"]

    assets = []
    page = 0

    while True:
        url = f"{BASE}/api/customer/{customer_id}/assets"
        params = {
            "pageSize": page_size,
            "page": page,
            "type": asset_type,
        }

        r = requests.get(url, headers=HEADERS, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        assets.extend(data.get("data", []))

        if not data.get("hasNext"):
            break

        page += 1

    return assets


def fetch_timeseries(asset_id: str, keys: list[str], start_ts: int, end_ts: int):
    url = f"{BASE}/api/plugins/telemetry/ASSET/{asset_id}/values/timeseries"
    params = {
        "keys": ",".join(keys),
        "startTs": start_ts,
        "endTs": end_ts,
        "agg": "NONE",
        "limit": 50000,
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=120)
    r.raise_for_status()
    return r.json()

def write_csv(path, asset, y, m, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Normalizamos: una fila por timestamp, columnas por key
    # payload: { key: [ {ts:..., value:...}, ...], ... }
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

def main():

    print("Estanques:", len(list_assets("La Aurora - Estanques")))
    print("Bombas:", len(list_assets("La Aurora - Bombas")))
    #return

    start_ym = "2024-01"
    end_ym   = "2025-12"

    for group, a_type in ASSET_TYPES.items():
        assets = list_assets(a_type)
        print(f"{group}: {len(assets)} assets")

        for asset in assets:
            asset_id = asset["id"]["id"]
            label = asset.get("label") or asset.get("name") or asset_id

            for y, m, start_dt, end_dt in month_ranges(start_ym, end_ym):
                payload = fetch_timeseries(asset_id, KEYS[group], ms(start_dt), ms(end_dt))
                out = os.path.join(OUTDIR, group, f"{label}", f"{label}_{y:04d}-{m:02d}.csv")
                write_csv(out, asset, y, m, payload)
                print("OK", out)

if __name__ == "__main__":
    main()
