# scripts/export_monthly_rest.py
import argparse
import os
import csv
import calendar
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # Python 3.9+

import requests
import yaml
from dotenv import load_dotenv


# ----------------------------
# Config (YAML export plan)
# ----------------------------

DEFAULT_PLAN_PATH = "config/export_plan.yaml"


@dataclass(frozen=True)
class ExportTarget:
    name: str
    asset_type: str
    keys: list[str]


@dataclass(frozen=True)
class ExportPlan:
    timezone: str
    output_dir: str
    targets: list[ExportTarget]


def load_export_plan(path: str) -> ExportPlan:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Config inválido: {path}")

    tz_name = data.get("timezone")
    out_dir = data.get("output_dir")
    targets_data = data.get("targets")

    if not tz_name or not out_dir or not isinstance(targets_data, list) or not targets_data:
        raise ValueError(f"Config incompleto: {path} (requiere timezone, output_dir, targets[])")

    targets: list[ExportTarget] = []
    for entry in targets_data:
        if not isinstance(entry, dict):
            raise ValueError(f"Target inválido en {path}: {entry}")
        name = entry.get("name")
        asset_type = entry.get("asset_type")
        keys = entry.get("keys")
        if not name or not asset_type or not isinstance(keys, list) or not keys:
            raise ValueError(f"Target incompleto en {path}: {entry}")
        # normalizar keys a strings
        keys = [str(k).strip() for k in keys if str(k).strip()]
        if not keys:
            raise ValueError(f"Target sin keys en {path}: {entry}")
        targets.append(ExportTarget(name=str(name).strip(), asset_type=str(asset_type).strip(), keys=keys))

    return ExportPlan(timezone=str(tz_name).strip(), output_dir=str(out_dir).strip(), targets=targets)


# ----------------------------
# Helpers
# ----------------------------

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
    Rangos mensuales definidos en tz_local.
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


def parse_targets_list(s: str | None):
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


# ----------------------------
# REST calls
# ----------------------------

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

    if r.status_code == 401:
        raise RuntimeError(
            "HTTP 401 (Unauthorized). TB_TOKEN vencido/incorrecto.\n"
            "→ Actualiza TB_TOKEN en .env y reintenta.\n"
            "→ Sugerencia: usa --resume para retomar sin sobreescribir.\n"
            f"URL: {r.url}"
        )

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


# ----------------------------
# CLI
# ----------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Export mensual a CSV (REST) con targets definidos en un plan YAML.",
    )
    p.add_argument("--start-ym", required=True, help="Inicio YYYY-MM (ej: 2024-01)")
    p.add_argument("--end-ym", required=True, help="Fin YYYY-MM (ej: 2025-12)")
    p.add_argument("--config", default=DEFAULT_PLAN_PATH, help="Ruta al plan YAML (default: config/export_plan.yaml)")
    p.add_argument(
        "--targets",
        default=None,
        help='Nombres de targets a exportar (separados por coma). Si se omite, exporta todos.',
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
    args = parse_args()

    plan = load_export_plan(args.config)

    # Cargar .env y variables
    load_dotenv()
    base = os.environ["TB_BASE_URL"].rstrip("/")
    token = os.environ["TB_TOKEN"].strip()
    headers = {"X-Authorization": f"Bearer {token}"}

    # TZ para definir rangos mensuales (desde YAML)
    tz_name = plan.timezone
    tz_local = ZoneInfo(tz_name)
    print(f"Using timezone: {tz_name}")

    # filtros
    only_set = parse_only_list(args.only)
    target_set = parse_targets_list(args.targets)

    # customerId 1 vez
    customer_id = get_customer_id(base, headers)

    for target in plan.targets:
        if target_set and target.name not in target_set:
            continue

        assets = list_assets(base, headers, customer_id, target.asset_type)
        print(f"{target.name}: {len(assets)} assets (type='{target.asset_type}')")

        for asset in assets:
            if not asset_matches(asset, only_set):
                continue

            asset_id = asset["id"]["id"]
            label = asset.get("label") or asset.get("name") or asset_id
            safe_label = sanitize(label)

            for y, m, start_dt, end_dt in month_ranges(args.start_ym, args.end_ym, tz_local):
                out = os.path.join(
                    plan.output_dir,
                    target.name,
                    safe_label,
                    f"{safe_label}_{y:04d}-{m:02d}.csv",
                )

                if args.resume and (not args.overwrite) and file_exists_and_nonempty(out):
                    print("SKIP", out)
                    continue

                payload = fetch_timeseries(base, headers, asset_id, target.keys, ms(start_dt), ms(end_dt))
                write_csv(out, asset, y, m, payload)
                print("OK", out)


if __name__ == "__main__":
    main()
