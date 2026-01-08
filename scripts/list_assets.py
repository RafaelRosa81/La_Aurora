# scripts/list_assets.py
import os
import csv
import argparse
import requests
from dotenv import load_dotenv


def get_customer_id(base: str, headers: dict) -> str:
    r = requests.get(f"{base}/api/auth/user", headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["customerId"]["id"]


def list_assets(base: str, headers: dict, customer_id: str, asset_type: str | None = None, page_size: int = 1000):
    out = []
    page = 0
    while True:
        url = f"{base}/api/customer/{customer_id}/assets"
        params = {"pageSize": page_size, "page": page}
        if asset_type:
            params["type"] = asset_type

        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("data", []))
        if not data.get("hasNext"):
            break
        page += 1
    return out


def main():
    ap = argparse.ArgumentParser(description="Listar assets (id, name, label, type) desde Nettra/ThingsBoard.")
    ap.add_argument("--type", default=None, help='Filtrar por asset_type exacto (ej: "La Aurora - Estanques")')
    ap.add_argument("--out", default="output/assets_list.csv", help="CSV de salida")
    args = ap.parse_args()

    load_dotenv()
    base = os.environ["TB_BASE_URL"].rstrip("/")
    token = os.environ["TB_TOKEN"].strip()
    headers = {"X-Authorization": f"Bearer {token}"}

    customer_id = get_customer_id(base, headers)
    assets = list_assets(base, headers, customer_id, asset_type=args.type)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["asset_id", "name", "label", "type"])
        for a in assets:
            w.writerow([
                a["id"]["id"],
                a.get("name", ""),
                a.get("label", ""),
                a.get("type", ""),
            ])

    print(f"OK: {len(assets)} assets -> {args.out}")


if __name__ == "__main__":
    main()
