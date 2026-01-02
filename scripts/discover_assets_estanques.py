import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import websocket

WS_URL = "wss://telemetry.nettra.tech/api/ws"
ASSET_TYPE = "La Aurora - Estanques"

OUT_CSV = Path("config/assets_estanques.csv")


def load_headers(headers_json_path: str) -> List[str]:
    headers = json.loads(Path(headers_json_path).read_text(encoding="utf-8"))
    return [f"{k}: {v}" for k, v in headers.items()]


def safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def extract_assets_from_msg(obj: Any) -> List[Tuple[str, str]]:
    """
    Intenta extraer (asset_id, asset_name) desde respuestas ENTITY_DATA.
    ThingsBoard suele devolver filas bajo data / data.data / data[...]
    """
    out = []

    def walk(x: Any):
        if isinstance(x, dict):
            # algunos formatos: {"data":{"data":[{"entityId":{"id":"..."}, ...}]}}
            if "entityId" in x and isinstance(x["entityId"], dict) and "id" in x["entityId"]:
                asset_id = x["entityId"]["id"]
                name = x.get("name") or x.get("label")
                # a veces el nombre viene en "latest" o "entityName"
                name = name or x.get("entityName")
                if isinstance(asset_id, str) and isinstance(name, str):
                    out.append((asset_id, name))
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)
    return out


def main():
    headers = load_headers("config/headers.json")
    ws = websocket.create_connection(WS_URL, header=headers, timeout=30)
    print("✅ Conectado al WS")

    cmd_id = 1
    query = {
        "cmds": [
            {
                "cmdId": cmd_id,
                "type": "ENTITY_DATA",
                "query": {
                    "entityFilter": {
                        "type": "assetType",
                        "resolveMultiple": True,
                        "assetNameFilter": "",
                        "assetTypes": [ASSET_TYPE],
                    },
                    "pageLink": {
                        "page": 0,
                        "pageSize": 200,
                        "textSearch": None,
                        "dynamic": True,
                        "sortOrder": {
                            "key": {"key": "name", "type": "ENTITY_FIELD"},
                            "direction": "ASC",
                        },
                    },
                    "entityFields": [
                        {"type": "ENTITY_FIELD", "key": "name"},
                        {"type": "ENTITY_FIELD", "key": "type"},
                    ],
                    "latestValues": [],
                },
            }
        ]
    }

    ws.send(json.dumps(query))
    print(f"📤 Query enviado para assetType='{ASSET_TYPE}'")

    assets = {}  # id -> name
    t0 = time.time()
    while time.time() - t0 < 12:
        try:
            raw = ws.recv()
        except Exception:
            break
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        # filtro suave: si viene cmdId=1 o trae data relevante
        for asset_id, name in extract_assets_from_msg(obj):
            assets[asset_id] = name

        # algunos payloads traen "data":{"hasNext":false}
        has_next = safe_get(obj, "data", "hasNext")
        if has_next is False:
            break

    ws.close()
    print("✅ WS cerrado")

    if not assets:
        print("⚠️ No se extrajeron assets. Si pasa, te pido un mensaje WS de ejemplo.")
        return

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    lines = ["asset_id,asset_label,asset_type"]
    for aid, name in sorted(assets.items(), key=lambda x: x[1].lower()):
        # escape simple por comas
        name_clean = '"' + name.replace('"', '""') + '"'
        lines.append(f"{aid},{name_clean},\"{ASSET_TYPE}\"")

    OUT_CSV.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Guardado: {OUT_CSV} ({len(assets)} assets)")


if __name__ == "__main__":
    main()
