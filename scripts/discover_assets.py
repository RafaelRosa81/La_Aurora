import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import websocket

WS_URL_DEFAULT = "wss://telemetry.nettra.tech/api/ws"


def load_headers(headers_json_path: str) -> List[str]:
    headers = json.loads(Path(headers_json_path).read_text(encoding="utf-8"))
    # websocket-client espera lista "Key: value"
    return [f"{k}: {v}" for k, v in headers.items()]


def extract_assets_from_response(obj: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """
    Formato confirmado por vos:
    {"cmdId":..., "data":{"data":[{"entityId":{"entityType":"ASSET","id":"..."},
                                  "latest":{"ENTITY_FIELD":{"label":{"value":"..."},
                                                           "name":{"value":"..."}}}} ...],
                         "hasNext":false}}
    """
    out: List[Tuple[str, str, str]] = []

    data_block = obj.get("data") or {}
    rows = data_block.get("data") or []
    if not isinstance(rows, list):
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue
        entity = row.get("entityId") or {}
        if not isinstance(entity, dict):
            continue
        if entity.get("entityType") != "ASSET":
            continue
        asset_id = entity.get("id")
        if not isinstance(asset_id, str) or not asset_id:
            continue

        label = None
        name = None

        latest = row.get("latest") or {}
        if isinstance(latest, dict):
            ef = latest.get("ENTITY_FIELD") or {}
            if isinstance(ef, dict):
                lab = ef.get("label")
                nam = ef.get("name")
                if isinstance(lab, dict):
                    label = lab.get("value")
                if isinstance(nam, dict):
                    name = nam.get("value")

        if not isinstance(name, str) or not name:
            name = label if isinstance(label, str) else asset_id
        if not isinstance(label, str) or not label:
            label = name

        out.append((asset_id, label, name))

    return out


def connect(ws_url: str, header_list: List[str], timeout: int = 30):
    ws = websocket.create_connection(ws_url, header=header_list, timeout=timeout)
    # IMPORTANT: set timeout for recv so we don't hang forever
    ws.settimeout(3)
    return ws


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--headers-json", default="config/headers.json")
    ap.add_argument("--ws-url", default=WS_URL_DEFAULT)
    ap.add_argument("--asset-type", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--page-size", type=int, default=100)  # más chico => menos chance de corte
    ap.add_argument("--max-pages", type=int, default=20)
    ap.add_argument("--retries", type=int, default=3)
    args = ap.parse_args()

    header_list = load_headers(args.headers_json)

    assets: Dict[str, Tuple[str, str]] = {}  # id -> (label, name)

    # Usamos cmdId fijo por página para simplificar matching (cmdId cambia con page)
    for page in range(args.max_pages):
        cmd_id = 1000 + page  # único por página

        query = {
            "cmds": [
                {
                    "type": "ENTITY_DATA",
                    "cmdId": cmd_id,
                    "query": {
                        "entityFilter": {
                            "type": "assetType",
                            "resolveMultiple": True,
                            "assetNameFilter": "",
                            "assetTypes": [args.asset_type],
                        },
                        "pageLink": {
                            "page": page,
                            "pageSize": args.page_size,
                            "textSearch": None,
                            "dynamic": True,
                            # En tu respuesta real, el nombre venía en ENTITY_FIELD,
                            # así que pedimos ordenar por name como ENTITY_FIELD.
                            "sortOrder": {
                                "key": {"key": "name", "type": "ENTITY_FIELD"},
                                "direction": "ASC",
                            },
                        },
                        # Mantenerlo MINIMAL para que el servidor no corte
                        "entityFields": [
                            {"type": "ENTITY_FIELD", "key": "name"},
                            {"type": "ENTITY_FIELD", "key": "label"},
                        ],
                        "latestValues": [],
                    },
                }
            ]
        }

        # Reintentos por si el servidor corta el socket
        last_has_next = None
        got_response = False

        for attempt in range(1, args.retries + 1):
            try:
                ws = connect(args.ws_url, header_list)
                print("✅ Conectado al WS")
                ws.send(json.dumps(query))
                print(f"📤 Query assetType='{args.asset_type}' page={page} (cmdId={cmd_id}) attempt={attempt}")

                t0 = time.time()
                while time.time() - t0 < 8:
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue  # seguimos esperando dentro del tiempo total
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                    except Exception:
                        continue

                    if obj.get("cmdId") != cmd_id:
                        continue

                    got_response = True
                    for aid, label, name in extract_assets_from_response(obj):
                        assets[aid] = (label, name)

                    last_has_next = (obj.get("data") or {}).get("hasNext")
                    # si hasNext viene explícito, ya terminamos esta página
                    if isinstance(last_has_next, bool):
                        break

                ws.close()
                print("✅ WS cerrado")

                if got_response:
                    break  # salimos del loop de reintentos

            except websocket.WebSocketConnectionClosedException:
                print("⚠️ WS se cerró inesperadamente. Reintentando...")
            except Exception as e:
                print(f"⚠️ Error WS: {e}. Reintentando...")
            finally:
                try:
                    ws.close()
                except Exception:
                    pass

        if not got_response:
            print("⚠️ No llegó respuesta para esta página. Corto aquí.")
            break

        # si no hay más páginas, terminamos
        if last_has_next is False:
            break

    if not assets:
        print("⚠️ No se extrajo ningún asset. Revisa el assetType.")
        return

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    lines = ["asset_label,asset_id,asset_name,asset_type"]
    for aid, (label, name) in sorted(assets.items(), key=lambda x: x[1][0].lower()):
        label_q = '"' + str(label).replace('"', '""') + '"'
        name_q = '"' + str(name).replace('"', '""') + '"'
        type_q = '"' + str(args.asset_type).replace('"', '""') + '"'
        lines.append(f"{label_q},{aid},{name_q},{type_q}")

    out_csv.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Guardado: {out_csv} ({len(assets)} assets)")


if __name__ == "__main__":
    main()
