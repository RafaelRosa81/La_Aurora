import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import websocket

WS_URL_DEFAULT = "wss://telemetry.nettra.tech/api/ws"


def load_headers(headers_json_path: str) -> List[str]:
    headers = json.loads(Path(headers_json_path).read_text(encoding="utf-8"))
    return [f"{k}: {v}" for k, v in headers.items()]


def load_token(token_path: str) -> str:
    return Path(token_path).read_text(encoding="utf-8").strip()


def extract_assets_from_response(obj: Dict[str, Any]) -> List[Tuple[str, str, str]]:
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

        latest = row.get("latest") or {}
        ef = (latest.get("ENTITY_FIELD") or {}) if isinstance(latest, dict) else {}

        label = None
        name = None
        if isinstance(ef, dict):
            if isinstance(ef.get("label"), dict):
                label = ef["label"].get("value")
            if isinstance(ef.get("name"), dict):
                name = ef["name"].get("value")

        if not isinstance(name, str) or not name:
            name = label if isinstance(label, str) else asset_id
        if not isinstance(label, str) or not label:
            label = name

        out.append((asset_id, label, name))

    return out


def connect(ws_url: str, headers: List[str]) -> websocket.WebSocket:
    ws = websocket.create_connection(ws_url, header=headers, timeout=30)
    ws.settimeout(3)
    return ws


def ws_send(ws: websocket.WebSocket, payload: Dict[str, Any]):
    ws.send(json.dumps(payload))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--headers-json", default="config/headers.json")
    ap.add_argument("--token-path", default="config/auth_token.txt")
    ap.add_argument("--ws-url", default=WS_URL_DEFAULT)
    ap.add_argument("--asset-type", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--retries", type=int, default=3)
    args = ap.parse_args()

    headers = load_headers(args.headers_json)
    token = load_token(args.token_path)

    cmd_id = 1

    # 1) authCmd SOLO (handshake)
    auth_msg = {"authCmd": {"cmdId": 0, "token": token}}

    # 2) query (clon del dashboard)
    query_msg = {
        "cmds": [
            {
                "type": "ENTITY_DATA",
                "query": {
                    "entityFilter": {
                        "type": "assetType",
                        "resolveMultiple": True,
                        "assetTypes": [args.asset_type],
                        "assetNameFilter": ""
                    },
                    "pageLink": {
                        "page": 0,
                        "pageSize": args.page_size,
                        "textSearch": None,
                        "dynamic": True,
                        "sortOrder": {
                            "key": {"key": "ordenTabla", "type": "ATTRIBUTE"},
                            "direction": "ASC"
                        }
                    },
                    "entityFields": [
                        {"type": "ENTITY_FIELD", "key": "label"},
                        {"type": "ENTITY_FIELD", "key": "name"},
                        {"type": "ENTITY_FIELD", "key": "additionalInfo"}
                    ],
                    "latestValues": [
                        {"type": "ATTRIBUTE", "key": "ordenTabla"}
                    ]
                },
                "latestCmd": {"keys": [{"type": "ATTRIBUTE", "key": "ordenTabla"}]},
                "cmdId": cmd_id
            }
        ]
    }

    last_error = None

    for attempt in range(1, args.retries + 1):
        ws = None
        try:
            ws = connect(args.ws_url, headers)
            print(f"✅ Conectado al WS (attempt={attempt})")

            # AUTH primero
            ws_send(ws, auth_msg)
            print("🔐 Enviado authCmd, esperando respuesta...")

            # Esperar breve respuesta/ACK (o al menos que no cierre)
            t_auth = time.time()
            auth_ok = False
            while time.time() - t_auth < 5:
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    # no necesariamente hay ACK, pero si no cerró, seguimos
                    auth_ok = True
                    break
                if not raw:
                    continue
                # algunos servers devuelven algo como {"errorCode":0} o {"cmdId":0,...}
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue

                # Si hay error explícito
                if obj.get("errorCode") not in (None, 0):
                    raise RuntimeError(f"Auth errorCode={obj.get('errorCode')} msg={obj.get('errorMsg')}")
                # Si vemos cmdId 0 o algo relacionado, lo tomamos como ok
                if obj.get("cmdId") == 0 or obj.get("cmdUpdateType") == "AUTH":
                    auth_ok = True
                    break

            if not auth_ok:
                # si no hubo nada pero no cerró, igual probamos query
                auth_ok = True

            # QUERY después
            ws_send(ws, query_msg)
            print(f"📤 Enviado listado assetType='{args.asset_type}'")

            assets = {}
            t0 = time.time()
            while time.time() - t0 < 12:
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue

                if obj.get("cmdId") != cmd_id:
                    continue

                for aid, label, name in extract_assets_from_response(obj):
                    assets[aid] = (label, name)

                has_next = (obj.get("data") or {}).get("hasNext")
                if has_next is False:
                    break

                # Si viene error explícito
                if obj.get("errorCode") not in (None, 0):
                    raise RuntimeError(f"Query errorCode={obj.get('errorCode')} msg={obj.get('errorMsg')}")

            ws.close()

            if not assets:
                raise RuntimeError("No se extrajeron assets (token expirado o assetType incorrecto).")

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
            return

        except websocket.WebSocketConnectionClosedException:
            last_error = "WS cerrado por el servidor (token inválido/expirado o auth fuera de orden)."
            print(f"⚠️ {last_error}")
        except Exception as e:
            last_error = str(e)
            print(f"⚠️ Error: {last_error}")
        finally:
            try:
                if ws:
                    ws.close()
            except Exception:
                pass

    print("\n❌ Falló tras reintentos.")
    print("Causa más probable: el token en config/auth_token.txt ya expiró o no corresponde a esta sesión.")
    print("Solución: refrescá el token desde DevTools → ws → Messages (authCmd.token) y reemplazalo en auth_token.txt.")
    if last_error:
        print(f"Detalle: {last_error}")


if __name__ == "__main__":
    main()
