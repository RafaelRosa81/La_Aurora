import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import websocket


WS_URL_DEFAULT = "wss://telemetry.nettra.tech/api/ws"


def load_headers(headers_json_path: str) -> List[str]:
    headers = json.loads(Path(headers_json_path).read_text(encoding="utf-8"))
    # websocket-client espera lista "Key: value"
    return [f"{k}: {v}" for k, v in headers.items()]


def try_parse_json(line: str) -> Any:
    try:
        return json.loads(line)
    except Exception:
        return None


def extract_asset_types_from_text(text: str) -> Set[str]:
    """
    Busca patrones "assetTypes":[...]
    Funciona aunque el mensaje sea grande o venga anidado.
    """
    found: Set[str] = set()
    # captura el array JSON luego de "assetTypes":
    for m in re.finditer(r'"assetTypes"\s*:\s*(\[[^\]]*\])', text):
        arr_txt = m.group(1)
        try:
            arr = json.loads(arr_txt)
            for x in arr:
                if isinstance(x, str) and x.strip():
                    found.add(x.strip())
        except Exception:
            pass
    return found


def extract_assets_with_types(obj: Any) -> List[Tuple[str, str]]:
    """
    Plan B: intenta encontrar activos y su 'type' si el backend lo devuelve.
    Devuelve lista (asset_name, asset_type).
    """
    out: List[Tuple[str, str]] = []

    def walk(x: Any):
        if isinstance(x, dict):
            # Algunas respuestas traen {"data":[{"entityType":"ASSET", ...}]}
            if x.get("entityType") == "ASSET":
                name = None
                atype = None
                # posibles lugares
                name = x.get("name") or x.get("label")
                atype = x.get("type") or x.get("assetType")
                if isinstance(name, str) and isinstance(atype, str):
                    out.append((name, atype))

            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)
    return out


def main():
    headers_path = "config/headers.json"
    ws_url = WS_URL_DEFAULT

    header_list = load_headers(headers_path)

    ws = websocket.create_connection(ws_url, header=header_list, timeout=30)
    print("✅ Conectado al WS")

    # Consulta tipo ENTITY_DATA (estilo lo que viste en DevTools)
    # Nota: algunos servidores aceptan assetTypes vacío => devuelve todo
    # Si el backend requiere assetTypes, igual nos sirve porque muchas veces
    # responde con estructuras que incluyen assetTypes o activos.
    query_msg: Dict[str, Any] = {
        "cmds": [
            {
                "type": "ENTITY_DATA",
                "query": {
                    "entityFilter": {
                        "type": "assetType",
                        "resolveMultiple": True,
                        "assetNameFilter": "",
                        "assetTypes": []
                    },
                    "pageLink": {
                        "page": 0,
                        "pageSize": 1000,
                        "textSearch": None,
                        "dynamic": True,
                        "sortOrder": {
                            "key": {"key": "name", "type": "ENTITY_FIELD"},
                            "direction": "ASC"
                        }
                    },
                    "entityFields": [
                        {"type": "ENTITY_FIELD", "key": "name"},
                        {"type": "ENTITY_FIELD", "key": "type"}
                    ],
                    "latestValues": []
                }
            }
        ]
    }

    ws.send(json.dumps(query_msg))
    print("📤 Enviado ENTITY_DATA (assetTypes=[]) ... esperando respuestas")

    asset_types: Set[str] = set()
    assets_with_types: Set[Tuple[str, str]] = set()

    t0 = time.time()
    timeout_sec = 12  # suficiente para respuestas típicas

    while time.time() - t0 < timeout_sec:
        try:
            msg = ws.recv()
        except Exception:
            break

        if not msg:
            continue

        # 1) extraer assetTypes por texto
        asset_types |= extract_asset_types_from_text(msg)

        # 2) intentar parsear json para plan B
        obj = try_parse_json(msg)
        if obj is not None:
            for pair in extract_assets_with_types(obj):
                assets_with_types.add(pair)

    ws.close()
    print("✅ WS cerrado")

    # Reporte
    if asset_types:
        print("\n=== Asset types encontrados (por 'assetTypes') ===")
        for s in sorted(asset_types):
            print("-", s)

    if assets_with_types:
        print("\n=== Asset types deducidos desde activos devueltos (name, type) ===")
        # imprimir únicos por type
        types2 = sorted({t for _, t in assets_with_types})
        for t in types2:
            print("-", t)

    if not asset_types and not assets_with_types:
        print(
            "\n⚠️ No se pudieron extraer assetTypes con este query.\n"
            "Esto puede pasar si el backend exige un assetTypes específico.\n"
            "Plan rápido: abrí DevTools -> Network -> ws -> Messages en Estanques,\n"
            "copiá UNA línea que contenga \"assetTypes\":[...], pegala en un archivo\n"
            "y yo te adapto el script para usar esa lista directamente."
        )


if __name__ == "__main__":
    main()
