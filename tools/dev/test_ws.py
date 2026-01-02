import json
from pathlib import Path
import websocket

WS_URL = "wss://telemetry.nettra.tech/api/ws"

headers = json.loads(Path("config/headers.json").read_text(encoding="utf-8"))

# websocket-client requiere lista de strings "Key: value"
header_list = [f"{k}: {v}" for k, v in headers.items()]

ws = websocket.create_connection(WS_URL, header=header_list, timeout=20)

print("✅ Conectado al WS")

# (Opcional) mandar un ping simple si el server lo soporta:
# ws.send('{"type":"ping"}')

ws.close()
print("✅ Cerrado")
