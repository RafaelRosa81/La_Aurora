# La Aurora Telemetry

Herramientas para exportar telemetría desde un websocket hacia archivos CSV.

## Requisitos

- Python 3.10+

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuración

1. Copia el archivo de ejemplo y completa las variables necesarias:

```bash
cp config/example.env .env
```

2. Edita `.env` con los valores de tu entorno (sin compartir credenciales).

3. El parámetro TB_TIMEZONE debe coincidir con el timeZoneId
utilizado por el dashboard (ver DevTools → Network → WS → Messages).
No necesariamente coincide con la ubicación física de los sensores.

## Uso

Ejecuta el exportador usando el módulo dentro de `src/`:

```bash
PYTHONPATH=src ./scripts/export_telemetry.py
```

Los archivos CSV se guardarán en el directorio indicado por `OUTPUT_DIR`.

Uso principal: export_monthly_rest.py

Uso principal: export_monthly_rest.py