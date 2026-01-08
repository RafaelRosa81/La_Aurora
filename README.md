# La Aurora Telemetry

Herramientas para exportar telemetría desde Nettra/ThingsBoard y generar reportes de análisis.

## Requisitos

- Python 3.10+

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> Nota Windows: activar el entorno virtual con `\.venv\Scripts\activate`.

## Configuración

1) Copia el archivo de ejemplo:

```bash
cp config/example.env .env
```

2) Completa `.env` con credenciales válidas:

- `TB_BASE_URL` (ej: `https://telemetry.nettra.tech`)
- `TB_TOKEN` (token de ThingsBoard)
- `TB_TIMEZONE` (zona horaria IANA usada por el dashboard)

3) Revisa el plan de exportación:

- `config/export_plan.yaml` define `timezone`, `output_dir` y `targets`.

## Quickstart (end-to-end)

> Asunción: ya existe `.env` y el plan YAML tiene los targets correctos.

### 1) Exportar datos mensuales (Fase 0)

```bash
python scripts/export_monthly_rest.py --start-ym 2024-01 --end-ym 2024-03 --resume
```

Outputs esperados:

```
output/datos_def/<grupo>/<asset>/<asset>_YYYY-MM.csv
```

### 2) Validar integridad temporal (Fase 1)

```bash
python analysis/validate_integrity.py --input-dir output/datos_def/estanques --group estanques --all --start-date 2024-01-01 --end-date 2024-03-31
```

Output:

```
reports/integrity_estanques_<timestamp>.xlsx
```

### 3) Detectar outliers + plan de limpieza (Fase 2)

```bash
python analysis/find_outliers.py --input-dir output/datos_def/estanques --group estanques --all --pct-min 0.5 --pct-max 99.5
```

Outputs:

```
reports/outliers_estanques_<timestamp>.xlsx
reports/cleaning_plan_estanques_<timestamp>.csv
```

### 4) Limpiar datos (Fase 3)

```bash
python analysis/clean_levels.py --plan reports/cleaning_plan_estanques_<timestamp>.csv --input-dir output/datos_def/estanques --output-dir output/datos_clean/estanques
```

Outputs:

```
output/datos_clean/estanques/<asset>/<asset>_YYYY-MM.csv
```

### 5) Análisis por tipo de activo (Fase 4)

```bash
python analysis/analyze_tanks.py --input-dir output/datos_clean/estanques --all --start-date 2024-01-01 --end-date 2024-03-31
```

Outputs:

```
reports/tanks_all_<timestamp>.xlsx
reports/tanks_<asset>_<timestamp>.xlsx
```

### 6) Análisis cross-asset (Fase 4A) y dashboard (Fase 4B)

```bash
python analysis/analyze_cross_assets.py --input-dir output/datos_clean --freq-minutes 1
python analysis/analyze_cross_assets_dashboard.py --input reports/fase4_cross_<timestamp>.xlsx
```

Outputs:

```
reports/fase4_cross_<timestamp>.xlsx
reports/fase4_dashboard_<timestamp>.xlsx
```

> Nota Windows: los ejemplos usan `/`. En Windows PowerShell se pueden usar rutas con `\`.

## Troubleshooting

- **`ModuleNotFoundError: requests`**
  - Ejecuta `pip install -r requirements.txt`.
- **`HTTP 401 (Unauthorized)` al exportar**
  - Revisa `TB_TOKEN` en `.env`.
- **Timezone incorrecta en CSVs**
  - Asegura que `TB_TIMEZONE` y `config/export_plan.yaml: timezone` sean correctos.
- **Scripts de `tools/dev` fallan por `config/headers.json`**
  - Son utilidades internas y esperan un archivo local con headers de autenticación.
  
## Documentación

La documentación del pipeline (MkDocs) está en la carpeta `docs/`.

- Ver documentación en GitHub: `docs/`
- Quickstart: `docs/quickstart.md`
- Pipeline: `docs/pipeline.md`

### Ver en local

```bash
python -m pip install -r docs/requirements.txt
python -m mkdocs serve -f mkdocs.yml