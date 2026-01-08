# Notas técnicas para documentación futura

## Puntos clave para el doc técnico
- **Entrada principal**: CSVs generados por `scripts/export_monthly_rest.py` en `output/datos_def/<grupo>/<asset>/...`.
- **Flujo recomendado**: export → validate_integrity → find_outliers → clean → analyze → cross_assets → dashboard.
- **Estructura esperada**: los scripts de análisis asumen subcarpetas por grupo dentro de `output/datos_clean/`.

## Esquema de datos observado (CSV)
- Columnas comunes observadas en los scripts:
  - `timestamp`/`ts`/`datetime`/`FechaHora` (scripts buscan varias variantes).
  - `asset_label`/`asset`/`assetName`/`estanque`/`bomba` (mapeo en `make_samples.py`).
  - Niveles y presión: `nivelPorcentual`, `nivelEstanque`, `presion`, `caudal`, `volumenAgua` (según targets del plan).

## Inputs/outputs por fase (sugerencia de doc)
- **Fase 0**: `config/export_plan.yaml` + `.env` → `output/datos_def/...`.
- **Fase 1**: `output/datos_def/...` → `reports/integrity_<grupo>_*.xlsx`.
- **Fase 2**: `output/datos_def/...` → `reports/outliers_<grupo>_*.xlsx` + `reports/cleaning_plan_<grupo>_*.csv`.
- **Fase 3**: `reports/cleaning_plan_<grupo>_*.csv` + `output/datos_def/...` → `output/datos_clean/...`.
- **Fase 4**: `output/datos_clean/...` → `reports/<tipo>_*.xlsx`.
- **Fase 4A/4B**: `reports/fase4_cross_*.xlsx` → `reports/fase4_dashboard_*.xlsx`.

## Notas de CLI
- Estándar recomendado (para doc y futuros cambios):
  - `--input-dir`/`--output-dir` en todos los scripts que operan sobre carpetas.
  - `--group` opcional si ya está implícito en el path.
  - Siempre incluir ejemplos en `--help`.

## Temas para troubleshooting
- `requests` faltante bloquea los scripts REST.
- `config/headers.json` requerido por utilidades WS en `tools/dev/`.
- Timestamp inválido en CSVs (ver warnings en scripts de outliers/validación).
- Ajustes de timezone: `export_plan.yaml` vs `.env`.