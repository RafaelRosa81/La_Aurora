# Overview

Este paquete cubre la **bajada de datos** de telemetría y un **pipeline de análisis** por fases (0–4B).

## Objetivo

- Exportar telemetría a CSV.
- Validar integridad y detectar outliers.
- Limpiar datasets y generar reportes Excel por tipo de activo y cross-asset.

## Fases (resumen)

- **Fase 0**: export REST a CSV (`scripts/export_monthly_rest.py`).
- **Fase 1**: validación de integridad (`analysis/validate_integrity.py`).
- **Fase 2**: outliers y plan de limpieza (`analysis/find_outliers*.py`).
- **Fase 3**: limpieza (`analysis/clean_*.py`).
- **Fase 4**: análisis por activo (`analysis/analyze_*.py`).
- **Fase 4A/4B**: cross-asset y dashboard (`analysis/analyze_cross_assets*.py`).

## Outputs clave

- Raw: `output/datos_def/<grupo>/<asset>/<asset>_YYYY-MM.csv`
- Limpios: `output/datos_clean/<grupo>/...`
- Reportes: `reports/*.xlsx`

> **Asunción**: la estructura `output/datos_def` y `output/datos_clean` se mantiene como en los scripts actuales.
