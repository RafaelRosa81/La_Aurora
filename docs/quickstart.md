# Quickstart

> **Asunción**: `.env` existe y `config/export_plan.yaml` tiene los targets correctos.

## 1) Exportar (Fase 0)

```bash
python scripts/export_monthly_rest.py --start-ym 2024-01 --end-ym 2024-03 --resume
```

## 2) Validar integridad (Fase 1)

```bash
python analysis/validate_integrity.py --input-dir output/datos_def/estanques --group estanques --all --start-date 2024-01-01 --end-date 2024-03-31
```

## 3) Outliers y plan de limpieza (Fase 2)

```bash
python analysis/find_outliers.py --input-dir output/datos_def/estanques --group estanques --all --pct-min 0.5 --pct-max 99.5
```

## 4) Limpieza (Fase 3)

```bash
python analysis/clean_levels.py --plan reports/cleaning_plan_estanques_<timestamp>.csv --input-dir output/datos_def/estanques --output-dir output/datos_clean/estanques
```

## 5) Análisis por activo (Fase 4)

```bash
python analysis/analyze_tanks.py --input-dir output/datos_clean/estanques --all --start-date 2024-01-01 --end-date 2024-03-31
```

## 6) Cross-asset + Dashboard (Fase 4A/4B)

```bash
python analysis/analyze_cross_assets.py --input-dir output/datos_clean --freq-minutes 1
python analysis/analyze_cross_assets_dashboard.py --input reports/fase4_cross_<timestamp>.xlsx
```

## Nota Windows

Los ejemplos usan `/`. En Windows PowerShell se puede usar `\`.
