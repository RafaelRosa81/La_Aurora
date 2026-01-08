# Cookbook

Recetas rápidas para explorar los datos con el pipeline existente.

## Reprocesar un asset específico (estanques)

```bash
python analysis/find_outliers.py --input-dir output/datos_def/estanques --group estanques --asset "Principal"
python analysis/clean_levels.py --plan reports/cleaning_plan_estanques_<timestamp>.csv --input-dir output/datos_def/estanques --output-dir output/datos_clean/estanques
python analysis/analyze_tanks.py --input-dir output/datos_clean/estanques --asset "Principal" --start-date 2024-01-01 --end-date 2024-03-31
```

## Analizar bombas con reportes por asset

```bash
python analysis/analyze_pumps.py --input-dir output/datos_clean/bombas --all --per-asset --start-date 2024-01-01 --end-date 2024-03-31
```

## Generar muestras reducidas para compartir

```bash
python analysis/make_samples.py --input output/datos_def/estanques/Principal/Principal_2024-01.csv --out-dir data_samples/estanques --group estanques --max-rows 5000
```

> **Asunción**: los CSVs tienen columnas de timestamp compatibles con los scripts (`timestamp`, `ts`, `datetime`, etc.).
