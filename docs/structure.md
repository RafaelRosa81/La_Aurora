# Estructura del repositorio

```
.
├─ analysis/                # Limpieza, validación y análisis (CSV/Excel)
├─ config/                  # Configs (env, export plan)
├─ data_samples/            # Muestras de datos
├─ docs/                    # Documentación MkDocs
├─ scripts/                 # Scripts productivos (REST, inventarios)
├─ src/                     # Librería interna (WS helpers)
├─ tools/                   # Utilidades dev y smoke test
├─ AUDIT_REPORT.md          # Auditoría completa
├─ README.md                # Guía rápida
└─ requirements.txt         # Dependencias runtime
```

## Carpetas clave

- `scripts/`: scripts principales para exportar y listar assets.
- `analysis/`: pipeline de validación, outliers, limpieza y análisis.
- `output/`: carpeta de datos generados (se crea al ejecutar).
- `reports/`: reportes Excel generados (se crea al ejecutar).

> **Asunción**: `output/` y `reports/` no existen en repo pero se crean en runtime.
