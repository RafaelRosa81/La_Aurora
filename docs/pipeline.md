# Pipeline (fases 0–4B)

```mermaid
flowchart TD
    A[Config .env + export_plan.yaml] --> B[FASE 0: export_monthly_rest.py]
    B --> C[output/datos_def/<grupo>/<asset>/<asset>_YYYY-MM.csv]
    C --> D[FASE 1: validate_integrity.py]
    C --> E[FASE 2: find_outliers*.py]
    E --> F[reports/cleaning_plan_<grupo>_*.csv]
    F --> G[FASE 3: clean_*.py]
    G --> H[output/datos_clean/<grupo>/...]
    H --> I[FASE 4: analyze_*.py]
    I --> J[reports/<tipo>_*.xlsx]
    H --> K[FASE 4A: analyze_cross_assets.py]
    K --> L[reports/fase4_cross_*.xlsx]
    L --> M[FASE 4B: analyze_cross_assets_dashboard.py]
    M --> N[reports/fase4_dashboard_*.xlsx]
```

## Notas

- **Fase 0** usa REST y plan YAML para definir targets/keys.
- **Fase 2** genera el plan de limpieza que **Fase 3** consume.
- **Fase 4A/4B** dependen de `output/datos_clean` con subcarpetas por grupo.

> **Asunción**: se ejecutan las fases en orden; los scripts no validan dependencias implícitas.
