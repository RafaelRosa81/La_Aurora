# Troubleshooting

## Errores comunes

- **`ModuleNotFoundError: requests`**
  - Ejecuta `pip install -r requirements.txt`.
- **`HTTP 401 (Unauthorized)` en export**
  - Revisa `TB_TOKEN` en `.env`.
- **Timezone incorrecta en CSVs**
  - Alinea `TB_TIMEZONE` y `config/export_plan.yaml: timezone`.
- **Scripts en `tools/dev` fallan por `config/headers.json`**
  - Son utilidades internas y esperan un archivo local con headers de autenticación.
- **`analysis/find_outliers.py --help` falla**
  - Asegura estar en la versión que corrige el texto con `%`.
