# Auditoría (resumen)

## Resumen ejecutivo

- El pipeline está operativo pero disperso entre `scripts/`, `analysis/` y `tools/dev`.
- Falta consistencia en CLI, naming de outputs y configuración canónica.
- README era incompleto; se incorporaron quick wins para documentación base.

## Checklist de quick wins

- [x] Agregar `requests` a `requirements.txt`.
- [x] Corregir `analysis/find_outliers.py --help` (texto con `%`).
- [x] Corregir textos de help en macro caudalímetro.
- [x] Actualizar README con quickstart y troubleshooting.
- [x] Base de documentación con MkDocs + Mermaid.
- [ ] Unificar naming de outputs (ALL vs all) de forma consistente.
- [ ] Definir un archivo de configuración canónico y deprecaciones.

## Próximos pasos sugeridos

- Documentar convenciones de naming y outputs en una sección única.
- Agregar validaciones de precondición entre fases (inputs mínimos).

> **Asunción**: las fases y scripts listados provienen del estado actual del repo.
