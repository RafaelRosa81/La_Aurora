#!/usr/bin/env bash
set -euo pipefail

status=0

check_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "[FAIL] Missing file: $path"
    status=1
  else
    echo "[OK] Found file: $path"
  fi
}

check_dir() {
  local path="$1"
  if [[ ! -d "$path" ]]; then
    mkdir -p "$path"
    echo "[OK] Created dir: $path"
  else
    echo "[OK] Dir exists: $path"
  fi
}

check_help() {
  local script="$1"
  echo "[INFO] Checking --help for $script"
  if ! python "$script" --help >/dev/null 2>&1; then
    echo "[FAIL] --help failed for $script"
    status=1
  else
    echo "[OK] --help OK for $script"
  fi
}

check_file "config/export_plan.yaml"
check_file "config/example.env"
check_file "README.md"
check_file "mkdocs.yml"

check_dir "output"
check_dir "reports"

# Scripts productivos (sin credenciales)
check_help "scripts/export_monthly_rest.py"
check_help "scripts/list_assets.py"
check_help "scripts/list_asset_keys.py"

check_help "analysis/validate_integrity.py"
check_help "analysis/find_outliers.py"
check_help "analysis/find_outliers_presion.py"
check_help "analysis/find_outliers_macro_caudalimetro.py"
check_help "analysis/find_outliers_pumps.py"
check_help "analysis/clean_levels.py"
check_help "analysis/clean_presion.py"
check_help "analysis/clean_macro_caudalimetro.py"
check_help "analysis/clean_pumps.py"
check_help "analysis/analyze_tanks.py"
check_help "analysis/analyze_pumps.py"
check_help "analysis/analyze_presion.py"
check_help "analysis/analyze_macro_caudalimetro.py"
check_help "analysis/analyze_cross_assets.py"
check_help "analysis/analyze_cross_assets_dashboard.py"
check_help "analysis/make_samples.py"

if [[ "$status" -ne 0 ]]; then
  echo "[FAIL] Smoke test detected issues."
  exit 1
fi

echo "[OK] Smoke test completed successfully."
