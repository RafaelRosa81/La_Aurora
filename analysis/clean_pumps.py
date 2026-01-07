# Funciona en ambiente la_aurora
"""
clean_pumps.py

Aplica el plan generado por find_outliers_pumps.py para limpiar (DROP) filas puntuales.

Características clave:
- NO borra “meses”: elimina solo filas específicas por (file, asset_id, row_index).
- Preserva estructura de carpetas: output_dir / ruta_relativa_respecto_a input_dir
- Robusto a:
  - plan con rutas relativas al repo
  - CSVs que no tienen asset_label
  - asset_label contaminado (valores tipo hora) -> se ignora y se usa fallback (carpeta/prefijo)
  
python analysis\clean_pumps.py --plan reports\cleaning_plan_bombas_YYYYMMDD_HHMMSS.csv --input-dir output\datos_def\bombas --output-dir output\datos_clean\bombas

"""

import argparse
from pathlib import Path
import re

import numpy as np
import pandas as pd


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora"]
STATE_CANDIDATES = ["estadoon", "state", "on", "ison", "run", "running"]
TIMEON_CANDIDATES = ["timeon", "tiempoon", "seconds_on", "segundoson", "runtime", "runseconds"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aplica un plan de limpieza (drop) para bombas preservando estructura de carpetas."
    )
    parser.add_argument("--plan", required=True, help="CSV plan (find_outliers_pumps.py)")
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Carpeta base de entrada con CSVs (ej: output\\datos_def\\bombas)",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Carpeta base de salida (ej: output\\datos_clean\\bombas)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No escribe archivos, solo reporta qué haría",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Limita la cantidad de archivos procesados (debug)",
    )
    return parser.parse_args()


def parse_timestamp(series: pd.Series) -> tuple[pd.Series, str]:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        median_value = float(numeric.dropna().median())
        if median_value >= 1e14:
            return pd.to_datetime(numeric, errors="coerce", unit="us"), "epoch_us"
        if median_value >= 1e11:
            return pd.to_datetime(numeric, errors="coerce", unit="ms"), "epoch_ms"
        if median_value >= 1e8:
            return pd.to_datetime(numeric, errors="coerce", unit="s"), "epoch_s"
    return pd.to_datetime(series, errors="coerce"), "string"


def find_col_by_candidates(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c in lower_map:
            return lower_map[c]
    return None


def find_timestamp_column(df: pd.DataFrame) -> str | None:
    return find_col_by_candidates(df, TIMESTAMP_CANDIDATES)


def looks_like_time_or_date(series: pd.Series) -> bool:
    """
    Si la mayoría de valores del asset_label se parsean como datetime/hora,
    probablemente está contaminado (ej '07:30:00') y NO debe usarse como asset_id.
    """
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return False
    sample = s.head(5000)
    parsed = pd.to_datetime(sample, errors="coerce", infer_datetime_format=True)
    return float(parsed.notna().mean()) >= 0.80


def determine_asset_series(df: pd.DataFrame, file_path: Path) -> pd.Series:
    """
    Orden:
    - asset_label (si existe y no parece hora)
    - sino nombre de carpeta padre
    - sino prefijo del archivo
    """
    lower_map = {col.lower(): col for col in df.columns}
    folder_name = file_path.parent.name
    prefix = file_path.stem.split("_")[0]
    fallback = folder_name or prefix or "asset"

    asset_col = lower_map.get("asset_label")
    if asset_col:
        series = df[asset_col]
        if series.notna().any():
            if looks_like_time_or_date(series):
                return pd.Series([fallback] * len(df), index=df.index)
            s = series.astype(str).str.strip()
            s = s.where(s != "", fallback)
            return s

    return pd.Series([fallback] * len(df), index=df.index)


def normalize_plan_file_to_path(file_value: str, repo_root: Path) -> Path:
    """
    En el plan, 'file' suele venir como path relativo al repo.
    Lo resolvemos contra repo_root.
    """
    p = Path(str(file_value))
    if p.is_absolute():
        return p
    return repo_root / p


def relative_from_input(input_dir: Path, file_path: Path) -> Path:
    """
    Devuelve la ruta relativa de file_path respecto a input_dir.
    Si no se puede, fallback al nombre del archivo.
    """
    try:
        return file_path.resolve().relative_to(input_dir.resolve())
    except Exception:
        return Path(file_path.name)


def write_output_csv(output_path: Path, df: pd.DataFrame) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[info] Limpio: {output_path}")


def safe_int(x) -> int | None:
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        return int(x)
    except Exception:
        return None


def main() -> None:
    args = parse_args()
    plan_path = Path(args.plan)
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not plan_path.exists():
        raise SystemExit(f"[error] plan no existe: {plan_path}")
    if not input_dir.exists():
        raise SystemExit(f"[error] input-dir no existe: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    repo_root = Path(__file__).resolve().parents[1]

    # --- leer plan ---
    try:
        plan_df = pd.read_csv(plan_path)
    except Exception as exc:
        raise SystemExit(f"[error] No se pudo leer plan: {exc}") from exc

    required = {"file", "asset_id", "action", "match", "row_index"}
    missing = required - set(plan_df.columns)
    if missing:
        raise SystemExit(f"[error] Plan incompleto, faltan columnas: {sorted(missing)}")

    plan_df = plan_df.copy()
    plan_df["action"] = plan_df["action"].astype(str).str.lower().str.strip()
    plan_df["match"] = plan_df["match"].astype(str).str.lower().str.strip()

    # Nos quedamos con reglas drop por row_index
    plan_df = plan_df[(plan_df["action"] == "drop") & (plan_df["match"] == "row_index")]

    # Normalizar row_index a int
    plan_df["row_index"] = plan_df["row_index"].apply(safe_int)
    plan_df = plan_df[plan_df["row_index"].notna()].copy()
    plan_df["row_index"] = plan_df["row_index"].astype(int)

    if plan_df.empty:
        print("[warning] El plan no tiene reglas drop por row_index. Copiando archivos sin cambios.")
        # Igual copiamos todo preservando estructura
        csv_files = sorted(input_dir.rglob("*.csv"))
        if args.max_files is not None:
            csv_files = csv_files[: args.max_files]
        for fp in csv_files:
            try:
                df = pd.read_csv(fp)
            except Exception as exc:
                print(f"[warning] No se pudo leer {fp}: {exc}")
                continue
            rel = relative_from_input(input_dir, fp)
            out_path = output_dir / rel
            if args.dry_run:
                print(f"[dry-run] Escribiría: {out_path}")
            else:
                write_output_csv(out_path, df)
        print(f"[info] Terminado. Salida en: {output_dir}")
        return

    # Indexar reglas por archivo (Path absoluto) -> DataFrame con (asset_id, row_index)
    rules_by_file: dict[Path, pd.DataFrame] = {}
    for file_value, group in plan_df.groupby("file"):
        src = normalize_plan_file_to_path(str(file_value), repo_root).resolve()
        rules_by_file[src] = group[["asset_id", "row_index"]].copy()

    # --- procesar TODOS los CSV del input_dir ---
    csv_files = sorted(input_dir.rglob("*.csv"))
    if args.max_files is not None:
        csv_files = csv_files[: args.max_files]

    if not csv_files:
        print(f"[warning] No se encontraron CSVs en {input_dir}")
        return

    for file_path in csv_files:
        # leer CSV
        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            print(f"[warning] No se pudo leer {file_path}: {exc}")
            continue

        # Si no hay reglas para este archivo: copiar tal cual
        file_rules = rules_by_file.get(file_path.resolve())
        if file_rules is None or file_rules.empty:
            cleaned = df
        else:
            df = df.copy()
            df["__row_index"] = df.index
            df["__asset_id"] = determine_asset_series(df, file_path)

            # Armamos set de (asset_id, row_index) a dropear
            to_drop = set(
                (str(r["asset_id"]), int(r["row_index"]))
                for r in file_rules.to_dict("records")
            )

            # Máscara: drop si coincide asset_id y row_index original
            keys = list(zip(df["__asset_id"].astype(str).tolist(), df["__row_index"].astype(int).tolist()))
            drop_mask = pd.Series([k in to_drop for k in keys], index=df.index)

            cleaned = df.loc[~drop_mask].drop(columns=["__row_index", "__asset_id"], errors="ignore")

        # escribir preservando estructura relativa al input-dir
        rel = relative_from_input(input_dir, file_path)
        out_path = output_dir / rel

        if args.dry_run:
            print(f"[dry-run] Escribiría: {out_path}")
        else:
            write_output_csv(out_path, cleaned)

    print(f"[info] Terminado. Salida en: {output_dir}")


if __name__ == "__main__":
    main()
