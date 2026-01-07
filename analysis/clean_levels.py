import argparse
from pathlib import Path
import os

import numpy as np
import pandas as pd


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora"]
LEVEL_COLUMNS = {
    "nivelporcentual": "nivelPorcentual",
    "nivelestanque": "nivelEstanque",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aplica un plan de limpieza (drop) preservando estructura de carpetas."
    )
    parser.add_argument("--plan", required=True, help="CSV de plan de limpieza (find_outliers)")
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Carpeta base de entrada con CSVs (ej: output\\datos_def\\estanques)",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Carpeta base de salida (ej: output\\datos_clean\\estanques)",
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


def find_timestamp_column(df: pd.DataFrame) -> str | None:
    lower_map = {col.lower(): col for col in df.columns}
    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def detect_level_columns(df: pd.DataFrame) -> dict[str, str]:
    lower_map = {col.lower(): col for col in df.columns}
    detected = {}
    for key, canonical in LEVEL_COLUMNS.items():
        if key in lower_map:
            detected[canonical] = lower_map[key]
    return detected


def looks_like_time_or_date(series: pd.Series) -> bool:
    """
    Si la mayoría de valores de asset_label se pueden parsear como datetime/hora,
    entonces está contaminado (ej '06:00 AM') y NO debe usarse como asset_id.
    """
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return False
    sample = s.head(5000)
    parsed = pd.to_datetime(sample, errors="coerce", infer_datetime_format=True)
    ratio = float(parsed.notna().mean())
    return ratio >= 0.80


def determine_asset_series(df: pd.DataFrame, file_path: Path) -> pd.Series:
    lower_map = {col.lower(): col for col in df.columns}
    folder_name = file_path.parent.name
    prefix = file_path.stem.split("_")[0]
    fallback = folder_name or prefix

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


def parse_date(value) -> pd.Timestamp | None:
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed


def norm_path_str(p: Path) -> str:
    """
    Normaliza paths para comparar:
    - resuelve separadores
    - lower en Windows
    """
    s = os.path.normpath(str(p))
    if os.name == "nt":
        s = s.lower()
    return s


def normalize_plan_file_to_abs(file_value: str, repo_root: Path) -> Path:
    """
    'file' del plan suele venir relativo al repo (ej output\\datos_def\\estanques\\Principal\\X.csv).
    Lo resolvemos contra repo_root y lo pasamos a absoluto.
    """
    p = Path(str(file_value))
    if not p.is_absolute():
        p = repo_root / p
    return p.resolve()


def relative_from_input(input_dir: Path, file_path: Path) -> Path:
    """
    Ruta relativa de file_path respecto a input_dir.
    """
    return file_path.resolve().relative_to(input_dir.resolve())


def write_output_csv(output_path: Path, df: pd.DataFrame) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[info] Limpio: {output_path}")


def main() -> None:
    args = parse_args()
    plan_path = Path(args.plan)
    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

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

    required = {"file", "asset_id", "column", "pct_min", "pct_max", "action"}
    missing = required - set(plan_df.columns)
    if missing:
        raise SystemExit(f"[error] Plan incompleto, faltan columnas: {sorted(missing)}")

    # Filtrar sólo drop en nivelPorcentual
    plan_df = plan_df.copy()
    plan_df["action"] = plan_df["action"].astype(str).str.lower().str.strip()
    plan_df = plan_df[(plan_df["action"] == "drop") & (plan_df["column"] == "nivelPorcentual")]

    # --- indexar reglas por archivo (robusto: abs + relativo al repo) ---
    rules_by_abs: dict[str, pd.DataFrame] = {}
    rules_by_rel: dict[str, pd.DataFrame] = {}

    for file_value, group in plan_df.groupby("file"):
        abs_path = normalize_plan_file_to_abs(str(file_value), repo_root)
        rules_by_abs[norm_path_str(abs_path)] = group

        # también guardamos key por path relativo al repo_root (si aplica)
        try:
            rel = abs_path.relative_to(repo_root.resolve())
            rules_by_rel[norm_path_str(rel)] = group
        except Exception:
            pass

    # --- procesar TODOS los CSV del input_dir ---
    csv_files = sorted(input_dir.rglob("*.csv"))
    if args.max_files is not None:
        csv_files = csv_files[: args.max_files]

    if not csv_files:
        print(f"[warning] No se encontraron CSVs en {input_dir}")
        return

    processed = 0
    copied = 0
    cleaned_files = 0

    for file_path in csv_files:
        processed += 1

        # determinar salida SIEMPRE como relativo a input_dir
        try:
            rel = relative_from_input(input_dir, file_path)
        except Exception:
            # si esto falla, algo raro hay con input_dir; para no duplicar carpetas,
            # caemos a un path simple "nombre.csv"
            rel = Path(file_path.name)

        out_path = output_dir / rel

        # leer CSV
        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            print(f"[warning] No se pudo leer {file_path}: {exc}")
            continue

        # buscar reglas: primero por abs, luego por rel al repo_root
        file_key_abs = norm_path_str(file_path.resolve())
        file_rules = rules_by_abs.get(file_key_abs)

        if file_rules is None:
            try:
                rel_repo = file_path.resolve().relative_to(repo_root.resolve())
                file_rules = rules_by_rel.get(norm_path_str(rel_repo))
            except Exception:
                file_rules = None

        # si no hay reglas -> COPIA 1:1 (esto evita “meses faltantes”)
        if file_rules is None or file_rules.empty:
            copied += 1
            if args.dry_run:
                print(f"[dry-run] Copiaría sin cambios: {out_path}")
            else:
                write_output_csv(out_path, df)
            continue

        # aplicar limpieza sólo si hay timestamp y nivelPorcentual
        timestamp_col = find_timestamp_column(df)
        if not timestamp_col:
            copied += 1
            print(f"[warning] Sin columna timestamp en {file_path} -> se copia sin cambios")
            if args.dry_run:
                print(f"[dry-run] Copiaría sin cambios: {out_path}")
            else:
                write_output_csv(out_path, df)
            continue

        df = df.copy()
        df["__timestamp_parsed"], _ = parse_timestamp(df[timestamp_col])
        df["__asset_id"] = determine_asset_series(df, file_path)

        level_columns = detect_level_columns(df)
        if "nivelPorcentual" in level_columns:
            df["__nivelPorcentual"] = pd.to_numeric(df[level_columns["nivelPorcentual"]], errors="coerce")
        else:
            # si no está, no se puede limpiar por percentiles -> copiamos
            copied += 1
            print(f"[warning] Sin nivelPorcentual en {file_path} -> se copia sin cambios")
            cleaned = df.drop(columns=["__timestamp_parsed", "__asset_id"], errors="ignore")
            if args.dry_run:
                print(f"[dry-run] Copiaría sin cambios: {out_path}")
            else:
                write_output_csv(out_path, cleaned)
            continue

        drop_mask = pd.Series(False, index=df.index)

        for _, rule in file_rules.iterrows():
            asset_id = str(rule.get("asset_id"))
            pct_min = float(rule.get("pct_min"))
            pct_max = float(rule.get("pct_max"))
            date_min = parse_date(rule.get("date_min"))
            date_max = parse_date(rule.get("date_max"))

            m = df["__asset_id"].astype(str) == asset_id
            m &= df["__nivelPorcentual"].notna()
            m &= (df["__nivelPorcentual"] < pct_min) | (df["__nivelPorcentual"] > pct_max)

            if date_min is not None:
                m &= df["__timestamp_parsed"] >= date_min
            if date_max is not None:
                m &= df["__timestamp_parsed"] <= date_max

            drop_mask |= m

        n_drop = int(drop_mask.sum())
        cleaned = df.loc[~drop_mask].drop(
            columns=["__timestamp_parsed", "__asset_id", "__nivelPorcentual"], errors="ignore"
        )

        cleaned_files += 1
        if args.dry_run:
            print(f"[dry-run] Limpiaría: {out_path}  (drop={n_drop}, keep={len(cleaned)})")
        else:
            write_output_csv(out_path, cleaned)
            print(f"[info] Stats: {rel}  drop={n_drop}  keep={len(cleaned)}")

    print(f"[info] Terminado. input={input_dir} output={output_dir}")
    print(f"[info] Archivos: procesados={processed} copiados_sin_cambios={copied} limpiados={cleaned_files}")


if __name__ == "__main__":
    main()
