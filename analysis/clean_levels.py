import argparse
from pathlib import Path

import numpy as np
import pandas as pd


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora"]
LEVEL_COLUMNS = {
    "nivelporcentual": "nivelPorcentual",
    "nivelestanque": "nivelEstanque",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aplica un plan de limpieza para remover valores fuera de rango."
    )
    parser.add_argument("--plan", required=True, help="CSV de plan de limpieza")
    parser.add_argument(
        "--output-dir",
        default="reports/cleaned",
        help="Carpeta de salida para CSVs limpios",
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


def determine_asset_series(df: pd.DataFrame, file_path: Path) -> pd.Series:
    lower_map = {col.lower(): col for col in df.columns}
    folder_name = file_path.parent.name
    prefix = file_path.stem.split("_")[0]
    fallback = folder_name or prefix
    asset_col = lower_map.get("asset_label")
    if asset_col:
        series = df[asset_col]
        if series.notna().any():
            series = series.astype(str).str.strip()
            series = series.where(series != "", fallback)
            return series
    return pd.Series([fallback] * len(df), index=df.index)


def detect_level_columns(df: pd.DataFrame) -> dict[str, str]:
    lower_map = {col.lower(): col for col in df.columns}
    detected = {}
    for key, canonical in LEVEL_COLUMNS.items():
        if key in lower_map:
            detected[canonical] = lower_map[key]
    return detected


def normalize_file_path(plan_file: str, repo_root: Path) -> Path:
    candidate = Path(plan_file)
    if candidate.is_absolute():
        return candidate
    return repo_root / candidate


def parse_date(value: str | float | int | None) -> pd.Timestamp | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed


def main() -> None:
    args = parse_args()
    plan_path = Path(args.plan)
    if not plan_path.exists():
        raise SystemExit(f"[error] plan no existe: {plan_path}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    repo_root = Path(__file__).resolve().parents[1]

    try:
        plan_df = pd.read_csv(plan_path)
    except Exception as exc:
        raise SystemExit(f"[error] No se pudo leer plan: {exc}") from exc

    required = {"file", "asset_id", "column", "pct_min", "pct_max", "action"}
    missing = required - set(plan_df.columns)
    if missing:
        raise SystemExit(f"[error] Plan incompleto, faltan columnas: {sorted(missing)}")

    for plan_file, file_plan in plan_df.groupby("file"):
        source_path = normalize_file_path(str(plan_file), repo_root)
        if not source_path.exists():
            print(f"[warning] Archivo no encontrado: {source_path}")
            continue
        try:
            df = pd.read_csv(source_path)
        except Exception as exc:
            print(f"[warning] No se pudo leer {source_path}: {exc}")
            continue

        timestamp_col = find_timestamp_column(df)
        if not timestamp_col:
            print(f"[warning] Sin columna timestamp en {source_path}")
            continue
        df["timestamp"], _ = parse_timestamp(df[timestamp_col])
        df["asset_id"] = determine_asset_series(df, source_path)

        level_columns = detect_level_columns(df)
        for canonical, original in level_columns.items():
            df[canonical] = pd.to_numeric(df[original], errors="coerce")
        for canonical in LEVEL_COLUMNS.values():
            if canonical not in df.columns:
                df[canonical] = np.nan

        drop_mask = pd.Series(False, index=df.index)
        for _, rule in file_plan.iterrows():
            if str(rule.get("action", "")).lower() != "drop":
                continue
            if str(rule.get("column")) != "nivelPorcentual":
                continue
            asset_id = str(rule.get("asset_id"))
            pct_min = float(rule.get("pct_min"))
            pct_max = float(rule.get("pct_max"))
            date_min = parse_date(rule.get("date_min"))
            date_max = parse_date(rule.get("date_max"))
            mask = df["asset_id"].astype(str) == asset_id
            mask &= df["nivelPorcentual"].notna()
            mask &= (df["nivelPorcentual"] < pct_min) | (df["nivelPorcentual"] > pct_max)
            if date_min is not None:
                mask &= df["timestamp"] >= date_min
            if date_max is not None:
                mask &= df["timestamp"] <= date_max
            drop_mask |= mask

        cleaned = df.loc[~drop_mask].drop(columns=["timestamp", "asset_id"])
        output_path = output_dir / Path(plan_file).name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cleaned.to_csv(output_path, index=False)
        print(f"[info] Limpio: {output_path}")


if __name__ == "__main__":
    main()
