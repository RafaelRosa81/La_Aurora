import argparse
from datetime import datetime
from pathlib import Path
import itertools

import numpy as np
import pandas as pd


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora"]
LEVEL_COLUMNS = {
    "nivelporcentual": "nivelPorcentual",
    "nivelestanque": "nivelEstanque",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audita y detecta valores singulares (outliers) en datasets de assets."
    )
    parser.add_argument("--input-dir", required=True, help="Carpeta base con CSVs")
    parser.add_argument("--group", required=True, help="Etiqueta del grupo de assets")
    parser.add_argument("--asset", help="Filtra por asset (contains, case-insensitive)")
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Procesa todos los assets detectados",
    )
    parser.add_argument("--start-date", help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Fecha de fin (YYYY-MM-DD)")
    parser.add_argument("--freq-minutes", type=int, default=1, help="Frecuencia esperada")
    parser.add_argument("--pct-min", type=float, default=0.0, help="Min % permitido")
    parser.add_argument("--pct-max", type=float, default=100.0, help="Max % permitido")
    parser.add_argument("--max-rows", type=int, default=500, help="Max filas OutOfRange")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=200000,
        help="Muestra maxima por asset para percentiles",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        help="Limita la cantidad de archivos procesados",
    )
    parser.add_argument("--output", help="Ruta de salida Excel (default reports/...)")
    parser.add_argument("--plan", help="Ruta de salida CSV plan (default reports/...)")
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


def parse_date(date_text: str | None, is_end: bool, freq_minutes: int) -> pd.Timestamp | None:
    if not date_text:
        return None
    parsed = pd.to_datetime(date_text, errors="coerce")
    if pd.isna(parsed):
        return None
    if is_end and len(date_text) == 10:
        parsed = parsed + pd.Timedelta(days=1) - pd.Timedelta(minutes=freq_minutes)
    return parsed


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


def load_csv(file_path: Path) -> tuple[pd.DataFrame | None, dict]:
    try:
        df = pd.read_csv(file_path)
    except Exception as exc:
        print(f"[warning] No se pudo leer {file_path}: {exc}")
        return None, {"error": "read_failed"}

    timestamp_col = find_timestamp_column(df)
    if not timestamp_col:
        print(f"[warning] Sin columna timestamp en {file_path}")
        return None, {"error": "no_timestamp"}

    df["__row_index"] = df.index
    df["timestamp"], strategy = parse_timestamp(df[timestamp_col])
    invalid_count = int(df["timestamp"].isna().sum())
    if invalid_count:
        print(f"[warning] {file_path} tiene {invalid_count} filas con timestamp inválido")
    df = df[df["timestamp"].notna()].copy()
    if df.empty:
        return None, {"timestamp_strategy": strategy, "detected_columns": []}

    df["asset_id"] = determine_asset_series(df, file_path)
    level_columns = detect_level_columns(df)
    detected = list(level_columns.keys())
    for canonical, original in level_columns.items():
        df[canonical] = pd.to_numeric(df[original], errors="coerce")

    for canonical in LEVEL_COLUMNS.values():
        if canonical not in df.columns:
            df[canonical] = np.nan

    return (
        df[["asset_id", "timestamp", *LEVEL_COLUMNS.values(), "__row_index"]],
        {
            "timestamp_column": timestamp_col,
            "timestamp_strategy": strategy,
            "detected_columns": detected,
        },
    )


def compute_percentiles(series: pd.Series, percentiles: list[int]) -> dict[int, float]:
    s = series.dropna()
    if s.empty:
        return {p: np.nan for p in percentiles}
    values = np.percentile(s.to_numpy(), percentiles)
    return {p: float(value) for p, value in zip(percentiles, values)}


def apply_filters(
    df: pd.DataFrame,
    asset_pattern: str | None,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    filtered = df
    if asset_pattern:
        mask = filtered["asset_id"].astype(str).str.contains(asset_pattern, case=False, na=False)
        filtered = filtered[mask]
    if start_date is not None:
        filtered = filtered[filtered["timestamp"] >= start_date]
    if end_date is not None:
        filtered = filtered[filtered["timestamp"] <= end_date]
    return filtered


def to_repo_relative(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(path)


def update_reservoir(
    sample: list[float],
    count: int,
    values: np.ndarray,
    sample_size: int,
    rng: np.random.Generator,
) -> int:
    for value in values:
        count += 1
        if len(sample) < sample_size:
            sample.append(float(value))
        else:
            idx = rng.integers(0, count)
            if idx < sample_size:
                sample[idx] = float(value)
    return count


# --- FIX: heap entries deben tener tie-breaker (no comparar dicts) ---
def update_min_heap(
    heap: list[tuple[float, int, dict]],
    item: dict,
    limit: int,
    tie: itertools.count,
) -> None:
    import heapq

    value = float(item["value"])
    entry = (-value, next(tie), item)  # (-value) para quedarnos con los mínimos
    if len(heap) < limit:
        heapq.heappush(heap, entry)
    else:
        if entry[0] > heap[0][0]:
            heapq.heapreplace(heap, entry)


def update_max_heap(
    heap: list[tuple[float, int, dict]],
    item: dict,
    limit: int,
    tie: itertools.count,
) -> None:
    import heapq

    value = float(item["value"])
    entry = (value, next(tie), item)  # (value) para quedarnos con los máximos
    if len(heap) < limit:
        heapq.heappush(heap, entry)
    else:
        if entry[0] > heap[0][0]:
            heapq.heapreplace(heap, entry)
# --- END FIX ---


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise SystemExit(f"[error] input-dir no existe: {input_dir}")

    asset_pattern = args.asset
    if not asset_pattern and not args.all:
        args.all = True

    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(args.output) if args.output else Path(
        f"reports/outliers_{args.group}_{run_timestamp}.xlsx"
    )
    plan_path = Path(args.plan) if args.plan else Path(
        f"reports/cleaning_plan_{args.group}_{run_timestamp}.csv"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.parent.mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parents[1]

    start_date = parse_date(args.start_date, False, args.freq_minutes)
    end_date = parse_date(args.end_date, True, args.freq_minutes)

    summary_rows: list[dict] = []
    outlier_rows: list[dict] = []
    extremes_rows: list[dict] = []
    plan_rows: list[dict] = []
    notes_rows: list[dict] = []
    file_notes: list[dict] = []
    sample_store: dict[str, list[float]] = {}
    sample_counts: dict[str, int] = {}
    extremes_store: dict[str, dict[str, list]] = {}
    rng = np.random.default_rng()

    # FIX: contador para tie-breaker en heaps
    tie = itertools.count()

    csv_files = sorted(input_dir.rglob("*.csv"))
    if not csv_files:
        print(f"[warning] No se encontraron CSVs en {input_dir}")
    if args.max_files is not None:
        csv_files = csv_files[: args.max_files]

    for file_path in csv_files:
        df, meta = load_csv(file_path)
        relative_file = to_repo_relative(file_path, repo_root)
        if df is None:
            file_notes.append(
                {
                    "file": relative_file,
                    "timestamp_column": meta.get("timestamp_column"),
                    "timestamp_strategy": meta.get("timestamp_strategy"),
                    "detected_columns": ", ".join(meta.get("detected_columns", [])),
                    "notes": meta.get("error", "sin datos"),
                }
            )
            continue

        df = df.copy()
        df["source_file"] = relative_file
        df = apply_filters(df, asset_pattern, start_date, end_date)
        file_notes.append(
            {
                "file": relative_file,
                "timestamp_column": meta.get("timestamp_column"),
                "timestamp_strategy": meta.get("timestamp_strategy"),
                "detected_columns": ", ".join(meta.get("detected_columns", [])),
                "notes": "",
            }
        )
        if df.empty:
            continue

        for asset_id, asset_df in df.groupby("asset_id"):
            asset_df = asset_df.sort_values("timestamp")
            series = asset_df["nivelPorcentual"]
            total = int(series.shape[0])
            valid = int(series.notna().sum())
            missing = total - valid
            out_mask = series.notna() & (
                (series < args.pct_min) | (series > args.pct_max)
            )
            out_count = int(out_mask.sum())
            out_pct = (out_count / total * 100.0) if total else 0.0
            percentiles = compute_percentiles(series, [1, 50, 99])
            in_range = series.notna() & ~out_mask
            min_in_range = float(series[in_range].min()) if in_range.any() else np.nan
            max_in_range = float(series[in_range].max()) if in_range.any() else np.nan
            date_min = asset_df["timestamp"].min()
            date_max = asset_df["timestamp"].max()

            summary_rows.append(
                {
                    "file": relative_file,
                    "asset_id": asset_id,
                    "n_rows": total,
                    "n_valid_pct": valid,
                    "n_missing_pct": missing,
                    "n_out_of_range": out_count,
                    "out_of_range_pct": out_pct,
                    "min": float(series.min()) if valid else np.nan,
                    "max": float(series.max()) if valid else np.nan,
                    "p1": percentiles[1],
                    "p50": percentiles[50],
                    "p99": percentiles[99],
                    "min_in_range": min_in_range,
                    "max_in_range": max_in_range,
                    "date_min": date_min,
                    "date_max": date_max,
                }
            )

            if out_count:
                remaining = args.max_rows - len(outlier_rows)
                if remaining > 0:
                    outlier_subset = asset_df.loc[
                        out_mask, ["timestamp", "nivelPorcentual", "__row_index"]
                    ].copy()

                    outlier_subset = outlier_subset.rename(columns={"nivelPorcentual": "value"})

                    for rec in outlier_subset.to_dict("records"):
                        if len(outlier_rows) >= args.max_rows:
                            break
                        outlier_rows.append(
                            {
                                "file": relative_file,
                                "asset_id": asset_id,
                                "timestamp": rec.get("timestamp"),
                                "column": "nivelPorcentual",
                                "value": rec.get("value"),
                                "row_index": int(rec.get("__row_index", -1)),
                            }
                        )

            if out_count:
                plan_rows.append(
                    {
                        "file": relative_file,
                        "asset_id": asset_id,
                        "column": "nivelPorcentual",
                        "pct_min": args.pct_min,
                        "pct_max": args.pct_max,
                        "action": "drop",
                        "n_out_of_range": out_count,
                        "date_min": date_min,
                        "date_max": date_max,
                    }
                )

            sample = sample_store.setdefault(asset_id, [])
            count = sample_counts.get(asset_id, 0)
            values = series.dropna().to_numpy()
            if values.size:
                sample_counts[asset_id] = update_reservoir(
                    sample, count, values, args.sample_size, rng
                )

            store = extremes_store.setdefault(asset_id, {"min": [], "max": []})
            for row in asset_df.loc[series.notna()].itertuples(index=False):
                item = {
                    "asset_id": asset_id,
                    "file": row.source_file,
                    "timestamp": row.timestamp,
                    "column": "nivelPorcentual",
                    "value": row.nivelPorcentual,
                }
                update_min_heap(store["min"], item, 50, tie)
                update_max_heap(store["max"], item, 50, tie)

    if outlier_rows:
        outlier_df = pd.DataFrame(outlier_rows)
        if not outlier_df.empty:
            outlier_df = outlier_df.sort_values("timestamp").head(args.max_rows)
    else:
        outlier_df = pd.DataFrame(
            columns=["file", "asset_id", "timestamp", "column", "value", "row_index"]
        )

    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        percentiles = []
        for asset_id in summary_df["asset_id"].unique():
            sample = sample_store.get(asset_id, [])
            if sample:
                values = np.array(sample)
                values = values[~np.isnan(values)]
                if values.size:
                    pcts = np.percentile(values, [1, 50, 99])
                    percentiles.append(
                        {
                            "asset_id": asset_id,
                            "p1": float(pcts[0]),
                            "p50": float(pcts[1]),
                            "p99": float(pcts[2]),
                        }
                    )
        if percentiles:
            pct_df = pd.DataFrame(percentiles)
            summary_df = summary_df.merge(
                pct_df, on="asset_id", how="left", suffixes=("", "_sample")
            )
            summary_df["p1"] = summary_df["p1_sample"].combine_first(summary_df["p1"])
            summary_df["p50"] = summary_df["p50_sample"].combine_first(summary_df["p50"])
            summary_df["p99"] = summary_df["p99_sample"].combine_first(summary_df["p99"])
            summary_df = summary_df.drop(columns=["p1_sample", "p50_sample", "p99_sample"])

    # FIX: ahora heap entries son (value, tie, item)
    for asset_id, store in extremes_store.items():
        min_items = sorted(store["min"], key=lambda x: x[0], reverse=True)  # (-value, ...)
        max_items = sorted(store["max"], key=lambda x: x[0], reverse=True)  # (value, ...)
        for _, _, item in min_items:
            extremes_rows.append({**item, "type": "min"})
        for _, _, item in max_items:
            extremes_rows.append({**item, "type": "max"})

    extremes_df = pd.DataFrame(extremes_rows)
    plan_df = pd.DataFrame(plan_rows).drop_duplicates(
        subset=["file", "asset_id", "column", "pct_min", "pct_max", "action"]
    )
    notes_rows.extend(
        [
            {"parameter": "input_dir", "value": str(input_dir)},
            {"parameter": "group", "value": args.group},
            {"parameter": "asset_pattern", "value": asset_pattern or "all"},
            {"parameter": "start_date", "value": args.start_date or ""},
            {"parameter": "end_date", "value": args.end_date or ""},
            {"parameter": "freq_minutes", "value": args.freq_minutes},
            {"parameter": "pct_min", "value": args.pct_min},
            {"parameter": "pct_max", "value": args.pct_max},
            {"parameter": "max_rows", "value": args.max_rows},
            {"parameter": "sample_size", "value": args.sample_size},
            {"parameter": "max_files", "value": args.max_files or ""},
            {
                "parameter": "percentile_method",
                "value": f"reservoir_sampling_n={args.sample_size} (aprox)",
            },
            {"parameter": "output", "value": str(output_path)},
            {"parameter": "plan", "value": str(plan_path)},
        ]
    )
    params_df = pd.DataFrame(notes_rows)
    file_notes_df = pd.DataFrame(file_notes)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        outlier_df.to_excel(writer, sheet_name="OutOfRangeRows", index=False)
        extremes_df.to_excel(writer, sheet_name="Extremes", index=False)
        params_df.to_excel(writer, sheet_name="Notes", index=False)
        start_row = len(params_df) + 2
        file_notes_df.to_excel(writer, sheet_name="Notes", index=False, startrow=start_row)

    plan_df.to_csv(plan_path, index=False)

    print(f"[info] Reporte generado: {output_path}")
    print(f"[info] Plan de limpieza generado: {plan_path}")


if __name__ == "__main__":
    main()
