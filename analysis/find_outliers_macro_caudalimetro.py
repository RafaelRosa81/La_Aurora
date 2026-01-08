# -----------------------------------------------------------------------------
# find_outliers_macro_caudalimetro.py
#
# CRITERIOS (AUDITORÍA / OUTLIERS)
# - Detecta timestamp en: timestamp/ts/datetime/fechahora.
# - asset_id: igual a presion (evita asset_label que parece hora/fecha).
# - Detecta la variable "macro_caudalimetro" por candidatos en VALUE_CANDIDATES.
# - Outliers por asset:
#   - fuera de percentiles (pct_min/pct_max) y/o límites absolutos (abs_min/abs_max)
#   - deltas anómalos (auditoría)
# - Produce Excel + plan CSV para clean_macro_caudalimetro.py
#
# EJEMPLO:
# python analysis\find_outliers_macro_caudalimetro.py --input-dir output\datos_def\macro_caudalimetro --group macro_caudalimetro --all --pct-min 0.5 --pct-max 99.5
# -----------------------------------------------------------------------------

import argparse
from datetime import datetime
from pathlib import Path
import itertools

import numpy as np
import pandas as pd


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora"]

# Agregá acá nombres reales si tu CSV usa otros headers
VALUE_CANDIDATES = [
   "macro_caudalimetro", "caudalimetro", "flow", "q",
   "m3h", "m3_h", "l_s", "lps", "litros_seg", "litros_s",
   "volumen", "totalizador", "lectura", "caudal", "volumenAgua", "volumenAguaPlot"
]

ASSET_LABEL_CANDIDATES = ["asset_label", "asset", "equipo", "tag", "nombre", "id"]


def parse_args() -> argparse.Namespace:
    #p = argparse.ArgumentParser(description="Audita outliers para target PRESION.")
    p = argparse.ArgumentParser(description="Audita outliers para target MACRO_CAUDALIMETRO.")
    p.add_argument("--input-dir", required=True, help="Carpeta base con CSVs")
    p.add_argument("--group", required=True, help="Etiqueta del grupo (ej: presion)")
    p.add_argument("--asset", help="Filtra por asset (contains, case-insensitive)")
    p.add_argument("--all", action="store_true", default=False, help="Procesa todos los assets detectados")
    p.add_argument("--start-date", help="YYYY-MM-DD")
    p.add_argument("--end-date", help="YYYY-MM-DD")
    p.add_argument("--freq-minutes", type=int, default=1, help="Frecuencia esperada (para end-date inclusivo)")
    p.add_argument("--pct-min", type=float, default=0.0, help="Percentil mínimo permitido (0..100)")
    p.add_argument("--pct-max", type=float, default=100.0, help="Percentil máximo permitido (0..100)")
    p.add_argument("--abs-min", type=float, default=None, help="Mínimo absoluto permitido (opcional)")
    p.add_argument("--abs-max", type=float, default=None, help="Máximo absoluto permitido (opcional)")
    p.add_argument("--max-rows", type=int, default=500, help="Max filas listadas en OutOfRangeRows")
    p.add_argument("--max-files", type=int, default=None, help="Limita cantidad de archivos (debug)")
    p.add_argument("--output", help="Ruta Excel salida (default reports/...)")
    p.add_argument("--plan", help="Ruta CSV plan salida (default reports/...)")
    return p.parse_args()


def parse_timestamp(series: pd.Series) -> tuple[pd.Series, str]:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        med = float(numeric.dropna().median())
        if med >= 1e14:
            return pd.to_datetime(numeric, errors="coerce", unit="us"), "epoch_us"
        if med >= 1e11:
            return pd.to_datetime(numeric, errors="coerce", unit="ms"), "epoch_ms"
        if med >= 1e8:
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
    lower_map = {c.lower(): c for c in df.columns}
    for cand in TIMESTAMP_CANDIDATES:
        if cand in lower_map:
            return lower_map[cand]
    return None


def find_value_column(df: pd.DataFrame) -> str | None:
    lower_map = {c.lower(): c for c in df.columns}
    for cand in VALUE_CANDIDATES:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    # fallback: si hay una sola columna numérica “importante”
    numeric_cols = []
    for c in df.columns:
        if c.lower() in [*TIMESTAMP_CANDIDATES, *ASSET_LABEL_CANDIDATES]:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().mean() > 0.5:
            numeric_cols.append(c)
    if len(numeric_cols) == 1:
        return numeric_cols[0]
    return None


def looks_like_time_or_date(series: pd.Series) -> bool:
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return False
    sample = s.head(2000)
    parsed = pd.to_datetime(sample, errors="coerce", infer_datetime_format=True)
    return float(parsed.notna().mean()) >= 0.80


def determine_asset_series(df: pd.DataFrame, file_path: Path) -> pd.Series:
    lower_map = {c.lower(): c for c in df.columns}
    folder_name = file_path.parent.name
    prefix = file_path.stem.split("_")[0]
    fallback = folder_name or prefix

    asset_col = None
    for cand in ASSET_LABEL_CANDIDATES:
        if cand in lower_map:
            asset_col = lower_map[cand]
            break

    if asset_col:
        series = df[asset_col]
        if series.notna().any():
            if looks_like_time_or_date(series):
                return pd.Series([fallback] * len(df), index=df.index)
            s = series.astype(str).str.strip()
            s = s.where(s != "", fallback)
            return s

    return pd.Series([fallback] * len(df), index=df.index)


def to_repo_relative(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(path)


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise SystemExit(f"[error] input-dir no existe: {input_dir}")

    if not args.asset and not args.all:
        args.all = True

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(args.output) if args.output else Path(f"reports/outliers_{args.group}_{run_ts}.xlsx")
    plan_path = Path(args.plan) if args.plan else Path(f"reports/cleaning_plan_{args.group}_{run_ts}.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.parent.mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parents[1]

    start_date = parse_date(args.start_date, False, args.freq_minutes)
    end_date = parse_date(args.end_date, True, args.freq_minutes)

    summary_rows = []
    outlier_rows = []
    plan_rows = []
    file_notes = []
    tie = itertools.count()

    csv_files = sorted(input_dir.rglob("*.csv"))
    if args.max_files is not None:
        csv_files = csv_files[: args.max_files]
    if not csv_files:
        print(f"[warning] No se encontraron CSVs en {input_dir}")

    for file_path in csv_files:
        rel_file = to_repo_relative(file_path, repo_root)

        try:
            raw = pd.read_csv(file_path)
        except Exception as exc:
            file_notes.append({"file": rel_file, "notes": f"read_failed: {exc}"})
            continue

        ts_col = find_timestamp_column(raw)
        val_col = find_value_column(raw)
        if not ts_col or not val_col:
            file_notes.append({"file": rel_file, "notes": f"missing_columns: ts={ts_col} value={val_col}"})
            continue

        df = raw.copy()
        df["timestamp"], strategy = parse_timestamp(df[ts_col])
        df = df[df["timestamp"].notna()].copy()
        if df.empty:
            file_notes.append({"file": rel_file, "notes": f"no_valid_timestamp ({strategy})"})
            continue

        df["asset_id"] = determine_asset_series(df, file_path)
        df["value"] = pd.to_numeric(df[val_col], errors="coerce")

        # filtros
        if args.asset:
            m = df["asset_id"].astype(str).str.contains(args.asset, case=False, na=False)
            df = df[m]
        if start_date is not None:
            df = df[df["timestamp"] >= start_date]
        if end_date is not None:
            df = df[df["timestamp"] <= end_date]
        if df.empty:
            continue

        # por asset
        for asset_id, g in df.groupby("asset_id", sort=False, observed=True):
            g = g.sort_values("timestamp")
            s = g["value"]
            total = int(len(s))
            valid = int(s.notna().sum())
            missing = total - valid

            s_valid = s.dropna()
            if s_valid.empty:
                summary_rows.append({
                    "file": rel_file, "asset_id": asset_id, "n_rows": total,
                    "n_valid": valid, "n_missing": missing,
                    "min": np.nan, "max": np.nan, "p1": np.nan, "p50": np.nan, "p99": np.nan,
                    "timestamp_strategy": strategy, "value_column": val_col
                })
                continue

            p1, p50, p99 = np.percentile(s_valid.to_numpy(), [1, 50, 99])

            # regla percentiles (interpretación: mantener dentro de [Ppct_min, Ppct_max])
            low_thr = float(np.percentile(s_valid.to_numpy(), args.pct_min)) if args.pct_min is not None else -np.inf
            high_thr = float(np.percentile(s_valid.to_numpy(), args.pct_max)) if args.pct_max is not None else np.inf

            # incorporar límites absolutos si se definen
            if args.abs_min is not None:
                low_thr = max(low_thr, float(args.abs_min))
            if args.abs_max is not None:
                high_thr = min(high_thr, float(args.abs_max))

            out_mask = s.notna() & ((s < low_thr) | (s > high_thr))
            out_count = int(out_mask.sum())
            out_pct = (out_count / total * 100.0) if total else 0.0

            # deltas (solo auditoría)
            delta = g["value"].diff().abs()
            d_valid = delta.dropna()
            d99 = float(np.percentile(d_valid.to_numpy(), 99)) if not d_valid.empty else np.nan

            summary_rows.append({
                "file": rel_file,
                "asset_id": asset_id,
                "n_rows": total,
                "n_valid": valid,
                "n_missing": missing,
                "out_count": out_count,
                "out_pct": out_pct,
                "min": float(s_valid.min()),
                "max": float(s_valid.max()),
                "p1": float(p1),
                "p50": float(p50),
                "p99": float(p99),
                "delta_abs_p99": d99,
                "date_min": g["timestamp"].min(),
                "date_max": g["timestamp"].max(),
                "timestamp_strategy": strategy,
                "value_column": val_col,
            })

            if out_count:
                # plan: drop valores fuera de umbral (low_thr/high_thr)
                plan_rows.append({
                    "file": rel_file,
                    "asset_id": str(asset_id),
                    "column": "value",
                    "pct_min": args.pct_min,
                    "pct_max": args.pct_max,
                    "abs_min": args.abs_min if args.abs_min is not None else "",
                    "abs_max": args.abs_max if args.abs_max is not None else "",
                    "value_min": low_thr,
                    "value_max": high_thr,
                    "action": "drop",
                    "n_out": out_count,
                    "date_min": g["timestamp"].min(),
                    "date_max": g["timestamp"].max(),
                })

                # filas outliers (cap)
                if len(outlier_rows) < args.max_rows:
                    subset = g.loc[out_mask, ["timestamp", "value"]].copy()
                    for rec in subset.head(args.max_rows - len(outlier_rows)).to_dict("records"):
                        outlier_rows.append({
                            "file": rel_file,
                            "asset_id": asset_id,
                            "timestamp": rec.get("timestamp"),
                            "column": "value",
                            "value": rec.get("value"),
                        })

    summary_df = pd.DataFrame(summary_rows)
    outliers_df = pd.DataFrame(outlier_rows) if outlier_rows else pd.DataFrame(columns=["file", "asset_id", "timestamp", "column", "value"])
    plan_df = pd.DataFrame(plan_rows).drop_duplicates(subset=["file", "asset_id", "column", "pct_min", "pct_max", "abs_min", "abs_max", "action"])
    notes_df = pd.DataFrame(file_notes) if file_notes else pd.DataFrame(columns=["file", "notes"])

    with pd.ExcelWriter(output_path, engine="openpyxl") as w:
        summary_df.to_excel(w, sheet_name="Summary", index=False)
        outliers_df.to_excel(w, sheet_name="OutOfRangeRows", index=False)
        notes_df.to_excel(w, sheet_name="Notes", index=False)

    plan_df.to_csv(plan_path, index=False)

    print(f"[info] Reporte generado: {output_path}")
    print(f"[info] Plan de limpieza generado: {plan_path}")


if __name__ == "__main__":
    main()
