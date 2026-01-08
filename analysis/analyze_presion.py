# -----------------------------------------------------------------------------
# analyze_presion.py
#
# ANÁLISIS (ANÁLOGO A ESTANQUES/BOMBAS)
# - Produce:
#   1) Un Excel "ALL" con todas las bombas/activos juntos
#   2) Un Excel por asset (si --all), que SOLO contenga ese asset
# - Hojas:
#   - Summary: stats por asset (min/max/media/mediana/std, missing%, rango temporal)
#   - Percentiles: P1..P99 por asset
#   - Histogram_Value: histograma de valores (por asset) + chart
#   - Histogram_DeltaAbs: histograma de |delta| (por asset) + chart
#   - DailyStats: min/max/mean diarios por asset
#   - Notes
#
# EJEMPLOS (PROMPT / COMMAND)
# - ALL + por asset:
#   python analysis\analyze_presion.py --input-dir output\datos_clean\presion --all --start-date 2024-01-01 --end-date 2025-12-31
# -----------------------------------------------------------------------------

import argparse
from datetime import datetime
from pathlib import Path
import re

import numpy as np
import pandas as pd
from openpyxl.chart import BarChart, Reference
from openpyxl.utils import get_column_letter


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora"]
ASSET_LABEL_CANDIDATES = ["asset_label", "asset", "equipo", "tag", "nombre", "id"]
VALUE_CANDIDATES = [
    "presion", "pressure", "press", "p",
    "presion_bar", "presion_kpa", "kpa", "bar", "psi"
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analiza PRESION y exporta reportes a Excel.")
    p.add_argument("--input-dir", required=True)
    p.add_argument("--asset", help="Filtra por asset (contains, case-insensitive)")
    p.add_argument("--all", action="store_true", default=False, help="Genera ALL + per-asset")
    p.add_argument("--start-date", help="YYYY-MM-DD")
    p.add_argument("--end-date", help="YYYY-MM-DD")
    p.add_argument("--freq-minutes", type=int, default=1)
    p.add_argument("--output-dir", default="reports", help="Carpeta de salida")
    p.add_argument("--bins", type=int, default=30, help="Bins histogramas")
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
    t = pd.to_datetime(date_text, errors="coerce")
    if pd.isna(t):
        return None
    if is_end and len(date_text) == 10:
        t = t + pd.Timedelta(days=1) - pd.Timedelta(minutes=freq_minutes)
    return t


def find_timestamp_column(df: pd.DataFrame) -> str | None:
    m = {c.lower(): c for c in df.columns}
    for cand in TIMESTAMP_CANDIDATES:
        if cand in m:
            return m[cand]
    return None


def find_value_column(df: pd.DataFrame) -> str | None:
    m = {c.lower(): c for c in df.columns}
    for cand in VALUE_CANDIDATES:
        if cand.lower() in m:
            return m[cand.lower()]
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
            return s.where(s != "", fallback)

    return pd.Series([fallback] * len(df), index=df.index)


def normalize_asset_filename(asset_id: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_-]+", "_", str(asset_id).strip())
    return s.strip("_") or "asset"


def compute_stats(s: pd.Series) -> dict:
    total = int(len(s))
    valid = int(s.notna().sum())
    missing_pct = (total - valid) / total * 100.0 if total else 0.0
    if valid == 0:
        return {"n_total": total, "n_valid": valid, "missing_pct": missing_pct,
                "min": np.nan, "max": np.nan, "mean": np.nan, "median": np.nan, "std": np.nan}
    v = s.dropna()
    return {"n_total": total, "n_valid": valid, "missing_pct": missing_pct,
            "min": float(v.min()), "max": float(v.max()),
            "mean": float(v.mean()), "median": float(v.median()), "std": float(v.std())}


def histogram_df(s: pd.Series, bins: int) -> pd.DataFrame:
    v = s.dropna()
    if v.empty:
        return pd.DataFrame(columns=["bin_left", "bin_right", "count"])
    counts, edges = np.histogram(v.to_numpy(), bins=bins)
    return pd.DataFrame({"bin_left": edges[:-1], "bin_right": edges[1:], "count": counts})


def add_hist_chart(ws, start_row: int, nrows: int, title: str, anchor: str) -> None:
    if nrows <= 0:
        return
    chart = BarChart()
    chart.title = title
    data = Reference(ws, min_col=4, min_row=start_row, max_row=start_row + nrows - 1)
    cats = Reference(ws, min_col=2, min_row=start_row, max_row=start_row + nrows - 1)
    chart.add_data(data, titles_from_data=False)
    chart.set_categories(cats)
    chart.height = 8
    chart.width = 16
    ws.add_chart(chart, anchor)


def load_all(input_dir: Path) -> pd.DataFrame:
    frames = []
    csvs = sorted(input_dir.rglob("*.csv"))
    for fp in csvs:
        try:
            raw = pd.read_csv(fp)
        except Exception:
            continue
        ts_col = find_timestamp_column(raw)
        val_col = find_value_column(raw)
        if not ts_col or not val_col:
            continue
        df = raw.copy()
        df["timestamp"], _ = parse_timestamp(df[ts_col])
        df = df[df["timestamp"].notna()].copy()
        if df.empty:
            continue
        df["asset_id"] = determine_asset_series(df, fp)
        df["value"] = pd.to_numeric(df[val_col], errors="coerce")
        frames.append(df[["asset_id", "timestamp", "value"]])
    if not frames:
        return pd.DataFrame(columns=["asset_id", "timestamp", "value"])
    return pd.concat(frames, ignore_index=True)


def build_report(df_all: pd.DataFrame, asset_filter: str | None, out_path: Path, bins: int, start_date, end_date) -> None:
    df = df_all.copy()
    if asset_filter:
        m = df["asset_id"].astype(str).str.contains(asset_filter, case=False, na=False)
        df = df[m]
    if start_date is not None:
        df = df[df["timestamp"] >= start_date]
    if end_date is not None:
        df = df[df["timestamp"] <= end_date]

    if df.empty:
        # crear excel vacío con notes
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            pd.DataFrame(columns=["asset_id"]).to_excel(w, sheet_name="Summary", index=False)
            pd.DataFrame([["note", "sin datos para filtros"]]).to_excel(w, sheet_name="Notes", index=False, header=False)
        return

    summary_rows = []
    pct_rows = []
    hist_rows = []
    dhist_rows = []
    daily_rows = []

    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]

    df["asset_id"] = df["asset_id"].astype("category")

    for asset_id, g in df.groupby("asset_id", sort=False, observed=True):
        g = g.sort_values("timestamp")
        s = g["value"]
        stats = compute_stats(s)
        summary_rows.append({
            "asset_id": str(asset_id),
            "date_min": g["timestamp"].min(),
            "date_max": g["timestamp"].max(),
            **stats,
            "value_unit(?)": "desconocida (configurar si aplica)"
        })

        v = s.dropna()
        if not v.empty:
            pvals = np.percentile(v.to_numpy(), percentiles)
            for p, val in zip(percentiles, pvals):
                pct_rows.append({"asset_id": str(asset_id), "percentile": f"P{p}", "value": float(val)})

        h = histogram_df(s, bins)
        if not h.empty:
            h.insert(0, "asset_id", str(asset_id))
            hist_rows.append(h)

        delta_abs = g["value"].diff().abs()
        hd = histogram_df(delta_abs, bins)
        if not hd.empty:
            hd.insert(0, "asset_id", str(asset_id))
            dhist_rows.append(hd)

        # daily stats
        g2 = g.dropna(subset=["value"]).copy()
        if not g2.empty:
            g2["date"] = g2["timestamp"].dt.floor("D")
            daily = g2.groupby("date")["value"].agg(["min", "max", "mean"]).reset_index()
            daily.insert(0, "asset_id", str(asset_id))
            daily_rows.append(daily)

    summary_df = pd.DataFrame(summary_rows)
    pct_df = pd.DataFrame(pct_rows)
    hist_df = pd.concat(hist_rows, ignore_index=True) if hist_rows else pd.DataFrame(columns=["asset_id","bin_left","bin_right","count"])
    dhist_df = pd.concat(dhist_rows, ignore_index=True) if dhist_rows else pd.DataFrame(columns=["asset_id","bin_left","bin_right","count"])
    daily_df = pd.concat(daily_rows, ignore_index=True) if daily_rows else pd.DataFrame(columns=["asset_id","date","min","max","mean"])

    notes = [
        ["asset_filter", asset_filter or "ALL"],
        ["start_date", str(start_date) if start_date is not None else ""],
        ["end_date", str(end_date) if end_date is not None else ""],
        ["bins", str(bins)],
        ["columns", "asset_id, timestamp, value"],
    ]
    notes_df = pd.DataFrame(notes)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        summary_df.to_excel(w, sheet_name="Summary", index=False)
        pct_df.to_excel(w, sheet_name="Percentiles", index=False)
        hist_df.to_excel(w, sheet_name="Histogram_Value", index=False)
        dhist_df.to_excel(w, sheet_name="Histogram_DeltaAbs", index=False)
        daily_df.to_excel(w, sheet_name="DailyStats", index=False)
        notes_df.to_excel(w, sheet_name="Notes", index=False, header=False)

        # Charts simples: para no explotar el tamaño, ponemos chart por asset en su bloque
        ws_h = w.sheets["Histogram_Value"]
        if not hist_df.empty:
            # construir un chart por asset, anclado en F{start}
            hd = hist_df.reset_index(drop=True)
            for aid in hd["asset_id"].unique().tolist():
                idxs = hd.index[hd["asset_id"] == aid]
                if idxs.empty:
                    continue
                start = int(idxs.min()) + 2
                end = int(idxs.max()) + 2
                nrows = end - start + 1
                add_hist_chart(ws_h, start, nrows, f"Hist value - {aid}", f"F{start}")

        ws_d = w.sheets["Histogram_DeltaAbs"]
        if not dhist_df.empty:
            hd2 = dhist_df.reset_index(drop=True)
            for aid in hd2["asset_id"].unique().tolist():
                idxs = hd2.index[hd2["asset_id"] == aid]
                if idxs.empty:
                    continue
                start = int(idxs.min()) + 2
                end = int(idxs.max()) + 2
                nrows = end - start + 1
                add_hist_chart(ws_d, start, nrows, f"Hist |delta| - {aid}", f"F{start}")


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    start_date = parse_date(args.start_date, False, args.freq_minutes)
    end_date = parse_date(args.end_date, True, args.freq_minutes)

    df_all = load_all(input_dir)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # modo asset único
    if args.asset and not args.all:
        out_path = out_dir / f"presion_{normalize_asset_filename(args.asset)}_{ts}.xlsx"
        build_report(df_all, args.asset, out_path, args.bins, start_date, end_date)
        print(f"[info] Reporte generado: {out_path}")
        return

    # modo ALL (+ per-asset si --all)
    out_all = out_dir / f"presion_ALL_{ts}.xlsx"
    build_report(df_all, None, out_all, args.bins, start_date, end_date)
    print(f"[info] Reporte generado: {out_all}")

    if args.all:
        asset_ids = sorted(df_all["asset_id"].dropna().astype(str).unique().tolist())
        for aid in asset_ids:
            out_path = out_dir / f"presion_{normalize_asset_filename(aid)}_{ts}.xlsx"
            build_report(df_all, aid, out_path, args.bins, start_date, end_date)
            print(f"[info] Reporte generado: {out_path}")


if __name__ == "__main__":
    main()
