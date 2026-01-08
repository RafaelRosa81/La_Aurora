# -----------------------------------------------------------------------------
# analyze_macro_caudalimetro.py
#
# ANÁLISIS (ANÁLOGO A ESTANQUES/BOMBAS)
# - Entrada: CSVs (posiblemente en subcarpetas) dentro de --input-dir.
# - Detecta timestamp (timestamp/ts/datetime/fechahora).
# - Detecta asset_id:
#   - usa asset_label (o similares) si existe y NO parece hora/fecha,
#   - si está contaminado (ej "07:30:00") usa fallback (carpeta o prefijo archivo).
# - Detecta la variable (lectura) del macro caudalímetro por nombres candidatos (VALUE_CANDIDATES).
#
# SALIDAS
# - Siempre genera un Excel ALL con info de TODOS los assets (macro_caudalimetro_ALL_*.xlsx)
# - Si usás --all, además genera un Excel por asset (macro_caudalimetro_{asset}_*.xlsx),
#   y cada uno contiene SOLO información de ese asset.
#
# HOJAS EN EL EXCEL
# - Summary: stats por asset (min/max/mean/median/std, missing%, rango temporal).
# - Percentiles: percentiles (P1..P99) por asset.
# - Histogram_Value: histograma de valores por asset + gráficos.
# - Histogram_DeltaAbs: histograma de |delta| por asset + gráficos. valor absoluto del cambio entre dos lecturas consecutivas del macro caudalímetro.
# - TotalizerCheck: diagnóstico por asset (¿parece totalizador monotónico? ratio de deltas >=0).
# - Histogram_Increment: (si parece totalizador) histograma de incrementos positivos.
# - DailyTotal: (si parece totalizador) suma diaria de incrementos positivos.
# - DailyStats: min/max/mean diarios del valor.
# - Notes: parámetros y columnas detectadas.
#
# CRITERIO DE "TOTALIZADOR"
# - Si la serie tiene suficientes puntos y el % de deltas >= 0 es alto (default >= 0.98),
#   se asume que es un totalizador (acumulado).
# - En ese caso:
#   - increment = diff(value)
#   - increment_pos = increment recortado a [0, +inf) (resets/negativos no suman)
#   - DailyTotal = suma diaria de increment_pos
#
# EJEMPLOS (PROMPT / COMMAND)
# - ALL + por asset:
#   python analysis\analyze_macro_caudalimetro.py --input-dir output\datos_clean\macro_caudalimetro --all --start-date 2024-01-01 --end-date 2025-12-31
#
# - Solo ALL:
#   python analysis\analyze_macro_caudalimetro.py --input-dir output\datos_clean\macro_caudalimetro --start-date 2024-01-01 --end-date 2025-12-31
#
# - Filtrar por asset (substring):
#   python analysis\analyze_macro_caudalimetro.py --input-dir output\datos_clean\macro_caudalimetro --asset Principal --start-date 2024-01-01 --end-date 2025-12-31
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

# Candidatos típicos para macro caudalímetro / totalizador
VALUE_CANDIDATES = [
   "macro_caudalimetro", "caudalimetro", "flow", "q",
   "m3h", "m3_h", "l_s", "lps", "litros_seg", "litros_s",
   "volumen", "totalizador", "lectura", "caudal", "volumenAgua", "volumenAguaPlot"
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analiza MACRO_CAUDALIMETRO y exporta reportes a Excel.")
    p.add_argument("--input-dir", required=True, help="Carpeta base con CSVs limpios")
    p.add_argument("--asset", help="Filtra por asset (contains, case-insensitive)")
    p.add_argument("--all", action="store_true", default=False, help="Genera ALL + per-asset")
    p.add_argument("--start-date", help="YYYY-MM-DD")
    p.add_argument("--end-date", help="YYYY-MM-DD")
    p.add_argument("--freq-minutes", type=int, default=1, help="Frecuencia para end-date inclusivo")
    p.add_argument("--output-dir", default="reports", help="Carpeta de salida")
    p.add_argument("--bins", type=int, default=30, help="Bins histogramas")
    p.add_argument("--max-files", type=int, default=None, help="Limita cantidad de archivos (debug)")
    p.add_argument("--totalizer-threshold", type=float, default=0.98,
                   help="Umbral de ratio(deltas>=0) para considerar totalizador (0..1)")
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

    # fallback: si hay una sola columna numérica "dominante"
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
        return {
            "n_total": total, "n_valid": valid, "missing_pct": missing_pct,
            "min": np.nan, "max": np.nan, "mean": np.nan,
            "median": np.nan, "std": np.nan
        }
    v = s.dropna()
    return {
        "n_total": total, "n_valid": valid, "missing_pct": missing_pct,
        "min": float(v.min()), "max": float(v.max()),
        "mean": float(v.mean()), "median": float(v.median()),
        "std": float(v.std())
    }


def percentiles_df_for_series(asset_id: str, s: pd.Series, percentiles: list[int]) -> pd.DataFrame:
    v = s.dropna()
    if v.empty:
        return pd.DataFrame(columns=["asset_id", "percentile", "value"])
    pvals = np.percentile(v.to_numpy(), percentiles)
    return pd.DataFrame({
        "asset_id": [asset_id] * len(percentiles),
        "percentile": [f"P{p}" for p in percentiles],
        "value": [float(x) for x in pvals],
    })


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
    # En el sheet: [asset_id, bin_left, bin_right, count]
    data = Reference(ws, min_col=4, min_row=start_row, max_row=start_row + nrows - 1)
    cats = Reference(ws, min_col=2, min_row=start_row, max_row=start_row + nrows - 1)
    chart.add_data(data, titles_from_data=False)
    chart.set_categories(cats)
    chart.height = 8
    chart.width = 16
    ws.add_chart(chart, anchor)


def load_all(input_dir: Path, max_files: int | None) -> tuple[pd.DataFrame, dict]:
    frames = []
    detected = {"timestamp_col": set(), "value_col": set(), "strategy": set()}
    csvs = sorted(input_dir.rglob("*.csv"))
    if max_files is not None:
        csvs = csvs[:max_files]

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
        ts_parsed, strat = parse_timestamp(df[ts_col])
        df["timestamp"] = ts_parsed
        df = df[df["timestamp"].notna()].copy()
        if df.empty:
            continue

        df["asset_id"] = determine_asset_series(df, fp)
        df["value"] = pd.to_numeric(df[val_col], errors="coerce")

        frames.append(df[["asset_id", "timestamp", "value"]])

        detected["timestamp_col"].add(ts_col)
        detected["value_col"].add(val_col)
        detected["strategy"].add(strat)

    if not frames:
        return pd.DataFrame(columns=["asset_id", "timestamp", "value"]), {
            "timestamp_cols": [],
            "value_cols": [],
            "timestamp_strategies": [],
        }

    combined = pd.concat(frames, ignore_index=True)
    meta = {
        "timestamp_cols": sorted(detected["timestamp_col"]),
        "value_cols": sorted(detected["value_col"]),
        "timestamp_strategies": sorted(detected["strategy"]),
    }
    return combined, meta


def build_report(
    df_all: pd.DataFrame,
    asset_filter: str | None,
    out_path: Path,
    bins: int,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
    totalizer_threshold: float,
    notes_extra: list[list[str]] | None = None
) -> None:
    df = df_all.copy()

    # filtro asset
    if asset_filter:
        m = df["asset_id"].astype(str).str.contains(asset_filter, case=False, na=False)
        df = df[m]

    # filtro fechas
    if start_date is not None:
        df = df[df["timestamp"] >= start_date]
    if end_date is not None:
        df = df[df["timestamp"] <= end_date]

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if df.empty:
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            pd.DataFrame(columns=["asset_id"]).to_excel(w, sheet_name="Summary", index=False)
            pd.DataFrame([["note", "sin datos para filtros"]]).to_excel(w, sheet_name="Notes", index=False, header=False)
        return

    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]

    summary_rows = []
    pct_frames = []
    hist_frames = []
    dhist_frames = []
    totalizer_rows = []
    inc_hist_frames = []
    daily_total_frames = []
    daily_stats_frames = []

    df["asset_id"] = df["asset_id"].astype("category")

    for asset_id, g in df.groupby("asset_id", sort=False, observed=True):
        asset_id_str = str(asset_id)
        g = g.sort_values("timestamp")
        s = g["value"]

        stats = compute_stats(s)
        summary_rows.append({
            "asset_id": asset_id_str,
            "date_min": g["timestamp"].min(),
            "date_max": g["timestamp"].max(),
            **stats,
            "value_units": "desconocidas (definir en VALUE_CANDIDATES si aplica)"
        })

        pct_frames.append(percentiles_df_for_series(asset_id_str, s, percentiles))

        # histogram de valores
        h = histogram_df(s, bins)
        if not h.empty:
            h.insert(0, "asset_id", asset_id_str)
            hist_frames.append(h)

        # histograma de |delta|
        delta = g["value"].diff()
        delta_abs = delta.abs()
        hd = histogram_df(delta_abs, bins)
        if not hd.empty:
            hd.insert(0, "asset_id", asset_id_str)
            dhist_frames.append(hd)

        # daily stats (min/max/mean)
        g2 = g.dropna(subset=["value"]).copy()
        if not g2.empty:
            g2["date"] = g2["timestamp"].dt.floor("D")
            daily = g2.groupby("date")["value"].agg(["min", "max", "mean"]).reset_index()
            daily.insert(0, "asset_id", asset_id_str)
            daily_stats_frames.append(daily)

        # totalizer detection
        d = delta.dropna()
        if d.empty:
            ratio_nonneg = np.nan
            is_totalizer = False
            n_deltas = 0
        else:
            n_deltas = int(len(d))
            ratio_nonneg = float((d >= 0).mean())
            # heurística: totalizador si la mayoría de deltas son no negativos
            is_totalizer = (ratio_nonneg >= totalizer_threshold)

        totalizer_rows.append({
            "asset_id": asset_id_str,
            "n_rows": int(len(g)),
            "n_deltas": n_deltas,
            "ratio_delta_ge_0": ratio_nonneg,
            "totalizer_threshold": totalizer_threshold,
            "is_totalizer": bool(is_totalizer),
            "n_negative_deltas": int((d < 0).sum()) if not d.empty else 0,
        })

        # si totalizador: incrementos positivos + daily total
        if is_totalizer:
            inc = delta.copy()
            inc_pos = inc.clip(lower=0)

            # hist incrementos positivos
            hi = histogram_df(inc_pos, bins)
            if not hi.empty:
                hi.insert(0, "asset_id", asset_id_str)
                inc_hist_frames.append(hi)

            # daily total increments
            g3 = g.copy()
            g3["increment_pos"] = inc_pos
            g3 = g3.dropna(subset=["increment_pos"])
            if not g3.empty:
                g3["date"] = g3["timestamp"].dt.floor("D")
                daily_tot = g3.groupby("date")["increment_pos"].sum().reset_index()
                daily_tot.insert(0, "asset_id", asset_id_str)
                daily_tot.rename(columns={"increment_pos": "daily_increment_pos_sum"}, inplace=True)
                daily_total_frames.append(daily_tot)

    summary_df = pd.DataFrame(summary_rows)
    pct_df = pd.concat(pct_frames, ignore_index=True) if pct_frames else pd.DataFrame(columns=["asset_id", "percentile", "value"])
    hist_df = pd.concat(hist_frames, ignore_index=True) if hist_frames else pd.DataFrame(columns=["asset_id","bin_left","bin_right","count"])
    dhist_df = pd.concat(dhist_frames, ignore_index=True) if dhist_frames else pd.DataFrame(columns=["asset_id","bin_left","bin_right","count"])
    totalizer_df = pd.DataFrame(totalizer_rows)
    inc_hist_df = pd.concat(inc_hist_frames, ignore_index=True) if inc_hist_frames else pd.DataFrame(columns=["asset_id","bin_left","bin_right","count"])
    daily_total_df = pd.concat(daily_total_frames, ignore_index=True) if daily_total_frames else pd.DataFrame(columns=["asset_id","date","daily_increment_pos_sum"])
    daily_stats_df = pd.concat(daily_stats_frames, ignore_index=True) if daily_stats_frames else pd.DataFrame(columns=["asset_id","date","min","max","mean"])

    notes = [
        ["asset_filter", asset_filter or "ALL"],
        ["start_date", str(start_date) if start_date is not None else ""],
        ["end_date", str(end_date) if end_date is not None else ""],
        ["bins", str(bins)],
        ["totalizer_threshold", str(totalizer_threshold)],
        ["columns", "asset_id, timestamp, value"],
    ]
    if notes_extra:
        notes.extend(notes_extra)
    notes_df = pd.DataFrame(notes)

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        summary_df.to_excel(w, sheet_name="Summary", index=False)
        pct_df.to_excel(w, sheet_name="Percentiles", index=False)
        hist_df.to_excel(w, sheet_name="Histogram_Value", index=False)
        dhist_df.to_excel(w, sheet_name="Histogram_DeltaAbs", index=False)
        totalizer_df.to_excel(w, sheet_name="TotalizerCheck", index=False)
        inc_hist_df.to_excel(w, sheet_name="Histogram_Increment", index=False)
        daily_total_df.to_excel(w, sheet_name="DailyTotal", index=False)
        daily_stats_df.to_excel(w, sheet_name="DailyStats", index=False)
        notes_df.to_excel(w, sheet_name="Notes", index=False, header=False)

        # Charts: Histogram_Value
        ws_h = w.sheets["Histogram_Value"]
        if not hist_df.empty:
            hd = hist_df.reset_index(drop=True)
            for aid in hd["asset_id"].astype(str).unique().tolist():
                idxs = hd.index[hd["asset_id"].astype(str) == aid]
                if idxs.empty:
                    continue
                start = int(idxs.min()) + 2
                end = int(idxs.max()) + 2
                nrows = end - start + 1
                add_hist_chart(ws_h, start, nrows, f"Hist value - {aid}", f"F{start}")

        # Charts: Histogram_DeltaAbs
        ws_d = w.sheets["Histogram_DeltaAbs"]
        if not dhist_df.empty:
            hd2 = dhist_df.reset_index(drop=True)
            for aid in hd2["asset_id"].astype(str).unique().tolist():
                idxs = hd2.index[hd2["asset_id"].astype(str) == aid]
                if idxs.empty:
                    continue
                start = int(idxs.min()) + 2
                end = int(idxs.max()) + 2
                nrows = end - start + 1
                add_hist_chart(ws_d, start, nrows, f"Hist |delta| - {aid}", f"F{start}")

        # Charts: Histogram_Increment (solo para totalizadores)
        ws_i = w.sheets["Histogram_Increment"]
        if not inc_hist_df.empty:
            hi = inc_hist_df.reset_index(drop=True)
            for aid in hi["asset_id"].astype(str).unique().tolist():
                idxs = hi.index[hi["asset_id"].astype(str) == aid]
                if idxs.empty:
                    continue
                start = int(idxs.min()) + 2
                end = int(idxs.max()) + 2
                nrows = end - start + 1
                add_hist_chart(ws_i, start, nrows, f"Hist increment+ - {aid}", f"F{start}")


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    start_date = parse_date(args.start_date, False, args.freq_minutes)
    end_date = parse_date(args.end_date, True, args.freq_minutes)

    df_all, meta = load_all(input_dir, args.max_files)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    notes_extra = [
        ["detected_timestamp_cols", ", ".join(meta.get("timestamp_cols", []))],
        ["detected_value_cols", ", ".join(meta.get("value_cols", []))],
        ["timestamp_strategies", ", ".join(meta.get("timestamp_strategies", []))],
    ]

    # modo asset único (si asset y no --all)
    if args.asset and not args.all:
        out_path = out_dir / f"macro_caudalimetro_{normalize_asset_filename(args.asset)}_{ts}.xlsx"
        build_report(
            df_all, args.asset, out_path, args.bins, start_date, end_date,
            args.totalizer_threshold, notes_extra
        )
        print(f"[info] Reporte generado: {out_path}")
        return

    # ALL
    out_all = out_dir / f"macro_caudalimetro_ALL_{ts}.xlsx"
    build_report(
        df_all, None, out_all, args.bins, start_date, end_date,
        args.totalizer_threshold, notes_extra
    )
    print(f"[info] Reporte generado: {out_all}")

    # per-asset si --all
    if args.all:
        asset_ids = sorted(df_all["asset_id"].dropna().astype(str).unique().tolist())
        for aid in asset_ids:
            out_path = out_dir / f"macro_caudalimetro_{normalize_asset_filename(aid)}_{ts}.xlsx"
            build_report(
                df_all, aid, out_path, args.bins, start_date, end_date,
                args.totalizer_threshold, notes_extra
            )
            print(f"[info] Reporte generado: {out_path}")


if __name__ == "__main__":
    main()
