# Funciona en ambiente la_aurora
#
# =========================
# analyze_pumps.py (FULL)
# =========================
# Qué hace:
# - Lee CSVs de bombas desde --input-dir (recursivo).
# - Detecta y parsea timestamp.
# - Determina asset_id de forma robusta (evita asset_label contaminado que parece hora/fecha).
# - Determina estado ON/OFF:
#     * Si existe columna tipo estadoOn/estado/on/... la usa.
#     * Si NO existe estado pero existe timeOn/ontime/... infiere ON como (timeOn > 0).
# - Calcula, por bomba:
#     * Ciclos (runs) ON/OFF: start, end, duration_minutes
#     * Histograma de duración ON (min)
#     * Arranques por día (# starts/day) + histograma
#     * Minutos ON por día (sum)
# - Exporta Excel con pestañas + gráficos.
#
# Importante (para ALL):
# - El Excel ALL incluye una hoja "SkippedFiles" que indica qué archivos se descartaron y por qué,
#   para que puedas ver exactamente qué bombas faltan y corregir la limpieza/detección de columnas.
#
# Cómo correr:
#   python analysis\analyze_pumps.py --input-dir output\datos_clean\bombas --all --start-date 2024-01-01 --end-date 2025-12-31
#
# Debug:
#   python analysis\analyze_pumps.py --input-dir output\datos_clean\bombas --all --start-date 2024-01-01 --end-date 2025-12-31 --debug

import argparse
from datetime import datetime
from pathlib import Path
import re

import numpy as np
import pandas as pd
from openpyxl.chart import BarChart, Reference
from openpyxl.utils import get_column_letter


# --- Detección de timestamp ---
TIMESTAMP_CANDIDATES = [
    "timestamp", "ts", "datetime", "fechahora", "fecha_hora", "fecha hora",
    "date", "time", "fecha", "hora"
]

# --- Detección de estado ON/OFF (ampliado) ---
STATE_CANDIDATES = [
    "estadoon", "estado_on", "on", "ison", "state", "estado", "pump_on",
    "bomba_on", "bombon", "estado_bomba", "estadobomba", "pumpstate", "pump_state",
    "modo", "mode", "status", "running", "run"
]

# --- Detección de timeOn (fallback) ---
TIMEON_CANDIDATES = [
    "timeon", "tiempoon", "tiempo_on", "ontime", "on_time",
    "on_minutes", "on_min", "on_mins", "on_seconds", "on_sec", "on_secs",
    "tiempo_encendido", "tiempo encendido", "minutos_on", "segundos_on"
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analiza bombas (ciclos ON/OFF) y exporta reportes a Excel.")
    p.add_argument("--input-dir", required=True, help="Carpeta base con CSVs limpios de bombas (recursivo)")
    p.add_argument("--asset", help="Filtra por asset (contains, case-insensitive)")
    p.add_argument("--all", action="store_true", default=False, help="Procesa todos los assets detectados")
    p.add_argument("--per-asset", action="store_true", default=False, help="Genera un Excel por asset en reports/")
    p.add_argument("--start-date", help="Fecha de inicio (YYYY-MM-DD)")
    p.add_argument("--end-date", help="Fecha de fin (YYYY-MM-DD)")
    p.add_argument("--freq-minutes", type=int, default=1, help="Frecuencia esperada (min) para heurísticas")
    p.add_argument("--output", help="Ruta de salida Excel (si NO usás --all/--per-asset)")
    p.add_argument("--debug", action="store_true", help="Imprime detalles de columnas detectadas y descartes")
    return p.parse_args()


def normalize_asset_filename(asset_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "_", str(asset_id).strip())
    return normalized.strip("_") or "asset"


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


def find_column_case_insensitive(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in lower_map:
            return lower_map[key]
    return None


def looks_like_time_or_date(series: pd.Series) -> bool:
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return False
    sample = s.head(5000)
    parsed = pd.to_datetime(sample, errors="coerce")
    return float(parsed.notna().mean()) >= 0.80


def determine_asset_series(df: pd.DataFrame, file_path: Path) -> pd.Series:
    """
    Prioridad:
    - asset_label / asset_name / asset_id si existe y NO está contaminado con "horas/fechas"
    - fallback: nombre de carpeta contenedora
    - fallback 2: prefijo del archivo
    """
    lower_map = {col.lower().strip(): col for col in df.columns}
    folder_name = file_path.parent.name
    prefix = file_path.stem.split("_")[0]
    fallback = folder_name or prefix

    asset_col = (
        lower_map.get("asset_label")
        or lower_map.get("asset_name")
        or lower_map.get("asset_id")
        or lower_map.get("bomba")
        or lower_map.get("pump")
    )
    if asset_col:
        series = df[asset_col]
        if series.notna().any():
            if looks_like_time_or_date(series):
                return pd.Series([fallback] * len(df), index=df.index)
            s = series.astype(str).str.strip()
            s = s.where(s != "", fallback)
            return s

    return pd.Series([fallback] * len(df), index=df.index)


def to_bool_state(series: pd.Series) -> pd.Series:
    """
    Convierte columna de estado a boolean.
    """
    if pd.api.types.is_numeric_dtype(series):
        s = pd.to_numeric(series, errors="coerce")
        return (s.fillna(0) > 0)

    s = series.astype(str).str.strip().str.lower()

    true_set = {"1", "true", "on", "yes", "si", "sí", "encendida", "encendido", "running", "run"}
    false_set = {"0", "false", "off", "no", "apagada", "apagado", "stopped", "stop"}

    out = pd.Series(pd.NA, index=series.index, dtype="boolean")
    out = out.mask(s.isin(true_set), True)
    out = out.mask(s.isin(false_set), False)

    # fallback: numérico embebido
    numeric = pd.to_numeric(series, errors="coerce")
    out = out.mask(numeric.notna() & (numeric > 0), True)
    out = out.mask(numeric.notna() & (numeric <= 0), False)
    return out


def infer_timeon_minutes(df: pd.DataFrame, timeon_col: str) -> pd.Series:
    """
    Lee timeOn y lo expresa en minutos (heurística por nombre).
    """
    s = pd.to_numeric(df[timeon_col], errors="coerce")
    name = str(timeon_col).lower()
    if "sec" in name or "seg" in name or "seconds" in name:
        return s / 60.0
    # default: minutos
    return s


def infer_state_from_timeon(df: pd.DataFrame, timeon_col: str) -> pd.Series:
    """
    Fallback cuando no hay estado:
    ON si timeOn_minutes > 0
    """
    minutes = infer_timeon_minutes(df, timeon_col)
    return (minutes.fillna(0) > 0)


def load_pump_csv(file_path: Path, debug: bool = False) -> tuple[pd.DataFrame | None, str]:
    try:
        df = pd.read_csv(file_path)
    except Exception as exc:
        return None, f"read_failed: {exc}"

    ts_col = find_column_case_insensitive(df, TIMESTAMP_CANDIDATES)
    if not ts_col:
        if debug:
            print(f"[debug] {file_path}: sin timestamp. cols={list(df.columns)}")
        return None, "no_timestamp"

    df = df.copy()
    df["timestamp"], strategy = parse_timestamp(df[ts_col])
    df = df[df["timestamp"].notna()].copy()
    if df.empty:
        return None, f"all_bad_timestamps({strategy})"

    df["asset_id"] = determine_asset_series(df, file_path)

    # Estado ON/OFF
    st_col = find_column_case_insensitive(df, STATE_CANDIDATES)
    if st_col:
        state = to_bool_state(df[st_col])
        if state.notna().any():
            df["state_on"] = state.fillna(False).astype(bool)
        else:
            st_col = None  # fuerza fallback

    if not st_col:
        t_col = find_column_case_insensitive(df, TIMEON_CANDIDATES)
        if t_col:
            df["state_on"] = infer_state_from_timeon(df, t_col).astype(bool)
        else:
            if debug:
                print(f"[debug] {file_path}: sin estado ni timeOn. cols={list(df.columns)}")
            return None, "no_state_columns"

    return df[["asset_id", "timestamp", "state_on"]], "ok"


def apply_filters(df: pd.DataFrame, asset_pattern: str | None, start_date, end_date) -> pd.DataFrame:
    out = df
    if asset_pattern:
        out = out[out["asset_id"].astype(str).str.contains(asset_pattern, case=False, na=False)]
    if start_date is not None:
        out = out[out["timestamp"] >= start_date]
    if end_date is not None:
        out = out[out["timestamp"] <= end_date]
    return out


def compute_runs(asset_df: pd.DataFrame) -> pd.DataFrame:
    """
    Tabla de runs: start_time, end_time, duration_minutes, state_on, date
    """
    if asset_df.empty:
        return pd.DataFrame(columns=["asset_id", "start_time", "end_time", "duration_minutes", "state_on", "date"])

    df = asset_df.sort_values("timestamp").copy()
    change = df["state_on"].ne(df["state_on"].shift(1, fill_value=df["state_on"].iloc[0]))
    run_id = change.cumsum()

    grouped = df.groupby(run_id, sort=False, observed=True)
    runs = grouped.agg(
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        state_on=("state_on", "first"),
    ).reset_index(drop=True)

    runs["duration_minutes"] = (runs["end_time"] - runs["start_time"]).dt.total_seconds() / 60.0
    runs["date"] = runs["start_time"].dt.date
    return runs


def histogram_table(values: pd.Series, bins: int, label_name: str) -> pd.DataFrame:
    v = pd.to_numeric(values, errors="coerce").dropna()
    if v.empty:
        return pd.DataFrame(columns=["bin_left", "bin_right", "count", label_name])
    counts, edges = np.histogram(v.to_numpy(), bins=bins)
    out = pd.DataFrame({"bin_left": edges[:-1], "bin_right": edges[1:], "count": counts})
    out[label_name] = out["bin_left"].round(3).astype(str) + "–" + out["bin_right"].round(3).astype(str)
    return out[["bin_left", "bin_right", "count", label_name]]


def add_bar_chart(ws, title: str, cat_col: int, val_col: int, start_row: int, n_rows: int, anchor: str) -> None:
    if n_rows <= 0:
        return
    chart = BarChart()
    chart.title = title
    chart.height = 8
    chart.width = 16
    data = Reference(ws, min_col=val_col, min_row=start_row, max_row=start_row + n_rows)
    cats = Reference(ws, min_col=cat_col, min_row=start_row + 1, max_row=start_row + n_rows)
    chart.add_data(data, titles_from_data=True)
    chart.set_categories(cats)
    ws.add_chart(chart, anchor)


def summarize_asset(asset_id: str, df_asset: pd.DataFrame) -> dict:
    df_asset = df_asset.sort_values("timestamp")
    date_min = df_asset["timestamp"].min() if not df_asset.empty else pd.NaT
    date_max = df_asset["timestamp"].max() if not df_asset.empty else pd.NaT

    runs = compute_runs(df_asset)
    on_runs = runs[runs["state_on"] == True].copy()
    off_runs = runs[runs["state_on"] == False].copy()

    total_on = float(on_runs["duration_minutes"].sum()) if not on_runs.empty else 0.0
    total_off = float(off_runs["duration_minutes"].sum()) if not off_runs.empty else 0.0

    state = df_asset["state_on"].astype(bool).to_numpy()
    starts = int(np.sum((state[1:] == True) & (state[:-1] == False))) if len(state) >= 2 else 0

    # arranques por día y on minutes por día
    df_day = df_asset.copy()
    df_day["date"] = df_day["timestamp"].dt.date
    prev = df_day["state_on"].shift(1, fill_value=df_day["state_on"].iloc[0])
    is_start = (df_day["state_on"] == True) & (prev == False)
    starts_by_day = is_start.groupby(df_day["date"]).sum().astype(int)

    # minutos ON por día: sum de runs ON por fecha de inicio (aprox simple)
    on_by_day = on_runs.groupby("date")["duration_minutes"].sum() if not on_runs.empty else pd.Series(dtype=float)

    starts_mean = float(starts_by_day.mean()) if len(starts_by_day) else np.nan
    starts_med = float(starts_by_day.median()) if len(starts_by_day) else np.nan

    on_med = float(on_runs["duration_minutes"].median()) if not on_runs.empty else np.nan

    return {
        "asset_id": asset_id,
        "n_rows": int(df_asset.shape[0]),
        "date_min": date_min,
        "date_max": date_max,
        "total_on_minutes": total_on,
        "total_off_minutes": total_off,
        "n_starts": starts,
        "starts_per_day_mean": starts_mean,
        "starts_per_day_median": starts_med,
        "on_duration_minutes_median": on_med,
    }


def build_excel_report(scope_name: str, df: pd.DataFrame, output_path: Path, skipped_df: pd.DataFrame | None) -> None:
    """
    scope_name:
      - "ALL"  => Summary por asset
      - asset_id => Summary 1 fila
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if df.empty:
        summary_df = pd.DataFrame([{
            "asset_id": scope_name,
            "n_rows": 0,
            "date_min": "",
            "date_max": "",
            "total_on_minutes": 0.0,
            "total_off_minutes": 0.0,
            "n_starts": 0,
            "starts_per_day_mean": np.nan,
            "starts_per_day_median": np.nan,
            "on_duration_minutes_median": np.nan,
        }])
        runs_df = pd.DataFrame(columns=["asset_id", "start_time", "end_time", "duration_minutes", "state_on", "date"])
        daily_df = pd.DataFrame(columns=["asset_id", "date", "starts_per_day", "on_minutes_per_day"])
        on_hist_df = pd.DataFrame(columns=["bin_left", "bin_right", "count", "range_minutes"])
        starts_hist_df = pd.DataFrame(columns=["bin_left", "bin_right", "count", "range_starts_per_day"])
    else:
        df = df.sort_values(["asset_id", "timestamp"]).copy()

        # Summary
        if scope_name == "ALL":
            rows = []
            for asset_id, g in df.groupby("asset_id", sort=False, observed=True):
                rows.append(summarize_asset(str(asset_id), g))
            summary_df = pd.DataFrame(rows).sort_values("asset_id")
        else:
            summary_df = pd.DataFrame([summarize_asset(scope_name, df)])

        # Runs (todos)
        runs_parts = []
        daily_parts = []
        on_durations_all = []
        starts_by_day_all = []

        for asset_id, g in df.groupby("asset_id", sort=False, observed=True):
            runs = compute_runs(g)
            if not runs.empty:
                runs.insert(0, "asset_id", str(asset_id))
                runs_parts.append(runs)

            # daily
            g2 = g.sort_values("timestamp").copy()
            g2["date"] = g2["timestamp"].dt.date
            prev = g2["state_on"].shift(1, fill_value=g2["state_on"].iloc[0])
            is_start = (g2["state_on"] == True) & (prev == False)
            starts_by_day = is_start.groupby(g2["date"]).sum().astype(int)

            on_runs = runs[runs["state_on"] == True].copy()
            on_by_day = on_runs.groupby("date")["duration_minutes"].sum() if not on_runs.empty else pd.Series(dtype=float)

            daily = pd.DataFrame({
                "date": starts_by_day.index.astype(object),
                "starts_per_day": starts_by_day.values,
            })
            daily["on_minutes_per_day"] = daily["date"].map(on_by_day).fillna(0.0).astype(float)
            daily.insert(0, "asset_id", str(asset_id))
            daily_parts.append(daily)

            # para histogramas ALL (distribución global)
            if not on_runs.empty:
                on_durations_all.append(on_runs["duration_minutes"])
            if len(starts_by_day):
                starts_by_day_all.append(starts_by_day.astype(float))

        runs_df = pd.concat(runs_parts, ignore_index=True) if runs_parts else pd.DataFrame(
            columns=["asset_id", "start_time", "end_time", "duration_minutes", "state_on", "date"]
        )
        daily_df = pd.concat(daily_parts, ignore_index=True) if daily_parts else pd.DataFrame(
            columns=["asset_id", "date", "starts_per_day", "on_minutes_per_day"]
        )

        # Histogramas
        # - Si scope=ALL => histogramas globales
        # - Si scope=asset => histogramas del asset
        if scope_name == "ALL":
            on_vals = pd.concat(on_durations_all, ignore_index=True) if on_durations_all else pd.Series(dtype=float)
            st_vals = pd.concat(starts_by_day_all, ignore_index=True) if starts_by_day_all else pd.Series(dtype=float)
        else:
            rr = compute_runs(df)
            on_vals = rr.loc[rr["state_on"] == True, "duration_minutes"] if not rr.empty else pd.Series(dtype=float)
            # starts/day del asset
            df_day = df.copy()
            df_day["date"] = df_day["timestamp"].dt.date
            prev = df_day["state_on"].shift(1, fill_value=df_day["state_on"].iloc[0])
            is_start = (df_day["state_on"] == True) & (prev == False)
            st_vals = is_start.groupby(df_day["date"]).sum().astype(float)

        on_hist_df = histogram_table(on_vals, bins=20, label_name="range_minutes")
        starts_hist_df = histogram_table(st_vals, bins=15, label_name="range_starts_per_day")

    notes_df = pd.DataFrame(
        [
            ["scope", scope_name],
            ["generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            ["units_total_on_minutes", "minutes"],
            ["units_total_off_minutes", "minutes"],
            ["units_on_duration_minutes_median", "minutes"],
            ["units_starts_per_day_mean", "starts/day"],
            ["units_starts_per_day_median", "starts/day"],
        ],
        columns=["parameter", "value"],
    )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        runs_df.to_excel(writer, sheet_name="Runs", index=False)
        daily_df.to_excel(writer, sheet_name="Daily", index=False)
        on_hist_df.to_excel(writer, sheet_name="Hist_ON_duration_min", index=False)
        starts_hist_df.to_excel(writer, sheet_name="Hist_Starts_per_day", index=False)
        notes_df.to_excel(writer, sheet_name="Notes", index=False)

        if skipped_df is not None:
            skipped_df.to_excel(writer, sheet_name="SkippedFiles", index=False)

        # Gráficos de histogramas
        ws_on = writer.sheets["Hist_ON_duration_min"]
        add_bar_chart(
            ws_on,
            title="Histograma duración ON (min)",
            cat_col=4,  # range_minutes
            val_col=3,  # count
            start_row=1,
            n_rows=on_hist_df.shape[0],
            anchor="F2",
        )

        ws_st = writer.sheets["Hist_Starts_per_day"]
        add_bar_chart(
            ws_st,
            title="Histograma # encendidos por día",
            cat_col=4,  # range_starts_per_day
            val_col=3,  # count
            start_row=1,
            n_rows=starts_hist_df.shape[0],
            anchor="F2",
        )


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise SystemExit(f"[error] input-dir no existe: {input_dir}")

    if not args.asset and not args.all and not args.per_asset:
        args.all = True

    start_date = parse_date(args.start_date, False, args.freq_minutes)
    end_date = parse_date(args.end_date, True, args.freq_minutes)

    csv_paths = sorted(input_dir.rglob("*.csv"))
    if not csv_paths:
        raise SystemExit(f"[error] No se encontraron CSVs en: {input_dir}")

    dfs: list[pd.DataFrame] = []
    skipped: list[dict] = []

    for fp in csv_paths:
        df, status = load_pump_csv(fp, debug=args.debug)
        if df is None:
            skipped.append({"file": str(fp), "status": status})
            continue

        # filtros por fecha/asset (asset solo si el usuario lo pide)
        df = apply_filters(df, args.asset, start_date, end_date)
        if df.empty:
            skipped.append({"file": str(fp), "status": "filtered_empty"})
            continue

        dfs.append(df)

    skipped_df = pd.DataFrame(skipped) if skipped else pd.DataFrame(columns=["file", "status"])

    if not dfs:
        print("[error] No quedó ningún dataframe para analizar (dfs vacío).")
        print(f"[info] CSVs encontrados: {len(csv_paths)}")
        if not skipped_df.empty:
            print("[info] Motivos de descarte (primeros 30):")
            for row in skipped_df.head(30).to_dict("records"):
                print(f"  - {row['file']} :: {row['status']}")
        raise SystemExit(2)

    combined = pd.concat(dfs, ignore_index=True)
    combined["asset_id"] = combined["asset_id"].astype(str)

    asset_ids = sorted(combined["asset_id"].dropna().unique().tolist())
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    # --all => ALL + por asset
    if args.all:
        out_all = reports_dir / f"pumps_ALL_{timestamp}.xlsx"
        build_excel_report("ALL", combined, out_all, skipped_df)
        print(f"[info] Reporte ALL generado: {out_all}")

        for asset_id in asset_ids:
            df_a = combined[combined["asset_id"] == asset_id].copy()
            safe = normalize_asset_filename(asset_id)
            out_a = reports_dir / f"pumps_{safe}_{timestamp}.xlsx"
            build_excel_report(asset_id, df_a, out_a, None)
            print(f"[info] Reporte asset generado: {out_a}")
        return

    # --per-asset
    if args.per_asset:
        for asset_id in asset_ids:
            df_a = combined[combined["asset_id"] == asset_id].copy()
            safe = normalize_asset_filename(asset_id)
            out_a = reports_dir / f"pumps_{safe}_{timestamp}.xlsx"
            build_excel_report(asset_id, df_a, out_a, None)
            print(f"[info] Reporte asset generado: {out_a}")
        return

    # uno solo
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = reports_dir / f"pumps_{timestamp}.xlsx"
    scope = args.asset or "ALL"
    build_excel_report(scope, combined, output_path, skipped_df)
    print(f"[info] Reporte generado: {output_path}")


if __name__ == "__main__":
    main()
