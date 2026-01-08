# =============================================================================
# analyze_cross_assets.py  (FASE 4A - Cross-asset indicators)
#
# OBJETIVO
#   Consolidar indicadores comparables entre TODOS los assets y targets:
#   estanques, bombas, presion y macro_caudalimetro. Produce un Excel único
#   con:
#     - Indicators_All: tabla maestra (una fila por asset y por variable/target)
#     - Rankings: rankings rápidos (missing, variabilidad, delta_abs, arranques)
#     - Flags: banderas operativas (OK / REVISAR / CRITICO) con razones
#     - Hojas por grupo: Estanques / Bombas / Presion / Macro
#
# CRITERIOS (resumen)
#   1) Calidad de datos (para todos):
#      - missing_pct = % de NaN en la variable principal
#      - cobertura_dias = (fecha_max - fecha_min) en días
#      - continuidad_pct = n_valid / n_expected (estimado por freq_minutes)
#
#   2) Estabilidad / variabilidad (para series numéricas):
#      - mean, std, p5, p50, p95, range_p95_p5
#      - DeltaAbs = abs(x(t) - x(t-1))  (en la misma unidad que x)
#        * delta_abs_mean, delta_abs_p95
#      - Increment = x(t) - x(t-1)
#        * inc_mean, inc_p95, neg_inc_pct
#
#   3) Bombas (ON/OFF):
#      - on_minutes_total, on_pct
#      - starts_total, starts_per_day_mean
#      - on_duration_mean_min, on_duration_p50_min, on_duration_p95_min
#      - Hist/Excel charts NO aquí (fase 4 es indicadores). Si querés, lo sumamos.
#
#   4) Estanques (nivelPorcentual):
#      - Recargas: detector simple por mínimos/máximos locales con suavizado
#        (similar a analyze_tanks.py)
#      - n_recharges_total, recharge_amp_mean_pct, recharge_dur_mean_min
#
# FLAGS (OK/REVISAR/CRITICO) - reglas simples
#   - missing_pct >= 50% -> CRITICO
#   - missing_pct >= 20% -> REVISAR
#   - continuidad_pct < 50% -> REVISAR
#   - bombas: starts_per_day_mean muy alto o % ON extremadamente alto -> REVISAR
#   - presion/macro: delta_abs_p95 extremadamente alto (outlier por grupo) -> REVISAR
#
# EJEMPLOS DE USO (prompts/comandos)
#   1) Todos los grupos desde output/datos_clean (estructura esperada):
#      python analysis/analyze_cross_assets.py --input-dir output\\datos_clean --freq-minutes 1 \
#         --start-date 2024-01-01 --end-date 2025-12-31
#
#   2) Solo algunos grupos:
#      python analysis/analyze_cross_assets.py --input-dir output\\datos_clean --groups estanques bombas
#
#   3) Output custom:
#      python analysis/analyze_cross_assets.py --input-dir output\\datos_clean --output reports\\fase4_cross.xlsx
# =============================================================================

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from typing import Iterable

import numpy as np
import pandas as pd


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora", "fecha_hora", "fechaHora"]

# Para reusar la heurística que ya usamos con estanques:
def looks_like_time_or_date(series: pd.Series) -> bool:
    """
    Si la mayoría de valores se pueden parsear como datetime/hora (ej '07:30:00'),
    entonces NO sirve como asset_label real.
    """
    s = series.dropna().astype(str).str.strip()
    if s.empty:
        return False
    sample = s.head(5000)
    parsed = pd.to_datetime(sample, errors="coerce", infer_datetime_format=True)
    return float(parsed.notna().mean()) >= 0.80


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
    lower_map = {c.lower(): c for c in df.columns}
    for cand in TIMESTAMP_CANDIDATES:
        if cand in lower_map:
            return lower_map[cand]
    return None


def determine_asset_series(df: pd.DataFrame, file_path: Path) -> pd.Series:
    lower_map = {c.lower(): c for c in df.columns}
    folder_name = file_path.parent.name
    prefix = file_path.stem.split("_")[0]
    fallback = folder_name or prefix

    asset_col = lower_map.get("asset_label")
    if asset_col:
        s = df[asset_col]
        if s.notna().any():
            if looks_like_time_or_date(s):
                return pd.Series([fallback] * len(df), index=df.index)
            s2 = s.astype(str).str.strip()
            s2 = s2.where(s2 != "", fallback)
            return s2

    return pd.Series([fallback] * len(df), index=df.index)


def safe_sheet_name(name: str) -> str:
    name = re.sub(r"[\[\]\*\?/\\:]", "_", str(name))
    name = name.strip()
    return (name[:31] if name else "Sheet")


def normalize_filename(name: str) -> str:
    out = re.sub(r"[^A-Za-z0-9_-]+", "_", str(name).strip())
    out = out.strip("_")
    return out or "asset"


def compute_percentiles(series: pd.Series, pcts: list[int]) -> dict[str, float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {f"p{p}": np.nan for p in pcts}
    values = np.percentile(s.to_numpy(), pcts)
    return {f"p{p}": float(v) for p, v in zip(pcts, values)}


def expected_count(date_min: pd.Timestamp, date_max: pd.Timestamp, freq_minutes: int) -> int:
    if pd.isna(date_min) or pd.isna(date_max):
        return 0
    if date_max < date_min:
        return 0
    minutes = (date_max - date_min).total_seconds() / 60.0
    return int(np.floor(minutes / max(freq_minutes, 1))) + 1


def series_indicators(
    series: pd.Series, timestamps: pd.Series, freq_minutes: int
) -> dict:
    x = pd.to_numeric(series, errors="coerce")
    t = pd.to_datetime(timestamps, errors="coerce")

    valid_mask = x.notna() & t.notna()
    x = x[valid_mask]
    t = t[valid_mask]

    out = {
        "n_total": int(series.shape[0]),
        "n_valid": int(valid_mask.sum()),
        "missing_pct": float((1.0 - (valid_mask.sum() / series.shape[0])) * 100.0) if series.shape[0] else np.nan,
    }

    if x.empty:
        out.update(
            {
                "date_min": pd.NaT,
                "date_max": pd.NaT,
                "coverage_days": np.nan,
                "n_expected": 0,
                "continuity_pct": np.nan,
                "mean": np.nan,
                "std": np.nan,
                "min": np.nan,
                "max": np.nan,
                "range_p95_p5": np.nan,
                "delta_abs_mean": np.nan,
                "delta_abs_p95": np.nan,
                "inc_mean": np.nan,
                "inc_p95": np.nan,
                "neg_inc_pct": np.nan,
            }
        )
        return out

    tmin = t.min()
    tmax = t.max()
    out["date_min"] = tmin
    out["date_max"] = tmax
    out["coverage_days"] = float((tmax - tmin).total_seconds() / 86400.0)

    n_exp = expected_count(tmin, tmax, freq_minutes)
    out["n_expected"] = int(n_exp)
    out["continuity_pct"] = float((x.shape[0] / n_exp) * 100.0) if n_exp else np.nan

    out["mean"] = float(x.mean())
    out["std"] = float(x.std())
    out["min"] = float(x.min())
    out["max"] = float(x.max())

    p = compute_percentiles(x, [5, 50, 95])
    out.update(p)
    out["range_p95_p5"] = float(p["p95"] - p["p5"]) if pd.notna(p["p95"]) and pd.notna(p["p5"]) else np.nan

    # DeltaAbs e Increment
    x_sorted = x.to_numpy()
    # ordenar por tiempo para diffs
    order = np.argsort(t.to_numpy())
    x_sorted = x_sorted[order]

    if x_sorted.size >= 2:
        diff = np.diff(x_sorted)
        delta_abs = np.abs(diff)
        out["delta_abs_mean"] = float(np.mean(delta_abs)) if delta_abs.size else np.nan
        out["delta_abs_p95"] = float(np.percentile(delta_abs, 95)) if delta_abs.size else np.nan

        out["inc_mean"] = float(np.mean(diff)) if diff.size else np.nan
        out["inc_p95"] = float(np.percentile(diff, 95)) if diff.size else np.nan
        out["neg_inc_pct"] = float((diff < 0).mean() * 100.0) if diff.size else np.nan
    else:
        out["delta_abs_mean"] = np.nan
        out["delta_abs_p95"] = np.nan
        out["inc_mean"] = np.nan
        out["inc_p95"] = np.nan
        out["neg_inc_pct"] = np.nan

    return out


# ------------------------
# Bombas: ON/OFF + eventos
# ------------------------
PUMP_STATE_CANDIDATES = [
    "estado", "state", "status", "on", "bomba", "pump", "estadoon", "encendida", "running"
]


def find_pump_state_column(df: pd.DataFrame) -> str | None:
    lower_map = {c.lower(): c for c in df.columns}
    for cand in PUMP_STATE_CANDIDATES:
        if cand in lower_map:
            return lower_map[cand]
    return None


def to_bool_on(series: pd.Series) -> pd.Series:
    s = series.copy()
    # numérico 0/1
    numeric = pd.to_numeric(s, errors="coerce")
    if numeric.notna().mean() > 0.5:
        return (numeric.fillna(0) != 0)

    # strings
    st = s.astype(str).str.strip().str.lower()
    true_set = {"1", "true", "t", "yes", "y", "on", "encendida", "running"}
    false_set = {"0", "false", "f", "no", "n", "off", "apagada", "stopped"}

    out = pd.Series(pd.NA, index=s.index, dtype="boolean")
    out = out.mask(st.isin(true_set), True)
    out = out.mask(st.isin(false_set), False)

    # fallback: si no mapea, intentar parseo simple
    out = out.fillna(False)
    return out.astype(bool)


def pump_indicators(
    is_on: pd.Series, timestamps: pd.Series, freq_minutes: int
) -> dict:
    t = pd.to_datetime(timestamps, errors="coerce")
    on = is_on.astype(bool)

    valid = t.notna()
    t = t[valid]
    on = on[valid]

    out = {
        "date_min": t.min() if not t.empty else pd.NaT,
        "date_max": t.max() if not t.empty else pd.NaT,
    }
    if t.empty:
        out.update(
            {
                "coverage_days": np.nan,
                "on_minutes_total": 0.0,
                "on_pct": np.nan,
                "starts_total": 0,
                "starts_per_day_mean": np.nan,
                "on_duration_mean_min": np.nan,
                "on_duration_p50_min": np.nan,
                "on_duration_p95_min": np.nan,
            }
        )
        return out

    out["coverage_days"] = float((out["date_max"] - out["date_min"]).total_seconds() / 86400.0)

    # Asumimos muestreo regular: cada fila ~ freq_minutes
    minutes_per_row = float(freq_minutes)
    on_minutes = float(on.sum() * minutes_per_row)
    out["on_minutes_total"] = on_minutes
    out["on_pct"] = float(on.mean() * 100.0) if on.size else np.nan

    # Detectar arranques (False -> True)
    on_np = on.to_numpy()
    if on_np.size >= 2:
        starts = int(((~on_np[:-1]) & (on_np[1:])).sum())
    else:
        starts = 0
    out["starts_total"] = starts

    # Arranques por día
    df_tmp = pd.DataFrame({"t": t, "on": on})
    df_tmp["day"] = df_tmp["t"].dt.date
    # arranques diarios
    def daily_starts(g: pd.DataFrame) -> int:
        arr = g["on"].to_numpy()
        if arr.size < 2:
            return 0
        return int(((~arr[:-1]) & (arr[1:])).sum())

    starts_per_day = df_tmp.groupby("day", observed=True).apply(daily_starts)
    out["starts_per_day_mean"] = float(starts_per_day.mean()) if not starts_per_day.empty else np.nan

    # Duraciones de estados ON (run-length)
    durations_min = []
    run = 0
    for v in on_np:
        if v:
            run += 1
        else:
            if run > 0:
                durations_min.append(run * minutes_per_row)
                run = 0
    if run > 0:
        durations_min.append(run * minutes_per_row)

    if durations_min:
        arr = np.array(durations_min, dtype=float)
        out["on_duration_mean_min"] = float(arr.mean())
        out["on_duration_p50_min"] = float(np.percentile(arr, 50))
        out["on_duration_p95_min"] = float(np.percentile(arr, 95))
    else:
        out["on_duration_mean_min"] = np.nan
        out["on_duration_p50_min"] = np.nan
        out["on_duration_p95_min"] = np.nan

    return out


# ------------------------
# Estanques: recargas
# ------------------------
MIN_AMPLITUDE_PCT = 10
MIN_DURATION_MIN = 30


def detect_recharge_events(t: pd.Series, level_pct: pd.Series) -> list[dict]:
    data = pd.DataFrame({"t": pd.to_datetime(t, errors="coerce"), "x": pd.to_numeric(level_pct, errors="coerce")})
    data = data.dropna().sort_values("t")
    if data.shape[0] < 3:
        return []
    smooth = data["x"].rolling(window=5, center=True, min_periods=1).median()
    vals = smooth.to_numpy()
    ts = data["t"].to_numpy()

    minima = []
    maxima = []
    for i in range(1, len(vals) - 1):
        if vals[i - 1] > vals[i] <= vals[i + 1]:
            minima.append(i)
        if vals[i - 1] < vals[i] >= vals[i + 1]:
            maxima.append(i)

    events = []
    max_iter = iter(sorted(maxima))
    try:
        current_max = next(max_iter)
    except StopIteration:
        return []

    for min_i in sorted(minima):
        while current_max <= min_i:
            try:
                current_max = next(max_iter)
            except StopIteration:
                return events

        start = pd.Timestamp(ts[min_i])
        end = pd.Timestamp(ts[current_max])
        dur_min = (end - start).total_seconds() / 60.0
        amp = float(vals[current_max] - vals[min_i])

        if amp >= MIN_AMPLITUDE_PCT and dur_min >= MIN_DURATION_MIN:
            events.append(
                {
                    "start_time": start,
                    "end_time": end,
                    "duration_min": float(dur_min),
                    "amp_pct": float(amp),
                }
            )
        try:
            current_max = next(max_iter)
        except StopIteration:
            break

    return events


# ------------------------
# Detección de grupos y variables
# ------------------------
GROUP_DEFAULTS = ["estanques", "bombas", "presion", "macro_caudalimetro"]


@dataclass
class LoadedFrame:
    group: str
    file: str
    asset_id: str
    timestamp: pd.Series
    df: pd.DataFrame


def load_group_frames(group_dir: Path, group_name: str) -> list[LoadedFrame]:
    frames: list[LoadedFrame] = []
    csvs = sorted(group_dir.rglob("*.csv"))
    for fp in csvs:
        try:
            df = pd.read_csv(fp)
        except Exception:
            continue
        ts_col = find_timestamp_column(df)
        if not ts_col:
            continue
        t, _ = parse_timestamp(df[ts_col])
        asset = determine_asset_series(df, fp)
        # dejamos df crudo, pero agregamos series parseadas
        frames.append(
            LoadedFrame(
                group=group_name,
                file=str(fp),
                asset_id=str(asset.iloc[0]) if len(asset) else fp.parent.name,
                timestamp=t,
                df=df,
            )
        )
    return frames


def pick_numeric_columns(df: pd.DataFrame, exclude: set[str]) -> list[str]:
    cols = []
    for c in df.columns:
        if c in exclude:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().any():
            cols.append(c)
    return cols


def flag_row(row: dict, group_name: str, group_stats: dict) -> tuple[str, str]:
    reasons = []

    missing = row.get("missing_pct")
    cont = row.get("continuity_pct")

    if pd.notna(missing):
        if missing >= 50:
            reasons.append(f"missing_pct={missing:.1f}% (>=50%)")
        elif missing >= 20:
            reasons.append(f"missing_pct={missing:.1f}% (>=20%)")

    if pd.notna(cont) and cont < 50:
        reasons.append(f"continuity_pct={cont:.1f}% (<50%)")

    # pumps rules
    if group_name == "bombas":
        spd = row.get("starts_per_day_mean")
        onpct = row.get("on_pct")
        if pd.notna(spd) and spd >= 50:
            reasons.append(f"starts_per_day_mean={spd:.1f} (alto)")
        if pd.notna(onpct) and onpct >= 90:
            reasons.append(f"on_pct={onpct:.1f}% (muy alto)")

    # delta_abs outlier vs group
    d95 = row.get("delta_abs_p95")
    if pd.notna(d95):
        mu = group_stats.get("delta_abs_p95_mean")
        sig = group_stats.get("delta_abs_p95_std")
        if pd.notna(mu) and pd.notna(sig) and sig > 0:
            z = (d95 - mu) / sig
            if z >= 3.0:
                reasons.append(f"delta_abs_p95 z={z:.1f} (outlier)")

    if reasons:
        # crítico si missing >= 50 o múltiples razones severas
        if any(">=50%" in r for r in reasons):
            return "CRITICO", "; ".join(reasons)
        return "REVISAR", "; ".join(reasons)

    return "OK", ""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FASE 4A: indicadores cross-asset (estanques/bombas/presion/macro).")
    p.add_argument("--input-dir", required=True, help="Carpeta base con subcarpetas de grupos (ej: output\\datos_clean)")
    p.add_argument("--groups", nargs="*", default=None, help=f"Grupos a incluir. Default: {GROUP_DEFAULTS}")
    p.add_argument("--start-date", help="YYYY-MM-DD")
    p.add_argument("--end-date", help="YYYY-MM-DD")
    p.add_argument("--freq-minutes", type=int, default=1, help="Frecuencia esperada (min)")
    p.add_argument("--output", help="Excel de salida (default reports/fase4_cross_*.xlsx)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    base = Path(args.input_dir)
    if not base.exists():
        raise SystemExit(f"[error] input-dir no existe: {base}")

    groups = args.groups if args.groups else GROUP_DEFAULTS
    start_date = parse_date(args.start_date, False, args.freq_minutes)
    end_date = parse_date(args.end_date, True, args.freq_minutes)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.output) if args.output else Path("reports") / f"fase4_cross_{run_ts}.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) cargar
    loaded: list[LoadedFrame] = []
    for g in groups:
        gdir = base / g
        if not gdir.exists():
            print(f"[warning] No existe grupo: {gdir} (skip)")
            continue
        frames = load_group_frames(gdir, g)
        if not frames:
            print(f"[warning] Grupo {g} sin CSVs legibles")
            continue
        loaded.extend(frames)

    if not loaded:
        raise SystemExit("[error] No se cargó ningún CSV. Revisar input-dir y subcarpetas.")

    # 2) construir indicadores
    rows_all: list[dict] = []

    for lf in loaded:
        df = lf.df.copy()
        t = lf.timestamp
        asset = determine_asset_series(df, Path(lf.file))

        # aplicar rango fechas
        mask = t.notna()
        if start_date is not None:
            mask &= (t >= start_date)
        if end_date is not None:
            mask &= (t <= end_date)
        if not mask.any():
            continue

        df = df.loc[mask].copy()
        t = t.loc[mask].copy()
        asset = asset.loc[mask].copy()
        df["__asset_id"] = asset.astype(str)
        df["__t"] = t

        group_name = lf.group

        # procesar por asset_id
        for asset_id, gdf in df.groupby("__asset_id", sort=False):
            tser = gdf["__t"]

            # bombas: buscar columna estado
            if group_name == "bombas":
                state_col = find_pump_state_column(gdf)
                if not state_col:
                    # si no hay estado, igual dejamos calidad básica (sin metrics ON)
                    # intentar tomar primera columna numérica como fallback
                    exclude = {"__asset_id", "__t"}
                    numeric_cols = pick_numeric_columns(gdf, exclude)
                    var = numeric_cols[0] if numeric_cols else None
                    base_row = {
                        "group": group_name,
                        "asset_id": asset_id,
                        "variable": "pump_state",
                        "source": "raw",
                    }
                    if var:
                        ind = series_indicators(gdf[var], tser, args.freq_minutes)
                        rows_all.append({**base_row, **ind})
                    else:
                        rows_all.append({**base_row})
                    continue

                is_on = to_bool_on(gdf[state_col])
                pind = pump_indicators(is_on, tser, args.freq_minutes)

                rows_all.append(
                    {
                        "group": group_name,
                        "asset_id": asset_id,
                        "variable": "pump_state (ON/OFF)",
                        "units": "minutes / counts",
                        **pind,
                        # placeholders para compatibilidad columnas:
                        "n_total": int(gdf.shape[0]),
                        "n_valid": int(tser.notna().sum()),
                        "missing_pct": np.nan,
                        "n_expected": expected_count(pind["date_min"], pind["date_max"], args.freq_minutes) if pd.notna(pind["date_min"]) else 0,
                        "continuity_pct": np.nan,
                        "mean": np.nan,
                        "std": np.nan,
                        "min": np.nan,
                        "max": np.nan,
                        "p5": np.nan,
                        "p50": np.nan,
                        "p95": np.nan,
                        "range_p95_p5": np.nan,
                        "delta_abs_mean": np.nan,
                        "delta_abs_p95": np.nan,
                        "inc_mean": np.nan,
                        "inc_p95": np.nan,
                        "neg_inc_pct": np.nan,
                        "n_recharges_total": np.nan,
                        "recharge_amp_mean_pct": np.nan,
                        "recharge_dur_mean_min": np.nan,
                    }
                )
                continue

            # estanques: priorizar nivelPorcentual
            if group_name == "estanques":
                # localizar columna nivelPorcentual / nivelporcentual
                lower_map = {c.lower(): c for c in gdf.columns}
                col_pct = lower_map.get("nivelporcentual")
                base_row = {
                    "group": group_name,
                    "asset_id": asset_id,
                    "variable": "nivelPorcentual",
                    "units": "percent",
                }
                if not col_pct:
                    # fallback numérico
                    exclude = {"__asset_id", "__t"}
                    numeric_cols = pick_numeric_columns(gdf, exclude)
                    col_pct = numeric_cols[0] if numeric_cols else None
                    base_row["variable"] = col_pct or "unknown_numeric"
                if not col_pct:
                    rows_all.append(base_row)
                    continue

                ind = series_indicators(gdf[col_pct], tser, args.freq_minutes)

                # recargas
                events = detect_recharge_events(tser, gdf[col_pct])
                if events:
                    amps = np.array([e["amp_pct"] for e in events], dtype=float)
                    durs = np.array([e["duration_min"] for e in events], dtype=float)
                    n_rech = int(len(events))
                    amp_mean = float(np.mean(amps)) if amps.size else np.nan
                    dur_mean = float(np.mean(durs)) if durs.size else np.nan
                else:
                    n_rech, amp_mean, dur_mean = 0, np.nan, np.nan

                rows_all.append(
                    {
                        **base_row,
                        **ind,
                        "n_recharges_total": n_rech,
                        "recharge_amp_mean_pct": amp_mean,
                        "recharge_dur_mean_min": dur_mean,
                        # placeholders para columnas bombas:
                        "on_minutes_total": np.nan,
                        "on_pct": np.nan,
                        "starts_total": np.nan,
                        "starts_per_day_mean": np.nan,
                        "on_duration_mean_min": np.nan,
                        "on_duration_p50_min": np.nan,
                        "on_duration_p95_min": np.nan,
                    }
                )
                continue

            # presion / macro: usar todas las columnas numéricas relevantes (1+)
            exclude = {"__asset_id", "__t"}
            numeric_cols = pick_numeric_columns(gdf, exclude)
            if not numeric_cols:
                rows_all.append({"group": group_name, "asset_id": asset_id, "variable": "no_numeric"})
                continue

            # Para mantener Excel legible: priorizar por nombres conocidos, luego el resto
            priority = []
            for cand in ["presion", "pressure", "caudal", "volumenagua", "volumenaguaplot", "volumen", "flow"]:
                for c in numeric_cols:
                    if cand in c.lower() and c not in priority:
                        priority.append(c)
            for c in numeric_cols:
                if c not in priority:
                    priority.append(c)

            for col in priority[:5]:  # límite para no explotar si hay 20 columnas
                ind = series_indicators(gdf[col], tser, args.freq_minutes)
                rows_all.append(
                    {
                        "group": group_name,
                        "asset_id": asset_id,
                        "variable": col,
                        "units": "native",
                        **ind,
                        "n_recharges_total": np.nan,
                        "recharge_amp_mean_pct": np.nan,
                        "recharge_dur_mean_min": np.nan,
                        "on_minutes_total": np.nan,
                        "on_pct": np.nan,
                        "starts_total": np.nan,
                        "starts_per_day_mean": np.nan,
                        "on_duration_mean_min": np.nan,
                        "on_duration_p50_min": np.nan,
                        "on_duration_p95_min": np.nan,
                    }
                )

    indicators = pd.DataFrame(rows_all)

    if indicators.empty:
        raise SystemExit("[error] No quedaron filas luego de filtrar por fechas/rangos.")

    # 3) stats por grupo (para flags)
    group_stats = {}
    for g, gdf in indicators.groupby("group", observed=True):
        d = pd.to_numeric(gdf.get("delta_abs_p95"), errors="coerce")
        group_stats[g] = {
            "delta_abs_p95_mean": float(d.mean()) if d.notna().any() else np.nan,
            "delta_abs_p95_std": float(d.std()) if d.notna().any() else np.nan,
        }

    # 4) flags
    flags_rows = []
    for rec in indicators.to_dict("records"):
        g = rec.get("group", "")
        status, reason = flag_row(rec, g, group_stats.get(g, {}))
        flags_rows.append(
            {
                "group": g,
                "asset_id": rec.get("asset_id"),
                "variable": rec.get("variable"),
                "flag": status,
                "reason": reason,
            }
        )
    flags_df = pd.DataFrame(flags_rows)

    # 5) rankings (simples, top 20)
    def top_rank(df: pd.DataFrame, metric: str, ascending: bool, n: int = 20) -> pd.DataFrame:
        tmp = df.copy()
        tmp[metric] = pd.to_numeric(tmp.get(metric), errors="coerce")
        tmp = tmp[tmp[metric].notna()]
        tmp = tmp.sort_values(metric, ascending=ascending).head(n)
        return tmp[["group", "asset_id", "variable", metric]]

    rankings = []
    for metric, asc in [
        ("missing_pct", False),
        ("std", False),
        ("range_p95_p5", False),
        ("delta_abs_p95", False),
        ("neg_inc_pct", False),
        ("starts_per_day_mean", False),
        ("on_pct", False),
    ]:
        if metric in indicators.columns:
            r = top_rank(indicators, metric, asc, 25)
            if not r.empty:
                r.insert(0, "ranking", metric)
                rankings.append(r)

    rankings_df = pd.concat(rankings, ignore_index=True) if rankings else pd.DataFrame(
        columns=["ranking", "group", "asset_id", "variable", "value"]
    )

    # 6) hojas por grupo
    sheets = {
        "Indicators_All": indicators,
        "Flags": flags_df,
        "Rankings": rankings_df,
    }
    for g, gdf in indicators.groupby("group", observed=True):
        sheets[safe_sheet_name(g.capitalize())] = gdf

    # 7) export Excel
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=safe_sheet_name(name), index=False)

        # Notes sheet
        notes = [
            ["input_dir", str(base)],
            ["groups", ", ".join(groups)],
            ["start_date", str(start_date) if start_date else ""],
            ["end_date", str(end_date) if end_date else ""],
            ["freq_minutes", str(args.freq_minutes)],
            ["MIN_AMPLITUDE_PCT (recharges)", str(MIN_AMPLITUDE_PCT)],
            ["MIN_DURATION_MIN (recharges)", str(MIN_DURATION_MIN)],
            ["generated", run_ts],
        ]
        pd.DataFrame(notes).to_excel(writer, sheet_name="Notes", index=False, header=False)

    print(f"[info] Fase 4A OK -> {out_path}")


if __name__ == "__main__":
    main()
