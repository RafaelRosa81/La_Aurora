# Funciona en ambiente la_aurora
"""
find_outliers_pumps.py

Criterios de outliers / anomalías (sin caudal ni potencia):

A) Timestamp
   1) timestamp inválido (no parseable) -> se descarta
   2) timestamps duplicados dentro del mismo asset -> se reporta (opcionalmente drop)
   3) gaps grandes: delta_timestamp > (freq_minutes * gap_multiplier) -> se reporta (NO drop por defecto)

B) estadoOn (cuando existe y no está todo vacío)
   1) valores fuera de {0, 1} -> outlier -> plan: drop
   2) estadoOn NaN: se permite, pero se reporta % missing

C) timeOn (segundos de operación acumulados o contador relacionado al ON)
   1) timeOn no numérico -> NaN (se reporta)
   2) timeOn < 0 -> outlier -> plan: drop
   3) timeOn saltos imposibles:
      - diff_timeOn muy grande respecto al tiempo transcurrido (delta_ts) -> outlier -> plan: drop
      - diff_timeOn > max_timeon_jump_seconds -> outlier -> plan: drop
   4) incoherencia con estadoOn (si estadoOn está disponible):
      - estadoOn == 0 y timeOn aumenta -> outlier -> plan: drop
      - estadoOn == 1 y timeOn baja fuerte sin “reset” razonable -> outlier -> plan: drop

Nota: no se “limpia el mes”; el plan apunta a filas puntuales por regla.

python analysis\find_outliers_pumps.py --input-dir output\datos_def\bombas --group bombas --all

"""

import argparse
from datetime import datetime
from pathlib import Path
import itertools
import re

import numpy as np
import pandas as pd


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora"]
# En bombas el ejemplo trae: ts, estadoOn, timeOn
STATE_CANDIDATES = ["estadoon", "state", "on", "ison", "run", "running"]
TIMEON_CANDIDATES = ["timeon", "tiempoon", "seconds_on", "segundoson", "runtime", "runseconds"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audita y detecta outliers/anomalías en datasets de bombas."
    )
    parser.add_argument("--input-dir", required=True, help="Carpeta base con CSVs")
    parser.add_argument("--group", required=True, help="Etiqueta del grupo (ej: bombas)")
    parser.add_argument("--asset", help="Filtra por asset (contains, case-insensitive)")
    parser.add_argument("--all", action="store_true", default=False, help="Procesa todos los assets")
    parser.add_argument("--start-date", help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Fecha de fin (YYYY-MM-DD)")
    parser.add_argument("--freq-minutes", type=int, default=1, help="Frecuencia esperada (min)")
    parser.add_argument("--max-files", type=int, default=None, help="Limita cantidad de archivos (debug)")
    parser.add_argument("--max-rows", type=int, default=500, help="Max filas en hoja Anomalies")
    parser.add_argument("--output", help="Ruta Excel salida (default reports/...)")
    parser.add_argument("--plan", help="Ruta CSV plan salida (default reports/...)")

    # thresholds
    parser.add_argument("--gap-multiplier", type=int, default=10,
                        help="Gap grande si delta_ts > freq_minutes * gap_multiplier")
    parser.add_argument("--max-timeon-jump-seconds", type=float, default=6 * 3600,
                        help="Salto máximo permitido en timeOn entre filas (segundos). Default 6h.")
    parser.add_argument("--timeon-rate-max", type=float, default=2.5,
                        help="Máxima 'tasa' permitida: diff_timeOn / delta_seconds. Default 2.5 (tolerancia).")
    parser.add_argument("--timeon-rate-min", type=float, default=-0.5,
                        help="Mínima 'tasa' permitida (negativa). Default -0.5 (permite resets pequeños).")

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


def find_col_by_candidates(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c in lower_map:
            return lower_map[c]
    return None


def find_timestamp_column(df: pd.DataFrame) -> str | None:
    return find_col_by_candidates(df, TIMESTAMP_CANDIDATES)


def find_state_column(df: pd.DataFrame) -> str | None:
    return find_col_by_candidates(df, STATE_CANDIDATES)


def find_timeon_column(df: pd.DataFrame) -> str | None:
    return find_col_by_candidates(df, TIMEON_CANDIDATES)


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


def to_repo_relative(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(path)


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


def update_min_heap(heap, item: dict, limit: int, tie: itertools.count) -> None:
    import heapq
    v = float(item["severity"])
    entry = (-v, next(tie), item)  # queremos severidad mínima? usamos (-) para quedarnos con mayores severidades al final
    if len(heap) < limit:
        heapq.heappush(heap, entry)
    else:
        if entry[0] > heap[0][0]:
            heapq.heapreplace(heap, entry)


def normalize_reason(reason: str) -> str:
    return re.sub(r"\s+", " ", reason.strip())


def load_csv(file_path: Path) -> tuple[pd.DataFrame | None, dict]:
    try:
        df = pd.read_csv(file_path)
    except Exception as exc:
        print(f"[warning] No se pudo leer {file_path}: {exc}")
        return None, {"error": "read_failed"}

    ts_col = find_timestamp_column(df)
    if not ts_col:
        print(f"[warning] Sin columna timestamp/ts en {file_path}")
        return None, {"error": "no_timestamp"}

    df = df.copy()
    df["__row_index"] = df.index

    df["timestamp"], ts_strategy = parse_timestamp(df[ts_col])
    invalid_ts = int(df["timestamp"].isna().sum())
    if invalid_ts:
        print(f"[warning] {file_path} tiene {invalid_ts} filas con timestamp inválido")

    df = df[df["timestamp"].notna()].copy()
    if df.empty:
        return None, {"timestamp_strategy": ts_strategy}

    df["asset_id"] = determine_asset_series(df, file_path)

    state_col = find_state_column(df)
    timeon_col = find_timeon_column(df)

    if state_col:
        df["estadoOn"] = pd.to_numeric(df[state_col], errors="coerce")
    else:
        df["estadoOn"] = np.nan

    if timeon_col:
        df["timeOn"] = pd.to_numeric(df[timeon_col], errors="coerce")
    else:
        df["timeOn"] = np.nan

    meta = {
        "timestamp_column": ts_col,
        "timestamp_strategy": ts_strategy,
        "state_column": state_col,
        "timeon_column": timeon_col,
    }
    keep_cols = ["asset_id", "timestamp", "estadoOn", "timeOn", "__row_index"]
    return df[keep_cols], meta


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise SystemExit(f"[error] input-dir no existe: {input_dir}")

    asset_pattern = args.asset
    if not asset_pattern and not args.all:
        args.all = True

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(args.output) if args.output else Path(
        f"reports/outliers_{args.group}_{run_ts}.xlsx"
    )
    plan_path = Path(args.plan) if args.plan else Path(
        f"reports/cleaning_plan_{args.group}_{run_ts}.csv"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.parent.mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parents[1]
    start_date = parse_date(args.start_date, False, args.freq_minutes)
    end_date = parse_date(args.end_date, True, args.freq_minutes)

    csv_files = sorted(input_dir.rglob("*.csv"))
    if args.max_files is not None:
        csv_files = csv_files[: args.max_files]
    if not csv_files:
        print(f"[warning] No se encontraron CSVs en {input_dir}")
        return

    summary_rows: list[dict] = []
    anomalies_rows: list[dict] = []
    plan_rows: list[dict] = []
    file_notes: list[dict] = []

    tie = itertools.count()
    top_anomalies_heap = []  # guarda top por severidad, limitado

    freq_seconds = float(args.freq_minutes) * 60.0
    gap_threshold = freq_seconds * float(args.gap_multiplier)

    for file_path in csv_files:
        df, meta = load_csv(file_path)
        rel_file = to_repo_relative(file_path, repo_root)

        if df is None:
            file_notes.append({
                "file": rel_file,
                "notes": meta.get("error", "sin datos"),
                "timestamp_column": meta.get("timestamp_column"),
                "timestamp_strategy": meta.get("timestamp_strategy"),
                "state_column": meta.get("state_column"),
                "timeon_column": meta.get("timeon_column"),
            })
            continue

        df = apply_filters(df, asset_pattern, start_date, end_date)
        file_notes.append({
            "file": rel_file,
            "notes": "",
            "timestamp_column": meta.get("timestamp_column"),
            "timestamp_strategy": meta.get("timestamp_strategy"),
            "state_column": meta.get("state_column"),
            "timeon_column": meta.get("timeon_column"),
        })
        if df.empty:
            continue

        # Procesar por asset_id
        for asset_id, asset_df in df.groupby("asset_id", sort=False, observed=True):
            asset_df = asset_df.sort_values("timestamp")
            n = int(asset_df.shape[0])

            state = asset_df["estadoOn"]
            timeon = asset_df["timeOn"]

            # Estado presente?
            state_present = state.notna().any()

            # Estadísticas básicas
            summary = {
                "file": rel_file,
                "asset_id": asset_id,
                "n_rows": n,
                "date_min": asset_df["timestamp"].min(),
                "date_max": asset_df["timestamp"].max(),
                "estadoOn_present": bool(state_present),
                "estadoOn_missing_pct": float(state.isna().mean() * 100.0) if n else np.nan,
                "timeOn_missing_pct": float(timeon.isna().mean() * 100.0) if n else np.nan,
                "timeOn_min": float(timeon.min()) if timeon.notna().any() else np.nan,
                "timeOn_max": float(timeon.max()) if timeon.notna().any() else np.nan,
            }

            # Out-of-range estadoOn
            n_bad_state = 0
            if state_present:
                bad_state_mask = state.notna() & ~state.isin([0, 1])
                n_bad_state = int(bad_state_mask.sum())
                summary["estadoOn_bad_values"] = n_bad_state
            else:
                summary["estadoOn_bad_values"] = 0

            # timeOn negativos
            neg_timeon_mask = timeon.notna() & (timeon < 0)
            n_neg_timeon = int(neg_timeon_mask.sum())
            summary["timeOn_negative"] = n_neg_timeon

            # Duplicados de timestamp
            dup_ts = int(asset_df["timestamp"].duplicated().sum())
            summary["timestamp_duplicates"] = dup_ts

            # Gaps grandes (solo reporte)
            deltas = asset_df["timestamp"].diff().dt.total_seconds()
            big_gaps = int((deltas > gap_threshold).sum())
            summary["big_gaps_count"] = big_gaps

            # Anomalías en timeOn (saltos/tasa)
            # Calculamos diff_timeOn y “rate” vs delta_seconds
            diff_timeon = timeon.diff()
            delta_seconds = deltas

            # Saltos enormes absolutos
            jump_mask = diff_timeon.notna() & (diff_timeon.abs() > float(args.max_timeon_jump_seconds))

            # Tasa: diff_timeOn / delta_seconds (cuando delta_seconds > 0)
            rate = pd.Series(np.nan, index=asset_df.index, dtype="float64")
            valid_rate = diff_timeon.notna() & delta_seconds.notna() & (delta_seconds > 0)
            rate.loc[valid_rate] = (diff_timeon.loc[valid_rate] / delta_seconds.loc[valid_rate])

            bad_rate_mask = rate.notna() & (
                (rate > float(args.timeon_rate_max)) | (rate < float(args.timeon_rate_min))
            )

            # Incoherencias con estadoOn si está disponible
            incoh_mask = pd.Series(False, index=asset_df.index)
            if state_present:
                # estadoOff pero timeOn sube
                incoh_mask |= (state == 0) & diff_timeon.notna() & (diff_timeon > 0)

                # estadoOn pero timeOn baja fuerte (permitimos resets: caer a ~0)
                # regla simple: si diff_timeon < 0 y timeOn no está cerca de 0, lo marcamos
                incoh_mask |= (state == 1) & diff_timeon.notna() & (diff_timeon < 0) & (timeon > freq_seconds)

            # Consolidar “drop-worthy”
            dropworthy = (
                neg_timeon_mask |
                (bad_state_mask if state_present else False) |
                jump_mask |
                bad_rate_mask |
                incoh_mask
            )

            n_drop = int(dropworthy.sum())
            summary["dropworthy_rows"] = n_drop
            summary_rows.append(summary)

            if n_drop == 0:
                continue

            # Para cada fila dropworthy, registrar anomalía y regla de plan
            # (limitamos cantidad de filas exportadas a Excel con heap por severidad)
            subset = asset_df.loc[dropworthy, ["timestamp", "estadoOn", "timeOn", "__row_index"]].copy()
            subset["diff_timeOn"] = diff_timeon.loc[subset.index]
            subset["delta_seconds"] = delta_seconds.loc[subset.index]
            subset["rate"] = rate.loc[subset.index]

            for rec in subset.to_dict("records"):
                # Determinar motivo principal y severidad
                idx = int(rec.get("__row_index", -1))
                ts = rec.get("timestamp")
                st = rec.get("estadoOn")
                to = rec.get("timeOn")
                d_to = rec.get("diff_timeOn")
                d_sec = rec.get("delta_seconds")
                r = rec.get("rate")

                reasons = []
                severity = 0.0

                if pd.notna(to) and float(to) < 0:
                    reasons.append("timeOn<0")
                    severity = max(severity, abs(float(to)))

                if state_present and pd.notna(st) and float(st) not in (0.0, 1.0):
                    reasons.append("estadoOn not in {0,1}")
                    severity = max(severity, 1000.0)

                if pd.notna(d_to) and abs(float(d_to)) > float(args.max_timeon_jump_seconds):
                    reasons.append("abs(diff_timeOn) > max_timeon_jump_seconds")
                    severity = max(severity, abs(float(d_to)))

                if pd.notna(r) and (float(r) > float(args.timeon_rate_max) or float(r) < float(args.timeon_rate_min)):
                    reasons.append("timeOn rate out of bounds")
                    severity = max(severity, abs(float(r)) * 100.0)

                if state_present and pd.notna(st) and float(st) == 0.0 and pd.notna(d_to) and float(d_to) > 0:
                    reasons.append("estadoOn=0 but timeOn increases")
                    severity = max(severity, float(d_to))

                if state_present and pd.notna(st) and float(st) == 1.0 and pd.notna(d_to) and float(d_to) < 0 and pd.notna(to) and float(to) > freq_seconds:
                    reasons.append("estadoOn=1 but timeOn decreases (non-reset)")
                    severity = max(severity, abs(float(d_to)))

                reason_txt = normalize_reason("; ".join(reasons) if reasons else "anomaly")

                anomaly_item = {
                    "file": rel_file,
                    "asset_id": asset_id,
                    "timestamp": ts,
                    "estadoOn": st,
                    "timeOn": to,
                    "diff_timeOn": d_to,
                    "delta_seconds": d_sec,
                    "rate": r,
                    "row_index": idx,
                    "reason": reason_txt,
                    "severity": float(severity),
                }

                # heap para top anomalías (para no explotar Excel)
                update_min_heap(top_anomalies_heap, anomaly_item, args.max_rows, tie)

                # plan: regla por fila (row_index) es la forma más segura de no borrar “mes entero”
                plan_rows.append({
                    "file": rel_file,
                    "asset_id": asset_id,
                    "action": "drop",
                    "match": "row_index",
                    "row_index": idx,
                    "reason": reason_txt,
                })

    # Materializar top anomalies (heap -> lista)
    if top_anomalies_heap:
        # heap tiene (-severity, tie, item). Queremos orden desc por severity
        items = [t[2] for t in top_anomalies_heap]
        anomalies_df = pd.DataFrame(items).sort_values(["severity", "timestamp"], ascending=[False, True])
    else:
        anomalies_df = pd.DataFrame(columns=[
            "file","asset_id","timestamp","estadoOn","timeOn","diff_timeOn","delta_seconds",
            "rate","row_index","reason","severity"
        ])

    summary_df = pd.DataFrame(summary_rows)
    file_notes_df = pd.DataFrame(file_notes)
    plan_df = pd.DataFrame(plan_rows).drop_duplicates(subset=["file","asset_id","match","row_index","action","reason"])

    # Escribir Excel
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        anomalies_df.to_excel(writer, sheet_name="Anomalies", index=False)
        file_notes_df.to_excel(writer, sheet_name="Files", index=False)

    # Escribir plan
    plan_df.to_csv(plan_path, index=False)

    print(f"[info] Reporte generado: {output_path}")
    print(f"[info] Plan de limpieza generado: {plan_path}")
    print("[info] Nota: el plan apunta a row_index (filas puntuales), no a rangos mensuales.")


if __name__ == "__main__":
    main()
