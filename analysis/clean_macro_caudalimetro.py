# -----------------------------------------------------------------------------
# clean_macro_caudalimetro.py
#
# Igual a clean_presion.py, cambiando VALUE_CANDIDATES a:
#
# VALUE_CANDIDATES = [
#   "macro_caudalimetro", "caudalimetro", "caudal", "flow", "q",
#   "m3h", "m3_h", "l_s", "lps", "litros_seg", "litros_s",
#   "volumen", "totalizador", "lectura"
# ]
# -----------------------------------------------------------------------------

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "fechahora"]
ASSET_LABEL_CANDIDATES = ["asset_label", "asset", "equipo", "tag", "nombre", "id"]

VALUE_CANDIDATES = [
   "macro_caudalimetro", "caudalimetro", "flow", "q",
   "m3h", "m3_h", "l_s", "lps", "litros_seg", "litros_s",
   "volumen", "totalizador", "lectura", "caudal", "volumenAgua", "volumenAguaPlot"
]


def parse_args() -> argparse.Namespace:
    #p = argparse.ArgumentParser(description="Aplica plan de limpieza para PRESION preservando subcarpetas.")
    p = argparse.ArgumentParser(
        description="Aplica plan de limpieza para MACRO_CAUDALIMETRO preservando subcarpetas."
    )
    p.add_argument("--plan", required=True, help="CSV plan (find_outliers_presion)")
    p.add_argument("--input-dir", required=True, help="Carpeta base entrada con CSVs")
    p.add_argument("--output-dir", required=True, help="Carpeta base salida")
    p.add_argument("--dry-run", action="store_true", help="No escribe archivos, solo reporta")
    p.add_argument("--max-files", type=int, default=None, help="Limita cantidad de archivos (debug)")
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


def parse_date(x) -> pd.Timestamp | None:
    if x is None:
        return None
    if isinstance(x, float) and np.isnan(x):
        return None
    t = pd.to_datetime(x, errors="coerce")
    return None if pd.isna(t) else t


def relative_from_input(input_dir: Path, file_path: Path) -> Path:
    try:
        return file_path.resolve().relative_to(input_dir.resolve())
    except Exception:
        return Path(file_path.name)


def write_output_csv(output_path: Path, df: pd.DataFrame) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[info] Limpio: {output_path}")


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

    plan_df = pd.read_csv(plan_path)
    required = {"file", "asset_id", "column", "value_min", "value_max", "action"}
    missing = required - set(plan_df.columns)
    if missing:
        raise SystemExit(f"[error] Plan incompleto, faltan: {sorted(missing)}")

    plan_df = plan_df.copy()
    plan_df["action"] = plan_df["action"].astype(str).str.lower().str.strip()
    plan_df = plan_df[(plan_df["action"] == "drop") & (plan_df["column"] == "value")]

    # index por file relativo exacto (como sale del plan)
    rules_by_file = {k: g for k, g in plan_df.groupby("file")}

    csv_files = sorted(input_dir.rglob("*.csv"))
    if args.max_files is not None:
        csv_files = csv_files[: args.max_files]
    if not csv_files:
        print(f"[warning] No se encontraron CSVs en {input_dir}")
        return

    # Para matchear "file" del plan, usamos el relativo al repo si lo guardaste así.
    # Más robusto: intentamos match por sufijo (path ending).
    def find_rules_for_file(file_rel_str: str):
        if file_rel_str in rules_by_file:
            return rules_by_file[file_rel_str]
        # fallback: match por nombre o sufijo
        matches = [k for k in rules_by_file.keys() if str(file_rel_str).replace("\\", "/").endswith(str(k).replace("\\", "/"))]
        if len(matches) == 1:
            return rules_by_file[matches[0]]
        return None

    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            print(f"[warning] No se pudo leer {file_path}: {exc}")
            continue

        ts_col = find_timestamp_column(df)
        val_col = find_value_column(df)

        if not ts_col or not val_col:
            cleaned = df
        else:
            df = df.copy()
            df["__ts"], _ = parse_timestamp(df[ts_col])
            df = df[df["__ts"].notna()].copy()
            df["__asset"] = determine_asset_series(df, file_path)
            df["__value"] = pd.to_numeric(df[val_col], errors="coerce")

            # armar key similar al plan (conservador: usamos ruta relativa al input)
            file_key = str(relative_from_input(input_dir, file_path)).replace("/", "\\")
            file_rules = find_rules_for_file(file_key)

            if file_rules is None:
                cleaned = df.drop(columns=["__ts", "__asset", "__value"], errors="ignore")
            else:
                drop_mask = pd.Series(False, index=df.index)
                for _, r in file_rules.iterrows():
                    asset_id = str(r.get("asset_id"))
                    vmin = float(r.get("value_min"))
                    vmax = float(r.get("value_max"))
                    dmin = parse_date(r.get("date_min"))
                    dmax = parse_date(r.get("date_max"))

                    m = df["__asset"].astype(str) == asset_id
                    m &= df["__value"].notna()
                    m &= (df["__value"] < vmin) | (df["__value"] > vmax)
                    if dmin is not None:
                        m &= df["__ts"] >= dmin
                    if dmax is not None:
                        m &= df["__ts"] <= dmax
                    drop_mask |= m

                cleaned = df.loc[~drop_mask].drop(columns=["__ts", "__asset", "__value"], errors="ignore")

        out_path = output_dir / relative_from_input(input_dir, file_path)
        if args.dry_run:
            print(f"[dry-run] Escribiría: {out_path}")
        else:
            write_output_csv(out_path, cleaned)

    print(f"[info] Terminado. Salida en: {output_dir}")


if __name__ == "__main__":
    main()
