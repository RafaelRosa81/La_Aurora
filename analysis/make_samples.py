#!/usr/bin/env python3
"""Create per-asset sample CSVs and a manifest from a raw dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ASSET_CANDIDATES = ["asset_label", "asset", "assetName", "estanque", "bomba"]
TIMESTAMP_CANDIDATES = ["timestamp", "ts", "datetime", "FechaHora"]


def _detect_column(columns: list[str], candidates: list[str]) -> str | None:
    column_map = {col.lower(): col for col in columns}
    for candidate in candidates:
        match = column_map.get(candidate.lower())
        if match:
            return match
    return None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create per-asset sample datasets.")
    parser.add_argument("--input", required=True, help="Path to input CSV file.")
    parser.add_argument("--out-dir", required=True, help="Output directory for samples.")
    parser.add_argument("--group", required=True, help="Group label used in file names.")
    parser.add_argument("--assets", type=int, default=None, help="Max assets to include.")
    parser.add_argument(
        "--asset-labels",
        nargs="+",
        default=None,
        help="Specific asset labels to include (overrides --assets).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Max rows per asset (or global if no asset column).",
    )
    parser.add_argument(
        "--round",
        type=int,
        default=None,
        help="Round numeric columns to this many decimals.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    return parser.parse_args()


def _map_assets(series: pd.Series) -> tuple[pd.Series, dict[str, str]]:
    unique_assets = pd.unique(series.dropna())
    mapping = {
        asset: f"ASSET_{idx:03d}" for idx, asset in enumerate(unique_assets, start=1)
    }
    mapped = series.map(mapping)
    return mapped, mapping


def _round_numeric(df: pd.DataFrame, decimals: int) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if not numeric_cols.empty:
        df.loc[:, numeric_cols] = df.loc[:, numeric_cols].round(decimals)
    return df


def _summarize(df: pd.DataFrame, asset_col: str, timestamp_col: str) -> dict:
    summary = {
        "assets_included": sorted(df[asset_col].dropna().unique().tolist()),
        "date_range": {
            "min": None,
            "max": None,
        },
        "columns": df.columns.tolist(),
        "rows_per_asset": {},
    }
    if not df.empty:
        summary["date_range"]["min"] = (
            df[timestamp_col].min().isoformat() if timestamp_col else None
        )
        summary["date_range"]["max"] = (
            df[timestamp_col].max().isoformat() if timestamp_col else None
        )
    if asset_col:
        counts = df.groupby(asset_col).size()
        summary["rows_per_asset"] = counts.to_dict()
    else:
        summary["rows_per_asset"] = {"ALL": int(len(df))}
    return summary


def main() -> None:
    args = _parse_args()
    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[make_samples] Reading {input_path}")
    df = pd.read_csv(input_path)

    asset_col = _detect_column(df.columns.tolist(), ASSET_CANDIDATES)
    timestamp_col = _detect_column(df.columns.tolist(), TIMESTAMP_CANDIDATES)
    if not timestamp_col:
        raise ValueError(
            "No timestamp column found. Expected one of: "
            + ", ".join(TIMESTAMP_CANDIDATES)
        )

    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
    if df[timestamp_col].isna().all():
        raise ValueError(
            f"Failed to parse any timestamps from column '{timestamp_col}'."
        )

    df = df.sort_values(by=timestamp_col).reset_index(drop=True)

    if asset_col:
        if args.asset_labels:
            df = df[df[asset_col].isin(args.asset_labels)]
        mapped_series, mapping = _map_assets(df[asset_col])
        df["asset_label"] = mapped_series
        asset_col = "asset_label"
    else:
        mapping = {}

    if args.assets and asset_col and not args.asset_labels:
        included_assets = df[asset_col].dropna().unique().tolist()[: args.assets]
        df = df[df[asset_col].isin(included_assets)]

    if args.max_rows:
        if asset_col:
            df = (
                df.groupby(asset_col, group_keys=False)
                .head(args.max_rows)
                .reset_index(drop=True)
            )
        else:
            df = df.head(args.max_rows)

    if args.round is not None:
        df = _round_numeric(df, args.round)

    if asset_col:
        for asset in sorted(df[asset_col].dropna().unique().tolist()):
            asset_df = df[df[asset_col] == asset]
            asset_id = asset.lower()
            out_path = out_dir / f"sample_{args.group}_{asset_id}.csv"
            asset_df.to_csv(out_path, index=False)
            print(f"[make_samples] Wrote {out_path} ({len(asset_df)} rows)")
    else:
        out_path = out_dir / f"sample_{args.group}_all.csv"
        df.to_csv(out_path, index=False)
        print(f"[make_samples] Wrote {out_path} ({len(df)} rows)")

    manifest = _summarize(df, asset_col, timestamp_col)
    manifest["parameters"] = {
        "input": str(input_path),
        "out_dir": str(out_dir),
        "group": args.group,
        "assets": args.assets,
        "asset_labels": args.asset_labels,
        "max_rows": args.max_rows,
        "round": args.round,
        "seed": args.seed,
        "asset_mapping": mapping,
        "asset_column_detected": asset_col,
        "timestamp_column_detected": timestamp_col,
    }

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    print(f"[make_samples] Wrote {manifest_path}")


if __name__ == "__main__":
    main()
