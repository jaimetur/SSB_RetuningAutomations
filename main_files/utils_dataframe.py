# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Optional, Dict

import pandas as pd


# ============================ DATAFRAME UTILS ============================

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        out[c] = (
            out[c].astype(str).str.strip()
            .replace({"nan": "", "NaN": "", "None": "", "none": "", "NULL": "", "null": ""})
        )
    return out


def select_latest_by_date(df: pd.DataFrame, side_value: str) -> pd.DataFrame:
    subset = df[df["Pre/Post"].str.lower() == side_value.lower()]
    if subset.empty:
        return subset
    if "Date" not in subset.columns or (subset["Date"].astype(str).str.len() == 0).all():
        return subset
    subset = subset.copy()
    subset["__Date_dt"] = pd.to_datetime(subset["Date"], format="%Y-%m-%d", errors="coerce")
    max_date = subset["__Date_dt"].max()
    return subset[subset["__Date_dt"] == max_date].drop(columns="__Date_dt")


def make_index_by_keys(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    dfx = df.copy()
    for c in keys:
        if c not in dfx.columns:
            dfx[c] = ""
    dfx["_join_key"] = dfx[keys].agg("||".join, axis=1)
    dfx = dfx.set_index("_join_key", drop=True)
    if dfx.index.has_duplicates:
        dfx = dfx[~dfx.index.duplicated(keep="last")]
    return dfx


def ensure_column_before(df: pd.DataFrame, col_to_move: str, before_col: str) -> pd.DataFrame:
    """
    Utility to keep a helper column immediately before another column in the Excel output.
    """
    if df is None or df.empty:
        return df
    if col_to_move in df.columns and before_col in df.columns:
        cols = list(df.columns)
        cols.remove(col_to_move)
        insert_pos = cols.index(before_col)
        cols.insert(insert_pos, col_to_move)
        df = df[cols]
    return df


def ensure_column_after(df: pd.DataFrame, col_to_move: str, after_col: str) -> pd.DataFrame:
    """
    Utility to keep a helper column immediately after another column in the Excel output.
    """
    if df is None or df.empty:
        return df
    if col_to_move in df.columns and after_col in df.columns:
        cols = list(df.columns)
        cols.remove(col_to_move)
        insert_pos = cols.index(after_col) + 1
        cols.insert(insert_pos, col_to_move)
        df = df[cols]
    return df


def drop_columns(df: pd.DataFrame, unwanted) -> pd.DataFrame:
    """
    Drop a list of unwanted columns if they exist; used to keep Excel output compact.
    """
    if df is None or df.empty:
        return df
    return df.drop(columns=[c for c in unwanted if c in df.columns], errors="ignore")


def build_row_lookup(
    relations_df: Optional[pd.DataFrame],
    key_cols,
    extra_strip_cols=None,
) -> Dict[tuple, pd.Series]:
    """
    Build a dict[(keys...)] -> row from relations_df, stripping spaces.

    This avoids repeating the same boilerplate in each builder.
    """
    lookup: Dict[tuple, pd.Series] = {}
    if relations_df is None or relations_df.empty:
        return lookup

    rel = relations_df.copy()
    cols_to_norm = list(key_cols) + list(extra_strip_cols or [])
    for col in cols_to_norm:
        if col not in rel.columns:
            rel[col] = ""
        rel[col] = rel[col].astype(str).str.strip()

    for _, r in rel.iterrows():
        key = tuple(str(r.get(k, "")).strip() for k in key_cols)
        lookup[key] = r
    return lookup


def pick_non_empty_value(rel_row: Optional[pd.Series], row: pd.Series, field: str) -> str:
    """
    Prefer value from relations_df; if empty/NaN, fallback to value in df row.
    Avoid returning literal 'nan'.
    """
    candidates = []
    if rel_row is not None:
        candidates.append(rel_row.get(field))
    candidates.append(row.get(field))

    for v in candidates:
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except TypeError:
            pass
        s = str(v).strip()
        if not s or s.lower() == "nan":
            continue
        return s
    return ""


def concat_or_empty(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Return a single concatenated DataFrame or an empty one if none;
    align on common columns when required.
    """
    if not dfs:
        return pd.DataFrame()
    try:
        return pd.concat(dfs, ignore_index=True)
    except Exception:
        common_cols = set.intersection(*(set(d.columns) for d in dfs)) if dfs else set()
        if not common_cols:
            return pd.DataFrame()
        dfs_aligned = [d[list(common_cols)].copy() for d in dfs]
        return pd.concat(dfs_aligned, ignore_index=True)
