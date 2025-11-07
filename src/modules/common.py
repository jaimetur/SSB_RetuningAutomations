# -*- coding: utf-8 -*-

import re
from typing import List, Optional, Tuple, Dict, Iterable
import pandas as pd

# ============================ IO / TEXT ============================

ENCODINGS_TRY = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "utf-8"]


def read_text_with_encoding(path: str) -> Tuple[List[str], Optional[str]]:
    for enc in ENCODINGS_TRY:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return [ln.rstrip("\n") for ln in f], enc
        except Exception:
            continue
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return [ln.rstrip("\n") for ln in f], None


def read_text_lines(path: str) -> Optional[List[str]]:
    try:
        lines, _ = read_text_with_encoding(path)
        return lines
    except Exception:
        return None


# ============================ PARSING ============================

SUMMARY_RE = re.compile(r"^\s*\d+\s+instance\(s\)\s*$", re.IGNORECASE)


def find_all_subnetwork_headers(lines: List[str]) -> List[int]:
    return [i for i, ln in enumerate(lines) if ln.strip().startswith("SubNetwork")]


def extract_mo_from_subnetwork_line(line: str) -> Optional[str]:
    if not line:
        return None
    if "," in line:
        last = line.strip().split(",")[-1].strip()
        return last or None
    toks = line.strip().split()
    return toks[-1].strip() if toks else None


def detect_data_separator(probe_lines: Iterable[str]) -> Optional[str]:
    probe = [ln for ln in probe_lines if ln.strip() and not SUMMARY_RE.match(ln)]
    if any("\t" in ln for ln in probe):
        return "\t"
    if any("," in ln for ln in probe):
        return ","
    return None


def split_line(line: str, sep: Optional[str]) -> List[str]:
    if sep is None:
        return re.split(r"\s+", line.strip())
    return line.split(sep)


def make_unique_columns(cols: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}.{seen[c]}")
    return out


def parse_table_slice_from_subnetwork(lines: List[str], header_idx: int, end_idx: int) -> pd.DataFrame:
    # 1) Data header: primera no vacía ni summary tras SubNetwork
    data_header_idx = None
    for j in range(header_idx + 1, end_idx):
        ln = lines[j]
        if not ln.strip() or SUMMARY_RE.match(ln):
            continue
        data_header_idx = j
        break
    if data_header_idx is None:
        return pd.DataFrame()

    header_line = lines[data_header_idx].strip()

    # 2) Separador dentro del slice
    probe_end = min(end_idx, data_header_idx + 50)
    sep = detect_data_separator(lines[data_header_idx:probe_end])

    # 3) Header cols
    header_cols = [c.strip() for c in (split_line(header_line, sep))]
    header_cols = make_unique_columns(header_cols)

    # 4) Filas
    rows: List[List[str]] = []
    for j in range(data_header_idx + 1, end_idx):
        ln = lines[j]
        if not ln.strip() or SUMMARY_RE.match(ln):
            continue
        parts = [p.strip() for p in split_line(ln, sep)]
        if len(parts) < len(header_cols):
            parts += [""] * (len(header_cols) - len(parts))
        elif len(parts) > len(header_cols):
            parts = parts[:len(header_cols)]
        rows.append(parts)

    df = pd.DataFrame(rows, columns=header_cols)
    df = df.replace({"nan": "", "NaN": "", "None": "", "none": "", "NULL": "", "null": ""}).dropna(how="all")
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df


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
    subset["__Date_dt"] = pd.to_datetime(subset["Date"], format="%Y%m%d", errors="coerce")
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


# ============================ FREQ UTILS ============================

def _base_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.split("-", n=1).str[0].fillna("").str.strip()


def extract_gu_freq_base(s: pd.Series) -> pd.Series:
    base = _base_series(s)
    fallback = s.astype(str).str.extract(r"(\d+)", expand=False)
    return base.where(base != "", fallback).fillna("").astype(str)


def extract_nr_freq_base(s: pd.Series) -> pd.Series:
    got = s.astype(str).str.extract(r"NRFreqRelation\s*=\s*(\d+)", expand=False)
    return got.fillna(_base_series(s)).fillna("").astype(str)


def detect_freq_column(table_name: str, columns: List[str]) -> Optional[str]:
    if table_name == "GUtranCellRelation" and "GUtranFreqRelationId" in columns:
        return "GUtranFreqRelationId"
    for c in columns:
        lc = c.lower()
        if "freqrelation" in lc or "freq" in lc:
            return c
    return None


def detect_key_columns(table_name: str, columns: List[str], freq_col: Optional[str]) -> List[str]:
    preferred = {
        "GUtranCellRelation": ["GUtranCellRelationId", "neighborCellRef"],
        "NRCellRelation": ["NRCellRelationId", "neighborCellRef"],
    }
    for cand in preferred.get(table_name, []):
        if cand in columns:
            return [cand]
    id_like = [c for c in columns if c.lower().endswith("id")]
    if freq_col and freq_col in id_like:
        id_like.remove(freq_col)
    id_like = [c for c in id_like if c not in ("Pre/Post", "Date")]
    if id_like:
        return id_like[:2]
    if "neighborCellRef" in columns:
        return ["neighborCellRef"]
    remaining = [c for c in columns if c not in ("Pre/Post", "Date")]
    return remaining[:1] if remaining else []


def enforce_gu_columns(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols_required = ["NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"]
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=cols_required)
    out = df.copy()
    for c in cols_required:
        if c not in out.columns:
            out[c] = ""
    other = [c for c in out.columns if c not in cols_required]
    return out[cols_required + other]


def enforce_nr_columns(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols_required = ["NodeId", "NRCellCUId", "NRCellRelationId"]
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=cols_required)
    out = df.copy()
    for c in cols_required:
        if c not in out.columns:
            out[c] = ""
    other = [c for c in out.columns if c not in cols_required]
    return out[cols_required + other]


# ============================ EXCEL HELPERS ============================

def sanitize_sheet_name(name: str) -> str:
    name = re.sub(r'[:\\/?*\[\]]', "_", name)
    name = name.strip().strip("'")
    return (name or "Sheet")[:31]


def unique_sheet_name(base: str, used: set) -> str:
    if base not in used:
        return base
    for k in range(1, 1000):
        suffix = f" ({k})"
        cand = (base[: max(0, 31 - len(suffix))] + suffix)
        if cand not in used:
            return cand
    i, cand = 1, base
    while cand in used:
        cand = f"{base[:28]}_{i:02d}"
        i += 1
    return cand


def try_read_text_file_with_encoding(path: str) -> Tuple[List[str], Optional[str]]:
    """
    Robust text reader that tries several encodings and returns (lines, encoding_used).
    If it falls back to 'replace' mode, returns (lines, None) to signal that encoding is uncertain.
    """
    encodings = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "utf-8"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return [ln.rstrip("\n") for ln in f], enc
        except Exception:
            continue
    # last permissive attempt
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return [ln.rstrip("\n") for ln in f], None


def try_read_text_file_lines(path: str) -> Optional[List[str]]:
    """
    Same as above but returns only the lines (used by PrePostRelations.loaders).
    """
    try:
        lines, _ = try_read_text_file_with_encoding(path)
        return lines
    except Exception:
        return None
