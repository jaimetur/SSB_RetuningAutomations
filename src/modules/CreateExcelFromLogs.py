# -*- coding: utf-8 -*-

import os
import re
from typing import List, Tuple, Optional, Dict
import pandas as pd

from src.modules.common import (
    read_text_with_encoding,
    find_all_subnetwork_headers,
    extract_mo_from_subnetwork_line,
    parse_table_slice_from_subnetwork,
    SUMMARY_RE,
    sanitize_sheet_name,
    unique_sheet_name,
)

class CreateExcelFromLogs:
    """
    Generates an Excel in input_dir with one sheet per *.log / *.logs / *.txt file.
    (Funcionalidad intacta.)
    """

    SUMMARY_RE = SUMMARY_RE  # mantener referencia de clase

    def __init__(self):
        pass

    def run(self, input_dir: str, module_name: Optional[str] = "", versioned_suffix: Optional[str] = None) -> str:
        if not os.path.isdir(input_dir):
            raise NotADirectoryError(f"Invalid directory: {input_dir}")

        log_files = self._find_log_files(input_dir)
        if not log_files:
            raise FileNotFoundError(f"No .log/.logs/.txt files found in: {input_dir}")

        excel_path = os.path.join(input_dir, f"LogsCombined_{versioned_suffix}.xlsx")
        table_entries: List[Dict[str, object]] = []

        for path in log_files:
            base_filename = os.path.basename(path)
            lines, encoding_used = self._read_text_file(path)

            header_indices = self._find_all_subnetwork_headers(lines)
            if not header_indices:
                # Fallback single-table (conserva nota)
                header_idx = self._find_subnetwork_header_index(lines)
                mo_name_prev = self._extract_mo_name_from_previous_line(lines, header_idx)
                df, note = self._parse_log_lines(lines, forced_header_idx=header_idx)
                if encoding_used:
                    note = (note + " | " if note else "") + f"encoding={encoding_used}"
                df, note = self._cap_rows(df, note)
                table_entries.append({
                    "df": df,
                    "sheet_candidate": mo_name_prev if mo_name_prev else os.path.splitext(base_filename)[0],
                    "log_file": base_filename,
                    "tables_in_log": 1,
                    "note": note or ""
                })
                continue

            tables_in_log = len(header_indices)
            header_indices.append(len(lines))

            for ix in range(tables_in_log):
                h = header_indices[ix]
                nxt = header_indices[ix + 1]
                mo_name_from_line = extract_mo_from_subnetwork_line(lines[h])
                desired_sheet = mo_name_from_line if mo_name_from_line else os.path.splitext(base_filename)[0]

                df = parse_table_slice_from_subnetwork(lines, h, nxt)
                note = "Slice parsed"
                if encoding_used:
                    note += f" | encoding={encoding_used}"
                df, note = self._cap_rows(df, note)

                table_entries.append({
                    "df": df,
                    "sheet_candidate": desired_sheet,
                    "log_file": base_filename,
                    "tables_in_log": tables_in_log,
                    "note": note or ""
                })

        used_sheet_names: set = set(["Summary"])
        for entry in table_entries:
            base_name = self._sanitize_sheet_name(str(entry["sheet_candidate"]))
            final_sheet = self._unique_sheet_name(base_name, used_sheet_names)
            used_sheet_names.add(final_sheet)
            entry["final_sheet"] = final_sheet

        summary_rows: List[Dict[str, object]] = []
        for entry in table_entries:
            note = str(entry.get("note", ""))
            separator_str, encoding_str = "", ""
            if note:
                parts = [p.strip() for p in note.split("|")]
                for part in parts:
                    pl = part.lower()
                    if pl.startswith("header=") or "separated" in pl:
                        separator_str = part
                    elif pl.startswith("encoding="):
                        encoding_str = part.replace("encoding=", "")
            df: pd.DataFrame = entry["df"]
            summary_rows.append({
                "File": entry["log_file"],
                "Sheet": entry["final_sheet"],
                "Rows": int(len(df)),
                "Columns": int(df.shape[1]),
                "Separator": separator_str,
                "Encoding": encoding_str,
                "LogFile": entry["log_file"],
                "TablesInLog": entry["tables_in_log"],
            })

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)
            for entry in table_entries:
                entry["df"].to_excel(writer, sheet_name=entry["final_sheet"], index=False)

        print(f"{module_name} Wrote Excel with {len(table_entries)} sheet(s) in: '{excel_path}'")
        return excel_path

    # --------- discovery ---------
    def _find_log_files(self, folder: str) -> List[str]:
        files = []
        for name in os.listdir(folder):
            lower = name.lower()
            if lower.endswith((".log", ".logs", ".txt")):
                p = os.path.join(folder, name)
                if os.path.isfile(p):
                    files.append(p)
        files.sort()
        return files

    # --------- reading ---------
    def _read_text_file(self, path: str) -> Tuple[List[str], Optional[str]]:
        return read_text_with_encoding(path)

    # --------- parsing (fallback single-table) ---------
    def _parse_log_lines(self, lines: List[str], forced_header_idx: Optional[int] = None) -> Tuple[pd.DataFrame, str]:
        valid = [ln for ln in lines if ln.strip() and not self.SUMMARY_RE.match(ln)]
        header_idx = forced_header_idx
        if header_idx is None:
            header_idx = self._fallback_header_index(valid, lines)
        if header_idx is None:
            return pd.DataFrame(), "No header detected"

        header_line = lines[header_idx].strip()
        any_tab = any("\t" in ln for ln in valid)
        data_sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in valid) else None)

        if header_line.startswith("SubNetwork"):
            header_cols = [c.strip() for c in header_line.split(",")]
        else:
            header_cols = [c.strip() for c in (header_line.split(data_sep) if data_sep else re.split(r"\s+", header_line.strip()))]
        header_cols = make_unique_columns(header_cols)

        rows: List[List[str]] = []
        for ln in lines[header_idx + 1:]:
            if not ln.strip() or self.SUMMARY_RE.match(ln):
                continue
            parts = [p.strip() for p in (ln.split(data_sep) if data_sep else re.split(r"\s+", ln.strip()))]
            if len(parts) < len(header_cols):
                parts += [""] * (len(header_cols) - len(parts))
            elif len(parts) > len(header_cols):
                parts = parts[:len(header_cols)]
            rows.append(parts)

        df = pd.DataFrame(rows, columns=header_cols)
        df = df.replace({"nan": "", "NaN": "", "None": "", "NULL": ""}).dropna(how="all")
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()

        note = "Header=SubNetwork-comma" if header_line.startswith("SubNetwork") else (
            "Tab-separated" if data_sep == "\t" else ("Comma-separated" if data_sep == "," else "Whitespace-separated")
        )
        return df, note

    def _fallback_header_index(self, valid_lines: List[str], all_lines: List[str]) -> Optional[int]:
        any_tab = any("\t" in ln for ln in valid_lines)
        sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in valid_lines) else None)
        for i, ln in enumerate(all_lines):
            if not ln.strip() or self.SUMMARY_RE.match(ln):
                continue
            if sep == "\t" and "\t" in ln:
                return i
            if sep == "," and "," in ln:
                return i
            if sep is None:
                return i
        return None

    # --------- header & MO helpers ---------
    @staticmethod
    def _find_subnetwork_header_index(lines: List[str]) -> Optional[int]:
        for i, ln in enumerate(lines):
            if ln.strip().startswith("SubNetwork"):
                return i
        return None

    @staticmethod
    def _extract_mo_name_from_previous_line(lines: List[str], header_idx: Optional[int]) -> Optional[str]:
        if header_idx is None or header_idx == 0:
            return None
        prev = lines[header_idx - 1].strip()
        if not prev:
            return None
        if "," in prev:
            last = prev.split(",")[-1].strip()
            return last or None
        toks = prev.split()
        return toks[-1].strip() if toks else None

    # --------- sheet naming ---------
    @staticmethod
    def _sanitize_sheet_name(name: str) -> str:
        return sanitize_sheet_name(name)

    @staticmethod
    def _unique_sheet_name(base: str, used: set) -> str:
        return unique_sheet_name(base, used)

    # --------- caps ---------
    @staticmethod
    def _cap_rows(df: pd.DataFrame, note: str, max_rows_excel: int = 1_048_576) -> Tuple[pd.DataFrame, str]:
        if len(df) > max_rows_excel:
            df = df.iloc[:max_rows_excel, :].copy()
            note = (note + " | " if note else "") + f"Trimmed to {max_rows_excel} rows"
        return df, note

    # --------- multi-table helpers ---------
    @staticmethod
    def _find_all_subnetwork_headers(lines: List[str]) -> List[int]:
        return find_all_subnetwork_headers(lines)
