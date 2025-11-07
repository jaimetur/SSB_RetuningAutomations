# -*- coding: utf-8 -*-

import os
import re
from typing import List, Tuple, Optional, Dict

import pandas as pd


class CreateExcelFromLogs:
    """
    Generates an Excel in input_dir with one sheet per *.log / *.logs file.

    Robustness:
      - Tries multiple encodings: utf-8-sig, utf-16, utf-16-le, utf-16-be, cp1252.
      - Preferred delimiter for DATA rows: TAB. Only if no TABs exist in any line, tries comma.
      - If neither works, splits by whitespace.
      - Removes fully empty rows and 'N instance(s)' lines.
      - Adds 'Summary' sheet.
      - Limits rows to ~1,048,576 (Excel limit).

    Output name: LogsCombined_{versioned_suffix}.xlsx

    Additional behavior:
      - Header line is detected as the one that starts with 'SubNetwork' (case-sensitive here).
      - The MO (table) name used for the sheet title is taken from the line BEFORE the header:
        it is the last token after the last comma in that previous line.
        Example:
          Line i-1: "SubNetwork,SubNetwork,MeContext,ManagedElement,ENodeBFunction,GUtraNetwork,GUtranSyncSignalFrequency"
          -> MO: "GUtranSyncSignalFrequency"
    """

    SUMMARY_RE = re.compile(r"^\s*\d+\s+instance\(s\)\s*$", re.IGNORECASE)

    def __init__(self):
        pass

    # ----------------------------- Public API ----------------------------- #
    def run(self, input_dir: str, module_name: Optional[str] = "", versioned_suffix: Optional[str] = None) -> str:
        if not os.path.isdir(input_dir):
            raise NotADirectoryError(f"Invalid directory: {input_dir}")

        log_files = self._find_log_files(input_dir)
        if not log_files:
            raise FileNotFoundError(f"No .log/.logs files found in: {input_dir}")

        excel_path = os.path.join(input_dir, f"LogsCombined_{versioned_suffix}.xlsx")

        # 1) Collect all tables first (do not build Summary yet)
        table_entries: List[Dict[str, object]] = []

        for path in log_files:
            base_filename = os.path.basename(path)
            lines, encoding_used = self._read_text_file(path)

            # Detect all 'SubNetwork' blocks (multi-table per file)
            header_indices = self._find_all_subnetwork_headers(lines)

            if not header_indices:
                # Fallback: old single-table logic
                header_idx = self._find_subnetwork_header_index(lines)
                mo_name_prevline = self._extract_mo_name_from_previous_line(lines, header_idx)
                df, note = self._parse_log_lines(lines, forced_header_idx=header_idx)
                if encoding_used:
                    note = (note + " | " if note else "") + f"encoding={encoding_used}"

                # Excel row cap
                max_rows_excel = 1_048_576
                if len(df) > max_rows_excel:
                    df = df.iloc[:max_rows_excel, :].copy()
                    note = (note + " | " if note else "") + f"Trimmed to {max_rows_excel} rows"

                table_entries.append({
                    "df": df,
                    "sheet_candidate": mo_name_prevline if mo_name_prevline else os.path.splitext(base_filename)[0],
                    "log_file": base_filename,
                    "tables_in_log": 1,
                    "note": note or ""
                })
                continue

            # There are one or more tables: slice between SubNetwork headers
            tables_in_log = len(header_indices)
            header_indices.append(len(lines))  # sentinel

            for ix in range(tables_in_log):
                header_idx = header_indices[ix]
                next_header_idx = header_indices[ix + 1]

                # MO from SubNetwork line (last token after last comma)
                subnetwork_line = lines[header_idx]
                mo_name_from_line = self._extract_mo_from_subnetwork_line(subnetwork_line)
                desired_sheet = mo_name_from_line if mo_name_from_line else os.path.splitext(base_filename)[0]

                # Parse this table slice
                df, note = self._parse_table_slice_from_subnetwork(lines, header_idx, next_header_idx)
                if encoding_used:
                    note = (note + " | " if note else "") + f"encoding={encoding_used}"

                # Excel row cap
                max_rows_excel = 1_048_576
                if len(df) > max_rows_excel:
                    df = df.iloc[:max_rows_excel, :].copy()
                    note = (note + " | " if note else "") + f"Trimmed to {max_rows_excel} rows"

                table_entries.append({
                    "df": df,
                    "sheet_candidate": desired_sheet,
                    "log_file": base_filename,
                    "tables_in_log": tables_in_log,
                    "note": note or ""
                })

        # 2) Compute final unique sheet names BEFORE writing Summary
        used_sheet_names: set = set(["Summary"])  # reserve Summary
        for entry in table_entries:
            base_name = self._sanitize_sheet_name(str(entry["sheet_candidate"]))
            final_sheet = self._unique_sheet_name(base_name, used_sheet_names)
            used_sheet_names.add(final_sheet)
            entry["final_sheet"] = final_sheet  # <- store final unique sheet name

        # 3) Build Summary rows using final_sheet and split Note into Separator/Encoding
        summary_rows: List[Dict[str, object]] = []
        for entry in table_entries:
            note = str(entry.get("note", ""))

            separator_str, encoding_str = "", ""
            if note:
                parts = [p.strip() for p in note.split("|")]
                for part in parts:
                    pl = part.lower()
                    if pl.startswith("header=") or "separated" in pl:
                        # Keep the full label or extract a cleaner variant if you prefer
                        separator_str = part  # e.g., "Header=SubNetwork-comma" or "Tab-separated"
                    elif pl.startswith("encoding="):
                        encoding_str = part.replace("encoding=", "")

            df: pd.DataFrame = entry["df"]
            summary_rows.append({
                "File": entry["log_file"],
                "Sheet": entry["final_sheet"],  # <- use final unique sheet name
                "Rows": int(len(df)),
                "Columns": int(df.shape[1]),
                "Separator": separator_str,
                "Encoding": encoding_str,
                "LogFile": entry["log_file"],
                "TablesInLog": entry["tables_in_log"],
            })

        # 4) Write Excel: Summary FIRST, then each table with its final_sheet name
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            # Summary first
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

            # Then data sheets
            for entry in table_entries:
                df: pd.DataFrame = entry["df"]
                df.to_excel(writer, sheet_name=entry["final_sheet"], index=False)

        print(f"{module_name} Wrote Excel with {len(table_entries)} sheet(s) in: '{excel_path}'")
        return excel_path

    # ---------------------------- file discovery -------------------------- #
    def _find_log_files(self, folder: str) -> List[str]:
        """
        Find all .log, .logs or .txt files in the given folder (non-recursive).
        """
        files = []
        for name in os.listdir(folder):
            lower = name.lower()
            if lower.endswith((".log", ".logs", ".txt")):  # <-- Added '.txt'
                p = os.path.join(folder, name)
                if os.path.isfile(p):
                    files.append(p)
        files.sort()
        return files

    # ----------------------- robust text reading -------------------------- #
    def _read_text_file(self, path: str) -> Tuple[List[str], Optional[str]]:
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

    # ---------------------------- log parser ------------------------------ #
    def _parse_log_lines(
        self,
        lines: List[str],
        forced_header_idx: Optional[int] = None
    ) -> Tuple[pd.DataFrame, str]:
        """
        Build the DataFrame detecting separator and header.
        - Header is preferably the line that starts with 'SubNetwork' (forced_header_idx).
        - DATA separator preference: if any TAB exists in non-summary lines, use TAB;
          else if comma exists, use comma; else whitespace.
        - IMPORTANT: If header is 'SubNetwork,...' it is split by COMMA,
          even if data uses TABs (common in these logs).
        """

        # 0) Filter out empty/summary lines just for detection
        valid_lines = [ln for ln in lines if ln.strip() and not self.SUMMARY_RE.match(ln)]

        # 1) Decide header index
        header_idx = forced_header_idx
        if header_idx is None:
            # fallback to old behavior: find first non-empty, non-summary line respecting sep logic
            header_idx = self._fallback_header_index(valid_lines, lines)

        if header_idx is None:
            return pd.DataFrame(), "No header detected"

        header_line = lines[header_idx].strip()

        # 2) Decide DATA separator (prefer TAB across the whole file, not for header)
        any_tab = any("\t" in ln for ln in valid_lines)
        data_sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in valid_lines) else None)

        # 3) Split header
        # If the header starts with "SubNetwork" we force comma split for columns
        if header_line.startswith("SubNetwork"):
            header_cols = [c.strip() for c in header_line.split(",")]
        else:
            # generic split by data_sep
            header_cols = self._split_line(header_line, data_sep)
            header_cols = [c.strip() for c in header_cols]

        # --- MINIMAL FIX: make column names unique to avoid DataFrame selection when duplicates exist ---
        header_cols = self._make_unique_columns(header_cols)

        # 4) Build rows from the following lines using DATA separator
        rows: List[List[str]] = []
        for ln in lines[header_idx + 1:]:
            if not ln.strip() or self.SUMMARY_RE.match(ln):
                continue
            parts = self._split_line(ln, data_sep)
            # adjust to header length
            if len(parts) < len(header_cols):
                parts += [""] * (len(header_cols) - len(parts))
            elif len(parts) > len(header_cols):
                parts = parts[:len(header_cols)]
            rows.append([p.strip() for p in parts])

        df = pd.DataFrame(rows, columns=header_cols)
        df = df.replace({"nan": "", "NaN": "", "None": "", "NULL": ""}).dropna(how="all")
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()

        note = (
            "Header=SubNetwork-comma"
            if header_line.startswith("SubNetwork")
            else ("Tab-separated" if data_sep == "\t" else ("Comma-separated" if data_sep == "," else "Whitespace-separated"))
        )
        return df, note

    def _fallback_header_index(self, valid_lines: List[str], all_lines: List[str]) -> Optional[int]:
        """
        Fallback strategy to detect a header index when no 'SubNetwork' line is found.
        Mimics the previous behavior but restricted to non-summary, non-empty lines.
        """
        # Decide a generic separator for detection
        any_tab = any("\t" in ln for ln in valid_lines)
        sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in valid_lines) else None)

        # Pick first line that matches the chosen separator (or any non-empty if sep is None)
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

    # --------------------------- header & MO helpers ---------------------- #
    @staticmethod
    def _find_subnetwork_header_index(lines: List[str]) -> Optional[int]:
        """
        Find the index of the header line that starts with 'SubNetwork'.
        Returns None if not found.
        """
        for i, ln in enumerate(lines):
            if ln.strip().startswith("SubNetwork"):
                return i
        return None

    @staticmethod
    def _extract_mo_name_from_previous_line(lines: List[str], header_idx: Optional[int]) -> Optional[str]:
        """
        From the line immediately BEFORE the header, extract the MO (table) name as the last token
        after the last comma. If cannot be extracted, return None.

        Example previous line:
          "SubNetwork,SubNetwork,MeContext,ManagedElement,ENodeBFunction,GUtraNetwork,GUtranSyncSignalFrequency"
           -> returns "GUtranSyncSignalFrequency"
        """
        if header_idx is None or header_idx == 0:
            return None
        prev_line = lines[header_idx - 1].strip()
        if not prev_line:
            return None

        # Prefer comma-based split according to the requested rule
        if "," in prev_line:
            last_token = prev_line.split(",")[-1].strip()
            return last_token if last_token else None

        # Fallback: try whitespace last token (very defensive)
        tokens = prev_line.split()
        if tokens:
            return tokens[-1].strip()
        return None

    # -------------------------- sheet name helpers ------------------------ #
    @staticmethod
    def _sanitize_sheet_name(name: str) -> str:
        # Excel: max 31 chars, cannot use : \ / ? * [ ]
        name = re.sub(r'[:\\/?*\[\]]', "_", name)
        name = name.strip().strip("'")
        return (name or "Sheet")[:31]

    @staticmethod
    def _unique_sheet_name(base: str, used: set) -> str:
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

    # ------------------------------- utils -------------------------------- #
    @staticmethod
    def _split_line(line: str, sep: Optional[str]) -> List[str]:
        if sep is None:
            return re.split(r"\s+", line.strip())
        return line.split(sep)

    @staticmethod
    def _make_unique_columns(cols: List[str]) -> List[str]:
        """Return a list of column names made unique by appending .1, .2, ... to duplicates."""
        seen: Dict[str, int] = {}
        unique = []
        for c in cols:
            if c not in seen:
                seen[c] = 0
                unique.append(c)
            else:
                seen[c] += 1
                unique.append(f"{c}.{seen[c]}")
        return unique

    # --------------------------- NEW: multi-table helpers --------------------------- #
    @staticmethod
    def _find_all_subnetwork_headers(lines: List[str]) -> List[int]:
        """Return a list of indices for all lines starting with 'SubNetwork'."""
        return [i for i, ln in enumerate(lines) if ln.strip().startswith("SubNetwork")]

    @staticmethod
    def _extract_mo_from_subnetwork_line(line: str) -> Optional[str]:
        """
        Extract the MO (sheet) name from the SubNetwork line itself:
        rule = last token after the last comma.
        """
        if not line:
            return None
        if "," in line:
            last = line.strip().split(",")[-1].strip()
            return last or None
        # Defensive fallback: last whitespace token
        toks = line.strip().split()
        return toks[-1].strip() if toks else None

    def _parse_table_slice_from_subnetwork(self, lines: List[str], header_idx: int, end_idx: int) -> Tuple[pd.DataFrame, str]:
        """
        Parse a single table from lines[header_idx:end_idx].
        - The SubNetwork line is at header_idx; the next non-empty/non-summary line is the real data header.
        - DATA separator preference inside the slice: TAB > comma > whitespace.
        """
        # 1) Find the data header index
        data_header_idx = None
        for j in range(header_idx + 1, end_idx):
            ln = lines[j]
            if not ln.strip() or self.SUMMARY_RE.match(ln):
                continue
            data_header_idx = j
            break
        if data_header_idx is None:
            return pd.DataFrame(), "No header detected (slice)"

        header_line = lines[data_header_idx].strip()

        # 2) Detect separator within slice (probe header + a few data lines)
        probe_lines = []
        for j in range(data_header_idx, min(end_idx, data_header_idx + 50)):
            ln = lines[j]
            if ln.strip() and not self.SUMMARY_RE.match(ln):
                probe_lines.append(ln)
        any_tab = any("\t" in ln for ln in probe_lines)
        data_sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in probe_lines) else None)

        # 3) Split header columns
        header_cols = [c.strip() for c in (header_line.split(data_sep) if data_sep else re.split(r"\s+", header_line.strip()))]
        header_cols = self._make_unique_columns(header_cols)

        # 4) Build rows
        rows: List[List[str]] = []
        for j in range(data_header_idx + 1, end_idx):
            ln = lines[j]
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

        note = "Slice parsed | " + ("Tab-separated" if data_sep == "\t" else ("Comma-separated" if data_sep == "," else "Whitespace-separated"))
        return df, note
