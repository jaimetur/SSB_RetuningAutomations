# -*- coding: utf-8 -*-

import os
from typing import List, Optional, Tuple

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


def find_log_files(folder: str) -> List[str]:
    """
    Return a sorted list of *.log / *.logs / *.txt files found in 'folder'.
    """
    files: List[str] = []
    for name in os.listdir(folder):
        lower = name.lower()
        if lower.endswith((".log", ".logs", ".txt")):
            p = os.path.join(folder, name)
            if os.path.isfile(p):
                files.append(p)
    files.sort()
    return files


def read_text_file(path: str) -> Tuple[List[str], Optional[str]]:
    """
    Thin wrapper around read_text_with_encoding to keep current behavior.
    Returns (lines, encoding_used).
    """
    return read_text_with_encoding(path)
