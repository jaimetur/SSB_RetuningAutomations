#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Copy Python files from ./src into a flat folder named 'main_files' in the script root.

Rules:
- Copy all *.py under ./src
- Exclude any __init__.py
- Exclude anything inside __pycache__ folders
- Destination is a flat folder (no subfolders)
- If total candidates > 25, copy ONLY up to 25, prioritizing folders in this order:
  1) ./src
  2) ./src/utils
  3) ./src/modules/ConfigurationAudit
  4) ./src/modules/ConsistencyChecks
  5) ./src/modules/ProfilesAudit
  6) ./src/modules/Common
  7) ./src/modules/CleanUp
- Avoid duplicate copies across overlapping folder scopes
- Overwrite destination files if they already exist (no suffixes)
"""

import shutil
from pathlib import Path
from typing import List, Set


MAX_FILES = 25
DEST_FOLDER_NAME = "main_files"

PRIORITY_FOLDERS = [
    r".\src",
    r".\src\utils",
    r".\src\modules\Common",
    r".\src\modules\ConfigurationAudit",
    r".\src\modules\ConsistencyChecks",
    r".\src\modules\ProfilesAudit",
    r".\src\modules\CleanUp",
]


def is_excluded_py(path: Path) -> bool:
    """Return True if the file must be excluded by rules."""
    if path.name == "__init__.py":
        return True
    if any(part == "__pycache__" for part in path.parts):
        return True
    return False


def list_py_files(root_folder: Path) -> List[Path]:
    """List eligible *.py under root_folder (recursive), excluding __init__.py and __pycache__."""
    files: List[Path] = []
    for p in root_folder.rglob("*.py"):
        if not p.is_file():
            continue
        if is_excluded_py(p):
            continue
        files.append(p)
    return files


def copy_files_flat_overwrite(files: List[Path], dest_folder: Path) -> int:
    """Copy files into dest_folder (flat) overwriting by filename. Returns number of files copied."""
    copied = 0
    for src in files:
        dest = dest_folder / src.name
        shutil.copy2(str(src), str(dest))
        print(f"[COPIED]  {src} -> {dest}")
        copied += 1
    return copied


def build_priority_selection(script_root: Path, candidates: List[Path], max_files: int) -> List[Path]:
    """Select up to max_files according to PRIORITY_FOLDERS order."""
    candidate_set: Set[Path] = set(p.resolve() for p in candidates)

    selected: List[Path] = []
    seen: Set[Path] = set()

    for rel_folder in PRIORITY_FOLDERS:
        folder = (script_root / Path(rel_folder)).resolve()
        if not folder.exists() or not folder.is_dir():
            continue

        for p in sorted(folder.rglob("*.py")):
            if not p.is_file():
                continue
            rp = p.resolve()
            if rp not in candidate_set:
                continue
            if rp in seen:
                continue
            if is_excluded_py(rp):
                continue

            selected.append(rp)
            seen.add(rp)
            if len(selected) >= max_files:
                return selected

    leftovers = [p for p in sorted(candidate_set) if p not in seen]
    for p in leftovers:
        selected.append(p)
        if len(selected) >= max_files:
            break

    return selected


def main() -> None:
    script_root = Path(__file__).resolve().parent
    src_folder = (script_root / Path(r".\src")).resolve()
    if not src_folder.exists() or not src_folder.is_dir():
        raise SystemExit(f"ERROR: src folder not found: {src_folder}")

    dest_folder = script_root / DEST_FOLDER_NAME
    dest_folder.mkdir(parents=True, exist_ok=True)

    all_candidates = list_py_files(src_folder)
    print(f"Eligible *.py found (excluding __init__.py and __pycache__): {len(all_candidates)}")

    if len(all_candidates) <= MAX_FILES:
        to_copy = sorted(p.resolve() for p in all_candidates)
        print(f"Copying ALL eligible files: {len(to_copy)}")
    else:
        to_copy = build_priority_selection(script_root, all_candidates, MAX_FILES)
        print(f"Candidates exceed {MAX_FILES}. Copying TOP {len(to_copy)} by priority order.")

    copied = copy_files_flat_overwrite(to_copy, dest_folder)
    print(f"\nDone. Copied: {copied} | Output: {dest_folder}")


if __name__ == "__main__":
    main()
