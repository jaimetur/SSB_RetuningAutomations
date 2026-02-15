#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import subprocess
import sys
from pathlib import Path
import tkinter as tk
from tkinter import messagebox

ROOT = Path(__file__).resolve().parents[1]
TOOL_MAIN_PATH = ROOT / "src" / "SSB_RetuningAutomations.py"
DOWNLOAD_SCRIPT = ROOT / "tools" / "Update-Download-Version.py"
GUIDES_SCRIPT = ROOT / "tools" / "Generate-User-Guides.py"


def read_version_date() -> tuple[str, str, str]:
    content = TOOL_MAIN_PATH.read_text(encoding="utf-8")
    version_match = re.search(r'^(TOOL_VERSION\s*=\s*")([^"]+)(")', content, flags=re.MULTILINE)
    date_match = re.search(r'^(TOOL_DATE\s*=\s*")([^"]+)(")', content, flags=re.MULTILINE)
    if not version_match or not date_match:
        raise RuntimeError("Unable to find TOOL_VERSION and/or TOOL_DATE in src/SSB_RetuningAutomations.py")
    return version_match.group(2), date_match.group(2), content


def write_version_date(content: str, new_version: str, new_date: str) -> str:
    updated = re.sub(
        r'^(TOOL_VERSION\s*=\s*")[^"]+(")',
        rf'\g<1>{new_version}\2',
        content,
        flags=re.MULTILINE,
    )
    updated = re.sub(
        r'^(TOOL_DATE\s*=\s*")[^"]+(")',
        rf'\g<1>{new_date}\2',
        updated,
        flags=re.MULTILINE,
    )
    TOOL_MAIN_PATH.write_text(updated, encoding="utf-8")
    return updated


def run_script(path: Path) -> None:
    result = subprocess.run([sys.executable, str(path)], cwd=str(ROOT), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{path.name} failed:\n{result.stdout}\n{result.stderr}")


def main() -> None:
    current_version, current_date, content = read_version_date()

    root = tk.Tk()
    root.title("Update TOOL_VERSION / TOOL_DATE")
    root.geometry("520x220")
    root.resizable(False, False)

    frame = tk.Frame(root, padx=16, pady=16)
    frame.pack(fill="both", expand=True)

    tk.Label(frame, text=f"Current TOOL_VERSION: {current_version}", anchor="w").pack(fill="x")
    tk.Label(frame, text=f"Current TOOL_DATE: {current_date}", anchor="w").pack(fill="x", pady=(0, 12))

    tk.Label(frame, text="New TOOL_VERSION:", anchor="w").pack(fill="x")
    version_var = tk.StringVar(value=current_version)
    tk.Entry(frame, textvariable=version_var).pack(fill="x", pady=(0, 10))

    tk.Label(frame, text="New TOOL_DATE (YYYY-MM-DD):", anchor="w").pack(fill="x")
    date_var = tk.StringVar(value=current_date)
    tk.Entry(frame, textvariable=date_var).pack(fill="x", pady=(0, 12))

    def on_save() -> None:
        new_version = version_var.get().strip()
        new_date = date_var.get().strip()

        if not re.fullmatch(r"\d+\.\d+\.\d+", new_version):
            messagebox.showerror("Invalid version", "TOOL_VERSION must match X.Y.Z")
            return
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", new_date):
            messagebox.showerror("Invalid date", "TOOL_DATE must match YYYY-MM-DD")
            return

        write_version_date(content, new_version, new_date)

        try:
            if new_version != current_version:
                run_script(DOWNLOAD_SCRIPT)
                run_script(GUIDES_SCRIPT)
        except Exception as exc:
            messagebox.showerror("Script execution error", str(exc))
            return

        version_msg = "TOOL_VERSION unchanged."
        if new_version != current_version:
            version_msg = "TOOL_VERSION changed. Update-Download-Version.py and Generate-User-Guides.py executed."

        messagebox.showinfo("Done", f"Updated values successfully.\n\n{version_msg}")
        root.destroy()

    buttons = tk.Frame(frame)
    buttons.pack(fill="x")
    tk.Button(buttons, text="Cancel", command=root.destroy).pack(side="right", padx=(8, 0))
    tk.Button(buttons, text="Save", command=on_save).pack(side="right")

    root.mainloop()


if __name__ == "__main__":
    main()
