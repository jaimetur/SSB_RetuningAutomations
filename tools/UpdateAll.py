#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import subprocess
import sys
from pathlib import Path
import tkinter as tk
from tkinter import messagebox

ROOT = Path(__file__).resolve().parents[1]
TOOL_MAIN_PATH = ROOT / "src" / "SSB_RetuningAutomations.py"
DOWNLOAD_SCRIPT = ROOT / "tools" / "UpdateDownloadLinks.py"
GUIDES_SCRIPT = ROOT / "tools" / "UpdateUserGuides.py"


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


def center_window(win: tk.Tk) -> None:
    win.update_idletasks()
    w = win.winfo_width()
    h = win.winfo_height()
    sw = win.winfo_screenwidth()
    sh = win.winfo_screenheight()
    x = max((sw - w) // 2, 0)
    y = max((sh - h) // 2, 0)
    win.geometry(f"{w}x{h}+{x}+{y}")


def clear_console() -> None:
    # Limpia la consola solo si es un terminal real (evita \f en consolas embebidas tipo PyCharm).
    try:
        if sys.stdout.isatty():
            os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def run_script(path: Path, *args: str) -> None:
    # NOTE: No capturamos stdout/stderr -> se verán en la consola (PyCharm/terminal).
    result = subprocess.run(
        [sys.executable, str(path), *args],
        cwd=str(ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError(f"{path.name} failed (exit code {result.returncode}). Check console output above.")


def main() -> None:
    current_version, current_date, _content = read_version_date()

    root = tk.Tk()
    root.title("Update TOOL_VERSION / TOOL_DATE")
    root.geometry("640x270")
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

    tk.Label(frame, text="User guide formats to update:", anchor="w").pack(fill="x")
    formats_frame = tk.Frame(frame)
    formats_frame.pack(fill="x", pady=(4, 12))

    format_vars: dict[str, tk.BooleanVar] = {
        "docx": tk.BooleanVar(value=True),
        "pptx": tk.BooleanVar(value=True),
        "docx.pdf": tk.BooleanVar(value=True),
        "pptx.pdf": tk.BooleanVar(value=True),
    }

    tk.Checkbutton(formats_frame, text=".docx", variable=format_vars["docx"]).grid(row=0, column=0, sticky="w", padx=(0, 12))
    tk.Checkbutton(formats_frame, text=".pptx", variable=format_vars["pptx"]).grid(row=0, column=1, sticky="w", padx=(0, 12))
    tk.Checkbutton(formats_frame, text=".docx.pdf", variable=format_vars["docx.pdf"]).grid(row=0, column=2, sticky="w")
    tk.Checkbutton(formats_frame, text=".pptx.pdf", variable=format_vars["pptx.pdf"]).grid(row=0, column=3, sticky="w", padx=(0, 12))

    def validate_inputs() -> tuple[str, str] | None:
        new_version = version_var.get().strip()
        new_date = date_var.get().strip()

        if not re.fullmatch(r"\d+\.\d+\.\d+", new_version):
            messagebox.showerror("Invalid version", "TOOL_VERSION must match X.Y.Z", parent=root)
            return None
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", new_date):
            messagebox.showerror("Invalid date", "TOOL_DATE must match YYYY-MM-DD", parent=root)
            return None
        return new_version, new_date

    def apply_version_date_from_inputs() -> tuple[str, str] | None:
        validated = validate_inputs()
        if not validated:
            return None
        new_version, new_date = validated
        print(f"▶️ Updating TOOL_VERSION and TOOL_DATE to : {new_version} - {new_date}...")

        # Re-read file to avoid using stale content
        fresh = TOOL_MAIN_PATH.read_text(encoding="utf-8")
        write_version_date(fresh, new_version, new_date)
        return new_version, new_date

    def selected_guide_formats() -> list[str] | None:
        selected = [fmt for fmt, var in format_vars.items() if var.get()]
        if not selected:
            messagebox.showerror("No formats selected", "Select at least one user guide format.", parent=root)
            return None
        return selected

    def on_update_version_date() -> None:
        clear_console()
        applied = apply_version_date_from_inputs()
        if not applied:
            return
        root.destroy()

    def on_update_all() -> None:
        clear_console()

        selected_formats = selected_guide_formats()
        if not selected_formats:
            return

        applied = apply_version_date_from_inputs()
        if not applied:
            return

        try:
            run_script(DOWNLOAD_SCRIPT)
            print("")
            run_script(GUIDES_SCRIPT, "--formats", *selected_formats)
        except Exception as exc:
            messagebox.showerror("Script execution error", str(exc), parent=root)
            return

        root.destroy()

    def on_generate_guides() -> None:
        clear_console()

        selected_formats = selected_guide_formats()
        if not selected_formats:
            return

        try:
            run_script(GUIDES_SCRIPT, "--formats", *selected_formats)
        except Exception as exc:
            messagebox.showerror("Script execution error", str(exc), parent=root)
            return

        root.destroy()

    def on_update_download_links() -> None:
        clear_console()

        try:
            run_script(DOWNLOAD_SCRIPT)
        except Exception as exc:
            messagebox.showerror("Script execution error", str(exc), parent=root)
            return

        root.destroy()

    buttons = tk.Frame(frame)
    buttons.pack(fill="x")
    tk.Button(buttons, text="Cancel", command=root.destroy).pack(side="right", padx=(8, 0))
    tk.Button(buttons, text="Update Version/Date", command=on_update_version_date).pack(side="left")
    tk.Button(buttons, text="Update Download Links", command=on_update_download_links).pack(side="left", padx=(8, 0))
    tk.Button(buttons, text="Update User Guides", command=on_generate_guides).pack(side="left", padx=(8, 0))
    tk.Button(buttons, text="Update All", command=on_update_all).pack(side="right")

    center_window(root)
    root.mainloop()


if __name__ == "__main__":
    main()
