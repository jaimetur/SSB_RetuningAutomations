#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
from typing import List

from src.utils.utils_io import to_long_path, pretty_path

# ============================ OPTIONAL TKINTER UI =========================== #
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception:
    tk = None
    ttk = None
    filedialog = None
    messagebox = None


# ============================ GENERIC YES/NO DIALOGS ========================= #
def ask_reopen_launcher() -> bool:
    title = "Finished"
    message = "The selected task has finished.\nDo you want to open the launcher again?"
    return ask_yes_no_dialog(title, message, default=False)


def ask_yes_no_dialog(title: str, message: str, default: bool = False) -> bool:
    # Tk dialog if possible
    if tk is not None and messagebox is not None:
        try:
            root = tk.Tk()
            root.withdraw()
            try:
                try:
                    root.lift()
                    root.attributes("-topmost", True)
                    root.after(200, lambda: root.attributes("-topmost", False))
                except Exception:
                    pass

                ans = messagebox.askyesno(title, message, parent=root)
                return bool(ans)
            finally:
                root.destroy()
        except Exception:
            pass

    # CLI fallback
    try:
        ans = input(f"{title}\n{message} [y/N]: ").strip().lower()
        return ans in ("y", "yes", "s", "si", "sí")
    except Exception:
        return default


def ask_yes_no_dialog_custom(title: str, message: str, default: bool = True) -> bool:
    """
    Bigger dialog for long messages (Tk), with CLI fallback.
    """
    def _cli_fallback() -> bool:
        default_str = "Y/n" if default else "y/N"
        while True:
            try:
                ans = input(f"{title}\n{message}\n[{default_str}] ").strip().lower()
            except EOFError:
                return default
            if not ans:
                return default
            if ans in ("y", "yes", "s", "si", "sí"):
                return True
            if ans in ("n", "no"):
                return False
            print("Please answer yes or no (y/n).")

    if tk is None or ttk is None or messagebox is None:
        return _cli_fallback()

    try:
        result = {"value": default}

        root = tk.Tk()
        root.title(title)
        root.geometry("1200x450")
        root.resizable(True, True)

        frm = ttk.Frame(root, padding=10)
        frm.grid(row=0, column=0, sticky="nsew")
        root.rowconfigure(0, weight=1)
        root.columnconfigure(0, weight=1)

        ttk.Label(frm, text=title).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 5))

        text_frame = ttk.Frame(frm)
        text_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(0, 10))
        frm.rowconfigure(1, weight=1)
        frm.columnconfigure(0, weight=1)

        txt = tk.Text(text_frame, wrap="none", height=14, width=160)
        txt.insert("1.0", message)
        txt.configure(state="disabled")

        yscroll = ttk.Scrollbar(text_frame, orient="vertical", command=txt.yview)
        xscroll = ttk.Scrollbar(text_frame, orient="horizontal", command=txt.xview)
        txt.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        txt.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        text_frame.rowconfigure(0, weight=1)
        text_frame.columnconfigure(0, weight=1)

        def on_yes():
            result["value"] = True
            root.destroy()

        def on_no():
            result["value"] = False
            root.destroy()

        btn_frame = ttk.Frame(frm)
        btn_frame.grid(row=2, column=0, columnspan=2, sticky="e")

        btn_no = ttk.Button(btn_frame, text="No", command=on_no)
        btn_yes = ttk.Button(btn_frame, text="Yes", command=on_yes)
        btn_no.pack(side="right", padx=5)
        btn_yes.pack(side="right")

        if default:
            btn_yes.focus_set()
        else:
            btn_no.focus_set()

        root.bind("<Return>", lambda _e: on_yes() if default else on_no())
        root.bind("<Escape>", lambda _e: on_no())

        try:
            root.update_idletasks()
            root.lift()
            root.attributes("-topmost", True)
            root.after(200, lambda: root.attributes("-topmost", False))
        except Exception:
            pass

        def on_close():
            result["value"] = default
            root.destroy()

        root.protocol("WM_DELETE_WINDOW", on_close)
        root.mainloop()
        return bool(result["value"])

    except Exception:
        return _cli_fallback()


# ============================ PUBLIC API (USED BY LAUNCHER) ================== #
def browse_input_folders_replace(module_var, input_var, root, module_names: List[str]) -> None:
    """
    Native Tk folder picker (single selection).
    Replaces current value with the selected folder.
    """
    if filedialog is None:
        return

    def _current_paths() -> List[str]:
        return [p.strip() for p in re.split(r"[;\n]+", input_var.get() or "") if p.strip()]

    current = _current_paths()
    initial_dir = current[-1] if current else os.getcwd()

    sel_module = (module_var.get() or "").strip()
    title = f"Select input folder — {sel_module}" if sel_module else "Select input folder"

    path = filedialog.askdirectory(title=title, initialdir=initial_dir)
    if not path:
        return

    input_var.set(pretty_path(os.path.normpath(path)))


def browse_input_folders_add(module_var, input_var, root, module_names: List[str]) -> None:
    """
    Native Tk folder picker (single selection).
    Adds a new folder to the current list (semicolon-separated).
    No loops: user adds again only by pressing the button again.
    """
    if filedialog is None:
        return

    def _split_paths(raw: str) -> List[str]:
        return [p.strip() for p in re.split(r"[;\n]+", raw or "") if p.strip()]

    def _unique_preserve_order(paths: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for p in paths:
            if not p:
                continue

            p_clean = pretty_path(os.path.normpath(p))

            # Case-insensitive dedup on Windows
            k = p_clean.lower() if os.name == "nt" else p_clean
            if k in seen:
                continue

            seen.add(k)
            out.append(p_clean)
        return out

    current = _unique_preserve_order(_split_paths(input_var.get()))
    initial_dir = current[-1] if current else os.getcwd()

    sel_module = (module_var.get() or "").strip()
    title = f"Add other input folder — {sel_module}" if sel_module else "Add other input folder"

    path = filedialog.askdirectory(title=title, initialdir=initial_dir)
    if not path:
        return

    merged = _unique_preserve_order(current + [path])
    input_var.set(";".join(merged))
