#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import fnmatch
from typing import List, Dict, Optional, Tuple

from src.utils.utils_io import to_long_path, pretty_path, detect_step0_folders, folder_or_zip_has_valid_logs


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


# ============================ STEP0 MULTI-SELECTION HELPERS ================== #
STEP0_SEARCH_MAX_DEPTH = 6  # How many subfolder levels below each selected input folder to scan for Step0 folders
STEP0_LOGS_SEARCH_MAX_DEPTH = 6  # How many subfolder levels below a Step0 folder to scan for valid logs/zips


def get_multi_step0_items(module_var, input_var, module_names: List[str]) -> List[Tuple[str, str]]:
    """
    Return the (parent, step0_folder) items that would be selectable.

    Only includes parents that contain *more than one* valid Step0 folder.
    This is used by the launcher to enable/disable the "Select Subfolders" button.
    """
    def _split_input_paths(raw: str) -> List[str]:
        return [p.strip() for p in re.split(r"[;\n]+", raw or "") if p.strip()]

    def _unique_preserve_order(paths: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for p in paths:
            if not p:
                continue
            p_clean = pretty_path(os.path.normpath(p))
            k = p_clean.lower() if os.name == "nt" else p_clean
            if k in seen:
                continue
            seen.add(k)
            out.append(p_clean)
        return out

    def _folder_tree_has_valid_logs(folder: str, max_depth: int = STEP0_LOGS_SEARCH_MAX_DEPTH) -> bool:
        folder_fs = to_long_path(folder) if folder else folder
        if not folder_fs or not os.path.isdir(folder_fs):
            return False

        if folder_or_zip_has_valid_logs(folder_fs):
            return True

        stack: List[Tuple[str, int]] = [(folder_fs, 0)]
        while stack:
            current_fs, depth = stack.pop()
            if depth >= max_depth:
                continue
            try:
                for e in os.scandir(current_fs):
                    if not e.is_dir(follow_symlinks=False):
                        continue
                    if folder_or_zip_has_valid_logs(e.path):
                        return True
                    stack.append((e.path, depth + 1))
            except Exception:
                continue

        return False

    def _find_valid_step0_folders_under(parent_folder: str, max_depth: int = STEP0_SEARCH_MAX_DEPTH) -> List[str]:
        parent_fs = to_long_path(parent_folder) if parent_folder else parent_folder
        if not parent_fs or not os.path.isdir(parent_fs):
            return []

        candidates: List[Tuple[object, str]] = []
        stack: List[Tuple[str, int]] = [(parent_fs, 0)]

        while stack:
            current_fs, depth = stack.pop()
            if depth > max_depth:
                continue

            try:
                for e in os.scandir(current_fs):
                    if not e.is_dir(follow_symlinks=False):
                        continue

                    info = detect_step0_folders(e.name, current_fs)
                    if info:
                        if _folder_tree_has_valid_logs(info.path, max_depth=STEP0_LOGS_SEARCH_MAX_DEPTH):
                            candidates.append((info.datetime_key, pretty_path(os.path.normpath(info.path))))
                        continue

                    if depth < max_depth:
                        stack.append((e.path, depth + 1))
            except Exception:
                continue

        candidates.sort(key=lambda x: x[0])
        return [p for _dt, p in candidates]

    sel_module = (module_var.get() or "").strip()
    if len(module_names) < 5:
        return []

    multi_modules = (module_names[1], module_names[3], module_names[4])
    if sel_module not in multi_modules:
        return []

    current_paths = _unique_preserve_order(_split_input_paths(input_var.get()))
    if not current_paths:
        return []

    items: List[Tuple[str, str]] = []
    for p in current_paths:
        step0s = _find_valid_step0_folders_under(p, max_depth=STEP0_SEARCH_MAX_DEPTH)
        if len(step0s) <= 1:
            continue
        for step0_folder in step0s:
            items.append((p, step0_folder))

    return items


def select_step0_subfolders(module_var, input_var, root, module_names: List[str]) -> Optional[bool]:
    """
    Expand a base folder into Step0 folders and optionally let the user select them.

    Returns:
    - True: input_var was updated
    - False: no change was required
    - None: a selection dialog was shown and the user cancelled
    """
    def _split_input_paths(raw: str) -> List[str]:
        return [p.strip() for p in re.split(r"[;\n]+", raw or "") if p.strip()]

    def _unique_preserve_order(paths: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for p in paths:
            if not p:
                continue
            p_clean = pretty_path(os.path.normpath(p))
            k = p_clean.lower() if os.name == "nt" else p_clean
            if k in seen:
                continue
            seen.add(k)
            out.append(p_clean)
        return out

    def _match_pattern(text: str, pattern: str) -> bool:
        patt = (pattern or "").strip()
        if not patt:
            return True
        t = text.lower() if os.name == "nt" else text
        p = patt.lower() if os.name == "nt" else patt
        return fnmatch.fnmatch(t, p)

    def _folder_tree_has_valid_logs(folder: str, max_depth: int = STEP0_LOGS_SEARCH_MAX_DEPTH) -> bool:
        folder_fs = to_long_path(folder) if folder else folder
        if not folder_fs or not os.path.isdir(folder_fs):
            return False

        if folder_or_zip_has_valid_logs(folder_fs):
            return True

        stack: List[Tuple[str, int]] = [(folder_fs, 0)]
        while stack:
            current_fs, depth = stack.pop()
            if depth >= max_depth:
                continue
            try:
                for e in os.scandir(current_fs):
                    if not e.is_dir(follow_symlinks=False):
                        continue
                    if folder_or_zip_has_valid_logs(e.path):
                        return True
                    stack.append((e.path, depth + 1))
            except Exception:
                continue

        return False

    def _find_valid_step0_folders_under(parent_folder: str, max_depth: int = STEP0_SEARCH_MAX_DEPTH) -> List[str]:
        parent_fs = to_long_path(parent_folder) if parent_folder else parent_folder
        if not parent_fs or not os.path.isdir(parent_fs):
            return []

        candidates: List[Tuple[object, str]] = []
        stack: List[Tuple[str, int]] = [(parent_fs, 0)]

        while stack:
            current_fs, depth = stack.pop()
            if depth > max_depth:
                continue

            try:
                for e in os.scandir(current_fs):
                    if not e.is_dir(follow_symlinks=False):
                        continue

                    info = detect_step0_folders(e.name, current_fs)
                    if info:
                        if _folder_tree_has_valid_logs(info.path, max_depth=STEP0_LOGS_SEARCH_MAX_DEPTH):
                            candidates.append((info.datetime_key, pretty_path(os.path.normpath(info.path))))
                        continue

                    if depth < max_depth:
                        stack.append((e.path, depth + 1))
            except Exception:
                continue

        candidates.sort(key=lambda x: x[0])
        return [p for _dt, p in candidates]

    def _format_step0_item_label(parent_folder: str, step0_folder: str) -> str:
        parent_clean = pretty_path(os.path.normpath(parent_folder))
        base_name = os.path.basename(parent_clean.rstrip("\\/")) or parent_clean
        step0_clean = pretty_path(os.path.normpath(step0_folder))

        rel = step0_clean
        try:
            rel = os.path.relpath(step0_clean, start=parent_clean)
        except Exception:
            rel = step0_clean

        return f"[{base_name}] {rel}"

    def _pick_step0_subfolders_dialog(parent_root, items: List[Tuple[str, str]], default_pattern: str = "*Step0*") -> Optional[List[str]]:
        if tk is None or ttk is None:
            print("\n[Step0 Selector] Tkinter not available. Using CLI fallback.\n")
            for idx, (parent_folder, step0_folder) in enumerate(items, start=1):
                print(f"{idx:3d}) {_format_step0_item_label(parent_folder, step0_folder)}")
            print("\nType comma-separated indexes to select (e.g. 1,3,5), or press Enter to cancel.")
            ans = input("Selection: ").strip()
            if not ans:
                return None
            selected: List[str] = []
            for part in ans.split(","):
                part = part.strip()
                if not part.isdigit():
                    continue
                i = int(part)
                if 1 <= i <= len(items):
                    selected.append(items[i - 1][1])
            return _unique_preserve_order(selected)

        result: Dict[str, object] = {"selected": None}

        win = tk.Toplevel(parent_root)
        win.title("Select Step0 folders to process")
        win.geometry("1200x650")
        win.resizable(True, True)

        try:
            win.transient(parent_root)
            win.grab_set()
        except Exception:
            pass

        header = ttk.Frame(win, padding=10)
        header.pack(fill="x")

        ttk.Label(header, text="Filter pattern:").pack(side="left")
        filter_var = tk.StringVar(value=default_pattern)
        filter_entry = ttk.Entry(header, textvariable=filter_var, width=40)
        filter_entry.pack(side="left", padx=8)

        body = ttk.Frame(win, padding=10)
        body.pack(fill="both", expand=True)

        canvas = tk.Canvas(body, highlightthickness=0)
        vscroll = ttk.Scrollbar(body, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vscroll.set)
        vscroll.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = ttk.Frame(canvas)
        inner_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_inner_config(_e=None):
            try:
                canvas.configure(scrollregion=canvas.bbox("all"))
            except Exception:
                pass

        def _on_canvas_config(_e=None):
            try:
                canvas.itemconfigure(inner_id, width=canvas.winfo_width())
            except Exception:
                pass

        inner.bind("<Configure>", _on_inner_config)
        canvas.bind("<Configure>", _on_canvas_config)

        vars_list: List[tk.IntVar] = []
        labels_list: List[str] = []

        for idx, (parent_folder, step0_folder) in enumerate(items):
            label = _format_step0_item_label(parent_folder, step0_folder)
            var = tk.IntVar(value=1)
            chk = ttk.Checkbutton(inner, text=label, variable=var)
            chk.grid(row=idx, column=0, sticky="w", pady=2)
            vars_list.append(var)
            labels_list.append(label)

        footer = ttk.Frame(win, padding=10)
        footer.pack(fill="x")

        def select_all():
            for v in vars_list:
                v.set(1)

        def select_none():
            for v in vars_list:
                v.set(0)

        def select_filtered():
            patt = filter_var.get().strip()
            for v, label in zip(vars_list, labels_list):
                v.set(1 if _match_pattern(label, patt) else 0)

        def on_apply():
            selected = [items[i][1] for i, v in enumerate(vars_list) if int(v.get()) == 1]
            result["selected"] = _unique_preserve_order(selected)
            win.destroy()

        def on_cancel():
            result["selected"] = None
            win.destroy()

        ttk.Button(footer, text="Select All", command=select_all).pack(side="left", padx=5)
        ttk.Button(footer, text="Select None", command=select_none).pack(side="left", padx=5)
        ttk.Button(footer, text="Select Filtered", command=select_filtered).pack(side="left", padx=5)
        ttk.Button(footer, text="Cancel", command=on_cancel).pack(side="right", padx=5)
        ttk.Button(footer, text="Apply", command=on_apply).pack(side="right", padx=5)

        try:
            win.update_idletasks()
            win.lift()
            win.attributes("-topmost", True)
            win.after(200, lambda: win.attributes("-topmost", False))
        except Exception:
            pass

        win.bind("<Escape>", lambda _e: on_cancel())
        win.wait_window()
        return result["selected"]  # type: ignore

    sel_module = (module_var.get() or "").strip()
    if len(module_names) < 5:
        return False

    multi_modules = (module_names[1], module_names[3], module_names[4])
    if sel_module not in multi_modules:
        return False

    current_paths = _unique_preserve_order(_split_input_paths(input_var.get()))
    if not current_paths:
        return False

    out_paths: List[str] = []
    multi_parent_map: Dict[str, List[str]] = {}
    multi_parent_order: List[str] = []

    for p in current_paths:
        step0s = _find_valid_step0_folders_under(p, max_depth=STEP0_SEARCH_MAX_DEPTH)
        if len(step0s) == 0:
            out_paths.append(pretty_path(os.path.normpath(p)))
        elif len(step0s) == 1:
            out_paths.append(step0s[0])
        else:
            multi_parent_map[p] = step0s
            multi_parent_order.append(p)

    if not multi_parent_order:
        new_value = ";".join(_unique_preserve_order(out_paths))
        if new_value != (input_var.get() or ""):
            input_var.set(new_value)
            return True
        return False

    items: List[Tuple[str, str]] = []
    for parent in multi_parent_order:
        for step0_folder in multi_parent_map.get(parent, []):
            items.append((parent, step0_folder))

    selected = _pick_step0_subfolders_dialog(root, items, default_pattern="*Step0*")
    if selected is None:
        return None

    merged = _unique_preserve_order(out_paths + selected)
    new_value = ";".join(merged)
    if new_value != (input_var.get() or ""):
        input_var.set(new_value)
        return True

    return False


# ============================ PUBLIC API (USED BY LAUNCHER) ================== #

def browse_input_folders_replace(module_var, input_var, root, module_names: List[str]) -> None:
    """
    Native Tk folder picker (single selection).
    Replaces current value with the selected folder.
    """
    def _split_input_paths(raw: str) -> List[str]:
        return [p.strip() for p in re.split(r"[;\n]+", raw or "") if p.strip()]

    def _unique_preserve_order(paths: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for p in paths:
            if not p:
                continue
            p_clean = pretty_path(os.path.normpath(p))
            k = p_clean.lower() if os.name == "nt" else p_clean
            if k in seen:
                continue
            seen.add(k)
            out.append(p_clean)
        return out

    if filedialog is None:
        return

    current = _unique_preserve_order(_split_input_paths(input_var.get() or ""))
    initial_dir = current[-1] if current else os.getcwd()

    sel_module = (module_var.get() or "").strip()
    title = f"Select input folder — {sel_module}" if sel_module else "Select input folder"

    path = filedialog.askdirectory(title=title, initialdir=initial_dir)
    if not path:
        return

    input_var.set(pretty_path(os.path.normpath(path)))
    # Legacy wrapper used by browse_input_folders_*.
    # If the user cancels the selection dialog, we keep the current input_var value.
    _ = select_step0_subfolders(module_var, input_var, root, module_names)


def browse_input_folders_add(module_var, input_var, root, module_names: List[str]) -> None:
    """
    Native Tk folder picker (single selection).
    Adds a new folder to the current list (semicolon-separated).
    No loops: user adds again only by pressing the button again.
    """
    def _split_input_paths(raw: str) -> List[str]:
        return [p.strip() for p in re.split(r"[;\n]+", raw or "") if p.strip()]

    def _unique_preserve_order(paths: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for p in paths:
            if not p:
                continue
            p_clean = pretty_path(os.path.normpath(p))
            k = p_clean.lower() if os.name == "nt" else p_clean
            if k in seen:
                continue
            seen.add(k)
            out.append(p_clean)
        return out

    if filedialog is None:
        return

    current = _unique_preserve_order(_split_input_paths(input_var.get()))
    initial_dir = current[-1] if current else os.getcwd()

    sel_module = (module_var.get() or "").strip()
    title = f"Add other input folder — {sel_module}" if sel_module else "Add other input folder"

    path = filedialog.askdirectory(title=title, initialdir=initial_dir)
    if not path:
        return

    merged = _unique_preserve_order(current + [path])
    input_var.set(";".join(merged))
    # Legacy wrapper used by browse_input_folders_*.
    # If the user cancels the selection dialog, we keep the current input_var value.
    _ = select_step0_subfolders(module_var, input_var, root, module_names)
