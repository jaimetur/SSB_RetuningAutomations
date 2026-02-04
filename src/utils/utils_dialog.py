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
    import tkinter.font as tkfont
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

    def _center_window(win_obj) -> None:
        try:
            win_obj.update_idletasks()
            w = win_obj.winfo_width()
            h = win_obj.winfo_height()
            sw = win_obj.winfo_screenwidth()
            sh = win_obj.winfo_screenheight()
            x = max(0, int((sw - w) / 2))
            y = max(0, int((sh - h) / 2))
            win_obj.geometry(f"{w}x{h}+{x}+{y}")
        except Exception:
            pass

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

        _center_window(root)

        try:
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


# ============================ MULTI-SELECTION HELPERS ================== #
def pick_checkboxes_dialog(parent_root, items: List[Tuple[object, object]], title: str, header_hint: str, default_pattern: str, default_checked: int, label_fn, value_fn, geometry: str = "1200x650", checked_fn=None) -> Optional[List[str]]:
    """
    Generic multi-select dialog:
    - Filter pattern
    - Select All / None / Filtered
    - Apply / Cancel
    - Vertical + horizontal scroll
    - CLI fallback if Tk is not available

    Returns:
      - List[str] selected values
      - [] if user applied with none selected
      - None if user cancelled
    """
    def _unique_preserve_order(paths: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for p in paths:
            if not p:
                continue
            p_clean = pretty_path(os.path.normpath(str(p)))
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

    def _center_window(win_obj) -> None:
        try:
            win_obj.update_idletasks()
            w = win_obj.winfo_width()
            h = win_obj.winfo_height()
            sw = win_obj.winfo_screenwidth()
            sh = win_obj.winfo_screenheight()
            x = max(0, int((sw - w) / 2))
            y = max(0, int((sh - h) / 2))
            win_obj.geometry(f"{w}x{h}+{x}+{y}")
        except Exception:
            pass

    def _bring_to_front(win_obj) -> None:
        try:
            win_obj.update_idletasks()
            win_obj.deiconify()
            win_obj.lift()
            try:
                win_obj.attributes("-topmost", True)
                win_obj.after(200, lambda: win_obj.attributes("-topmost", False))
            except Exception:
                pass
            win_obj.focus_force()
        except Exception:
            pass

    if tk is None or ttk is None:
        print(f"\n[{title}] Tkinter not available. Using CLI fallback.\n")
        for idx, it in enumerate(items, start=1):
            print(f"{idx:3d}) {label_fn(it)}")
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
                selected.append(str(value_fn(items[i - 1])))
        return _unique_preserve_order(selected)

    # IMPORTANT: When parent_root is None, create a dedicated Tk root and run mainloop.
    # This avoids the "flash and close instantly" behavior seen on some Windows/Tk setups.
    is_root_dialog = parent_root is None
    win = tk.Tk() if is_root_dialog else tk.Toplevel(parent_root)

    style = ttk.Style()
    style.configure("Mono.TCheckbutton", font=("Consolas", 10))  # o "Courier New"

    win.title(title)
    win.geometry(geometry)
    win.resizable(True, True)

    _center_window(win)
    win.after(0, lambda: _bring_to_front(win))

    try:
        if not is_root_dialog and parent_root is not None:
            win.transient(parent_root)
        win.grab_set()
    except Exception:
        pass

    result: Dict[str, object] = {"selected": None}

    header = ttk.Frame(win, padding=10)
    header.pack(fill="x")

    ttk.Label(header, text="Filter pattern:").pack(side="left")
    filter_var = tk.StringVar(value=default_pattern)
    ttk.Entry(header, textvariable=filter_var, width=40).pack(side="left", padx=8)
    if header_hint:
        ttk.Label(header, text=header_hint).pack(side="left", padx=12)

    body = ttk.Frame(win, padding=10)
    body.pack(fill="both", expand=True)

    canvas = tk.Canvas(body, highlightthickness=0)
    vscroll = ttk.Scrollbar(body, orient="vertical", command=canvas.yview)
    xscroll = ttk.Scrollbar(body, orient="horizontal", command=canvas.xview)
    canvas.configure(yscrollcommand=vscroll.set, xscrollcommand=xscroll.set)

    vscroll.pack(side="right", fill="y")
    xscroll.pack(side="bottom", fill="x")
    canvas.pack(side="left", fill="both", expand=True)

    inner = ttk.Frame(canvas)
    canvas.create_window((0, 0), window=inner, anchor="nw")

    def _on_inner_config(_e=None):
        try:
            canvas.configure(scrollregion=canvas.bbox("all"))
        except Exception:
            pass

    inner.bind("<Configure>", _on_inner_config)

    vars_list: List[tk.IntVar] = []
    labels_list: List[str] = []

    for idx, it in enumerate(items):
        label = str(label_fn(it))
        init_checked = int(default_checked) if checked_fn is None else int(bool(checked_fn(it)))
        var = tk.IntVar(value=init_checked)
        ttk.Checkbutton(inner, text=label, variable=var, style="Mono.TCheckbutton").grid(row=idx, column=0, sticky="w", pady=2)
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
        selected_values = [str(value_fn(items[i])) for i, v in enumerate(vars_list) if int(v.get()) == 1]
        result["selected"] = _unique_preserve_order(selected_values)
        try:
            win.destroy()
        except Exception:
            pass

    def on_cancel():
        result["selected"] = None
        try:
            win.destroy()
        except Exception:
            pass

    def _on_escape(_e=None):
        on_cancel()
        return "break"

    ttk.Button(footer, text="Select All", command=select_all).pack(side="left", padx=5)
    ttk.Button(footer, text="Select None", command=select_none).pack(side="left", padx=5)
    ttk.Button(footer, text="Select Filtered", command=select_filtered).pack(side="left", padx=5)
    ttk.Button(footer, text="Cancel", command=on_cancel).pack(side="right", padx=5)
    ttk.Button(footer, text="Apply", command=on_apply).pack(side="right", padx=5)

    win.protocol("WM_DELETE_WINDOW", on_cancel)
    win.bind("<Escape>", _on_escape)

    if is_root_dialog:
        win.mainloop()
    else:
        win.wait_window()

    return result["selected"]  # type: ignore


# ============================ STEP0 MULTI-SELECTION HELPERS ================== #
def get_multi_step0_items(module_var, input_var, module_names: List[str]) -> List[Tuple[str, str]]:
    """
    Return the (parent, step0_folder) items that would be selectable.

    Only includes parents that contain *more than one* valid Step0 folder.
    This is used by the launcher to enable/disable the "Select Subfolders" button.
    """
    STEP0_SEARCH_MAX_DEPTH = 6  # How many subfolder levels below each selected input folder to scan for Step0 folders
    STEP0_LOGS_SEARCH_MAX_DEPTH = 6  # How many subfolder levels below a Step0 folder to scan for valid logs/zips

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

    def _is_multi_input_module(sel_module: str, names: List[str]) -> bool:
        if len(names) < 5:
            return False
        multi_modules = (names[1], names[3], names[4])
        return sel_module in multi_modules

    def _build_step0_map(parent_folders: List[str]) -> Dict[str, List[str]]:
        """
        Returns a map: parent_folder -> [valid_step0_folder_paths]
        """
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

        step0_map: Dict[str, List[str]] = {}
        for parent in parent_folders:
            step0_map[parent] = _find_valid_step0_folders_under(parent, max_depth=STEP0_SEARCH_MAX_DEPTH)
        return step0_map

    sel_module = (module_var.get() or "").strip()
    if not _is_multi_input_module(sel_module, module_names):
        return []

    current_paths = _unique_preserve_order(_split_input_paths(input_var.get()))
    if not current_paths:
        return []

    def _is_step0_folder_path(path: str) -> bool:
        p = pretty_path(os.path.normpath(path or ""))
        if not p:
            return False
        parent_dir = os.path.dirname(p.rstrip("\\/"))
        base_name = os.path.basename(p.rstrip("\\/"))
        try:
            return bool(detect_step0_folders(base_name, to_long_path(parent_dir) if parent_dir else parent_dir))
        except Exception:
            return False

    scan_parents = current_paths
    if len(current_paths) > 1 and any(_is_step0_folder_path(p) for p in current_paths):
        common_candidates: List[str] = []
        for p in current_paths:
            common_candidates.append(os.path.dirname(p.rstrip("\\/")) if _is_step0_folder_path(p) else p)
        try:
            scan_parents = [pretty_path(os.path.normpath(os.path.commonpath(common_candidates)))]
        except Exception:
            scan_parents = _unique_preserve_order(common_candidates)

    step0_map = _build_step0_map(scan_parents)

    items: List[Tuple[str, str]] = []
    for parent, step0s in step0_map.items():
        for step0_folder in step0s:
            items.append((parent, step0_folder))

    return items


def select_step0_subfolders(module_var, input_var, root, module_names: List[str]) -> Optional[bool]:
    """
    Expand a base folder into Step0 folders and optionally let the user select them.

    Returns:
    - True: input_var was updated
    - False: no change was required
    - None: a selection dialog was shown and the user cancelled
    """
    STEP0_SEARCH_MAX_DEPTH = 6  # How many subfolder levels below each selected input folder to scan for Step0 folders
    STEP0_LOGS_SEARCH_MAX_DEPTH = 6  # How many subfolder levels below a Step0 folder to scan for valid logs/zips

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

    def _is_multi_input_module(sel_module: str, names: List[str]) -> bool:
        if len(names) < 5:
            return False
        multi_modules = (names[1], names[3], names[4])
        return sel_module in multi_modules

    def _build_step0_map(parent_folders: List[str]) -> Dict[str, List[str]]:
        """
        Returns a map: parent_folder -> [valid_step0_folder_paths]
        """
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

        step0_map: Dict[str, List[str]] = {}
        for parent in parent_folders:
            step0_map[parent] = _find_valid_step0_folders_under(parent, max_depth=STEP0_SEARCH_MAX_DEPTH)
        return step0_map

    def _match_pattern(text: str, pattern: str) -> bool:
        patt = (pattern or "").strip()
        if not patt:
            return True
        t = text.lower() if os.name == "nt" else text
        p = patt.lower() if os.name == "nt" else patt
        return fnmatch.fnmatch(t, p)

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


    sel_module = (module_var.get() or "").strip()
    if not _is_multi_input_module(sel_module, module_names):
        return False

    current_paths = _unique_preserve_order(_split_input_paths(input_var.get()))
    if not current_paths:
        return False

    def _path_key(p: str) -> str:
        p_clean = pretty_path(os.path.normpath(p or ""))
        return p_clean.lower() if os.name == "nt" else p_clean

    def _is_step0_folder_path(path: str) -> bool:
        p = pretty_path(os.path.normpath(path or ""))
        if not p:
            return False
        parent_dir = os.path.dirname(p.rstrip("\\/"))
        base_name = os.path.basename(p.rstrip("\\/"))
        try:
            return bool(detect_step0_folders(base_name, to_long_path(parent_dir) if parent_dir else parent_dir))
        except Exception:
            return False

    existing_paths = current_paths
    existing_keys = set(_path_key(p) for p in existing_paths)

    scan_parents = existing_paths
    if len(existing_paths) > 1 and any(_is_step0_folder_path(p) for p in existing_paths):
        common_candidates: List[str] = []
        for p in existing_paths:
            common_candidates.append(os.path.dirname(p.rstrip("\\/")) if _is_step0_folder_path(p) else p)
        try:
            scan_parents = [pretty_path(os.path.normpath(os.path.commonpath(common_candidates)))]
        except Exception:
            scan_parents = _unique_preserve_order(common_candidates)

    step0_map = _build_step0_map(scan_parents)

    selectable_items: List[Tuple[str, str]] = []
    for parent, step0s in step0_map.items():
        for step0_folder in step0s:
            # if _path_key(step0_folder) in existing_keys:
            #     continue
            selectable_items.append((parent, step0_folder))

    if not selectable_items:
        return False

    def _step0_item_sort_key(it: Tuple[str, str]) -> Tuple[str, str, str]:
        parent = pretty_path(os.path.normpath(it[0] or ""))
        step0 = pretty_path(os.path.normpath(it[1] or ""))

        parent_base = os.path.basename(parent.rstrip("\\/")) or parent

        step0_parent = os.path.dirname(step0.rstrip("\\/"))
        step0_parent_base = os.path.basename(step0_parent.rstrip("\\/")) or step0_parent

        step0_base = os.path.basename(step0.rstrip("\\/")) or step0

        k1 = parent_base.lower() if os.name == "nt" else parent_base
        k2 = step0_parent_base.lower() if os.name == "nt" else step0_parent_base
        k3 = step0_base.lower() if os.name == "nt" else step0_base
        return (k1, k2, k3)

    def _format_step0_item_label(it: Tuple[str, str]) -> str:
        parent = pretty_path(os.path.normpath(it[0] or ""))
        step0 = pretty_path(os.path.normpath(it[1] or ""))

        parent_base = os.path.basename(parent.rstrip("\\/")) or parent

        step0_parent = os.path.dirname(step0.rstrip("\\/"))
        step0_parent_base = os.path.basename(step0_parent.rstrip("\\/")) or step0_parent

        step0_base = os.path.basename(step0.rstrip("\\/")) or step0

        # Adjust Width
        PARENT_W = 10
        STEP0_PARENT_W = 30

        pad1 = max(0, PARENT_W - len(parent_base))
        pad2 = max(0, STEP0_PARENT_W - len(step0_parent_base))

        # 3 columns: [parent] - [step0_parent] --> [step0]
        return (f"[{parent_base}]{' ' * pad1}  [{step0_parent_base}]{' ' * pad2} --> {step0_base}")

    selectable_items = sorted(selectable_items, key=_step0_item_sort_key)

    selected = pick_checkboxes_dialog(
        root,
        selectable_items,
        title="Subfolders Selection: Select Step0 folders to process",
        header_hint="Select which sub-folders do you want to process.",
        default_pattern="*Step0*",
        default_checked=0,
        label_fn=_format_step0_item_label,
        value_fn=lambda it: it[1],
        checked_fn=lambda it: (_path_key(it[1]) in existing_keys),
    )

    # selected = pick_checkboxes_dialog(root, selectable_items, title="Subfolders Selection: Select Step0 folders to process", header_hint="Select which sub-folders do you want to process.", default_pattern="*Step0*", default_checked=1, label_fn=lambda it: _format_step0_item_label(it[0], it[1]), value_fn=lambda it: it[1])


    if selected is None:
        return None

    merged = _unique_preserve_order(selected)
    new_value = ";".join(merged)
    if new_value != (input_var.get() or ""):
        input_var.set(new_value)
        return True

    return False



# ============================ PUBLIC API (USED BY LAUNCHER) ================== #
def browse_input_folders(module_var, input_var, root, module_names: List[str], add_mode: bool) -> None:
    """
    Native Tk folder picker (single selection).
    - add_mode=False => replace current value
    - add_mode=True  => add folder to current list (semicolon-separated)
    """
    if filedialog is None:
        return

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

    current = _unique_preserve_order(_split_input_paths(input_var.get() or ""))
    initial_dir = current[-1] if current else os.getcwd()

    sel_module = (module_var.get() or "").strip()
    if add_mode:
        title = f"Add other input folder — {sel_module}" if sel_module else "Add other input folder"
    else:
        title = f"Select input folder — {sel_module}" if sel_module else "Select input folder"

    path = filedialog.askdirectory(title=title, initialdir=initial_dir)
    if not path:
        return

    if add_mode:
        merged = _unique_preserve_order(current + [pretty_path(os.path.normpath(path))])
        input_var.set(";".join(merged))
    else:
        input_var.set(pretty_path(os.path.normpath(path)))

    _ = select_step0_subfolders(module_var, input_var, root, module_names)





