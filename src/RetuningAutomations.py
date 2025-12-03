#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main launcher with GUI/CLI to run one of:
  1) Pre/Post Relations Consistency Check (PrePostRelations)
  2) Create Excel from Logs (stub)
  3) Clean-Up (stub)

Behavior:
- If run with NO args, opens a single Tkinter window to choose module,
  input folder(s), and (optionally) frequencies (with defaults).
- If run with CLI args, behaves headless:
    • requires --module
    • uses provided args or persisted defaults (config.cfg) if missing.
"""

import argparse
import os
import sys
import time  # high-resolution timing for module execution
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List
import textwrap
import importlib
from pathlib import Path

# Import our different Classes
from src.utils.utils_datetime import format_duration_hms
from src.utils.utils_dialog import tk, ttk, filedialog, messagebox, ask_reopen_launcher, ask_yes_no_dialog, ask_yes_no_dialog_custom
from src.utils.utils_infrastructure import LoggerDual
from src.utils.utils_io import normalize_csv_list, parse_arfcn_csv_to_set, load_cfg_values, save_cfg_values, log_module_exception, to_long_path, pretty_path, folder_has_valid_logs, detect_pre_post_subfolders

from src.modules.ConsistencyChecks.ConsistencyChecks import ConsistencyChecks
from src.modules.ConfigurationAudit import ConfigurationAudit
from src.modules.CleanUp.InitialCleanUp import InitialCleanUp
from src.modules.CleanUp.FinalCleanUp import FinalCleanUp


# ================================ VERSIONING ================================ #

TOOL_NAME           = "RetuningAutomations"
TOOL_VERSION        = "0.3.10"
TOOL_DATE           = "2025-12-03"
TOOL_NAME_VERSION   = f"{TOOL_NAME}_v{TOOL_VERSION}"
COPYRIGHT_TEXT      = "(c) 2025 - Jaime Tur (jaime.tur@ericsson.com)"
TOOL_DESCRIPTION    = textwrap.dedent(f"""
{TOOL_NAME_VERSION} - {TOOL_DATE}
Multi-Platform/Multi-Arch tool designed to Automate some process during SSB Retuning
©️ 2025 by Jaime Tur (jaime.tur@ericsson.com)
""")

# ================================ DEFAULTS ================================= #
# Input Folder(s)
INPUT_FOLDER = ""        # single-input default if not defined
INPUT_FOLDER_PRE = ""    # default Pre folder for dual-input GUI
INPUT_FOLDER_POST = ""   # default Post folder for dual-input GUI

# Frequencies (single Pre/Post used by ConsistencyChecks)
DEFAULT_N77_SSB_PRE = "648672"
DEFAULT_N77_SSBQ_POST = "647328"

# Default N77B SSB frequency
DEFAULT_N77B_SSB = "653952"

# Default ARFCN lists (CSV) for ConfigurationAudit (PRE)
DEFAULT_ALLOWED_N77_SSB_PRE_CSV = "647328,648672,653952"
DEFAULT_ALLOWED_N77_ARFCN_PRE_CSV = "650006,654652,655324,655984,656656"

# Default ARFCN lists (CSV) for ConfigurationAudit (POST)
DEFAULT_ALLOWED_N77_SSB_POST_CSV = "647328,648672,653952"
DEFAULT_ALLOWED_N77_ARFCN_POST_CSV = "650006,654652,655324,655984,656656"

# Global selectable list for filtering summary columns in ConfigurationAudit
NETWORK_FREQUENCIES: List[str] = [
    "174970","176410","176430","176910","177150","392410","393410","394500","394590","432970",
    "647328","648672","650004","650006","653952",
    "2071667","2071739","2073333","2074999","2076665","2078331","2079997","2081663","2083329"
]

# TABLES_ORDER defines the desired priority of table sheet ordering.
TABLES_ORDER: List[str] = []

# Module names (GUI labels)
MODULE_NAMES = [
    "1. Configuration Audit & Logs Parser",
    "2. Consistency Check (Pre/Post Comparison)",
    "3. Initial Clean-Up (During Maintenance Window)",
    "4. Final Clean-Up (After Retune is completed)",
]

# ============================== PERSISTENT CONFIG =========================== #
CONFIG_DIR  = Path.home() / ".retuning_automations"
CONFIG_PATH = CONFIG_DIR / "config.cfg"
CONFIG_SECTION = "general"

CONFIG_KEY_LAST_INPUT               = "last_input_dir"
CONFIG_KEY_LAST_INPUT_PRE           = "last_input_dir_pre"
CONFIG_KEY_LAST_INPUT_POST          = "last_input_dir_post"
CONFIG_KEY_N77_SSB_PRE              = "n77_ssb_pre"
CONFIG_KEY_N77_SSB_POST             = "n77_ssb_post"
CONFIG_KEY_N77B_SSB                 = "n77b_ssb"
CONFIG_KEY_FREQ_FILTERS             = "summary_freq_filters"
CONFIG_KEY_ALLOWED_N77_SSB_PRE      = "allowed_n77_ssb_pre_csv"
CONFIG_KEY_ALLOWED_N77_ARFCN_PRE    = "allowed_n77_arfcn_pre_csv"
CONFIG_KEY_ALLOWED_N77_SSB_POST     = "allowed_n77_ssb_post_csv"
CONFIG_KEY_ALLOWED_N77_ARFCN_POST   = "allowed_n77_arfcn_post_csv"

# Logic Map -> Key in config
CFG_FIELD_MAP = {
    "last_input":             CONFIG_KEY_LAST_INPUT,
    "last_input_pre":         CONFIG_KEY_LAST_INPUT_PRE,
    "last_input_post":        CONFIG_KEY_LAST_INPUT_POST,
    "n77_ssb_pre":            CONFIG_KEY_N77_SSB_PRE,
    "n77_ssb_post":           CONFIG_KEY_N77_SSB_POST,
    "n77b_ssb":               CONFIG_KEY_N77B_SSB,
    "freq_filters":           CONFIG_KEY_FREQ_FILTERS,
    "allowed_n77_ssb_pre":    CONFIG_KEY_ALLOWED_N77_SSB_PRE,
    "allowed_n77_arfcn_pre":  CONFIG_KEY_ALLOWED_N77_ARFCN_PRE,
    "allowed_n77_ssb_post":   CONFIG_KEY_ALLOWED_N77_SSB_POST,
    "allowed_n77_arfcn_post": CONFIG_KEY_ALLOWED_N77_ARFCN_POST,
}


@dataclass
class GuiResult:
    module: str
    # Single-input mode
    input_dir: str
    # Dual-input mode
    input_pre_dir: str
    input_post_dir: str
    # Common frequencies
    n77_ssb_pre: str
    n77_ssb_post: str
    # N77B SSB frequency
    n77b_ssb: str
    # Summary filters for ConfigurationAudit
    freq_filters_csv: str
    # SSB/ARFCN lists for ConfigurationAudit (PRE)
    allowed_n77_ssb_pre_csv: str
    allowed_n77_arfcn_pre_csv: str
    # SSB/ARFCN lists for ConfigurationAudit (POST)
    allowed_n77_ssb_post_csv: str
    allowed_n77_arfcn_post_csv: str


def is_consistency_module(selected_text: str) -> bool:
    """True if selected module is the second (Consistency Check)."""
    try:
        idx = MODULE_NAMES.index(selected_text)
        return idx == 1
    except ValueError:
        lowered = selected_text.strip().lower()
        return lowered.startswith("2.") or "consistency" in lowered


def gui_config_dialog(
    default_input: str = "",
    default_n77_ssb_pre: str = DEFAULT_N77_SSB_PRE,
    default_n77_ssb_post: str = DEFAULT_N77_SSBQ_POST,
    default_n77b_ssb: str = DEFAULT_N77B_SSB,
    default_filters_csv: str = "",
    default_input_pre: str = "",
    default_input_post: str = "",
    default_allowed_n77_ssb_csv: str = DEFAULT_ALLOWED_N77_SSB_PRE_CSV,
    default_allowed_n77_arfcn_csv: str = DEFAULT_ALLOWED_N77_ARFCN_PRE_CSV,
    default_allowed_n77_ssb_post_csv: str = DEFAULT_ALLOWED_N77_SSB_POST_CSV,
    default_allowed_n77_arfcn_post_csv: str = DEFAULT_ALLOWED_N77_ARFCN_POST_CSV,
) -> Optional[GuiResult]:
    """
    Ventana única con:
      - Combobox de módulo
      - Input único o dual (Pre/Post)
      - Frecuencias N77 Pre/Post + N77B
      - Filtros de Summary (multi-select)
      - Listas Allowed N77 SSB / N77 ARFCN (PRE/POST)
    """
    if tk is None or ttk is None or filedialog is None:
        return None

    root = tk.Tk()
    root.title(f"🛠️ {TOOL_NAME_VERSION} -- 1️⃣ Select Module. 2️⃣ Configure Paths & Freqs. 3️⃣ Press Run to execute...")
    root.resizable(False, False)

    # Center window
    try:
        root.update_idletasks()
        w, h = 760, 720
        sw = root.winfo_screenwidth()
        sh = root.winfo_screenheight()
        x = (sw // 2) - (w // 2)
        y = (sh // 3) - (h // 2)
        root.geometry(f"{w}x{h}+{x}+{y}")
    except Exception:
        pass

    # Vars
    module_var = tk.StringVar(value=MODULE_NAMES[0])
    input_var = tk.StringVar(value=default_input or "")
    input_pre_var = tk.StringVar(value=default_input_pre or "")
    input_post_var = tk.StringVar(value=default_input_post or "")
    n77_ssb_pre_var = tk.StringVar(value=default_n77_ssb_pre or "")
    n77_ssb_post_var = tk.StringVar(value=default_n77_ssb_post or "")
    n77b_ssb_var = tk.StringVar(value=default_n77b_ssb or "")
    selected_csv_var = tk.StringVar(value=normalize_csv_list(default_filters_csv))
    allowed_n77_ssb_pre_var = tk.StringVar(value=normalize_csv_list(default_allowed_n77_ssb_csv))
    allowed_n77_arfcn_pre_var = tk.StringVar(value=normalize_csv_list(default_allowed_n77_arfcn_csv))
    allowed_n77_ssb_post_var = tk.StringVar(value=normalize_csv_list(default_allowed_n77_ssb_post_csv))
    allowed_n77_arfcn_post_var = tk.StringVar(value=normalize_csv_list(default_allowed_n77_arfcn_post_csv))
    result: Optional[GuiResult] = None

    pad = {'padx': 10, 'pady': 6}
    pad_tight = {'padx': 4, 'pady': 2}
    frm = ttk.Frame(root, padding=12)
    frm.pack(fill="both", expand=True)

    # Row 0: module
    ttk.Label(frm, text="Module to run:").grid(row=0, column=0, sticky="w", **pad)
    cmb = ttk.Combobox(frm, textvariable=module_var, values=MODULE_NAMES, state="readonly", width=50)
    cmb.grid(row=0, column=1, columnspan=2, sticky="ew", **pad)

    def browse_single():
        path = filedialog.askdirectory(title="Select input folder", initialdir=input_var.get() or os.getcwd())
        if path:
            input_var.set(path)

    # Single-input frame
    single_frame = ttk.Frame(frm)

    # Spacer row so single_frame has similar height to dual_frame
    ttk.Label(single_frame, text="").grid(row=0, column=0, columnspan=3, sticky="w")

    # Single-input frame
    ttk.Label(single_frame, text="Input folder:").grid(row=1, column=0, sticky="w", **pad)
    ttk.Entry(single_frame, textvariable=input_var, width=80).grid(row=1, column=1, sticky="ew", **pad)
    ttk.Button(single_frame, text="Browse…", command=browse_single).grid(row=1, column=2, sticky="ew", **pad)

    # Spacer row so single_frame has similar height to dual_frame
    ttk.Label(single_frame, text="").grid(row=2, column=0, columnspan=3, sticky="w")

    # Dual-input frame
    dual_frame = ttk.Frame(frm)
    ttk.Label(dual_frame, text="Pre input folder:").grid(row=0, column=0, sticky="w", **pad)
    ttk.Entry(dual_frame, textvariable=input_pre_var, width=80).grid(row=0, column=1, sticky="ew", **pad)

    def browse_pre():
        path = filedialog.askdirectory(title="Select PRE input folder", initialdir=input_pre_var.get() or os.getcwd())
        if path:
            input_pre_var.set(path)

    ttk.Button(dual_frame, text="Browse…", command=browse_pre).grid(row=0, column=2, sticky="ew", **pad)

    ttk.Label(dual_frame, text="Post input folder:").grid(row=1, column=0, sticky="w", **pad)
    ttk.Entry(dual_frame, textvariable=input_post_var, width=80).grid(row=1, column=1, sticky="ew", **pad)

    def browse_post():
        path = filedialog.askdirectory(title="Select POST input folder", initialdir=input_post_var.get() or os.getcwd())
        if path:
            input_post_var.set(path)

    ttk.Button(dual_frame, text="Browse…", command=browse_post).grid(row=1, column=2, sticky="ew", **pad)

    def refresh_input_mode(*_e):
        single_frame.grid_forget()
        dual_frame.grid_forget()
        if is_consistency_module(module_var.get()):
            dual_frame.grid(row=1, column=0, columnspan=3, sticky="ew")
        else:
            single_frame.grid(row=1, column=0, columnspan=3, sticky="ew")

    cmb.bind("<<ComboboxSelected>>", refresh_input_mode)
    refresh_input_mode()

    # Frecuencias
    ttk.Separator(frm).grid(row=2, column=0, columnspan=3, sticky="ew", **pad)
    ttk.Label(frm, text="SSB Frequencies:").grid(row=3, column=0, columnspan=3, sticky="w", **pad)
    ttk.Label(frm, text="N77 SSB Frequency (Pre):").grid(row=4, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=n77_ssb_pre_var, width=22).grid(row=4, column=1, sticky="w", **pad)

    ttk.Label(frm, text="N77 SSB Frequency (Post):").grid(row=5, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=n77_ssb_post_var, width=22).grid(row=5, column=1, sticky="w", **pad)

    ttk.Label(frm, text="N77B SSB Frequency:").grid(row=6, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=n77b_ssb_var, width=22).grid(row=6, column=1, sticky="w", **pad)

    # ARFCN lists
    ttk.Separator(frm).grid(row=7, column=0, columnspan=3, sticky="ew", **pad)
    ttk.Label(frm, text="Allowed SSB & ARFCN sets for Configuration Audit (comma separated values):").grid(row=8, column=0, columnspan=3, sticky="w", **pad)

    ttk.Label(frm, text="[PRE]: Allowed N77 SSB (comma separated values):").grid(row=9, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=allowed_n77_ssb_pre_var, width=40).grid(row=9, column=1, columnspan=2, sticky="ew", **pad)

    ttk.Label(frm, text="[PRE]: Allowed N77 ARFCN (comma separated values):").grid(row=10, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=allowed_n77_arfcn_pre_var, width=40).grid(row=10, column=1, columnspan=2, sticky="ew", **pad)

    ttk.Label(frm, text="[POST]: Allowed N77 SSB (comma separated values):").grid(row=11, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=allowed_n77_ssb_post_var, width=40).grid(row=11, column=1, columnspan=2, sticky="ew", **pad)

    ttk.Label(frm, text="[POST]: Allowed N77 ARFCN (comma separated values):").grid(row=12, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=allowed_n77_arfcn_post_var, width=40).grid(row=12, column=1, columnspan=2, sticky="ew", **pad)

    # Filtros Summary
    ttk.Separator(frm).grid(row=13, column=0, columnspan=3, sticky="ew", **pad)
    ttk.Label(frm, text="Summary Filters (for pivot columns in Configuration Audit):").grid(row=14, column=0, columnspan=3, sticky="w", **pad)

    list_frame = ttk.Frame(frm)
    list_frame.grid(row=15, column=0, columnspan=1, sticky="nsw", **pad_tight)
    ttk.Label(list_frame, text="Available frequencies:").pack(anchor="w")

    lb_container = ttk.Frame(list_frame)
    lb_container.pack(fill="both", expand=True)

    scrollbar = ttk.Scrollbar(lb_container, orient="vertical")
    scrollbar.pack(side="right", fill="y")

    lb = tk.Listbox(
        lb_container,
        selectmode="extended",
        height=10,
        width=24,
        exportselection=False,
        yscrollcommand=scrollbar.set
    )
    for freq in NETWORK_FREQUENCIES:
        lb.insert("end", freq)
    lb.pack(side="left", fill="both", expand=True)
    scrollbar.config(command=lb.yview)

    btns_frame = ttk.Frame(frm)
    btns_frame.grid(row=15, column=1, sticky="n", **pad_tight)

    right_frame = ttk.Frame(frm)
    right_frame.grid(row=15, column=2, sticky="nsew", **pad_tight)
    ttk.Label(right_frame, text="Frequencies Filter (Empty = No Filter):").grid(row=0, column=0, sticky="w")
    ttk.Entry(right_frame, textvariable=selected_csv_var, width=40).grid(row=1, column=0, sticky="ew")

    def current_selected_set() -> List[str]:
        return [s.strip() for s in normalize_csv_list(selected_csv_var.get()).split(",") if s.strip()]

    def add_selected():
        chosen = [lb.get(i) for i in lb.curselection()]
        pool = set(current_selected_set())
        pool.update(chosen)
        selected_csv_var.set(",".join(sorted(pool)))

    def remove_selected():
        chosen = set(lb.get(i) for i in lb.curselection())
        pool = [x for x in current_selected_set() if x not in chosen]
        selected_csv_var.set(",".join(pool))

    def select_all():
        lb.select_set(0, "end")
        add_selected()

    def clear_filters():
        lb.selection_clear(0, "end")
        selected_csv_var.set("")

    ttk.Button(btns_frame, text="Add →", command=add_selected).pack(pady=4, fill="x")
    ttk.Button(btns_frame, text="← Remove", command=remove_selected).pack(pady=4, fill="x")
    ttk.Button(btns_frame, text="Select all", command=select_all).pack(pady=4, fill="x")
    ttk.Button(btns_frame, text="Clear Filter", command=clear_filters).pack(pady=4, fill="x")

    btns = ttk.Frame(frm)
    btns.grid(row=999, column=0, columnspan=3, sticky="e", **pad)

    def on_run():
        nonlocal result
        sel_module = module_var.get().strip()

        normalized_allowed_n77_ssb_pre = normalize_csv_list(allowed_n77_ssb_pre_var.get())
        normalized_allowed_n77_arfcn_pre = normalize_csv_list(allowed_n77_arfcn_pre_var.get())
        normalized_allowed_n77_ssb_post = normalize_csv_list(allowed_n77_ssb_post_var.get())
        normalized_allowed_n77_arfcn_post = normalize_csv_list(allowed_n77_arfcn_post_var.get())

        if is_consistency_module(sel_module):
            sel_input_pre = input_pre_var.get().strip()
            sel_input_post = input_post_var.get().strip()
            if not sel_input_pre and not sel_input_post:
                messagebox.showerror("Missing input", "Please select at least one folder (Pre or Post). The missing one will be auto-detected if possible.")
                return
            result = GuiResult(
                module=sel_module,
                input_dir="",
                input_pre_dir=sel_input_pre,
                input_post_dir=sel_input_post,
                n77_ssb_pre=n77_ssb_pre_var.get().strip(),
                n77_ssb_post=n77_ssb_post_var.get().strip(),
                n77b_ssb=n77b_ssb_var.get().strip(),
                freq_filters_csv=normalize_csv_list(selected_csv_var.get()),
                allowed_n77_ssb_pre_csv=normalized_allowed_n77_ssb_pre,
                allowed_n77_arfcn_pre_csv=normalized_allowed_n77_arfcn_pre,
                allowed_n77_ssb_post_csv=normalized_allowed_n77_ssb_post,
                allowed_n77_arfcn_post_csv=normalized_allowed_n77_arfcn_post,
            )
        else:
            sel_input = input_var.get().strip()
            if not sel_input:
                messagebox.showerror("Missing input", "Please select an input folder.")
                return
            result = GuiResult(
                module=sel_module,
                input_dir=sel_input,
                input_pre_dir="",
                input_post_dir="",
                n77_ssb_pre=n77_ssb_pre_var.get().strip(),
                n77_ssb_post=n77_ssb_post_var.get().strip(),
                n77b_ssb=n77b_ssb_var.get().strip(),
                freq_filters_csv=normalize_csv_list(selected_csv_var.get()),
                allowed_n77_ssb_pre_csv=normalized_allowed_n77_ssb_pre,
                allowed_n77_arfcn_pre_csv=normalized_allowed_n77_arfcn_pre,
                allowed_n77_ssb_post_csv=normalized_allowed_n77_ssb_post,
                allowed_n77_arfcn_post_csv=normalized_allowed_n77_arfcn_post,
            )
        root.destroy()

    def on_cancel():
        nonlocal result
        result = None
        root.destroy()

    ttk.Button(btns, text="Cancel", command=on_cancel).pack(side="right", padx=6)
    ttk.Button(btns, text="Run", command=on_run).pack(side="right")
    root.bind("<Return>", lambda e: on_run())
    root.bind("<Escape>", lambda e: on_cancel())
    root.mainloop()
    return result


# ================================ CLI PARSER ================================ #
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launcher Retuning Automations Tool with GUI fallback.")
    parser.add_argument(
        "--module",
        choices=["configuration-audit", "consistency-check", "initial-cleanup", "final-cleanup"],
        help="Module to run: configuration-audit|consistency-check|initial-cleanup|final-cleanup. "
             "If omitted and no other args are provided, GUI appears (if available)."
    )
    # Single-input (most modules)
    parser.add_argument("--input", help="Input folder to process (single-input modules)")
    # Dual-input (consistency-check)
    parser.add_argument("--input-pre", help="PRE input folder (only for consistency-check)")
    parser.add_argument("--input-post", help="POST input folder (only for consistency-check)")

    parser.add_argument("--n77-ssb-pre", help="Frequency before refarming (Pre)")
    parser.add_argument("--n77-ssb-post", help="Frequency after refarming (Post)")
    parser.add_argument("--n77b-ssb", help="N77B SSB frequency (ARFCN).")
    parser.add_argument("--freq-filters", help="Comma-separated list of frequencies to filter pivot columns in Configuration Audit (substring match per column header).")

    # ARFCN list options for ConfigurationAudit (PRE)
    parser.add_argument("--allowed-n77-ssb-pre", help="Comma-separated SSB (Pre) list for N77 SSB allowed values (Configuration Audit).")
    parser.add_argument("--allowed-n77-arfcn-pre", help="Comma-separated ARFCN (Pre) list for N77 ARFCN allowed values (Configuration Audit).")

    # ARFCN list options for ConfigurationAudit (POST)
    parser.add_argument("--allowed-n77-ssb-post", help="Comma-separated SSB (Post) list for N77 SSB allowed values (Configuration Audit).")
    parser.add_argument("--allowed-n77-arfcn-post", help="Comma-separated ARFCN (Post) list for N77 ARFCN allowed values (Configuration Audit).")

    parser.add_argument("--no-gui", action="store_true", help="Disable GUI usage (only CLI).")

    args = parser.parse_args()
    setattr(args, "_parser", parser)
    return args


# ============================== RUNNERS (TASKS) ============================= #
def run_configuration_audit(
    input_dir: str,
    freq_filters_csv: str = "",
    n77_ssb_pre: Optional[str] = None,
    n77_ssb_post: Optional[str] = None,
    n77b_ssb: Optional[str] = None,
    allowed_n77_ssb_pre_csv: Optional[str] = None,
    allowed_n77_arfcn_pre_csv: Optional[str] = None,
    allowed_n77_ssb_post_csv: Optional[str] = None,
    allowed_n77_arfcn_post_csv: Optional[str] = None,
) -> Optional[str]:
    """
    Run ConfigurationAudit on a folder or recursively on all its subfolders
    that contain valid logs.

    Behavior:
    - If the base folder contains "valid logs" (as per folder_has_valid_logs),
      run ConfigurationAudit only once on that folder.
    - If the base folder does NOT contain valid logs:
        * Ask the user (Tk dialog if available, otherwise console) whether
          to scan subfolders recursively.
        * If accepted, run ConfigurationAudit only in subfolders that
          contain valid logs.
    - A folder is considered to have "valid logs" if folder_has_valid_logs()
      returns True (there is at least one .log/.logs/.txt file with a line
      starting by 'SubNetwork').
    """

    module_name = "[Configuration Audit]"

    if not input_dir:
        print(f"{module_name} [ERROR] No input folder provided.")
        return None

    base_dir_fs = to_long_path(input_dir)

    if not os.path.isdir(base_dir_fs):
        print(f"{module_name} [ERROR] Input folder does not exist or is not a directory: '{pretty_path(base_dir_fs)}'")
        return None

    # Normalize CSV arguments (so recursion uses cleaned values)
    freq_filters_csv = normalize_csv_list(freq_filters_csv or "")
    allowed_n77_ssb_pre_csv = normalize_csv_list(allowed_n77_ssb_pre_csv or "")
    allowed_n77_arfcn_pre_csv = normalize_csv_list(allowed_n77_arfcn_pre_csv or "")
    allowed_n77_ssb_post_csv = normalize_csv_list(allowed_n77_ssb_post_csv or "")
    allowed_n77_arfcn_post_csv = normalize_csv_list(allowed_n77_arfcn_post_csv or "")

    # SSB Pre/Post
    try:
        local_n77_ssb_pre = int(n77_ssb_pre) if n77_ssb_pre else int(DEFAULT_N77_SSB_PRE)
    except ValueError:
        local_n77_ssb_pre = int(DEFAULT_N77_SSB_PRE)

    try:
        local_n77_ssb_post = int(n77_ssb_post) if n77_ssb_post else int(DEFAULT_N77_SSBQ_POST)
    except ValueError:
        local_n77_ssb_post = int(DEFAULT_N77_SSBQ_POST)

    # N77B SSB
    local_n77b_ssb = n77b_ssb
    if local_n77b_ssb:
        try:
            local_n77b_ssb = int(local_n77b_ssb)
        except ValueError:
            print(f"{module_name} [WARN] Invalid N77B SSB frequency '{local_n77b_ssb}'. Ignoring.")
            local_n77b_ssb = None

    # Allowed sets (PRE)
    default_n77_ssb_pre_list = [local_n77_ssb_post, 653952]
    default_n77_pre_list = [654652, 655324, 655984, 656656]

    allowed_n77_ssb_pre = parse_arfcn_csv_to_set(
        csv_text=allowed_n77_ssb_pre_csv,
        default_values=default_n77_ssb_pre_list,
        label="Allowed N77 SSB (Pre)",
    )
    allowed_n77_arfcn_pre = parse_arfcn_csv_to_set(
        csv_text=allowed_n77_arfcn_pre_csv,
        default_values=default_n77_pre_list,
        label="Allowed N77 ARFCN (Pre)",
    )

    # Allowed sets (POST) – by default same values, but independent set
    default_n77_ssb_post_list = [local_n77_ssb_post, 653952]
    default_n77_post_list = [654652, 655324, 655984, 656656]

    allowed_n77_ssb_post = parse_arfcn_csv_to_set(
        csv_text=allowed_n77_ssb_post_csv,
        default_values=default_n77_ssb_post_list,
        label="Allowed N77 SSB (Post)",
    )
    allowed_n77_arfcn_post = parse_arfcn_csv_to_set(
        csv_text=allowed_n77_arfcn_post_csv,
        default_values=default_n77_post_list,
        label="Allowed N77 ARFCN (Post)",
    )

    # Print ConfigurationAudit Settings:
    print(f"{module_name} Configuration Audit Settings:")
    print(f"{module_name} Using old N77 SSB = {local_n77_ssb_pre} --> new N77 SSB = {local_n77_ssb_post}")
    if local_n77b_ssb is not None:
        print(f"{module_name} N77B SSB = {local_n77b_ssb}")
    else:
        print(f"{module_name} N77B SSB not provided or invalid.")

    print(f"{module_name} Allowed N77 SSB set (Pre)    = {sorted(allowed_n77_ssb_pre)}")
    print(f"{module_name} Allowed N77 ARFCN set (Pre)  = {sorted(allowed_n77_arfcn_pre)}")
    print(f"{module_name} Allowed N77 SSB set (Post)   = {sorted(allowed_n77_ssb_post)}")
    print(f"{module_name} Allowed N77 ARFCN set (Post) = {sorted(allowed_n77_arfcn_post)}")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    def run_for_folder(folder: str) -> Optional[str]:
        """
        Run ConfigurationAudit for a single folder that is already known
        to contain valid logs.
        """
        print(f"{module_name} Running Audit…")
        print(f"{module_name} Input folder: '{pretty_path(folder)}'")
        if freq_filters_csv:
            print(f"{module_name} Summary column filters: {freq_filters_csv}")

        # Use long-path version for filesystem operations
        folder_fs = to_long_path(folder) if folder else folder

        # Create dedicated output folder for ConfigurationAudit
        output_dir = os.path.join(folder_fs, f"ConfigurationAudit_{versioned_suffix}")
        os.makedirs(output_dir, exist_ok=True)
        print(f"{module_name} Output folder: '{pretty_path(output_dir)}'")

        # Progressive fallback in case the installed ConfigurationAudit has an older signature
        try:
            app = ConfigurationAudit(
                n77_ssb_pre=local_n77_ssb_pre,
                n77_ssb_post=local_n77_ssb_post,
                n77b_ssb_arfcn=local_n77b_ssb,
                allowed_n77_ssb_pre=allowed_n77_ssb_pre,
                allowed_n77_arfcn_pre=allowed_n77_arfcn_pre,
                allowed_n77_ssb_post=allowed_n77_ssb_post,
                allowed_n77_arfcn_post=allowed_n77_arfcn_post,
            )
        except TypeError:
            print(f"{module_name} [WARN] Installed ConfigurationAudit does not support full PRE/POST + N77B parameters.")
            try:
                app = ConfigurationAudit(
                    n77_ssb_pre=local_n77_ssb_pre,
                    n77_ssb_post=local_n77_ssb_post,
                    allowed_n77_ssb_pre=allowed_n77_ssb_pre,
                    allowed_n77_arfcn_pre=allowed_n77_arfcn_pre,
                    allowed_n77_ssb_post=allowed_n77_ssb_post,
                    allowed_n77_arfcn_post=allowed_n77_arfcn_post,
                )
            except TypeError:
                print(f"{module_name} [WARN] Installed ConfigurationAudit does not support PRE/POST allowed sets.")
                try:
                    app = ConfigurationAudit(
                        n77_ssb_pre=local_n77_ssb_pre,
                        n77_ssb_post=local_n77_ssb_post,
                        allowed_n77_ssb_pre=allowed_n77_ssb_pre,
                        allowed_n77_arfcn_pre=allowed_n77_arfcn_pre,
                    )
                except TypeError:
                    print(f"{module_name} [WARN] Installed ConfigurationAudit only supports basic old/new SSB parameters.")
                    app = ConfigurationAudit(
                        n77_ssb_pre=local_n77_ssb_pre,
                        n77_ssb_post=local_n77_ssb_post,
                    )

        # Include output_dir in kwargs passed to ConfigurationAudit.run
        kwargs = dict(
            module_name=module_name,
            versioned_suffix=versioned_suffix,
            tables_order=TABLES_ORDER,
            output_dir=output_dir,
        )
        if freq_filters_csv:
            kwargs["filter_frequencies"] = [x.strip() for x in freq_filters_csv.split(",") if x.strip()]

        try:
            out = app.run(folder_fs, **kwargs)
        except TypeError:
            # Legacy fallback: ConfigurationAudit without output_dir / filters
            print(f"{module_name} [WARN] Installed ConfigurationAudit does not support 'output_dir' and/or 'filter_frequencies'. Running with legacy signature.")
            out = app.run(folder_fs, module_name=module_name, versioned_suffix=versioned_suffix, tables_order=TABLES_ORDER)

        if out:
            print(f"{module_name} Done → '{pretty_path(out)}'")
            if os.path.isdir(output_dir):
                print(f"{module_name} Outputs saved to: '{pretty_path(output_dir)}'")
        else:
            msg = f"{module_name} No logs found or nothing written."
            print(msg)
            if messagebox is not None:
                try:
                    messagebox.showwarning("Missing Logs in input folder", msg)
                except Exception:
                    pass

        # Return main Excel path so it can be used by ConsistencyChecks
        return out

    # ---- MAIN LOGIC: decide where to run ----

    # 1) If the base folder itself has valid logs, just run once there.
    if folder_has_valid_logs(base_dir_fs):
        return run_for_folder(base_dir_fs)

    # 2) Otherwise, ask the user if we should scan subfolders recursively.
    title = "Missing valid logs in input folder"
    message = (
        f"{module_name} No valid *.log/*.logs/*.txt files with 'SubNetwork' rows were "
        f"found in:\n'{pretty_path(base_dir_fs)}'\n\n"
        "Do you want to search recursively in all subfolders and run "
        "Configuration Audit only in those that contain valid logs?"
    )

    if not ask_yes_no_dialog(title, message, default=False):
        print(f"{module_name} Recursive search cancelled by user.")
        return None

    # 3) Recursive search: only keep subfolders with valid logs
    candidate_dirs: List[str] = []
    for dirpath, dirnames, filenames in os.walk(base_dir_fs):
        if dirpath == base_dir_fs:
            continue
        try:
            if folder_has_valid_logs(dirpath):
                candidate_dirs.append(dirpath)
        except Exception:
            # Any unexpected error checking a folder is ignored; we simply skip it
            continue

    candidate_dirs.sort()
    if not candidate_dirs:
        print(f"{module_name} No subfolders with valid log files were found under '{pretty_path(base_dir_fs)}'.")
        return None

    print(f"{module_name} Found {len(candidate_dirs)} subfolder(s) with valid log files. Running Configuration Audit for each of them...")

    last_excel: Optional[str] = None
    for sub_dir in candidate_dirs:
        print(f"{module_name} → Running Configuration Audit in subfolder: '{pretty_path(sub_dir)}'")
        try:
            excel_path = run_for_folder(sub_dir)
            if excel_path:
                last_excel = excel_path
        except Exception as ex:
            print(f"{module_name} [WARN] Failed to run Configuration Audit in '{pretty_path(sub_dir)}': {ex}")

    return last_excel


def run_consistency_checks(
    input_pre_dir: Optional[str],
    input_post_dir: Optional[str],
    n77_ssb_pre: Optional[str],
    n77_ssb_post: Optional[str],
    n77b_ssb: Optional[str] = None,
    freq_filters_csv: str = "",
    allowed_n77_ssb_pre_csv: Optional[str] = None,
    allowed_n77_arfcn_pre_csv: Optional[str] = None,
    allowed_n77_ssb_post_csv: Optional[str] = None,
    allowed_n77_arfcn_post_csv: Optional[str] = None,
) -> None:
    """
    Runner for ConsistencyChecks supporting dual-input mode.

    Behavior:
    ---------
    - If BOTH PRE and POST folders are explicitly provided:
          -> Use them as a single GLOBAL pair (no auto-detection, no markets).

    - If ONLY ONE folder is provided (typical GUI case where the user selects
      the root 'WP2_SamsungBorder_Logs'):
          1) Use that folder as base_dir.
          2) Call detect_pre_post_subfolders(base_dir) to:
                * Find PRE and POST Step0 runs based on date/time.
                * Detect markets inside those runs (Indiana, Westside, etc.).
          3) Show a confirmation dialog listing ALL markets and their PRE/POST
             folders.
          4) If accepted, run:
                - Configuration Audit on PRE market folder.
                - Configuration Audit on POST market folder.
                - ConsistencyChecks for that market.
             sequentially for each detected market.

    - If detect_pre_post_subfolders cannot find a PRE/POST pair at all
      (i.e. not enough Step0 runs), a warning dialog is shown and no
      processing is performed.
    """
    module_name = "[Consistency Checks (Pre/Post Comparison)]"
    print(f"{module_name} Running Consistency Check…")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    original_pre = (input_pre_dir or "").strip()
    original_post = (input_post_dir or "").strip()
    input_pre_dir = original_pre
    input_post_dir = original_post

    pre_auto_detected = False
    post_auto_detected = False
    used_auto_detection = False

    # market_pairs: { market_label -> (pre_dir, post_dir) }
    market_pairs: Dict[str, Tuple[str, str]] = {}

    # -------------------------------------------------------------------------
    # 1) Auto-detection ONLY when one side is missing (single base root).
    # -------------------------------------------------------------------------
    if not input_pre_dir or not input_post_dir:
        base_dir = input_pre_dir or input_post_dir
        base_dir_fs = to_long_path(base_dir) if base_dir else base_dir

        if base_dir_fs and os.path.isdir(base_dir_fs):
            used_auto_detection = True
            base_pre, base_post, detected_market_pairs = detect_pre_post_subfolders(base_dir_fs)

            input_pre_dir = base_pre
            input_post_dir = base_post
            market_pairs = detected_market_pairs or {}

            if base_pre and not original_pre:
                pre_auto_detected = True
            if base_post and not original_post:
                post_auto_detected = True

            # If we still do not have both PRE and POST, abort with message
            if not input_pre_dir or not input_post_dir:
                msg_lines = [
                    "It was not possible to auto-detect a valid PRE/POST Step0 run",
                    "under the selected root folder:",
                    f"  {pretty_path(base_dir_fs)}",
                    "",
                    "Please select PRE and POST folders manually in the launcher dialog.",
                ]
                msg = "\n".join(msg_lines)
                print(f"{module_name} [WARNING] {msg}")
                if messagebox is not None:
                    try:
                        messagebox.showwarning("Could not auto-detect Pre/Post folders", msg)
                    except Exception:
                        pass
                return

            # If for some reason no markets were detected, use a single GLOBAL pair
            if not market_pairs:
                market_pairs = {"GLOBAL": (input_pre_dir, input_post_dir)}

            # Build confirmation dialog listing all market pairs
            lines: List[str] = []
            lines.append("The following PRE/POST folders have been detected for Consistency Check:\n")
            lines.append(f"  PRE  base run : {pretty_path(input_pre_dir)}" +
                         (" (auto-detected)" if pre_auto_detected else ""))
            lines.append(f"  POST base run : {pretty_path(input_post_dir)}" +
                         (" (auto-detected)" if post_auto_detected else ""))
            lines.append("")
            lines.append("One Consistency Check will be executed per market:\n")

            for market, (pre_mkt, post_mkt) in sorted(market_pairs.items()):
                lines.append(f"  Market: {market}")
                lines.append(f"    PRE : {pretty_path(pre_mkt)}")
                lines.append(f"    POST: {pretty_path(post_mkt)}")
                lines.append("")

            lines.append("Do you want to run Consistency Checks for all these markets?")

            if not ask_yes_no_dialog("Detected Pre/Post folders", "\n".join(lines), default=True):
                print(f"{module_name} User cancelled after Pre/Post auto-detection.")
                return

    # -------------------------------------------------------------------------
    # 2) If NOT in auto-detection mode, we require both dirs explicitly.
    #    Single global PRE/POST pair (no auto markets).
    # -------------------------------------------------------------------------
    if not used_auto_detection:
        if not input_pre_dir or not input_post_dir:
            missing = []
            if not input_pre_dir:
                missing.append("Pre")
            if not input_post_dir:
                missing.append("Post")
            msg = (
                f"Missing required folder(s): {', '.join(missing)}\n\n"
                "No processing will be performed. Please provide both PRE and POST folders."
            )
            print(f"{module_name} [WARNING] {msg}")
            if messagebox is not None:
                try:
                    messagebox.showwarning("Missing Pre/Post folders", msg)
                except Exception:
                    pass
            return

        market_pairs = {"GLOBAL": (input_pre_dir, input_post_dir)}

    # -------------------------------------------------------------------------
    # 3) Run Configuration Audit + ConsistencyChecks per market pair
    # -------------------------------------------------------------------------
    freq_filters_csv = normalize_csv_list(freq_filters_csv or "")
    print(f"{module_name} Detected {len(market_pairs)} market pair(s) to process.")

    for market_label, (pre_dir, post_dir) in sorted(market_pairs.items()):
        market_tag = f"[Market: {market_label}]" if market_label != "GLOBAL" else ""
        print(f"\n{module_name} {market_tag} Processing PRE/POST pair:")
        print(f"{module_name} {market_tag} PRE folder:  '{pretty_path(pre_dir)}'")
        print(f"{module_name} {market_tag} POST folder: '{pretty_path(post_dir)}'")

        pre_dir_fs = to_long_path(pre_dir)
        post_dir_fs = to_long_path(post_dir)

        # --- Run Configuration Audit for PRE and POST ---
        print(f"{module_name} {market_tag} Running Configuration Audit for PRE folder before consistency checks...")
        pre_audit_excel = run_configuration_audit(
            input_dir=pre_dir_fs,
            freq_filters_csv=freq_filters_csv,
            n77_ssb_pre=n77_ssb_pre,
            n77_ssb_post=n77_ssb_post,
            n77b_ssb=n77b_ssb,
            allowed_n77_ssb_pre_csv=allowed_n77_ssb_pre_csv,
            allowed_n77_arfcn_pre_csv=allowed_n77_arfcn_pre_csv,
            allowed_n77_ssb_post_csv=allowed_n77_ssb_post_csv,
            allowed_n77_arfcn_post_csv=allowed_n77_arfcn_post_csv,
        )
        if pre_audit_excel:
            print(f"{module_name} {market_tag} PRE Configuration Audit output: '{pretty_path(pre_audit_excel)}'")
        else:
            print(f"{module_name} {market_tag} PRE Configuration Audit did not generate an output Excel file.")

        print(f"{module_name} {market_tag} Running Configuration Audit for POST folder before consistency checks...")
        post_audit_excel = run_configuration_audit(
            input_dir=post_dir_fs,
            freq_filters_csv=freq_filters_csv,
            n77_ssb_pre=n77_ssb_pre,
            n77_ssb_post=n77_ssb_post,
            n77b_ssb=n77b_ssb,
            allowed_n77_ssb_pre_csv=allowed_n77_ssb_pre_csv,
            allowed_n77_arfcn_pre_csv=allowed_n77_arfcn_pre_csv,
            allowed_n77_ssb_post_csv=allowed_n77_ssb_post_csv,
            allowed_n77_arfcn_post_csv=allowed_n77_arfcn_post_csv,
        )
        if post_audit_excel:
            print(f"{module_name} {market_tag} POST Configuration Audit output: '{pretty_path(post_audit_excel)}'")
        else:
            print(f"{module_name} {market_tag} POST Configuration Audit did not generate an output Excel file.")

        # --- Run ConsistencyChecks for this market ---
        app = ConsistencyChecks(n77_ssb_pre=n77_ssb_pre, n77_ssb_post=n77_ssb_post)

        loaded = False
        try:
            app.loadPrePost(pre_dir_fs, post_dir_fs)
            loaded = True
        except TypeError:
            loaded = False

        if not loaded:
            print(f"{module_name} {market_tag} [ERROR] ConsistencyChecks class does not support dual folders (Pre/Post).")
            print(f"{module_name} {market_tag}         Please update ConsistencyChecks.loadPrePost(pre_dir, post_dir) to enable dual-input mode.")
            continue

        output_dir = os.path.join(post_dir_fs, f"ConsistencyChecks_{versioned_suffix}_{market_label}")

        results = None
        if n77_ssb_pre and n77_ssb_post:
            try:
                # Preferred new signature: (old_ssb, new_ssb, module_name, pre_audit_excel, post_audit_excel)
                results = app.comparePrePost(n77_ssb_pre, n77_ssb_post, module_name, pre_audit_excel, post_audit_excel)
            except TypeError:
                try:
                    # Fallback: older version that only knows post_audit_excel
                    results = app.comparePrePost(n77_ssb_pre, n77_ssb_post, module_name, post_audit_excel)
                except TypeError:
                    try:
                        # Fallback: older version that only knows module_name
                        results = app.comparePrePost(n77_ssb_pre, n77_ssb_post, module_name)
                    except TypeError:
                        # Legacy: only (old_ssb, new_ssb)
                        results = app.comparePrePost(n77_ssb_pre, n77_ssb_post)
        else:
            print(f"{module_name} {market_tag} [INFO] Frequencies not provided. Comparison will be skipped; only tables will be saved.")

        app.save_outputs_excel(output_dir, results, versioned_suffix=versioned_suffix)

        print(f"\n{module_name} {market_tag} Outputs saved to: '{pretty_path(output_dir)}'")
        if results:
            print(f"{module_name} {market_tag} Wrote CellRelation.xlsx and CellRelationDiscrepancies.xlsx (with Summary and details).")
        else:
            print(f"{module_name} {market_tag} Wrote CellRelation.xlsx (all tables). No comparison Excel because frequencies were not provided.")


def run_initial_cleanup(input_dir: str, *_args) -> None:
    module_name = "[Initial Clean-Up]"
    input_dir_fs = to_long_path(input_dir) if input_dir else input_dir

    print(f"{module_name} Running Initial Clean-up…")
    print(f"{module_name} Input folder: '{pretty_path(input_dir_fs)}'")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    app = InitialCleanUp()
    out = app.run(input_dir_fs, module_name=module_name, versioned_suffix=versioned_suffix)

    if out:
        print(f"{module_name} Done → '{pretty_path(out)}'")
    else:
        print(f"{module_name} Module logic not yet implemented (under development). Exiting...")


def run_final_cleanup(input_dir: str, *_args) -> None:
    module_name = "[Final Clean-Up]"
    input_dir_fs = to_long_path(input_dir) if input_dir else input_dir

    print(f"{module_name} Running Final Clean-up…")
    print(f"{module_name} Input folder: '{pretty_path(input_dir_fs)}'")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    app = FinalCleanUp()
    out = app.run(input_dir_fs, module_name=module_name, versioned_suffix=versioned_suffix)

    if out:
        print(f"{module_name} Done → '{pretty_path(out)}'")
    else:
        print(f"{module_name} Module logic not yet implemented (under development). Exiting...")


def resolve_module_callable(name: str):
    name = (name or "").strip().lower()
    if name in ("audit", MODULE_NAMES[0].lower(), "configuration-audit"):
        return run_configuration_audit
    if name in ("consistency-check", MODULE_NAMES[1].lower()):
        return run_consistency_checks
    if name in ("initial-cleanup", MODULE_NAMES[2].lower()):
        return run_initial_cleanup
    if name in ("final-cleanup", MODULE_NAMES[3].lower(), "final-cleanup"):
        return run_final_cleanup
    return None


# =============================== EXECUTION CORE ============================= #
def execute_module(
    module_fn,
    input_dir: str = "",
    input_pre_dir: str = "",
    input_post_dir: str = "",
    n77_ssb_pre: str = "",
    n77_ssb_post: str = "",
    n77b_ssb: str = "",
    freq_filters_csv: str = "",
    allowed_n77_ssb_pre_csv: str = "",
    allowed_n77_arfcn_pre_csv: str = "",
    allowed_n77_ssb_post_csv: str = "",
    allowed_n77_arfcn_post_csv: str = "",
) -> None:
    """
    Launch selected module with the proper signature (and measure execution time).

    Special handling for run_consistency_checks:
    - If BOTH input_pre_dir and input_post_dir are provided, use them directly.
    - If ONLY ONE of them is provided, use that folder as the base folder so
      run_consistency_checks can auto-detect PRE/POST subfolders.
    - If none is provided (should not happen from GUI), fall back to input_dir.
    """
    start_ts = time.perf_counter()
    label = getattr(module_fn, "__name__", "module")

    try:
        if module_fn is run_consistency_checks:
            # Dual-input preferred; also support single base folder for auto-detection.
            if input_pre_dir and input_post_dir:
                # Both PRE and POST explicitly provided
                module_fn(
                    input_pre_dir,
                    input_post_dir,
                    n77_ssb_pre,
                    n77_ssb_post,
                    n77b_ssb,
                    freq_filters_csv,
                    allowed_n77_ssb_pre_csv,
                    allowed_n77_arfcn_pre_csv,
                    allowed_n77_ssb_post_csv,
                    allowed_n77_arfcn_post_csv,
                )
            else:
                # NEW: if only one side is provided, use it as base folder so that
                # run_consistency_checks can auto-detect PRE/POST subfolders.
                base_dir = input_pre_dir or input_post_dir or input_dir
                module_fn(
                    base_dir,
                    None,
                    n77_ssb_pre,
                    n77_ssb_post,
                    n77b_ssb,
                    freq_filters_csv,
                    allowed_n77_ssb_pre_csv,
                    allowed_n77_arfcn_pre_csv,
                    allowed_n77_ssb_post_csv,
                    allowed_n77_arfcn_post_csv,
                )

        elif module_fn is run_configuration_audit:
            module_fn(
                input_dir,
                freq_filters_csv=freq_filters_csv,
                n77_ssb_pre=n77_ssb_pre,
                n77_ssb_post=n77_ssb_post,
                allowed_n77_ssb_pre_csv=allowed_n77_ssb_pre_csv,
                allowed_n77_arfcn_pre_csv=allowed_n77_arfcn_pre_csv,
                allowed_n77_ssb_post_csv=allowed_n77_ssb_post_csv,
                allowed_n77_arfcn_post_csv=allowed_n77_arfcn_post_csv,
                n77b_ssb=n77b_ssb,
            )

        elif module_fn is run_initial_cleanup:
            module_fn(input_dir, n77_ssb_pre, n77_ssb_post)

        elif module_fn is run_final_cleanup:
            module_fn(input_dir, n77_ssb_pre, n77_ssb_post)

        else:
            # Simple fallback for future modules
            module_fn(input_dir, n77_ssb_pre, n77_ssb_post)

    finally:
        elapsed = time.perf_counter() - start_ts
        print(f"[Timer] {label} finished in {format_duration_hms(elapsed)}")



# ================================== MAIN =================================== #
def main():
    os.system('cls' if os.name == 'nt' else 'clear')

    # --- Initialize log file inside ./Logs folder ---
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_filename = f"RetuningAutomation_{timestamp}_v{TOOL_VERSION}.log"

    # Detect base directory
    try:
        base_dir = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))
    except Exception:
        base_dir = os.getcwd()

    logs_dir = os.path.join(base_dir, "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, log_filename)

    # Replace stdout/stderr with our dual logger
    sys.stdout = LoggerDual(log_path)
    sys.stderr = sys.stdout
    print(f"[Logger] Output will also be written to: {log_path}\n")

    print("\nLoading Tool...")
    # Remove Splash image from Pyinstaller
    if '_PYI_SPLASH_IPC' in os.environ and importlib.util.find_spec("pyi_splash"):
        import pyi_splash
        pyi_splash.update_text('UI Loaded ...')
        pyi_splash.close()

    # Remove Splash image from Nuitka
    if "NUITKA_ONEFILE_PARENT" in os.environ:
        import tempfile
        splash_filename = os.path.join(
            tempfile.gettempdir(),
            "onefile_%d_splash_feedback.tmp" % int(os.environ["NUITKA_ONEFILE_PARENT"]),
        )
        with open(splash_filename, "wb") as f:
            f.write(b"READY")
        if os.path.exists(splash_filename):
            os.unlink(splash_filename)

    print("Tool loaded!")
    print(TOOL_DESCRIPTION)
    print(f"\n[Config] Using config file: {CONFIG_PATH}\n")

    # Parse CLI
    args = parse_args()
    parser = getattr(args, "_parser")
    no_args = (len(sys.argv) == 1)

    # Load persisted config (all de golpe)
    cfg = load_cfg_values(
        CONFIG_PATH,
        CONFIG_SECTION,
        CFG_FIELD_MAP,
        "last_input",
        "last_input_pre",
        "last_input_post",
        "n77_ssb_pre",
        "n77_ssb_post",
        "n77b_ssb",
        "freq_filters",
        "allowed_n77_ssb_pre",
        "allowed_n77_arfcn_pre",
        "allowed_n77_ssb_post",
        "allowed_n77_arfcn_post",
    )

    persisted_last_single         = cfg["last_input"]
    persisted_pre_dir             = cfg["last_input_pre"]
    persisted_post_dir            = cfg["last_input_post"]
    persisted_n77_ssb_pre         = cfg["n77_ssb_pre"]
    persisted_n77_ssb_post        = cfg["n77_ssb_post"]
    persisted_n77b_ssb            = cfg["n77b_ssb"]
    persisted_filters             = cfg["freq_filters"]
    persisted_allowed_ssb_pre     = cfg["allowed_n77_ssb_pre"]
    persisted_allowed_arfcn_pre   = cfg["allowed_n77_arfcn_pre"]
    persisted_allowed_ssb_post    = cfg["allowed_n77_ssb_post"]
    persisted_allowed_arfcn_post  = cfg["allowed_n77_arfcn_post"]

    # Defaults (CLI > persisted > hardcode)
    default_input = args.input or persisted_last_single or INPUT_FOLDER or ""
    default_input_pre = args.input_pre or persisted_pre_dir or INPUT_FOLDER_PRE or ""
    default_input_post = args.input_post or persisted_post_dir or INPUT_FOLDER_POST or ""

    default_n77_ssb_pre = args.n77_ssb_pre or persisted_n77_ssb_pre or DEFAULT_N77_SSB_PRE
    default_n77_ssb_post = args.n77_ssb_post or persisted_n77_ssb_post or DEFAULT_N77_SSBQ_POST
    default_n77b_ssb = args.n77b_ssb or persisted_n77b_ssb or DEFAULT_N77B_SSB

    default_filters_csv = normalize_csv_list(args.freq_filters or persisted_filters)

    default_allowed_n77_ssb_pre_csv = normalize_csv_list(args.allowed_n77_ssb_pre or persisted_allowed_ssb_pre or DEFAULT_ALLOWED_N77_SSB_PRE_CSV)
    default_allowed_n77_arfcn_pre_csv = normalize_csv_list(args.allowed_n77_arfcn_pre or persisted_allowed_arfcn_pre or DEFAULT_ALLOWED_N77_ARFCN_PRE_CSV)
    default_allowed_n77_ssb_post_csv = normalize_csv_list(args.allowed_n77_ssb_post or persisted_allowed_ssb_post or DEFAULT_ALLOWED_N77_SSB_POST_CSV)
    default_allowed_n77_arfcn_post_csv = normalize_csv_list(args.allowed_n77_arfcn_post or persisted_allowed_arfcn_post or DEFAULT_ALLOWED_N77_ARFCN_POST_CSV)

    # ====================== MODE 1: GUI (NO ARGS) ===========================
    if no_args:
        if tk is None or args.no_gui:
            print("[INFO] GUI is not available on this system or has been disabled.")
            print("[INFO] Please use the CLI arguments as shown below:\n")
            parser.print_help()
            return

        while True:
            sel = gui_config_dialog(
                default_input=default_input,
                default_input_pre=default_input_pre,
                default_input_post=default_input_post,
                default_n77_ssb_pre=default_n77_ssb_pre,
                default_n77_ssb_post=default_n77_ssb_post,
                default_n77b_ssb=default_n77b_ssb,
                default_filters_csv=default_filters_csv,
                default_allowed_n77_ssb_csv=default_allowed_n77_ssb_pre_csv,
                default_allowed_n77_arfcn_csv=default_allowed_n77_arfcn_pre_csv,
                default_allowed_n77_ssb_post_csv=default_allowed_n77_ssb_post_csv,
                default_allowed_n77_arfcn_post_csv=default_allowed_n77_arfcn_post_csv,
            )
            if sel is None:
                raise SystemExit("Cancelled.")

            module_fn = resolve_module_callable(sel.module)
            if module_fn is None:
                raise SystemExit(f"Unknown module selected: {sel.module}")

            if is_consistency_module(sel.module):
                # Dual-input module: keep single-input default untouched
                input_dir = ""
                default_input_pre = sel.input_pre_dir
                default_input_post = sel.input_post_dir
            else:
                # Single-input module: keep dual-input defaults untouched
                input_dir = sel.input_dir
                default_input = sel.input_dir

            # Build persist kwargs so we do not clear unrelated input dirs
            persist_kwargs = dict(
                n77_ssb_pre=sel.n77_ssb_pre,
                n77_ssb_post=sel.n77_ssb_post,
                n77b_ssb=sel.n77b_ssb,
                freq_filters=sel.freq_filters_csv,
                allowed_n77_ssb_pre=sel.allowed_n77_ssb_pre_csv,
                allowed_n77_arfcn_pre=sel.allowed_n77_arfcn_pre_csv,
                allowed_n77_ssb_post=sel.allowed_n77_ssb_post_csv,
                allowed_n77_arfcn_post=sel.allowed_n77_arfcn_post_csv,
            )

            if is_consistency_module(sel.module):
                # Only persist dual-input paths for consistency-check
                persist_kwargs["last_input_pre"] = sel.input_pre_dir
                persist_kwargs["last_input_post"] = sel.input_post_dir
            else:
                # Only persist single-input path for other modules
                persist_kwargs["last_input"] = sel.input_dir

            # Persist all with a single call (only the relevant input dirs)
            save_cfg_values(
                config_dir=CONFIG_DIR,
                config_path=CONFIG_PATH,
                config_section=CONFIG_SECTION,
                cfg_field_map=CFG_FIELD_MAP,
                **persist_kwargs,
            )

            # Update defaults in memory
            default_n77_ssb_pre = sel.n77_ssb_pre
            default_n77_ssb_post = sel.n77_ssb_post
            default_n77b_ssb = sel.n77b_ssb
            default_filters_csv = sel.freq_filters_csv
            default_allowed_n77_ssb_pre_csv = sel.allowed_n77_ssb_pre_csv
            default_allowed_n77_arfcn_pre_csv = sel.allowed_n77_arfcn_pre_csv
            default_allowed_n77_ssb_post_csv = sel.allowed_n77_ssb_post_csv
            default_allowed_n77_arfcn_post_csv = sel.allowed_n77_arfcn_post_csv

            try:
                execute_module(
                    module_fn,
                    input_dir=input_dir,
                    input_pre_dir=sel.input_pre_dir,
                    input_post_dir=sel.input_post_dir,
                    n77_ssb_pre=sel.n77_ssb_pre,
                    n77_ssb_post=sel.n77_ssb_post,
                    n77b_ssb=sel.n77b_ssb,
                    freq_filters_csv=sel.freq_filters_csv,
                    allowed_n77_ssb_pre_csv=sel.allowed_n77_ssb_pre_csv,
                    allowed_n77_arfcn_pre_csv=sel.allowed_n77_arfcn_pre_csv,
                    allowed_n77_ssb_post_csv=sel.allowed_n77_ssb_post_csv,
                    allowed_n77_arfcn_post_csv=sel.allowed_n77_arfcn_post_csv,
                )
            except Exception as e:
                log_module_exception(sel.module, e)

            if not ask_reopen_launcher():
                break
        return

    # ====================== MODE 2: PURE CLI (WITH ARGS) ====================
    if not args.module:
        print("Error: --module is required when running in CLI mode.\n")
        parser.print_help()
        return

    module_fn = resolve_module_callable(args.module)
    if module_fn is None:
        print(f"Error: Unknown module '{args.module}'.\n")
        parser.print_help()
        return

    n77_ssb_pre = default_n77_ssb_pre
    n77_ssb_post = default_n77_ssb_post
    n77b_ssb = default_n77b_ssb
    freq_filters_csv = default_filters_csv
    allowed_n77_ssb_pre_csv = default_allowed_n77_ssb_pre_csv
    allowed_n77_arfcn_pre_csv = default_allowed_n77_arfcn_pre_csv
    allowed_n77_ssb_post_csv = default_allowed_n77_ssb_post_csv
    allowed_n77_arfcn_post_csv = default_allowed_n77_arfcn_post_csv

    if module_fn is run_consistency_checks:
        input_pre_dir = args.input_pre or default_input_pre
        input_post_dir = args.input_post or default_input_post

        if not input_pre_dir or not input_post_dir:
            print("Error: --input-pre and --input-post are required for consistency-check in CLI mode.\n")
            parser.print_help()
            return

        save_cfg_values(
            config_dir=CONFIG_DIR,
            config_path=CONFIG_PATH,
            config_section=CONFIG_SECTION,
            cfg_field_map=CFG_FIELD_MAP,
            last_input_pre=input_pre_dir,
            last_input_post=input_post_dir,
            n77_ssb_pre=n77_ssb_pre,
            n77_ssb_post=n77_ssb_post,
            n77b_ssb=n77b_ssb,
            freq_filters=freq_filters_csv,
            allowed_n77_ssb_pre=allowed_n77_ssb_pre_csv,
            allowed_n77_arfcn_pre=allowed_n77_arfcn_pre_csv,
            allowed_n77_ssb_post=allowed_n77_ssb_post_csv,
            allowed_n77_arfcn_post=allowed_n77_arfcn_post_csv,
        )

        execute_module(
            module_fn,
            input_pre_dir=input_pre_dir,
            input_post_dir=input_post_dir,
            n77_ssb_pre=n77_ssb_pre,
            n77_ssb_post=n77_ssb_post,
            n77b_ssb=n77b_ssb,
            freq_filters_csv=freq_filters_csv,
            allowed_n77_ssb_pre_csv=allowed_n77_ssb_pre_csv,
            allowed_n77_arfcn_pre_csv=allowed_n77_arfcn_pre_csv,
            allowed_n77_ssb_post_csv=allowed_n77_ssb_post_csv,
            allowed_n77_arfcn_post_csv=allowed_n77_arfcn_post_csv,
        )
        return

    # Other modules: single-input
    input_dir = args.input or default_input
    if not input_dir:
        print("Error: --input is required for this module in CLI mode.\n")
        parser.print_help()
        return

    save_cfg_values(
        config_dir=CONFIG_DIR,
        config_path=CONFIG_PATH,
        config_section=CONFIG_SECTION,
        cfg_field_map=CFG_FIELD_MAP,
        last_input=input_dir,
        n77_ssb_pre=n77_ssb_pre,
        n77_ssb_post=n77_ssb_post,
        n77b_ssb=n77b_ssb,
        freq_filters=freq_filters_csv,
        allowed_n77_ssb_pre=allowed_n77_ssb_pre_csv,
        allowed_n77_arfcn_pre=allowed_n77_arfcn_pre_csv,
        allowed_n77_ssb_post=allowed_n77_ssb_post_csv,
        allowed_n77_arfcn_post=allowed_n77_arfcn_post_csv,
    )

    execute_module(
        module_fn,
        input_dir=input_dir,
        n77_ssb_pre=n77_ssb_pre,
        n77_ssb_post=n77_ssb_post,
        n77b_ssb=n77b_ssb,
        freq_filters_csv=freq_filters_csv,
        allowed_n77_ssb_pre_csv=allowed_n77_ssb_pre_csv,
        allowed_n77_arfcn_pre_csv=allowed_n77_arfcn_pre_csv,
        allowed_n77_ssb_post_csv=allowed_n77_ssb_post_csv,
        allowed_n77_arfcn_post_csv=allowed_n77_arfcn_post_csv,
    )


if __name__ == "__main__":
    main()
