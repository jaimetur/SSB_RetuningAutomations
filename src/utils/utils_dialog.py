# ============================ OPTIONAL TKINTER UI =========================== #
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception:
    # Tkinter not available (CLI-only environments)
    tk = None
    ttk = None
    filedialog = None
    messagebox = None


def ask_yes_no_dialog(title: str, message: str, default: bool = False) -> bool:
    """
    Ask a Yes/No question using Tkinter (if available) or console as a fallback.
    """
    if messagebox is not None:
        try:
            # Ensure there is a Tk root before showing the dialog (optional, if you need it)
            if tk is not None and tk._default_root is None:
                root = tk.Tk()
                root.withdraw()
            return bool(messagebox.askyesno(title, message))
        except Exception:
            # If something goes wrong with Tkinter, fall back to console
            pass

    try:
        answer = input(f"{title}\n{message} [y/N]: ").strip().lower()
        return answer in ("y", "yes", "s", "si", "sí")
    except Exception:
        return default

def ask_yes_no_dialog_custom(title: str, message: str, default: bool = True) -> bool:
    """
    Show a Yes/No dialog.

    - If Tkinter is available, open a custom Toplevel window with a scrollable
      Text widget so that very long lines (e.g. Windows paths) are fully visible.
    - If Tkinter is not available (or running in pure CLI), ask in the console.

    Returns True for "Yes" and False for "No".
    """
    # -------------------- CLI fallback (no GUI available) -------------------- #
    if tk is None or ttk is None:
        # Preserve old console behavior
        default_str = "Y/n" if default else "y/N"
        while True:
            try:
                ans = input(f"{title}\n{message}\n[{default_str}] ").strip().lower()
            except EOFError:
                # If stdin is not interactive, just return the default
                return default
            if not ans:
                return default
            if ans in ("y", "yes", "s", "si", "sí"):
                return True
            if ans in ("n", "no"):
                return False
            print("Please answer yes or no (y/n).")
        # Not reached
        # --------------------------------------------------------------------- #

    # ------------------------- GUI implementation --------------------------- #
    # Reuse existing root if any, otherwise create a temporary one
    created_root = False
    root = tk._default_root
    if root is None:
        root = tk.Tk()
        root.withdraw()
        created_root = True

    dialog = tk.Toplevel(root)
    dialog.title(title)
    dialog.resizable(True, True)
    dialog.grab_set()
    dialog.transient(root)

    # Main frame
    frm = ttk.Frame(dialog, padding=10)
    frm.grid(row=0, column=0, sticky="nsew")
    dialog.rowconfigure(0, weight=1)
    dialog.columnconfigure(0, weight=1)

    # Info label
    ttk.Label(frm, text="").grid(row=0, column=0, columnspan=2, sticky="w")

    # Scrollable text area
    text_frame = ttk.Frame(frm)
    text_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(0, 10))
    frm.rowconfigure(1, weight=1)
    frm.columnconfigure(0, weight=1)

    txt = tk.Text(text_frame, wrap="none", height=12, width=100)
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

    result = {"value": default}

    def on_yes():
        result["value"] = True
        dialog.destroy()

    def on_no():
        result["value"] = False
        dialog.destroy()

    # Buttons
    btn_frame = ttk.Frame(frm)
    btn_frame.grid(row=2, column=0, columnspan=2, sticky="e")

    btn_no = ttk.Button(btn_frame, text="No", command=on_no)
    btn_yes = ttk.Button(btn_frame, text="Yes", command=on_yes)

    btn_no.pack(side="right", padx=5)
    btn_yes.pack(side="right")

    # Default focus / Return key behavior
    if default:
        btn_yes.focus_set()
    else:
        btn_no.focus_set()

    def on_return(event):
        if default:
            on_yes()
        else:
            on_no()

    dialog.bind("<Return>", on_return)
    dialog.bind("<Escape>", lambda e: on_no())

    # Center the dialog on screen
    dialog.update_idletasks()
    w = dialog.winfo_width()
    h = dialog.winfo_height()
    sw = dialog.winfo_screenwidth()
    sh = dialog.winfo_screenheight()
    x = (sw // 2) - (w // 2)
    y = (sh // 2) - (h // 2)
    dialog.geometry(f"+{x}+{y}")

    # Wait until the window is closed
    root.wait_window(dialog)

    if created_root:
        root.destroy()

    return result["value"]


def ask_reopen_launcher() -> bool:
    """Ask the user if the launcher should reopen after a module finishes."""
    if messagebox is None:
        return False
    try:
        # Ensure there is a Tk root before showing the dialog (optional, if you need it)
        if tk is not None and tk._default_root is None:
            root = tk.Tk()
            root.withdraw()
        return bool(messagebox.askyesno(
            "Finished",
            "The selected task has finished.\nDo you want to open the launcher again?"
        ))
    except Exception:
        return False

