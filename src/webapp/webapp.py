from __future__ import annotations

import json
import logging
import os
import re
import shutil
import zipfile
from logging.handlers import RotatingFileHandler
import secrets
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext

from src.utils.utils_io import load_cfg_values, save_cfg_values

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "web_frontend.db"
ACCESS_LOG_PATH = DATA_DIR / "access.log"
APP_LOG_PATH = DATA_DIR / "app.log"

CONFIG_DIR = Path.home() / ".retuning_automations"
CONFIG_PATH = CONFIG_DIR / "config.cfg"
CONFIG_SECTION = "general"

CFG_FIELD_MAP = {
    "last_input": "last_input_dir",
    "last_input_audit": "last_input_dir_audit",
    "last_input_cc_pre": "last_input_dir_cc_pre",
    "last_input_cc_post": "last_input_dir_cc_post",
    "last_input_cc_bulk": "last_input_dir_cc_bulk",
    "last_input_final_cleanup": "last_input_dir_final_cleanup",
    "n77_ssb_pre": "n77_ssb_pre",
    "n77_ssb_post": "n77_ssb_post",
    "n77b_ssb": "n77b_ssb",
    "ca_freq_filters": "ca_freq_filters",
    "cc_freq_filters": "cc_freq_filters",
    "allowed_n77_ssb_pre": "allowed_n77_ssb_pre_csv",
    "allowed_n77_arfcn_pre": "allowed_n77_arfcn_pre_csv",
    "allowed_n77_ssb_post": "allowed_n77_ssb_post_csv",
    "allowed_n77_arfcn_post": "allowed_n77_arfcn_post_csv",
    "profiles_audit": "profiles_audit",
    "frequency_audit": "frequency_audit",
    "export_correction_cmd": "export_correction_cmd",
    "fast_excel_export": "fast_excel_export",
    "network_frequencies": "network_frequencies",
}

CFG_FIELDS = tuple(CFG_FIELD_MAP.keys())

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("web_frontend")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        # Persist application logs across restarts.
        app_handler = RotatingFileHandler(APP_LOG_PATH, maxBytes=2_000_000, backupCount=3)
        app_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(app_handler)
    return logger


def setup_access_logger() -> logging.Logger:
    logger = logging.getLogger("web_access")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        # Keep HTTP access logs separate from application logs.
        access_handler = RotatingFileHandler(ACCESS_LOG_PATH, maxBytes=2_000_000, backupCount=3)
        access_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(access_handler)
    return logger


logger = setup_logging()
access_logger = setup_access_logger()


MODULE_OPTIONS = [
    ("configuration-audit", "1. Configuration Audit & Logs Parser"),
    ("consistency-check", "2. Consistency Check (Pre/Post Comparison)"),
    ("consistency-check-bulk", "3. Consistency Check (Bulk mode Pre/Post auto-detection)"),
    ("final-cleanup", "4. Final Clean-Up"),
]

TOOL_METADATA_PATH = PROJECT_ROOT / "src" / "SSB_RetuningAutomations.py"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_tool_metadata() -> dict[str, str]:
    version = "unknown"
    date = "unknown"
    try:
        content = TOOL_METADATA_PATH.read_text(encoding="utf-8", errors="ignore")
        for line in content.splitlines():
            if line.strip().startswith("TOOL_VERSION"):
                version = line.split("=", 1)[1].strip().strip('"').strip("'")
            if line.strip().startswith("TOOL_DATE"):
                date = line.split("=", 1)[1].strip().strip('"').strip("'")
            if version != "unknown" and date != "unknown":
                break
    except OSError:
        pass
    return {"version": version, "date": date}


def format_timestamp(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo:
            parsed = parsed.astimezone()
        return parsed.strftime("%Y-%m-%d-%H:%M:%S")
    except ValueError:
        return value


def strip_ansi(text: str) -> str:
    if not text:
        return text
    return re.sub(r"\x1b\\[[0-9;]*[A-Za-z]", "", text)


def sanitize_component(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "_", value.strip())
    return cleaned or "unknown"


def safe_extract_zip(zip_path: Path, target_dir: Path) -> None:
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            name = member.filename
            if not name or name.endswith("/"):
                continue
            flat_name = Path(name).name
            if not flat_name:
                continue
            member_path = target_dir / flat_name
            resolved = member_path.resolve()
            if not str(resolved).startswith(str(target_dir.resolve())):
                continue
            with zf.open(member) as source, member_path.open("wb") as dest:
                shutil.copyfileobj(source, dest)


def remove_output_folders(root_dir: Path) -> None:
    output_prefixes = ("ConfigurationAudit_", "ConsistencyChecks_", "ConcistencyChecks_", "FinalCleanUp_", "Cleanup_")
    for path in root_dir.rglob("*"):
        if path.is_dir() and path.name.startswith(output_prefixes):
            shutil.rmtree(path, ignore_errors=True)


def create_zip_from_dir(source_dir: Path, dest_zip: Path) -> None:
    if dest_zip.exists():
        dest_zip.unlink()
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in source_dir.rglob("*"):
            if file_path.is_file():
                zf.write(file_path, file_path.relative_to(source_dir))


def is_safe_path(base_dir: Path, target: Path) -> bool:
    try:
        return str(target.resolve()).startswith(str(base_dir.resolve()))
    except OSError:
        return False


def compute_dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            try:
                total += file_path.stat().st_size
            except OSError:
                continue
    return total


def format_mb(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.2f} MB"


def format_seconds_hms(value: float | int | None) -> str:
    if value is None:
        return "00:00:00"
    total = int(round(value))
    hours = total // 3600
    minutes = (total % 3600) // 60
    seconds = total % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def get_user_storage_dirs(username: str) -> dict[str, Path]:
    safe_name = sanitize_component(username)
    user_root = DATA_DIR / "users" / safe_name
    return {
        "uploads": user_root / "upload",
        "exports": user_root / "export",
    }


def parse_frequency_csv(raw: str) -> list[str]:
    values = [v.strip() for v in (raw or "").split(",") if v.strip()]
    return values


def sort_frequencies(values: list[str]) -> list[str]:
    def sort_key(value: str) -> tuple[int, int | str]:
        try:
            return (0, int(value))
        except ValueError:
            return (1, value)

    return sorted(values, key=sort_key)


def load_default_network_frequencies() -> list[str]:
    try:
        content = TOOL_METADATA_PATH.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return []

    match = re.search(r"NETWORK_FREQUENCIES\s*:\s*List\[str\]\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if not match:
        match = re.search(r"NETWORK_FREQUENCIES\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if not match:
        return []

    block = match.group(1)
    values = re.findall(r"['\"](\d+)['\"]", block)
    return sort_frequencies(values)


def load_network_frequencies() -> list[str]:
    config_values = load_persistent_config()
    persisted = parse_frequency_csv(config_values.get("network_frequencies", ""))
    if persisted:
        return sort_frequencies(persisted)
    return load_default_network_frequencies()


def find_latest_output_dir(base_dir: Path, prefixes: tuple[str, ...]) -> Path | None:
    if not base_dir.exists():
        return None
    candidates = [p for p in base_dir.iterdir() if p.is_dir() and p.name.startswith(prefixes)]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def build_output_prefixes(module_value: str) -> tuple[str, ...]:
    if module_value == "consistency-check":
        return ("ConsistencyChecks_", "ConcistencyChecks_")
    if module_value == "consistency-check-bulk":
        return ("ConsistencyChecks_", "ConcistencyChecks_")
    if module_value == "final-cleanup":
        return ("FinalCleanUp_", "Cleanup_")
    return ("ConfigurationAudit_", "ProfilesAudit_")


def init_db() -> None:
    # Initialize schema and seed the default admin account if missing.
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            settings_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS task_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            module TEXT NOT NULL,
            tool_version TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            duration_seconds REAL,
            command TEXT NOT NULL,
            output_log TEXT,
            input_dir TEXT,
            output_dir TEXT,
            output_zip TEXT,
            output_log_file TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )

    existing_columns = {
        row["name"] for row in cur.execute("PRAGMA table_info(task_runs)").fetchall()
    }
    if "input_dir" not in existing_columns:
        cur.execute("ALTER TABLE task_runs ADD COLUMN input_dir TEXT")
    if "output_dir" not in existing_columns:
        cur.execute("ALTER TABLE task_runs ADD COLUMN output_dir TEXT")
    if "output_zip" not in existing_columns:
        cur.execute("ALTER TABLE task_runs ADD COLUMN output_zip TEXT")
    if "output_log_file" not in existing_columns:
        cur.execute("ALTER TABLE task_runs ADD COLUMN output_log_file TEXT")
    if "tool_version" not in existing_columns:
        cur.execute("ALTER TABLE task_runs ADD COLUMN tool_version TEXT")

    admin = cur.execute(
        "SELECT id, password_hash FROM users WHERE username = ?", ("admin",)
    ).fetchone()
    if admin is None:
        default_password = "admin123"
        cur.execute(
            "INSERT INTO users(username, password_hash, role, active, created_at) VALUES (?, ?, ?, 1, ?)",
            (
                "admin",
                pwd_context.hash(default_password),
                "admin",
                now_iso(),
            ),
        )
        logger.info("Admin user created with default credentials admin/admin123. Change it immediately.")
    else:
        password_hash = admin["password_hash"] or ""
        if password_hash.startswith(("$2a$", "$2b$", "$2y$")):
            # Reset legacy bcrypt hashes to avoid backend errors in minimal containers.
            default_password = "admin123"
            cur.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (pwd_context.hash(default_password), admin["id"]),
            )
            logger.info("Admin password hash reset to pbkdf2_sha256 defaults. Change it immediately.")

    conn.commit()
    conn.close()


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def get_current_user(request: Request) -> sqlite3.Row | None:
    token = request.cookies.get("session_token")
    if not token:
        return None
    conn = get_conn()
    session = conn.execute(
        "SELECT token, user_id, active FROM sessions WHERE token = ?", (token,)
    ).fetchone()
    if not session or session["active"] == 0:
        conn.close()
        return None

    user = conn.execute("SELECT * FROM users WHERE id = ?", (session["user_id"],)).fetchone()
    if not user or user["active"] == 0:
        conn.close()
        return None

    conn.execute("UPDATE sessions SET last_seen_at = ? WHERE token = ?", (now_iso(), token))
    conn.commit()
    conn.close()
    return user


def require_user(request: Request) -> sqlite3.Row:
    user = get_current_user(request)
    if user is None:
        raise PermissionError("Authentication required")
    return user


def require_admin(request: Request) -> sqlite3.Row:
    user = require_user(request)
    if user["role"] != "admin":
        raise PermissionError("Admin required")
    return user


def save_user_settings(user_id: int, settings: dict[str, Any]) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO user_settings(user_id, settings_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET settings_json = excluded.settings_json, updated_at = excluded.updated_at
        """,
        (user_id, json.dumps(settings), now_iso()),
    )
    conn.commit()
    conn.close()


def load_user_settings(user_id: int) -> dict[str, Any]:
    conn = get_conn()
    row = conn.execute("SELECT settings_json FROM user_settings WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    if not row:
        return {}
    try:
        return json.loads(row["settings_json"])
    except json.JSONDecodeError:
        return {}


def parse_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).lower() in {"on", "true", "1", "yes"}


def load_persistent_config() -> dict[str, str]:
    return load_cfg_values(CONFIG_PATH, CONFIG_SECTION, CFG_FIELD_MAP, *CFG_FIELDS)


def build_settings_defaults(module_value: str, config_values: dict[str, str]) -> dict[str, Any]:
    settings: dict[str, Any] = {
        "module": module_value,
        "n77_ssb_pre": config_values.get("n77_ssb_pre", ""),
        "n77_ssb_post": config_values.get("n77_ssb_post", ""),
        "n77b_ssb": config_values.get("n77b_ssb", ""),
        "allowed_n77_ssb_pre": config_values.get("allowed_n77_ssb_pre", ""),
        "allowed_n77_arfcn_pre": config_values.get("allowed_n77_arfcn_pre", ""),
        "allowed_n77_ssb_post": config_values.get("allowed_n77_ssb_post", ""),
        "allowed_n77_arfcn_post": config_values.get("allowed_n77_arfcn_post", ""),
        "ca_freq_filters": config_values.get("ca_freq_filters", ""),
        "cc_freq_filters": config_values.get("cc_freq_filters", ""),
        "profiles_audit": parse_bool(config_values.get("profiles_audit")),
        "frequency_audit": parse_bool(config_values.get("frequency_audit")),
        "export_correction_cmd": parse_bool(config_values.get("export_correction_cmd")),
        "fast_excel_export": parse_bool(config_values.get("fast_excel_export")),
    }

    if module_value == "consistency-check":
        settings["input_pre"] = config_values.get("last_input_cc_pre", "")
        settings["input_post"] = config_values.get("last_input_cc_post", "")
        settings["input"] = ""
        return settings

    if module_value == "consistency-check-bulk":
        settings["input"] = config_values.get("last_input_cc_bulk", "") or config_values.get("last_input", "")
    elif module_value == "final-cleanup":
        settings["input"] = config_values.get("last_input_final_cleanup", "") or config_values.get("last_input", "")
    else:
        settings["input"] = config_values.get("last_input_audit", "") or config_values.get("last_input", "")

    settings["input_pre"] = ""
    settings["input_post"] = ""
    return settings


def persist_settings_to_config(module_value: str, payload: dict[str, Any]) -> None:
    persist_kwargs: dict[str, str] = {
        "n77_ssb_pre": payload.get("n77_ssb_pre", ""),
        "n77_ssb_post": payload.get("n77_ssb_post", ""),
        "n77b_ssb": payload.get("n77b_ssb", ""),
        "ca_freq_filters": payload.get("ca_freq_filters", ""),
        "cc_freq_filters": payload.get("cc_freq_filters", ""),
        "allowed_n77_ssb_pre": payload.get("allowed_n77_ssb_pre", ""),
        "allowed_n77_arfcn_pre": payload.get("allowed_n77_arfcn_pre", ""),
        "allowed_n77_ssb_post": payload.get("allowed_n77_ssb_post", ""),
        "allowed_n77_arfcn_post": payload.get("allowed_n77_arfcn_post", ""),
        "profiles_audit": "1" if parse_bool(payload.get("profiles_audit")) else "0",
        "frequency_audit": "1" if parse_bool(payload.get("frequency_audit")) else "0",
        "export_correction_cmd": "1" if parse_bool(payload.get("export_correction_cmd")) else "0",
        "fast_excel_export": "1" if parse_bool(payload.get("fast_excel_export")) else "0",
        "network_frequencies": payload.get("network_frequencies", ""),
    }

    if module_value == "consistency-check":
        persist_kwargs["last_input_cc_pre"] = payload.get("input_pre", "")
        persist_kwargs["last_input_cc_post"] = payload.get("input_post", "")
    else:
        input_dir = payload.get("input", "")
        if module_value == "consistency-check-bulk":
            persist_kwargs["last_input_cc_bulk"] = input_dir
            persist_kwargs["last_input"] = input_dir
        elif module_value == "final-cleanup":
            persist_kwargs["last_input_final_cleanup"] = input_dir
            persist_kwargs["last_input"] = input_dir
        else:
            persist_kwargs["last_input_audit"] = input_dir
            persist_kwargs["last_input"] = input_dir

    save_cfg_values(
        config_dir=CONFIG_DIR,
        config_path=CONFIG_PATH,
        config_section=CONFIG_SECTION,
        cfg_field_map=CFG_FIELD_MAP,
        **persist_kwargs,
    )


def build_cli_command(payload: dict[str, Any]) -> list[str]:
    # Build the CLI command executed by the backend worker.
    cmd = [sys.executable, "src/SSB_RetuningAutomations.py", "--no-gui", "--module", payload["module"]]

    if payload.get("input"):
        cmd.extend(["--input", payload["input"]])
    if payload.get("input_pre"):
        cmd.extend(["--input-pre", payload["input_pre"]])
    if payload.get("input_post"):
        cmd.extend(["--input-post", payload["input_post"]])

    if payload.get("n77_ssb_pre"):
        cmd.extend(["--n77-ssb-pre", payload["n77_ssb_pre"]])
    if payload.get("n77_ssb_post"):
        cmd.extend(["--n77-ssb-post", payload["n77_ssb_post"]])
    if payload.get("n77b_ssb"):
        cmd.extend(["--n77b-ssb", payload["n77b_ssb"]])

    if payload.get("allowed_n77_ssb_pre"):
        cmd.extend(["--allowed-n77-ssb-pre", payload["allowed_n77_ssb_pre"]])
    if payload.get("allowed_n77_arfcn_pre"):
        cmd.extend(["--allowed-n77-arfcn-pre", payload["allowed_n77_arfcn_pre"]])
    if payload.get("allowed_n77_ssb_post"):
        cmd.extend(["--allowed-n77-ssb-post", payload["allowed_n77_ssb_post"]])
    if payload.get("allowed_n77_arfcn_post"):
        cmd.extend(["--allowed-n77-arfcn-post", payload["allowed_n77_arfcn_post"]])

    if payload.get("ca_freq_filters"):
        cmd.extend(["--ca-freq-filters", payload["ca_freq_filters"]])
    if payload.get("cc_freq_filters"):
        cmd.extend(["--cc-freq-filters", payload["cc_freq_filters"]])

    if parse_bool(payload.get("profiles_audit")):
        cmd.append("--profiles-audit")
    else:
        cmd.append("--no-profiles-audit")
    if parse_bool(payload.get("frequency_audit")):
        cmd.append("--frequency-audit")
    else:
        cmd.append("--no-frequency-audit")
    if parse_bool(payload.get("export_correction_cmd")):
        cmd.append("--export-correction-cmd")
    else:
        cmd.append("--no-export-correction-cmd")
    if parse_bool(payload.get("fast_excel_export")):
        cmd.append("--fast-excel-export")

    return cmd


app = FastAPI(title="SSB Retuning Automations Web Frontend")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.middleware("http")
async def access_log_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    client = request.client.host if request.client else "unknown"
    access_logger.info(
        '%s "%s %s" status=%s duration_ms=%.2f',
        client,
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    user = get_current_user(request)
    if user is None:
        return RedirectResponse("/login", status_code=302)

    config_values = load_persistent_config()
    user_settings = load_user_settings(user["id"])
    module_value = user_settings.get("module") or "configuration-audit"
    settings = build_settings_defaults(module_value, config_values)
    tool_meta = load_tool_metadata()
    network_frequencies = load_network_frequencies()
    settings.update(user_settings)
    settings["module"] = module_value
    conn = get_conn()
    latest_runs = conn.execute(
        """
        SELECT id, module, tool_version, status, started_at, finished_at, duration_seconds, output_zip, output_log_file
        FROM task_runs
        WHERE user_id = ?
        ORDER BY id DESC
        
        """,
        (user["id"],),
    ).fetchall()
    latest_runs = [dict(row) for row in latest_runs]
    for row in latest_runs:
        row["started_at"] = format_timestamp(row["started_at"])
        row["finished_at"] = format_timestamp(row["finished_at"])
        row["duration_hms"] = format_seconds_hms(row.get("duration_seconds"))
        row["status_lower"] = (row.get("status") or "").strip().lower()

    all_runs = conn.execute(
        "SELECT id, input_dir, output_dir FROM task_runs WHERE user_id = ?",
        (user["id"],),
    ).fetchall()
    conn.close()

    run_sizes: dict[int, int] = {}
    total_bytes = 0
    for row in all_runs:
        input_dir = Path(row["input_dir"]) if row["input_dir"] else None
        output_dir = Path(row["output_dir"]) if row["output_dir"] else None
        input_size = compute_dir_size(input_dir) if input_dir else 0
        output_size = 0
        if output_dir and not (input_dir and is_safe_path(input_dir, output_dir)):
            output_size = compute_dir_size(output_dir)
        size_bytes = input_size + output_size
        run_sizes[row["id"]] = size_bytes
        total_bytes += size_bytes

    for row in latest_runs:
        size_bytes = run_sizes.get(row["id"], 0)
        row["size_mb"] = format_mb(size_bytes)

    total_size_mb = format_mb(total_bytes)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": user,
            "module_options": MODULE_OPTIONS,
            "settings": settings,
            "latest_runs": latest_runs,
            "tool_meta": tool_meta,
            "network_frequencies": network_frequencies,
            "total_runs_size": total_size_mb,
        },
    )


@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@app.post("/login", response_class=HTMLResponse)
def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    conn = get_conn()
    user = conn.execute(
        "SELECT * FROM users WHERE username = ? AND active = 1", (username.strip(),)
    ).fetchone()
    try:
        verified = user and pwd_context.verify(password, user["password_hash"])
    except (ValueError, TypeError):
        verified = False
    if not verified:
        conn.close()
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid credentials or disabled user."},
            status_code=401,
        )

    token = secrets.token_urlsafe(32)
    conn.execute(
        "INSERT INTO sessions(token, user_id, created_at, last_seen_at, active) VALUES (?, ?, ?, ?, 1)",
        (token, user["id"], now_iso(), now_iso()),
    )
    conn.commit()
    conn.close()

    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie("session_token", token, httponly=True, samesite="lax")
    return response


@app.get("/logout")
def logout(request: Request):
    token = request.cookies.get("session_token")
    if token:
        conn = get_conn()
        conn.execute("UPDATE sessions SET active = 0 WHERE token = ?", (token,))
        conn.commit()
        conn.close()
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("session_token")
    return response


@app.post("/run")
def run_module(
    request: Request,
    module: str = Form(...),
    input: str = Form(""),
    input_pre: str = Form(""),
    input_post: str = Form(""),
    n77_ssb_pre: str = Form(""),
    n77_ssb_post: str = Form(""),
    n77b_ssb: str = Form(""),
    allowed_n77_ssb_pre: str = Form(""),
    allowed_n77_arfcn_pre: str = Form(""),
    allowed_n77_ssb_post: str = Form(""),
    allowed_n77_arfcn_post: str = Form(""),
    ca_freq_filters: str = Form(""),
    cc_freq_filters: str = Form(""),
    network_frequencies: str = Form(""),
    profiles_audit: str | None = Form(None),
    frequency_audit: str | None = Form(None),
    export_correction_cmd: str | None = Form(None),
    fast_excel_export: str | None = Form(None),
):
    try:
        user = require_user(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    payload = {
        "module": module,
        "input": input.strip(),
        "input_pre": input_pre.strip(),
        "input_post": input_post.strip(),
        "n77_ssb_pre": n77_ssb_pre.strip(),
        "n77_ssb_post": n77_ssb_post.strip(),
        "n77b_ssb": n77b_ssb.strip(),
        "allowed_n77_ssb_pre": allowed_n77_ssb_pre.strip(),
        "allowed_n77_arfcn_pre": allowed_n77_arfcn_pre.strip(),
        "allowed_n77_ssb_post": allowed_n77_ssb_post.strip(),
        "allowed_n77_arfcn_post": allowed_n77_arfcn_post.strip(),
        "ca_freq_filters": ca_freq_filters.strip(),
        "cc_freq_filters": cc_freq_filters.strip(),
        "network_frequencies": network_frequencies.strip(),
        "profiles_audit": profiles_audit,
        "frequency_audit": frequency_audit,
        "export_correction_cmd": export_correction_cmd,
        "fast_excel_export": fast_excel_export,
    }
    save_user_settings(user["id"], payload)
    persist_settings_to_config(module, payload)

    cmd = build_cli_command(payload)

    start = time.perf_counter()
    started_at = now_iso()
    status = "ok"
    output_log = ""

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "TERM": "xterm", "SSB_RA_NO_CLEAR": "1"},
        )
        if proc.returncode != 0:
            status = "error"
        output_log = (proc.stdout or "") + "\n" + (proc.stderr or "")
    except Exception as exc:
        status = "error"
        output_log = f"Execution error: {exc}"

    output_log = strip_ansi(output_log)
    duration = time.perf_counter() - start
    finished_at = now_iso()

    input_dir_value = ""
    output_dir_value = ""
    output_zip_value = ""
    output_log_file_value = ""
    base_dir = ""
    if module == "consistency-check":
        base_dir = payload.get("input_post", "")
        input_dir_value = base_dir
    else:
        base_dir = payload.get("input", "")
        input_dir_value = base_dir

    if base_dir:
        output_dir = find_latest_output_dir(Path(base_dir), build_output_prefixes(module))
        if output_dir:
            output_dir_value = str(output_dir)
            try:
                for existing in output_dir.glob("RetuningAutomation_*.log"):
                    existing.unlink(missing_ok=True)
                legacy_log = output_dir / "webapp_output.log"
                if legacy_log.exists():
                    legacy_log.unlink()
                tool_meta = load_tool_metadata()
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                version = tool_meta.get("version", "unknown")
                log_name = f"RetuningAutomation_{timestamp}_v{version}.log"
                output_log_file = output_dir / log_name
                output_log_file.write_text(output_log, encoding="utf-8")
                output_log_file_value = str(output_log_file)
            except OSError:
                output_log_file_value = ""
            exports_dir = DATA_DIR / "users" / sanitize_component(user["username"]) / "export"
            exports_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            tool_meta = load_tool_metadata()
            tool_version = tool_meta.get("version", "unknown")
            zip_name = f"{sanitize_component(module)}_{timestamp}_v{sanitize_component(tool_version)}.zip"
            output_zip_path = exports_dir / zip_name
            try:
                create_zip_from_dir(output_dir, output_zip_path)
                output_zip_value = str(output_zip_path)
            except OSError:
                output_zip_value = ""

    conn = get_conn()
    conn.execute(
        """
        INSERT INTO task_runs(user_id, module, tool_version, status, started_at, finished_at, duration_seconds, command, output_log, input_dir, output_dir, output_zip, output_log_file)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user["id"],
            module,
            tool_version,
            status,
            started_at,
            finished_at,
            duration,
            " ".join(cmd),
            output_log[:20000],
            input_dir_value,
            output_dir_value,
            output_zip_value,
            output_log_file_value,
        ),
    )
    conn.commit()
    conn.close()

    return RedirectResponse("/", status_code=302)


@app.post("/settings/update")
async def update_settings(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    save_user_settings(user["id"], payload)
    module_value = payload.get("module") or "configuration-audit"
    persist_settings_to_config(module_value, payload)
    return {"ok": True}


@app.post("/account/change_password")
async def change_password(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    new_password = (payload.get("new_password") or "").strip()
    if not new_password:
        return {"ok": False}

    conn = get_conn()
    conn.execute("UPDATE users SET password_hash = ? WHERE id = ?", (pwd_context.hash(new_password), user["id"]))
    conn.execute("UPDATE sessions SET active = 0 WHERE user_id = ?", (user["id"],))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/config/load")
def load_config():
    return load_persistent_config()


@app.get("/config/export")
def export_config():
    if CONFIG_PATH.exists():
        return FileResponse(CONFIG_PATH, filename="config.cfg")
    return PlainTextResponse("", media_type="text/plain")


@app.post("/uploads/zip")
async def upload_zip(
    request: Request,
    module: str = Form(...),
    kind: str = Form(...),
    session_id: str = Form(""),
    files: list[UploadFile] = File(...),
):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False, "error": "auth"}

    if not files:
        return {"ok": False, "error": "invalid_file"}

    tool_meta = load_tool_metadata()
    timestamp = session_id.strip() or datetime.now().strftime("%Y%m%d_%H%M%S")
    version = tool_meta.get("version", "unknown")
    user_root = DATA_DIR / "users" / sanitize_component(user["username"]) / "upload"
    run_root = user_root / f"{sanitize_component(module)}_{sanitize_component(timestamp)}_v{sanitize_component(version)}"
    target_dir = run_root / sanitize_component(kind)
    target_dir.mkdir(parents=True, exist_ok=True)
    for upload in files:
        filename = upload.filename or ""
        lower_name = filename.lower()
        if not (lower_name.endswith(".zip") or lower_name.endswith(".log") or lower_name.endswith(".logs") or lower_name.endswith(".txt")):
            continue

        if lower_name.endswith(".zip"):
            zip_path = run_root / sanitize_component(filename)
            with zip_path.open("wb") as buffer:
                while True:
                    chunk = await upload.read(1024 * 1024)
                    if not chunk:
                        break
                    buffer.write(chunk)
            try:
                safe_extract_zip(zip_path, target_dir)
                remove_output_folders(target_dir)
            finally:
                if zip_path.exists():
                    zip_path.unlink()
        else:
            dest = target_dir / sanitize_component(filename)
            with dest.open("wb") as buffer:
                while True:
                    chunk = await upload.read(1024 * 1024)
                    if not chunk:
                        break
                    buffer.write(chunk)

    return {"ok": True, "path": str(target_dir)}


@app.get("/logs/latest")
def latest_logs(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"log": ""}

    conn = get_conn()
    row = conn.execute(
        "SELECT output_log FROM task_runs WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (user["id"],),
    ).fetchone()
    conn.close()
    return {"log": row["output_log"] if row else ""}


@app.get("/runs/{run_id}/download")
def download_run_output(request: Request, run_id: int):
    try:
        user = require_user(request)
    except PermissionError:
        return PlainTextResponse("", status_code=403)

    conn = get_conn()
    row = conn.execute(
        "SELECT output_zip FROM task_runs WHERE id = ? AND user_id = ?",
        (run_id, user["id"]),
    ).fetchone()
    conn.close()

    if not row or not row["output_zip"]:
        return PlainTextResponse("", status_code=404)

    output_zip = Path(row["output_zip"])
    if not output_zip.exists():
        return PlainTextResponse("", status_code=404)

    return FileResponse(output_zip, filename=output_zip.name)


@app.get("/runs/{run_id}/log")
def download_run_log(request: Request, run_id: int):
    try:
        user = require_user(request)
    except PermissionError:
        return PlainTextResponse("", status_code=403)

    conn = get_conn()
    row = conn.execute(
        "SELECT output_log_file FROM task_runs WHERE id = ? AND user_id = ?",
        (run_id, user["id"]),
    ).fetchone()
    conn.close()

    if not row or not row["output_log_file"]:
        return PlainTextResponse("", status_code=404)

    log_path = Path(row["output_log_file"])
    if not log_path.exists():
        return PlainTextResponse("", status_code=404)

    return FileResponse(log_path, filename=log_path.name)


@app.post("/runs/delete")
async def delete_runs(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    run_ids = payload.get("ids", [])
    if not run_ids:
        return {"ok": False}

    conn = get_conn()
    rows = conn.execute(
        "SELECT id, input_dir, output_dir, output_zip, output_log_file FROM task_runs WHERE user_id = ? AND id IN (%s)"
        % ",".join("?" for _ in run_ids),
        (user["id"], *run_ids),
    ).fetchall()

    for row in rows:
        input_dir = Path(row["input_dir"]) if row["input_dir"] else None
        output_dir = Path(row["output_dir"]) if row["output_dir"] else None
        output_zip = Path(row["output_zip"]) if row["output_zip"] else None
        output_log = Path(row["output_log_file"]) if row["output_log_file"] else None

        if input_dir and is_safe_path(DATA_DIR / "users", input_dir):
            shutil.rmtree(input_dir, ignore_errors=True)
        if output_dir and is_safe_path(DATA_DIR / "users", output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        if output_zip and is_safe_path(DATA_DIR / "users", output_zip):
            output_zip.unlink(missing_ok=True)
        if output_log and is_safe_path(DATA_DIR / "users", output_log):
            output_log.unlink(missing_ok=True)

    conn.execute(
        "DELETE FROM task_runs WHERE user_id = ? AND id IN (%s)" % ",".join("?" for _ in run_ids),
        (user["id"], *run_ids),
    )
    conn.commit()
    conn.close()

    return {"ok": True}


@app.get("/admin", response_class=HTMLResponse)
def admin_panel(request: Request):
    try:
        admin = require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    conn = get_conn()
    users = conn.execute(
        """
        SELECT u.id, u.username, u.role, u.active, u.created_at,
               COALESCE(SUM(CASE WHEN s.active = 1 THEN (julianday('now') - julianday(s.created_at))*86400 ELSE (julianday(s.last_seen_at) - julianday(s.created_at))*86400 END), 0) AS total_login_seconds,
               COALESCE((SELECT SUM(duration_seconds) FROM task_runs tr WHERE tr.user_id = u.id), 0) AS total_execution_seconds
        FROM users u
        LEFT JOIN sessions s ON s.user_id = u.id
        GROUP BY u.id
        ORDER BY u.username
        """
    ).fetchall()
    users = [dict(row) for row in users]
    for row in users:
        dirs = get_user_storage_dirs(row["username"])
        uploads_size = compute_dir_size(dirs["uploads"])
        exports_size = compute_dir_size(dirs["exports"])
        row["storage_size"] = format_mb(uploads_size + exports_size)
        row["total_login_hms"] = format_seconds_hms(row.get("total_login_seconds"))
        row["total_execution_hms"] = format_seconds_hms(row.get("total_execution_seconds"))
    recent_runs = conn.execute(
        """
        SELECT tr.id, u.username, tr.module, tr.tool_version, tr.status, tr.started_at, tr.finished_at, tr.duration_seconds, tr.output_zip, tr.output_log_file, tr.input_dir, tr.output_dir
        FROM task_runs tr
        JOIN users u ON u.id = tr.user_id
        ORDER BY tr.id DESC
        LIMIT 50
        """
    ).fetchall()
    conn.close()

    recent_runs = [dict(row) for row in recent_runs]
    for row in recent_runs:
        row["started_at"] = format_timestamp(row["started_at"])
        row["finished_at"] = format_timestamp(row["finished_at"])
        input_dir = Path(row["input_dir"]) if row["input_dir"] else None
        output_dir = Path(row["output_dir"]) if row["output_dir"] else None
        input_size = compute_dir_size(input_dir) if input_dir else 0
        output_size = 0
        if output_dir and not (input_dir and is_safe_path(input_dir, output_dir)):
            output_size = compute_dir_size(output_dir)
        row["size_mb"] = format_mb(input_size + output_size)

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "user": admin,
            "users": users,
            "recent_runs": recent_runs,
        },
    )


@app.post("/admin/users/create")
def admin_create_user(request: Request, username: str = Form(...), password: str = Form(...), role: str = Form("user")):
    try:
        require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO users(username, password_hash, role, active, created_at) VALUES (?, ?, ?, 1, ?)",
            (username.strip(), pwd_context.hash(password), "admin" if role == "admin" else "user", now_iso()),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()
    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/users/{user_id}/toggle")
def admin_toggle_user(request: Request, user_id: int):
    try:
        admin = require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    conn = get_conn()
    target = conn.execute("SELECT id, active, username FROM users WHERE id = ?", (user_id,)).fetchone()
    if target and target["username"] != admin["username"]:
        next_state = 0 if target["active"] == 1 else 1
        conn.execute("UPDATE users SET active = ? WHERE id = ?", (next_state, user_id))
        if next_state == 0:
            conn.execute("UPDATE sessions SET active = 0 WHERE user_id = ?", (user_id,))
        conn.commit()
    conn.close()
    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/users/{user_id}/set_password")
def admin_set_password(request: Request, user_id: int, new_password: str = Form(...)):
    try:
        require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    conn = get_conn()
    conn.execute("UPDATE users SET password_hash = ? WHERE id = ?", (pwd_context.hash(new_password), user_id))
    conn.execute("UPDATE sessions SET active = 0 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/users/{user_id}/update")
def admin_update_user(
    request: Request,
    user_id: int,
    username: str = Form(...),
    role: str = Form("user"),
    active: str = Form("1"),
    new_password: str = Form(""),
):
    try:
        require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    new_username = username.strip()
    new_role = "admin" if role == "admin" else "user"
    new_active = 1 if active == "1" else 0

    conn = get_conn()
    current = conn.execute("SELECT username FROM users WHERE id = ?", (user_id,)).fetchone()
    if not current:
        conn.close()
        return RedirectResponse("/admin", status_code=302)

    old_username = current["username"]
    try:
        if old_username == new_username and new_role == "admin" and new_active == 0:
            new_active = 1
        conn.execute(
            "UPDATE users SET username = ?, role = ?, active = ? WHERE id = ?",
            (new_username, new_role, new_active, user_id),
        )
        if new_password.strip():
            conn.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (pwd_context.hash(new_password), user_id),
            )
            conn.execute("UPDATE sessions SET active = 0 WHERE user_id = ?", (user_id,))
        if new_active == 0:
            conn.execute("UPDATE sessions SET active = 0 WHERE user_id = ?", (user_id,))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return RedirectResponse("/admin", status_code=302)
    finally:
        conn.close()

    if old_username != new_username:
        old_dirs = get_user_storage_dirs(old_username)
        new_dirs = get_user_storage_dirs(new_username)
        for key in ("uploads", "exports"):
            if old_dirs[key].exists():
                new_dirs[key].parent.mkdir(parents=True, exist_ok=True)
                old_dirs[key].rename(new_dirs[key])

    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/users/{user_id}/clear_storage")
def admin_clear_storage(request: Request, user_id: int):
    try:
        require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    conn = get_conn()
    user_row = conn.execute("SELECT username FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user_row:
        conn.close()
        return RedirectResponse("/admin", status_code=302)

    rows = conn.execute(
        "SELECT input_dir, output_dir, output_zip, output_log_file FROM task_runs WHERE user_id = ?",
        (user_id,),
    ).fetchall()

    for row in rows:
        input_dir = Path(row["input_dir"]) if row["input_dir"] else None
        output_dir = Path(row["output_dir"]) if row["output_dir"] else None
        output_zip = Path(row["output_zip"]) if row["output_zip"] else None
        output_log = Path(row["output_log_file"]) if row["output_log_file"] else None

        if input_dir and is_safe_path(DATA_DIR / "users", input_dir):
            shutil.rmtree(input_dir, ignore_errors=True)
        if output_dir and is_safe_path(DATA_DIR / "users", output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        if output_zip and is_safe_path(DATA_DIR / "users", output_zip):
            output_zip.unlink(missing_ok=True)
        if output_log and is_safe_path(DATA_DIR / "users", output_log):
            output_log.unlink(missing_ok=True)

    conn.execute("DELETE FROM task_runs WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

    dirs = get_user_storage_dirs(user_row["username"])
    shutil.rmtree(dirs["uploads"], ignore_errors=True)
    shutil.rmtree(dirs["exports"], ignore_errors=True)

    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/users/{user_id}/delete")
def admin_delete_user(request: Request, user_id: int):
    try:
        admin = require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    if admin["id"] == user_id:
        return RedirectResponse("/admin", status_code=302)

    conn = get_conn()
    user_row = conn.execute("SELECT username FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user_row:
        conn.close()
        return RedirectResponse("/admin", status_code=302)

    conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM task_runs WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    dirs = get_user_storage_dirs(user_row["username"])
    shutil.rmtree(dirs["uploads"], ignore_errors=True)
    shutil.rmtree(dirs["exports"], ignore_errors=True)

    return RedirectResponse("/admin", status_code=302)
