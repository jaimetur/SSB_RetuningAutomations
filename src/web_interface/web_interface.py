from __future__ import annotations

import json
import logging
import os
import re
import shutil
import zipfile
from logging.handlers import RotatingFileHandler
import secrets
import shlex
import sqlite3
import subprocess
import sys
import threading
import time
import tempfile
import importlib

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, Request, UploadFile, Depends
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext

from src.utils.utils_io import load_cfg_values, save_cfg_values

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATA_DB_DIR = DATA_DIR / "db"
DATA_DB_DIR.mkdir(parents=True, exist_ok=True)
SYSTEM_LOGS_DIR = DATA_DIR / "system_logs"
SYSTEM_LOGS_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DB_DIR / "web_interface.db"
LEGACY_DB_PATHS = [
    DATA_DIR / "web_interface.db",
    DATA_DIR / "web_frontend.db",  # legacy filename from previous Web Interface naming
]
API_LOG_PATH = SYSTEM_LOGS_DIR / "web-api.log"
WEB_ACCESS_LOG_PATH = SYSTEM_LOGS_DIR / "web-access.log"
APP_LOG_PATH = SYSTEM_LOGS_DIR / "web-interface.log"
LEGACY_ACCESS_LOG_PATHS = [DATA_DIR / "web-api.log", DATA_DIR / "access.log"]
LEGACY_APP_LOG_PATHS = [DATA_DIR / "web-interface.log", DATA_DIR / "app.log"]
HELP_DIR = PROJECT_ROOT / "help"

LOG_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}

if not DB_PATH.exists():
    for legacy_db_path in LEGACY_DB_PATHS:
        if legacy_db_path.exists():
            try:
                legacy_db_path.rename(DB_PATH)
                break
            except OSError:
                continue

for legacy_log_path in LEGACY_ACCESS_LOG_PATHS:
    if not API_LOG_PATH.exists() and legacy_log_path.exists():
        try:
            legacy_log_path.rename(API_LOG_PATH)
            break
        except OSError:
            continue

for legacy_log_path in LEGACY_APP_LOG_PATHS:
    if not APP_LOG_PATH.exists() and legacy_log_path.exists():
        try:
            legacy_log_path.rename(APP_LOG_PATH)
            break
        except OSError:
            continue

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

USER_SETTINGS_ALLOWED_KEYS = {
    "module",
    "input",
    "input_pre",
    "input_post",
    "profiles_audit",
    "frequency_audit",
    "export_correction_cmd",
    "fast_excel_export",
    "output",
    "module_inputs_map",
    "ui_panels",
    "user_system_log_source",
    "runs_user_filter",
    "inputs_user_filter",
    "wildcard_history_inputs",
    "wildcard_history_executions",
    "admin_system_log_source",
    "admin_users_filter",
    "admin_inputs_filter",
    "admin_runs_filter",
    "admin_users_sort",
}

USER_SETTINGS_OBSOLETE_KEYS = {
    "max_cpu_percent",
    "max_memory_percent",
    "max_parallel_tasks",
    "db_backup_auto_mode",
    "db_backup_auto_enabled",
    "db_backup_auto_path",
    "db_backup_auto_hour",
    "db_backup_max_to_store",
    "db_backup_last_run_date",
    "n77_ssb_pre",
    "n77_ssb_post",
    "n77b_ssb",
    "allowed_n77_ssb_pre",
    "allowed_n77_arfcn_pre",
    "allowed_n77_ssb_post",
    "allowed_n77_arfcn_post",
    "ca_freq_filters",
    "cc_freq_filters",
    "network_frequencies",
}

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("web_interface")
    logger.setLevel(logging.INFO)

    # Persist application logs across restarts and avoid duplicated handlers.
    app_handler = next(
        (
            handler
            for handler in logger.handlers
            if isinstance(handler, RotatingFileHandler)
            and getattr(handler, "baseFilename", "") == str(APP_LOG_PATH)
        ),
        None,
    )
    if app_handler is None:
        app_handler = RotatingFileHandler(APP_LOG_PATH, maxBytes=2_000_000, backupCount=3)
        app_handler.setFormatter(logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(message)s", datefmt=LOG_DATETIME_FORMAT))
        logger.addHandler(app_handler)

    # Uvicorn emits most lifecycle errors through `uvicorn.error`.
    # Attach our file handler once there and disable propagation to avoid duplicates.
    uv_error_logger = logging.getLogger("uvicorn.error")
    uv_error_logger.setLevel(logging.INFO)
    if all(handler is not app_handler for handler in uv_error_logger.handlers):
        uv_error_logger.addHandler(app_handler)
    uv_error_logger.propagate = False

    # Ensure parent uvicorn logger does not also write the same events to the same file.
    uv_logger = logging.getLogger("uvicorn")
    uv_logger.setLevel(logging.INFO)
    uv_logger.propagate = False
    uv_logger.handlers = [
        handler
        for handler in uv_logger.handlers
        if not (
            isinstance(handler, RotatingFileHandler)
            and getattr(handler, "baseFilename", "") == str(APP_LOG_PATH)
        )
    ]

    return logger


def setup_api_logger() -> logging.Logger:
    logger = logging.getLogger("web_api")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        # Keep API request logs separate from application logs.
        api_handler = RotatingFileHandler(API_LOG_PATH, maxBytes=2_000_000, backupCount=3)
        api_handler.setFormatter(logging.Formatter("[%(asctime)s] - %(message)s", datefmt=LOG_DATETIME_FORMAT))
        logger.addHandler(api_handler)
    return logger


def setup_web_access_logger() -> logging.Logger:
    logger = logging.getLogger("web_access")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        # Keep web access/authentication logs separate from API call logs.
        access_handler = RotatingFileHandler(WEB_ACCESS_LOG_PATH, maxBytes=2_000_000, backupCount=3)
        access_handler.setFormatter(logging.Formatter("[%(asctime)s] - %(message)s", datefmt=LOG_DATETIME_FORMAT))
        logger.addHandler(access_handler)
    return logger


logger = setup_logging()
api_logger = setup_api_logger()
web_access_logger = setup_web_access_logger()


MODULE_OPTIONS = [
    ("update-network-frequencies", "0. Update Network Frequencies"),
    ("configuration-audit", "1. Configuration Audit & Logs Parser"),
    ("consistency-check", "2. Consistency Check (Pre/Post Comparison)"),
    ("consistency-check-bulk", "3. Consistency Check (Bulk mode Pre/Post auto-detection)"),
    ("final-cleanup", "4. Final Clean-Up"),
]
WEB_INTERFACE_BLOCKED_MODULES = {"consistency-check-bulk"}
SESSION_IDLE_TIMEOUT_SECONDS = 600

TOOL_METADATA_PATH = PROJECT_ROOT / "src" / "SSB_RetuningAutomations.py"

INPUTS_REPOSITORY_DIR = DATA_DIR / "inputs"
INPUTS_REPOSITORY_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR = DATA_DIR / "outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

MAX_CPU_DEFAULT = 80
MAX_MEMORY_DEFAULT = 80
MAX_PARALLEL_DEFAULT = 1
SQLITE_BUSY_TIMEOUT_MS = 30000
SQLITE_CONNECT_TIMEOUT_S = 30

queue_event = threading.Event()
worker_started = False
worker_lock = threading.Lock()
backup_worker_started = False
backup_worker_lock = threading.Lock()
running_processes: dict[int, subprocess.Popen[str]] = {}
running_processes_lock = threading.Lock()
canceled_task_ids: set[int] = set()
canceled_task_ids_lock = threading.Lock()


def sync_running_process_registry() -> None:
    """Keep only alive processes in the running registry (no CPU/memory rebalancing)."""
    with running_processes_lock:
        active_processes = {
            task_id: proc
            for task_id, proc in running_processes.items()
            if proc.poll() is None and proc.pid is not None
        }
        running_processes.clear()
        running_processes.update(active_processes)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_CONNECT_TIMEOUT_S)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def recover_incomplete_tasks() -> None:
    """Re-queue tasks left in non-terminal states after an unexpected restart."""
    conn = get_conn()
    pending_count = conn.execute(
        "SELECT COUNT(*) AS total FROM task_runs WHERE status IN ('running', 'canceling')"
    ).fetchone()["total"]
    if pending_count:
        conn.execute(
            """
            UPDATE task_runs
            SET status = 'queued',
                finished_at = NULL,
                duration_seconds = NULL,
                output_log = TRIM(COALESCE(output_log, '') || '\nRecovered after service restart. Task re-queued automatically.')
            WHERE status IN ('running', 'canceling')
            """
        )
        conn.commit()
        logger.warning("Recovered %s interrupted task(s) after startup and moved them back to queue.", pending_count)
    conn.close()


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


def format_last_connection(value: str | None) -> str:
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo:
            parsed = parsed.astimezone()
        return parsed.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return value


def strip_ansi(text: str) -> str:
    if not text:
        return text
    return re.sub(r"\x1b\\[[0-9;]*[A-Za-z]", "", text)


def read_tail(path: Path, max_lines: int = 400) -> str:
    if not path.exists():
        return ""
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    lines = content.splitlines()
    if len(lines) <= max_lines:
        return "\n".join(lines)
    return "\n".join(lines[-max_lines:])


def sanitize_component(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "_", value.strip())
    return cleaned or "unknown"


def infer_parent_folder_name(files: list[UploadFile]) -> str:
    for upload in files:
        filename = (upload.filename or "").strip().replace("\\", "/")
        parts = [part for part in filename.split("/") if part and part not in {".", ".."}]
        if len(parts) > 1:
            return sanitize_component(parts[0])
    first_name = (files[0].filename or "uploaded_input").strip().replace("\\", "/") if files else "uploaded_input"
    return sanitize_component(Path(first_name).stem)


def execution_output_has_error(output_log: str) -> bool:
    if not output_log:
        return False
    lowered = output_log.lower()
    if "[error]" in lowered:
        return True
    error_markers = (
        "input folder does not exist or is not a directory",
        "traceback (most recent call last)",
        "execution error:",
    )
    return any(marker in lowered for marker in error_markers)


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
                if file_path.resolve() == dest_zip.resolve():
                    continue
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
    try:
        iterator = path.rglob("*")
    except OSError:
        return 0
    for file_path in iterator:
        try:
            if file_path.is_file():
                total += file_path.stat().st_size
        except OSError:
            continue
    return total


def compute_path_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    return compute_dir_size(path)


def cleanup_stale_runs_for_user(user_id: int) -> None:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT id, status, output_dir, output_zip, output_log_file
        FROM task_runs
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchall()

    stale_ids: list[int] = []
    for row in rows:
        status = (row["status"] or "").strip().lower()
        if status not in {"ok", "error"}:
            continue
        tracked_paths = [row["output_dir"], row["output_zip"], row["output_log_file"]]
        existing = False
        has_reference = any(tracked_paths)
        for raw_path in tracked_paths:
            if not raw_path:
                continue
            if Path(raw_path).exists():
                existing = True
                break
        if has_reference and not existing:
            stale_ids.append(int(row["id"]))

    if stale_ids:
        conn.execute(
            "DELETE FROM task_runs WHERE user_id = ? AND id IN (%s)" % ",".join("?" for _ in stale_ids),
            (user_id, *stale_ids),
        )
        conn.commit()
    conn.close()


def compute_runs_size(run_rows: list[sqlite3.Row] | list[dict[str, Any]]) -> tuple[dict[int, int], int]:
    run_sizes: dict[int, int] = {}
    total_bytes = 0

    def read_value(row: sqlite3.Row | dict[str, Any], key: str) -> Any:
        if isinstance(row, sqlite3.Row):
            return row[key] if key in row.keys() else None
        return row.get(key)

    for row in run_rows:
        row_id = int(read_value(row, "id") or 0)
        status_value = (read_value(row, "status") or "").strip().lower()
        if status_value not in {"ok", "error", "canceled"}:
            run_sizes[row_id] = 0
            continue

        output_zip_value = read_value(row, "output_zip")
        output_zip = Path(output_zip_value) if output_zip_value else None
        size_bytes = compute_path_size(output_zip) if output_zip else 0

        if size_bytes == 0:
            output_dir_value = read_value(row, "output_dir")
            output_dir = Path(output_dir_value) if output_dir_value else None
            if output_dir and output_dir.exists():
                try:
                    latest_zip = max(output_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, default=None)
                    if latest_zip is not None:
                        size_bytes = compute_path_size(latest_zip)
                except OSError:
                    size_bytes = 0
        run_sizes[row_id] = size_bytes
        total_bytes += size_bytes
    return run_sizes, total_bytes


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
    outputs_root = OUTPUTS_DIR / safe_name
    return {
        "uploads": user_root / "upload",
        "outputs": outputs_root,
        "exports": outputs_root,
    }


def rewrite_user_owned_path(value: str | None, old_username: str, new_username: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""

    old_safe = sanitize_component(old_username)
    new_safe = sanitize_component(new_username)
    old_new_pairs = [
        (DATA_DIR / "users" / old_safe, DATA_DIR / "users" / new_safe),
        (OUTPUTS_DIR / old_safe, OUTPUTS_DIR / new_safe),
    ]

    for old_root, new_root in old_new_pairs:
        old_root_str = str(old_root)
        if raw == old_root_str:
            return str(new_root)
        prefix = f"{old_root_str}{os.sep}"
        if raw.startswith(prefix):
            suffix = raw[len(prefix):]
            return str(new_root / suffix)
    return raw


def rewrite_payload_user_paths(payload: Any, old_username: str, new_username: str) -> Any:
    if isinstance(payload, dict):
        return {key: rewrite_payload_user_paths(value, old_username, new_username) for key, value in payload.items()}
    if isinstance(payload, list):
        return [rewrite_payload_user_paths(value, old_username, new_username) for value in payload]
    if isinstance(payload, str):
        return rewrite_user_owned_path(payload, old_username, new_username)
    return payload


def migrate_user_references(conn: sqlite3.Connection, user_id: int, old_username: str, new_username: str) -> None:
    if old_username == new_username:
        return

    input_rows = conn.execute(
        "SELECT id, input_path FROM inputs_repository WHERE user_id = ?",
        (user_id,),
    ).fetchall()
    for row in input_rows:
        rewritten_input_path = rewrite_user_owned_path(row["input_path"], old_username, new_username)
        if rewritten_input_path != (row["input_path"] or ""):
            conn.execute("UPDATE inputs_repository SET input_path = ? WHERE id = ?", (rewritten_input_path, row["id"]))

    run_rows = conn.execute(
        "SELECT id, input_dir, output_dir, output_zip, output_log_file, payload_json FROM task_runs WHERE user_id = ?",
        (user_id,),
    ).fetchall()
    for row in run_rows:
        input_dir = rewrite_user_owned_path(row["input_dir"], old_username, new_username)
        output_dir = rewrite_user_owned_path(row["output_dir"], old_username, new_username)
        output_zip = rewrite_user_owned_path(row["output_zip"], old_username, new_username)
        output_log_file = rewrite_user_owned_path(row["output_log_file"], old_username, new_username)

        payload_json = row["payload_json"] or ""
        rewritten_payload_json = payload_json
        if payload_json:
            try:
                payload_obj = json.loads(payload_json)
                rewritten_payload_json = json.dumps(
                    rewrite_payload_user_paths(payload_obj, old_username, new_username),
                    ensure_ascii=False,
                )
            except json.JSONDecodeError:
                rewritten_payload_json = payload_json

        conn.execute(
            """
            UPDATE task_runs
            SET input_dir = ?, output_dir = ?, output_zip = ?, output_log_file = ?, payload_json = ?
            WHERE id = ?
            """,
            (input_dir, output_dir, output_zip, output_log_file, rewritten_payload_json, row["id"]),
        )

    user_settings_row = conn.execute(
        "SELECT settings_json FROM user_settings WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if user_settings_row and user_settings_row["settings_json"]:
        raw_settings_json = user_settings_row["settings_json"]
        rewritten_settings_json = raw_settings_json
        try:
            settings_obj = json.loads(raw_settings_json)
            rewritten_settings_json = json.dumps(
                rewrite_payload_user_paths(settings_obj, old_username, new_username),
                ensure_ascii=False,
            )
        except json.JSONDecodeError:
            rewritten_settings_json = raw_settings_json

        if rewritten_settings_json != raw_settings_json:
            conn.execute(
                "UPDATE user_settings SET settings_json = ?, updated_at = ? WHERE user_id = ?",
                (rewritten_settings_json, now_iso(), user_id),
            )


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




def parse_output_candidates(payload: dict[str, Any], input_dir_value: str) -> list[Path]:
    candidates: list[Path] = []
    for raw_path in (payload.get("output", ""), input_dir_value):
        if not raw_path:
            continue
        path = Path(raw_path)
        if path.exists() and path not in candidates:
            candidates.append(path)
    return candidates


def snapshot_output_dirs(candidates: list[Path], prefixes: tuple[str, ...]) -> dict[str, float]:
    if not prefixes:
        return {}
    snapshot: dict[str, float] = {}
    for base in candidates:
        try:
            for child in base.iterdir():
                if not child.is_dir() or not child.name.startswith(prefixes):
                    continue
                try:
                    snapshot[str(child.resolve())] = child.stat().st_mtime
                except OSError:
                    continue
        except OSError:
            continue
    return snapshot


def find_task_output_dir(candidates: list[Path], prefixes: tuple[str, ...], started_at_raw: str | None) -> Path | None:
    if not prefixes:
        return None
    started_at_dt = parse_iso_datetime(started_at_raw)
    all_dirs: list[Path] = []
    for base in candidates:
        try:
            all_dirs.extend([p for p in base.iterdir() if p.is_dir() and p.name.startswith(prefixes)])
        except OSError:
            continue
    if not all_dirs:
        return None

    if started_at_dt:
        recent_dirs: list[Path] = []
        for candidate in all_dirs:
            try:
                candidate_dt = datetime.fromtimestamp(candidate.stat().st_mtime, tz=timezone.utc).astimezone()
                if candidate_dt >= (started_at_dt - timedelta(seconds=5)):
                    recent_dirs.append(candidate)
            except OSError:
                continue
        if recent_dirs:
            return max(recent_dirs, key=lambda p: p.stat().st_mtime)

    return max(all_dirs, key=lambda p: p.stat().st_mtime)

def build_output_prefixes(module_value: str) -> tuple[str, ...]:
    if module_value == "update-network-frequencies":
        return tuple()
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
            created_at TEXT NOT NULL,
            access_request_reason TEXT
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
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
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
            input_name TEXT,
            payload_json TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inputs_repository (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            input_name TEXT NOT NULL UNIQUE,
            input_path TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            size_bytes INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )

    existing_columns = {row["name"] for row in cur.execute("PRAGMA table_info(task_runs)").fetchall()}
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
    if "input_name" not in existing_columns:
        cur.execute("ALTER TABLE task_runs ADD COLUMN input_name TEXT")
    if "payload_json" not in existing_columns:
        cur.execute("ALTER TABLE task_runs ADD COLUMN payload_json TEXT")

    user_columns = {row["name"] for row in cur.execute("PRAGMA table_info(users)").fetchall()}
    if "access_request_reason" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN access_request_reason TEXT")

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


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc).astimezone()
    return parsed.astimezone()


def apply_session_idle_timeout(
    conn: sqlite3.Connection, session_row: sqlite3.Row, now: datetime, client_ip: str
) -> bool:
    if session_row["active"] == 0:
        return False
    last_seen = parse_iso_datetime(session_row["last_seen_at"])
    if not last_seen:
        return False
    idle_seconds = (now - last_seen).total_seconds()
    if idle_seconds <= SESSION_IDLE_TIMEOUT_SECONDS:
        return False
    cutoff = last_seen + timedelta(seconds=SESSION_IDLE_TIMEOUT_SECONDS)
    conn.execute(
        "UPDATE sessions SET active = 0, last_seen_at = ? WHERE token = ?",
        (cutoff.isoformat(), session_row["token"]),
    )
    conn.commit()
    username = session_row["username"] if "username" in session_row.keys() and session_row["username"] else "unknown"
    idle_seconds_int = max(int(idle_seconds), 0)
    web_access_logger.info(
        "logout user=%s ip=%s reason=inactivity idle_seconds=%s idle_hms=%s timeout_seconds=%s",
        username,
        client_ip or "unknown",
        idle_seconds_int,
        format_seconds_hms(idle_seconds_int),
        SESSION_IDLE_TIMEOUT_SECONDS,
    )
    return True


def build_connected_users_snapshot(conn: sqlite3.Connection) -> tuple[dict[int, bool], int]:
    """Return connected status per user based on active sessions updated within timeout window."""
    now_dt = datetime.now().astimezone()
    connected_users: dict[int, bool] = {}
    active_sessions = conn.execute(
        "SELECT user_id, last_seen_at FROM sessions WHERE active = 1"
    ).fetchall()
    for session_row in active_sessions:
        user_id = int(session_row["user_id"])
        if connected_users.get(user_id):
            continue
        last_seen = parse_iso_datetime(session_row["last_seen_at"])
        if not last_seen:
            continue
        if (now_dt - last_seen).total_seconds() <= SESSION_IDLE_TIMEOUT_SECONDS:
            connected_users[user_id] = True
    return connected_users, len(connected_users)


def get_current_user(request: Request) -> sqlite3.Row | None:
    token = request.cookies.get("session_token")
    if not token:
        return None
    conn = get_conn()
    session = conn.execute(
        """
        SELECT s.token, s.user_id, s.active, s.last_seen_at, u.username
        FROM sessions s
        LEFT JOIN users u ON u.id = s.user_id
        WHERE s.token = ?
        """,
        (token,),
    ).fetchone()
    if not session or session["active"] == 0:
        conn.close()
        return None
    if apply_session_idle_timeout(conn, session, datetime.now().astimezone(), get_client_ip(request)):
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


def get_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    return request.client.host if request.client else "unknown"


def save_user_settings(user_id: int, settings: dict[str, Any]) -> None:
    sanitized_settings = sanitize_user_settings_payload(settings)
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO user_settings(user_id, settings_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET settings_json = excluded.settings_json, updated_at = excluded.updated_at
        """,
        (user_id, json.dumps(sanitized_settings, ensure_ascii=False), now_iso()),
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
        payload = json.loads(row["settings_json"])
    except json.JSONDecodeError:
        payload = {}
    sanitized_payload = sanitize_user_settings_payload(payload)
    if sanitized_payload != payload:
        save_user_settings(user_id, sanitized_payload)
    return sanitized_payload


def parse_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).lower() in {"on", "true", "1", "yes"}


def coerce_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def load_legacy_admin_settings_payload() -> dict[str, Any]:
    """Backward-compatible reader for old builds storing admin settings in user_settings(admin)."""
    conn = get_conn()
    admin_row = conn.execute("SELECT id FROM users WHERE LOWER(username) = 'admin' ORDER BY id ASC LIMIT 1").fetchone()
    storage_user_id = int(admin_row["id"]) if admin_row else -1
    row = conn.execute("SELECT settings_json FROM user_settings WHERE user_id = ?", (storage_user_id,)).fetchone()
    conn.close()
    if not row:
        return {}
    try:
        payload = json.loads(row["settings_json"])
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def load_app_setting_payload(key: str) -> dict[str, Any]:
    conn = get_conn()
    row = conn.execute("SELECT value_json FROM app_settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    if not row:
        return {}
    try:
        payload = json.loads(row["value_json"])
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def save_app_setting_payload(key: str, payload: dict[str, Any]) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO app_settings(key, value_json, updated_at)
        VALUES(?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
        """,
        (key, json.dumps(payload, ensure_ascii=False), now_iso()),
    )
    conn.commit()
    conn.close()


def load_admin_settings_payload() -> dict[str, Any]:
    payload = load_app_setting_payload("global_admin_settings")
    if payload:
        return payload

    # One-time fallback for older installations.
    legacy_payload = load_legacy_admin_settings_payload()
    if legacy_payload:
        save_app_setting_payload("global_admin_settings", legacy_payload)
    return legacy_payload


def save_admin_settings_payload(payload: dict[str, Any]) -> None:
    save_app_setting_payload("global_admin_settings", payload)


def coerce_hour(value: Any, default: int = 2) -> int:
    return coerce_int(value, default, 0, 23)


def coerce_backup_mode(value: Any) -> str:
    allowed = {"disabled", "daily", "weekly", "monthly"}
    normalized = str(value or "").strip().lower()
    if normalized in allowed:
        return normalized
    # Backward compatibility with previous boolean flag.
    if parse_bool(value):
        return "daily"
    return "disabled"


def get_global_radio_defaults() -> dict[str, str]:
    config_values = load_persistent_config()
    return {
        "n77_ssb_pre": str(config_values.get("n77_ssb_pre", "") or ""),
        "n77_ssb_post": str(config_values.get("n77_ssb_post", "") or ""),
        "n77b_ssb": str(config_values.get("n77b_ssb", "") or ""),
        "allowed_n77_ssb_pre": str(config_values.get("allowed_n77_ssb_pre", "") or ""),
        "allowed_n77_arfcn_pre": str(config_values.get("allowed_n77_arfcn_pre", "") or ""),
        "allowed_n77_ssb_post": str(config_values.get("allowed_n77_ssb_post", "") or ""),
        "allowed_n77_arfcn_post": str(config_values.get("allowed_n77_arfcn_post", "") or ""),
        "ca_freq_filters": str(config_values.get("ca_freq_filters", "") or ""),
        "cc_freq_filters": str(config_values.get("cc_freq_filters", "") or ""),
        "network_frequencies": str(config_values.get("network_frequencies", "") or ""),
    }


def get_admin_settings() -> dict[str, Any]:
    payload = load_admin_settings_payload()
    defaults = get_global_radio_defaults()
    backup_auto_path = str(payload.get("db_backup_auto_path") or "").strip()
    if not backup_auto_path:
        backup_auto_path = str((DATA_DB_DIR / "backups").resolve())
    return {
        "max_cpu_percent": coerce_int(payload.get("max_cpu_percent"), MAX_CPU_DEFAULT, 10, 100),
        "max_memory_percent": coerce_int(payload.get("max_memory_percent"), MAX_MEMORY_DEFAULT, 10, 100),
        "max_parallel_tasks": coerce_int(payload.get("max_parallel_tasks"), MAX_PARALLEL_DEFAULT, 1, 8),
        "db_backup_auto_mode": coerce_backup_mode(payload.get("db_backup_auto_mode", payload.get("db_backup_auto_enabled"))),
        "db_backup_auto_path": backup_auto_path,
        "db_backup_auto_hour": coerce_hour(payload.get("db_backup_auto_hour"), 2),
        "db_backup_max_to_store": coerce_int(payload.get("db_backup_max_to_store"), 30, 1, 3650),
        "db_backup_last_run_date": str(payload.get("db_backup_last_run_date") or ""),
        "n77_ssb_pre": str(payload.get("n77_ssb_pre", defaults["n77_ssb_pre"]) or ""),
        "n77_ssb_post": str(payload.get("n77_ssb_post", defaults["n77_ssb_post"]) or ""),
        "n77b_ssb": str(payload.get("n77b_ssb", defaults["n77b_ssb"]) or ""),
        "allowed_n77_ssb_pre": str(payload.get("allowed_n77_ssb_pre", defaults["allowed_n77_ssb_pre"]) or ""),
        "allowed_n77_arfcn_pre": str(payload.get("allowed_n77_arfcn_pre", defaults["allowed_n77_arfcn_pre"]) or ""),
        "allowed_n77_ssb_post": str(payload.get("allowed_n77_ssb_post", defaults["allowed_n77_ssb_post"]) or ""),
        "allowed_n77_arfcn_post": str(payload.get("allowed_n77_arfcn_post", defaults["allowed_n77_arfcn_post"]) or ""),
        "ca_freq_filters": str(payload.get("ca_freq_filters", defaults["ca_freq_filters"]) or ""),
        "cc_freq_filters": str(payload.get("cc_freq_filters", defaults["cc_freq_filters"]) or ""),
        "network_frequencies": str(payload.get("network_frequencies", defaults["network_frequencies"]) or ""),
    }


def save_admin_settings(
    max_cpu_percent: int,
    max_memory_percent: int,
    max_parallel_tasks: int,
    db_backup_auto_mode: str = "disabled",
    db_backup_auto_path: str = "",
    db_backup_auto_hour: int = 2,
    db_backup_max_to_store: int = 30,
    db_backup_last_run_date: str | None = None,
    n77_ssb_pre: str | None = None,
    n77_ssb_post: str | None = None,
    n77b_ssb: str | None = None,
    allowed_n77_ssb_pre: str | None = None,
    allowed_n77_arfcn_pre: str | None = None,
    allowed_n77_ssb_post: str | None = None,
    allowed_n77_arfcn_post: str | None = None,
    ca_freq_filters: str | None = None,
    cc_freq_filters: str | None = None,
    network_frequencies: str | None = None,
) -> None:
    current = get_admin_settings()
    backup_path = str(db_backup_auto_path or "").strip() or current["db_backup_auto_path"]
    settings = {
        "max_cpu_percent": coerce_int(max_cpu_percent, MAX_CPU_DEFAULT, 10, 100),
        "max_memory_percent": coerce_int(max_memory_percent, MAX_MEMORY_DEFAULT, 10, 100),
        "max_parallel_tasks": coerce_int(max_parallel_tasks, MAX_PARALLEL_DEFAULT, 1, 8),
        "db_backup_auto_mode": coerce_backup_mode(db_backup_auto_mode),
        "db_backup_auto_path": backup_path,
        "db_backup_auto_hour": coerce_hour(db_backup_auto_hour, current["db_backup_auto_hour"]),
        "db_backup_max_to_store": coerce_int(db_backup_max_to_store, current["db_backup_max_to_store"], 1, 3650),
        "db_backup_last_run_date": db_backup_last_run_date if db_backup_last_run_date is not None else current["db_backup_last_run_date"],
        "n77_ssb_pre": str(n77_ssb_pre if n77_ssb_pre is not None else current.get("n77_ssb_pre", "")),
        "n77_ssb_post": str(n77_ssb_post if n77_ssb_post is not None else current.get("n77_ssb_post", "")),
        "n77b_ssb": str(n77b_ssb if n77b_ssb is not None else current.get("n77b_ssb", "")),
        "allowed_n77_ssb_pre": str(allowed_n77_ssb_pre if allowed_n77_ssb_pre is not None else current.get("allowed_n77_ssb_pre", "")),
        "allowed_n77_arfcn_pre": str(allowed_n77_arfcn_pre if allowed_n77_arfcn_pre is not None else current.get("allowed_n77_arfcn_pre", "")),
        "allowed_n77_ssb_post": str(allowed_n77_ssb_post if allowed_n77_ssb_post is not None else current.get("allowed_n77_ssb_post", "")),
        "allowed_n77_arfcn_post": str(allowed_n77_arfcn_post if allowed_n77_arfcn_post is not None else current.get("allowed_n77_arfcn_post", "")),
        "ca_freq_filters": str(ca_freq_filters if ca_freq_filters is not None else current.get("ca_freq_filters", "")),
        "cc_freq_filters": str(cc_freq_filters if cc_freq_filters is not None else current.get("cc_freq_filters", "")),
        "network_frequencies": str(network_frequencies if network_frequencies is not None else current.get("network_frequencies", "")),
    }
    save_admin_settings_payload(settings)
    sync_running_process_registry()


def create_database_backup(backup_dir: Path, reason: str = "manual", max_to_store: int = 30) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_file = backup_dir / f"web_interface_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(DB_PATH, backup_file)

    max_keep = coerce_int(max_to_store, 30, 1, 3650)
    backup_files = sorted(
        [p for p in backup_dir.glob("web_interface_backup_*.db") if p.is_file()],
        key=lambda p: p.stat().st_mtime,
    )
    overflow = len(backup_files) - max_keep
    if overflow > 0:
        for old_file in backup_files[:overflow]:
            old_file.unlink(missing_ok=True)

    logger.info("Database backup created (%s): %s", reason, backup_file)
    return backup_file


def run_scheduled_database_backup_if_needed() -> None:
    settings = get_admin_settings()
    mode = settings["db_backup_auto_mode"]
    if mode == "disabled":
        return

    now_dt = datetime.now().astimezone()
    today = now_dt.date()
    today_str = today.isoformat()

    if settings["db_backup_last_run_date"] == today_str:
        return
    if int(settings["db_backup_auto_hour"]) != now_dt.hour:
        return

    should_run = False
    if mode == "daily":
        should_run = True
    elif mode == "weekly":
        should_run = today.weekday() == 0  # Monday
    elif mode == "monthly":
        should_run = today.day == 1

    if not should_run:
        return

    backup_dir = Path(str(settings["db_backup_auto_path"]).strip() or str((DATA_DB_DIR / "backups").resolve()))
    create_database_backup(backup_dir, reason=f"auto-{mode}", max_to_store=int(settings["db_backup_max_to_store"]))
    save_admin_settings(
        settings["max_cpu_percent"],
        settings["max_memory_percent"],
        settings["max_parallel_tasks"],
        db_backup_auto_mode=mode,
        db_backup_auto_path=str(backup_dir),
        db_backup_auto_hour=settings["db_backup_auto_hour"],
        db_backup_max_to_store=settings["db_backup_max_to_store"],
        db_backup_last_run_date=today_str,
    )

def detect_task_name_from_input(input_path: str) -> str:
    if not input_path:
        return "unknown"
    return Path(input_path).name or sanitize_component(input_path)


def enqueue_payloads_for_user(
    conn: sqlite3.Connection,
    user_id: int,
    username: str,
    queue_payloads: list[dict[str, Any]],
    tool_version: str,
    force_user_output_root: bool = False,
) -> int:
    if not queue_payloads:
        return 0

    default_output_root = Path(OUTPUTS_DIR / sanitize_component(username))
    queue_batch_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    for index, queue_payload in enumerate(queue_payloads, start=1):
        if force_user_output_root:
            base_output_root = default_output_root
        else:
            configured_output = str(queue_payload.get("output") or "").strip()
            base_output_root = Path(configured_output) if configured_output else default_output_root
        queue_output_dir = base_output_root / f"queue_task_{queue_batch_stamp}_{index:02d}"
        queue_output_dir.mkdir(parents=True, exist_ok=True)
        queue_payload["output"] = str(queue_output_dir)

    for queue_payload in queue_payloads:
        module_name = str(queue_payload.get("module") or "").strip()
        task_input = queue_payload.get("input_post", "") if module_name == "consistency-check" else queue_payload.get("input", "")
        task_name = detect_task_name_from_input(str(task_input or ""))
        conn.execute(
            """
            INSERT INTO task_runs(user_id, module, tool_version, input_name, status, started_at, command, output_log, payload_json, input_dir)
            VALUES (?, ?, ?, ?, 'queued', ?, '', '', ?, ?)
            """,
            (
                user_id,
                module_name,
                tool_version,
                task_name,
                "",
                json.dumps(queue_payload),
                task_input,
            ),
        )

    return len(queue_payloads)


def format_task_status(raw_status: str) -> str:
    normalized = (raw_status or "").strip().lower()
    mapping = {
        "queued": "Queued",
        "running": "Running",
        "canceling": "Canceling",
        "ok": "Success",
        "error": "Error",
        "canceled": "Canceled",
    }
    return mapping.get(normalized, raw_status or "Unknown")


def list_inputs_repository() -> list[dict[str, Any]]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT ir.id, ir.input_name, ir.input_path, ir.uploaded_at, ir.size_bytes, u.username
        FROM inputs_repository ir
        LEFT JOIN users u ON u.id = ir.user_id
        ORDER BY ir.uploaded_at DESC
        """
    ).fetchall()
    conn.close()
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "id": row["id"],
                "input_name": row["input_name"],
                "input_path": row["input_path"],
                "uploaded_by": row["username"] or "unknown",
                "uploaded_at": format_timestamp(row["uploaded_at"]),
                "size_mb": format_mb(row["size_bytes"] or 0),
            }
        )
    return items




def list_inputs_uploaders() -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT DISTINCT COALESCE(u.username, 'unknown') AS username
        FROM inputs_repository ir
        LEFT JOIN users u ON u.id = ir.user_id
        ORDER BY username COLLATE NOCASE ASC
        """
    ).fetchall()
    conn.close()
    return [str(row["username"]) for row in rows if row["username"]]

def get_inputs_repository_total_size() -> str:
    conn = get_conn()
    row = conn.execute("SELECT COALESCE(SUM(size_bytes), 0) AS total_size FROM inputs_repository").fetchone()
    conn.close()
    return format_mb(int(row["total_size"] or 0))


def move_directory_best_effort(source_dir: Path, destination_dir: Path) -> Path:
    """Move a directory, falling back to copy+delete when rename/move fails."""
    try:
        shutil.move(str(source_dir), str(destination_dir))
        return destination_dir
    except OSError:
        pass

    try:
        shutil.copytree(source_dir, destination_dir)
        shutil.rmtree(source_dir, ignore_errors=True)
        return destination_dir
    except OSError:
        return source_dir


def run_queued_task(task_row: sqlite3.Row) -> None:
    task_id = task_row["id"]
    payload = json.loads(task_row["payload_json"] or "{}")
    module = task_row["module"]
    username = task_row["username"]
    started_at = task_row["started_at"] or now_iso()

    cmd = build_cli_command(payload)
    status = "ok"
    output_log = ""
    start = time.perf_counter()

    input_dir_value = payload.get("input_post", "") if module == "consistency-check" else payload.get("input", "")
    output_candidates = parse_output_candidates(payload, input_dir_value)
    output_prefixes = build_output_prefixes(module)
    output_snapshot_before = snapshot_output_dirs(output_candidates, output_prefixes)

    def persist_partial_output_log(log_text: str) -> None:
        conn_partial = get_conn()
        conn_partial.execute(
            "UPDATE task_runs SET output_log = ? WHERE id = ?",
            (strip_ansi(log_text)[:20000], task_id),
        )
        conn_partial.commit()
        conn_partial.close()

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env={**os.environ, "TERM": "xterm", "SSB_RA_NO_CLEAR": "1"},
        )
        with running_processes_lock:
            running_processes[task_id] = proc
        output_chunks: list[str] = []
        last_partial_update = time.perf_counter()
        stream = proc.stdout
        if stream is not None:
            for line in iter(stream.readline, ""):
                output_chunks.append(line)
                now_perf = time.perf_counter()
                if now_perf - last_partial_update >= 1.0:
                    persist_partial_output_log("".join(output_chunks))
                    last_partial_update = now_perf
            stream.close()

        proc.wait()
        output_log = "".join(output_chunks)
        persist_partial_output_log(output_log)
        canceled = False
        with canceled_task_ids_lock:
            if task_id in canceled_task_ids:
                canceled_task_ids.remove(task_id)
                canceled = True
        if canceled:
            status = "canceled"
        if proc.returncode != 0 or execution_output_has_error(output_log):
            status = "error" if status != "canceled" else status
    except Exception as exc:
        status = "error"
        output_log = f"Execution error: {exc}"
    finally:
        with running_processes_lock:
            running_processes.pop(task_id, None)
        sync_running_process_registry()

    output_log = strip_ansi(output_log)
    duration = time.perf_counter() - start
    finished_at = now_iso()

    output_dir_value = ""
    output_zip_value = ""
    output_log_file_value = ""
    tool_version = task_row["tool_version"] or "unknown"

    output_dir = None
    temp_output_root_value = payload.get("output", "")
    temp_output_root = Path(temp_output_root_value) if temp_output_root_value else None

    if (
        temp_output_root
        and temp_output_root.exists()
        and temp_output_root.is_dir()
        and temp_output_root.name.startswith("queue_task_")
        and any(temp_output_root.iterdir())
    ):
        output_dir = temp_output_root


    if output_dir is None:
        output_snapshot_after = snapshot_output_dirs(output_candidates, output_prefixes)
        changed_dirs: list[Path] = []
        for raw_path, after_mtime in output_snapshot_after.items():
            before_mtime = output_snapshot_before.get(raw_path)
            if before_mtime is None or after_mtime > before_mtime:
                changed_dirs.append(Path(raw_path))

        if changed_dirs:
            output_dir = max(changed_dirs, key=lambda p: p.stat().st_mtime)
        if output_dir is None:
            output_snapshot_after = snapshot_output_dirs(output_candidates, output_prefixes)
            changed_dirs: list[Path] = []
            for raw_path, after_mtime in output_snapshot_after.items():
                before_mtime = output_snapshot_before.get(raw_path)
                if before_mtime is None or after_mtime > before_mtime:
                    changed_dirs.append(Path(raw_path))

            if changed_dirs:
                output_dir = max(changed_dirs, key=lambda p: p.stat().st_mtime)
            if output_dir is None:
                output_dir = find_task_output_dir(output_candidates, output_prefixes, started_at)

            if output_dir is None and temp_output_root and temp_output_root.exists() and temp_output_root.is_dir() and any(temp_output_root.iterdir()):
                output_dir = temp_output_root

        if output_dir is None and temp_output_root and temp_output_root.exists() and temp_output_root.is_dir() and any(temp_output_root.iterdir()):
            output_dir = temp_output_root

    if output_dir:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        outputs_dir = OUTPUTS_DIR / sanitize_component(username)
        outputs_dir.mkdir(parents=True, exist_ok=True)
        task_name_component = sanitize_component(detect_task_name_from_input(input_dir_value))
        module_prefix = sanitize_component(module)
        dest_name = f"{timestamp}_{module_prefix}_v{sanitize_component(tool_version)}_{task_name_component}"
        persisted_output_dir = outputs_dir / sanitize_component(dest_name)
        suffix = 1
        while persisted_output_dir.exists():
            persisted_output_dir = outputs_dir / sanitize_component(f"{dest_name}_{suffix:02d}")
            suffix += 1
        output_dir = move_directory_best_effort(output_dir, persisted_output_dir)
        output_dir_value = str(output_dir)
        try:
            for existing in output_dir.glob("RetuningAutomation_*.log"):
                existing.unlink(missing_ok=True)
            legacy_log = output_dir / "webapp_output.log"
            if legacy_log.exists():
                legacy_log.unlink()
            log_name = f"RetuningAutomation_{timestamp}_v{tool_version}.log"
            output_log_file = output_dir / log_name
            output_log_file.write_text(output_log, encoding="utf-8")
            output_log_file_value = str(output_log_file)
        except OSError:
            output_log_file_value = ""

        zip_name = f"{sanitize_component(module)}_{timestamp}_v{sanitize_component(tool_version)}.zip"
        output_zip_path = output_dir / zip_name
        try:
            create_zip_from_dir(output_dir, output_zip_path)
            output_zip_value = str(output_zip_path)
            for item in list(output_dir.iterdir()):
                if item == output_zip_path:
                    continue
                if output_log_file_value and item == Path(output_log_file_value):
                    continue
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink(missing_ok=True)
        except OSError:
            output_zip_value = ""

    temp_output_root_value = payload.get("output", "")
    if temp_output_root_value:
        temp_output_root = Path(temp_output_root_value)
        if temp_output_root.name.startswith("queue_task_"):
            try:
                if temp_output_root.exists() and temp_output_root.is_dir():
                    moved_elsewhere = False
                    if output_dir_value:
                        try:
                            moved_elsewhere = Path(output_dir_value).resolve() != temp_output_root.resolve()
                        except OSError:
                            moved_elsewhere = output_dir_value != str(temp_output_root)
                    if moved_elsewhere:
                        shutil.rmtree(temp_output_root, ignore_errors=True)
                    elif not any(temp_output_root.iterdir()):
                        temp_output_root.rmdir()
            except OSError:
                pass

    conn = get_conn()
    conn.execute(
        """
        UPDATE task_runs
        SET status = ?, finished_at = ?, duration_seconds = ?, command = ?, output_log = ?,
            input_dir = ?, output_dir = ?, output_zip = ?, output_log_file = ?
        WHERE id = ?
        """,
        (
            status,
            finished_at,
            duration,
            " ".join(shlex.quote(part) for part in cmd),
            output_log[:20000],
            input_dir_value,
            output_dir_value,
            output_zip_value,
            output_log_file_value,
            task_id,
        ),
    )
    conn.commit()
    conn.close()


def queue_worker() -> None:
    while True:
        queue_event.wait(timeout=1.0)
        scheduled = False
        while True:
            conn = get_conn()
            admin_settings = get_admin_settings()
            max_parallel = admin_settings["max_parallel_tasks"]
            running_count = conn.execute(
                "SELECT COUNT(*) AS total FROM task_runs WHERE status IN ('running', 'canceling')"
            ).fetchone()["total"]
            if running_count >= max_parallel:
                conn.close()
                break

            queued_row = conn.execute(
                "SELECT id FROM task_runs WHERE status = 'queued' ORDER BY id ASC LIMIT 1"
            ).fetchone()
            if queued_row is None:
                conn.close()
                break

            started_at = now_iso()
            updated = conn.execute(
                "UPDATE task_runs SET status = 'running', started_at = ? WHERE id = ? AND status = 'queued'",
                (started_at, queued_row["id"]),
            )
            conn.commit()
            if updated.rowcount == 0:
                conn.close()
                continue

            task_row = conn.execute(
                """
                SELECT tr.*, u.username
                FROM task_runs tr
                JOIN users u ON u.id = tr.user_id
                WHERE tr.id = ?
                """,
                (queued_row["id"],),
            ).fetchone()
            conn.close()
            if task_row is None:
                continue

            threading.Thread(target=run_queued_task, args=(task_row,), daemon=True).start()
            sync_running_process_registry()
            scheduled = True

        if not scheduled:
            queue_event.clear()


def ensure_worker_started() -> None:
    global worker_started
    with worker_lock:
        if worker_started:
            return
        threading.Thread(target=queue_worker, daemon=True, name="task-queue-worker").start()
        worker_started = True

def database_backup_worker() -> None:
    while True:
        try:
            run_scheduled_database_backup_if_needed()
        except Exception as exc:
            logger.exception("Automatic database backup worker error: %s", exc)
        time.sleep(30)


def ensure_backup_worker_started() -> None:
    global backup_worker_started
    with backup_worker_lock:
        if backup_worker_started:
            return
        threading.Thread(target=database_backup_worker, daemon=True, name="db-backup-worker").start()
        backup_worker_started = True



def load_persistent_config() -> dict[str, str]:
    return load_cfg_values(CONFIG_PATH, CONFIG_SECTION, CFG_FIELD_MAP, *CFG_FIELDS)


def build_system_config_payload() -> dict[str, dict[str, str]]:
    return {CONFIG_SECTION: load_persistent_config()}


def apply_system_config_payload(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        raise ValueError("invalid_payload")

    source = payload.get(CONFIG_SECTION) if isinstance(payload.get(CONFIG_SECTION), dict) else payload
    if not isinstance(source, dict):
        raise ValueError("invalid_payload")

    current_cfg = load_persistent_config()
    persist_kwargs: dict[str, str] = {}
    for field in CFG_FIELDS:
        if field in source:
            persist_kwargs[field] = str(source.get(field, "") or "")
        else:
            persist_kwargs[field] = str(current_cfg.get(field, "") or "")

    save_cfg_values(
        config_dir=CONFIG_DIR,
        config_path=CONFIG_PATH,
        config_section=CONFIG_SECTION,
        cfg_field_map=CFG_FIELD_MAP,
        **persist_kwargs,
    )
    return load_persistent_config()


GLOBAL_RUNTIME_FORM_KEYS = (
    "n77_ssb_pre",
    "n77_ssb_post",
    "n77b_ssb",
    "allowed_n77_ssb_pre",
    "allowed_n77_arfcn_pre",
    "allowed_n77_ssb_post",
    "allowed_n77_arfcn_post",
    "ca_freq_filters",
    "cc_freq_filters",
    "network_frequencies",
)


def save_global_runtime_form_settings(values: dict[str, Any]) -> None:
    current = get_admin_settings()
    save_admin_settings(
        current["max_cpu_percent"],
        current["max_memory_percent"],
        current["max_parallel_tasks"],
        db_backup_auto_mode=current["db_backup_auto_mode"],
        db_backup_auto_path=current["db_backup_auto_path"],
        db_backup_auto_hour=current["db_backup_auto_hour"],
        db_backup_max_to_store=current["db_backup_max_to_store"],
        db_backup_last_run_date=current["db_backup_last_run_date"],
        n77_ssb_pre=str(values.get("n77_ssb_pre", current.get("n77_ssb_pre", "")) or ""),
        n77_ssb_post=str(values.get("n77_ssb_post", current.get("n77_ssb_post", "")) or ""),
        n77b_ssb=str(values.get("n77b_ssb", current.get("n77b_ssb", "")) or ""),
        allowed_n77_ssb_pre=str(values.get("allowed_n77_ssb_pre", current.get("allowed_n77_ssb_pre", "")) or ""),
        allowed_n77_arfcn_pre=str(values.get("allowed_n77_arfcn_pre", current.get("allowed_n77_arfcn_pre", "")) or ""),
        allowed_n77_ssb_post=str(values.get("allowed_n77_ssb_post", current.get("allowed_n77_ssb_post", "")) or ""),
        allowed_n77_arfcn_post=str(values.get("allowed_n77_arfcn_post", current.get("allowed_n77_arfcn_post", "")) or ""),
        ca_freq_filters=str(values.get("ca_freq_filters", current.get("ca_freq_filters", "")) or ""),
        cc_freq_filters=str(values.get("cc_freq_filters", current.get("cc_freq_filters", "")) or ""),
        network_frequencies=str(values.get("network_frequencies", current.get("network_frequencies", "")) or ""),
    )


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
        settings["input_pre"] = ""
        settings["input_post"] = ""
        settings["input"] = ""
        return settings

    settings["input"] = ""

    settings["input_pre"] = ""
    settings["input_post"] = ""
    return settings


def normalize_module_inputs_map(raw_map: Any) -> dict[str, dict[str, str]]:
    if not isinstance(raw_map, dict):
        return {}

    normalized: dict[str, dict[str, str]] = {}
    for module_key, module_inputs in raw_map.items():
        module_name = str(module_key or "").strip()
        if not module_name or not isinstance(module_inputs, dict):
            continue
        normalized[module_name] = {
            "input": str(module_inputs.get("input", "") or ""),
            "input_pre": str(module_inputs.get("input_pre", "") or ""),
            "input_post": str(module_inputs.get("input_post", "") or ""),
        }
    return normalized


def sanitize_wildcard_history(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    seen: set[str] = set()
    cleaned: list[str] = []
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
        if len(cleaned) >= 10:
            break
    return cleaned


def sanitize_ui_panels(raw_panels: Any) -> dict[str, bool]:
    if not isinstance(raw_panels, dict):
        return {}
    sanitized: dict[str, bool] = {}
    for key, value in raw_panels.items():
        key_name = str(key or "").strip()
        if not key_name:
            continue
        sanitized[key_name] = bool(value)
    return sanitized


def sanitize_admin_users_sort(raw_sort: Any) -> dict[str, str]:
    if not isinstance(raw_sort, dict):
        return {}
    sort_id = str(raw_sort.get("sort_id") or "").strip()
    direction = str(raw_sort.get("direction") or "asc").strip().lower()
    if direction not in {"asc", "desc"}:
        direction = "asc"
    return {"sort_id": sort_id, "direction": direction}


def sanitize_user_settings_payload(raw_payload: Any) -> dict[str, Any]:
    if not isinstance(raw_payload, dict):
        return {}

    sanitized: dict[str, Any] = {}
    for key, value in raw_payload.items():
        key_name = str(key or "").strip()
        if not key_name:
            continue
        if key_name in USER_SETTINGS_OBSOLETE_KEYS:
            continue
        if key_name not in USER_SETTINGS_ALLOWED_KEYS:
            continue

        if key_name in {"profiles_audit", "frequency_audit", "export_correction_cmd", "fast_excel_export"}:
            sanitized[key_name] = parse_bool(value)
        elif key_name == "module_inputs_map":
            sanitized[key_name] = normalize_module_inputs_map(value)
        elif key_name == "ui_panels":
            sanitized[key_name] = sanitize_ui_panels(value)
        elif key_name in {"wildcard_history_inputs", "wildcard_history_executions"}:
            sanitized[key_name] = sanitize_wildcard_history(value)
        elif key_name == "admin_users_sort":
            sanitized[key_name] = sanitize_admin_users_sort(value)
        else:
            sanitized[key_name] = str(value or "") if value is not None else ""

    return sanitized


def sanitize_all_user_settings_rows() -> None:
    conn = get_conn()
    rows = conn.execute("SELECT user_id, settings_json FROM user_settings").fetchall()
    changed = 0
    for row in rows:
        raw_json = row["settings_json"] or ""
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            payload = {}
        sanitized = sanitize_user_settings_payload(payload)
        if sanitized != payload:
            conn.execute(
                "UPDATE user_settings SET settings_json = ?, updated_at = ? WHERE user_id = ?",
                (json.dumps(sanitized, ensure_ascii=False), now_iso(), int(row["user_id"])),
            )
            changed += 1

    if changed:
        conn.commit()
    conn.close()


def build_settings_with_module_inputs(existing_settings: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    merged_settings = dict(existing_settings)
    merged_settings.update(payload)

    module_value = str(merged_settings.get("module") or "configuration-audit")
    module_inputs_map = normalize_module_inputs_map(merged_settings.get("module_inputs_map"))
    module_inputs_map[module_value] = {
        "input": str(merged_settings.get("input", "") or ""),
        "input_pre": str(merged_settings.get("input_pre", "") or ""),
        "input_post": str(merged_settings.get("input_post", "") or ""),
    }
    merged_settings["module_inputs_map"] = module_inputs_map
    return merged_settings


def persist_settings_to_config(module_value: str, payload: dict[str, Any]) -> None:
    # Keep network frequencies global and stable across users/sessions.
    # They are persisted by Module 0 itself (launcher logic), not by per-user web form autosave.
    current_cfg = load_persistent_config()
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
        "network_frequencies": current_cfg.get("network_frequencies", ""),
    }

    # Input paths are intentionally not persisted so every execution starts with empty input fields.
    persist_kwargs["last_input"] = ""
    persist_kwargs["last_input_audit"] = ""
    persist_kwargs["last_input_cc_pre"] = ""
    persist_kwargs["last_input_cc_post"] = ""
    persist_kwargs["last_input_cc_bulk"] = ""
    persist_kwargs["last_input_final_cleanup"] = ""

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
    if payload.get("output"):
        cmd.extend(["--output", payload["output"]])

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




def resolve_run_zip_path(conn: sqlite3.Connection, user_id: int, username: str, run_id: int, stored_output_zip: str | None, stored_output_dir: str | None, module: str | None, tool_version: str | None, finished_at_raw: str | None) -> Path | None:
    """Best-effort locate the zip file for a run even if output folders were renamed manually."""
    zip_path = Path(stored_output_zip) if stored_output_zip else None
    if zip_path and zip_path.exists():
        return zip_path

    user_root = OUTPUTS_DIR / sanitize_component(username)
    if not user_root.exists() or not user_root.is_dir():
        return None

    candidates: list[Path] = []

    # 1) If we have a stored zip filename, search it under the user's outputs root.
    if zip_path is not None:
        zip_name = zip_path.name
        try:
            for found in user_root.rglob(zip_name):
                if found.is_file() and is_safe_path(OUTPUTS_DIR, found):
                    candidates.append(found)
        except OSError:
            candidates = []

    # 2) Fallback: if output_dir still exists, use any zip inside it.
    if not candidates and stored_output_dir:
        out_dir = Path(stored_output_dir)
        if out_dir.exists() and out_dir.is_dir():
            try:
                for found in out_dir.glob("*.zip"):
                    if found.is_file() and is_safe_path(OUTPUTS_DIR, found):
                        candidates.append(found)
            except OSError:
                candidates = []

    # 3) Last fallback: search by module/version pattern (covers renamed zip filenames).
    if not candidates:
        mod = sanitize_component(module or "")
        ver = sanitize_component(tool_version or "")
        pattern = f"{mod}_*_v{ver}.zip" if (mod and ver) else "*.zip"
        try:
            for found in user_root.rglob(pattern):
                if found.is_file() and is_safe_path(OUTPUTS_DIR, found):
                    candidates.append(found)
        except OSError:
            candidates = []

    if not candidates:
        return None

    target_dt = parse_iso_datetime(finished_at_raw) if finished_at_raw else None
    best: Path
    if target_dt:
        target_ts = target_dt.timestamp()
        try:
            best = min(candidates, key=lambda p: abs(p.stat().st_mtime - target_ts))
        except OSError:
            best = max(candidates, key=lambda p: p.stat().st_mtime, default=candidates[0])
    else:
        best = max(candidates, key=lambda p: p.stat().st_mtime)

    if not best.exists():
        return None

    # Update DB so next downloads are instant and sizes are computed correctly.
    try:
        resolved_zip = str(best.resolve())
        resolved_dir = str(best.parent.resolve())
    except OSError:
        resolved_zip = str(best)
        resolved_dir = str(best.parent)

    conn.execute("UPDATE task_runs SET output_zip = ?, output_dir = ? WHERE id = ? AND user_id = ?", (resolved_zip, resolved_dir, run_id, user_id))
    conn.commit()
    return best


def resolve_run_log_path(conn: sqlite3.Connection, user_id: int, username: str, run_id: int, stored_output_log_file: str | None, stored_output_dir: str | None, module: str | None, tool_version: str | None, finished_at_raw: str | None) -> Path | None:
    """Best-effort locate the log file for a run even if output folders were renamed manually."""
    log_path = Path(stored_output_log_file) if stored_output_log_file else None
    if log_path and log_path.exists():
        return log_path

    user_root = OUTPUTS_DIR / sanitize_component(username)
    if not user_root.exists() or not user_root.is_dir():
        return None

    candidates: list[Path] = []

    # 1) If we have a stored log filename, search it under the user's outputs root.
    if log_path is not None:
        log_name = log_path.name
        try:
            for found in user_root.rglob(log_name):
                if found.is_file() and is_safe_path(OUTPUTS_DIR, found):
                    candidates.append(found)
        except OSError:
            candidates = []

    # 2) Fallback: if output_dir still exists, use the newest *.log inside it.
    if not candidates and stored_output_dir:
        out_dir = Path(stored_output_dir)
        if out_dir.exists() and out_dir.is_dir():
            try:
                local_logs = [p for p in out_dir.glob("*.log") if p.is_file() and is_safe_path(OUTPUTS_DIR, p)]
                if local_logs:
                    candidates.extend(local_logs)
            except OSError:
                candidates = []

    # 3) Last fallback: search by common RetuningAutomation log naming.
    if not candidates:
        ver = sanitize_component(tool_version or "")
        patterns = [f"RetuningAutomation_*_v{ver}.log"] if ver else []
        patterns.extend(["RetuningAutomation_*.log", "*.log"])
        try:
            for pattern in patterns:
                for found in user_root.rglob(pattern):
                    if found.is_file() and is_safe_path(OUTPUTS_DIR, found):
                        candidates.append(found)
        except OSError:
            candidates = []

    if not candidates:
        return None

    target_dt = parse_iso_datetime(finished_at_raw) if finished_at_raw else None
    best: Path
    if target_dt:
        target_ts = target_dt.timestamp()
        try:
            best = min(candidates, key=lambda p: abs(p.stat().st_mtime - target_ts))
        except OSError:
            best = max(candidates, key=lambda p: p.stat().st_mtime, default=candidates[0])
    else:
        best = max(candidates, key=lambda p: p.stat().st_mtime)

    if not best.exists():
        return None

    # Update DB so next downloads are instant.
    try:
        resolved_log = str(best.resolve())
        resolved_dir = str(best.parent.resolve())
    except OSError:
        resolved_log = str(best)
        resolved_dir = str(best.parent)

    conn.execute("UPDATE task_runs SET output_log_file = ?, output_dir = ? WHERE id = ? AND user_id = ?", (resolved_log, resolved_dir, run_id, user_id))
    conn.commit()
    return best


def resolve_queue_task_output_dir(stored_output_dir: str | None, payload_json: str | None) -> Path | None:
    """Resolve queue output folder safely for queued/running tasks.

    For non-finished tasks we should only delete their dedicated queue_task_* folder,
    never fallback-scan by module/version because that can match completed runs.
    """
    candidate = Path(stored_output_dir) if stored_output_dir else None
    if not (candidate and candidate.name.startswith("queue_task_")):
        try:
            payload = json.loads(payload_json or "{}")
        except json.JSONDecodeError:
            payload = {}
        output_value = str(payload.get("output") or "").strip()
        if output_value:
            candidate = Path(output_value)

    if not candidate:
        return None
    if not candidate.name.startswith("queue_task_"):
        return None
    if not (candidate.exists() and candidate.is_dir()):
        return None
    if not is_safe_path(OUTPUTS_DIR, candidate):
        return None
    return candidate


def resolve_strict_run_artifacts_for_deletion(stored_output_dir: str | None, stored_output_zip: str | None, stored_output_log_file: str | None, payload_json: str | None) -> tuple[Path | None, Path | None, Path | None]:
    """Resolve run artifacts using only explicitly stored paths.

    Deletion operations must never perform heuristic discovery because that can
    match artifacts belonging to a different execution.
    """
    output_dir: Path | None = None
    output_zip: Path | None = None
    output_log: Path | None = None

    queue_output_dir = resolve_queue_task_output_dir(stored_output_dir, payload_json)
    if queue_output_dir:
        output_dir = queue_output_dir

    if output_dir is None and stored_output_dir:
        candidate = Path(stored_output_dir)
        if candidate.exists() and candidate.is_dir() and is_safe_path(OUTPUTS_DIR, candidate):
            output_dir = candidate

    if stored_output_zip:
        zip_candidate = Path(stored_output_zip)
        if zip_candidate.exists() and zip_candidate.is_file() and is_safe_path(OUTPUTS_DIR, zip_candidate):
            output_zip = zip_candidate

    if stored_output_log_file:
        log_candidate = Path(stored_output_log_file)
        if log_candidate.exists() and log_candidate.is_file() and is_safe_path(OUTPUTS_DIR, log_candidate):
            output_log = log_candidate

    return output_dir, output_zip, output_log


def resolve_run_output_dir_path(conn: sqlite3.Connection, user_id: int, username: str, run_id: int, stored_output_dir: str | None, stored_output_zip: str | None, stored_output_log_file: str | None, module: str | None, tool_version: str | None, finished_at_raw: str | None) -> Path | None:
    """Best-effort locate the output directory for a run even if output folders were renamed manually."""
    out_dir = Path(stored_output_dir) if stored_output_dir else None
    if out_dir and out_dir.exists() and out_dir.is_dir() and is_safe_path(OUTPUTS_DIR, out_dir):
        return out_dir

    zip_path = resolve_run_zip_path(conn, user_id, username, run_id, stored_output_zip, stored_output_dir, module, tool_version, finished_at_raw)
    if zip_path and zip_path.exists() and is_safe_path(OUTPUTS_DIR, zip_path):
        out_dir = zip_path.parent

    if not (out_dir and out_dir.exists() and out_dir.is_dir() and is_safe_path(OUTPUTS_DIR, out_dir)):
        log_path = resolve_run_log_path(conn, user_id, username, run_id, stored_output_log_file, stored_output_dir, module, tool_version, finished_at_raw)
        if log_path and log_path.exists() and is_safe_path(OUTPUTS_DIR, log_path):
            out_dir = log_path.parent

    if out_dir and out_dir.exists() and out_dir.is_dir() and is_safe_path(OUTPUTS_DIR, out_dir):
        try:
            resolved_dir = str(out_dir.resolve())
        except OSError:
            resolved_dir = str(out_dir)
        conn.execute("UPDATE task_runs SET output_dir = ? WHERE id = ? AND user_id = ?", (resolved_dir, run_id, user_id))
        conn.commit()
        return out_dir

    return None

###############
### OPENAPI ###
###############
OPENAPI_TAGS = [
    {"name": "System", "description": "Service health and system-level diagnostics."},
    {"name": "Documentation", "description": "Access API-adjacent product documentation resources."},
    {"name": "Authentication", "description": "Session and account operations."},
    {"name": "Configuration", "description": "Read, update, and export persisted tool configuration."},
    {"name": "Inputs", "description": "Upload and manage reusable input datasets."},
    {"name": "Execution", "description": "Start and manage execution workflows."},
    {"name": "Logs", "description": "Manage system and execution Logs."},
    {"name": "Administration", "description": "Administrative endpoints."},
]


app = FastAPI(
    title="SSB Retuning Automations Web Interface",
    openapi_tags=OPENAPI_TAGS,
    docs_url="/api",
)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(title=app.title, version="1.1.0", routes=app.routes, tags=OPENAPI_TAGS)
    if "paths" in schema:
        sorted_paths = sorted(schema["paths"].items(), key=lambda item: (item[0].startswith("/admin"), item[0]))
        schema["paths"] = dict(sorted_paths)
    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = custom_openapi
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
app.mount("/assets", StaticFiles(directory=str(PROJECT_ROOT / "assets")), name="assets")
templates = Jinja2Templates(directory=str(BASE_DIR / "html"))


@app.middleware("http")
async def access_log_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    client = get_client_ip(request)
    api_logger.info(
        '[%s] - "%s %s" status=%s duration_ms=%.2f',
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
    sanitize_all_user_settings_rows()
    recover_incomplete_tasks()
    ensure_worker_started()
    ensure_backup_worker_started()
    queue_event.set()


# ========= System Section ===========

@app.get("/healthz", tags=["System"])
def healthz():
    try:
        conn = get_conn()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return {"status": "ok"}
    except sqlite3.Error as exc:
        logger.exception("Health check failed: %s", exc)
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(exc)})


# ========= Documentation Section ===========
@app.get("/release-notes", response_class=HTMLResponse, tags=["Documentation"])
async def release_notes(request: Request, user=Depends(get_current_user)):
    # Serve CHANGELOG.md from the repo mounted at /app and render it client-side
    changelog_path = Path("/app/CHANGELOG.md")
    changelog_md = changelog_path.read_text(encoding="utf-8", errors="replace") if changelog_path.exists() else "CHANGELOG.md not found at /app/CHANGELOG.md"
    tool_meta = load_tool_metadata()
    return templates.TemplateResponse("release_notes.html", {"request": request, "tool_meta": tool_meta, "user": user, "changelog_md": changelog_md})


@app.get("/documentation/user-guide/{file_format}", tags=["Documentation"])
def download_user_guide(request: Request, file_format: str, mode: str = "download"):
    def get_latest_user_guide_file(extension: str) -> Path | None:
        normalized_ext = extension.strip().lstrip(".").lower()
        if normalized_ext not in {"md", "docx", "pptx", "pdf"}:
            return None
        pattern = f"User-Guide-*.{normalized_ext}"
        candidates = sorted((path for path in HELP_DIR.glob(pattern) if path.is_file()), key=lambda path: path.stat().st_mtime, reverse=True)
        return candidates[0] if candidates else None

    try:
        require_user(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    guide_path = get_latest_user_guide_file(file_format)
    if not guide_path or not guide_path.exists():
        return PlainTextResponse("User guide not found.", status_code=404)
    normalized_format = file_format.strip().lower()
    normalized_mode = (mode or "download").strip().lower()

    if normalized_mode == "view" and normalized_format == "md":
        md_text = guide_path.read_text(encoding="utf-8", errors="ignore")
        if importlib.util.find_spec("markdown") is None:
            return FileResponse(guide_path, filename=guide_path.name)
        markdown_module = importlib.import_module("markdown")
        def _normalize_markdown_lists(text: str) -> str:
            lines = text.splitlines()
            out = []
            in_fence = False
            fence_marker = None

            def is_list_line(s: str) -> bool:
                return bool(re.match(r"^\s*(?:[-*+]|(?:\d+\.))\s+", s))

            def is_block_boundary(s: str) -> bool:
                t = s.strip()
                if not t:
                    return True
                if t.startswith(("#", ">", "|")):
                    return True
                return False

            def last_nonempty(lst: list[str]) -> str:
                for prev in reversed(lst):
                    if prev.strip():
                        return prev
                return ""

            for i, line in enumerate(lines):
                stripped = line.strip()

                if stripped.startswith("```") or stripped.startswith("~~~"):
                    if in_fence:
                        in_fence = False
                        fence_marker = None
                    else:
                        in_fence = True
                        fence_marker = stripped[:3]
                    out.append(line)
                    continue

                if in_fence:
                    out.append(line)
                    continue

                if is_list_line(stripped):
                    prev = out[-1] if out else ""
                    if prev.strip() and (not is_list_line(prev.strip())) and (not is_block_boundary(prev)):
                        out.append("")
                    # Normalize sublist indentation: markdown expects 4 spaces or a tab.
                    if re.match(r"^\s{2}(?:[-*+]|(?:\d+\.))\s+", line):
                        parent = last_nonempty(out[:-1])
                        if is_list_line(parent.strip()):
                            line = " " * 4 + line.lstrip()
                    out.append(line)
                    continue

                out.append(line)

            return "\n".join(out)

        md_text = _normalize_markdown_lists(md_text)
        md_text = re.sub(r"^\s*<!--.*?-->\s*$", "", md_text, flags=re.MULTILINE)
        md_text = re.sub(r"\]\(\.{2}/assets/", "](/assets/", md_text)
        md_text = re.sub(r"\]\(assets/", "](/assets/", md_text)
        tool_meta = load_tool_metadata()
        guide_version = tool_meta.get("version", "unknown")
        if guide_version == "unknown":
            filename_match = re.search(r"-v([\d.]+)\.md$", guide_path.name)
            if filename_match:
                guide_version = filename_match.group(1)

        base_guide_title = "Technical User Guide — SSB Retuning Automations"
        if guide_version and guide_version != "unknown":
            rendered_guide_title = f"{base_guide_title} v{guide_version}"
            md_text = re.sub(
                rf"^(\s*#\s*){re.escape(base_guide_title)}\s*$",
                rf"\1{rendered_guide_title}",
                md_text,
                count=1,
                flags=re.MULTILINE,
            )
        else:
            rendered_guide_title = base_guide_title

        html_body = markdown_module.markdown(md_text, extensions=["tables", "fenced_code", "sane_lists"])
        guide_title = rendered_guide_title
        html_doc = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{guide_title}</title>
  <link rel="icon" type="image/png" href="/assets/logos/logo_02.png" />
  <link rel="shortcut icon" type="image/png" href="/assets/logos/logo_02.png" />
  <style>
    :root {{ color-scheme: light; }}
    body {{
      margin: 0;
      background: #f6f8fa;
      color: #1f2328;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans", Helvetica, Arial, sans-serif;
      line-height: 1.5;
      font-size: 16px;
    }}
    .markdown-body {{
      box-sizing: border-box;
      max-width: 980px;
      margin: 2rem auto;
      padding: 2rem 2.5rem;
      background: #ffffff;
      border: 1px solid #d0d7de;
      border-radius: 8px;
      box-shadow: 0 1px 2px rgba(31, 35, 40, 0.04);
    }}
    .markdown-body h1,
    .markdown-body h2 {{ border-bottom: 1px solid #d8dee4; padding-bottom: .3em; }}
    .markdown-body code {{
      background: rgba(175, 184, 193, 0.2);
      padding: .2em .4em;
      border-radius: 6px;
      font-size: 85%;
      font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, "Liberation Mono", monospace;
    }}
    .markdown-body pre {{
      background: #f6f8fa;
      color: #1f2328;
      padding: 1rem;
      border: 1px solid #d0d7de;
      border-radius: 6px;
      overflow-x: auto;
    }}
    .markdown-body pre code {{ background: transparent; padding: 0; font-size: 100%; }}
    .markdown-body table {{ border-collapse: collapse; width: 100%; margin: .75rem 0; display: block; overflow-x: auto; }}
    .markdown-body th,
    .markdown-body td {{ border: 1px solid #d0d7de; padding: .45rem .8rem; text-align: left; vertical-align: top; }}
    .markdown-body th {{ background: #f6f8fa; font-weight: 600; }}
    .markdown-body a {{ color: #0969da; text-decoration: none; }}
    .markdown-body a:hover {{ text-decoration: underline; }}
    .markdown-body img {{ max-width: 100%; height: auto; }}
    .markdown-body blockquote {{ margin: 0; padding: 0 1em; color: #59636e; border-left: .25em solid #d0d7de; }}
    .markdown-body hr {{ border: 0; border-top: 1px solid #d8dee4; margin: 1.5rem 0; }}
    @media (max-width: 768px) {{
      .markdown-body {{ margin: 1rem; padding: 1rem; }}
    }}
  </style>
</head>
<body>
<article class="markdown-body">
{html_body}
</article>
</body>
</html>
"""
        return HTMLResponse(html_doc)

    if normalized_mode == "view" and normalized_format == "pdf":
        return FileResponse(
            guide_path,
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{guide_path.name}"'},
        )

    return FileResponse(guide_path, filename=guide_path.name)




# ========= Authentication Section ===========

def list_request_access_admin_contacts() -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT username
        FROM users
        WHERE role = 'admin' AND LOWER(username) != 'admin'
        ORDER BY username COLLATE NOCASE ASC
        """
    ).fetchall()
    conn.close()
    return [str(row["username"]) for row in rows if row["username"]]


@app.get("/login", response_class=HTMLResponse, tags=["Authentication"])
def login_get(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": "",
            "request_access_success": False,
            "request_access_message": "",
            "request_access_admin_contacts": list_request_access_admin_contacts(),
            "request_access_reason": "",
            "request_access_username": "",
        },
    )


@app.post("/login", response_class=HTMLResponse, tags=["Authentication"])
def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    conn = get_conn()
    user = conn.execute(
        "SELECT * FROM users WHERE LOWER(username) = LOWER(?) AND active = 1", (username.strip(),)
    ).fetchone()
    try:
        verified = user and pwd_context.verify(password, user["password_hash"])
    except (ValueError, TypeError):
        verified = False
    if not verified:
        web_access_logger.warning(
            "login failed user=%s ip=%s",
            username.strip() or "unknown",
            get_client_ip(request),
        )
        conn.close()
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Invalid credentials or disabled user.",
                "request_access_success": False,
                "request_access_message": "",
                "request_access_admin_contacts": list_request_access_admin_contacts(),
                "request_access_reason": "",
                "request_access_username": "",
            },
            status_code=401,
        )

    token = secrets.token_urlsafe(32)
    conn.execute(
        "INSERT INTO sessions(token, user_id, created_at, last_seen_at, active) VALUES (?, ?, ?, ?, 1)",
        (token, user["id"], now_iso(), now_iso()),
    )
    conn.commit()
    conn.close()

    web_access_logger.info(
        "login success user=%s ip=%s",
        user["username"],
        get_client_ip(request),
    )
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie("session_token", token, httponly=True, samesite="lax")
    return response

@app.get("/logout", tags=["Authentication"])
def logout(request: Request):
    token = request.cookies.get("session_token")
    if token:
        conn = get_conn()
        session = conn.execute(
            """
            SELECT u.username
            FROM sessions s
            LEFT JOIN users u ON u.id = s.user_id
            WHERE s.token = ?
            """,
            (token,),
        ).fetchone()
        conn.execute("UPDATE sessions SET active = 0 WHERE token = ?", (token,))
        conn.commit()
        conn.close()
        web_access_logger.info(
            "logout user=%s ip=%s reason=manual",
            session["username"] if session and session["username"] else "unknown",
            get_client_ip(request),
        )
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("session_token")
    return response


@app.post("/request-access", response_class=HTMLResponse, tags=["Authentication"])
def request_access_post(request: Request, username: str = Form(...), password: str = Form(...), reason: str = Form(...)):
    requested_username = username.strip()
    requested_password = password.strip()
    request_reason = reason.strip()

    if not requested_username or not requested_password or not request_reason:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "All request access fields are required.",
                "request_access_success": False,
                "request_access_message": "",
                "request_access_admin_contacts": list_request_access_admin_contacts(),
                "request_access_reason": request_reason,
                "request_access_username": requested_username,
            },
            status_code=400,
        )

    signum_username = requested_username.upper()

    conn = get_conn()
    existing_user = conn.execute(
        "SELECT id FROM users WHERE LOWER(username) = LOWER(?)",
        (signum_username,),
    ).fetchone()
    if existing_user:
        conn.close()
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "This username already exists. Please contact an administrator.",
                "request_access_success": False,
                "request_access_message": "",
                "request_access_admin_contacts": list_request_access_admin_contacts(),
                "request_access_reason": request_reason,
                "request_access_username": signum_username,
            },
            status_code=409,
        )

    conn.execute(
        "INSERT INTO users(username, password_hash, role, active, created_at, access_request_reason) VALUES (?, ?, 'user', 0, ?, ?)",
        (signum_username, pwd_context.hash(requested_password), now_iso(), request_reason),
    )
    conn.commit()
    conn.close()

    web_access_logger.info(
        "request access user=%s ip=%s reason=%s",
        signum_username,
        get_client_ip(request),
        request_reason,
    )

    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": "",
            "request_access_success": True,
            "request_access_message": (
                "Your user has been created and is currently inactive. "
                "Please email any administrator below and include your reason to request access."
            ),
            "request_access_admin_contacts": list_request_access_admin_contacts(),
            "request_access_reason": request_reason,
            "request_access_username": signum_username,
        },
        status_code=201,
    )


@app.post("/account/change_password", tags=["Authentication"])
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


@app.post("/settings/update", tags=["Configuration"])
async def settings_update(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    incoming_payload = await request.json()
    existing_settings = load_user_settings(user["id"])
    payload = build_settings_with_module_inputs(existing_settings, incoming_payload)
    save_user_settings(user["id"], payload)
    save_global_runtime_form_settings(incoming_payload)
    module_value = payload.get("module") or "configuration-audit"
    persist_settings_to_config(module_value, payload)
    return {"ok": True}


@app.get("/config/load", tags=["Configuration"])
def load_config():
    return load_persistent_config()


@app.get("/config/export", tags=["Configuration"])
def export_config():
    if CONFIG_PATH.exists():
        return FileResponse(CONFIG_PATH, filename="config.cfg")
    return PlainTextResponse("", media_type="text/plain")


@app.get("/system-config/export", tags=["Configuration"])
def export_system_config():
    payload = build_system_config_payload()
    return JSONResponse(content=payload, headers=NO_CACHE_HEADERS)


@app.post("/system-config/apply", tags=["Configuration"])
async def apply_system_config(request: Request):
    try:
        require_user(request)
    except PermissionError:
        return JSONResponse({"ok": False}, status_code=401)

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    try:
        updated_config = apply_system_config_payload(payload)
    except ValueError:
        return JSONResponse({"ok": False, "error": "invalid_payload"}, status_code=400)

    return JSONResponse({CONFIG_SECTION: updated_config}, headers=NO_CACHE_HEADERS)





@app.post("/inputs/upload", tags=["Inputs"])
async def inputs_upload(
    request: Request,
    module: str = Form(...),
    kind: str = Form(...),
    session_id: str = Form(""),
    input_name: str = Form(""),
    parent_folder_name: str = Form(""),
    overwrite: str = Form("0"),
    files: list[UploadFile] = File(...),
):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False, "error": "auth"}

    if not files:
        return {"ok": False, "error": "invalid_file"}

    inferred_parent = sanitize_component(parent_folder_name) if parent_folder_name.strip() else infer_parent_folder_name(files)
    requested_name = sanitize_component(input_name) if input_name.strip() else inferred_parent
    if not requested_name:
        requested_name = inferred_parent or "uploaded_input"

    target_dir = INPUTS_REPOSITORY_DIR / requested_name
    zip_target = target_dir / f"{requested_name}.zip"
    should_overwrite = parse_bool(overwrite)
    if target_dir.exists() and not should_overwrite:
        return {"ok": False, "error": "already_exists", "existing_name": requested_name}

    if target_dir.exists() and should_overwrite:
        shutil.rmtree(target_dir, ignore_errors=True)

    accepted_files: list[UploadFile] = []
    for upload in files:
        filename = upload.filename or ""
        lower_name = filename.lower()
        if lower_name.endswith((".zip", ".log", ".logs", ".txt")):
            accepted_files.append(upload)

    if not accepted_files:
        return {"ok": False, "error": "invalid_file"}

    target_dir.mkdir(parents=True, exist_ok=True)

    # Keep already compressed uploads as-is to avoid unnecessary recompression,
    # but normalize the zip filename to <input_name>.zip.
    if len(accepted_files) == 1 and (accepted_files[0].filename or "").lower().endswith(".zip"):
        source_upload = accepted_files[0]
        with zip_target.open("wb") as buffer:
            while True:
                chunk = await source_upload.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)
    else:
        with zipfile.ZipFile(zip_target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for upload in accepted_files:
                raw_name = (upload.filename or "uploaded_input").replace("\\", "/").strip("/")
                path_parts = [sanitize_component(part) for part in raw_name.split("/") if part and part not in {".", ".."}]
                if len(path_parts) > 1 and path_parts[0] == requested_name:
                    zip_member = "/".join(path_parts[1:])
                elif len(path_parts) > 1:
                    zip_member = "/".join(path_parts)
                else:
                    zip_member = path_parts[0] if path_parts else "uploaded_input.log"
                raw_bytes = bytearray()
                while True:
                    chunk = await upload.read(1024 * 1024)
                    if not chunk:
                        break
                    raw_bytes.extend(chunk)
                zf.writestr(zip_member, bytes(raw_bytes))

    size_bytes = compute_path_size(target_dir)
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO inputs_repository(user_id, input_name, input_path, uploaded_at, size_bytes)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(input_name) DO UPDATE SET
            user_id = excluded.user_id,
            input_path = excluded.input_path,
            uploaded_at = excluded.uploaded_at,
            size_bytes = excluded.size_bytes
        """,
        (user["id"], requested_name, str(target_dir), now_iso(), size_bytes),
    )
    conn.commit()
    conn.close()

    return {"ok": True, "path": str(target_dir), "input_name": requested_name}


@app.get("/inputs/list", tags=["Inputs"])
def inputs_list(request: Request):
    try:
        require_user(request)
    except PermissionError:
        return {"ok": False, "items": [], "total_size": format_mb(0)}
    return {"ok": True, "items": list_inputs_repository(), "total_size": get_inputs_repository_total_size(), "uploaders": list_inputs_uploaders()}


@app.post("/inputs/delete", tags=["Inputs"])
async def inputs_delete(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    input_ids = payload.get("ids", [])
    if not input_ids:
        return {"ok": False}

    conn = get_conn()
    rows = conn.execute(
        "SELECT id, user_id, input_path FROM inputs_repository WHERE id IN (%s)" % ",".join("?" for _ in input_ids),
        tuple(input_ids),
    ).fetchall()
    denied_count = 0
    for row in rows:
        if row["user_id"] != user["id"]:
            denied_count += 1
            continue
        input_path = Path(row["input_path"])
        if is_safe_path(INPUTS_REPOSITORY_DIR, input_path):
            if input_path.is_dir():
                shutil.rmtree(input_path, ignore_errors=True)
            elif input_path.exists():
                input_path.unlink(missing_ok=True)
        conn.execute("DELETE FROM inputs_repository WHERE id = ?", (row["id"],))
    conn.commit()
    conn.close()
    if denied_count:
        return {
            "ok": False,
            "error": "forbidden_inputs",
            "message": "Only inputs uploaded by your own user can be deleted from this panel.",
        }
    return {"ok": True}



def inputs_rename_common(user_id: int | None, rename_items: list[dict[str, Any]]) -> dict[str, Any]:
    valid_items: list[tuple[int, str]] = []
    for item in rename_items:
        raw_id = item.get("id") if isinstance(item, dict) else None
        raw_name = item.get("new_name") if isinstance(item, dict) else None
        if raw_id is None or raw_name is None:
            continue
        if not str(raw_id).isdigit():
            continue
        sanitized_name = sanitize_component(str(raw_name))
        if not sanitized_name:
            continue
        valid_items.append((int(raw_id), sanitized_name))

    if not valid_items:
        return {"ok": False, "error": "no_valid_items"}

    requested_by_id: dict[int, str] = {item_id: new_name for item_id, new_name in valid_items}
    ids = list(requested_by_id.keys())

    conn = get_conn()
    rows = conn.execute(
        "SELECT id, user_id, input_name, input_path FROM inputs_repository WHERE id IN (%s)" % ",".join("?" for _ in ids),
        tuple(ids),
    ).fetchall()

    denied_count = 0
    renamed_count = 0
    conflict_names: list[str] = []

    for row in rows:
        if user_id is not None and int(row["user_id"]) != int(user_id):
            denied_count += 1
            continue

        input_id = int(row["id"])
        old_name = str(row["input_name"] or "")
        new_name = requested_by_id.get(input_id, old_name)
        if not new_name or new_name == old_name:
            continue

        source_path = Path(row["input_path"] or "")
        target_path = INPUTS_REPOSITORY_DIR / new_name

        if target_path.exists():
            same_target = False
            try:
                same_target = source_path.exists() and source_path.resolve() == target_path.resolve()
            except OSError:
                same_target = str(source_path) == str(target_path)
            if not same_target:
                conflict_names.append(new_name)
                continue

        if source_path.exists() and is_safe_path(INPUTS_REPOSITORY_DIR, source_path):
            try:
                source_path.rename(target_path)
            except OSError:
                try:
                    shutil.move(str(source_path), str(target_path))
                except OSError:
                    conflict_names.append(new_name)
                    continue

        conn.execute(
            "UPDATE inputs_repository SET input_name = ?, input_path = ? WHERE id = ?",
            (new_name, str(target_path), input_id),
        )
        renamed_count += 1

    conn.commit()
    conn.close()

    if denied_count:
        return {
            "ok": False,
            "error": "forbidden_inputs",
            "message": "Only inputs uploaded by your own user can be renamed from this panel.",
            "renamed": renamed_count,
            "conflicts": conflict_names,
        }
    if conflict_names:
        return {
            "ok": False,
            "error": "name_conflict",
            "message": "Some input names already exist and could not be renamed.",
            "renamed": renamed_count,
            "conflicts": conflict_names,
        }
    return {"ok": True, "renamed": renamed_count}


@app.post("/inputs/rename", tags=["Inputs"])
async def inputs_rename(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    items = payload.get("items", []) if isinstance(payload, dict) else []
    return inputs_rename_common(int(user["id"]), items)



# ========= Execution Section ===========

def serialize_run_row(row: dict[str, Any], run_sizes: dict[int, int]) -> dict[str, Any]:
    data = dict(row)
    started_at_raw = data.get("started_at")
    data["started_at_raw"] = started_at_raw
    started_at_epoch = 0
    if started_at_raw:
        try:
            started_at_epoch = int(datetime.fromisoformat(started_at_raw).timestamp())
        except ValueError:
            started_at_epoch = 0
    data["started_at_epoch"] = started_at_epoch
    data["started_at"] = format_timestamp(data.get("started_at"))
    data["finished_at"] = format_timestamp(data.get("finished_at"))
    data["duration_hms"] = format_seconds_hms(data.get("duration_seconds"))
    raw_status = (data.get("status") or "").strip().lower()
    data["status_lower"] = "success" if raw_status == "ok" else raw_status
    data["status_display"] = format_task_status(data.get("status") or "")
    data["run_label"] = f"#{data.get('id')} - {data.get('module') or '—'} - {data.get('started_at') or '—'}"
    data["size_mb"] = format_mb(run_sizes.get(int(data.get("id") or 0), 0))
    return data



@app.get("/", response_class=HTMLResponse, tags=["Execution"])
def index(request: Request):
    user = get_current_user(request)
    if user is None:
        return RedirectResponse("/login", status_code=302)

    config_values = load_persistent_config()
    user_settings = load_user_settings(user["id"])
    module_value = user_settings.get("module") or "configuration-audit"
    settings = build_settings_defaults(module_value, config_values)
    admin_settings = get_admin_settings()
    for key in GLOBAL_RUNTIME_FORM_KEYS:
        settings[key] = str(admin_settings.get(key, settings.get(key, "")) or "")
    tool_meta = load_tool_metadata()
    network_frequencies = load_network_frequencies()
    settings.update(user_settings)
    settings["module"] = module_value
    for key in GLOBAL_RUNTIME_FORM_KEYS:
        settings[key] = str(admin_settings.get(key, settings.get(key, "")) or "")
    module_inputs_map = normalize_module_inputs_map(settings.get("module_inputs_map"))
    current_module_inputs = module_inputs_map.get(module_value, {})
    settings["input"] = str(current_module_inputs.get("input", settings.get("input", "")) or "")
    settings["input_pre"] = str(current_module_inputs.get("input_pre", settings.get("input_pre", "")) or "")
    settings["input_post"] = str(current_module_inputs.get("input_post", settings.get("input_post", "")) or "")
    settings["module_inputs_map"] = module_inputs_map
    cleanup_stale_runs_for_user(user["id"])

    conn = get_conn()
    latest_runs = conn.execute(
        """
        SELECT tr.id, tr.user_id, u.username, tr.module, tr.tool_version, tr.input_name, tr.status, tr.started_at, tr.finished_at, tr.duration_seconds, tr.output_zip, tr.output_log_file
        FROM task_runs tr
        JOIN users u ON u.id = tr.user_id
        ORDER BY tr.id DESC
        """
    ).fetchall()
    latest_runs = [dict(row) for row in latest_runs]

    all_runs = conn.execute("SELECT id, status, input_dir, output_dir, output_zip FROM task_runs").fetchall()
    conn.close()

    run_sizes, total_bytes = compute_runs_size(all_runs)

    latest_runs = [serialize_run_row(row, run_sizes) for row in latest_runs]

    total_size_mb = format_mb(total_bytes)
    input_items = list_inputs_repository()
    input_uploaders = list_inputs_uploaders()
    inputs_total_size = get_inputs_repository_total_size()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": user,
            "module_options": MODULE_OPTIONS,
            "settings": settings,
            "module_inputs_map": module_inputs_map,
            "latest_runs": latest_runs,
            "tool_meta": tool_meta,
            "network_frequencies": network_frequencies,
            "total_runs_size": total_size_mb,
            "input_items": input_items,
            "input_uploaders": input_uploaders,
            "inputs_total_size": inputs_total_size,
            "runs_users": sorted({user["username"], *{str(row.get("username") or "") for row in latest_runs if row.get("username")}}),
        },
    )



@app.post("/run", tags=["Execution"])
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
    output: str = Form(""),
    network_frequencies: str = Form(""),
    profiles_audit: str | None = Form(None),
    frequency_audit: str | None = Form(None),
    export_correction_cmd: str | None = Form(None),
    fast_excel_export: str | None = Form(None),
    selected_inputs_single: str = Form(""),
    selected_inputs_pre: str = Form(""),
    selected_inputs_post: str = Form(""),
):
    try:
        user = require_user(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    raw_payload = {
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
        "output": (output or str((OUTPUTS_DIR / sanitize_component(user["username"])).resolve())).strip(),
    }
    existing_settings = load_user_settings(user["id"])
    payload = build_settings_with_module_inputs(existing_settings, raw_payload)
    save_user_settings(user["id"], payload)
    save_global_runtime_form_settings(raw_payload)
    persist_settings_to_config(module, payload)
    if module in WEB_INTERFACE_BLOCKED_MODULES:
        logger.info("Blocked module run from web interface: %s", module)
        return RedirectResponse("/", status_code=302)

    tool_meta = load_tool_metadata()
    tool_version = tool_meta.get("version", "unknown")

    def parse_selected(raw: str) -> list[str]:
        return [item.strip() for item in (raw or "").split("|") if item.strip()]

    queue_payloads = [dict(raw_payload)]
    selected_single = parse_selected(selected_inputs_single)
    selected_pre = parse_selected(selected_inputs_pre)
    selected_post = parse_selected(selected_inputs_post)

    if module != "consistency-check" and len(selected_single) > 1:
        queue_payloads = []
        for selected_input in selected_single:
            payload_copy = dict(raw_payload)
            payload_copy["input"] = selected_input
            queue_payloads.append(payload_copy)
    elif module == "consistency-check" and (len(selected_pre) > 1 or len(selected_post) > 1):
        pairable = len(selected_pre) == len(selected_post) or len(selected_pre) == 1 or len(selected_post) == 1
        if pairable:
            queue_payloads = []
            queued_len = max(len(selected_pre), len(selected_post))
            for idx in range(queued_len):
                pre_value = selected_pre[idx] if len(selected_pre) > 1 else (selected_pre[0] if selected_pre else raw_payload.get("input_pre", ""))
                post_value = selected_post[idx] if len(selected_post) > 1 else (selected_post[0] if selected_post else raw_payload.get("input_post", ""))
                payload_copy = dict(raw_payload)
                payload_copy["input_pre"] = pre_value
                payload_copy["input_post"] = post_value
                queue_payloads.append(payload_copy)

    conn = get_conn()
    enqueue_payloads_for_user(conn, user["id"], user["username"], queue_payloads, tool_version)
    conn.commit()
    conn.close()

    if queue_payloads:
        latest_payload = queue_payloads[-1]
        latest_settings_payload = build_settings_with_module_inputs(payload, latest_payload)
        save_user_settings(user["id"], latest_settings_payload)
        persist_settings_to_config(module, latest_settings_payload)

    queue_event.set()
    ensure_worker_started()
    return RedirectResponse("/", status_code=302)



@app.post("/runs/stop", tags=["Execution"])
async def runs_stop(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    raw_run_ids = payload.get("ids", [])
    run_ids = [int(run_id) for run_id in raw_run_ids if str(run_id).isdigit()]
    if not run_ids:
        return {"ok": False}

    conn = get_conn()
    rows = conn.execute(
        "SELECT id, status FROM task_runs WHERE user_id = ? AND id IN (%s)" % ",".join("?" for _ in run_ids),
        (user["id"], *run_ids),
    ).fetchall()

    now_value = now_iso()
    queued_ids: list[int] = []
    running_ids: list[int] = []
    running_without_process_ids: list[int] = []
    for row in rows:
        status = (row["status"] or "").lower()
        if status == "queued":
            queued_ids.append(row["id"])
        elif status == "running":
            running_ids.append(row["id"])

    if queued_ids:
        conn.execute(
            "UPDATE task_runs SET status = 'canceled', finished_at = ?, output_log = TRIM(output_log || '\\nCanceled by user before execution.') WHERE user_id = ? AND id IN (%s)"
            % ",".join("?" for _ in queued_ids),
            (now_value, user["id"], *queued_ids),
        )
    running_with_process_ids: list[int] = []
    with running_processes_lock:
        for task_id in running_ids:
            proc = running_processes.get(task_id)
            if proc and proc.poll() is None:
                running_with_process_ids.append(task_id)
            else:
                running_without_process_ids.append(task_id)

    if running_with_process_ids:
        conn.execute(
            "UPDATE task_runs SET status = 'canceling', output_log = TRIM(output_log || '\\nCancellation requested by user.') WHERE user_id = ? AND id IN (%s)"
            % ",".join("?" for _ in running_with_process_ids),
            (user["id"], *running_with_process_ids),
        )
    if running_without_process_ids:
        conn.execute(
            "UPDATE task_runs SET status = 'canceled', finished_at = ?, output_log = TRIM(output_log || '\\nCanceled by user (no active process found).') WHERE user_id = ? AND id IN (%s)"
            % ",".join("?" for _ in running_without_process_ids),
            (now_value, user["id"], *running_without_process_ids),
        )
    conn.commit()
    conn.close()

    with canceled_task_ids_lock:
        canceled_task_ids.update(running_with_process_ids)
    with running_processes_lock:
        for task_id in running_with_process_ids:
            proc = running_processes.get(task_id)
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                except OSError:
                    pass

    queue_event.set()
    return {
        "ok": True,
        "stopped": len(queued_ids) + len(running_ids),
        "canceled_immediately": len(queued_ids) + len(running_without_process_ids),
    }


@app.post("/runs/rerun", tags=["Execution"])
async def runs_rerun(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    raw_run_ids = payload.get("ids", [])
    run_ids = [int(run_id) for run_id in raw_run_ids if str(run_id).isdigit()]
    if not run_ids:
        return {"ok": False}

    conn = get_conn()
    rows = conn.execute(
        "SELECT id, payload_json FROM task_runs WHERE id IN (%s)" % ",".join("?" for _ in run_ids),
        (*run_ids,),
    ).fetchall()

    run_payload_by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            payload_json = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            payload_json = {}
        run_payload_by_id[int(row["id"])] = dict(payload_json)

    queue_payloads: list[dict[str, Any]] = []
    for run_id in run_ids:
        payload_json = run_payload_by_id.get(run_id)
        if payload_json:
            queue_payloads.append(payload_json)

    if not queue_payloads:
        conn.close()
        return {"ok": False, "queued": 0}

    tool_meta = load_tool_metadata()
    tool_version = tool_meta.get("version", "unknown")
    queued = enqueue_payloads_for_user(conn, user["id"], user["username"], queue_payloads, tool_version, force_user_output_root=True)

    conn.commit()
    conn.close()

    if queued:
        queue_event.set()
        ensure_worker_started()

    return {"ok": True, "queued": queued}



@app.get("/runs/list", tags=["Execution"])
def runs_list(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return JSONResponse({"ok": False, "items": []}, headers=NO_CACHE_HEADERS)

    conn = get_conn()
    latest_runs = conn.execute(
        """
        SELECT tr.id, tr.user_id, u.username, tr.module, tr.tool_version, tr.input_name, tr.status, tr.started_at, tr.finished_at, tr.duration_seconds, tr.output_zip, tr.output_log_file
        FROM task_runs tr
        JOIN users u ON u.id = tr.user_id
        ORDER BY tr.id DESC
        """
    ).fetchall()
    latest_runs = [dict(row) for row in latest_runs]
    all_runs = conn.execute("SELECT id, status, input_dir, output_dir, output_zip FROM task_runs").fetchall()
    conn.close()

    run_sizes, _ = compute_runs_size(all_runs)
    items = [serialize_run_row(row, run_sizes) for row in latest_runs]
    return JSONResponse({"ok": True, "items": items}, headers=NO_CACHE_HEADERS)


@app.get("/runs/{run_id}/download", tags=["Execution"])
def runs_download_output(request: Request, run_id: int):
    try:
        require_user(request)
    except PermissionError:
        return PlainTextResponse("", status_code=403)

    conn = get_conn()
    row = conn.execute(
        "SELECT tr.user_id, u.username, tr.module, tr.tool_version, tr.output_zip, tr.output_dir, tr.finished_at FROM task_runs tr JOIN users u ON u.id = tr.user_id WHERE tr.id = ?",
        (run_id,),
    ).fetchone()
    if not row:
        conn.close()
        return PlainTextResponse("", status_code=404)

    output_zip = resolve_run_zip_path(conn, int(row["user_id"]), row["username"], run_id, row["output_zip"], row["output_dir"], row["module"], row["tool_version"], row["finished_at"])
    conn.close()

    if not output_zip or not output_zip.exists():
        return PlainTextResponse("", status_code=404)

    return FileResponse(output_zip, filename=output_zip.name)



@app.get("/runs/{run_id}/log", tags=["Execution"])
def runs_download_log(request: Request, run_id: int):
    try:
        require_user(request)
    except PermissionError:
        return PlainTextResponse("", status_code=403)

    conn = get_conn()
    row = conn.execute(
        "SELECT tr.user_id, u.username, tr.module, tr.tool_version, tr.output_log_file, tr.output_dir, tr.finished_at FROM task_runs tr JOIN users u ON u.id = tr.user_id WHERE tr.id = ?",
        (run_id,),
    ).fetchone()
    if not row:
        conn.close()
        return PlainTextResponse("", status_code=404)

    log_path = resolve_run_log_path(conn, int(row["user_id"]), row["username"], run_id, row["output_log_file"], row["output_dir"], row["module"], row["tool_version"], row["finished_at"])
    conn.close()

    if not log_path or not log_path.exists():
        return PlainTextResponse("", status_code=404)

    return FileResponse(log_path, filename=log_path.name)



@app.post("/runs/delete", tags=["Execution"])
async def runs_delete(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    raw_run_ids = payload.get("ids", [])
    run_ids = [int(run_id) for run_id in raw_run_ids if str(run_id).isdigit()]
    if not run_ids:
        return {"ok": False}

    conn = get_conn()
    selected_rows = conn.execute(
        "SELECT id, user_id FROM task_runs WHERE id IN (%s)" % ",".join("?" for _ in run_ids),
        tuple(run_ids),
    ).fetchall()
    foreign_run_ids = [int(row["id"]) for row in selected_rows if int(row["user_id"]) != int(user["id"])]
    if foreign_run_ids:
        conn.close()
        return JSONResponse(
            {
                "ok": False,
                "error": "Only executions run by your user can be deleted.",
                "forbidden_ids": foreign_run_ids,
            },
            status_code=403,
            headers=NO_CACHE_HEADERS,
        )

    rows = conn.execute(
        "SELECT id, status, module, tool_version, finished_at, output_dir, output_zip, output_log_file, payload_json FROM task_runs WHERE user_id = ? AND id IN (%s)" % ",".join("?" for _ in run_ids),
        (user["id"], *run_ids),
    ).fetchall()

    for row in rows:
        run_status = (row["status"] or "").strip().lower()

        output_dir, output_zip, output_log = resolve_strict_run_artifacts_for_deletion(
            row["output_dir"],
            row["output_zip"],
            row["output_log_file"],
            row["payload_json"],
        )

        if output_dir and run_status in {"queued", "running", "canceling", "canceled"}:
            shutil.rmtree(output_dir, ignore_errors=True)
            continue

        if output_dir and is_safe_path(OUTPUTS_DIR, output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
            continue

        if output_zip and output_zip.is_file() and is_safe_path(OUTPUTS_DIR, output_zip):
            output_zip.unlink(missing_ok=True)
        if output_log and output_log.is_file() and is_safe_path(OUTPUTS_DIR, output_log):
            output_log.unlink(missing_ok=True)

    conn.execute("DELETE FROM task_runs WHERE user_id = ? AND id IN (%s)" % ",".join("?" for _ in run_ids), (user["id"], *run_ids))
    conn.commit()
    conn.close()

    return {"ok": True}






@app.get("/logs/latest", tags=["Logs"])
def logs_latest(request: Request):
    try:
        user = require_user(request)
    except PermissionError:
        return JSONResponse({"log": ""}, headers=NO_CACHE_HEADERS)

    conn = get_conn()
    row = conn.execute(
        "SELECT output_log FROM task_runs WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (user["id"],),
    ).fetchone()
    conn.close()
    return JSONResponse({"log": row["output_log"] if row else ""}, headers=NO_CACHE_HEADERS)



@app.get("/logs/by_run/{run_id}", tags=["Logs"])
def logs_by_run(request: Request, run_id: int):
    try:
        user = require_user(request)
    except PermissionError:
        return JSONResponse({"log": ""}, headers=NO_CACHE_HEADERS)

    conn = get_conn()
    row = conn.execute(
        "SELECT output_log FROM task_runs WHERE id = ? AND user_id = ?",
        (run_id, user["id"]),
    ).fetchone()
    conn.close()
    return JSONResponse({"log": row["output_log"] if row else ""}, headers=NO_CACHE_HEADERS)



@app.get("/logs/system", tags=["Logs"])
def logs_system(request: Request, source: str = "app"):
    try:
        user = require_user(request)
    except PermissionError:
        return JSONResponse({"log": ""}, headers=NO_CACHE_HEADERS)

    source_key = (source or "app").lower().strip()
    if source_key in {"api", "access"}:
        path = API_LOG_PATH
    elif source_key in {"web-access", "web_access"}:
        if user["role"] != "admin":
            return JSONResponse({"log": ""}, headers=NO_CACHE_HEADERS)
        path = WEB_ACCESS_LOG_PATH
    else:
        path = APP_LOG_PATH

    return JSONResponse({"log": read_tail(path)}, headers=NO_CACHE_HEADERS)



@app.post("/logs/system/delete", tags=["Logs"])
def logs_system_delete(request: Request, source: str = "app"):
    try:
        user = require_admin(request)
    except PermissionError:
        return {"ok": False}

    source_key = (source or "app").lower().strip()
    if source_key in {"api", "access"}:
        path = API_LOG_PATH
    elif source_key in {"web-access", "web_access"}:
        if user["role"] != "admin":
            return {"ok": False}
        path = WEB_ACCESS_LOG_PATH
    else:
        path = APP_LOG_PATH

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    except OSError:
        return {"ok": False}
    return {"ok": True}



# ========= Admin Section ============

@app.post("/admin/inputs/rename", tags=["Administration"])
async def admin_rename_inputs(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    items = payload.get("items", []) if isinstance(payload, dict) else []
    return inputs_rename_common(None, items)


@app.post("/admin/inputs/delete", tags=["Administration"])
async def admin_delete_inputs(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    input_ids = payload.get("ids", [])
    if not input_ids:
        return {"ok": False}

    conn = get_conn()
    rows = conn.execute(
        "SELECT id, input_path FROM inputs_repository WHERE id IN (%s)" % ",".join("?" for _ in input_ids),
        tuple(input_ids),
    ).fetchall()
    for row in rows:
        input_path = Path(row["input_path"])
        if is_safe_path(INPUTS_REPOSITORY_DIR, input_path):
            if input_path.is_dir():
                shutil.rmtree(input_path, ignore_errors=True)
            elif input_path.exists():
                input_path.unlink(missing_ok=True)
    conn.execute("DELETE FROM inputs_repository WHERE id IN (%s)" % ",".join("?" for _ in input_ids), tuple(input_ids))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/admin/logs/by_run/{run_id}", tags=["Administration"])
def admin_logs_by_run(request: Request, run_id: int):
    try:
        require_admin(request)
    except PermissionError:
        return JSONResponse({"log": ""}, headers=NO_CACHE_HEADERS)

    conn = get_conn()
    row = conn.execute("SELECT output_log FROM task_runs WHERE id = ?", (run_id,)).fetchone()
    conn.close()
    return JSONResponse({"log": row["output_log"] if row else ""}, headers=NO_CACHE_HEADERS)


@app.get("/admin/runs/list", tags=["Administration"])
def admin_runs_list(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return JSONResponse({"ok": False, "items": []}, headers=NO_CACHE_HEADERS)

    conn = get_conn()
    latest_runs = conn.execute(
        """
        SELECT tr.id, u.username, tr.module, tr.tool_version, tr.input_name, tr.status, tr.started_at, tr.finished_at, tr.duration_seconds, tr.output_zip, tr.output_log_file, tr.input_dir, tr.output_dir
        FROM task_runs tr
        JOIN users u ON u.id = tr.user_id
        ORDER BY tr.id DESC
        """
    ).fetchall()
    latest_runs = [dict(row) for row in latest_runs]
    run_sizes, _ = compute_runs_size(latest_runs)
    conn.close()

    items = [serialize_run_row(row, run_sizes) for row in latest_runs]
    return JSONResponse({"ok": True, "items": items}, headers=NO_CACHE_HEADERS)


@app.get("/admin/runs/active_status", tags=["Administration"])
def admin_runs_active_status(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return JSONResponse({"ok": False, "has_active_tasks": False}, headers=NO_CACHE_HEADERS)

    conn = get_conn()
    row = conn.execute(
        """
        SELECT COUNT(1) AS active_count
        FROM task_runs
        WHERE LOWER(COALESCE(status, '')) IN ('running', 'queued', 'canceling')
        """
    ).fetchone()
    conn.close()
    active_count = int(row["active_count"] or 0) if row else 0
    return JSONResponse({"ok": True, "has_active_tasks": active_count > 0}, headers=NO_CACHE_HEADERS)


@app.get("/admin/users/connected_status", tags=["Administration"])
def admin_users_connected_status(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return JSONResponse({"ok": False, "connected_users": 0}, headers=NO_CACHE_HEADERS)

    conn = get_conn()
    _, connected_users_count = build_connected_users_snapshot(conn)
    conn.close()
    return JSONResponse({"ok": True, "connected_users": connected_users_count}, headers=NO_CACHE_HEADERS)


@app.post("/admin/runs/stop", tags=["Administration"])
async def admin_stop_runs(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    raw_run_ids = payload.get("ids", [])
    run_ids = [int(run_id) for run_id in raw_run_ids if str(run_id).isdigit()]
    if not run_ids:
        return {"ok": False}

    conn = get_conn()
    rows = conn.execute(
        "SELECT id, status FROM task_runs WHERE id IN (%s)" % ",".join("?" for _ in run_ids),
        tuple(run_ids),
    ).fetchall()

    now_value = now_iso()
    queued_ids: list[int] = []
    running_ids: list[int] = []
    running_without_process_ids: list[int] = []
    for row in rows:
        status = (row["status"] or "").lower()
        if status == "queued":
            queued_ids.append(row["id"])
        elif status == "running":
            running_ids.append(row["id"])

    if queued_ids:
        conn.execute(
            "UPDATE task_runs SET status = 'canceled', finished_at = ?, output_log = TRIM(output_log || '\nCanceled by admin before execution.') WHERE id IN (%s)"
            % ",".join("?" for _ in queued_ids),
            (now_value, *queued_ids),
        )

    running_with_process_ids: list[int] = []
    with running_processes_lock:
        for task_id in running_ids:
            proc = running_processes.get(task_id)
            if proc and proc.poll() is None:
                running_with_process_ids.append(task_id)
            else:
                running_without_process_ids.append(task_id)

    if running_with_process_ids:
        conn.execute(
            "UPDATE task_runs SET status = 'canceling', output_log = TRIM(output_log || '\nCancellation requested by admin.') WHERE id IN (%s)"
            % ",".join("?" for _ in running_with_process_ids),
            tuple(running_with_process_ids),
        )
        with canceled_task_ids_lock:
            canceled_task_ids.update(running_with_process_ids)
        with running_processes_lock:
            for task_id in running_with_process_ids:
                proc = running_processes.get(task_id)
                if proc and proc.poll() is None:
                    try:
                        proc.terminate()
                    except OSError:
                        pass

    if running_without_process_ids:
        conn.execute(
            "UPDATE task_runs SET status = 'canceled', finished_at = ?, output_log = TRIM(output_log || '\nCanceled by admin (process already finished).') WHERE id IN (%s)"
            % ",".join("?" for _ in running_without_process_ids),
            (now_value, *running_without_process_ids),
        )

    conn.commit()
    conn.close()

    if queued_ids:
        queue_event.set()
    return {"ok": True}


@app.post("/admin/runs/rerun", tags=["Administration"])
async def admin_rerun_runs(request: Request):
    try:
        admin = require_admin(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    raw_run_ids = payload.get("ids", [])
    run_ids = [int(run_id) for run_id in raw_run_ids if str(run_id).isdigit()]
    if not run_ids:
        return {"ok": False}

    conn = get_conn()
    rows = conn.execute(
        "SELECT id, payload_json FROM task_runs WHERE id IN (%s)" % ",".join("?" for _ in run_ids),
        tuple(run_ids),
    ).fetchall()

    run_payload_by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            payload_json = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            payload_json = {}
        run_payload_by_id[int(row["id"])] = dict(payload_json)

    queue_payloads: list[dict[str, Any]] = []
    for run_id in run_ids:
        payload_json = run_payload_by_id.get(run_id)
        if payload_json:
            queue_payloads.append(payload_json)

    if not queue_payloads:
        conn.close()
        return {"ok": False, "queued": 0}

    tool_meta = load_tool_metadata()
    tool_version = tool_meta.get("version", "unknown")
    queued = enqueue_payloads_for_user(conn, int(admin["id"]), str(admin["username"]), queue_payloads, tool_version, force_user_output_root=True)

    conn.commit()
    conn.close()

    if queued:
        queue_event.set()
        ensure_worker_started()

    return {"ok": True, "queued": queued}


@app.post("/admin/runs/delete", tags=["Administration"])
async def admin_delete_runs(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    raw_run_ids = payload.get("ids", [])
    run_ids = [int(run_id) for run_id in raw_run_ids if str(run_id).isdigit()]
    if not run_ids:
        return {"ok": False}

    conn = get_conn()
    rows = conn.execute(
        "SELECT tr.id, tr.user_id, u.username, tr.status, tr.module, tr.tool_version, tr.finished_at, tr.output_dir, tr.output_zip, tr.output_log_file, tr.payload_json FROM task_runs tr JOIN users u ON u.id = tr.user_id WHERE tr.id IN (%s)" % ",".join("?" for _ in run_ids),
        tuple(run_ids),
    ).fetchall()

    for row in rows:
        run_status = (row["status"] or "").strip().lower()

        output_dir, output_zip, output_log = resolve_strict_run_artifacts_for_deletion(
            row["output_dir"],
            row["output_zip"],
            row["output_log_file"],
            row["payload_json"],
        )

        if output_dir and run_status in {"queued", "running", "canceling", "canceled"}:
            shutil.rmtree(output_dir, ignore_errors=True)
            continue

        if output_dir and is_safe_path(OUTPUTS_DIR, output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
            continue

        if output_zip and output_zip.is_file() and is_safe_path(OUTPUTS_DIR, output_zip):
            output_zip.unlink(missing_ok=True)
        if output_log and output_log.is_file() and is_safe_path(OUTPUTS_DIR, output_log):
            output_log.unlink(missing_ok=True)

    conn.execute("DELETE FROM task_runs WHERE id IN (%s)" % ",".join("?" for _ in run_ids), tuple(run_ids))
    conn.commit()
    conn.close()

    return {"ok": True}


@app.get("/admin", response_class=HTMLResponse, tags=["Administration"])
def admin_panel(request: Request):
    try:
        admin = require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    conn = get_conn()
    users = conn.execute(
        """
        SELECT u.id, u.username, u.role, u.active, u.created_at, u.access_request_reason,
               MAX(s.last_seen_at) AS last_connection,
               COALESCE(SUM(
                    CASE
                        WHEN s.active = 1 THEN
                            CASE
                                WHEN (julianday('now') - julianday(s.last_seen_at)) * 86400 > ?
                                THEN (julianday(s.last_seen_at) + ? / 86400.0 - julianday(s.created_at)) * 86400
                                ELSE (julianday('now') - julianday(s.created_at)) * 86400
                            END
                        ELSE (julianday(s.last_seen_at) - julianday(s.created_at)) * 86400
                    END
               ), 0) AS total_login_seconds,
               COALESCE((SELECT SUM(duration_seconds) FROM task_runs tr WHERE tr.user_id = u.id), 0) AS total_execution_seconds
        FROM users u
        LEFT JOIN sessions s ON s.user_id = u.id
        GROUP BY u.id
        ORDER BY u.username
        """
        ,
        (SESSION_IDLE_TIMEOUT_SECONDS, SESSION_IDLE_TIMEOUT_SECONDS),
    ).fetchall()
    users = [dict(row) for row in users]
    connected_users, connected_users_count = build_connected_users_snapshot(conn)
    for row in users:
        dirs = get_user_storage_dirs(row["username"])
        uploads_size = compute_dir_size(dirs["uploads"])
        outputs_size = compute_dir_size(dirs["outputs"])
        row["storage_size"] = format_mb(uploads_size + outputs_size)
        row["total_login_hms"] = format_seconds_hms(row.get("total_login_seconds"))
        row["total_execution_hms"] = format_seconds_hms(row.get("total_execution_seconds"))
        row["last_connection"] = format_last_connection(row.get("last_connection"))

        row["connected"] = bool(connected_users.get(row["id"], False))
    recent_runs = conn.execute(
        """
        SELECT tr.id, u.username, tr.module, tr.tool_version, tr.input_name, tr.status, tr.started_at, tr.finished_at, tr.duration_seconds, tr.output_zip, tr.output_log_file, tr.input_dir, tr.output_dir
        FROM task_runs tr
        JOIN users u ON u.id = tr.user_id
        ORDER BY tr.id DESC
        """
    ).fetchall()
    conn.close()

    tool_meta = load_tool_metadata()
    admin_user_settings = load_user_settings(admin["id"])
    run_sizes, total_bytes = compute_runs_size(recent_runs)
    recent_runs = [dict(row) for row in recent_runs]
    for row in recent_runs:
        row["started_at"] = format_timestamp(row["started_at"])
        row["finished_at"] = format_timestamp(row["finished_at"])
        row["duration_hms"] = format_seconds_hms(row.get("duration_seconds"))
        raw_status = (row.get("status") or "").strip().lower()
        row["status_lower"] = "success" if raw_status == "ok" else raw_status
        row["status_display"] = format_task_status(row.get("status") or "")
        row["size_mb"] = format_mb(run_sizes.get(row["id"], 0))

    editable_tables = ["users", "sessions", "task_runs", "inputs_repository", "user_settings", "app_settings"]

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "user": admin,
            "users": users,
            "recent_runs": recent_runs,
            "global_runs_size": format_mb(total_bytes),
            "input_items": list_inputs_repository(),
            "inputs_total_size": get_inputs_repository_total_size(),
            "admin_settings": get_admin_settings(),
            "user_settings": admin_user_settings,
            "tool_meta": tool_meta,
            "editable_tables": editable_tables,
            "connected_users_count": connected_users_count,
        },
    )


@app.post("/admin/settings", tags=["Administration"])
def admin_settings_update(
    request: Request,
    max_cpu_percent: int = Form(MAX_CPU_DEFAULT),
    max_memory_percent: int = Form(MAX_MEMORY_DEFAULT),
    max_parallel_tasks: int = Form(MAX_PARALLEL_DEFAULT),
    db_backup_auto_mode: str = Form("disabled"),
    db_backup_auto_path: str = Form(""),
    db_backup_auto_hour: int = Form(2),
    db_backup_max_to_store: int = Form(30),
):
    try:
        require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)
    current = get_admin_settings()
    save_admin_settings(
        max_cpu_percent,
        max_memory_percent,
        max_parallel_tasks,
        db_backup_auto_mode=db_backup_auto_mode,
        db_backup_auto_path=db_backup_auto_path,
        db_backup_auto_hour=db_backup_auto_hour,
        db_backup_max_to_store=db_backup_max_to_store,
        db_backup_last_run_date=current["db_backup_last_run_date"],
    )
    queue_event.set()
    return RedirectResponse("/admin", status_code=302)


@app.get("/admin/database/export", tags=["Administration"])
def admin_export_database(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    if not DB_PATH.exists():
        return PlainTextResponse("Database file not found.", status_code=404)
    return FileResponse(DB_PATH, filename=f"web_interface_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")


@app.post("/admin/database/import", tags=["Administration"])
async def admin_import_database(request: Request, backup_file: UploadFile = File(...)):
    try:
        require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    suffix = Path(backup_file.filename or "backup.db").suffix or ".db"
    candidate_path: Path | None = None
    try:
        DATA_DB_DIR.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(delete=False, dir=str(DATA_DB_DIR), prefix="db_import_", suffix=suffix) as tmp:
            candidate_path = Path(tmp.name)
            while True:
                chunk = await backup_file.read(1024 * 1024)
                if not chunk:
                    break
                tmp.write(chunk)

        if not candidate_path.exists() or candidate_path.stat().st_size == 0:
            if candidate_path and candidate_path.exists():
                candidate_path.unlink(missing_ok=True)
            return PlainTextResponse("Uploaded backup is empty.", status_code=400)

        conn = sqlite3.connect(candidate_path)
        try:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            required_tables = {"users", "sessions", "task_runs", "inputs_repository"}
            if not required_tables.issubset(tables):
                missing_tables = ", ".join(sorted(required_tables - tables))
                raise ValueError(f"Invalid backup: missing tables: {missing_tables}")
            conn.execute("PRAGMA quick_check")
        finally:
            conn.close()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        previous_backup_path = DATA_DB_DIR / f"web_interface.db.pre_import_{timestamp}.bak"
        if DB_PATH.exists():
            shutil.copy2(DB_PATH, previous_backup_path)

        candidate_path.replace(DB_PATH)
        web_access_logger.info(
            "database import executed by admin ip=%s backup=%s",
            get_client_ip(request),
            backup_file.filename or candidate_path.name,
        )
    except ValueError as exc:
        if candidate_path and candidate_path.exists():
            candidate_path.unlink(missing_ok=True)
        return PlainTextResponse(str(exc), status_code=400)
    except OSError:
        if candidate_path and candidate_path.exists():
            candidate_path.unlink(missing_ok=True)
        return PlainTextResponse("Failed to import database backup.", status_code=500)
    finally:
        await backup_file.close()

    return RedirectResponse("/admin", status_code=302)


@app.get("/admin/database/table_data", tags=["Administration"])
def admin_database_table_data(request: Request, table: str):
    try:
        require_admin(request)
    except PermissionError:
        return JSONResponse({"ok": False, "columns": [], "rows": [], "primary_keys": []}, headers=NO_CACHE_HEADERS)

    allowed_tables = {"users", "sessions", "task_runs", "inputs_repository", "user_settings", "app_settings"}
    if table not in allowed_tables:
        return JSONResponse({"ok": False, "columns": [], "rows": [], "primary_keys": []}, headers=NO_CACHE_HEADERS)

    conn = get_conn()
    pragma_rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    columns = [row["name"] for row in pragma_rows]
    primary_keys = [row["name"] for row in pragma_rows if int(row["pk"] or 0) > 0]
    rows = [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY ROWID DESC").fetchall()]
    conn.close()

    return JSONResponse(
        {
            "ok": True,
            "columns": columns,
            "rows": rows,
            "primary_keys": primary_keys,
        },
        headers=NO_CACHE_HEADERS,
    )


@app.post("/admin/database/table_update", tags=["Administration"])
async def admin_database_table_update(request: Request):
    try:
        require_admin(request)
    except PermissionError:
        return {"ok": False}

    payload = await request.json()
    table = str(payload.get("table") or "").strip()
    primary_keys = payload.get("primary_keys") or {}
    updates = payload.get("updates") or {}

    allowed_tables = {"users", "sessions", "task_runs", "inputs_repository", "user_settings", "app_settings"}
    if table not in allowed_tables or not isinstance(primary_keys, dict) or not isinstance(updates, dict) or not updates:
        return {"ok": False}

    conn = get_conn()
    pragma_rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    allowed_columns = {row["name"] for row in pragma_rows}
    pk_ordered = [row["name"] for row in pragma_rows if int(row["pk"] or 0) > 0]
    if not pk_ordered:
        conn.close()
        return {"ok": False}

    if not set(primary_keys.keys()).issubset(allowed_columns):
        conn.close()
        return {"ok": False}
    if not set(updates.keys()).issubset(allowed_columns):
        conn.close()
        return {"ok": False}

    where_columns = [col for col in pk_ordered if col in primary_keys]
    if not where_columns:
        conn.close()
        return {"ok": False}

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    where_clause = " AND ".join(f"{col} = ?" for col in where_columns)
    query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

    params: list[Any] = [updates[col] for col in updates]
    params.extend(primary_keys[col] for col in where_columns)
    conn.execute(query, tuple(params))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/admin/users/create", tags=["Administration"])
def admin_create_user(request: Request, username: str = Form(...), password: str = Form(...), role: str = Form("user")):
    try:
        require_admin(request)
    except PermissionError:
        return RedirectResponse("/login", status_code=302)

    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO users(username, password_hash, role, active, created_at, access_request_reason) VALUES (?, ?, ?, 1, ?, NULL)",
            (username.strip(), pwd_context.hash(password), "admin" if role == "admin" else "user", now_iso()),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()
    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/users/{user_id}/toggle", tags=["Administration"])
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


@app.post("/admin/users/{user_id}/set_password", tags=["Administration"])
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


@app.post("/admin/users/{user_id}/update", tags=["Administration"])
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
    old_dirs = get_user_storage_dirs(old_username)
    new_dirs = get_user_storage_dirs(new_username)
    username_changed = old_username != new_username

    try:
        if old_username == new_username and new_role == "admin" and new_active == 0:
            new_active = 1
        conn.execute(
            "UPDATE users SET username = ?, role = ?, active = ? WHERE id = ?",
            (new_username, new_role, new_active, user_id),
        )

        if username_changed:
            migrate_user_references(conn, user_id, old_username, new_username)

        if new_password.strip():
            conn.execute(
                "UPDATE users SET password_hash = ? WHERE id = ?",
                (pwd_context.hash(new_password), user_id),
            )
            conn.execute("UPDATE sessions SET active = 0 WHERE user_id = ?", (user_id,))
        if new_active == 0:
            conn.execute("UPDATE sessions SET active = 0 WHERE user_id = ?", (user_id,))

        if username_changed:
            old_output_root = old_dirs["outputs"]
            new_output_root = new_dirs["outputs"]
            if old_output_root.exists():
                new_output_root.parent.mkdir(parents=True, exist_ok=True)
                old_output_root.rename(new_output_root)
            old_upload_root = old_dirs["uploads"]
            new_upload_root = new_dirs["uploads"]
            if old_upload_root.exists():
                new_upload_root.parent.mkdir(parents=True, exist_ok=True)
                old_upload_root.rename(new_upload_root)

        conn.commit()
    except (sqlite3.IntegrityError, OSError):
        conn.rollback()
        return RedirectResponse("/admin", status_code=302)
    finally:
        conn.close()

    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/users/{user_id}/clear_storage", tags=["Administration"])
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
        output_dir = Path(row["output_dir"]) if row["output_dir"] else None
        output_zip = Path(row["output_zip"]) if row["output_zip"] else None
        output_log = Path(row["output_log_file"]) if row["output_log_file"] else None

        if output_dir and is_safe_path(OUTPUTS_DIR, output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        if output_zip and is_safe_path(OUTPUTS_DIR, output_zip):
            output_zip.unlink(missing_ok=True)
        if output_log and is_safe_path(OUTPUTS_DIR, output_log):
            output_log.unlink(missing_ok=True)

    conn.execute("DELETE FROM task_runs WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

    dirs = get_user_storage_dirs(user_row["username"])
    shutil.rmtree(dirs["uploads"], ignore_errors=True)
    shutil.rmtree(dirs["outputs"], ignore_errors=True)

    return RedirectResponse("/admin", status_code=302)


@app.post("/admin/users/{user_id}/delete", tags=["Administration"])
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
    shutil.rmtree(dirs["outputs"], ignore_errors=True)

    return RedirectResponse("/admin", status_code=302)



@app.get("/admin/runs/{run_id}/download", tags=["Administration"])
def admin_download_run_output(request: Request, run_id: int):
    try:
        require_admin(request)
    except PermissionError:
        return PlainTextResponse("", status_code=403)

    conn = get_conn()
    row = conn.execute("SELECT tr.user_id, u.username, tr.module, tr.tool_version, tr.output_zip, tr.output_dir, tr.finished_at FROM task_runs tr JOIN users u ON u.id = tr.user_id WHERE tr.id = ?", (run_id,)).fetchone()
    if not row:
        conn.close()
        return PlainTextResponse("", status_code=404)

    output_zip = resolve_run_zip_path(conn, int(row["user_id"]), row["username"], run_id, row["output_zip"], row["output_dir"], row["module"], row["tool_version"], row["finished_at"])
    conn.close()

    if not output_zip or not output_zip.exists():
        return PlainTextResponse("", status_code=404)

    return FileResponse(output_zip, filename=output_zip.name)



@app.get("/admin/runs/{run_id}/log", tags=["Administration"])
def admin_download_run_log(request: Request, run_id: int):
    try:
        require_admin(request)
    except PermissionError:
        return PlainTextResponse("", status_code=403)

    conn = get_conn()
    row = conn.execute("SELECT tr.user_id, u.username, tr.module, tr.tool_version, tr.output_log_file, tr.output_dir, tr.finished_at FROM task_runs tr JOIN users u ON u.id = tr.user_id WHERE tr.id = ?", (run_id,)).fetchone()
    if not row:
        conn.close()
        return PlainTextResponse("", status_code=404)

    log_path = resolve_run_log_path(conn, int(row["user_id"]), row["username"], run_id, row["output_log_file"], row["output_dir"], row["module"], row["tool_version"], row["finished_at"])
    conn.close()

    if not log_path or not log_path.exists():
        return PlainTextResponse("", status_code=404)

    return FileResponse(log_path, filename=log_path.name)
