from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
import secrets
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "web_frontend.db"
ACCESS_LOG_PATH = DATA_DIR / "access.log"
APP_LOG_PATH = DATA_DIR / "app.log"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("web_frontend")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        app_handler = RotatingFileHandler(APP_LOG_PATH, maxBytes=2_000_000, backupCount=3)
        app_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(app_handler)
    return logger


def setup_access_logger() -> logging.Logger:
    logger = logging.getLogger("web_access")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
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


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
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
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            duration_seconds REAL,
            command TEXT NOT NULL,
            output_log TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )

    admin = cur.execute("SELECT id FROM users WHERE username = ?", ("admin",)).fetchone()
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

    conn.commit()
    conn.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def parse_bool(value: str | None) -> bool:
    return value in {"on", "true", "1", "yes"}


def build_cli_command(payload: dict[str, Any]) -> list[str]:
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
    if parse_bool(payload.get("frequency_audit")):
        cmd.append("--frequency-audit")
    if parse_bool(payload.get("export_correction_cmd")):
        cmd.append("--export-correction-cmd")
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

    settings = load_user_settings(user["id"])
    conn = get_conn()
    latest_runs = conn.execute(
        """
        SELECT module, status, started_at, finished_at, duration_seconds
        FROM task_runs
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 10
        """,
        (user["id"],),
    ).fetchall()
    conn.close()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": user,
            "module_options": MODULE_OPTIONS,
            "settings": settings,
            "latest_runs": latest_runs,
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
    if not user or not pwd_context.verify(password, user["password_hash"]):
        conn.close()
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Credenciales inválidas o usuario desactivado."},
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
        "profiles_audit": profiles_audit,
        "frequency_audit": frequency_audit,
        "export_correction_cmd": export_correction_cmd,
        "fast_excel_export": fast_excel_export,
    }
    save_user_settings(user["id"], payload)

    cmd = build_cli_command(payload)

    start = time.perf_counter()
    started_at = now_iso()
    status = "ok"
    output_log = ""

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(BASE_DIR.parent),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            status = "error"
        output_log = (proc.stdout or "") + "\n" + (proc.stderr or "")
    except Exception as exc:
        status = "error"
        output_log = f"Execution error: {exc}"

    duration = time.perf_counter() - start
    finished_at = now_iso()

    conn = get_conn()
    conn.execute(
        """
        INSERT INTO task_runs(user_id, module, status, started_at, finished_at, duration_seconds, command, output_log)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (user["id"], module, status, started_at, finished_at, duration, " ".join(cmd), output_log[:20000]),
    )
    conn.commit()
    conn.close()

    return RedirectResponse("/", status_code=302)


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
    recent_runs = conn.execute(
        """
        SELECT tr.id, u.username, tr.module, tr.status, tr.started_at, tr.finished_at, tr.duration_seconds
        FROM task_runs tr
        JOIN users u ON u.id = tr.user_id
        ORDER BY tr.id DESC
        LIMIT 50
        """
    ).fetchall()
    conn.close()

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


@app.post("/admin/users/{user_id}/reset_password")
def admin_reset_password(request: Request, user_id: int, new_password: str = Form(...)):
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
