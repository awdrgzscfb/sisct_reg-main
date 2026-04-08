from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from core.proxy_utils import normalize_proxy_url

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "register_machine.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
_WRITE_LOCK = threading.Lock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def connection(write: bool = False):
    if write:
        with _WRITE_LOCK:
            conn = _connect()
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()
        return

    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with connection(write=True) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS app_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS task_runs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                total INTEGER NOT NULL DEFAULT 0,
                success INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                skipped INTEGER NOT NULL DEFAULT 0,
                progress TEXT NOT NULL DEFAULT '0/0',
                request_json TEXT NOT NULL DEFAULT '{}',
                summary_json TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                attempt_index INTEGER,
                level TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY(task_id) REFERENCES task_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_task_events_task_id_id ON task_events(task_id, id);

            CREATE TABLE IF NOT EXISTS task_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                attempt_index INTEGER NOT NULL,
                status TEXT NOT NULL,
                email TEXT NOT NULL DEFAULT '',
                password TEXT NOT NULL DEFAULT '',
                access_token TEXT NOT NULL DEFAULT '',
                refresh_token TEXT NOT NULL DEFAULT '',
                session_token TEXT NOT NULL DEFAULT '',
                workspace_id TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                extra_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                FOREIGN KEY(task_id) REFERENCES task_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_task_results_task_id_id ON task_results(task_id, id);

            CREATE TABLE IF NOT EXISTS task_account_states (
                task_id TEXT NOT NULL,
                attempt_index INTEGER NOT NULL,
                email TEXT NOT NULL DEFAULT '',
                label TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(task_id, attempt_index),
                FOREIGN KEY(task_id) REFERENCES task_runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_task_account_states_task_id_attempt_index
                ON task_account_states(task_id, attempt_index);

            CREATE TABLE IF NOT EXISTS outlook_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL DEFAULT '',
                client_id TEXT NOT NULL DEFAULT '',
                refresh_token TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_used REAL
            );
            CREATE INDEX IF NOT EXISTS idx_outlook_accounts_enabled_id ON outlook_accounts(enabled, id);

            CREATE TABLE IF NOT EXISTS luckmail_token_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                token TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                last_used REAL
            );
            CREATE INDEX IF NOT EXISTS idx_luckmail_token_accounts_enabled_id ON luckmail_token_accounts(enabled, id);

            CREATE TABLE IF NOT EXISTS proxy_pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proxy_url TEXT NOT NULL UNIQUE,
                enabled INTEGER NOT NULL DEFAULT 1,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_checked_at REAL,
                last_check_status TEXT NOT NULL DEFAULT '',
                last_check_message TEXT NOT NULL DEFAULT '',
                last_ip TEXT NOT NULL DEFAULT '',
                last_country TEXT NOT NULL DEFAULT '',
                last_used_at REAL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_proxy_pool_enabled_id ON proxy_pool(enabled, id);
            """
        )

        def _has_column(table: str, column: str) -> bool:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return any(str(row["name"]) == column for row in rows)

        if not _has_column("task_events", "attempt_index"):
            conn.execute("ALTER TABLE task_events ADD COLUMN attempt_index INTEGER")


def _proxy_row_to_item(row: sqlite3.Row | None) -> dict[str, Any] | None:
    data = row_to_dict(row)
    if not data:
        return None
    return {
        "id": int(data.get("id") or 0),
        "proxy_url": str(data.get("proxy_url") or ""),
        "enabled": bool(data.get("enabled")),
        "success_count": int(data.get("success_count") or 0),
        "failure_count": int(data.get("failure_count") or 0),
        "last_checked_at": float(data.get("last_checked_at") or 0),
        "last_check_status": str(data.get("last_check_status") or ""),
        "last_check_message": str(data.get("last_check_message") or ""),
        "last_ip": str(data.get("last_ip") or ""),
        "last_country": str(data.get("last_country") or ""),
        "last_used_at": float(data.get("last_used_at") or 0),
        "created_at": float(data.get("created_at") or 0),
        "updated_at": float(data.get("updated_at") or 0),
    }


def finalize_orphaned_tasks() -> int:
    with connection(write=True) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM task_runs WHERE status IN ('pending', 'running')"
        ).fetchone()
        total = int((row_to_dict(row) or {}).get("total") or 0)
        if total <= 0:
            return 0
        now = time.time()
        conn.execute(
            """
            UPDATE task_runs
            SET status = 'stopped',
                error = CASE
                    WHEN error IS NULL OR error = '' THEN '服务重启，任务已中断'
                    ELSE error
                END,
                updated_at = ?
            WHERE status IN ('pending', 'running')
            """,
            (now,),
        )
        return total


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def get_config() -> dict[str, str]:
    with connection() as conn:
        rows = conn.execute("SELECT key, value FROM app_config").fetchall()
    return {str(row["key"]): str(row["value"]) for row in rows}


def set_config(values: dict[str, Any]) -> None:
    with connection(write=True) as conn:
        conn.executemany(
            "INSERT INTO app_config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [
                (
                    str(k),
                    json.dumps(v, ensure_ascii=False)
                    if isinstance(v, (dict, list, bool, int, float))
                    else str(v or ""),
                )
                for k, v in values.items()
            ],
        )


def parse_config_row_values(values: dict[str, str]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for key, value in values.items():
        text = str(value)
        try:
            parsed[key] = json.loads(text)
        except Exception:
            parsed[key] = text
    return parsed


def create_task_run(task_id: str, *, total: int, request_payload: dict[str, Any]) -> None:
    now = time.time()
    with connection(write=True) as conn:
        conn.execute(
            "INSERT INTO task_runs(id, status, total, progress, request_json, created_at, updated_at) VALUES(?, 'pending', ?, ?, ?, ?, ?)",
            (task_id, total, f"0/{total}", json.dumps(request_payload, ensure_ascii=False), now, now),
        )


def update_task_run(task_id: str, **fields: Any) -> None:
    if not fields:
        return
    allowed = {
        "status",
        "total",
        "success",
        "failed",
        "skipped",
        "progress",
        "summary_json",
        "error",
        "updated_at",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    updates["updated_at"] = time.time()
    columns = ", ".join(f"{key} = ?" for key in updates)
    values = [
        json.dumps(v, ensure_ascii=False) if key == "summary_json" and not isinstance(v, str) else v
        for key, v in updates.items()
    ]
    values.append(task_id)
    with connection(write=True) as conn:
        conn.execute(f"UPDATE task_runs SET {columns} WHERE id = ?", values)


def update_task_request_count(task_id: str, count: int) -> None:
    with connection(write=True) as conn:
        row = conn.execute(
            "SELECT request_json FROM task_runs WHERE id = ? LIMIT 1",
            (task_id,),
        ).fetchone()
        data = row_to_dict(row) or {}
        try:
            payload = json.loads(data.get("request_json") or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload["count"] = int(count)
        conn.execute(
            "UPDATE task_runs SET request_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(payload, ensure_ascii=False), time.time(), task_id),
        )


def append_task_event(
    task_id: str,
    *,
    seq: int,
    message: str,
    level: str = "info",
    attempt_index: int | None = None,
) -> None:
    with connection(write=True) as conn:
        conn.execute(
            "INSERT INTO task_events(task_id, seq, attempt_index, level, message, created_at) VALUES(?, ?, ?, ?, ?, ?)",
            (task_id, seq, attempt_index, level, message, time.time()),
        )


def insert_task_result(
    task_id: str,
    *,
    attempt_index: int,
    status: str,
    email: str = "",
    password: str = "",
    access_token: str = "",
    refresh_token: str = "",
    session_token: str = "",
    workspace_id: str = "",
    error: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    with connection(write=True) as conn:
        conn.execute(
            """
            INSERT INTO task_results(
                task_id, attempt_index, status, email, password, access_token, refresh_token, session_token, workspace_id, error, extra_json, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                attempt_index,
                status,
                email,
                password,
                access_token,
                refresh_token,
                session_token,
                workspace_id,
                error,
                json.dumps(extra or {}, ensure_ascii=False),
                time.time(),
            ),
        )


def get_task_run(task_id: str) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute("SELECT * FROM task_runs WHERE id = ?", (task_id,)).fetchone()
    data = row_to_dict(row)
    if not data:
        return None
    for key in ("request_json", "summary_json"):
        try:
            data[key] = json.loads(data.get(key) or "{}")
        except Exception:
            data[key] = {}
    return data


def list_task_runs(*, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    with connection() as conn:
        rows = conn.execute(
            "SELECT * FROM task_runs ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    items = []
    for row in rows:
        data = row_to_dict(row) or {}
        for key in ("request_json", "summary_json"):
            try:
                data[key] = json.loads(data.get(key) or "{}")
            except Exception:
                data[key] = {}
        items.append(data)
    return items


def count_task_runs() -> int:
    with connection() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM task_runs").fetchone()
    return int(row["c"]) if row else 0


def get_task_events(task_id: str, *, after_seq: int = 0) -> list[dict[str, Any]]:
    with connection() as conn:
        rows = conn.execute(
            "SELECT seq, attempt_index, level, message, created_at FROM task_events WHERE task_id = ? AND seq > ? ORDER BY seq ASC",
            (task_id, after_seq),
        ).fetchall()
    return [row_to_dict(row) or {} for row in rows]


def get_task_result(result_id: int) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute("SELECT * FROM task_results WHERE id = ?", (int(result_id),)).fetchone()
    data = row_to_dict(row)
    if not data:
        return None
    try:
        data["extra_json"] = json.loads(data.get("extra_json") or "{}")
    except Exception:
        data["extra_json"] = {}
    return data


def get_task_results(task_id: str) -> list[dict[str, Any]]:
    with connection() as conn:
        rows = conn.execute(
            "SELECT * FROM task_results WHERE task_id = ? ORDER BY attempt_index ASC, id ASC",
            (task_id,),
        ).fetchall()
    items = []
    for row in rows:
        data = row_to_dict(row) or {}
        try:
            data["extra_json"] = json.loads(data.get("extra_json") or "{}")
        except Exception:
            data["extra_json"] = {}
        items.append(data)
    return items


def get_task_account_states(task_id: str) -> list[dict[str, Any]]:
    with connection() as conn:
        rows = conn.execute(
            """
            SELECT task_id, attempt_index, email, label, status, error, created_at, updated_at
            FROM task_account_states
            WHERE task_id = ?
            ORDER BY attempt_index ASC
            """,
            (task_id,),
        ).fetchall()
    return [row_to_dict(row) or {} for row in rows]


def upsert_task_account_state(
    task_id: str,
    attempt_index: int,
    *,
    email: str = "",
    label: str = "",
    status: str = "pending",
    error: str = "",
    created_at: float | None = None,
    updated_at: float | None = None,
) -> None:
    created = float(created_at or time.time())
    updated = float(updated_at or time.time())
    with connection(write=True) as conn:
        existing = conn.execute(
            """
            SELECT created_at, email, label, status, error
            FROM task_account_states
            WHERE task_id = ? AND attempt_index = ?
            LIMIT 1
            """,
            (task_id, int(attempt_index)),
        ).fetchone()
        existing_data = row_to_dict(existing) or {}
        final_created = float(existing_data.get("created_at") or created)
        final_email = str(email or existing_data.get("email") or "")
        final_label = str(label or existing_data.get("label") or final_email or f"第 {int(attempt_index)} 个账号")
        final_status = str(status or existing_data.get("status") or "pending")
        final_error = str(error if error is not None else existing_data.get("error") or "")
        conn.execute(
            """
            INSERT INTO task_account_states(
                task_id, attempt_index, email, label, status, error, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id, attempt_index) DO UPDATE SET
                email = excluded.email,
                label = excluded.label,
                status = excluded.status,
                error = excluded.error,
                updated_at = excluded.updated_at
            """,
            (
                task_id,
                int(attempt_index),
                final_email,
                final_label,
                final_status,
                final_error,
                final_created,
                updated,
            ),
        )


def delete_task_result(task_id: str, attempt_index: int) -> int:
    with connection(write=True) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM task_results WHERE task_id = ? AND attempt_index = ?",
            (task_id, int(attempt_index)),
        ).fetchone()
        total = int((row_to_dict(row) or {}).get("total") or 0)
        if total > 0:
            conn.execute(
                "DELETE FROM task_results WHERE task_id = ? AND attempt_index = ?",
                (task_id, int(attempt_index)),
            )
    return total


def delete_task_account(task_id: str, attempt_index: int) -> dict[str, int]:
    with connection(write=True) as conn:
        result_row = conn.execute(
            "SELECT COUNT(*) AS total FROM task_results WHERE task_id = ? AND attempt_index = ?",
            (task_id, int(attempt_index)),
        ).fetchone()
        event_row = conn.execute(
            "SELECT COUNT(*) AS total FROM task_events WHERE task_id = ? AND attempt_index = ?",
            (task_id, int(attempt_index)),
        ).fetchone()
        deleted_results = int((row_to_dict(result_row) or {}).get("total") or 0)
        deleted_events = int((row_to_dict(event_row) or {}).get("total") or 0)
        state_row = conn.execute(
            "SELECT COUNT(*) AS total FROM task_account_states WHERE task_id = ? AND attempt_index = ?",
            (task_id, int(attempt_index)),
        ).fetchone()
        deleted_states = int((row_to_dict(state_row) or {}).get("total") or 0)
        conn.execute(
            "DELETE FROM task_results WHERE task_id = ? AND attempt_index = ?",
            (task_id, int(attempt_index)),
        )
        conn.execute(
            "DELETE FROM task_events WHERE task_id = ? AND attempt_index = ?",
            (task_id, int(attempt_index)),
        )
        conn.execute(
            "DELETE FROM task_account_states WHERE task_id = ? AND attempt_index = ?",
            (task_id, int(attempt_index)),
        )

        remain_result_row = conn.execute(
            "SELECT COUNT(*) AS total FROM task_results WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        remain_event_row = conn.execute(
            "SELECT COUNT(*) AS total FROM task_events WHERE task_id = ? AND attempt_index IS NOT NULL",
            (task_id,),
        ).fetchone()
        remain_state_row = conn.execute(
            "SELECT COUNT(*) AS total FROM task_account_states WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        remain_results = int((row_to_dict(remain_result_row) or {}).get("total") or 0)
        remain_events = int((row_to_dict(remain_event_row) or {}).get("total") or 0)
        remain_states = int((row_to_dict(remain_state_row) or {}).get("total") or 0)

        if remain_results <= 0 and remain_events <= 0 and remain_states <= 0:
            conn.execute("DELETE FROM task_events WHERE task_id = ?", (task_id,))
            conn.execute("DELETE FROM task_account_states WHERE task_id = ?", (task_id,))
            conn.execute("DELETE FROM task_runs WHERE id = ?", (task_id,))

    return {
        "deleted_results": deleted_results,
        "deleted_events": deleted_events,
        "deleted_states": deleted_states,
    }


def get_proxy_pool_summary(*, limit: int = 100) -> dict[str, Any]:
    with connection() as conn:
        total_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled,
                SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) AS disabled,
                SUM(CASE WHEN last_check_status = 'ok' THEN 1 ELSE 0 END) AS healthy,
                SUM(CASE WHEN last_check_status = 'fail' THEN 1 ELSE 0 END) AS unhealthy,
                SUM(success_count) AS success_count,
                SUM(failure_count) AS failure_count
            FROM proxy_pool
            """
        ).fetchone()
        rows = conn.execute(
            """
            SELECT *
            FROM proxy_pool
            ORDER BY enabled DESC, updated_at DESC, id DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 1),),
        ).fetchall()

    total_data = row_to_dict(total_row) or {}
    items = [_proxy_row_to_item(row) for row in rows]
    return {
        "total": int(total_data.get("total") or 0),
        "enabled": int(total_data.get("enabled") or 0),
        "disabled": int(total_data.get("disabled") or 0),
        "healthy": int(total_data.get("healthy") or 0),
        "unhealthy": int(total_data.get("unhealthy") or 0),
        "success_count": int(total_data.get("success_count") or 0),
        "failure_count": int(total_data.get("failure_count") or 0),
        "items": [item for item in items if item],
    }


def get_proxy_account(proxy_id: int) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute(
            "SELECT * FROM proxy_pool WHERE id = ? LIMIT 1",
            (int(proxy_id),),
        ).fetchone()
    return _proxy_row_to_item(row)


def find_proxy_account_id_by_url(proxy_url: str | None) -> int | None:
    normalized = normalize_proxy_url(proxy_url)
    if not normalized:
        return None
    with connection() as conn:
        row = conn.execute(
            "SELECT id FROM proxy_pool WHERE proxy_url = ? LIMIT 1",
            (normalized,),
        ).fetchone()
    if row is None:
        return None
    try:
        return int(row["id"])
    except Exception:
        return None


def batch_import_proxy_pool(data: str, *, enabled: bool = True) -> dict[str, Any]:
    now = time.time()
    lines = [str(line or "") for line in (data or "").splitlines()]
    parsed_total = 0
    success = 0
    updated = 0
    failed = 0
    items: list[dict[str, Any]] = []
    errors: list[str] = []

    with connection(write=True) as conn:
        for index, raw_line in enumerate(lines, start=1):
            line = str(raw_line or "").strip()
            if not line or line.startswith("#"):
                continue
            parsed_total += 1
            proxy_url = normalize_proxy_url(line)
            if not proxy_url:
                failed += 1
                errors.append(f"行 {index}: 代理为空")
                continue

            row = conn.execute(
                "SELECT id FROM proxy_pool WHERE proxy_url = ? LIMIT 1",
                (proxy_url,),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE proxy_pool
                    SET enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (1 if enabled else 0, now, int(row["id"])),
                )
                updated += 1
                proxy_id = int(row["id"])
                status = "updated"
            else:
                cur = conn.execute(
                    """
                    INSERT INTO proxy_pool(
                        proxy_url, enabled, success_count, failure_count,
                        last_checked_at, last_check_status, last_check_message,
                        last_ip, last_country, last_used_at, created_at, updated_at
                    ) VALUES(?, ?, 0, 0, NULL, '', '', '', '', NULL, ?, ?)
                    """,
                    (proxy_url, 1 if enabled else 0, now, now),
                )
                proxy_id = int(cur.lastrowid or 0)
                success += 1
                status = "imported"

            items.append(
                {
                    "id": proxy_id,
                    "proxy_url": proxy_url,
                    "enabled": bool(enabled),
                    "status": status,
                }
            )

    return {
        "total": parsed_total,
        "success": success,
        "updated": updated,
        "failed": failed,
        "items": items,
        "errors": errors,
        "summary": get_proxy_pool_summary(),
    }


def delete_proxy_account(proxy_id: int) -> bool:
    with connection(write=True) as conn:
        row = conn.execute(
            "SELECT id FROM proxy_pool WHERE id = ? LIMIT 1",
            (int(proxy_id),),
        ).fetchone()
        if row is None:
            return False
        conn.execute("DELETE FROM proxy_pool WHERE id = ?", (int(proxy_id),))
        return True


def delete_all_proxy_accounts() -> int:
    with connection(write=True) as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM proxy_pool").fetchone()
        total = int((row_to_dict(row) or {}).get("total") or 0)
        conn.execute("DELETE FROM proxy_pool")
        return total


def list_enabled_proxy_pool() -> list[dict[str, Any]]:
    with connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM proxy_pool
            WHERE enabled = 1
            ORDER BY COALESCE(last_used_at, 0) ASC, id ASC
            """
        ).fetchall()
    return [item for item in (_proxy_row_to_item(row) for row in rows) if item]


def acquire_proxy_pool_entry(*, exclude_ids: list[int] | None = None) -> dict[str, Any] | None:
    now = time.time()
    exclude_values = [int(item) for item in (exclude_ids or []) if int(item or 0) > 0]
    with connection(write=True) as conn:
        query = """
            SELECT *
            FROM proxy_pool
            WHERE enabled = 1
        """
        params: list[Any] = []
        if exclude_values:
            placeholders = ",".join("?" for _ in exclude_values)
            query += f" AND id NOT IN ({placeholders})"
            params.extend(exclude_values)
        query += " ORDER BY COALESCE(last_used_at, 0) ASC, id ASC LIMIT 1"
        row = conn.execute(query, params).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE proxy_pool SET last_used_at = ?, updated_at = ? WHERE id = ?",
            (now, now, int(row["id"])),
        )
        refreshed = conn.execute(
            "SELECT * FROM proxy_pool WHERE id = ?",
            (int(row["id"]),),
        ).fetchone()
        return _proxy_row_to_item(refreshed)


def update_proxy_check_result(
    proxy_id: int,
    *,
    ok: bool,
    message: str = "",
    ip: str = "",
    country: str = "",
) -> None:
    now = time.time()
    with connection(write=True) as conn:
        conn.execute(
            """
            UPDATE proxy_pool
            SET last_checked_at = ?,
                last_check_status = ?,
                last_check_message = ?,
                last_ip = ?,
                last_country = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                now,
                "ok" if ok else "fail",
                str(message or ""),
                str(ip or ""),
                str(country or ""),
                now,
                int(proxy_id),
            ),
        )


def update_proxy_usage_result(proxy_id: int, *, success: bool) -> None:
    now = time.time()
    field = "success_count" if success else "failure_count"
    with connection(write=True) as conn:
        conn.execute(
            f"UPDATE proxy_pool SET {field} = {field} + 1, updated_at = ? WHERE id = ?",
            (now, int(proxy_id)),
        )


def get_outlook_pool_summary(*, limit: int = 8) -> dict[str, Any]:
    with connection() as conn:
        total_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled,
                SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) AS disabled,
                SUM(CASE WHEN client_id != '' AND refresh_token != '' THEN 1 ELSE 0 END) AS with_oauth
            FROM outlook_accounts
            """
        ).fetchone()
        item_rows = conn.execute(
            """
            SELECT id, email, enabled, client_id, refresh_token, created_at, updated_at, last_used
            FROM outlook_accounts
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 1),),
        ).fetchall()

    total_data = row_to_dict(total_row) or {}
    items = []
    for row in item_rows:
        data = row_to_dict(row) or {}
        items.append(
            {
                "id": data.get("id"),
                "email": data.get("email") or "",
                "enabled": bool(data.get("enabled")),
                "has_oauth": bool(data.get("client_id") and data.get("refresh_token")),
                "created_at": data.get("created_at") or 0,
                "updated_at": data.get("updated_at") or 0,
                "last_used": data.get("last_used") or 0,
            }
        )
    return {
        "total": int(total_data.get("total") or 0),
        "enabled": int(total_data.get("enabled") or 0),
        "disabled": int(total_data.get("disabled") or 0),
        "with_oauth": int(total_data.get("with_oauth") or 0),
        "items": items,
    }


def get_luckmail_token_pool_summary(*, limit: int = 8) -> dict[str, Any]:
    with connection() as conn:
        total_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled,
                SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) AS disabled
            FROM luckmail_token_accounts
            """
        ).fetchone()
        item_rows = conn.execute(
            """
            SELECT id, email, token, enabled, created_at, updated_at, last_used
            FROM luckmail_token_accounts
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (max(int(limit or 0), 1),),
        ).fetchall()

    total_data = row_to_dict(total_row) or {}
    items = []
    for row in item_rows:
        data = row_to_dict(row) or {}
        items.append(
            {
                "id": data.get("id"),
                "email": data.get("email") or "",
                "token": data.get("token") or "",
                "enabled": bool(data.get("enabled")),
                "created_at": data.get("created_at") or 0,
                "updated_at": data.get("updated_at") or 0,
                "last_used": data.get("last_used") or 0,
            }
        )
    return {
        "total": int(total_data.get("total") or 0),
        "enabled": int(total_data.get("enabled") or 0),
        "disabled": int(total_data.get("disabled") or 0),
        "items": items,
    }


def batch_import_luckmail_token_accounts(data: str, *, enabled: bool = True) -> dict[str, Any]:
    now = time.time()
    lines = [str(line or "") for line in (data or "").splitlines()]
    parsed_total = 0
    success = 0
    updated = 0
    failed = 0
    accounts: list[dict[str, Any]] = []
    errors: list[str] = []

    with connection(write=True) as conn:
        for index, raw_line in enumerate(lines, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parsed_total += 1

            parts = [part.strip() for part in line.split("----")]
            if len(parts) != 2:
                failed += 1
                errors.append(f"行 {index}: 格式错误，应为 邮箱----token")
                continue

            email = parts[0].strip().lower()
            token = parts[1].strip()

            if not email or "@" not in email:
                failed += 1
                errors.append(f"行 {index}: 邮箱格式无效")
                continue
            if not token:
                failed += 1
                errors.append(f"行 {index}: token 不能为空")
                continue

            row = conn.execute(
                "SELECT id FROM luckmail_token_accounts WHERE lower(email) = lower(?) LIMIT 1",
                (email,),
            ).fetchone()

            if row:
                conn.execute(
                    """
                    UPDATE luckmail_token_accounts
                    SET token = ?, enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (token, 1 if enabled else 0, now, row["id"]),
                )
                updated += 1
                account_id = int(row["id"])
                status = "updated"
            else:
                cur = conn.execute(
                    """
                    INSERT INTO luckmail_token_accounts(email, token, enabled, created_at, updated_at, last_used)
                    VALUES(?, ?, ?, ?, ?, NULL)
                    """,
                    (email, token, 1 if enabled else 0, now, now),
                )
                success += 1
                account_id = int(cur.lastrowid or 0)
                status = "imported"

            accounts.append(
                {
                    "id": account_id,
                    "email": email,
                    "enabled": bool(enabled),
                    "status": status,
                }
            )

    return {
        "total": parsed_total,
        "success": success,
        "updated": updated,
        "failed": failed,
        "accounts": accounts,
        "errors": errors,
        "summary": get_luckmail_token_pool_summary(),
    }


def batch_import_outlook_accounts(data: str, *, enabled: bool = True) -> dict[str, Any]:
    now = time.time()
    lines = [str(line or "") for line in (data or "").splitlines()]
    parsed_total = 0
    success = 0
    updated = 0
    failed = 0
    accounts: list[dict[str, Any]] = []
    errors: list[str] = []

    with connection(write=True) as conn:
        for index, raw_line in enumerate(lines, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parsed_total += 1

            parts = [part.strip() for part in line.split("----")]
            if len(parts) not in {2, 4}:
                failed += 1
                errors.append(f"行 {index}: 格式错误，应为 邮箱----密码 或 邮箱----密码----client_id----refresh_token")
                continue

            email = parts[0].strip().lower()
            password = parts[1].strip()
            client_id = parts[2].strip() if len(parts) == 4 else ""
            refresh_token = parts[3].strip() if len(parts) == 4 else ""

            if not email or "@" not in email:
                failed += 1
                errors.append(f"行 {index}: 邮箱格式无效")
                continue
            if not password:
                failed += 1
                errors.append(f"行 {index}: 密码不能为空")
                continue
            if (client_id and not refresh_token) or (refresh_token and not client_id):
                failed += 1
                errors.append(f"行 {index}: client_id 与 refresh_token 必须同时提供")
                continue

            row = conn.execute(
                "SELECT id FROM outlook_accounts WHERE lower(email) = lower(?) LIMIT 1",
                (email,),
            ).fetchone()

            if row:
                conn.execute(
                    """
                    UPDATE outlook_accounts
                    SET password = ?, client_id = ?, refresh_token = ?, enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (password, client_id, refresh_token, 1 if enabled else 0, now, row["id"]),
                )
                updated += 1
                account_id = int(row["id"])
                status = "updated"
            else:
                cur = conn.execute(
                    """
                    INSERT INTO outlook_accounts(email, password, client_id, refresh_token, enabled, created_at, updated_at, last_used)
                    VALUES(?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (email, password, client_id, refresh_token, 1 if enabled else 0, now, now),
                )
                success += 1
                account_id = int(cur.lastrowid or 0)
                status = "imported"

            accounts.append(
                {
                    "id": account_id,
                    "email": email,
                    "enabled": bool(enabled),
                    "has_oauth": bool(client_id and refresh_token),
                    "status": status,
                }
            )

    return {
        "total": parsed_total,
        "success": success,
        "updated": updated,
        "failed": failed,
        "accounts": accounts,
        "errors": errors,
        "summary": get_outlook_pool_summary(),
    }


def take_outlook_account(*, preferred_email: str | None = None) -> dict[str, Any] | None:
    now = time.time()
    preferred_email = str(preferred_email or "").strip()
    with connection(write=True) as conn:
        if preferred_email:
            row = conn.execute(
                """
                SELECT * FROM outlook_accounts
                WHERE enabled = 1 AND lower(email) = lower(?)
                ORDER BY id ASC
                LIMIT 1
                """,
                (preferred_email,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT * FROM outlook_accounts
                WHERE enabled = 1
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()

        if row is None:
            return None

        conn.execute(
            "UPDATE outlook_accounts SET enabled = 0, updated_at = ?, last_used = ? WHERE id = ?",
            (now, now, row["id"]),
        )
        data = row_to_dict(row) or {}
        data["enabled"] = 0
        data["last_used"] = now
        data["updated_at"] = now
        return data


def take_luckmail_token_account(*, preferred_email: str | None = None) -> dict[str, Any] | None:
    now = time.time()
    preferred_email = str(preferred_email or "").strip()
    with connection(write=True) as conn:
        if preferred_email:
            row = conn.execute(
                """
                SELECT * FROM luckmail_token_accounts
                WHERE enabled = 1 AND lower(email) = lower(?)
                ORDER BY id ASC
                LIMIT 1
                """,
                (preferred_email,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT * FROM luckmail_token_accounts
                WHERE enabled = 1
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()

        if row is None:
            return None

        conn.execute(
            "UPDATE luckmail_token_accounts SET enabled = 0, updated_at = ?, last_used = ? WHERE id = ?",
            (now, now, row["id"]),
        )
        data = row_to_dict(row) or {}
        data["enabled"] = 0
        data["last_used"] = now
        data["updated_at"] = now
        return data


def get_outlook_account_by_email(email: str) -> dict[str, Any] | None:
    target_email = str(email or "").strip()
    if not target_email:
        return None

    now = time.time()
    with connection(write=True) as conn:
        row = conn.execute(
            """
            SELECT * FROM outlook_accounts
            WHERE lower(email) = lower(?)
            ORDER BY id ASC
            LIMIT 1
            """,
            (target_email,),
        ).fetchone()
        if row is None:
            return None

        data = row_to_dict(row) or {}
        if int(data.get("enabled") or 0) == 1:
            conn.execute(
                "UPDATE outlook_accounts SET enabled = 0, updated_at = ?, last_used = ? WHERE id = ?",
                (now, now, row["id"]),
            )
            data["enabled"] = 0
            data["last_used"] = now
            data["updated_at"] = now
        return data


def get_luckmail_token_account_by_email(email: str) -> dict[str, Any] | None:
    target_email = str(email or "").strip()
    if not target_email:
        return None

    now = time.time()
    with connection(write=True) as conn:
        row = conn.execute(
            """
            SELECT * FROM luckmail_token_accounts
            WHERE lower(email) = lower(?)
            ORDER BY id ASC
            LIMIT 1
            """,
            (target_email,),
        ).fetchone()
        if row is None:
            return None

        data = row_to_dict(row) or {}
        if int(data.get("enabled") or 0) == 1:
            conn.execute(
                "UPDATE luckmail_token_accounts SET enabled = 0, updated_at = ?, last_used = ? WHERE id = ?",
                (now, now, row["id"]),
            )
            data["enabled"] = 0
            data["last_used"] = now
            data["updated_at"] = now
        return data


def delete_outlook_account(account_id: int) -> bool:
    with connection(write=True) as conn:
        row = conn.execute("SELECT id FROM outlook_accounts WHERE id = ?", (int(account_id),)).fetchone()
        if row is None:
            return False
        conn.execute("DELETE FROM outlook_accounts WHERE id = ?", (int(account_id),))
        return True


def delete_luckmail_token_account(account_id: int) -> bool:
    with connection(write=True) as conn:
        row = conn.execute("SELECT id FROM luckmail_token_accounts WHERE id = ?", (int(account_id),)).fetchone()
        if row is None:
            return False
        conn.execute("DELETE FROM luckmail_token_accounts WHERE id = ?", (int(account_id),))
        return True


def delete_all_outlook_accounts() -> int:
    with connection(write=True) as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM outlook_accounts").fetchone()
        total = int((row_to_dict(row) or {}).get("total") or 0)
        conn.execute("DELETE FROM outlook_accounts")
        return total


def delete_all_luckmail_token_accounts() -> int:
    with connection(write=True) as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM luckmail_token_accounts").fetchone()
        total = int((row_to_dict(row) or {}).get("total") or 0)
        conn.execute("DELETE FROM luckmail_token_accounts")
        return total


def delete_taken_outlook_accounts() -> int:
    with connection(write=True) as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM outlook_accounts WHERE enabled = 0").fetchone()
        total = int((row_to_dict(row) or {}).get("total") or 0)
        conn.execute("DELETE FROM outlook_accounts WHERE enabled = 0")
        return total


def delete_taken_luckmail_token_accounts() -> int:
    with connection(write=True) as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM luckmail_token_accounts WHERE enabled = 0").fetchone()
        total = int((row_to_dict(row) or {}).get("total") or 0)
        conn.execute("DELETE FROM luckmail_token_accounts WHERE enabled = 0")
        return total
