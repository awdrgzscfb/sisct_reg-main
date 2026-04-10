from __future__ import annotations

import logging
import threading
import time
from copy import deepcopy
from typing import Any

import requests

from .db import get_config, parse_config_row_values
from .defaults import DEFAULT_CONFIG

logger = logging.getLogger(__name__)


class AccountPoolMonitor:
    def __init__(self, manager) -> None:
        self._manager = manager
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_trigger_at = 0.0
        self._state: dict[str, Any] = {
            "running": False,
            "enabled": False,
            "status": "idle",
            "message": "",
            "last_check_at": 0.0,
            "last_active_count": 0,
            "last_total_count": 0,
            "last_task_id": "",
            "last_error": "",
            "check_count": 0,
            "trigger_count": 0,
        }

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop, name="account-pool-monitor", daemon=True)
            self._thread.start()
            self._state["running"] = True
            self._state["status"] = "running"
            self._state["message"] = "monitor thread started"

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
            self._thread = None
            self._stop_event.set()
            self._state["running"] = False
            self._state["status"] = "stopped"
            self._state["message"] = "monitor thread stopped"
        if thread is not None:
            thread.join(timeout=5)

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self._state)

    def test_connection(self) -> dict[str, Any]:
        started_at = time.time()
        merged_config = self._load_config()
        api_url = str(merged_config.get("codexproxy_api_url") or "").strip()
        admin_key = str(merged_config.get("codexproxy_admin_key") or "").strip()
        timeout_seconds = self._as_int(
            merged_config.get("pool_monitor_request_timeout_seconds"),
            default=15,
            min_value=3,
            max_value=120,
        )
        statuses = self._normalize_statuses(merged_config.get("pool_monitor_account_status"))
        if not api_url or not admin_key:
            return {
                "ok": False,
                "reason": "misconfigured",
                "message": "codexproxy_api_url 或 codexproxy_admin_key 未配置",
                "checked_at": time.time(),
            }
        try:
            accounts = self._fetch_accounts(
                api_url=api_url,
                admin_key=admin_key,
                timeout_seconds=timeout_seconds,
            )
            active_count = sum(1 for item in accounts if str(item.get("status") or "").strip().lower() in statuses)
            status_counts: dict[str, int] = {}
            for item in accounts:
                key = str(item.get("status") or "").strip().lower() or "unknown"
                status_counts[key] = int(status_counts.get(key) or 0) + 1
            elapsed_ms = int((time.time() - started_at) * 1000)
            return {
                "ok": True,
                "message": "连接成功",
                "checked_at": time.time(),
                "elapsed_ms": elapsed_ms,
                "api_url": api_url,
                "total_count": len(accounts),
                "active_count": active_count,
                "status_filter": sorted(statuses),
                "status_counts": status_counts,
            }
        except Exception as exc:
            elapsed_ms = int((time.time() - started_at) * 1000)
            return {
                "ok": False,
                "reason": "request_failed",
                "message": str(exc) or "请求失败",
                "checked_at": time.time(),
                "elapsed_ms": elapsed_ms,
                "api_url": api_url,
            }

    def _update_state(self, **fields: Any) -> None:
        with self._lock:
            self._state.update(fields)

    @staticmethod
    def _as_int(value: Any, *, default: int, min_value: int, max_value: int) -> int:
        try:
            parsed = int(float(value))
        except Exception:
            parsed = int(default)
        return max(min_value, min(max_value, parsed))

    @staticmethod
    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "on"}

    @staticmethod
    def _normalize_statuses(value: Any) -> set[str]:
        text = str(value or "").strip().lower()
        if not text:
            return {"active"}
        items = {part.strip() for part in text.split(",") if part.strip()}
        return items or {"active"}

    @staticmethod
    def _load_config() -> dict[str, Any]:
        stored = parse_config_row_values(get_config())
        merged = dict(DEFAULT_CONFIG)
        merged.update(stored)
        return merged

    def _wait(self, seconds: float) -> bool:
        if seconds <= 0:
            return self._stop_event.is_set()
        return self._stop_event.wait(timeout=seconds)

    def _fetch_accounts(
        self,
        *,
        api_url: str,
        admin_key: str,
        timeout_seconds: int,
    ) -> list[dict[str, Any]]:
        url = f"{api_url.rstrip('/')}/api/admin/accounts"
        response = requests.get(
            url,
            headers={"X-Admin-Key": admin_key},
            timeout=max(3, int(timeout_seconds)),
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("pool response is not an object")
        accounts = payload.get("accounts")
        if not isinstance(accounts, list):
            raise RuntimeError("pool response missing accounts list")
        return [item for item in accounts if isinstance(item, dict)]

    def _run_once(self, merged_config: dict[str, Any]) -> None:
        enabled = self._as_bool(merged_config.get("pool_monitor_enabled"))
        interval = self._as_int(
            merged_config.get("pool_monitor_interval_seconds"),
            default=60,
            min_value=5,
            max_value=86400,
        )
        debug_logging = self._as_bool(merged_config.get("pool_monitor_debug_logging"))
        self._update_state(enabled=enabled)
        if not enabled:
            self._update_state(status="disabled", message="pool monitor disabled by config")
            self._wait(min(interval, 10))
            return

        api_url = str(merged_config.get("codexproxy_api_url") or "").strip()
        admin_key = str(merged_config.get("codexproxy_admin_key") or "").strip()
        if not api_url or not admin_key:
            self._update_state(
                status="misconfigured",
                message="codexproxy_api_url or codexproxy_admin_key is empty",
            )
            self._wait(min(interval, 15))
            return

        timeout_seconds = self._as_int(
            merged_config.get("pool_monitor_request_timeout_seconds"),
            default=15,
            min_value=3,
            max_value=120,
        )
        statuses = self._normalize_statuses(merged_config.get("pool_monitor_account_status"))
        threshold = self._as_int(
            merged_config.get("pool_monitor_threshold"),
            default=10,
            min_value=0,
            max_value=100000,
        )
        target_count = self._as_int(
            merged_config.get("pool_monitor_target_count"),
            default=max(threshold, 1),
            min_value=1,
            max_value=100000,
        )
        cooldown_seconds = self._as_int(
            merged_config.get("pool_monitor_cooldown_seconds"),
            default=300,
            min_value=0,
            max_value=86400,
        )

        checked_at = time.time()
        accounts = self._fetch_accounts(api_url=api_url, admin_key=admin_key, timeout_seconds=timeout_seconds)
        active_count = sum(1 for item in accounts if str(item.get("status") or "").strip().lower() in statuses)
        total_count = len(accounts)
        self._update_state(
            status="healthy" if active_count >= threshold else "low",
            message=f"active={active_count}, threshold={threshold}, total={total_count}",
            last_check_at=checked_at,
            last_active_count=active_count,
            last_total_count=total_count,
            last_error="",
            check_count=int(self.get_status().get("check_count") or 0) + 1,
        )

        if debug_logging:
            logger.info(
                "Pool monitor check: active=%s threshold=%s total=%s statuses=%s",
                active_count,
                threshold,
                total_count,
                sorted(statuses),
            )

        if active_count >= threshold:
            self._wait(interval)
            return

        if self._manager.has_active_tasks():
            self._update_state(status="busy", message="skip trigger: registration task already running")
            self._wait(interval)
            return

        now = time.time()
        if cooldown_seconds > 0 and now - float(self._last_trigger_at or 0.0) < cooldown_seconds:
            remain = int(cooldown_seconds - (now - float(self._last_trigger_at or 0.0)))
            self._update_state(status="cooldown", message=f"skip trigger: cooldown {max(0, remain)}s")
            self._wait(interval)
            return

        need_count = max(1, target_count - active_count)
        task_id = self._manager.create_auto_replenish_task(
            need_count,
            merged_config,
            trigger_meta={
                "active_count": active_count,
                "threshold": threshold,
                "target_count": target_count,
                "status_filter": sorted(statuses),
                "pool_total_count": total_count,
            },
        )
        self._last_trigger_at = now
        self._update_state(
            status="triggered",
            message=f"triggered replenish task {task_id} with count={need_count}",
            last_task_id=task_id,
            trigger_count=int(self.get_status().get("trigger_count") or 0) + 1,
        )
        logger.info(
            "Pool monitor triggered task: task_id=%s active=%s threshold=%s target=%s need=%s",
            task_id,
            active_count,
            threshold,
            target_count,
            need_count,
        )
        self._wait(interval)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                merged = self._load_config()
                self._run_once(merged)
            except Exception as exc:
                self._update_state(
                    status="error",
                    message=f"monitor error: {exc}",
                    last_error=str(exc),
                    last_check_at=time.time(),
                    check_count=int(self.get_status().get("check_count") or 0) + 1,
                )
                logger.exception("Pool monitor loop error")
                self._wait(10)
