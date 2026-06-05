"""
alert_registry — firing/cleared state for Scout monitors.

Each monitor calls mark_firing() when it raises an alert and mark_cleared()
when the signal returns to normal. _build_home_view reads current_state() to
render the health line on App Home.

Storage: Upstash Redis hash (scout:alert_registry) when
UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN are set. State then
survives Render deploys and is visible to any process sharing those credentials.
Falls back to a process-local dict when the env vars are absent (dev/testing).

Failure mode: all three public functions are best-effort and never raise; the
Home scoreboard must not crash because the registry hiccuped.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

_REDIS_KEY = "scout:alert_registry"

# Lazy-init, cached after first call. None = use in-memory fallback.
_redis_client = None
_redis_init_done = False
_redis_init_lock = threading.Lock()  # guards the lazy-init critical section

# In-memory fallback (used when Upstash env vars are not set).
_LOCK = threading.Lock()
_STATE: dict[str, AlertState] = {}


@dataclass(frozen=True)
class AlertState:
    alert_name: str
    status: str  # "firing" | "cleared"
    context: dict[str, Any] = field(default_factory=dict)
    last_change: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def _get_redis():
    """Return Upstash Redis client, or None if not configured."""
    global _redis_client, _redis_init_done
    if _redis_init_done:
        return _redis_client
    with _redis_init_lock:
        # Re-check inside the lock — another thread may have initialised while
        # we were waiting.
        if _redis_init_done:
            return _redis_client
        try:
            url = os.environ.get("UPSTASH_REDIS_REST_URL")
            token = os.environ.get("UPSTASH_REDIS_REST_TOKEN")
            if url and token:
                from upstash_redis import Redis  # noqa: PLC0415
                _redis_client = Redis(url=url, token=token)
                log.info(
                    "alert_registry: Upstash Redis connected (%s)",
                    url.split("//")[-1].split(".")[0],
                )
        except Exception:
            log.warning("alert_registry: Upstash Redis init failed — using in-memory fallback")
            _redis_client = None
        finally:
            _redis_init_done = True
    return _redis_client


def _persist_registry_state() -> None:
    """Persist current registry state to pulse_state.json for deploy-survivability.

    Must be called while holding _LOCK to prevent TOCTOU races on _STATE.
    The Redis path (when configured) handles its own persistence and this is a no-op.
    """
    try:
        from scout_state import _load_pulse_state, _save_pulse_state
        state = _load_pulse_state()
        state["alert_registry"] = {
            name: {
                "status": s.status,
                "context": s.context,
                "last_change": s.last_change.isoformat(),
            }
            for name, s in _STATE.items()
        }
        _save_pulse_state(state)
    except Exception:
        log.exception("alert_registry: failed to persist state")


def _load_registry_from_state() -> None:
    """Restore registry state from pulse_state.json on startup after a deploy.

    Safe without _LOCK: called at module import time, before any monitor threads start.
    """
    try:
        from scout_state import _load_pulse_state
        stored = _load_pulse_state().get("alert_registry", {})
        for name, data in stored.items():
            _STATE[name] = AlertState(
                alert_name=name,
                status=data["status"],
                context=data.get("context", {}),
                last_change=datetime.fromisoformat(data["last_change"]),
            )
        if stored:
            log.info("alert_registry: restored %d alerts from pulse_state", len(stored))
    except Exception:
        log.exception("alert_registry: failed to restore state from pulse_state")


def mark_firing(alert_name: str, context: dict[str, Any] | None = None) -> None:
    """Record that `alert_name` is firing. Idempotent — repeated calls are harmless."""
    if not alert_name:
        return
    try:
        now = datetime.now(timezone.utc)
        r = _get_redis()
        if r is not None:
            payload = json.dumps({
                "status": "firing",
                "context": dict(context or {}),
                "last_change": now.isoformat(),
            })
            r.hset(_REDIS_KEY, alert_name, payload)
        else:
            with _LOCK:
                _STATE[alert_name] = AlertState(
                    alert_name=alert_name,
                    status="firing",
                    context=dict(context or {}),
                    last_change=now,
                )
                _persist_registry_state()
    except Exception:
        log.exception("alert_registry.mark_firing failed (alert=%s)", alert_name)


def mark_cleared(alert_name: str) -> None:
    """Record that `alert_name` has cleared. Idempotent."""
    if not alert_name:
        return
    try:
        r = _get_redis()
        if r is not None:
            r.hdel(_REDIS_KEY, alert_name)
        else:
            with _LOCK:
                _STATE.pop(alert_name, None)
                _persist_registry_state()
    except Exception:
        log.exception("alert_registry.mark_cleared failed (alert=%s)", alert_name)


def current_state(window_days: int = 7) -> list[AlertState]:
    """Return currently-firing alerts, newest first. Never raises."""
    try:
        r = _get_redis()
        if r is not None:
            raw = r.hgetall(_REDIS_KEY) or {}
            snapshot: list[AlertState] = []
            for alert_name, payload_str in raw.items():
                try:
                    d = json.loads(payload_str)
                    snapshot.append(AlertState(
                        alert_name=alert_name,
                        status=d.get("status", "firing"),
                        context=d.get("context", {}),
                        last_change=datetime.fromisoformat(d["last_change"]),
                    ))
                except Exception:
                    log.warning("alert_registry: skipping malformed entry (key=%s)", alert_name)
        else:
            with _LOCK:
                snapshot = list(_STATE.values())
    except Exception:
        log.exception("alert_registry.current_state failed")
        return []
    snapshot.sort(key=lambda s: s.last_change, reverse=True)
    return snapshot


# Restore registry state from pulse_state.json on module load (deploy-survivability).
# Only applies to the in-memory fallback path; Redis handles its own persistence.
_load_registry_from_state()
