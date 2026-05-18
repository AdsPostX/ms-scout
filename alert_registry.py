"""
alert_registry — in-memory firing/cleared state for Scout monitors.

Each monitor in scout_bot.py calls mark_firing() when it raises an alert and
mark_cleared() when the underlying signal returns to normal. _build_home_view
reads current_state() to render the "is anything on fire?" health line on App
Home.

Storage: process-local dict. Single Render worker, so consistency is trivial;
state resets on deploy (≈ daily), which is honest for a thin-slice PR 1. If
adoption signal justifies PR 2, swap to Upstash Redis (HSET/HDEL/HGETALL —
~20 lines, new UPSTASH_REDIS_URL env var, no infra ask).

Failure mode: all three public functions are best-effort and never raise; the
Home scoreboard must not crash because the registry hiccuped. Lock guards
against the rare interleaving when the monitor thread fires concurrently with
an `app_home_opened` event.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AlertState:
    alert_name: str
    status: str  # "firing" | "cleared"
    context: dict[str, Any] = field(default_factory=dict)
    last_change: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


_LOCK = threading.Lock()
_STATE: dict[str, AlertState] = {}


def mark_firing(alert_name: str, context: dict[str, Any] | None = None) -> None:
    """Record that `alert_name` is firing. Idempotent — repeated calls are harmless."""
    if not alert_name:
        return
    try:
        with _LOCK:
            _STATE[alert_name] = AlertState(
                alert_name=alert_name,
                status="firing",
                context=dict(context or {}),
                last_change=datetime.now(timezone.utc),
            )
    except Exception:
        log.exception("alert_registry.mark_firing failed (alert=%s)", alert_name)


def mark_cleared(alert_name: str) -> None:
    """Record that `alert_name` has cleared. Idempotent."""
    if not alert_name:
        return
    try:
        with _LOCK:
            _STATE.pop(alert_name, None)
    except Exception:
        log.exception("alert_registry.mark_cleared failed (alert=%s)", alert_name)


def current_state(window_days: int = 7) -> list[AlertState]:
    """Return currently-firing alerts, newest first. Never raises."""
    try:
        with _LOCK:
            snapshot = list(_STATE.values())
    except Exception:
        log.exception("alert_registry.current_state snapshot failed")
        return []
    snapshot.sort(key=lambda s: s.last_change, reverse=True)
    return snapshot
