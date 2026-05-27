"""
scout_state.py — All JSON state I/O for Scout.

This is the ONLY module that reads/writes the data/ directory
(besides offer_scraper.py). All other modules get state by calling
functions here — they do not touch pathlib or json directly.

Pattern: _load_*() returns a dict/list; _save_*() writes atomically.
Atomic writes: write to .tmp → os.replace() to prevent partial writes on crash.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import random
import re
import threading
import time
from datetime import datetime

log = logging.getLogger("scout_state")

# ── Data directory ─────────────────────────────────────────────────────────────
_DATA_DIR = pathlib.Path(__file__).parent / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── State file paths ───────────────────────────────────────────────────────────
_STATE_FILE              = _DATA_DIR / "pending_briefs.json"
_THREAD_CTX_FILE         = _DATA_DIR / "thread_context.json"
_LAUNCHED_OFFERS_FILE    = _DATA_DIR / "launched_offers.json"
_PULSE_STATE_FILE        = _DATA_DIR / "pulse_state.json"
_WATCHDOG_STATE_PATH     = _DATA_DIR / "watchdog_state.json"
_NOTION_NOTIFIED_FILE    = _DATA_DIR / "notion_notified.json"
_LEARNINGS_FILE          = _DATA_DIR / "learnings.json"
_LEARNED_BENCHMARKS_FILE = _DATA_DIR / "learned_benchmarks.json"
_THRESHOLD_OVERRIDES_FILE = _DATA_DIR / "threshold_overrides.json"
_THRESHOLD_CHANGELOG_FILE = _DATA_DIR / "threshold_changelog.jsonl"
_MAINTENANCE_FILE        = _DATA_DIR / "maintenance_state.json"

# ── Pulse-state concurrency lock ───────────────────────────────────────────────
# Covers the read-modify-write race on pulse_state.json.  Within a single
# process (Render deploys a single worker) this lock is sufficient; across
# restart events the per-signal Slack dedup key in demand_feed_main.py acts
# as a belt-and-suspenders guard.
_PULSE_STATE_LOCK = threading.Lock()

# ── Maintenance-state concurrency lock ─────────────────────────────────────────
# Socket Mode dispatches events on concurrent threads; every read-modify-write
# cycle on maintenance_state.json must hold this lock.
_MAINTENANCE_LOCK = threading.Lock()


# ── Maintenance helpers ────────────────────────────────────────────────────────

def get_maintenance() -> dict | None:
    """Returns maintenance state or None if not active. Thread-safe."""
    with _MAINTENANCE_LOCK:
        if not _MAINTENANCE_FILE.exists():
            return None
        try:
            m = json.loads(_MAINTENANCE_FILE.read_text())
        except Exception:
            return None
        if not m.get("active"):
            return None
        return m


def set_maintenance(set_by: str) -> dict:
    """Activates maintenance mode. Stays on until clear_maintenance() is called. Thread-safe."""
    with _MAINTENANCE_LOCK:
        now = datetime.utcnow()
        m = {
            "active": True,
            "set_at": now.isoformat(),
            "set_by": set_by,
            "attempts": [],
        }
        _MAINTENANCE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_MAINTENANCE_FILE, m)
        return m


def clear_maintenance() -> list:
    """Clears maintenance state. Returns attempts list for reporting. Thread-safe."""
    with _MAINTENANCE_LOCK:
        try:
            m = json.loads(_MAINTENANCE_FILE.read_text()) if _MAINTENANCE_FILE.exists() else {}
        except Exception:
            m = {}
        _MAINTENANCE_FILE.unlink(missing_ok=True)
        return m.get("attempts", [])


def log_maintenance_attempt(user_id: str, query_preview: str) -> None:
    """Appends a blocked-attempt record to the active maintenance state. Thread-safe."""
    with _MAINTENANCE_LOCK:
        if not _MAINTENANCE_FILE.exists():
            return
        try:
            m = json.loads(_MAINTENANCE_FILE.read_text())
        except Exception:
            return
        if not m.get("active"):
            return
        m.setdefault("attempts", []).append({
            "ts": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "query": query_preview[:100],
        })
        _atomic_write(_MAINTENANCE_FILE, m)


# ── Atomic write ───────────────────────────────────────────────────────────────

def _atomic_write(path: pathlib.Path, data: dict) -> None:
    """Write JSON atomically — temp file + os.replace prevents partial writes on crash."""
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


# ── Pending briefs ─────────────────────────────────────────────────────────────
# Briefs are written to disk so process restarts (launchd, deploys) never
# cause "No brief found" on the Launch button click.

def _load_briefs() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_briefs(briefs: dict):
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_STATE_FILE, briefs)
    except Exception as e:
        log.warning(f"Could not persist brief state: {e}")


def _store_brief(thread_ts: str, brief_data: dict, copy: dict):
    briefs = _load_briefs()
    briefs[thread_ts] = {"brief_data": brief_data, "copy": copy}
    _save_briefs(briefs)


def _get_brief(thread_ts: str) -> dict | None:
    return _load_briefs().get(thread_ts)


def _delete_brief(thread_ts: str):
    briefs = _load_briefs()
    briefs.pop(thread_ts, None)
    _save_briefs(briefs)


# ── Thread entity context ──────────────────────────────────────────────────────
# Stores structured entities extracted from tool results — publisher, offer,
# payout, category, scenarios run — keyed by thread_ts.
# Injected at position 0 in history so follow-ups like "@Scout yes, $50 CPA"
# always have the entities from earlier in the thread available.

def _load_thread_contexts() -> dict:
    try:
        if _THREAD_CTX_FILE.exists():
            return json.loads(_THREAD_CTX_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_thread_contexts(contexts: dict):
    try:
        _THREAD_CTX_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_THREAD_CTX_FILE, contexts)
    except Exception as e:
        log.warning(f"Could not persist thread context: {e}")


def _get_thread_context(thread_ts: str) -> dict | None:
    return _load_thread_contexts().get(thread_ts)


def _merge_thread_context(thread_ts: str, new_data: dict):
    """Merge new entity data into existing thread context.
    Accumulates scenarios_run as a list — never overwrites prior values.
    """
    if not new_data:
        return
    existing = _load_thread_contexts()
    ctx = existing.get(thread_ts) or {}
    # Accumulate scenarios_run rather than overwrite
    incoming_scenarios = new_data.pop("scenarios_run", [])
    ctx.update({k: v for k, v in new_data.items() if v is not None})
    if incoming_scenarios:
        seen = ctx.get("scenarios_run") or []
        for s in incoming_scenarios:
            if s not in seen:
                seen.append(s)
        ctx["scenarios_run"] = seen
    from datetime import datetime, timezone
    ctx["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    existing[thread_ts] = ctx
    _save_thread_contexts(existing)


# ── Launched offers ────────────────────────────────────────────────────────────

def _load_launched_offers() -> dict:
    try:
        if _LAUNCHED_OFFERS_FILE.exists():
            return json.loads(_LAUNCHED_OFFERS_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_launched_offers(state: dict):
    try:
        _LAUNCHED_OFFERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_LAUNCHED_OFFERS_FILE, state)
    except Exception as e:
        log.warning(f"Could not persist launched_offers: {e}")


# ── Revenue alert state ────────────────────────────────────────────────────────
# Tracks the last calendar date (YYYY-MM-DD, CT timezone) the revenue tracker
# daemon fired an alert. Persisted in pulse_state.json under "last_revenue_alert_date".
# Used to enforce once-per-day firing — no double-posts if daemon restarts mid-day.

def _load_revenue_alert_state() -> str | None:
    """Return the date string (YYYY-MM-DD) of the last revenue alert, or None."""
    return _load_pulse_state().get("last_revenue_alert_date")


def _save_revenue_alert_date(date_str: str) -> None:
    """Persist today's date as the last revenue alert date in pulse_state.json."""
    _update_pulse_state("last_revenue_alert_date", date_str)


# ── Per-monitor alert state ───────────────────────────────────────────────────
# Each silent monitor (cap, velocity_down, ghost, fill) gets its own state key in
# pulse_state.json so the first fire of the day doesn't suppress the others.
# Without separate keys, all four monitors would share `last_revenue_alert_date`
# and only the first one to fire on a given day would post.

def _load_cap_alert_state() -> str | None:
    return _load_pulse_state().get("last_cap_alert_date")


def _save_cap_alert_date(date_str: str) -> None:
    _update_pulse_state("last_cap_alert_date", date_str)


def _load_velocity_down_alert_state() -> str | None:
    return _load_pulse_state().get("last_velocity_down_alert_date")


def _save_velocity_down_alert_date(date_str: str) -> None:
    _update_pulse_state("last_velocity_down_alert_date", date_str)


def _load_ghost_alert_state() -> str | None:
    return _load_pulse_state().get("last_ghost_alert_date")


def _save_ghost_alert_date(date_str: str) -> None:
    _update_pulse_state("last_ghost_alert_date", date_str)


def _load_fill_alert_state() -> str | None:
    return _load_pulse_state().get("last_fill_alert_date")


def _save_fill_alert_date(date_str: str) -> None:
    _update_pulse_state("last_fill_alert_date", date_str)


def _load_cvr_anomaly_alert_state() -> str | None:
    return _load_pulse_state().get("last_cvr_anomaly_alert_date")


def _save_cvr_anomaly_alert_date(date_str: str) -> None:
    _update_pulse_state("last_cvr_anomaly_alert_date", date_str)


def _load_expiration_alert_state() -> str | None:
    return _load_pulse_state().get("last_expiration_alert_date")


def _save_expiration_alert_date(date_str: str) -> None:
    _update_pulse_state("last_expiration_alert_date", date_str)


# ── Projection autocheck slot ─────────────────────────────────────────────────
# Hourly CT-anchored slot key ("YYYY-MM-DDTHH") for the projection autocheck
# monitor — fires once per CT hour into #sidd-qa. Persisted so a mid-hour
# restart does not re-fire the current slot.

def _load_projection_autocheck_slot() -> str | None:
    return _load_pulse_state().get("last_projection_autocheck_slot")


def _save_projection_autocheck_slot(slot: str) -> None:
    _update_pulse_state("last_projection_autocheck_slot", slot)


# ── Projection autocheck EOD-posted marker ────────────────────────────────────
# CT date ("YYYY-MM-DD") of the last day the 17:30 CT EOD rollup posted.
# Persisted so a restart after EOD does not re-post the EOD summary.

def _load_eod_posted_date() -> str | None:
    return _load_pulse_state().get("last_projection_autocheck_eod_date")


def _save_eod_posted_date(date_str: str) -> None:
    _update_pulse_state("last_projection_autocheck_eod_date", date_str)


# ── Projection autocheck per-day fires log ────────────────────────────────────
# Per-day list of autocheck fire records used by the 17:30 CT EOD rollup.
# Persisted so a restart mid-day does not produce an empty EOD summary.
# Stored under pulse_state.json as {"projection_autocheck_fires": {date: [...]}}.

def _load_projection_autocheck_fires(date_str: str) -> list[dict]:
    """Return persisted autocheck fire records for the given CT date."""
    return _load_pulse_state().get("projection_autocheck_fires", {}).get(date_str, [])


def _append_projection_autocheck_fire(date_str: str, entry: dict) -> None:
    """Append a single autocheck fire record under the given CT date."""
    with _PULSE_STATE_LOCK:
        state = _load_pulse_state()
        fires = state.setdefault("projection_autocheck_fires", {})
        fires.setdefault(date_str, []).append(entry)
        _PULSE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_PULSE_STATE_FILE, state)


def _evict_stale_projection_autocheck_fires(today_str: str) -> None:
    """Drop fire records for any CT date other than today_str."""
    with _PULSE_STATE_LOCK:
        state = _load_pulse_state()
        fires = state.get("projection_autocheck_fires")
        if not fires:
            return
        stale = [d for d in fires if d != today_str]
        if not stale:
            return
        for d in stale:
            fires.pop(d, None)
        _PULSE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_PULSE_STATE_FILE, state)


# ── Digest poster state ───────────────────────────────────────────────────────
# CT date ("YYYY-MM-DD") of the last day the morning digest posted.

def _load_digest_post_state() -> str | None:
    return _load_pulse_state().get("last_digest_post_date")


def _save_digest_post_date(date_str: str) -> None:
    _update_pulse_state("last_digest_post_date", date_str)


# ── Pulse state ────────────────────────────────────────────────────────────────

def _load_pulse_state() -> dict:
    """Load pulse_state.json.

    Raises json.JSONDecodeError on corrupt data (fail-closed behaviour — callers
    must not silently swallow this; a corrupt state file should be treated as an
    error, not as 'monitor hasn't fired yet').  Returns {} when the file does
    not exist yet (first-run).
    """
    if _PULSE_STATE_FILE.exists():
        return json.loads(_PULSE_STATE_FILE.read_text())  # let JSONDecodeError propagate
    return {}


def _save_pulse_state(state: dict):
    """Write the full pulse state dict atomically.

    Acquires _PULSE_STATE_LOCK so direct callers don't race with
    _update_pulse_state.  Prefer _update_pulse_state for single-key writes.
    Propagates write errors so callers are not misled into thinking persistence
    succeeded (fail-closed, consistent with _update_pulse_state).
    """
    with _PULSE_STATE_LOCK:
        _PULSE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_PULSE_STATE_FILE, state)


def _update_pulse_state(key: str, value) -> None:
    """Thread-safe single-key RMW on pulse_state.json.

    Acquires _PULSE_STATE_LOCK so concurrent monitor threads cannot interleave
    their load→mutate→save cycles and clobber each other's date keys.
    Propagates json.JSONDecodeError on corrupt state (fail-closed — let the
    monitor record status='error' rather than fire as if it never did before).
    """
    with _PULSE_STATE_LOCK:
        state = _load_pulse_state()
        state[key] = value
        _PULSE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_PULSE_STATE_FILE, state)


# ── Watchdog state ─────────────────────────────────────────────────────────────

def _load_watchdog_state() -> dict:
    try:
        if _WATCHDOG_STATE_PATH.exists():
            return json.loads(_WATCHDOG_STATE_PATH.read_text())
    except Exception:
        pass
    return {}


def _save_watchdog_state(state: dict) -> None:
    _WATCHDOG_STATE_PATH.write_text(json.dumps(state, indent=2))


# ── Learnings store ────────────────────────────────────────────────────────────

def _load_learnings() -> dict:
    try:
        if _LEARNINGS_FILE.exists():
            return json.loads(_LEARNINGS_FILE.read_text())
    except Exception:
        pass
    return {"corrections": [], "positive_signals": []}


def _save_learnings(data: dict):
    try:
        _LEARNINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_LEARNINGS_FILE, data)
    except Exception as e:
        log.warning(f"Could not persist learnings: {e}")


# ── Notion watcher notified state ──────────────────────────────────────────────

def _load_notion_notified() -> dict:
    """Load the set of Notion page IDs we've already posted status updates for."""
    try:
        if _NOTION_NOTIFIED_FILE.exists():
            return json.loads(_NOTION_NOTIFIED_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_notion_notified(state: dict) -> None:
    try:
        _NOTION_NOTIFIED_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_NOTION_NOTIFIED_FILE, state)
    except Exception as e:
        log.warning(f"[notion-watcher] save error: {e}")


# ── Usage log ──────────────────────────────────────────────────────────────────

def _log_usage(user_id: str, user_name: str, query: str, tools: list, elapsed_ms: int) -> None:
    """Append one query record to data/usage_log.jsonl for admin reporting."""
    import datetime as _dt2
    record = {
        "ts": _dt2.datetime.utcnow().isoformat(),
        "user_id": user_id,
        "user_name": user_name,
        "query": query[:200],
        "tools": tools,
        "ms": elapsed_ms,
    }
    try:
        log_path = _DATA_DIR / "usage_log.jsonl"
        with open(log_path, "a") as f:
            f.write(json.dumps(record) + "\n")
    except Exception as e:
        log.warning(f"[usage] log write failed: {e}")


# ── Benchmark recalibration from 14-day actuals ───────────────────────────────

def _update_benchmark_from_actuals(advertiser: str, actual_rpm: float, payout_type: str = "") -> None:
    """
    After a 14-day recap, fold the actual RPM into learned_benchmarks.json.
    Stored as a rolling average per (advertiser, payout_type).
    Scout loads this on startup to improve future estimates.
    """
    try:
        key = f"{advertiser.lower()}:{payout_type.lower()}" if payout_type else advertiser.lower()
        data: dict = {}
        if _LEARNED_BENCHMARKS_FILE.exists():
            try:
                data = json.loads(_LEARNED_BENCHMARKS_FILE.read_text())
            except Exception:
                data = {}

        entry = data.get(key, {"rpm_actual_avg": 0.0, "sample_count": 0})
        n     = entry["sample_count"]
        avg   = entry["rpm_actual_avg"]
        # Rolling average (max 20 samples — recent data is more relevant)
        n_new = min(n + 1, 20)
        w     = 1 / n_new  # weight for new sample
        new_avg = avg * (1 - w) + actual_rpm * w
        data[key] = {"rpm_actual_avg": round(new_avg, 2), "sample_count": n_new}

        _LEARNED_BENCHMARKS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_LEARNED_BENCHMARKS_FILE, data)
        log.info(f"Learned benchmark updated: {key} → avg RPM ${new_avg:.2f} (n={n_new})")
    except Exception as e:
        log.warning(f"_update_benchmark_from_actuals failed for {advertiser}: {e}")


# ── Slack utilities (shared across Scout modules) ───────────────────────────

def _strip_mention(text: str) -> str:
    """Remove @mention tokens so the agent sees the clean query."""
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()


def _sanitize_slack(text: str) -> str:
    """Convert markdown to Slack-compatible formatting."""
    text = re.sub(r'\*\*(.+?)\*\*', r'*\1*', text)
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)', r'<\2|\1>', text)
    text = re.sub(r'^#{1,3} (.+)$', r'*\1*', text, flags=re.MULTILINE)
    text = re.sub(r'^---+$', '', text, flags=re.MULTILINE)
    return text


def _slack_thread_url(channel: str, thread_ts: str) -> str:
    """Build a direct link to a Slack thread message."""
    ts_nodot = thread_ts.replace(".", "")
    return f"https://momentscience.slack.com/archives/{channel}/p{ts_nodot}"


# ── Environment-aware channel routing ─────────────────────────────────────────
_SCOUT_ENV = os.getenv("SCOUT_ENV", "development")
_SCOUT_HQ_CHANNEL = "C0AQEECF800"  # #bot-qa (renamed from #scout-qa)
_PULSE_CHANNEL = os.getenv("PULSE_CHANNEL", _SCOUT_HQ_CHANNEL)
_SCOUT_DIGEST_CHANNEL = os.getenv("SCOUT_DIGEST_CHANNEL", _SCOUT_HQ_CHANNEL)


def _route_channel(purpose: str, force: bool = False) -> str:
    """Return the correct Slack channel for a given message purpose."""
    if force or _SCOUT_ENV != "production":
        return _SCOUT_HQ_CHANNEL
    return _PULSE_CHANNEL if purpose in ("pulse", "watchdog") else (_SCOUT_DIGEST_CHANNEL if purpose == "offers" else _SCOUT_HQ_CHANNEL)


# ── Loading messages (from scout_bot for handler use) ──────────────────────────────

_MESSAGE_POOLS = {
    "pool_generic": [
        {"text": "Checking the vault...", "tone": "grind"}, {"text": "Pulling signals...", "tone": "grind"},
        {"text": "Running the numbers...", "tone": "grind"}, {"text": "Mining the data...", "tone": "grind"},
        {"text": "Asking the oracle...", "tone": "grind"}, {"text": "Crunching the numbers...", "tone": "grind"},
        {"text": "Consulting the archives...", "tone": "late"}, {"text": "Wake up, Neo...", "tone": "late"},
        {"text": "Deep thought in progress...", "tone": "late"},
    ],
    "pool_ops": [
        {"text": "Checking the queue...", "tone": "grind"}, {"text": "Scanning active campaigns...", "tone": "grind"},
        {"text": "Loading pipeline...", "tone": "grind"}, {"text": "Syncing with Notion...", "tone": "grind"},
        {"text": "Midnight oil...", "tone": "late"}, {"text": "Night watch...", "tone": "late"},
    ],
    "pool_data": [
        {"text": "Computing revenue...", "tone": "grind"}, {"text": "Crunching performance...", "tone": "grind"},
        {"text": "Benchmarking...", "tone": "grind"}, {"text": "Running regression...", "tone": "grind"},
        {"text": "Data mine running...", "tone": "late"}, {"text": "Calculating...", "tone": "late"},
    ],
    "pool_brief": [
        {"text": "Drafting brief...", "tone": "grind"}, {"text": "Building campaign...", "tone": "grind"},
        {"text": "Writing copy...", "tone": "grind"}, {"text": "Loading creative...", "tone": "grind"},
        {"text": "Late night drafting...", "tone": "late"}, {"text": "Burning the midnight oil...", "tone": "late"},
    ],
    "pool_publisher": [
        {"text": "Checking integrations...", "tone": "grind"}, {"text": "Loading partners...", "tone": "grind"},
        {"text": "Verifying connections...", "tone": "grind"}, {"text": "Mapping the network...", "tone": "grind"},
        {"text": "Late night debugging...", "tone": "late"}, {"text": "Syncing...", "tone": "late"},
    ],
}


def _pick_loading_message(query: str = "") -> str:
    """Pick a context-aware loading message based on query content and time of day."""
    from datetime import datetime
    import pytz

    try:
        chicago = pytz.timezone("America/Chicago")
        hour = datetime.now(chicago).hour
        is_late = hour >= 21 or hour < 6
    except Exception:
        is_late = False

    q = (query or "").lower()

    if any(w in q for w in ("brief", "campaign", "build a brief", "draft", "write")):
        pool_key = "pool_brief"
    elif any(w in q for w in ("queue", "status", "pending", "live", "launch", "enter")):
        pool_key = "pool_ops"
    elif any(w in q for w in ("revenue", "projection", "cap", "budget", "forecast")):
        pool_key = "pool_data"
    elif any(w in q for w in ("performance", "rpm", "cvr", "data", "benchmark", "report", "rank")):
        pool_key = "pool_data"
    elif any(w in q for w in ("publisher", "partner", "integration", "network")):
        pool_key = "pool_publisher"
    elif any(w in q for w in ("find", "opportunity", "gap", "search")):
        pool_key = "pool_generic"
    else:
        pool_key = "pool_generic"

    pool = _MESSAGE_POOLS.get(pool_key, _MESSAGE_POOLS["pool_generic"])
    tone = "late" if is_late else "grind"
    candidates = [e for e in pool if e["tone"] == tone] or pool
    return random.choice(candidates)["text"]


# ── Smart history truncation (from scout_bot for handler use) ────────

def _smart_history(history: list, max_full: int = 4) -> list:
    """Keep last max_full messages verbatim; summarize older ones as a single context line."""
    if len(history) <= max_full:
        return history
    older, recent = history[:-max_full], history[-max_full:]
    entities = set()
    for msg in older:
        content = msg.get("content", "")
        if isinstance(content, str):
            entities.update(re.findall(r'\b[A-Z][a-zA-Z+]{2,}\b', content))
    summary = (
        f"[Earlier context: {', '.join(list(entities)[:8])}]"
        if entities
        else "[Earlier messages truncated]"
    )
    return [
        {"role": "user", "content": summary},
        {"role": "assistant", "content": "Understood."},
    ] + recent


def _rotating_status(
    web,
    channel: str,
    ts: str,
    interval: float = 2.0,
):
    """Rotating status with typing indicator — returns stop function."""
    stop_event = threading.Event()
    start = time.monotonic()
    pool = _MESSAGE_POOLS.get("pool_generic", [])
    msgs = [e["text"] for e in pool] or ["Thinking..."]
    random.shuffle(msgs)
    idx = [0]

    def _run():
        while not stop_event.wait(interval):
            elapsed = int(time.monotonic() - start)
            msg = msgs[idx[0] % len(msgs)]
            update_text = f"_{msg}_ · {elapsed}s"
            try:
                web.chat_update(
                    channel=channel, ts=ts, text=update_text,
                    blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": update_text}}],
                )
            except Exception:
                pass
            idx[0] += 1

    _t = threading.Thread(target=_run, daemon=True)
    _t.start()

    def _stop():
        stop_event.set()
        _t.join(timeout=0.5)  # wait for any in-flight chat_update to complete

    return _stop


def _post_error_update(web, channel: str, ts: str, err: Exception) -> None:
    """Replace the loading placeholder with a clean error message block."""
    s = str(err)
    if "429" in s or "rate_limit" in s:
        msg = "Scout hit the rate limit — give it 60 seconds and try again."
    elif "credit balance" in s.lower() or ("400" in s and "credit" in s.lower()):
        msg = "Scout is out of Anthropic credits — ping Sidd to top up at console.anthropic.com."
    elif "529" in s or "overloaded" in s:
        msg = "Anthropic is slammed right now — try again in a minute."
    elif "timeout" in s.lower() or "timed out" in s.lower():
        msg = "Scout timed out — try a narrower question."
    elif "connection" in s.lower() or "network" in s.lower():
        msg = "Scout just restarted (deploy or crash) — please resend your message."
    else:
        msg = "Something broke — try again, or rephrase the question."
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f":warning: *Scout hit a snag* — {msg}"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": "If this keeps happening, check Render logs."}]},
    ]
    try:
        web.chat_update(channel=channel, ts=ts, text=msg, blocks=blocks)
    except Exception as _e:
        log.warning(f"_post_error_update: could not update {channel}:{ts}: {_e}")


# ── Threshold overrides + changelog (PR-B) ─────────────────────────────────────
# Runtime overrides written by the `set_threshold` agent tool. Layered on top of
# config/scout_thresholds.json at _load_thresholds() time. Defaults stay in git;
# overrides live on Render's persistent disk.
#
# Override file shape:
#   {"signals": {"cap_alert_pct": {"value": 80, "set_by": "U123", "set_at": "...", "reason": "..."}}}
#
# Changelog is append-only JSON Lines — one event per line. Never rewritten.

def _load_threshold_overrides() -> dict:
    """Return the current overrides dict, or {} if file missing or unreadable."""
    try:
        if _THRESHOLD_OVERRIDES_FILE.exists():
            return json.loads(_THRESHOLD_OVERRIDES_FILE.read_text())
    except Exception as e:
        log.warning(f"_load_threshold_overrides failed: {e}")
    return {}


def _save_threshold_overrides(overrides: dict) -> None:
    """Atomic write of the full overrides dict."""
    try:
        _atomic_write(_THRESHOLD_OVERRIDES_FILE, overrides)
    except Exception as e:
        log.warning(f"_save_threshold_overrides failed: {e}")


def _append_threshold_changelog(entry: dict) -> None:
    """Append one JSON line to the changelog. Single-writer (Scout process), append-only."""
    try:
        line = json.dumps(entry, separators=(",", ":")) + "\n"
        with open(_THRESHOLD_CHANGELOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        log.warning(f"_append_threshold_changelog failed: {e}")


def _read_threshold_changelog(limit: int = 50, key: str | None = None) -> list[dict]:
    """Read the last `limit` changelog entries, optionally filtered by threshold key.

    Returns newest-first. Tolerates malformed lines (skips them).
    """
    try:
        if not _THRESHOLD_CHANGELOG_FILE.exists():
            return []
        lines = _THRESHOLD_CHANGELOG_FILE.read_text(encoding="utf-8").splitlines()
    except Exception as e:
        log.warning(f"_read_threshold_changelog failed: {e}")
        return []

    out: list[dict] = []
    for raw in reversed(lines):
        if not raw.strip():
            continue
        try:
            entry = json.loads(raw)
        except Exception as e:
            log.warning(f"_read_threshold_changelog: skipping malformed line ({e}): {raw[:120]!r}")
            continue
        if key and entry.get("key") != key:
            continue
        out.append(entry)
        if len(out) >= limit:
            break
    return out
