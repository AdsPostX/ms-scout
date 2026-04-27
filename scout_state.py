"""
scout_state.py — All JSON state I/O for Scout.

This is the ONLY module that reads/writes the data/ directory
(besides offer_scraper.py). All other modules get state by calling
functions here — they do not touch pathlib or json directly.

Pattern: _load_*() returns a dict/list; _save_*() writes atomically.
Atomic writes: write to .tmp → os.replace() to prevent partial writes on crash.
"""

import json
import logging
import os
import pathlib

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


# ── Pulse state ────────────────────────────────────────────────────────────────

def _load_pulse_state() -> dict:
    try:
        if _PULSE_STATE_FILE.exists():
            return json.loads(_PULSE_STATE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_pulse_state(state: dict):
    try:
        _PULSE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_PULSE_STATE_FILE, state)
    except Exception as e:
        log.warning(f"Could not persist pulse_state: {e}")


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
    import re
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()


def _sanitize_slack(text: str) -> str:
    """Convert markdown to Slack-compatible formatting."""
    import re as _re
    text = _re.sub(r'\*\*(.+?)\*\*', r'*\1*', text)
    text = _re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)', r'<\2|\1>', text)
    text = _re.sub(r'^#{1,3} (.+)$', r'*\1*', text, flags=_re.MULTILINE)
    text = _re.sub(r'^---+$', '', text, flags=_re.MULTILINE)
    return text


def _slack_thread_url(channel: str, thread_ts: str) -> str:
    """Build a direct link to a Slack thread message."""
    ts_nodot = thread_ts.replace(".", "")
    return f"https://momentscience.slack.com/archives/{channel}/p{ts_nodot}"
