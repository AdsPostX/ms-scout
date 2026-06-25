from __future__ import annotations

# Standard library
import json
import logging
import os
import pathlib
import re
import datetime as _dt_mod
from datetime import datetime, timezone

# Local — NO scout_agent (circular import)
from scout_ch import _get_ch_client, _run_parallel, _LoggingCHClient  # noqa: F401
from scout_thresholds import _manager

log = logging.getLogger("scout_agent")

# ── Environment config — read once at import, warn if required vars absent ────
_ADMIN_UID       = os.getenv("SCOUT_ADMIN_USER_ID", "")
_RAKUTEN_TOKEN   = os.getenv("RAKUTEN_API_TOKEN", "")
_AWIN_PUB_ID     = os.getenv("AWIN_PUBLISHER_ID", "")
_AWIN_API_KEY    = os.getenv("AWIN_API_KEY", "")
_DEMAND_FEED_URL = os.getenv("DEMAND_FEED_URL", "")

if not _ADMIN_UID:
    log.warning("[scout_tools_admin] SCOUT_ADMIN_USER_ID not set — usage reports locked for all users")
if not _RAKUTEN_TOKEN:
    log.warning("[scout_tools_admin] RAKUTEN_API_TOKEN not set — Rakuten excluded from scraper inventory")
if not (_AWIN_PUB_ID and _AWIN_API_KEY):
    log.warning("[scout_tools_admin] AWIN_PUBLISHER_ID/AWIN_API_KEY not set — Awin excluded from scraper inventory")

# ── Module-level constants used only by these functions ───────────────────────

SNAPSHOT_PATH = pathlib.Path(__file__).parent / "data" / "offers_latest.json"

_LARGE_TABLES = (
    "adpx_sdk_sessions",
    "adpx_impressions_details",
    "adpx_tracked_clicks",
    "adpx_conversionsdetails",
)

# ── Private helpers delegated to ThresholdManager ────────────────────────────

def _load_entity_overrides() -> dict:
    return _manager.entity_overrides()


def _save_entity_overrides(overrides: dict) -> None:
    return _manager.save_entity_overrides(overrides)


def _get_benchmarks() -> dict:
    return _manager.benchmarks()


def _load_launched_offers_state() -> dict:
    return _manager.load_launched_offers_state()


def _load_offers() -> list:
    """Load offers from DEMAND_FEED_URL when set; fall back to disk snapshot.

    Delegated copy — the canonical version lives in scout_agent._load_offers().
    This module re-implements it to avoid importing scout_agent (circular).
    """
    import urllib.parse
    import urllib.request

    url = _DEMAND_FEED_URL
    if url:
        endpoint = f"{url.rstrip('/')}/offers"
        _p = urllib.parse.urlparse(endpoint)
        _netloc = _p.hostname or ""
        if _p.port:
            _netloc = f"{_netloc}:{_p.port}"
        safe_endpoint = urllib.parse.urlunparse((_p.scheme, _netloc, _p.path, "", "", ""))
        if _p.scheme not in ("http", "https"):
            log.warning(
                f"[scout_tools_admin] DEMAND_FEED_URL has unsupported scheme {_p.scheme!r}; "
                f"falling back to disk snapshot at {SNAPSHOT_PATH}"
            )
        else:
            try:
                with urllib.request.urlopen(endpoint, timeout=10) as resp:
                    offers = json.loads(resp.read())
                log.info(f"[scout_tools_admin] loaded {len(offers)} offers from {safe_endpoint}")
                return offers
            except Exception as exc:
                log.warning(
                    f"[scout_tools_admin] DEMAND_FEED_URL fetch failed "
                    f"({type(exc).__name__}: {exc}); falling back to disk snapshot at {SNAPSHOT_PATH}"
                )
    if not SNAPSHOT_PATH.exists():
        log.warning(
            f"[scout_tools_admin] no offers source available — "
            f"DEMAND_FEED_URL unset and {SNAPSHOT_PATH} missing"
        )
        return []
    with open(SNAPSHOT_PATH) as f:
        offers = json.load(f)
    log.info(f"[scout_tools_admin] loaded {len(offers)} offers from disk snapshot {SNAPSHOT_PATH}")
    return offers


# ── QA suite (used by run_self_qa) ───────────────────────────────────────────

_QA_SUITE: list[tuple[str, str, list[str], str]] = [
    # ── Core health ──────────────────────────────────────────────────────────
    ("System status",
     "status",
     ["healthy"],
     "Core Health"),

    # ── Dark offers ───────────────────────────────────────────────────────────
    ("Dark offers",
     "ghost campaigns",
     ["ghost", "campaign", "impression", "revenue", "postback", "no ghost"],
     "Core Health"),

    # ── Scout threshold config ────────────────────────────────────────────────
    ("Scout threshold config",
     "what are Scout's current alert thresholds?",
     ["threshold", "config/scout_thresholds", "cvr", "fill", "velocity", "cap", "expiration"],
     "Core Health"),

    # ── Offer search — vertical ───────────────────────────────────────────────
    ("Offer search — finance vertical",
     "best offers for a financial services partner",
     ["capital one", "rocket", "payout", "rpm", "cpa", "offer"],
     "Offer Intelligence"),

    # ── Offer search — specific publisher ────────────────────────────────────
    ("Offers for named publisher",
     "what offers should we pitch to AT&T?",
     ["offer", "payout", "rpm", "finance", "campaign", "epc"],
     "Offer Intelligence"),

    # ── Supply/demand gaps ────────────────────────────────────────────────────
    ("Supply demand gaps",
     "where are our biggest supply gaps right now?",
     ["gap", "publisher", "offer", "category", "missing", "opportunity"],
     "Offer Intelligence"),

    # ── Offer inventory count ─────────────────────────────────────────────────
    ("Offer inventory count",
     "how many offers do we have in the inventory and which network has the most?",
     ["offer", "network", "cj", "impact", "total", "active"],
     "Offer Intelligence"),

    # ── Pipeline health ───────────────────────────────────────────────────────
    ("Pipeline health",
     "what is the health of our offer pipeline?",
     ["pipeline", "offer", "notion", "queue", "network", "active"],
     "Offer Intelligence"),

    # ── Revenue drop analysis ─────────────────────────────────────────────────
    ("WoW revenue drop",
     "which publishers dropped the most revenue this week vs last week?",
     ["publisher", "revenue", "drop", "week", "$"],
     "Revenue & Publisher"),

    # ── Publisher health ──────────────────────────────────────────────────────
    ("Publisher health",
     "how is TextNow performing?",
     ["textnow", "impression", "revenue", "click", "cvr", "funnel"],
     "Revenue & Publisher"),

    # ── Campaign status ───────────────────────────────────────────────────────
    ("Campaign status check",
     "is Capital One Shopping still active?",
     ["capital one", "active", "campaign", "status", "paused"],
     "Revenue & Publisher"),

    # ── Revenue projection ────────────────────────────────────────────────────
    ("Revenue projection",
     "project Truist revenue for this month",
     ["truist", "revenue", "$", "projection", "month", "forecast"],
     "Revenue & Publisher"),

    # ── Perkswall ────────────────────────────────────────────────────────────
    ("Perkswall engagement",
     "how is perkswall performing?",
     ["perkswall", "engagement", "click", "impression", "session"],
     "Revenue & Publisher"),

    # ── Multi-part question (new protocol test) ───────────────────────────────
    ("Multi-part question decomposition",
     "For our Intuit TurboTax review: what revenue did they drive this year? What were the top publishers? What offers worked best?",
     ["turbotax", "revenue", "publisher", "campaign", "$"],
     "Revenue & Publisher"),

    # ── CVR anomalies ────────────────────────────────────────────────────────
    ("CVR anomaly detection",
     "are there any campaigns with unusual drops in conversion rate recently?",
     ["cvr", "campaign", "drop", "impression", "anomaly", "conversion", "no anomaly", "no campaign"],
     "Revenue & Publisher"),

    # ── Expiring campaigns ───────────────────────────────────────────────────
    ("Expiring campaign warnings",
     "which campaigns are expiring in the next two weeks?",
     ["expir", "campaign", "end", "day", "active", "no campaign", "none expiring"],
     "Revenue & Publisher"),

    # ── Publisher revenue trends ─────────────────────────────────────────────
    ("Publisher revenue trends",
     "which publishers have shown declining revenue trends recently?",
     ["publisher", "trend", "revenue", "decline", "period", "down", "no publisher", "stable"],
     "Revenue & Publisher"),

    # ── Data boundary: SOV (should gracefully decline) ────────────────────────
    ("Data boundary — SOV",
     "what is our share of voice vs competitors?",
     ["don't have", "not tracked", "not in", "isn't in", "network", "dashboard", "sov"],
     "Data Boundaries"),

    # ── Data boundary: strategic question ────────────────────────────────────
    ("Data boundary — strategic intent",
     "what does AT&T want from us next year?",
     ["don't have", "not in", "judgment", "can't", "call", "data"],
     "Data Boundaries"),
]


# ── Extracted admin tool functions ────────────────────────────────────────────

def _validate_sql_query(sql: str) -> list:
    """
    Lightweight safety validator for run_sql_query.

    Returns a list of warning strings (empty = clean). NEVER blocks execution —
    callers should log the warnings but proceed. Heuristics:
      - References a large table without PREWHERE → warn
      - References a large table without any created_at filter → warn
      - Lacks any LIMIT clause → warn
    """
    warnings: list = []
    if not sql or not isinstance(sql, str):
        return warnings

    sql_upper = sql.upper()
    has_prewhere = bool(re.search(r"\bPREWHERE\b", sql_upper))
    # Only treat created_at as a date filter when it appears in a PREWHERE/WHERE
    # clause — references in SELECT columns or ORDER BY don't filter rows.
    has_created_at = bool(
        re.search(r"\b(?:PREWHERE|WHERE)\b[\s\S]*?\bCREATED_AT\b", sql_upper)
    )
    has_limit = bool(re.search(r"\bLIMIT\b", sql_upper))

    for table in _LARGE_TABLES:
        if re.search(r"\b" + re.escape(table) + r"\b", sql, re.IGNORECASE):
            if not has_prewhere:
                warnings.append(f"missing PREWHERE on large table: {table}")
            if not has_created_at:
                warnings.append(f"no date filter on large table: {table}")

    if not has_limit:
        warnings.append("no LIMIT clause")

    return warnings


def run_sql_query(sql: str, description: str = "", max_rows: int = 500) -> dict:
    """
    Execute an arbitrary SELECT query against ClickHouse.
    Safety: SELECT-only, 500 row default max, 30s timeout.
    Returns structured results for Claude to format.
    """
    import re as _re

    # Safety guard — SELECT only
    sql_stripped = sql.strip()
    first_word = sql_stripped.split()[0].upper() if sql_stripped else ""
    if first_word not in ("SELECT", "WITH"):
        return {
            "error": "Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, etc.",
            "sql": sql_stripped,
        }

    # Safety gate — log warnings for queries hitting large tables without filters.
    # Logs only — does NOT block execution.
    for _warning in _validate_sql_query(sql_stripped):
        log.warning("sql_query safety: %s", _warning)

    # Inject LIMIT if not present
    sql_upper = sql_stripped.upper()
    has_limit = bool(_re.search(r'\bLIMIT\b', sql_upper))
    if not has_limit:
        sql_stripped = sql_stripped.rstrip(";") + f"\nLIMIT {max_rows}"

    try:
        ch = _get_ch_client()
        result = ch.query(sql_stripped, settings={"max_execution_time": 30})
        rows = result.result_rows
        try:
            col_names = list(result.column_names)
        except (AttributeError, TypeError):
            col_names = []

        truncated = len(rows) >= max_rows

        # Sanitize values — ClickHouse returns date/datetime as Python objects
        def _sanitize(v):
            if isinstance(v, (_dt_mod.date, _dt_mod.datetime)):
                return str(v)
            if isinstance(v, (list, tuple)):
                return [_sanitize(x) for x in v]
            return v

        # Convert rows to list of dicts for readability
        if col_names:
            rows_as_dicts = [
                {k: _sanitize(v) for k, v in zip(col_names, row)}
                for row in rows[:max_rows]
            ]
        else:
            rows_as_dicts = [[_sanitize(v) for v in row] for row in rows[:max_rows]]

        # Strip internal ID columns — never surface user_id, publisher_id, campaign_id, etc. to LLM
        import re as _re_id
        _ID_SUFFIX = _re_id.compile(r'(?:^|_)id$', _re_id.IGNORECASE)
        if col_names:
            _keep = [c for c in col_names if not _ID_SUFFIX.search(c)]
            rows_as_dicts = [{k: v for k, v in row.items() if not _ID_SUFFIX.search(k)} for row in rows_as_dicts]
            col_names = _keep

        return {
            "finding": f"{len(rows_as_dicts)} row(s): {description or 'query run'}"[:80],
            "description": description,
            "sql_run": sql_stripped,
            "row_count": len(rows_as_dicts),
            "truncated": truncated,
            "truncation_note": f"Results limited to {max_rows} rows. Add LIMIT to your query to control this." if truncated else None,
            "columns": col_names,
            "rows": rows_as_dicts,
            "data_quality": {
                "tier": "free_form",
                "note": f"Live query — {len(rows_as_dicts)} rows.",
            },
        }
    except Exception as e:
        err = str(e)
        return {
            "error": err,
            "sql_run": sql_stripped,
            "description": description,
            "hint": "Check table/column names against the DATA DICTIONARY. Common issues: wrong join type (pid vs user_id), missing PREWHERE, type mismatch (toFloat64OrNull for revenue).",
        }


def get_scout_status() -> dict:
    """
    System health snapshot: benchmark freshness, offer inventory, queue depth,
    ClickHouse connectivity, and data quality warnings.
    """
    import time as _time
    from datetime import datetime, timezone

    status: dict = {}

    # PR 19a: self-heal — if benchmarks haven't loaded in this process yet OR
    # are stale, trigger a reload BEFORE reporting status. _get_benchmarks()
    # respects its TTL and will only hit CH if needed. This means status check
    # never reports "not loaded" except in real CH outage scenarios.
    if not _manager._benchmarks_loaded_at or (_time.time() - _manager._benchmarks_loaded_at) > _manager._benchmarks_ttl:
        try:
            _get_benchmarks()  # populates _manager cache
        except Exception as e:
            log.warning(f"[status] benchmark self-heal failed: {e}")

    # Benchmark freshness (post self-heal attempt)
    age_secs = _time.time() - _manager._benchmarks_loaded_at if _manager._benchmarks_loaded_at else None
    if age_secs is None:
        # Self-heal failed → real ClickHouse problem (heartbeat already alerts)
        status["benchmarks"] = "load failed (ClickHouse issue — heartbeat will alert)"
    elif age_secs < 120:
        status["benchmarks"] = f"{int(age_secs)}s ago"
    elif age_secs < 3600:
        status["benchmarks"] = f"{int(age_secs / 60)}m ago"
    else:
        status["benchmarks"] = f"{age_secs / 3600:.1f}h ago"

    # Benchmark coverage
    bench = _manager._benchmarks_cache or {}
    status["benchmark_coverage"] = {
        "by_offer":    len(bench.get("by_offer_impact_id", {})),
        "by_advertiser": len(bench.get("by_adv_name", {})),
        "by_category_payout": len(bench.get("by_category_payout", {})),
        "by_payout_type": len(bench.get("by_payout_type", {})),
    }

    # Offer inventory
    offers = _load_offers()
    status["offer_inventory"] = len(offers)
    if offers:
        advertisers = {o.get("advertiser") for o in offers}
        status["unique_advertisers"] = len(advertisers)
        networks = {}
        for o in offers:
            n = (o.get("network") or "unknown").lower()
            networks[n] = networks.get(n, 0) + 1
        status["by_network"] = networks
        status["available_networks"] = sorted(networks.keys())

    # Offer file age — how long ago the last successful scrape wrote the snapshot
    if SNAPSHOT_PATH.exists():
        _age_secs = _time.time() - SNAPSHOT_PATH.stat().st_mtime
        if _age_secs < 3600:
            status["offers_age"] = f"{int(_age_secs / 60)}m ago"
        elif _age_secs < 86400:
            status["offers_age"] = f"{_age_secs / 3600:.1f}h ago"
        else:
            status["offers_age"] = f"{_age_secs / 86400:.1f}d ago — consider refreshing"
    else:
        status["offers_age"] = "no snapshot — run @Scout refresh offers"

    # Unconfigured networks (creds absent → scraper silently skips them)
    _missing_nets = []
    if not _RAKUTEN_TOKEN:
        _missing_nets.append("rakuten")
    if not (_AWIN_PUB_ID and _AWIN_API_KEY):
        _missing_nets.append("awin")
    if _missing_nets:
        status["unconfigured_networks"] = _missing_nets
        warnings = status.get("warnings", [])
        warnings.append(
            f"Creds missing for: {', '.join(_missing_nets)} — inventory excludes these networks"
        )
        status["warnings"] = warnings

    # Demand queue
    state = _load_launched_offers_state()
    queued    = [k for k, v in state.items() if v.get("status") == "queued"]
    launched  = [k for k, v in state.items() if v.get("status") == "launched"]
    status["queue_depth"] = len(queued)
    status["launched_count"] = len(launched)
    if queued:
        status["queue_items"] = queued

    # ClickHouse connectivity
    try:
        ch = _get_ch_client()
        rows = ch.query("SELECT 1").result_rows
        status["clickhouse"] = "ok" if rows else "degraded"
    except Exception as e:
        status["clickhouse"] = f"unavailable: {str(e)[:80]}"

    # Data quality warnings — extend rather than overwrite so earlier warnings survive
    warnings = list(status.get("warnings", []))
    if bench and not bench.get("by_offer_impact_id"):
        warnings.append("No Tier 1 (exact offer) benchmarks — all scoring from Tier 2+")
    cats_null = sum(1 for o in offers if not o.get("category"))
    if cats_null > 0:
        pct = cats_null / max(len(offers), 1) * 100
        warnings.append(f"{cats_null} offers ({pct:.0f}%) have no category — Tier 3 scoring disabled for these")
    if warnings:
        status["warnings"] = warnings

    status["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status["finding"] = f"{status.get('offer_inventory', 0)} offers, {status.get('queue_depth', 0)} queued, CH {status.get('clickhouse', 'unknown')}"

    try:
        import scout_digest as _sd
        status["digest_env"] = _sd._SCOUT_ENV
        if _sd._SCOUT_ENV != "production":
            status["digest_routing"] = f"#scout-qa (SCOUT_ENV={_sd._SCOUT_ENV!r} — not production)"
        else:
            status["digest_routing"] = f"#scout-offers ({_sd._digest_channel()})"
    except Exception as _e:
        log.warning(f"[status] scout_digest unavailable: {_e}")
        status["digest_env"]     = "unavailable"
        status["digest_routing"] = "unavailable"

    return status


def get_usage_report(requesting_user_id: str = "") -> str:
    """
    Return Scout usage statistics. Admin-only (SCOUT_ADMIN_USER_ID env var).
    Shows: queries per period, top users, most-used tools, avg response time.
    """
    import pathlib, json as _json
    from collections import Counter
    from datetime import datetime, timezone, timedelta

    if not _ADMIN_UID or requesting_user_id != _ADMIN_UID:
        return ":lock: Usage reports are admin-only."

    log_path = pathlib.Path(__file__).parent / "data" / "usage_log.jsonl"
    if not log_path.exists():
        return "No usage data yet — logging started after this deploy. Check back after a few queries."

    records = []
    for line in log_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(_json.loads(line))
        except Exception as e:
            log.debug("get_usage_report malformed line swallowed: %s", e)
    now = datetime.utcnow()  # naive UTC to match stored timestamps (utcnow().isoformat())
    cutoff_7d  = now - timedelta(days=7)
    cutoff_30d = now - timedelta(days=30)
    recent_7d  = [r for r in records if datetime.fromisoformat(r["ts"]) >= cutoff_7d]
    recent_30d = [r for r in records if datetime.fromisoformat(r["ts"]) >= cutoff_30d]

    user_counts = Counter(r.get("user_name", r.get("user_id", "unknown")) for r in recent_30d)
    tool_counts = Counter(t for r in recent_30d for t in (r.get("tools") or []))
    avg_ms = int(sum(r.get("ms", 0) for r in recent_7d) / max(len(recent_7d), 1))

    lines = [f"*Scout Usage Report*\n"]
    lines.append(f"• *{len(recent_7d)}* queries last 7 days, *{len(recent_30d)}* last 30 days")
    lines.append(f"• Avg response time (7d): *{avg_ms // 1000}s*\n")
    lines.append("*Top users (30d):*")
    for name, count in user_counts.most_common(8):
        lines.append(f"• {name} — *{count}* queries")
    if tool_counts:
        lines.append("\n*Top tools called (30d):*")
        for tool, count in tool_counts.most_common(10):
            lines.append(f"• {tool} — *{count}x*")
    return "\n".join(lines)


def export_usage_log(days: int = 30, limit: int = 200,
                     requesting_user_id: str = "") -> str:
    """
    Dump raw (query → tools fired) pairs from usage_log.jsonl so an admin can
    eyeball whether Scout's tool routing is matching user intent. Admin-only.
    Returns a Slack-formatted block: one line per query, newest last.
    """
    import pathlib, json as _json
    from datetime import datetime, timedelta

    if not _ADMIN_UID or requesting_user_id != _ADMIN_UID:
        return ":lock: Usage export is admin-only."

    log_path = pathlib.Path(__file__).parent / "data" / "usage_log.jsonl"
    if not log_path.exists():
        return "No usage log yet."

    try:
        days_i = max(1, min(int(days or 30), 365))
    except (TypeError, ValueError):
        days_i = 30
    try:
        limit_i = max(1, min(int(limit or 200), 500))
    except (TypeError, ValueError):
        limit_i = 200

    cutoff = datetime.utcnow() - timedelta(days=days_i)
    rows = []
    for line in log_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = _json.loads(line)
            if datetime.fromisoformat(r["ts"]) >= cutoff:
                rows.append(r)
        except (ValueError, KeyError, TypeError, _json.JSONDecodeError) as e:
            log.debug("export_usage_log malformed row skipped: %s", e)
            continue

    rows = rows[-limit_i:]
    if not rows:
        return f"No usage entries in the last {days_i} days."

    lines = [f"*Scout usage export — last {days_i}d, {len(rows)} entries (newest last):*", "```"]
    for r in rows:
        ts    = r.get("ts", "")[:19]
        who   = r.get("user_name") or r.get("user_id", "?")
        q     = (r.get("query") or "").replace("\n", " ")[:140]
        tools = ",".join(r.get("tools") or []) or "<none>"
        ms    = r.get("ms", 0)
        lines.append(f"{ts}  {who:<14}  [{tools}]  ({ms}ms)  {q}")
    lines.append("```")
    return "\n".join(lines)


def record_entity_note(entity_name: str, entity_type: str, note: str,
                       exclude_from_fill_rate: bool = False,
                       _caller_user_id: str = "",
                       _caller_permalink: str = "") -> str:
    """
    Record publisher or advertiser knowledge in Scout's persistent learning store.
    Writes immediately and shows exactly what was stored (write-confirm-correct pattern).
    Calling again overwrites the previous entry — idempotent upsert.
    entity_type: 'publisher' or 'advertiser'
    exclude_from_fill_rate: publishers only — True suppresses from Pulse fill rate signals.

    Plan v3 §3.4 — provenance: `added_by` is the caller's Slack user_id (when
    available); falls back to "scout-agent" only if no caller is wired through.
    `permalink` records the Slack message that taught Scout this fact, so
    `why_entity_note` can return a clickable receipt.
    """
    import datetime as _dt

    overrides = _load_entity_overrides()
    section = "publishers" if entity_type.lower() == "publisher" else "advertisers"
    entry = {
        "note": note,
        "exclude_from_fill_rate": exclude_from_fill_rate if section == "publishers" else False,
        "added": _dt.date.today().isoformat(),
        "added_by": _caller_user_id or "scout-agent",
    }
    if _caller_permalink:
        entry["permalink"] = _caller_permalink
    overrides.setdefault(section, {})[entity_name] = entry
    _save_entity_overrides(overrides)

    lines = [f":white_check_mark: *{entity_name}* ({entity_type}) logged:"]
    lines.append(f"> _{note}_")
    if exclude_from_fill_rate and section == "publishers":
        lines.append(":no_entry_sign: Excluded from Pulse fill rate signals starting tomorrow's 8am run.")
    lines.append("_Reply to correct if I got anything wrong — I'll overwrite it, "
                 "or say `@Scout forget that about " + entity_name + "` to drop it._")
    return "\n".join(lines)


def forget_entity_note(entity_name: str, entity_type: str,
                       _caller_user_id: str = "",
                       _caller_permalink: str = "") -> str:
    """
    Plan v3 §3.4 — drop a previously-recorded publisher/advertiser fact.
    No-op (with friendly message) if the entry doesn't exist. Records a
    deletion audit row to data/entity_overrides_audit.jsonl for review.
    """
    import datetime as _dt

    overrides = _load_entity_overrides()
    section = "publishers" if entity_type.lower() == "publisher" else "advertisers"
    bucket = overrides.get(section) or {}
    if entity_name not in bucket:
        return (f":mag: I had no note for *{entity_name}* ({entity_type}) — "
                "nothing to forget.")
    dropped = bucket.pop(entity_name)
    overrides[section] = bucket
    _save_entity_overrides(overrides)

    # Append-only audit so we can review later who dropped what
    try:
        audit_path = pathlib.Path(__file__).parent / "data" / "entity_overrides_audit.jsonl"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("a") as _fh:
            _fh.write(json.dumps({
                "ts": _dt.datetime.utcnow().isoformat() + "Z",
                "action": "forget",
                "section": section,
                "entity": entity_name,
                "dropped": dropped,
                "by_user_id": _caller_user_id or "",
                "permalink": _caller_permalink or "",
            }) + "\n")
    except Exception as e:
        log.debug("forget_entity_note audit swallowed: %s", e)

    return (f":wastebasket: Forgot the note about *{entity_name}* ({entity_type}). "
            f"Was: _{dropped.get('note','(no note)')}_")


def why_entity_note(entity_name: str, entity_type: str = "") -> str:
    """
    Plan v3 §3.4 — explain where a publisher/advertiser fact came from.
    Returns the stored note plus provenance (who taught Scout, when, and the
    Slack permalink if available). Searches both sections when type omitted.
    """
    overrides = _load_entity_overrides()
    sections = (["publishers", "advertisers"] if not entity_type
                else ["publishers" if entity_type.lower() == "publisher" else "advertisers"])
    hits = []
    for section in sections:
        bucket = overrides.get(section) or {}
        if entity_name in bucket:
            row = bucket[entity_name]
            line = (f"*{entity_name}* ({section[:-1]}): _{row.get('note','(no note)')}_\n"
                    f":bookmark: learned from `{row.get('added_by','?')}` "
                    f"on {row.get('added','?')}")
            if row.get("permalink"):
                line += f" — <{row['permalink']}|Slack receipt>"
            hits.append(line)
    if not hits:
        return f":mag: I don't have a note for *{entity_name}*. Nothing to explain."
    return "\n\n".join(hits)


def run_self_qa() -> dict:
    """
    Run Scout's full QA suite against itself and return a structured report.
    Each test calls ask() with a representative question, checks the response
    for expected content, and records pass/fail + elapsed time.
    Called when a user says 'QA yourself', 'self test', 'run QA suite'.
    """
    import time as _time
    # Deferred import — ask() lives in scout_agent; importing at call time (not
    # module load time) avoids the circular import scout_tools_admin → scout_agent.
    # _is_admin stays in scout_agent.py; admin gate calls in TOOL_MAP wrappers
    # remain there as well.
    from scout_agent import ask  # noqa: PLC0415

    results = []
    total = len(_QA_SUITE)

    for label, question, pass_hints, _category in _QA_SUITE:
        t0 = _time.monotonic()
        try:
            response = ask(question, history=[], user_id="self-qa")
            elapsed = _time.monotonic() - t0

            # Part 4: ask() returns AskResult; payload carries structured
            # dispatch (brief/opportunities) — prefer fallback_text when present
            # so QA scores the human-facing string, not the dataclass repr.
            payload = response.payload or {}
            text = payload.get("fallback_text") or response.text

            text_lower = text.lower()
            responded = len(text.strip()) > 40
            hint_match = any(h.lower() in text_lower for h in pass_hints)
            passed = responded and hint_match
            snippet = text[:120].replace("\n", " ")

        except Exception as e:
            elapsed = _time.monotonic() - t0
            passed = False
            snippet = f"ERROR: {e}"

        results.append({
            "label": label,
            "question": question,
            "passed": passed,
            "elapsed": round(elapsed, 1),
            "snippet": snippet,
        })

    passed_count = sum(1 for r in results if r["passed"])
    return {
        "finding": f"{passed_count}/{total} checks passed",
        "total": total,
        "passed": passed_count,
        "failed": total - passed_count,
        "results": results,
    }
