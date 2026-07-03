"""
Scout — Slack Bot (Socket Mode)
Listens for @Scout mentions and responds with offer intelligence.
Run as a persistent background process: python scout_bot.py
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
from dataclasses import dataclass
import random
import re
import threading
import time

import requests

import queries as _q
from dotenv import load_dotenv
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler
from slack_sdk.web import WebClient

from scout_agent import ask
from scout_thresholds import _manager as _tm
from scout_notion import (
    _copy_coalescer_loop,
    _notion_watcher_loop,
)
from scout_ui_kit import Card, ResponsePattern, Severity, Surface, enforce, wrap_response, context_block
from scout_ch import _query_cvr_anomaly, _query_expiring_campaigns
from scout_state import (
    _DATA_DIR,
    _atomic_write,
    _load_briefs,
    _load_thread_contexts,
    _load_launched_offers, _save_launched_offers,
    _load_pulse_state, _save_pulse_state,
    _load_watchdog_state, _save_watchdog_state,
    _update_benchmark_from_actuals,
)
from scout_handlers import (
    _set_bot_user_id, _set_thread_state, _set_force_monitor_fn,
    handle_event,
)

load_dotenv()  # plist env vars (SCOUT_ENV, PULSE_CHANNEL, etc.) take precedence over .env


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scout_bot")

_LAST_THREAD_PER_CHANNEL: dict = {}  # channel → thread_ts
_LAST_THREAD_LOCK = threading.Lock()
_BOT_USER_ID: str = ""  # cached at startup — never changes


# ── Queue confirmation Block Kit helpers ─────────────────────────────────────





# ── Block Kit brief builder ───────────────────────────────────────────────────







# ── Opportunity cards ─────────────────────────────────────────────────────────



# ── Help / capabilities card ──────────────────────────────────────────────────





# Tokenizer for inline elements within a single text line.
# Groups: bold_d (**), bold_s (*), italic, code, emoji, link, user, plain










# ── Scout Signal: approve / reject handlers ───────────────────────────────────

_SCOUT_HQ_CHANNEL  = "C0AQEECF800"   # #bot-qa (was #scout-qa, was #scout-hq)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        log.warning("[scout-bot] %s is not a valid integer; using default %d", name, default)
        return default


# All env vars read once at module init — no scattered os.getenv() calls beyond this block.
@dataclass(frozen=True)
class _BotConfig:
    bot_token: str = ""
    app_token: str = ""
    anthropic_api_key: str = ""
    scout_env: str = "development"
    pulse_channel: str = ""
    scout_digest_channel: str = ""
    revenue_ops_channel: str = ""
    digest_source: str = "local"
    demand_feed_url: str = ""
    notion_queue_db_id: str = ""
    sidd_qa_channel_id: str = ""
    port: int = 10000
    is_render: bool = False

    def __post_init__(self) -> None:
        if self.scout_env == "production" and self.scout_digest_channel == _SCOUT_HQ_CHANNEL:
            log.warning(
                "[scout-bot] SCOUT_DIGEST_CHANNEL not set in production — "
                "_route_channel('offers') will fall back to #bot-qa"
            )

    @classmethod
    def from_env(cls) -> "_BotConfig":
        _hq = _SCOUT_HQ_CHANNEL
        return cls(
            bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
            app_token=os.getenv("SLACK_APP_TOKEN", ""),
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
            scout_env=os.getenv("SCOUT_ENV", "development"),
            pulse_channel=os.getenv("PULSE_CHANNEL", _hq),
            scout_digest_channel=os.getenv("SCOUT_DIGEST_CHANNEL", _hq),
            revenue_ops_channel=os.getenv("REVENUE_OPS_CHANNEL", _hq),
            digest_source=os.getenv("DIGEST_SOURCE", "local"),
            demand_feed_url=os.getenv("DEMAND_FEED_URL", "").rstrip("/"),
            notion_queue_db_id=os.getenv("NOTION_QUEUE_DB_ID", ""),
            sidd_qa_channel_id=os.getenv("SIDD_QA_CHANNEL_ID", ""),
            port=_env_int("PORT", 10000),
            is_render=bool(os.getenv("RENDER")),
        )


_BOT_CFG = _BotConfig.from_env()


# ── Approve helpers ───────────────────────────────────────────────────────────





# ── AI copy coalescer: cache + batch queue ────────────────────────────────────
# Keyed by (advertiser.lower(), payout_type, category) → copy dict + expiry

# Pending enrichment jobs: list of (notion_url, offer_kwargs_dict)




















def _slack_thread_url(channel: str, thread_ts: str) -> str:
    """Build a direct link to a Slack thread message."""
    ts_nodot = thread_ts.replace(".", "")
    return f"https://momentscience.slack.com/archives/{channel}/p{ts_nodot}"


# Fill rate exclusions are now managed dynamically via data/entity_overrides.json.
# Use _load_entity_overrides() at pulse time (imported from scout_agent).
# Seeded with Button on first deploy by _seed_entity_overrides() in main().
# ── Live health heartbeat (PR 15c) ────────────────────────────────────────────
# The HTTP /health endpoint (Render probe, every 30s) checks file-based + thread state
# only. ClickHouse outages are NOT checked there — a CH ping in /health would cause
# Render to restart the container on transient CH downtime.
#
# This heartbeat runs separately every 30 minutes, includes a CH ping, and posts a
# single Slack alert on first transition to degraded. It does NOT affect the HTTP
# probe or container restart behavior.
_HEALTH_STATUS_LOCK            = threading.Lock()
_LAST_HEALTH_STATUS: dict | None = None  # last status seen by the heartbeat (used for transition detection)

# PR 18: health knobs now read from config/scout_thresholds.json so the team can
# tune without a code change. `@Scout config` shows the live values. Lazy import
# from scout_agent because scout_bot is normally imported first by smoke tests.
def _load_health_cfg() -> dict:
    """Load the health section of scout_thresholds.json. Returns {} on any error."""
    try:
        return _tm.load().get("health", {})
    except Exception as e:
        log.warning(f"[health] could not load thresholds, using fallback defaults: {e}")
        return {}

_HEALTH_CFG                     = _load_health_cfg()
_HEALTH_HEARTBEAT_WARMUP_SECS   = int(_HEALTH_CFG.get("heartbeat_warmup_seconds", 300))
_HEALTH_CONSECUTIVE_THRESHOLD   = int(_HEALTH_CFG.get("heartbeat_consecutive_threshold", 2))
_HEALTH_HEARTBEAT_INTERVAL_SECS = int(_HEALTH_CFG.get("heartbeat_interval_minutes", 30)) * 60
_OFFER_STALENESS_HOURS          = int(_HEALTH_CFG.get("offer_staleness_hours", 30))


def _load_signal_cfg() -> dict:
    """Load the signals section of scout_thresholds.json. Returns {} on any error."""
    try:
        return _tm.load().get("signals", {})
    except Exception as e:
        log.warning(f"[signals] could not load thresholds, using fallback defaults: {e}")
        return {}

_SIGNAL_CFG                  = _load_signal_cfg()
_FILL_RATE_MIN_SESSIONS_7D   = int(_SIGNAL_CFG.get("fill_rate_min_sessions_7d", 2500))
_GHOST_RECENCY_HOURS         = int(_SIGNAL_CFG.get("ghost_recency_hours", 48))
_VELOCITY_DOWN_THRESHOLD_PCT = float(_SIGNAL_CFG.get("velocity_down_threshold_pct", -25))
_VELOCITY_UP_THRESHOLD_PCT   = float(_SIGNAL_CFG.get("velocity_up_threshold_pct", 20))
_CAP_ALERT_PCT               = float(_SIGNAL_CFG.get("cap_alert_pct", 85))

# PR 16b: Single source of truth for "which daemons must be alive."
# Daemons register themselves here at startup via _start_daemon() instead of
# being hardcoded in two places (the health check AND the watchdog).
# When a new daemon ships, _start_daemon() handles registration automatically.
_REQUIRED_DAEMONS: set[str] = set()


def _start_daemon(target, name: str, args: tuple = ()) -> None:
    """
    Start a daemon thread AND register its name in _REQUIRED_DAEMONS.

    PR 16b: replaces the dual hardcoded sets in _compute_health_status() and
    _thread_watchdog. Adding a new daemon now requires only the call site —
    health checks and the watchdog see it automatically.
    """
    threading.Thread(target=target, args=args, daemon=True, name=name).start()
    _REQUIRED_DAEMONS.add(name)

# ── Environment-aware channel routing ─────────────────────────────────────────
# SCOUT_ENV=production → messages go to production channels (set in launchd plist)
# Anything else (unset, "development") → everything goes to #scout-qa
# force=True → always #scout-qa regardless of environment

def _route_channel(purpose: str, force: bool = False) -> str:
    """
    Return the correct Slack channel for a given message purpose.
    Foolproof: force=True OR non-production env always routes to #scout-qa.
    Production channels require SCOUT_ENV=production (set in launchd plist only).
    """
    if force or _BOT_CFG.scout_env != "production":
        return _SCOUT_HQ_CHANNEL
    channels = {
        "pulse":    _BOT_CFG.pulse_channel,     # #revenue-operations
        "watchdog": _BOT_CFG.pulse_channel,     # #revenue-operations
        "offers":   _BOT_CFG.scout_digest_channel,  # #scout-offers
        "revenue":  _BOT_CFG.revenue_ops_channel,   # #revenue-operations
        "qa":       _SCOUT_HQ_CHANNEL,          # #sidd-qa — projection autocheck etc.
    }
    return channels.get(purpose, _SCOUT_HQ_CHANNEL)


# ── Feedback buttons ──────────────────────────────────────────────────────────



# ── Pulse signal helpers (one per signal, each owns its own ch connection) ────

def _resolve_ref_date(as_of_date: str | None):
    """Validate as_of_date and return (date_obj, sql_token).

    Raises ValueError for malformed dates so callers fail fast before any SQL
    is constructed — consistent across all four pulse signal functions.
    """
    from datetime import date as _date
    if as_of_date:
        d = _date.fromisoformat(as_of_date)   # raises ValueError on bad input
        return d, f"toDate('{d.isoformat()}')"
    return _date.today(), "today()"


def _pulse_signal_cap(ch, as_of_date: str | None = None) -> list:
    import queries as _q
    return _q.cap_alert_campaigns(
        ch,
        as_of_date=as_of_date,
        cap_alert_pct=_CAP_ALERT_PCT,
    )


def _pulse_signal_velocity(ch, as_of_date: str | None = None) -> list:
    _, _ref_date = _resolve_ref_date(as_of_date)  # _ref_date used by _hyp_and_gap closure
    results: list = []
    try:
        import queries as _q
        base = _q.velocity_alerts(
            ch,
            as_of_date=as_of_date,
            down_threshold_pct=_VELOCITY_DOWN_THRESHOLD_PCT,
            up_threshold_pct=_VELOCITY_UP_THRESHOLD_PCT,
            min_rev_30d=5000.0,
        )
        for b in base:
            rev_7d_ann = round((b["rev_7d"] / 7) * 30, 2)
            processed_advs = []
            for a in b.get("advertisers", []):
                adv_rev_7d_ann = (a["rev_7d"] / 7) * 30
                processed_advs.append({
                    "adv_name":  a["advertiser"],
                    "delta_ann": round(adv_rev_7d_ann - a["rev_30d"], 0),
                    "rev_7d":    a["rev_7d"],
                })
            results.append({
                "publisher_name":  b["publisher_name"],
                "publisher_id":    b["publisher_id"],
                "revenue_30d":     b["rev_30d"],
                "revenue_7d_ann":  rev_7d_ann,
                "pct_delta":       b["pct_delta"],
                "direction":       b["direction"],
                "top_advertisers": processed_advs,
            })

        # ── Batch: fetch existing advertisers for all down publishers in one query ──
        for v in results:
            v["hypothesis"] = ""
            v["gaps"] = []

        down_entries = [
            (v, v["publisher_id"], v.get("publisher_name", ""),
             next((a for a in v.get("top_advertisers", [])
                   if a.get("delta_ann", 0) < 0 and a.get("adv_name", "") != "(unattributed)"), None))
            for v in results
            if v["direction"] == "down" and v.get("publisher_id")
        ]

        existing_by_pub: dict[int, set] = {}
        if down_entries:
            try:
                uid_csv = ",".join(str(e[1]) for e in down_entries)
                batch_existing = ch.query(
                    f"SELECT pc.user_id, c.adv_name "
                    f"FROM from_airbyte_publisher_campaigns pc "
                    f"JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = toInt64(c.id) "
                    f"WHERE pc.user_id IN ({uid_csv}) AND pc.is_active = true AND pc.deleted_at IS NULL"
                ).result_rows
                for uid, adv in batch_existing:
                    existing_by_pub.setdefault(int(uid), set()).add(adv)
            except Exception as e:
                log.warning(f"Pulse batch existing-advertisers fetch failed: {e}")

        def _hyp_and_gap(pub_id, pub_name, top_adv):
            from scout_agent import _get_ch_client as _gcc
            _ch = _gcc()
            hyp = ""
            gaps = []
            if top_adv:
                try:
                    _hyp_sql = """
                        SELECT
                            u.organization,
                            sum(toFloat64OrNull(cv.revenue))                                    AS rev_30d,
                            sumIf(toFloat64OrNull(cv.revenue), cv.created_at >= today() - 7)   AS rev_7d
                        FROM adpx_conversionsdetails cv
                        JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
                        JOIN from_airbyte_users u ON toInt64(cv.user_id) = u.id
                        PREWHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - 30)
                        WHERE cv.created_at >= today() - 30
                          AND c.adv_name ILIKE %(adv)s
                          AND cv.user_id != %(pub_id)s
                        GROUP BY u.organization
                        HAVING rev_30d > 500
                        ORDER BY rev_30d DESC
                        LIMIT 5
                        """
                    if as_of_date:
                        _hyp_sql = _hyp_sql.replace("today()", _ref_date)
                    hyp_rows = _ch.query(
                        _hyp_sql,
                        parameters={"adv": f"%{top_adv['adv_name']}%", "pub_id": pub_id},
                    ).result_rows
                    also_down = [r[0] for r in hyp_rows
                                 if r[2] > 0 and (r[2] / 7 * 30) < r[1] * 0.80][:2]
                    adv_abs   = abs(top_adv["delta_ann"])
                    delta_fmt = f"${adv_abs/1000:.0f}K" if adv_abs >= 1000 else f"${adv_abs:.0f}"
                    if also_down:
                        hyp = (
                            f"_{top_adv['adv_name']} dropped {delta_fmt} — "
                            f"also down at {' & '.join(also_down)}. "
                            f"Likely advertiser-side cap, not a {pub_name} issue._"
                        )
                    else:
                        hyp = (
                            f"_{top_adv['adv_name']} dropped {delta_fmt} here but holding elsewhere — "
                            f"check {pub_name} provisioning or targeting config._"
                        )
                except Exception as e:
                    log.warning(f"Pulse hypothesis failed for {pub_name}: {e}")

            existing = existing_by_pub.get(pub_id, set())
            try:
                _gap_sql = f"""
                    WITH imp_agg AS (
                        SELECT campaign_id, count() AS imp_30d
                        FROM adpx_impressions_details
                        PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
                        WHERE created_at >= today() - 30
                        GROUP BY campaign_id
                    ),
                    conv_agg AS (
                        SELECT campaign_id, sum(toFloat64OrNull(revenue)) AS rev_30d
                        FROM adpx_conversionsdetails
                        PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
                        WHERE created_at >= today() - 30
                        GROUP BY campaign_id
                    )
                    SELECT
                        c.adv_name,
                        count(DISTINCT pc.user_id) AS pub_count,
                        sum(ca.rev_30d)             AS revenue_30d,
                        round(sum(ca.rev_30d) / nullIf(sum(ia.imp_30d), 0) * 1000, 2) AS rpm
                    FROM from_airbyte_publisher_campaigns pc
                    JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = toInt64(c.id)
                    LEFT JOIN imp_agg ia ON toString(ia.campaign_id) = toString(pc.campaign_id)
                    LEFT JOIN conv_agg ca ON toString(ca.campaign_id) = toString(pc.campaign_id)
                    WHERE pc.is_active = true AND pc.deleted_at IS NULL
                      AND pc.user_id != {pub_id}
                    GROUP BY c.adv_name
                    HAVING pub_count >= 2 AND sum(ca.rev_30d) > 0
                    ORDER BY sum(ca.rev_30d) DESC
                    LIMIT 20
                """
                if as_of_date:
                    _gap_sql = _gap_sql.replace("today()", _ref_date)
                gap_rows = _ch.query(_gap_sql).result_rows
                gaps = [
                    (adv, rpm) for adv, _cnt, _rev, rpm in gap_rows
                    if adv not in existing and rpm and rpm > 0
                ][:3]
            except Exception as e:
                log.warning(f"Pulse gap check failed for {pub_name}: {e}")
            return hyp, gaps

        if down_entries:
            from concurrent.futures import ThreadPoolExecutor as _TPEX, as_completed as _ac
            with _TPEX(max_workers=min(len(down_entries), 5), thread_name_prefix="pulse-hyp") as pool:
                futs = {
                    pool.submit(_hyp_and_gap, pub_id, pub_name, top_adv): v
                    for v, pub_id, pub_name, top_adv in down_entries
                }
                for fut in _ac(futs):
                    v = futs[fut]
                    try:
                        hyp, gaps = fut.result()
                        v["hypothesis"] = hyp
                        v["gaps"] = gaps
                    except Exception as e:
                        log.warning(f"Pulse hyp+gap future failed: {e}")

    except Exception as e:
        log.warning(f"Pulse velocity signal failed: {e}")
        raise
    return results


def _pulse_signal_ghost(ch, as_of_date: str | None = None) -> list:
    # Validate early — ghost delegates downstream so validate here before passing on.
    if as_of_date:
        as_of_date = _resolve_ref_date(as_of_date)[0].isoformat()
    results = []
    try:
        from scout_agent import _query_ghost_campaigns
        ghost_detail_rows = _query_ghost_campaigns(ch, as_of_date=as_of_date)
        by_adv: dict = {}
        for r in ghost_detail_rows:
            adv = r["adv_name"]
            by_adv.setdefault(adv, {"impressions_7d": 0, "impressions_2d": 0})
            by_adv[adv]["impressions_7d"] += r["impressions_7d"]
            by_adv[adv]["impressions_2d"] += r["impressions_2d"]
        for adv, agg in sorted(by_adv.items(), key=lambda x: -x[1]["impressions_7d"])[:10]:
            results.append({"adv_name": adv, **agg})
    except Exception as e:
        log.warning(f"Pulse ghost campaign signal failed: {e}")
        raise
    return results


def _pulse_signal_fill_rate(ch, as_of_date: str | None = None) -> list:
    import queries as _q
    return _q.fill_rate_publishers(
        ch,
        as_of_date=as_of_date,
        min_sessions=_FILL_RATE_MIN_SESSIONS_7D,
    )


def _format_revenue_alert(total: dict, publishers: list, as_of: str | None = None, alert_name: str = "") -> tuple[str, list[dict]]:
    """
    Format the proactive revenue alert message.

    total: dict from _query_intraday_revenue_total (today_revenue, projected_full_day,
           dow_median, pct_of_expected, weekday, sample_days)
    publishers: list of dicts from _query_intraday_revenue_by_publisher
                (publisher_name, publisher_id, delta, root_cause, ...)
    as_of: human-readable time string, e.g. "3pm CT" — defaults to current CT time
    """
    import pytz
    from datetime import datetime as _dt

    if as_of is None:
        _ct = _dt.now(pytz.timezone("America/Chicago"))
        as_of = _ct.strftime("%-I:%M%p CT").lower()  # e.g. "3:04pm CT"

    pct        = round(total["pct_of_expected"])
    today_rev  = total["today_revenue"]
    projected  = total["projected_full_day"]
    expected   = total["dow_median"]
    weekday    = total["weekday"]
    samples    = total["sample_days"]

    # Body items only — header is supplied by the alert renderer at the call site
    items = [
        f"Platform so far ({as_of}): *${today_rev:,.0f}* | projected: *${projected:,.0f}* | expected [{weekday}]: ~*${expected:,.0f}*",
        f"Tracking at *{pct}%* of expected ({samples} same-weekday samples)",
    ]

    _ROOT_LABELS = {
        "ghost_campaign": "impressions ✓, $0 revenue → ghost campaign",
        "fill_rate":      "zero impressions → fill rate or cap hit",
        "traffic":        "zero sessions → no upstream traffic",
        "revenue_down":   "revenue below expected, specific cause unclear",
    }

    if publishers:
        items.append("*Where the gap is:*")
        for p in publishers:
            name     = p.get("publisher_name") or f"pub {p.get('publisher_id', '?')}"
            pub_id   = p.get("publisher_id", "")
            delta    = p.get("delta", 0.0)
            cause    = p.get("root_cause", "normal")
            label    = _ROOT_LABELS.get(cause, cause)
            id_str   = f" *(pub {pub_id})*" if pub_id else ""
            items.append(f"{name}{id_str}: *−${abs(delta):,.0f}* below expected · {label}")

        items.append("All other publishers within normal range.")

        # Suggest a next step based on top root cause
        top_cause = publishers[0].get("root_cause", "normal")
        top_pub   = publishers[0].get("publisher_name", "")
        if top_cause == "ghost_campaign":
            items.append(f"Immediate: `@Scout ghost campaigns` — {top_pub} matches ghost detection criteria.")
        elif top_cause == "fill_rate":
            items.append(f"Immediate: `@Scout fill rate` — {top_pub} has zero impressions despite active sessions.")
        elif top_cause == "revenue_down":
            items.append(
                f"Immediate: `@Scout {top_pub}` — revenue is below expected with no single dominant signal; "
                f"check traffic, fill rate, and ghost-campaign indicators."
            )
        elif top_cause == "traffic":
            items.append(f"Immediate: `@Scout {top_pub}` — no sessions; confirm SDK is sending traffic.")
    else:
        items.append(
            "No single publisher accounts for the gap — revenue is spread-down across the platform.\n"
            "Likely causes: session volume drop, fill rate platform-wide, or a slow day.\n"
            "Run `@Scout fill rate` to check publisher-level session health."
        )

    headline = "Revenue alert — today is tracking soft"
    body = "\n".join(items)
    card = Card(severity=Severity.CRITICAL, headline=headline, body=body)
    if alert_name:
        card.actions = [
            ("✓ Acknowledge", "scout_acknowledge", alert_name, "primary"),
            ("Snooze ▾", "scout_snooze_open", alert_name, ""),
        ]
    _, blocks = wrap_response(card=card, surface=Surface.MONITOR_ALARM, pattern=ResponsePattern.ALERT)
    return f"🔴 {headline}", blocks


def _digest_poster(web) -> None:
    """
    Daemon: daily Scout Signal offers digest poster.

    PR-26 split the scraper into ms-demand-feed, but the assumed handoff
    ('Scout's own daemon handles the digest') was never wired. This daemon
    closes that gap — runs once per day at digest_post_hour_ct (default 7am CT,
    one hour after ms-demand-feed's 06:00 CT scrape) and calls
    scout_digest.post_digest(). post_digest() already pulls offers from
    DEMAND_FEED_URL with a disk-snapshot fallback, so this just needs to
    schedule it.

    Feature flag: SCOUT_THRESHOLDS["signals"]["digest_daemon_enabled"] (default False).
    Idempotent per calendar day via _load/_save_digest_post_state.
    Outer restart wrapper so any unhandled crash self-heals.
    """
    import time as _time
    import pytz
    from datetime import datetime as _dt
    from scout_state import _load_digest_post_state, _save_digest_post_date

    while True:
        try:
            CT_TZ = pytz.timezone("America/Chicago")

            while True:
                try:
                    # Re-read both knobs each tick so live config changes apply
                    # without a crash/restart (matches digest_daemon_enabled).
                    check_hour = int(
                        _tm.load().get("signals", {}).get("digest_post_hour_ct", 7)
                    )
                    if not _tm.load().get("signals", {}).get(
                        "digest_daemon_enabled", False
                    ):
                        _time.sleep(300)
                        continue

                    now_ct = _dt.now(CT_TZ)
                    if not (now_ct.hour == check_hour and now_ct.minute < 10):
                        _time.sleep(300)
                        continue

                    today_str = now_ct.date().isoformat()
                    if _load_digest_post_state() == today_str:
                        _time.sleep(300)
                        continue

                    import scout_digest
                    log.info("[digest-poster] posting Scout Signal digest...")
                    # Save state first so a transient post failure can't trigger
                    # duplicate same-day posts on the next 5-min poll. If the
                    # post itself fails, revert so the next poll retries.
                    _save_digest_post_date(today_str)
                    try:
                        _digest_source = _BOT_CFG.digest_source
                        if _digest_source == "remote":
                            _demand_url = _BOT_CFG.demand_feed_url
                            _used_remote = False
                            if not _demand_url:
                                log.warning("[digest-poster] DIGEST_SOURCE=remote but DEMAND_FEED_URL is unset — falling back to local")
                            else:
                                try:
                                    import requests as _req
                                    from slack_sdk.web import WebClient as _WC
                                    from scout_slack_safe import guard_web_client as _gwc
                                    _r = _req.get(f"{_demand_url}/digest/blocks", timeout=120)
                                    if _r.status_code == 204:
                                        log.info("[digest-poster] demand-feed: no payload (event gate skipped)")
                                        _used_remote = True
                                    elif _r.ok:
                                        _payload = _r.json()
                                        _channel = _route_channel("offers")
                                        _web = _WC(token=_BOT_CFG.bot_token)
                                        _gwc(_web)
                                        _resp = _web.chat_postMessage(
                                            channel=_channel,
                                            text=_payload["fallback"],
                                            blocks=_payload["blocks"],
                                            unfurl_links=False,
                                            unfurl_media=False,
                                        )
                                        log.info(f"[digest-poster] remote digest posted → {_channel} ts={_resp['ts']}")
                                        _used_remote = True
                                    else:
                                        log.error("[digest-poster] demand-feed /digest/blocks returned %s — falling back to local", _r.status_code)
                                except Exception as _rem_exc:
                                    log.error("[digest-poster] remote fetch failed: %s — falling back to local", _rem_exc)
                            if not _used_remote:
                                scout_digest.post_digest()
                        else:
                            scout_digest.post_digest()
                    except Exception:
                        _save_digest_post_date("")
                        raise
                    log.info(f"[digest-poster] Digest run complete for {today_str}.")
                    _time.sleep(300)

                except Exception as e:
                    log.warning(f"[digest-poster] Unexpected error: {e}")
                    _time.sleep(300)

        except Exception as e:
            log.error(
                f"[digest-poster] Fatal crash — restarting in 30s: {e}",
                exc_info=True,
            )
            _time.sleep(30)


# ── Silent per-signal monitors (decomposed pulse) ────────────────────────────
# Each monitor mirrors the _revenue_tracker shape:
#   outer restart wrapper + inner 5-min poll loop
#   feature flag via _monitor_enabled(<key>) — opt-in, default False
#   fire window: target hour CT ± 10 minutes (configurable via
#     SCOUT_THRESHOLDS["signals"][f"{key}_monitor_check_hour_ct"], default 9)
#   idempotent per calendar day via _load/_save_<key>_alert_state in scout_state
#   silent unless the underlying signal returns non-empty
# Alert only when there's something to act on — no "all signals nominal" digests.


def _build_alert_response(severity: Severity, headline: str, body: str, alert_name: str = "") -> tuple[str, list]:
    """Shared boilerplate for all monitor alert formatters.

    Builds a Card, calls wrap_response on MONITOR_ALARM with the ALERT pattern,
    and returns (fallback_text, blocks). Per-formatter row-parsing and
    headline/body construction stays in each formatter.
    """
    card = Card(severity=severity, headline=headline, body=body)
    if alert_name:
        card.actions = [
            ("✓ Acknowledge", "scout_acknowledge", alert_name, "primary"),
            ("Snooze ▾", "scout_snooze_open", alert_name, ""),
        ]
    _, blocks = wrap_response(
        card=card,
        surface=Surface.MONITOR_ALARM,
        pattern=ResponsePattern.ALERT,
    )
    return f"🟠 {headline}", blocks


def _build_alert_body(items: list[str], action_footer: str = "") -> str:
    body = "\n".join(f"• {item}" for item in items)
    if body and action_footer:
        return f"{body}\n\n{action_footer}"
    return body or action_footer


def _format_cap_alert(rows: list, alert_name: str = "") -> tuple[str, list[dict]]:
    """Return (fallback, blocks) Block Kit alert for advertisers nearing monthly cap.

    Each item shows: pct of cap, MTD revenue / cap, dollar headroom at risk,
    and days-to-cap vs days-remaining — so the reader knows urgency at a glance.
    Action footer prompts the only two levers: contact advertiser or lower bid floor.
    """
    items = []
    for r in rows[:8]:
        adv     = r.get("adv_name", "Unknown")
        cap_pct = r.get("cap_pct", 0)
        rev_mtd = r.get("revenue_mtd", 0)
        cap     = r.get("monthly_cap", 0)
        dtc     = r.get("days_to_cap", 0)
        dr      = r.get("days_remaining", 0)
        at_risk = max(cap - rev_mtd, 0)
        pace_note = (
            f"caps in ~{dtc:.0f}d, {dr}d left — *~${at_risk:,.0f} at risk*"
            if dtc < dr
            else f"~{dtc:.0f}d to cap, {dr}d remaining"
        )
        items.append(
            f"*{adv}*: *{cap_pct:.0f}%* of cap · "
            f"${rev_mtd:,.0f} / ${cap:,.0f} · "
            f"{pace_note}"
        )

    headline = "Cap alert — advertisers approaching monthly budget"
    return _build_alert_response(Severity.WARN, headline, _build_alert_body(items, "→ Contact advertiser or lower bid floor before cap hits"), alert_name=alert_name)


def _format_velocity_down_alert(rows: list, alert_name: str = "") -> tuple[str, list[dict]]:
    """Return (fallback, blocks) Block Kit alert for publishers with declining revenue velocity.

    Frames the drop in monthly-run-rate terms (what the publisher is on pace to miss)
    alongside the raw weekly comparison so the magnitude is immediately actionable.
    Hypothesis is shown when available to distinguish publisher-side issues from
    advertiser-side cap events.
    """
    downs = [v for v in rows if v.get("direction") == "down"]
    if not downs:
        return ("", [])
    items = []
    for v in downs[:5]:
        name      = v.get("publisher_name", "Unknown")
        rev_30d   = v.get("revenue_30d", 0)
        rev_7d_a  = v.get("revenue_7d_ann", 0)
        pct       = v.get("pct_delta", 0)
        rev_7d_wk = rev_7d_a / (30 / 7) if rev_7d_a else 0   # un-annualise to weekly
        rev_30d_wk = rev_30d / (30 / 7) if rev_30d else 0
        miss_mo   = rev_30d - rev_7d_a   # projected monthly shortfall vs last 30d pace
        line = (
            f"*{name}* on pace to miss *${miss_mo:,.0f}*/mo · "
            f"was ${rev_30d_wk:,.0f}/wk, now ${rev_7d_wk:,.0f}/wk · "
            f"{pct:+.0f}%"
        )
        hyp = v.get("hypothesis", "")
        if hyp:
            line += f"\n  {hyp}"
        items.append(line)

    headline = "Revenue velocity — publishers tracking down"
    return _build_alert_response(Severity.WARN, headline, _build_alert_body(items, "→ Check publisher fill rate and CPM floor"), alert_name=alert_name)


def _format_ghost_alert(rows: list, alert_name: str = "") -> tuple[str, list[dict]]:
    """Return (fallback, blocks) Block Kit alert for campaigns with impressions but no revenue.

    Shows traffic volume in recency-first order (48h impressions first) so the reader
    sees how much inventory is burning right now, not just the 7d aggregate.
    Action footer targets the two most common root causes: pixel breakage and dead creative.
    """
    items = []
    for r in rows[:8]:
        adv    = r.get("adv_name", "Unknown")
        imp_7d = r.get("impressions_7d", 0)
        imp_2d = r.get("impressions_2d", 0)
        # Express recency burn rate to convey urgency
        items.append(
            f"*{adv}*: {imp_2d:,} imps in 48h · {imp_7d:,} in 7d · $0 revenue — "
            f"*{imp_7d:,} impressions wasted*"
        )

    headline = "Ghost campaigns — impressions without revenue"
    return _build_alert_response(Severity.WARN, headline, _build_alert_body(items, "→ Check tracking pixel / confirm creative is live"), alert_name=alert_name)


def _format_fill_alert(rows: list, alert_name: str = "") -> tuple[str, list[dict]]:
    """Return (fallback, blocks) Block Kit alert for publishers with low fill rate.

    Anchors the fill rate against the 15% threshold so the gap is visible,
    and shows session count to convey opportunity size alongside the rate.
    Action footer names the two levers: floor price and supply source health.
    """
    items = []
    _FILL_THRESHOLD_PCT = 15  # mirrors the WHERE clause threshold in _pulse_signal_fill_rate
    for r in rows[:5]:
        name   = r.get("publisher_name", "Unknown")
        fill   = r.get("fill_rate_pct", 0)
        missed = r.get("missed_sessions", 0)
        sess   = r.get("sessions_7d", 0)
        gap    = max(_FILL_THRESHOLD_PCT - fill, 0)
        items.append(
            f"*{name}*: *{fill:.0f}%* fill rate (7d), {sess:,} sessions · "
            f"threshold {_FILL_THRESHOLD_PCT}% — *{gap:.0f}pp below, {missed:,} missed*"
        )

    headline = "Low fill rate — publishers with significant unfilled sessions"
    return _build_alert_response(Severity.WARN, headline, _build_alert_body(items, "→ Review floor price or supply source health"), alert_name=alert_name)


def _format_cvr_alert(rows: list, alert_name: str = "") -> tuple[str, list[dict]]:
    """Return (fallback, blocks) Block Kit alert for publisher-campaign CVR drops."""
    items = []
    for r in rows[:6]:
        pub      = r.get("publisher_name", "Unknown")
        adv      = r.get("adv_name", "Unknown")
        cvr_7d   = float(r.get("cvr_7d") or 0)
        cvr_yd   = float(r.get("cvr_yesterday") or 0)
        delta    = float(r.get("delta_pct") or 0)
        payout   = float(r.get("payout_per_conversion") or 0)
        items.append(
            f"*{pub} — {adv}*: "
            f"CVR {cvr_yd:.2%} vs {cvr_7d:.2%} baseline "
            f"({delta:+.0f}%) - ${payout:.0f} payout"
        )

    headline = "CVR anomalies — significant conversion rate drops since yesterday"
    return _build_alert_response(Severity.WARN, headline, _build_alert_body(items), alert_name=alert_name)


def _format_expiration_alert(rows: list, alert_name: str = "") -> tuple[str, list[dict]]:
    """Return (fallback, blocks) Block Kit alert for campaigns expiring soon."""
    items = []
    for r in rows[:8]:
        adv      = r.get("adv_name", "Unknown")
        end_date = r.get("end_date", "?")
        days     = r.get("days_remaining", 0)
        pubs     = r.get("publisher_count", 0)
        rev_7d   = r.get("revenue_7d", 0)
        imp_7d   = r.get("impressions_7d", 0)
        items.append(
            f"*{adv}*: expires {end_date} ({days}d) - "
            f"{pubs} publisher(s), {imp_7d:,} impressions, ${rev_7d:,.0f} revenue/7d"
        )

    headline = "Expiring campaigns — active campaigns ending within the alert window"
    return _build_alert_response(Severity.WARN, headline, _build_alert_body(items), alert_name=alert_name)


def _check_campaign_health(adv_name: str, launched_at) -> dict | None:
    """Query impressions, clicks, revenue since launch. Returns alert dict or None."""
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime as _dt, timezone as _utc

    try:
        from scout_agent import _get_ch_client
        ch = _get_ch_client()
        launched_str = launched_at.strftime("%Y-%m-%d %H:%M:%S")
        partition = launched_at.strftime("%Y%m")

        def q_impressions():
            rows = ch.query("""
                SELECT count() AS impressions
                FROM adpx_impressions_details i
                JOIN from_airbyte_campaigns c ON i.campaign_id = toUInt64(c.id)
                WHERE c.adv_name ILIKE %(adv)s
                  AND i.created_at >= %(launched_at)s
                  AND toYYYYMM(i.created_at) >= %(partition)s
            """, parameters={"adv": adv_name, "launched_at": launched_str, "partition": int(partition)}).result_rows
            return rows[0][0] if rows else 0

        def q_clicks():
            rows = ch.query("""
                SELECT count() AS clicks
                FROM adpx_tracked_clicks tc
                JOIN from_airbyte_campaigns c ON tc.campaign_id = toUInt64(c.id)
                WHERE c.adv_name ILIKE %(adv)s
                  AND tc.created_at >= %(launched_at)s
                  AND toYYYYMM(tc.created_at) >= %(partition)s
            """, parameters={"adv": adv_name, "launched_at": launched_str, "partition": int(partition)}).result_rows
            return rows[0][0] if rows else 0

        def q_revenue():
            rows = ch.query("""
                SELECT sum(toFloat64OrNull(revenue)) AS revenue
                FROM adpx_conversionsdetails cd
                JOIN from_airbyte_campaigns c ON cd.campaign_id = toUInt64(c.id)
                WHERE c.adv_name ILIKE %(adv)s
                  AND cd.created_at >= %(launched_at)s
                  AND toYYYYMM(cd.created_at) >= %(partition)s
            """, parameters={"adv": adv_name, "launched_at": launched_str, "partition": int(partition)}).result_rows
            return (rows[0][0] or 0) if rows else 0

        with ThreadPoolExecutor(max_workers=3) as ex:
            f_imp = ex.submit(q_impressions)
            f_clk = ex.submit(q_clicks)
            f_rev = ex.submit(q_revenue)
            impressions = f_imp.result()
            clicks = f_clk.result()
            revenue = f_rev.result()

        # Alert conditions
        hours_since = (_dt.now(_utc.utc) - launched_at).total_seconds() / 3600
        alert = None

        if impressions > 1000 and clicks == 0 and hours_since >= 3:
            alert = {
                "impressions": impressions, "clicks": clicks, "revenue": revenue,
                "hypothesis": "CTA not rendering or link broken — no clicks despite high impression volume."
            }
        elif impressions > 5000 and revenue == 0 and clicks > 0 and hours_since >= 6:
            alert = {
                "impressions": impressions, "clicks": clicks, "revenue": revenue,
                "hypothesis": "Tracking pixel not firing or landing page failure — clicks present but no conversions."
            }
        elif impressions == 0 and hours_since >= 3:
            alert = {
                "impressions": 0, "clicks": 0, "revenue": 0,
                "hypothesis": "Not serving at all — check geo/OS restrictions or provisioning config."
            }

        return alert

    except Exception as e:
        log.error(f"[watchdog] health check failed for {adv_name}: {e}")
        return None


def _post_watchdog_alert(web: WebClient, adv_name: str, result: dict, hours_since: float) -> None:
    """Post launch health alert to #revenue-operations (production) or #scout-qa (dev/force)."""
    channel = _route_channel("watchdog")
    hours_str = f"{int(hours_since)}h" if hours_since < 24 else f"{hours_since / 24:.1f}d"
    imp = f"{result['impressions']:,}"
    clk = f"{result['clicks']:,}"
    rev = f"${result['revenue']:,.2f}"

    text = (
        f":rotating_light: *Launch Health Alert — {adv_name}*\n"
        f"Launched {hours_str} ago · {imp} impressions · "
        f"*{clk} clicks · {rev} revenue*\n\n"
        f"Likely cause: {result['hypothesis']}\n\n"
        f":zap: Reply `@Scout health brief on {adv_name}` for a full breakdown."
    )
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    try:
        web.chat_postMessage(channel=channel, text=text, blocks=blocks)
        log.info(f"[watchdog] alert posted for {adv_name}")
    except Exception as e:
        log.error(f"[watchdog] failed to post alert: {e}")


def _run_watchdog_checks(web: WebClient, state: dict) -> None:
    """Check recently launched campaigns for zero-engagement patterns."""
    from datetime import datetime as _dt, timezone as _utc, timedelta
    import pytz

    alerted = set(state.get("alerted", []))
    new_alerts = []

    # --- Source A: Scout-tracked launches from launched_offers.json ---
    offers = _load_launched_offers()
    now_utc = _dt.now(_utc.utc)

    for adv_name, offer in offers.items():
        if offer.get("status") != "launched":
            continue
        launched_at_str = offer.get("launched_at")
        if not launched_at_str:
            continue
        try:
            launched_at = _dt.fromisoformat(launched_at_str.replace("Z", "+00:00"))
            if launched_at.tzinfo is None:
                launched_at = launched_at.replace(tzinfo=_utc.utc)
        except Exception:
            continue
        hours_since = (now_utc - launched_at).total_seconds() / 3600
        if hours_since < 3 or hours_since > 48:
            continue  # too early or too old
        if adv_name in alerted:
            continue

        result = _check_campaign_health(adv_name, launched_at)
        if result:
            _post_watchdog_alert(web, adv_name, result, hours_since)
            new_alerts.append(adv_name)

    # --- Source B: Platform-launched campaigns (not in Scout queue) ---
    # Query from_airbyte_publisher_campaigns for new entries in last 48h
    try:
        from scout_agent import _get_ch_client
        ch = _get_ch_client()
        rows = ch.query("""
            SELECT DISTINCT c.adv_name, min(pc.created_at) AS first_seen
            FROM from_airbyte_publisher_campaigns pc
            JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
            WHERE pc.created_at >= now() - INTERVAL 48 HOUR
              AND pc.is_active = true
              AND pc.deleted_at IS NULL
              AND c.deleted_at IS NULL
            GROUP BY c.adv_name
        """).result_rows
    except Exception as e:
        log.error(f"[watchdog] platform launch query failed: {e}")
        rows = []

    for (adv_name, first_seen) in rows:
        if adv_name in offers:
            continue  # already handled by Source A
        if adv_name in alerted:
            continue
        now_utc = _dt.now(_utc.utc)
        if hasattr(first_seen, 'tzinfo') and first_seen.tzinfo is None:
            import pytz as _pytz
            first_seen = _pytz.utc.localize(first_seen)
        hours_since = (now_utc - first_seen).total_seconds() / 3600
        if hours_since < 3 or hours_since > 48:
            continue

        result = _check_campaign_health(adv_name, first_seen)
        if result:
            _post_watchdog_alert(web, adv_name, result, hours_since)
            new_alerts.append(adv_name)

    if new_alerts:
        state.setdefault("alerted", [])
        state["alerted"].extend(new_alerts)
        _save_watchdog_state(state)

    log.info(f"[watchdog] checked launches, fired {len(new_alerts)} alert(s)")


def _launch_watchdog(web: WebClient) -> None:
    """
    Launch health watchdog daemon.

    Runs daily at 10:00 AM Chicago time. Check-first pattern — fires immediately
    on startup if today's run was missed (e.g. Mac was off at 10am).
    Catches broken campaign launches within hours, not days.
    Posts alerts to #revenue-operations (no @mentions).
    """
    import pytz
    from datetime import datetime as _dt, timedelta

    while True:
        try:
            chicago = pytz.timezone("America/Chicago")
            now_chi = _dt.now(chicago)
            today_str = now_chi.strftime("%Y-%m-%d")

            # Load state
            state = _load_watchdog_state()

            # CHECK FIRST: if past 10am and haven't run today, fire immediately
            if state.get("last_run_date") != today_str and now_chi.hour >= 10:
                _run_watchdog_checks(web, state)
                state["last_run_date"] = today_str
                _save_watchdog_state(state)

            # Sleep until next 10am
            target = now_chi.replace(hour=10, minute=0, second=0, microsecond=0)
            if now_chi >= target:
                target += timedelta(days=1)
            sleep_secs = (target - now_chi).total_seconds()
            log.info(f"[watchdog] sleeping {sleep_secs / 3600:.1f}h until next run at {target}")
            time.sleep(sleep_secs)

        except Exception as e:
            log.error(f"[watchdog] cycle failed: {e}", exc_info=True)
            time.sleep(3600)


def _one_shot_monitor(web, channel: str, signal_fn, format_fn, thread_ts: str = "", alert_name: str = "") -> None:
    """Run a monitor signal query once and post the result in-thread immediately.
    Used by the force-trigger admin commands (@Scout force cap/velocity/ghost/fill).
    """
    from scout_agent import _get_ch_client
    _ts = thread_ts or None
    ch = _get_ch_client()
    rows = signal_fn(ch)
    if not rows:
        web.chat_postMessage(channel=channel, thread_ts=_ts, text="No active signal — nothing to report right now.")
        return
    fallback, blocks = format_fn(rows, alert_name=alert_name)
    if not fallback:
        web.chat_postMessage(channel=channel, thread_ts=_ts, text="No active signal — nothing to report right now.")
        return
    resp = web.chat_postMessage(channel=channel, thread_ts=_ts, text=fallback, blocks=blocks)
    if alert_name and resp and resp.get("ok"):
        from datetime import datetime, timezone
        import alert_registry
        alert_registry.set_post_state(alert_name, resp["ts"], channel, datetime.now(timezone.utc).isoformat())


def _run_revenue_check_once(web, channel: str, thread_ts: str = "") -> None:
    """Force-trigger the revenue tracker check once — bypasses time gate and daily state."""
    from scout_agent import _get_ch_client, _query_intraday_revenue_total, _query_intraday_revenue_by_publisher
    _ts = thread_ts or None
    ch = _get_ch_client()
    try:
        total = _query_intraday_revenue_total(ch)
    except Exception as e:
        web.chat_postMessage(channel=channel, thread_ts=_ts, text=f":x: Revenue tracker Phase 1 query failed: {e}")
        return
    if total is None:
        web.chat_postMessage(channel=channel, thread_ts=_ts, text=":white_check_mark: Revenue on pace — no alert would fire right now.")
        return
    try:
        publishers = _query_intraday_revenue_by_publisher(ch, total)
    except Exception:
        publishers = []
    fallback, blocks = _format_revenue_alert(total, publishers, alert_name="revenue_tracker")
    resp = web.chat_postMessage(channel=channel, thread_ts=_ts, text=fallback, blocks=blocks)
    if resp and resp.get("ok"):
        from datetime import datetime, timezone
        import alert_registry
        alert_registry.set_post_state("revenue_tracker", resp["ts"], channel, datetime.now(timezone.utc).isoformat())





















# ── Interactive (button click) handler ───────────────────────────────────────



# ── Feedback handler ─────────────────────────────────────────────────────────



# ── App Home tutorial ─────────────────────────────────────────────────────────

# Five real, working queries organized by JTBD.
# Values are real advertisers/partners confirmed in the MS platform.








# ── Main event handler ────────────────────────────────────────────────────────





def _check_stale_queue(web: WebClient) -> None:
    """
    Daily background check: any offer approved 7+ days ago with status 'queued'
    (never went live) gets a Slack nudge in the original approval thread.

    Runs every 24h via a daemon thread started in main(). Silent on failures
    so a bad entry never crashes the bot.
    """
    STALE_DAYS = 7
    while True:
        try:
            time.sleep(86_400)  # 24 hours
            from datetime import datetime, timezone
            state = _load_launched_offers()
            now = datetime.now(timezone.utc)
            for advertiser, entry in state.items():
                if entry.get("status") != "queued":
                    continue
                approved_at_str = entry.get("approved_at", "")
                if not approved_at_str:
                    continue
                try:
                    approved_at = datetime.fromisoformat(approved_at_str).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                age_days = (now - approved_at).days
                if age_days < STALE_DAYS:
                    continue

                thread_url = entry.get("thread_url", "")
                payout     = entry.get("payout", "")
                network    = entry.get("network", "")

                # Find the original approval channel + thread from the URL
                # thread_url format: https://momentscience.slack.com/archives/C.../p...
                import re as _re
                m = _re.search(r'/archives/([A-Z0-9]+)/p(\d+)', thread_url)
                if not m:
                    log.debug(f"Stale queue: can't parse thread URL for {advertiser}")
                    continue

                channel   = m.group(1)
                ts_raw    = m.group(2)
                thread_ts = f"{ts_raw[:10]}.{ts_raw[10:]}"

                msg = (
                    f":hourglass: *{advertiser}* has been in the queue for *{age_days} days* "
                    f"({payout} · {network}) with no impressions detected.\n"
                    f"Still in progress? Reply to confirm — or Reject to free the slot."
                )
                web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)
                log.info(f"Stale queue nudge sent for {advertiser} ({age_days} days)")

        except Exception as e:
            log.error(f"[stale_queue] cycle failed, will retry in 24h: {e}", exc_info=True)


def _find_campaign_for_offer(ch, adv_name: str, approved_at: str) -> list:
    """
    Find campaign IDs in from_airbyte_campaigns that started within 30 days after
    an offer was approved for this advertiser.

    AdOps creates the campaign in the MS platform AFTER approval — so campaigns
    won't exist at approval time. Using start_date in [approved_at, approved_at+30d]
    targets the specific campaign launched for this offer, not pre-existing ones.

    Returns list of int campaign_id. Empty list means fall back to fuzzy name match.
    """
    if not adv_name or not adv_name.strip() or not approved_at:
        return []
    try:
        q = """
        SELECT toUInt64(id) AS campaign_id
        FROM from_airbyte_campaigns
        WHERE adv_name ILIKE {adv_pattern:String}
          AND trim(status) = 'active'
          AND start_date >= toDate({approved_at:String})
          AND start_date <= toDate({approved_at:String}) + INTERVAL 30 DAY
        ORDER BY start_date ASC
        LIMIT 10
        """
        rows = ch.query(q, parameters={
            "adv_pattern": f"%{adv_name}%",
            "approved_at": approved_at[:10],  # YYYY-MM-DD
        }).result_rows
        return [int(r[0]) for r in rows if r[0]]
    except Exception as e:
        log.warning(f"_find_campaign_for_offer failed for {adv_name!r}: {e}")
        return []


def _performance_recap(web: WebClient) -> None:
    """
    7-day post-launch performance recap daemon.

    Runs daily. For every queued/live offer where:
      - approved_at is 7+ days ago
      - performance_recap_sent is False

    Pulls actual RPM from ClickHouse, compares to Scout's estimate at approval time,
    posts a 3-line recap in the original approval thread, marks recap as sent.

    Campaign matching uses a timing-based approach: find campaigns where start_date
    falls in [approved_at, approved_at+30d] — this targets the campaign AdOps created
    for this specific offer rather than pre-existing campaigns for the same advertiser.
    Falls back to all-time fuzzy adv_name match if no timing match found.

    This is the feedback loop that makes the model legible:
      Scout estimated $X → actual came in at $Y (+/-Z%)
    The ClickHouse benchmarks already self-improve as offers accumulate data.
    This thread post makes that improvement visible and builds team trust.
    """
    RECAP_DAYS = 7

    while True:
        try:
            time.sleep(86_400)
            from datetime import datetime, timezone
            from scout_agent import _get_ch_client
            state   = _load_launched_offers()
            now     = datetime.now(timezone.utc)
            updated = False

            # Single ClickHouse connection for the full cycle — avoids N+1 per offer
            ch = _get_ch_client()

            for advertiser, entry in list(state.items()):
                if entry.get("performance_recap_sent"):
                    continue
                approved_at_str = entry.get("approved_at", "")
                if not approved_at_str:
                    continue
                try:
                    approved_at = datetime.fromisoformat(approved_at_str).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if (now - approved_at).days < RECAP_DAYS:
                    continue

                thread_url = entry.get("thread_url", "")
                m = re.search(r'/archives/([A-Z0-9]+)/p(\d+)', thread_url)
                if not m:
                    continue
                channel   = m.group(1)
                ts_raw    = m.group(2)
                thread_ts = f"{ts_raw[:10]}.{ts_raw[10:]}"

                # Pull actual impressions + revenue since approval date.
                # Timing-based match: find campaigns AdOps created for this offer
                # (start_date window). Fallback to fuzzy name if no match found.
                try:
                    campaign_ids = _find_campaign_for_offer(ch, advertiser, approved_at_str)
                    if campaign_ids:
                        id_list = ", ".join(str(i) for i in campaign_ids)
                        q = f"""
                        SELECT
                            count()                          AS impressions,
                            sum(toFloat64OrNull(revenue))    AS total_revenue
                        FROM default.adpx_conversionsdetails conv
                        WHERE conv.campaign_id IN ({id_list})
                          AND conv.created_at >= toDateTime({{approved_at_str:String}})
                          AND toYYYYMM(conv.created_at) >= toYYYYMM(toDate({{approved_at_date:String}}))
                        """
                        log.info(f"[recap] Timing-anchored match for {advertiser}: campaign_ids={campaign_ids}")
                    else:
                        q = """
                        SELECT
                            count()                          AS impressions,
                            sum(toFloat64OrNull(revenue))    AS total_revenue
                        FROM default.adpx_conversionsdetails conv
                        JOIN default.mv_adpx_campaigns c
                          ON toInt64(conv.campaign_id) = toInt64(c.id)
                        WHERE c.adv_name ILIKE {adv_pattern:String}
                          AND conv.created_at >= toDateTime({approved_at_str:String})
                          AND toYYYYMM(conv.created_at) >= toYYYYMM(toDate({approved_at_date:String}))
                        """
                        log.info(f"[recap] No timing match for {advertiser} — using fuzzy name fallback")
                    rows = ch.query(q, parameters={
                        "adv_pattern":      f"%{advertiser}%",
                        "approved_at_str":  approved_at_str,
                        "approved_at_date": approved_at_str[:10],
                    }).result_rows
                    impressions   = int((rows[0][0] if rows else 0) or 0)
                    total_revenue = float((rows[0][1] if rows else 0) or 0)
                except Exception as ch_err:
                    log.warning(f"Recap ClickHouse query failed for {advertiser}: {ch_err}")
                    continue

                # Build the recap message
                estimated = entry.get("scout_score_estimated", 0)
                payout    = entry.get("payout", "")
                network   = entry.get("network", "")

                if impressions < 100:
                    # Not enough data yet — skip, will catch next cycle
                    log.info(f"Recap skipped for {advertiser}: only {impressions} impressions at 7d")
                    continue

                actual_rpm = round(total_revenue / impressions * 1000, 0) if impressions else 0

                if estimated and actual_rpm:
                    delta_pct = round((actual_rpm - estimated) / estimated * 100)
                    direction = f"+{delta_pct}%" if delta_pct >= 0 else f"{delta_pct}%"
                    accuracy  = "on the money" if abs(delta_pct) <= 15 else ("above estimate" if delta_pct > 0 else "below estimate")
                    score_line = f"Scout estimated *${estimated:,.0f} RPM* → actual *${actual_rpm:,.0f} RPM* ({direction}, {accuracy})"
                elif actual_rpm:
                    score_line = f"Actual RPM at 7 days: *${actual_rpm:,.0f}* ({impressions:,} impressions)"
                else:
                    score_line = f"No conversions detected at 7 days ({impressions:,} impressions)"

                msg = (
                    f":bar_chart: *{advertiser}* — 7-day performance recap\n"
                    f"{score_line}\n"
                    f"_{payout} · {network} · {impressions:,} impressions_"
                )
                web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)

                # Also DM the approver — they won't be watching a week-old thread
                approved_by = entry.get("approved_by", "")
                if approved_by:
                    try:
                        dm_ch = web.conversations_open(users=[approved_by])["channel"]["id"]
                        dm_body = (
                            f":bar_chart: *{advertiser}* — 7-day recap\n"
                            f"{score_line}\n"
                            f"_{payout} · {network} · {impressions:,} impressions_"
                        )
                        if thread_url:
                            dm_body += f" · <{thread_url}|view brief>"
                        web.chat_postMessage(channel=dm_ch, text=dm_body)
                    except Exception as _dm_err:
                        log.warning(f"Recap DM failed for {approved_by}: {_dm_err}")

                # Mark sent — won't re-post
                state[advertiser]["performance_recap_sent"] = True
                state[advertiser]["actual_rpm_7d"]          = actual_rpm
                state[advertiser]["impressions_7d"]         = impressions
                updated = True
                log.info(f"7-day recap sent for {advertiser}: est=${estimated} actual=${actual_rpm}")

                # Feed actuals back into learned benchmarks
                if actual_rpm > 0:
                    _update_benchmark_from_actuals(
                        advertiser, actual_rpm,
                        payout_type=entry.get("payout_type", ""),
                    )

            if updated:
                _save_launched_offers(state)

        except Exception as e:
            log.error(f"[performance_recap] cycle failed, will retry in 24h: {e}", exc_info=True)


def _cleanup_state() -> None:
    """
    Nightly cleanup of state files to prevent unbounded growth.
    - pending_briefs.json: drop entries > 30 days old
    - thread_context.json: keep last 500 by last_updated
    - launched_offers.json: keep all (lifecycle data, small)
    - digest_state.json: keep approved forever, drop rejected > 90 days
    Also called once at startup to recover from accumulated debt.
    """
    from datetime import datetime, timezone, timedelta

    def _parse_ts_age(ts_str: str, now: datetime) -> int:
        """Return age in days of a Slack thread timestamp (epoch.microseconds format)."""
        try:
            epoch = float(ts_str.replace(".", "")[:10])
            created = datetime.fromtimestamp(epoch, tz=timezone.utc)
            return (now - created).days
        except Exception:
            return 0  # unknown age → keep

    def _run_cleanup():
        now = datetime.now(timezone.utc)

        # 1. pending_briefs.json — drop entries older than 30 days
        try:
            briefs = _load_briefs()
            pruned_briefs = {
                ts: data for ts, data in briefs.items()
                if _parse_ts_age(ts, now) < 30
            }
            if len(pruned_briefs) < len(briefs):
                _atomic_write(_STATE_FILE, pruned_briefs)
                log.info(f"Cleanup: pruned {len(briefs) - len(pruned_briefs)} old brief entries")
        except Exception as e:
            log.warning(f"Cleanup: briefs prune failed: {e}")

        # 2. thread_context.json — LRU eviction, keep last 500
        try:
            ctx = _load_thread_contexts()
            if len(ctx) > 500:
                sorted_keys = sorted(
                    ctx.keys(),
                    key=lambda k: ctx[k].get("last_updated", ""),
                    reverse=True,
                )
                pruned_ctx = {k: ctx[k] for k in sorted_keys[:500]}
                _atomic_write(_THREAD_CTX_FILE, pruned_ctx)
                log.info(f"Cleanup: evicted {len(ctx) - 500} old thread contexts")
        except Exception as e:
            log.warning(f"Cleanup: thread context eviction failed: {e}")

        # 3. digest_state.json — keep approved forever, drop rejected > 90 days
        try:
            digest_state_path = _DATA_DIR / "digest_state.json"
            if digest_state_path.exists():
                state = json.loads(digest_state_path.read_text())
                rejected = state.get("rejected", {})
                cutoff_90 = (now - timedelta(days=90)).strftime("%Y-%m-%d")
                pruned_rejected = {
                    k: v for k, v in rejected.items()
                    if v.get("actioned_at", "9999") > cutoff_90
                }
                if len(pruned_rejected) < len(rejected):
                    state["rejected"] = pruned_rejected
                    _atomic_write(digest_state_path, state)
                    log.info(f"Cleanup: pruned {len(rejected) - len(pruned_rejected)} old rejections")
        except Exception as e:
            log.warning(f"Cleanup: digest state prune failed: {e}")

    # Run once at startup
    _run_cleanup()

    # Then nightly
    while True:
        try:
            time.sleep(86_400)
            _run_cleanup()
        except Exception as e:
            log.error(f"[cleanup_state] cycle failed: {e}", exc_info=True)


_PID_FILE = _DATA_DIR / "scout.pid"


def _check_singleton() -> None:
    """Prevent two Scout processes from running simultaneously and double-posting.

    On Render, Background Workers are single-instance by platform design — skip
    the PID check entirely.  Render recycles small container PIDs (1-10) between
    restarts, so os.kill(stale_pid, 0) would hit an unrelated system process,
    return successfully, and cause a false-positive sys.exit(1) crash loop.
    """
    import atexit, sys

    # Render sets RENDER=true automatically; trust the platform for single-instance.
    if _BOT_CFG.is_render:
        log.info("[main] Running on Render — skipping singleton PID check")
        _PID_FILE.write_text(str(os.getpid()))
        atexit.register(lambda: _PID_FILE.unlink(missing_ok=True))
        return

    # Local: check for an already-running Scout process via PID file
    if _PID_FILE.exists():
        try:
            existing_pid = int(_PID_FILE.read_text().strip())
            os.kill(existing_pid, 0)   # raises ProcessLookupError if dead
            log.error(
                "[main] Scout already running (PID %s). "
                "Kill it first or delete data/scout.pid. Exiting.",
                existing_pid,
            )
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            pass   # stale PID file — safe to overwrite
    _PID_FILE.write_text(str(os.getpid()))
    atexit.register(lambda: _PID_FILE.unlink(missing_ok=True))


def _seed_entity_overrides() -> None:
    """Ensure Button fill-rate exclusion exists in data/entity_overrides.json on first deploy."""
    import datetime as _dt
    overrides = _tm.entity_overrides()
    pubs = overrides.setdefault("publishers", {})
    if "Button" not in pubs:
        pubs["Button"] = {
            "note": (
                "Pre-purchase SDK calls — Button cannot detect the purchase page, so they fire "
                "SDK calls early in the user journey before a purchase is confirmed. "
                "High session counts with low fill rate are expected behavior, not a signal failure."
            ),
            "exclude_from_fill_rate": True,
            "added": _dt.date.today().isoformat(),
            "added_by": "seed",
        }
        _tm.save_entity_overrides(overrides)
        log.info("[startup] seeded Button exclusion into data/entity_overrides.json")


def _run_startup_smoke_test(web: WebClient) -> None:
    """
    Run smoke tests on every startup and post results to #scout-qa.
    Non-blocking — runs in a background thread so it doesn't delay bot startup.
    Catches the class of bug that just burned us: bad model name, broken import,
    ClickHouse down, etc. — all invisible until someone @mentions Scout.

    Also seeds _LAST_HEALTH_STATUS so the heartbeat (PR 15c) starts with a baseline.
    Without this, the first heartbeat tick fires a Slack alert for the OK→OK transition
    on every restart.
    """
    global _LAST_HEALTH_STATUS
    try:
        import smoke_test as _st
        results, pass_count = _st.run_tests(quiet=True)
        total = len(results)
        blocks, fallback = _st.format_slack_blocks(results, pass_count)
        web.chat_postMessage(channel=_SCOUT_HQ_CHANNEL, text=fallback, blocks=blocks, unfurl_links=False)
        log.info(f"[smoke] {pass_count}/{total} checks passed — posted to #scout-qa")
        # PR 15c: seed health baseline AFTER smoke posts so the heartbeat doesn't double-alert
        try:
            initial_status = _compute_health_status()
            with _HEALTH_STATUS_LOCK:
                _LAST_HEALTH_STATUS = initial_status
            log.info(f"[health] heartbeat baseline seeded: ok={initial_status['ok']}")
        except Exception as seed_err:
            log.warning(f"[health] failed to seed heartbeat baseline: {seed_err}")
        # PR 16c: close the 35-min CH startup blind spot. The heartbeat doesn't
        # tick until WARMUP+INTERVAL = ~35 min after startup. Without this check,
        # a CH outage at deploy time stays invisible for that window. Run a
        # standalone CH ping NOW and post an immediate alert on failure.
        try:
            from scout_agent import _get_ch_client
            ch = _get_ch_client()
            ch.query("SELECT 1")
            log.info("[smoke] startup CH ping ok")
        except Exception as ch_err:
            log.error(f"[smoke] startup CH ping failed: {ch_err}")
            try:
                web.chat_postMessage(
                    channel=_SCOUT_HQ_CHANNEL,
                    text=(
                        f":red_circle: *Scout startup: ClickHouse unreachable* — `{ch_err}`\n"
                        f"Heartbeat won't catch this for ~35 min. Investigate now."
                    ),
                )
            except Exception as slack_err:
                log.warning(f"[smoke] startup CH alert failed: {slack_err}")
        # Mirror the CH startup ping for Anthropic: a revoked/invalid API key at
        # deploy time would otherwise stay invisible until the first heartbeat
        # (~35 min). 1-token ping is cheap and only runs once at startup.
        try:
            import anthropic as _anthropic
            _anth_client = _anthropic.Anthropic()
            _anth_client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=1,
                messages=[{"role": "user", "content": "ping"}],
            )
            log.info("[smoke] startup Anthropic ping ok")
        except Exception as anth_err:
            log.error(f"[smoke] startup Anthropic ping failed: {anth_err}")
            try:
                web.chat_postMessage(
                    channel=_SCOUT_HQ_CHANNEL,
                    text=(
                        f":red_circle: *Scout startup: Anthropic API unreachable* — `{anth_err}`\n"
                        f"Heartbeat won't catch this for ~35 min. Investigate now."
                    ),
                )
            except Exception as slack_err:
                log.warning(f"[smoke] startup Anthropic alert failed: {slack_err}")
        # PR 19: schema-deps validation. Validates the columns Scout reads against
        # system.columns and (where flagged must_have_data=True) confirms the column
        # has at least 100 non-null rows. Catches the categories-NULL class of
        # silent failure that bit us when Scout was reading a column with no data.
        try:
            from scout_agent import _get_ch_client as _gcc
            ch = _gcc()
            schema_result = _tm.validate_schema_deps(ch)
            if schema_result["ok"]:
                log.info(
                    f"[smoke] schema deps OK — {schema_result['checked']} columns validated"
                )
            else:
                bullets = "\n".join(f"  • {v}" for v in schema_result["violations"])
                log.error(
                    f"[smoke] schema deps violations:\n{bullets}"
                )
                try:
                    web.chat_postMessage(
                        channel=_SCOUT_HQ_CHANNEL,
                        text=(
                            f":warning: *Scout schema-deps validation: {len(schema_result['violations'])} violation(s)*\n"
                            f"{bullets}\n"
                            f"_Scout may be reading columns that no longer have data. "
                            f"Update scout_agent._SCHEMA_DEPS or fix the upstream column._"
                        ),
                    )
                except Exception as slack_err:
                    log.warning(f"[smoke] schema-deps alert failed: {slack_err}")
            for warning_msg in schema_result.get("warnings", []):
                log.warning(f"[smoke] schema-deps warning: {warning_msg}")
        except Exception as schema_err:
            log.warning(f"[smoke] schema-deps validation crashed: {schema_err}")
        # PR 19a: warm the benchmarks cache at boot. Before this, _get_benchmarks()
        # was lazy-loaded — first call (digest, Pulse, scoring) would populate it.
        # Result: a fresh deploy + immediate `@Scout status` showed "Benchmarks
        # not loaded" → the LLM recommended `@Scout refresh offers` → user had
        # to run a 2-minute scrape to fix a 2-second cache warmup. Stop that.
        try:
            t0 = time.time()
            bm = _tm.benchmarks()
            n_cats = len(bm.get("by_category", {})) if bm else 0
            n_advs = len(bm.get("by_adv_name", {})) if bm else 0
            log.info(
                f"[smoke] benchmarks warmed in {time.time()-t0:.1f}s — "
                f"{n_advs} advertisers, {n_cats} categories"
            )
        except Exception as bm_err:
            log.warning(f"[smoke] benchmark warmup failed: {bm_err}")
    except Exception as e:
        log.warning(f"[smoke] startup smoke test failed to run: {e}")
        try:
            web.chat_postMessage(
                channel=_SCOUT_HQ_CHANNEL,
                text=f":red_circle: *Scout startup smoke test crashed* — `{e}`\nCheck Render logs.",
            )
        except Exception:
            log.warning("Failed to post smoke test crash notification to Slack")


def _run_health_heartbeat(web: WebClient) -> None:
    """
    Live health heartbeat (PR 15c).

    Runs every 30 min after a 5-min warmup. Checks _compute_health_status() PLUS a
    standalone ClickHouse ping (SELECT 1). The CH ping result affects this heartbeat
    only — NOT the HTTP /health endpoint that Render uses for container restarts.
    Critical: a CH outage must not restart the bot.

    Posts a single Slack alert on the first transition to degraded after
    _HEALTH_CONSECUTIVE_THRESHOLD consecutive bad checks. Transitions back to OK
    also post once. No spam during sustained degradation.
    """
    import time as _time
    global _LAST_HEALTH_STATUS

    _time.sleep(_HEALTH_HEARTBEAT_WARMUP_SECS)  # let daemons settle and smoke test post
    consecutive_bad = 0

    while True:
        try:
            status = _compute_health_status()

            # Standalone CH ping — affects HEARTBEAT only, never the HTTP probe
            ch_ok = True
            ch_detail = "ok"
            try:
                from scout_agent import _get_ch_client
                ch = _get_ch_client()
                ch.query("SELECT 1")
            except Exception as ch_err:
                ch_ok = False
                ch_detail = f"CH ping failed: {ch_err}"

            # Live Anthropic auth ping — 1-token completion confirms the API key still
            # works. Like the CH ping, this affects HEARTBEAT only, never the HTTP probe:
            # an Anthropic outage or rate-limit must not restart the bot.
            anthropic_ok = True
            anthropic_detail = "ok"
            try:
                import anthropic as _anthropic
                _anth_client = _anthropic.Anthropic()
                _anth_client.messages.create(
                    model="claude-haiku-4-5",
                    max_tokens=1,
                    messages=[{"role": "user", "content": "ping"}],
                )
            except Exception as anth_err:
                anthropic_ok = False
                anthropic_detail = f"Anthropic ping failed: {anth_err}"

            # Combine — if any live probe is bad, the heartbeat is bad even if HTTP /health is fine
            heartbeat_ok = bool(status["ok"]) and ch_ok and anthropic_ok
            status_with_ch = dict(status)
            status_with_ch["checks"] = dict(status["checks"])
            status_with_ch["checks"]["clickhouse_heartbeat"] = {"ok": ch_ok, "detail": ch_detail}
            status_with_ch["checks"]["anthropic_heartbeat"] = {"ok": anthropic_ok, "detail": anthropic_detail}
            status_with_ch["ok"] = heartbeat_ok

            with _HEALTH_STATUS_LOCK:
                last = _LAST_HEALTH_STATUS
                last_ok = bool(last and last.get("ok"))

            if not heartbeat_ok:
                consecutive_bad += 1
                if consecutive_bad >= _HEALTH_CONSECUTIVE_THRESHOLD and last_ok:
                    # Transition OK → BAD: alert once
                    bad_checks = [k for k, v in status_with_ch["checks"].items() if not v["ok"]]
                    detail_lines = "\n".join(
                        f"  • *{k}*: {status_with_ch['checks'][k]['detail']}" for k in bad_checks
                    )
                    try:
                        web.chat_postMessage(
                            channel=_SCOUT_HQ_CHANNEL,
                            text=(
                                f":red_circle: *Scout heartbeat: degraded* "
                                f"({consecutive_bad} consecutive checks failed)\n{detail_lines}"
                            ),
                        )
                    except Exception as slack_err:
                        log.warning(f"[heartbeat] Slack alert failed: {slack_err}")
                    log.error(f"[heartbeat] degraded — failing checks: {bad_checks}")
            else:
                if not last_ok and last is not None:
                    # Transition BAD → OK: confirm recovery once
                    try:
                        web.chat_postMessage(
                            channel=_SCOUT_HQ_CHANNEL,
                            text=":large_green_circle: *Scout heartbeat: recovered* — all checks passing",
                        )
                    except Exception as slack_err:
                        log.warning(f"[heartbeat] recovery alert failed: {slack_err}")
                    log.info("[heartbeat] recovered to ok")
                consecutive_bad = 0

            with _HEALTH_STATUS_LOCK:
                _LAST_HEALTH_STATUS = status_with_ch

        except Exception as e:
            log.warning(f"[heartbeat] check loop error: {e}")

        _time.sleep(_HEALTH_HEARTBEAT_INTERVAL_SECS)


# ── Notion → Slack status watcher ────────────────────────────────────────────





# ── Health check HTTP server ────────────────────────────────────────────────────

def _compute_health_status() -> dict:
    """Return a health dict. ok=True means Scout is fully operational."""
    import pathlib as _pl
    checks: dict[str, dict] = {}

    # 1. Offer inventory freshness
    snap = _pl.Path(__file__).parent / "data" / "offers_latest.json"
    if not snap.exists():
        checks["offer_inventory"] = {"ok": False, "detail": "offers_latest.json missing"}
    else:
        age_hours = (time.time() - snap.stat().st_mtime) / 3600
        # PR 18: staleness threshold from config/scout_thresholds.json (was hardcoded 30h)
        if age_hours > _OFFER_STALENESS_HOURS:
            checks["offer_inventory"] = {"ok": False, "detail": f"Stale — {age_hours:.0f}h old (limit {_OFFER_STALENESS_HOURS}h)"}
        else:
            checks["offer_inventory"] = {"ok": True, "detail": f"{age_hours:.0f}h old"}

    # 2. Required daemon threads alive
    # PR 16b: read from _REQUIRED_DAEMONS (populated at startup via _start_daemon)
    # instead of a hardcoded set. New daemons no longer need an edit here.
    live = {t.name for t in threading.enumerate()}
    required = set(_REQUIRED_DAEMONS)
    dead = required - live
    if dead:
        checks["daemon_threads"] = {"ok": False, "detail": f"Dead threads: {', '.join(sorted(dead))}"}
    else:
        checks["daemon_threads"] = {"ok": True, "detail": f"{len(required)} threads alive"}

    # 3. NOTION_QUEUE_DB_ID — required for correct Pipeline links in Slack messages
    queue_db_id = _BOT_CFG.notion_queue_db_id
    if not queue_db_id:
        checks["notion_queue_url"] = {"ok": False, "detail": "NOTION_QUEUE_DB_ID not set — Pipeline links point to generic Notion homepage"}
    else:
        checks["notion_queue_url"] = {"ok": True, "detail": f"Pipeline DB configured ({queue_db_id[:8]}...)"}

    # 4. Environment
    for key, val in (
        ("ANTHROPIC_API_KEY", _BOT_CFG.anthropic_api_key),
        ("SLACK_BOT_TOKEN", _BOT_CFG.bot_token),
        ("SLACK_APP_TOKEN", _BOT_CFG.app_token),
    ):
        checks[key] = {"ok": bool(val), "detail": "set" if val else "missing"}

    all_ok = all(v["ok"] for v in checks.values())
    return {"ok": all_ok, "checks": checks}


def _start_health_server(port: int = 10000) -> None:
    """Start a minimal HTTP health check server on the given port."""
    import http.server
    import json as _json

    class _HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                status = _compute_health_status()
                code = 200 if status["ok"] else 503
                body = _json.dumps(status).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args):
            pass  # silence access logs — Render health checks fire every 30s

    def _serve():
        try:
            server = http.server.HTTPServer(("0.0.0.0", port), _HealthHandler)
            log.info(f"[health] HTTP server listening on :{port}/health")
            server.serve_forever()
        except Exception as e:
            log.error(f"[health] server error: {e}")

    threading.Thread(target=_serve, daemon=True, name="health-server").start()


# ── Thread watchdog ─────────────────────────────────────────────────────────────

def _benchmarks_warmer() -> None:
    """
    PR 19a: keep CVR/RPM benchmarks warm in memory by triggering a refresh
    every 30 min. _get_benchmarks() respects its own TTL (1h) and will reload
    from ClickHouse when stale. Calling it on a schedule means the cache is
    always fresh BEFORE a digest/Pulse/status query asks for it.

    Before this daemon: benchmarks were lazy-loaded. A `@Scout status` immediately
    after deploy showed "Benchmarks not loaded" because nothing had triggered a
    load yet. The recommended action ("Run @Scout refresh offers") was a
    sledgehammer — re-scrape 4 networks for 2 min to fix a 2-second cache.
    Now: benchmarks warm at boot (in _run_startup_smoke_test) and stay warm
    via this daemon. Status check no longer surfaces "not loaded" except in
    actual ClickHouse outage scenarios (which the heartbeat already alerts on).
    """
    import time as _time
    _time.sleep(60)  # let boot-time warm finish first
    while True:
        try:
            t0 = _time.time()
            bm = _tm.benchmarks()
            n_cats = len(bm.get("by_category", {})) if bm else 0
            log.debug(
                f"[benchmarks-warmer] refreshed in {_time.time()-t0:.1f}s — {n_cats} categories"
            )
        except Exception as e:
            log.warning(f"[benchmarks-warmer] refresh failed: {e}")
        _time.sleep(1800)  # 30 min — same cadence as health-heartbeat


def _thread_watchdog(web: WebClient) -> None:
    """Check all named daemon threads are alive every 60s.

    Alerts #scout-qa on TRANSITION only — when a daemon newly dies or recovers.
    Does NOT spam on every check interval when a daemon is already known-dead.
    """
    import time as _time

    # PR 16b: read from _REQUIRED_DAEMONS (populated at startup via _start_daemon)
    # instead of a hardcoded set. Same source of truth as _compute_health_status().
    _time.sleep(120)  # Give all threads time to start before first check
    last_dead: set[str] = set()  # tracks already-alerted dead daemons
    while True:
        try:
            live = {t.name for t in threading.enumerate()}
            required = set(_REQUIRED_DAEMONS)
            dead = required - live
            newly_dead = dead - last_dead
            recovered  = last_dead - dead
            if newly_dead:
                names = ", ".join(sorted(newly_dead))
                log.error(f"[watchdog] daemon thread(s) newly died: {names}")
                try:
                    web.chat_postMessage(
                        channel=_SCOUT_HQ_CHANNEL,
                        text=f":warning: Scout daemon thread(s) died: *{names}*. Render may need a restart.",
                    )
                except Exception as slack_err:
                    log.warning(f"[watchdog] Slack alert failed: {slack_err}")
            if recovered:
                names = ", ".join(sorted(recovered))
                log.info(f"[watchdog] daemon thread(s) recovered: {names}")
                try:
                    web.chat_postMessage(
                        channel=_SCOUT_HQ_CHANNEL,
                        text=f":white_check_mark: Scout daemon thread(s) recovered: *{names}*",
                    )
                except Exception as slack_err:
                    log.warning(f"[watchdog] Slack recovery alert failed: {slack_err}")
            last_dead = dead
        except Exception as e:
            log.warning(f"[watchdog] check error: {e}")
        _time.sleep(60)


def _on_startup(web: WebClient) -> None:
    """Post 'Scout is back' to sidd-qa on every boot.
    If maintenance is active, reports the count and tells admin to run /scout-maintenance off.
    Does NOT auto-clear maintenance — clearing is done only via /scout-maintenance off."""
    from scout_state import get_maintenance
    m = get_maintenance()
    sidd_qa = _BOT_CFG.sidd_qa_channel_id
    if not sidd_qa:
        return
    msg = ":white_check_mark: Scout is back online."
    if m:
        attempt_count = len(m.get("attempts", []))
        msg += f" :wrench: Maintenance is still active ({attempt_count} blocked attempt(s) so far). Run `/scout-maintenance off` to re-open."
    try:
        web.chat_postMessage(channel=sidd_qa, text=msg)
    except Exception as e:
        log.warning(f"[startup] sidd-qa post failed: {e}")


def main():
    global _BOT_USER_ID
    _check_singleton()
    from scout_ch import _validate_ch_timeout_budget
    _validate_ch_timeout_budget()
    _seed_entity_overrides()  # ensure Button exclusion survives fresh Render deploys
    if not _BOT_CFG.bot_token or not _BOT_CFG.app_token:
        raise RuntimeError("SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env")

    web_client    = WebClient(token=_BOT_CFG.bot_token, retry_handlers=[RateLimitErrorRetryHandler(max_retry_count=3)])
    from scout_slack_safe import guard_web_client
    guard_web_client(web_client)
    _BOT_USER_ID  = web_client.auth_test()["user_id"]
    # Inject shared state into scout_handlers (avoids circular import — handlers don't import scout_bot)
    _set_bot_user_id(_BOT_USER_ID)
    _set_thread_state(_LAST_THREAD_PER_CHANNEL, _LAST_THREAD_LOCK)
    _set_force_monitor_fn("cap",        lambda web, ch, t="": _one_shot_monitor(web, ch, _pulse_signal_cap, _format_cap_alert, thread_ts=t, alert_name="cap-monitor"))
    _set_force_monitor_fn("velocity",   lambda web, ch, t="": _one_shot_monitor(web, ch, _pulse_signal_velocity, _format_velocity_down_alert, thread_ts=t, alert_name="velocity-down-monitor"))
    _set_force_monitor_fn("ghost",      lambda web, ch, t="": _one_shot_monitor(web, ch, _pulse_signal_ghost, _format_ghost_alert, thread_ts=t, alert_name="ghost-monitor"))
    _set_force_monitor_fn("fill",       lambda web, ch, t="": _one_shot_monitor(web, ch, _pulse_signal_fill_rate, _format_fill_alert, thread_ts=t, alert_name="fill-monitor"))
    _set_force_monitor_fn("cvr",        lambda web, ch, t="": _one_shot_monitor(web, ch, _query_cvr_anomaly, _format_cvr_alert, thread_ts=t, alert_name="cvr-anomaly-monitor"))
    _set_force_monitor_fn("expiration", lambda web, ch, t="": _one_shot_monitor(web, ch, _query_expiring_campaigns, _format_expiration_alert, thread_ts=t, alert_name="expiration-monitor"))
    _set_force_monitor_fn("revenue",    lambda web, ch, t="": _run_revenue_check_once(web, ch, t))
    # PR-B: inject web + CH factory so the force_run_monitor agent tool can call
    # the same monitor lambdas registered above.
    from scout_agent import _set_force_monitor_ctx as _set_fmc, _get_ch_client as _ch_factory
    _set_fmc(web_client, _ch_factory)
    socket_client = SocketModeClient(app_token=_BOT_CFG.app_token, web_client=web_client)
    socket_client.socket_mode_request_listeners.append(handle_event)

    # PR 16b: required daemons go through _start_daemon() — auto-registered in
    # _REQUIRED_DAEMONS so health check + watchdog see them without manual edits.
    # One-shot or self-monitoring threads (smoke-test, watchdogs, HTTP server) keep
    # the raw threading.Thread call so they don't appear in the required set.

    # Startup smoke test — one-shot, NOT a long-running daemon
    threading.Thread(target=_run_startup_smoke_test, args=(web_client,), daemon=True, name="smoke-test").start()

    # Required long-running daemons (registered via _start_daemon)
    _start_daemon(_check_stale_queue,     name="stale-queue-checker", args=(web_client,))
    _start_daemon(_performance_recap,     name="perf-recap",          args=(web_client,))
    _start_daemon(_cleanup_state,         name="state-cleanup")
    _start_daemon(_digest_poster,         name="digest-poster",         args=(web_client,))
    _start_daemon(_notion_watcher_loop,   name="notion-watcher",        args=(web_client,))
    _start_daemon(_copy_coalescer_loop,   name="copy-coalescer")
    _start_daemon(lambda: _run_health_heartbeat(web_client), name="health-heartbeat")
    _start_daemon(_benchmarks_warmer,     name="benchmarks-warmer")

    # Background: daily launch health watchdog (no register — campaign-level, not infrastructure)
    threading.Thread(target=_launch_watchdog, args=(web_client,), daemon=True, name="launch-watchdog").start()
    # Background: thread watchdog — must NOT register itself (would alert if it died, but it's the alerter)
    threading.Thread(target=_thread_watchdog, args=(web_client,), daemon=True, name="thread-watchdog").start()

    # Health check HTTP server — Render pings /health every 30s to verify Scout is alive
    _start_health_server(port=_BOT_CFG.port)

    log.info("Scout is online — listening for @mentions via Socket Mode")
    socket_client.connect()
    _on_startup(web_client)

    import signal as _signal

    def _handle_sigterm(signum, frame):
        log.info("SIGTERM received — shutting down cleanly")
        try:
            socket_client.close()
        except Exception:
            pass
        time.sleep(3)  # allow in-flight Slack acks to flush
        import sys as _sys
        _sys.exit(0)

    _signal.signal(_signal.SIGTERM, _handle_sigterm)
    _signal.pause()


if __name__ == "__main__":
    main()
