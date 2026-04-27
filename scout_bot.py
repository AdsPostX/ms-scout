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
from scout_notion import (
    _copy_coalescer_loop,
    _notion_watcher_loop,
)
from scout_slack_ui import (
    _format_pulse_blocks,
)
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
    _set_bot_user_id, _set_thread_state, _set_pulse_runner,
    handle_event,
)

load_dotenv()  # plist env vars (SCOUT_ENV, PULSE_CHANNEL, etc.) take precedence over .env

# Mutex: prevents daemon and on-demand trigger from running run_headless() concurrently.
# On-demand run_offer_scraper() checks this before calling run_headless().
_SCRAPER_RUNNING = threading.Event()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scout_bot")

BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
APP_TOKEN = os.getenv("SLACK_APP_TOKEN")

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

_SCOUT_HQ_CHANNEL  = "C0AQEECF800"   # #scout-qa (was #scout-hq)


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
_PULSE_CHANNEL               = os.getenv("PULSE_CHANNEL", "")  # kept for backwards compat
_PULSE_ENABLED               = os.getenv("PULSE_ENABLED", "true").lower() == "true"

# ── Environment-aware channel routing ─────────────────────────────────────────
# SCOUT_ENV=production → messages go to production channels (set in launchd plist)
# Anything else (unset, "development") → everything goes to #scout-qa
# force=True → always #scout-qa regardless of environment
_SCOUT_ENV = os.getenv("SCOUT_ENV", "development")
_PRODUCTION_CHANNELS = {
    "pulse":    os.getenv("PULSE_CHANNEL", _SCOUT_HQ_CHANNEL),          # #revenue-operations
    "watchdog": os.getenv("PULSE_CHANNEL", _SCOUT_HQ_CHANNEL),          # #revenue-operations
    "offers":   os.getenv("SCOUT_DIGEST_CHANNEL", _SCOUT_HQ_CHANNEL),   # #scout-offers
}

def _route_channel(purpose: str, force: bool = False) -> str:
    """
    Return the correct Slack channel for a given message purpose.
    Foolproof: force=True OR non-production env always routes to #scout-qa.
    Production channels require SCOUT_ENV=production (set in launchd plist only).
    """
    if force or _SCOUT_ENV != "production":
        return _SCOUT_HQ_CHANNEL
    return _PRODUCTION_CHANNELS.get(purpose, _SCOUT_HQ_CHANNEL)


# ── Feedback buttons ──────────────────────────────────────────────────────────



# ── Pulse signal helpers (one per signal, each owns its own ch connection) ────

def _pulse_signal_cap(ch) -> list:
    import json as _json
    from datetime import date as _date
    import calendar as _cal
    results = []
    try:
        cap_rows = ch.query(
            """
            SELECT
                c.id          AS campaign_id,
                c.adv_name,
                c.capping_config,
                coalesce(sum(toFloat64OrNull(cv.revenue)), 0) AS revenue_this_month
            FROM from_airbyte_campaigns c
            LEFT JOIN adpx_conversionsdetails cv
                ON toInt64(cv.campaign_id) = c.id
                AND toYYYYMM(cv.created_at) = toYYYYMM(today())
            WHERE c.deleted_at IS NULL
              AND c.capping_config IS NOT NULL
              AND c.capping_config != ''
              AND c.capping_config != 'null'
            GROUP BY c.id, c.adv_name, c.capping_config
            """
        ).result_rows
        today_d = _date.today()
        days_in_month = _cal.monthrange(today_d.year, today_d.month)[1]
        days_remaining = days_in_month - today_d.day + 1
        for camp_id, adv_name, cap_cfg, revenue_mtd in cap_rows:
            try:
                cfg = _json.loads(cap_cfg) if isinstance(cap_cfg, str) else (cap_cfg or {})
                mb  = float((cfg.get("month") or {}).get("budget") or 0)
            except Exception:
                mb = 0.0
            if mb <= 0:
                continue
            cap_pct = revenue_mtd / mb
            if cap_pct < 0.70:
                continue
            daily_run_rate = revenue_mtd / max(today_d.day, 1)
            days_to_cap    = (mb - revenue_mtd) / daily_run_rate if daily_run_rate > 0 else 999
            results.append({
                "adv_name":       adv_name,
                "campaign_id":    int(camp_id) if camp_id else None,
                "monthly_cap":    mb,
                "revenue_mtd":    round(revenue_mtd, 2),
                "cap_pct":        round(cap_pct * 100, 1),
                "days_remaining": days_remaining,
                "days_to_cap":    round(days_to_cap, 1),
            })
        results.sort(key=lambda x: x["cap_pct"], reverse=True)
    except Exception as e:
        log.warning(f"Pulse cap signal failed: {e}")
    return results


def _pulse_signal_velocity(ch) -> list:
    results: list = []
    try:
        vel_rows = ch.query(
            """
            SELECT
                user_id,
                sum(toFloat64OrNull(revenue))                                           AS revenue_30d,
                sumIf(toFloat64OrNull(revenue), created_at >= today() - 7)              AS revenue_7d
            FROM adpx_conversionsdetails
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
            WHERE created_at >= today() - 30
            GROUP BY user_id
            HAVING revenue_30d > 5000
            ORDER BY revenue_30d DESC
            LIMIT 200
            """
        ).result_rows

        uid_list = [str(r[0]) for r in vel_rows if r[0]]
        org_map: dict = {}
        if uid_list:
            try:
                id_csv = ",".join(uid_list[:200])
                name_rows = ch.query(
                    f"SELECT id, organization FROM from_airbyte_users WHERE id IN ({id_csv}) LIMIT 200"
                ).result_rows
                org_map = {str(r[0]): r[1] for r in name_rows}
            except Exception:
                log.debug("suppressed: publisher name enrichment query failed", exc_info=True)

        for user_id, rev_30d, rev_7d in vel_rows:
            rev_7d_ann = (rev_7d / 7) * 30 if rev_7d else 0
            if rev_30d <= 0:
                continue
            pct_delta = (rev_7d_ann - rev_30d) / rev_30d * 100
            if abs(pct_delta) < 40:
                continue
            results.append({
                "publisher_name":  org_map.get(str(user_id), f"Partner {user_id}"),
                "publisher_id":    int(user_id) if user_id else None,
                "revenue_30d":     round(rev_30d, 2),
                "revenue_7d_ann":  round(rev_7d_ann, 2),
                "pct_delta":       round(pct_delta, 1),
                "direction":       "up" if pct_delta > 0 else "down",
                "top_advertisers": [],
            })
        results.sort(key=lambda x: abs(x["pct_delta"]), reverse=True)
        results = results[:5]

        vel_pub_ids = [v["publisher_id"] for v in results if v["publisher_id"]]
        if vel_pub_ids:
            try:
                pub_id_csv = ",".join(str(p) for p in vel_pub_ids)
                attr_rows = ch.query(
                    f"""
                    SELECT
                        cv.user_id,
                        c.adv_name,
                        sum(toFloat64OrNull(cv.revenue))                                       AS rev_30d,
                        sumIf(toFloat64OrNull(cv.revenue), cv.created_at >= today() - 7)       AS rev_7d,
                        (sumIf(toFloat64OrNull(cv.revenue), cv.created_at >= today() - 7)
                            / 7 * 30) - sum(toFloat64OrNull(cv.revenue))                      AS delta_ann
                    FROM adpx_conversionsdetails cv
                    JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
                    PREWHERE cv.user_id IN ({pub_id_csv})
                        AND toYYYYMM(cv.created_at) >= toYYYYMM(today() - 30)
                    WHERE cv.created_at >= today() - 30
                      AND c.deleted_at IS NULL
                    GROUP BY cv.user_id, c.adv_name
                    ORDER BY cv.user_id, abs(delta_ann) DESC
                    """
                ).result_rows
                attr_map: dict = {}
                for uid, adv_name, rev_30d_a, rev_7d_a, delta_a in attr_rows:
                    key = int(uid) if uid else None
                    if key not in attr_map:
                        attr_map[key] = []
                    delta_rounded = round(delta_a or 0, 0)
                    if abs(delta_rounded) < 100:
                        continue
                    if len(attr_map[key]) < 2:
                        attr_map[key].append({
                            "adv_name": adv_name,
                            "delta_ann": delta_rounded,
                            "rev_7d":    round(rev_7d_a or 0, 0),
                        })
                for v in results:
                    v["top_advertisers"] = attr_map.get(v["publisher_id"], [])
            except Exception as e:
                log.warning(f"Pulse advertiser attribution failed: {e}")

        # ── Batch: fetch existing advertisers for all down publishers in one query ──
        for v in results:
            v["hypothesis"] = ""
            v["gaps"] = []

        down_entries = [
            (v, v["publisher_id"], v.get("publisher_name", ""),
             next((a for a in v.get("top_advertisers", []) if a.get("delta_ann", 0) < 0), None))
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
                    hyp_rows = _ch.query(
                        """
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
                        """,
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
                gap_rows = _ch.query(f"""
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
                """).result_rows
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
    return results


def _pulse_signal_overnight(ch) -> list:
    import json as _json
    results = []
    try:
        event_rows = ch.query(
            """
            SELECT type, old_data, created_at
            FROM adpx_system_activity_logs
            WHERE created_at >= now() - INTERVAL 24 HOUR
              AND type IN ('pause', 'resume')
              AND entity = 'campaigns'
            ORDER BY created_at DESC
            LIMIT 15
            """
        ).result_rows
        for ev_type, old_data_str, created_at in event_rows:
            adv_name = ""
            try:
                od = _json.loads(old_data_str) if old_data_str else {}
                adv_name = od.get("adv_name") or od.get("name") or ""
            except Exception:
                log.debug("suppressed: overnight event old_data JSON parse failed", exc_info=True)
            results.append({
                "type":      ev_type,
                "adv_name":  adv_name,
                "timestamp": str(created_at) if created_at else "",
            })
    except Exception as e:
        log.warning(f"Pulse events signal failed: {e}")
    return results


def _pulse_signal_ghost(ch) -> list:
    results = []
    try:
        from scout_agent import _query_ghost_campaigns
        ghost_detail_rows = _query_ghost_campaigns(ch)
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
    return results


def _pulse_signal_fill_rate(ch) -> list:
    results = []
    try:
        from scout_agent import _POST_TX_PLACEMENTS, _load_entity_overrides as _load_eo
        placements_sql = ", ".join(_POST_TX_PLACEMENTS)
        fill_rows = ch.query(
            f"""
            WITH sessions_agg AS (
                SELECT
                    toInt64(user_id) AS publisher_id,
                    count()          AS sessions_7d
                FROM adpx_sdk_sessions
                PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
                WHERE created_at >= today() - 7
                  AND placement IN ({placements_sql})
                GROUP BY user_id
                HAVING sessions_7d > 5000
            ),
            imps_agg AS (
                SELECT
                    toInt64(pid) AS publisher_id,
                    count(DISTINCT session_id) AS sessions_with_imps
                FROM adpx_impressions_details
                PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
                WHERE created_at >= today() - 7
                GROUP BY pid
            )
            SELECT
                s.publisher_id,
                u.organization AS publisher_name,
                s.sessions_7d,
                coalesce(i.sessions_with_imps, 0) AS sessions_with_imps,
                round(100.0 * coalesce(i.sessions_with_imps, 0) / s.sessions_7d, 2) AS fill_rate_pct,
                s.sessions_7d - coalesce(i.sessions_with_imps, 0) AS missed_sessions
            FROM sessions_agg s
            LEFT JOIN imps_agg i ON i.publisher_id = s.publisher_id
            LEFT JOIN from_airbyte_users u ON s.publisher_id = u.id
            WHERE coalesce(i.sessions_with_imps, 0) * 100.0 / s.sessions_7d < 15
            ORDER BY missed_sessions DESC
            LIMIT 5
            """
        ).result_rows
        _pub_overrides = _load_eo().get("publishers", {})
        for pub_id, pub_name, sessions_7d, with_imps, fill_pct, missed in fill_rows:
            name = pub_name or f"Pub #{pub_id}"
            _override = _pub_overrides.get(name, {})
            if _override.get("exclude_from_fill_rate"):
                log.info(f"[pulse] fill rate: skipping {name!r} — {_override.get('note', '')[:60]}...")
                continue
            results.append({
                "publisher_id":   int(pub_id),
                "publisher_name": name,
                "sessions_7d":    int(sessions_7d),
                "fill_rate_pct":  round(float(fill_pct), 1),
                "missed_sessions": int(missed),
            })
    except Exception as e:
        log.warning(f"Pulse fill rate signal failed: {e}")
    return results


def _pulse_signal_opportunities(ch) -> list:
    """
    Revenue gap opportunities for the Pulse signal.

    Uses queries.revenue_opportunities() — the same SQL + fuzzy name-match anti-join
    as the agent tool (get_top_revenue_opportunities). Pulse takes the top 5 rows;
    the agent shows the full 20-item detail view with additional Python-level grouping.

    This ensures the Pulse and agent never recommend different things for the same
    publisher × advertiser pair (fuzzy suppression: "Disney+" ⊂ "Disney+ and Hulu").
    """
    try:
        rows = _q.revenue_opportunities(ch)
    except Exception as e:
        log.warning(f"Pulse opportunities signal failed: {e}")
        return []
    return [
        {
            "publisher_name":  r["publisher_name"] or "Unknown Publisher",
            "adv_name":        r["adv_name"],
            "est_monthly_rev": r["est_monthly_rev"],
            "sessions_30d":    r["sessions_30d"],
        }
        for r in rows[:5]
    ]


# ── Pulse signal orchestrator ─────────────────────────────────────────────────

def _run_pulse_signals() -> dict:
    """
    Run all 6 Pulse signals in parallel against ClickHouse.
    Each signal owns its own connection — no shared state, no lock needed.
    Returns a dict with cap_alerts, velocity_shifts, overnight_events,
    ghost_campaigns, fill_rate, opportunities.
    """
    from scout_agent import _get_ch_client
    from concurrent.futures import ThreadPoolExecutor, as_completed

    signals: dict = {"cap_alerts": [], "velocity_shifts": [], "overnight_events": [], "ghost_campaigns": [], "fill_rate": [], "opportunities": []}

    _signal_fns = [
        ("cap_alerts",       _pulse_signal_cap),
        ("velocity_shifts",  _pulse_signal_velocity),
        ("overnight_events", _pulse_signal_overnight),
        ("ghost_campaigns",  _pulse_signal_ghost),
        ("fill_rate",        _pulse_signal_fill_rate),
        ("opportunities",    _pulse_signal_opportunities),
    ]

    def _run_one(key, fn):
        ch = _get_ch_client()
        return key, fn(ch)

    with ThreadPoolExecutor(max_workers=6, thread_name_prefix="pulse") as pool:
        futures = {pool.submit(_run_one, key, fn): key for key, fn in _signal_fns}
        for future in as_completed(futures):
            try:
                key, result = future.result()
                signals[key] = result
            except Exception as e:
                log.warning(f"Pulse {futures[future]} signal failed unexpectedly: {e}")

    return signals




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


def _run_pulse_once(web: WebClient, force: bool = False) -> None:
    """
    Execute one pulse run immediately. If force=True, always routes to #scout-qa
    and skips the idempotency state write (so the scheduled pulse still fires today).
    Called by _proactive_pulse daemon and by the @Scout force pulse admin command.
    """
    import pytz
    from datetime import datetime as _dt

    chicago = pytz.timezone("America/Chicago")
    now_chi = _dt.now(chicago)
    today_str = now_chi.strftime("%Y-%m-%d")
    is_weekend = now_chi.weekday() >= 5

    state = _load_pulse_state()
    flagged_history: dict = state.get("flagged_history", {})
    today_s = today_str
    from datetime import datetime as _fdt

    signals = _run_pulse_signals()

    for v in signals.get("velocity_shifts", []):
        if v.get("direction") != "down":
            continue
        pname = v["publisher_name"]
        rec   = flagged_history.get(pname, {})
        last  = rec.get("last_flagged", "")
        if last and (_fdt.strptime(today_s, "%Y-%m-%d") - _fdt.strptime(last, "%Y-%m-%d")).days <= 2:
            rec["count"] = rec.get("count", 1) + 1
        else:
            rec = {"count": 1, "first_flagged": today_s}
        rec["last_flagged"] = today_s
        flagged_history[pname] = rec
    state["flagged_history"] = flagged_history

    chronic: list = state.get("chronic", [])
    today_dt = _fdt.strptime(today_s, "%Y-%m-%d")
    evicted = [
        pname for pname in chronic
        if (today_dt - _fdt.strptime(
            flagged_history.get(pname, {}).get("last_flagged") or "2000-01-01",
            "%Y-%m-%d",
        )).days > 14
    ]
    for pname in evicted:
        chronic.remove(pname)
    for pname, rec in flagged_history.items():
        if pname in chronic:
            continue
        count = rec.get("count", 0)
        first = rec.get("first_flagged", "")
        last  = rec.get("last_flagged", "")
        if not first or not last:
            continue
        span_days = (_fdt.strptime(last, "%Y-%m-%d") - _fdt.strptime(first, "%Y-%m-%d")).days
        if count >= 3 and span_days <= 14:
            chronic.append(pname)
    state["chronic"] = chronic

    has_content = (
        signals.get("cap_alerts")
        or signals.get("velocity_shifts")
        or signals.get("overnight_events")
        or signals.get("ghost_campaigns")
        or signals.get("fill_rate")
        or signals.get("opportunities")
    )
    # Force pulse always routes to #scout-qa; normal pulse uses _route_channel
    channel = _route_channel("pulse", force=force)
    if has_content:
        fallback, blocks = _format_pulse_blocks(
            signals,
            is_weekend=is_weekend,
            flagged_history=flagged_history,
            chronic=chronic,
        )
        web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)
        log.info(f"[pulse{'|force' if force else ''}] posted to {channel}")
    else:
        log.info("[pulse] no signals — skipping post")

    # Only update state for scheduled (non-force) runs
    if not force:
        state["last_pulse_date"] = today_str
        _save_pulse_state(state)


def _proactive_pulse(web: WebClient) -> None:
    """
    Daily proactive intelligence briefing daemon.

    Posts once per day at 8:00 AM Chicago time to the pulse channel.
    Idempotent — uses pulse_state.json to avoid double-posting.
    Surfaces: cap proximity alerts, revenue velocity shifts, overnight events.
    """
    import pytz
    from datetime import datetime as _dt, timedelta

    while True:
        try:
            chicago = pytz.timezone("America/Chicago")
            now_chi = _dt.now(chicago)
            today_str = now_chi.strftime("%Y-%m-%d")

            # CHECK FIRST: fire immediately if past 8am and haven't run today.
            # This handles: Mac was off at 8am, just resumed.
            state = _load_pulse_state()
            if state.get("last_pulse_date") != today_str and now_chi.hour >= 8:
                is_weekend = now_chi.weekday() >= 5  # Saturday=5, Sunday=6

                # Run signals
                signals = _run_pulse_signals()

                # ── Update signal fatigue tracking ────────────────────────────
                # flagged_history: publisher_name → {count, first_flagged, last_flagged}
                # Uses "last_flagged within 48h" window — resilient to Scout being
                # offline for a day (strict consecutive counter would reset on gap).
                flagged_history: dict = state.get("flagged_history", {})
                today_s = today_str
                from datetime import datetime as _fdt
                for v in signals.get("velocity_shifts", []):
                    if v.get("direction") != "down":
                        continue
                    pname = v["publisher_name"]
                    rec   = flagged_history.get(pname, {})
                    last  = rec.get("last_flagged", "")
                    # Within 48h = either yesterday or today (handles one offline day)
                    if last and (_fdt.strptime(today_s, "%Y-%m-%d") - _fdt.strptime(last, "%Y-%m-%d")).days <= 2:
                        rec["count"] = rec.get("count", 1) + 1
                    else:
                        rec = {"count": 1, "first_flagged": today_s}
                    rec["last_flagged"] = today_s
                    flagged_history[pname] = rec
                state["flagged_history"] = flagged_history

                # ── Chronic classification (P9-2) ─────────────────────────────────
                # Threshold: count >= 3 AND (last_flagged - first_flagged) <= 14 days
                # Eviction: today - last_flagged > 14 days → remove from chronic list
                chronic: list = state.get("chronic", [])
                today_dt = _fdt.strptime(today_s, "%Y-%m-%d")

                # Evict stale chronic partners
                evicted = [
                    pname for pname in chronic
                    if (today_dt - _fdt.strptime(
                        flagged_history.get(pname, {}).get("last_flagged") or "2000-01-01",
                        "%Y-%m-%d",
                    )).days > 14
                ]
                for pname in evicted:
                    chronic.remove(pname)
                    log.info(f"[pulse] {pname} evicted from chronic list (no flag in 14d)")

                # Promote new chronic partners
                for pname, rec in flagged_history.items():
                    if pname in chronic:
                        continue
                    count = rec.get("count", 0)
                    first = rec.get("first_flagged", "")
                    last  = rec.get("last_flagged", "")
                    if not first or not last:
                        continue
                    span_days = (_fdt.strptime(last, "%Y-%m-%d") - _fdt.strptime(first, "%Y-%m-%d")).days
                    if count >= 3 and span_days <= 14:
                        chronic.append(pname)
                        log.info(f"[pulse] {pname} classified as chronic ({count} flags in {span_days}d)")

                state["chronic"] = chronic

                # Only post if there's something to say
                has_content = (
                    signals.get("cap_alerts")
                    or signals.get("velocity_shifts")
                    or signals.get("overnight_events")
                    or signals.get("ghost_campaigns")
                    or signals.get("fill_rate")
                    or signals.get("opportunities")
                )
                channel = _route_channel("pulse")
                if has_content:
                    fallback, blocks = _format_pulse_blocks(
                        signals,
                        is_weekend=is_weekend,
                        flagged_history=flagged_history,
                        chronic=chronic,
                    )
                    web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)
                    log.info(f"[pulse] posted to {channel}: {len(signals['cap_alerts'])} caps, "
                             f"{len(signals['velocity_shifts'])} velocity, "
                             f"{len(signals['overnight_events'])} events, "
                             f"{len(signals['ghost_campaigns'])} ghosts, "
                             f"{len(signals['fill_rate'])} low-fill, "
                             f"{len(signals['opportunities'])} opportunities"
                             f"{' [weekend]' if is_weekend else ''}")
                else:
                    log.info("[pulse] no signals today — skipping post")

                # Record posted
                state["last_pulse_date"] = today_str
                _save_pulse_state(state)

            # SLEEP until next 8am
            target = now_chi.replace(hour=8, minute=0, second=0, microsecond=0)
            if now_chi >= target:
                target += timedelta(days=1)
            sleep_secs = (target - now_chi).total_seconds()
            log.info(f"[pulse] sleeping {sleep_secs / 3600:.1f}h until next pulse at {target}")
            time.sleep(sleep_secs)

        except Exception as e:
            log.error(f"[pulse] cycle failed: {e}", exc_info=True)
            time.sleep(3600)  # back off 1h on error




















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


def _performance_recap(web: WebClient) -> None:
    """
    14-day post-launch performance recap daemon.

    Runs daily. For every queued/live offer where:
      - approved_at is 14+ days ago
      - performance_recap_sent is False

    Pulls actual RPM from ClickHouse, compares to Scout's estimate at approval time,
    posts a 3-line recap in the original approval thread, marks recap as sent.

    This is the feedback loop that makes the model legible:
      Scout estimated $X → actual came in at $Y (+/-Z%)
    The ClickHouse benchmarks already self-improve as offers accumulate data.
    This thread post makes that improvement visible and builds team trust.
    """
    RECAP_DAYS = 14

    while True:
        try:
            time.sleep(86_400)
            from datetime import datetime, timezone
            from scout_agent import _get_ch_client
            state   = _load_launched_offers()
            now     = datetime.now(timezone.utc)
            updated = False

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
                import re as _re
                m = _re.search(r'/archives/([A-Z0-9]+)/p(\d+)', thread_url)
                if not m:
                    continue
                channel   = m.group(1)
                ts_raw    = m.group(2)
                thread_ts = f"{ts_raw[:10]}.{ts_raw[10:]}"

                # Pull actual impressions + revenue since approval date
                try:
                    ch = _get_ch_client()
                    q  = """
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
                    log.info(f"Recap skipped for {advertiser}: only {impressions} impressions at 14d")
                    continue

                actual_rpm = round(total_revenue / impressions * 1000, 0) if impressions else 0

                if estimated and actual_rpm:
                    delta_pct = round((actual_rpm - estimated) / estimated * 100)
                    direction = f"+{delta_pct}%" if delta_pct >= 0 else f"{delta_pct}%"
                    accuracy  = "on the money" if abs(delta_pct) <= 15 else ("above estimate" if delta_pct > 0 else "below estimate")
                    score_line = f"Scout estimated *${estimated:,.0f} RPM* → actual *${actual_rpm:,.0f} RPM* ({direction}, {accuracy})"
                elif actual_rpm:
                    score_line = f"Actual RPM at 14 days: *${actual_rpm:,.0f}* ({impressions:,} impressions)"
                else:
                    score_line = f"No conversions detected at 14 days ({impressions:,} impressions)"

                msg = (
                    f":bar_chart: *{advertiser}* — 14-day performance recap\n"
                    f"{score_line}\n"
                    f"_{payout} · {network} · {impressions:,} impressions_"
                )
                web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)

                # Also DM the approver — they won't be watching a 2-week-old thread
                approved_by = entry.get("approved_by", "")
                if approved_by:
                    try:
                        dm_ch = web.conversations_open(users=[approved_by])["channel"]["id"]
                        dm_body = (
                            f":bar_chart: *{advertiser}* — 14-day recap\n"
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
                state[advertiser]["actual_rpm_14d"]         = actual_rpm
                state[advertiser]["impressions_14d"]        = impressions
                updated = True
                log.info(f"14-day recap sent for {advertiser}: est=${estimated} actual=${actual_rpm}")

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


def _nightly_harvest():
    """Background daemon: harvest Slack channel context once per day at midnight CT."""
    import zoneinfo
    from context_harvester import harvest, is_stale

    ct = zoneinfo.ZoneInfo("America/Chicago")
    while True:
        try:
            from datetime import datetime as _dt
            now = _dt.now(ct)
            # Run at midnight CT — calculate seconds until next midnight
            from datetime import timedelta as _td
            tomorrow_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + _td(days=1)
            sleep_secs = (tomorrow_midnight - now).total_seconds()

            # On startup, if context is stale, harvest immediately
            if is_stale():
                log.info("[harvest] context stale or missing — running immediate harvest")
                result = harvest()
                _post_harvest_audit(result)
            else:
                log.info(f"[harvest] context is fresh — sleeping {sleep_secs / 3600:.1f}h until midnight CT")

            time.sleep(sleep_secs)
            # After sleep, harvest
            log.info("[harvest] midnight CT — running nightly harvest")
            result = harvest()
            _post_harvest_audit(result)
        except Exception as e:
            log.error(f"[harvest] cycle failed: {e}", exc_info=True)
            time.sleep(3600)  # retry in 1 hour on failure


def _post_harvest_audit(harvest_result: dict) -> None:
    """Post a brief audit summary to #scout-qa if the harvester learned any entity facts."""
    try:
        audit = harvest_result.get("audit", []) if isinstance(harvest_result, dict) else []
        if not audit:
            return  # nothing to report

        written = [e for e in audit if e.get("action") == "written"]
        skipped = [e for e in audit if e.get("action") == "skipped"]

        if not written and not skipped:
            return

        lines = [f":newspaper: *Scout learned overnight* ({len(written)} fact{'s' if len(written) != 1 else ''} added to entity knowledge)"]
        for e in written:
            icon = ":office:" if e.get("type") == "publisher" else ":chart_with_upwards_trend:"
            lines.append(f"{icon} *{e['name']}* ({e['type']}) — {e.get('note', '')[:80]}")
        for e in skipped:
            lines.append(f":grey_exclamation: *{e['name']}* — skipped: {e.get('reason', 'manual entry exists')}")
        lines.append("_To correct anything: `@Scout, actually [entity] does X` — I'll overwrite it._")

        web_client.chat_postMessage(
            channel=_SCOUT_HQ_CHANNEL,
            text="\n".join(lines),
        )
        log.info(f"[harvest] audit posted — {len(written)} written, {len(skipped)} skipped")
    except Exception as e:
        log.warning(f"[harvest] audit post failed (non-fatal): {e}")


def _run_scraper_daemon() -> None:
    """
    Offer scraper daemon — fetches affiliate inventory (Impact/FlexOffers/MaxBounty)
    once per day at 6:00 AM CT, then posts the Scout Signal digest.

    First-boot behaviour: if scraper_state.json doesn't exist, run immediately
    regardless of time of day. This ensures Render deployments (which can happen
    any time) don't leave offer inventory empty for hours.

    Check-first on subsequent starts: if past 6am and haven't run today, fire now.
    State: data/scraper_state.json
    """
    import pytz
    from datetime import datetime as _dt, timedelta
    from offer_scraper import run_headless as _run_scraper

    _SCRAPER_STATE = _DATA_DIR / "scraper_state.json"

    def _load_state():
        try:
            return json.loads(_SCRAPER_STATE.read_text())
        except Exception:
            return {}

    def _save_state(s):
        _atomic_write(_SCRAPER_STATE, s)

    while True:
        try:
            chicago = pytz.timezone("America/Chicago")
            now_chi = _dt.now(chicago)
            today_str = now_chi.strftime("%Y-%m-%d")
            state = _load_state()

            # FIRST BOOT: no state file → run immediately regardless of hour.
            # Prevents "offer inventory at 0" immediately after a Render deploy.
            is_first_boot = not _SCRAPER_STATE.exists() or not state

            # CHECK FIRST: past 6am and haven't run today → fire immediately.
            should_run = is_first_boot or (
                state.get("last_run_date") != today_str and now_chi.hour >= 6
            )

            if should_run:
                reason = "first boot" if is_first_boot else "daily run"
                log.info(f"[scraper] running offer fetch ({reason})")
                _SCRAPER_RUNNING.set()
                try:
                    _run_scraper()
                finally:
                    _SCRAPER_RUNNING.clear()
                state["last_run_date"] = today_str
                _save_state(state)
                log.info("[scraper] done — offers_latest.json updated")

            # Sleep until next 6am CT
            target = now_chi.replace(hour=6, minute=0, second=0, microsecond=0)
            if now_chi >= target:
                target += timedelta(days=1)
            sleep_secs = (target - now_chi).total_seconds()
            log.info(f"[scraper] sleeping {sleep_secs / 3600:.1f}h until next run at {target}")
            time.sleep(sleep_secs)

        except Exception as e:
            log.error(f"[scraper] cycle failed: {e}", exc_info=True)
            time.sleep(3600)  # retry in 1 hour on failure


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
    if os.getenv("RENDER"):
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
    from scout_agent import _load_entity_overrides, _save_entity_overrides
    import datetime as _dt
    overrides = _load_entity_overrides()
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
        _save_entity_overrides(overrides)
        log.info("[startup] seeded Button exclusion into data/entity_overrides.json")


def _run_startup_smoke_test(web: WebClient) -> None:
    """
    Run smoke tests on every startup and post results to #scout-qa.
    Non-blocking — runs in a background thread so it doesn't delay bot startup.
    Catches the class of bug that just burned us: bad model name, broken import,
    ClickHouse down, etc. — all invisible until someone @mentions Scout.
    """
    try:
        import smoke_test as _st
        results, pass_count = _st.run_tests(quiet=True)
        total = len(results)
        blocks, fallback = _st.format_slack_blocks(results, pass_count)
        web.chat_postMessage(channel=_SCOUT_HQ_CHANNEL, text=fallback, blocks=blocks, unfurl_links=False)
        log.info(f"[smoke] {pass_count}/{total} checks passed — posted to #scout-qa")
    except Exception as e:
        log.warning(f"[smoke] startup smoke test failed to run: {e}")
        try:
            web.chat_postMessage(
                channel=_SCOUT_HQ_CHANNEL,
                text=f":red_circle: *Scout startup smoke test crashed* — `{e}`\nCheck Render logs.",
            )
        except Exception:
            log.warning("Failed to post smoke test crash notification to Slack")


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
        if age_hours > 30:
            checks["offer_inventory"] = {"ok": False, "detail": f"Stale — {age_hours:.0f}h old"}
        else:
            checks["offer_inventory"] = {"ok": True, "detail": f"{age_hours:.0f}h old"}

    # 2. Required daemon threads alive
    live = {t.name for t in threading.enumerate()}
    required = {
        "scraper", "notion-watcher", "copy-coalescer",
        "context-harvest", "stale-queue-checker", "perf-recap", "state-cleanup",
    }
    if _PULSE_ENABLED:
        required.add("proactive-pulse")
    dead = required - live
    if dead:
        checks["daemon_threads"] = {"ok": False, "detail": f"Dead threads: {', '.join(sorted(dead))}"}
    else:
        checks["daemon_threads"] = {"ok": True, "detail": f"{len(required)} threads alive"}

    # 3. NOTION_QUEUE_DB_ID — required for correct Pipeline links in Slack messages
    queue_db_id = os.getenv("NOTION_QUEUE_DB_ID", "")
    if not queue_db_id:
        checks["notion_queue_url"] = {"ok": False, "detail": "NOTION_QUEUE_DB_ID not set — Pipeline links point to generic Notion homepage"}
    else:
        checks["notion_queue_url"] = {"ok": True, "detail": f"Pipeline DB configured ({queue_db_id[:8]}...)"}

    # 4. Environment
    for env_var in ("ANTHROPIC_API_KEY", "SLACK_BOT_TOKEN", "SLACK_APP_TOKEN"):
        val = os.getenv(env_var, "")
        checks[env_var] = {"ok": bool(val), "detail": "set" if val else "missing"}

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

def _thread_watchdog(web: WebClient) -> None:
    """Check all named daemon threads are alive every 60s. Alert #scout-qa if any die."""
    import time as _time

    # Core threads that must always run (smoke-test excluded — it's one-shot, not a daemon)
    REQUIRED = {
        "scraper", "notion-watcher", "copy-coalescer",
        "context-harvest", "stale-queue-checker", "perf-recap", "state-cleanup",
    }

    _time.sleep(120)  # Give all threads time to start before first check
    while True:
        try:
            live = {t.name for t in threading.enumerate()}
            # Conditional: proactive-pulse only if PULSE_ENABLED
            required = REQUIRED | ({"proactive-pulse"} if _PULSE_ENABLED else set())
            dead = required - live
            if dead:
                names = ", ".join(sorted(dead))
                log.error(f"[watchdog] daemon thread(s) died: {names}")
                try:
                    web.chat_postMessage(
                        channel=_SCOUT_HQ_CHANNEL,
                        text=f":warning: Scout daemon thread(s) died: *{names}*. Render may need a restart.",
                    )
                except Exception as slack_err:
                    log.warning(f"[watchdog] Slack alert failed: {slack_err}")
        except Exception as e:
            log.warning(f"[watchdog] check error: {e}")
        _time.sleep(60)


def main():
    global _BOT_USER_ID
    _check_singleton()
    _seed_entity_overrides()  # ensure Button exclusion survives fresh Render deploys
    if not BOT_TOKEN or not APP_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env")

    web_client    = WebClient(token=BOT_TOKEN, retry_handlers=[RateLimitErrorRetryHandler(max_retry_count=3)])
    _BOT_USER_ID  = web_client.auth_test()["user_id"]
    # Inject shared state into scout_handlers (avoids circular import — handlers don't import scout_bot)
    _set_bot_user_id(_BOT_USER_ID)
    _set_thread_state(_LAST_THREAD_PER_CHANNEL, _LAST_THREAD_LOCK)
    _set_pulse_runner(_run_pulse_once)
    socket_client = SocketModeClient(app_token=APP_TOKEN, web_client=web_client)
    socket_client.socket_mode_request_listeners.append(handle_event)

    # Startup: smoke test — runs immediately in background, posts pass/fail to #scout-qa.
    # Catches bad model names, broken imports, ClickHouse down, etc. before anyone @mentions Scout.
    threading.Thread(target=_run_startup_smoke_test, args=(web_client,), daemon=True, name="smoke-test").start()
    # Background: daily stale queue alerts (daemon thread dies cleanly on exit)
    threading.Thread(target=_check_stale_queue, args=(web_client,), daemon=True, name="stale-queue-checker").start()
    # Background: 14-day performance recap — compares Scout estimates to actual ClickHouse RPM
    threading.Thread(target=_performance_recap, args=(web_client,), daemon=True, name="perf-recap").start()
    # Background: nightly cleanup of state files to prevent unbounded growth
    threading.Thread(target=_cleanup_state, daemon=True, name="state-cleanup").start()
    # Background: daily proactive pulse — cap alerts, velocity shifts, overnight events
    # PULSE_ENABLED=false on local (LaunchAgent) to avoid double-posting when Render is also live
    if _PULSE_ENABLED:
        threading.Thread(target=_proactive_pulse, args=(web_client,), daemon=True, name="proactive-pulse").start()
    else:
        log.info("[pulse] disabled via PULSE_ENABLED=false — skipping pulse thread")
    # Background: daily launch health watchdog — catches broken campaigns within hours
    threading.Thread(target=_launch_watchdog, args=(web_client,), daemon=True, name="launch-watchdog").start()
    # Background: nightly channel context harvest — reads Slack channels, compresses to notes
    threading.Thread(target=_nightly_harvest, daemon=True, name="context-harvest").start()
    # Background: daily offer scraper — keeps offers_latest.json fresh (6am CT, or immediately on first boot)
    threading.Thread(target=_run_scraper_daemon, daemon=True, name="scraper").start()
    # Background: Notion → Slack watcher — posts when queue page Status changes from "Awaiting Entry"
    threading.Thread(target=_notion_watcher_loop, args=(web_client,), daemon=True, name="notion-watcher").start()
    # Background: AI copy coalescer — batches enrichment requests with 10s window + 24h cache
    threading.Thread(target=_copy_coalescer_loop, daemon=True, name="copy-coalescer").start()
    # Background: thread watchdog — alerts #scout-qa if any named daemon thread dies
    threading.Thread(target=_thread_watchdog, args=(web_client,), daemon=True, name="thread-watchdog").start()

    # Health check HTTP server — Render pings /health every 30s to verify Scout is alive
    _start_health_server(port=int(os.getenv("PORT", "10000")))

    log.info("Scout is online — listening for @mentions via Socket Mode")
    socket_client.connect()

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
