from __future__ import annotations

import json
import logging
import os
import pathlib
from datetime import datetime, timezone

from scout_ch import _get_ch_client, _run_parallel
from scout_thresholds import _manager
import queries as _q
from scout_ch import (
    _query_ghost_campaigns,
    _query_expiring_campaigns,
)

log = logging.getLogger("scout_agent")

_LAUNCHED_OFFERS_PATH = pathlib.Path(__file__).parent / "data" / "launched_offers.json"


def get_queue_status() -> dict:
    """
    Fetch the offer pipeline queue from Notion and return a Block Kit card.
    Shared render path with App Home tab and /scout-queue — same data, same view.
    Returns a dict with 'blocks' (list) and 'text' (fallback string).
    """
    from scout_notion import _fetch_notion_queue_items
    from scout_ui_kit import _build_queue_card

    items = _fetch_notion_queue_items()
    blocks = _build_queue_card(items)
    if items is None:
        text = "Could not reach Notion — queue data unavailable."
    elif not items:
        text = "Queue is clear — nothing awaiting entry or in platform."
    else:
        text = f"Offer pipeline: {len(items)} offer{'s' if len(items) != 1 else ''} in queue."
    return {"finding": text, "blocks": blocks, "text": text}


def get_demand_queue_status() -> dict:
    """
    Read the MS Demand Queue state from launched_offers.json.
    Cross-references ClickHouse for impressions since each offer's approved_at date —
    if impressions > 0 the offer is likely live. Returns pending items with status.
    """
    state = _manager.load_launched_offers_state()
    pending = [
        {**{"advertiser": k}, **v}
        for k, v in state.items()
        if v.get("status") == "queued"
    ]

    if not pending:
        return {"finding": "0 queued", "pending": [], "count": 0}

    # Batch ClickHouse check: impressions per advertiser since approved_at
    impression_counts: dict = {}
    try:
        ch = _get_ch_client()
        # Find campaign IDs matching each advertiser name, then count impressions
        # since that offer's approved_at date
        for item in pending:
            adv = item["advertiser"]
            approved_at = item.get("approved_at", "2000-01-01T00:00:00")
            try:
                rows = ch.query(
                    """
                    SELECT count() AS imp_count
                    FROM default.adpx_impressions_details imp
                    JOIN default.mv_adpx_campaigns c ON toInt64(imp.campaign_id) = toInt64(c.id)
                    WHERE c.adv_name ILIKE %(adv)s
                      AND imp.created_at >= parseDateTimeBestEffort(%(approved_at)s)
                      AND toYYYYMM(imp.created_at) >= toYYYYMM(parseDateTimeBestEffort(%(approved_at)s))
                    """,
                    parameters={"adv": f"%{adv}%", "approved_at": approved_at},
                ).result_rows
                impression_counts[adv] = rows[0][0] if rows else 0
            except Exception as e:
                log.debug(f"CH impression check failed for {adv}: {e}")
                impression_counts[adv] = 0
    except Exception as e:
        log.warning(f"get_demand_queue_status: ClickHouse unavailable: {e}")

    from datetime import datetime, timezone as _tz

    result_items = []
    for item in pending:
        adv = item["advertiser"]
        imp = impression_counts.get(adv, 0)
        approved_at_str = item.get("approved_at", "")
        days_queued = 0
        if approved_at_str:
            try:
                approved_dt = datetime.fromisoformat(approved_at_str.replace("Z", "+00:00"))
                days_queued = (datetime.now(_tz.utc) - approved_dt).days
            except Exception as e:
                log.debug("get_demand_queue_status approved_at parse swallowed: %s", e)
                days_queued = 0

        result_items.append({
            "advertiser":  adv,
            "payout":      item.get("payout", ""),
            "network":     item.get("network", ""),
            "brief_url":   item.get("thread_url", ""),
            "notion_url":  item.get("notion_url", ""),
            "approved_by": item.get("approved_by", ""),
            "approved_at": approved_at_str,
            "days_queued": days_queued,
            "status":      "likely_live" if imp > 0 else "pending",
            "impressions_since_approval": imp,
        })

    return {"finding": f"{len(result_items)} queued", "pending": result_items, "count": len(result_items)}


def mark_offer_launched(advertiser: str) -> dict:
    """
    Mark an approved offer as live. Updates launched_offers.json status to 'launched'.
    scout_bot.py reads the result and sends a targeted notification to the approver + AdOps.
    """
    state = _manager.load_launched_offers_state()

    # Fuzzy match — handles "TurboTax" matching "TurboTax 2025" etc.
    key = next(
        (k for k in state if advertiser.lower() in k.lower() or k.lower() in advertiser.lower()),
        advertiser,
    )

    entry = state.get(key, {})
    entry.update({"status": "launched", "launched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")})
    state[key] = entry

    try:
        _LAUNCHED_OFFERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _LAUNCHED_OFFERS_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2))
        os.replace(tmp, _LAUNCHED_OFFERS_PATH)
    except Exception as e:
        log.warning(f"mark_offer_launched write failed: {e}")

    return {
        "finding":     f"{key} launched",
        "status":      "launched",
        "advertiser":  key,
        "approved_by": entry.get("approved_by"),
        "thread_url":  entry.get("thread_url"),
        "notion_url":  entry.get("notion_url", ""),
        "payout":      entry.get("payout"),
        "network":     entry.get("network"),
    }


def get_campaign_status(advertiser_name: str) -> dict:
    """
    Check if an advertiser's campaigns are active/paused and show recent changes from audit log.
    """
    try:
        import json as _json
        ch = _get_ch_client()

        # ── Queries 1 + 2 run in parallel — both depend only on advertiser_name ─
        # Query 1: current status from publisher_campaigns
        # Query 2: recent audit log entries
        def _fetch_q1():
            return ch.query(
                """
                SELECT
                    pc.id, pc.campaign_id, pc.is_active,
                    c.adv_name,
                    pc.updated_at, pc.deleted_at
                FROM from_airbyte_publisher_campaigns pc
                JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
                WHERE c.adv_name ILIKE {adv: String}
                  AND c.deleted_at IS NULL
                ORDER BY pc.updated_at DESC
                LIMIT 50
                """,
                parameters={"adv": f"%{advertiser_name}%"},
            ).result_rows

        def _fetch_q2():
            return ch.query(
                """
                SELECT
                    entity, type, old_data, new_data, created_at, user_type, user_role
                FROM adpx_system_activity_logs
                WHERE (lower(new_data) LIKE lower({adv_pat: String}) OR lower(old_data) LIKE lower({adv_pat: String}))
                  AND created_at >= today() - 30
                ORDER BY created_at DESC
                LIMIT 20
                """,
                parameters={"adv_pat": f"%{advertiser_name}%"},
            ).result_rows

        q1_rows = []
        q2_rows = []
        try:
            q1_rows, q2_rows = _run_parallel([_fetch_q1, _fetch_q2])
        except Exception as e:
            log.warning(f"get_campaign_status parallel fetch failed: {e}")
            try:
                q1_rows = _fetch_q1()
            except Exception as e2:
                log.warning(f"get_campaign_status q1 failed: {e2}")
            try:
                q2_rows = _fetch_q2()
            except Exception as e2:
                log.warning(f"get_campaign_status q2 failed: {e2}")

        # ── Build campaigns list ──────────────────────────────────────────────
        campaigns = []
        for row in q1_rows:
            pc_id, camp_id, is_active, adv_name, updated_at, deleted_at = row
            campaigns.append({
                "publisher_campaign_id": int(pc_id) if pc_id else None,
                "campaign_id":           int(camp_id) if camp_id else None,
                "adv_name":              adv_name or advertiser_name,
                "is_active":             bool(is_active),
                "last_updated":          str(updated_at) if updated_at else None,
                "is_deleted":            deleted_at is not None,
            })

        active_count = sum(1 for c in campaigns if c["is_active"] and not c["is_deleted"])
        paused_count = sum(1 for c in campaigns if not c["is_active"] and not c["is_deleted"])

        # ── Parse audit log ───────────────────────────────────────────────────
        recent_changes = []
        for row in q2_rows:
            entity, change_type_raw, old_data_str, new_data_str, created_at, user_type, user_role = row
            try:
                old_data = _json.loads(old_data_str) if old_data_str else {}
            except Exception as e:
                log.debug("get_campaign_status old_data parse swallowed: %s", e)
                old_data = {}
            try:
                new_data = _json.loads(new_data_str) if new_data_str else {}
            except Exception as e:
                log.debug("get_campaign_status new_data parse swallowed: %s", e)
                new_data = {}

            # Determine change type
            old_active = old_data.get("is_active")
            new_active = new_data.get("is_active")
            if old_active is True and new_active is False:
                c_type = "paused"
                summary = f"Campaign paused by {user_role or user_type or 'system'}"
            elif old_active is False and new_active is True:
                c_type = "resumed"
                summary = f"Campaign resumed by {user_role or user_type or 'system'}"
            elif "capping_config" in new_data or "capping_config" in old_data:
                c_type = "budget_changed"
                summary = "Budget/cap configuration changed"
            else:
                c_type = "other"
                summary = f"{change_type_raw or 'Change'} by {user_role or user_type or 'system'}"

            recent_changes.append({
                "entity":      entity or "",
                "change_type": c_type,
                "timestamp":   str(created_at) if created_at else None,
                "summary":     summary,
            })

        # ── Status summary ────────────────────────────────────────────────────
        last_change_str = ""
        if recent_changes:
            last = recent_changes[0]
            last_change_str = f" Last change: {last['change_type']} ({last['timestamp'][:10] if last['timestamp'] else 'unknown'})."
        status_summary = f"{active_count} active, {paused_count} paused.{last_change_str}"

        return {
            "finding":        status_summary,
            "advertiser":     advertiser_name,
            "campaign_count": len(campaigns),
            "active_count":   active_count,
            "paused_count":   paused_count,
            "campaigns":      campaigns,
            "recent_changes": recent_changes,
            "status_summary": status_summary,
        }
    except Exception as e:
        log.exception("get_campaign_status failed")
        return {"error": str(e), "advertiser": advertiser_name}


def get_ghost_campaigns() -> str:
    """
    Return full ghost campaign list with per-campaign diagnosis.
    Ghost = actively serving impressions + clicks but generating near-zero revenue
    (< $5 in last 7 days), older than 7 days (not a new launch).

    NOTE: SQL lives in _query_ghost_campaigns(). Any threshold or filter change belongs there.
    This function is a formatting wrapper only.
    """
    ch = _get_ch_client()
    try:
        rows = _query_ghost_campaigns(ch)
    except Exception as e:
        return f"Ghost campaign query failed: {e}"

    if not rows:
        return (
            "*Ghost Campaign Report*\n\n"
            ":white_check_mark: No ghost campaigns detected — all active campaigns with "
            "high engagement are generating revenue."
        )

    lines = [f"*Ghost Campaign Report — Full List* ({len(rows)} campaigns)\n"]
    for r in rows:
        adv_name    = r["adv_name"]
        campaign_id = r["campaign_id"]
        imps        = r["impressions_7d"]
        imps_2d     = r["impressions_2d"]
        clicks      = r["clicks_7d"]
        rev         = r["revenue_7d"]
        first_date  = r["first_impression_date"]
        pub_ids     = r["publisher_ids"]
        pub_names   = r["publisher_names"]

        imp_str    = f"{imps / 1000:.0f}K" if imps >= 1000 else str(imps)
        imp_2d_str = f"{imps_2d / 1000:.1f}K" if imps_2d >= 1000 else str(imps_2d)
        rev_str    = f"${rev:.2f}" if rev > 0 else "$0"

        # Publisher context — deduplicate and format as "Name (#ID)"
        seen, pub_parts = set(), []
        for pid, pname in zip(pub_ids, pub_names):
            if pid not in seen and pname:
                seen.add(pid)
                pub_parts.append(f"{pname} (#{pid})")
        pub_str = ", ".join(pub_parts[:3]) if pub_parts else "unknown publisher"

        if clicks > 0 and rev == 0:
            hypothesis = "Zero conversions in 7 days — postback not firing post-click. Check postback URL config."
        else:
            hypothesis = "Clicks converting but revenue not flowing — check campaign payout config."

        lines.append(
            f"• *{adv_name}* · Campaign #{campaign_id} · {pub_str}\n"
            f"  {imp_str} impressions (7d) · {imp_2d_str} in last 48h · {clicks:,} clicks · {rev_str} · since {first_date}\n"
            f"  ↳ _{hypothesis}_"
        )

    lines.append(
        "\n:zap: Start with the highest-impression campaigns — they're burning the most inventory. "
        "Pull the postback URL for each campaign from the network dashboard and confirm pixel fires."
    )
    return "\n".join(lines)


def get_expiring_campaigns(warning_days: int = None) -> dict:
    """
    Find active campaigns expiring within the next N days.
    Default window comes from scout_thresholds.json; caller can override.
    """
    try:
        ch = _get_ch_client()
        t = _manager.load().get("signals", {})
        window = int(warning_days if warning_days is not None else t.get("expiration_warning_days", 7))
        rows = _query_expiring_campaigns(ch, warning_days=window)
        if not rows:
            return {"campaigns": [], "count": 0, "summary": f"No active campaigns expiring in the next {window} days."}
        return {
            "campaigns": rows,
            "count": len(rows),
            "window_days": window,
            "summary": f"{len(rows)} active campaign(s) expiring within {window} days.",
        }
    except Exception as e:
        log.exception("get_expiring_campaigns failed")
        return {"error": str(e), "campaigns": []}
