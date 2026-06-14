"""ClickHouse queries — publisher health, lookups, and per-publisher aggregations."""

from __future__ import annotations

import logging
from typing import Optional

from queries_revenue import _ch_date_filter, _ct_days_ago, _revenue_trend_sql, _trend

log = logging.getLogger(__name__)


# ===========================================================================
# Post-transaction placement names (canonical list — single source of truth)
# Used by low_fill_publishers() to filter sessions to monetizable placements.
# Kept here so callers don't need to import from scout_agent.py.
# ===========================================================================
POST_TX_PLACEMENTS: tuple[str, ...] = (
    "checkout_confirmation_page", "order_confirmation", "order-confirmation",
    "buy_flow_thank_you", "buyflowthankyou", "acctmgmt_payment_confirmation",
    "acctmgmtpaymentconfirmation", "receipt", "visit-receipt", "visit_receipt",
    "parking_pass_receipt", "order-receipt", "receipt-parkingdotcom",
    "post_checkout_receipt", "post_transaction", "post_transaction_page",
    "metropolis_transaction_details", "7eleven-fuel-transactionreceipt-bottom",
    "7Eleven_Fuel_TransactionReceipt_Bottom", "conv-orderconfirmation",
    "thank_you", "message_confirmation", "registration_complete",
    "order_status_offers",
)


# ===========================================================================
# Publisher resolution
# ===========================================================================

def publisher_lookup_by_name(ch, name: str) -> list[dict]:
    """
    Find publishers whose organization or username contains `name` (case-insensitive).
    Returns only publishers with a non-null, non-empty sdk_id (i.e. active accounts).

    Returns: list of dicts with keys: id, organization, sdk_id
    """
    rows = ch.query(
        """
        SELECT id, organization, sdk_id
        FROM default.from_airbyte_users
        WHERE (lower(organization) LIKE lower(concat('%', {name:String}, '%'))
            OR lower(username) LIKE lower(concat('%', {name:String}, '%')))
          AND deletedAt IS NULL
          AND sdk_id IS NOT NULL
          AND sdk_id != ''
        ORDER BY createdAt ASC
        LIMIT 10
        """,
        parameters={"name": name},
    ).result_rows
    return [{"id": r[0], "organization": r[1], "sdk_id": r[2]} for r in rows]


def publisher_lookup_by_id(ch, pub_id: int) -> Optional[dict]:
    """
    Find a single publisher by numeric ID.

    Returns: dict with keys id, organization, sdk_id — or None if not found.
    """
    rows = ch.query(
        """
        SELECT id, organization, sdk_id
        FROM default.from_airbyte_users
        WHERE id = {pub_id: Int64}
          AND deletedAt IS NULL
        LIMIT 1
        """,
        parameters={"pub_id": pub_id},
    ).result_rows
    if not rows:
        return None
    return {"id": rows[0][0], "organization": rows[0][1], "sdk_id": rows[0][2]}


def publisher_impression_volume(ch, pid_list: list[str], days: int = 7) -> dict[str, int]:
    """
    Return impression count per publisher pid (as string) over the last `days` days.
    Used to disambiguate between multiple publishers matching the same name
    by picking the one with the most recent traffic.

    Returns: dict mapping pid_str -> impression_count (0 if absent from result)
    """
    if not pid_list:
        return {}
    # Build parameterized IN list — pid values are validated numeric strings from
    # publisher_lookup_by_name, so no injection risk. ClickHouse IN({list}) with
    # parameters requires Array type; use multiValue approach instead.
    rows = ch.query(
        """
        SELECT pid, count() AS impressions
        FROM adpx_impressions_details
        PREWHERE pid IN {pids: Array(String)}
            AND toYYYYMM(created_at) >= toYYYYMM(today() - {days: UInt32})
        WHERE created_at >= today() - {days: UInt32}
        GROUP BY pid
        """,
        parameters={"pids": pid_list, "days": days},
    ).result_rows
    result = {p: 0 for p in pid_list}
    for pid, cnt in rows:
        result[str(pid)] = int(cnt)
    return result


def publisher_recent_sessions(ch, candidate_ids: list[int], days: int = 7) -> dict[int, int]:
    """
    Return session count per publisher user_id over the last `days` days.
    Used alongside publisher_impression_volume to pick the most active account
    when multiple accounts share a publisher name.

    Returns: dict mapping user_id (int) -> session_count
    """
    if not candidate_ids:
        return {}
    rows = ch.query(
        """
        SELECT user_id, count() AS sessions
        FROM adpx_sdk_sessions
        PREWHERE user_id IN {ids: Array(Int64)}
            AND toYYYYMM(created_at) >= toYYYYMM(today() - {days: UInt32})
        WHERE created_at >= today() - {days: UInt32}
        GROUP BY user_id
        ORDER BY sessions DESC
        LIMIT 10
        """,
        parameters={"ids": [int(i) for i in candidate_ids], "days": days},
    ).result_rows
    return {int(r[0]): int(r[1]) for r in rows}


def publisher_name_by_id(ch, pub_id: int) -> Optional[str]:
    """
    Return the organization name for a publisher, or None if not found.
    Lightweight fallback for callers that have a numeric ID but need the display name.
    """
    rows = ch.query(
        "SELECT organization FROM from_airbyte_users WHERE id = {pid: UInt64} LIMIT 1",
        parameters={"pid": int(pub_id)},
    ).result_rows
    return rows[0][0] if rows else None


# ===========================================================================
# Publisher competitive landscape (was: get_publisher_competitive_landscape)
# These 4 functions replace the 5 f-string SQL injections in the original.
# ===========================================================================

def publisher_weekly_impressions(ch, pub_pid: str, days: int = 28) -> list[dict]:
    """
    Weekly impression breakdown for a publisher over the last `days` days.
    Used to compute the 4-week average impression volume for rank projections.

    pub_pid: numeric publisher ID as a string (the 'pid' column in impressions table).

    Returns: list of dicts with keys: week (date), impressions (int)
    """
    rows = ch.query(
        """
        SELECT
            toStartOfWeek(i.created_at) AS week,
            count() AS impressions
        FROM default.adpx_impressions_details i
        PREWHERE i.pid = {pub_pid: String}
        WHERE i.created_at >= today() - {days: UInt32}
        GROUP BY week
        ORDER BY week DESC
        """,
        parameters={"pub_pid": pub_pid, "days": days},
    ).result_rows
    return [{"week": r[0], "impressions": int(r[1])} for r in rows]


def publisher_provisioned_campaigns(ch, pub_id: int) -> list[dict]:
    """
    Active campaigns currently provisioned (assigned) to this publisher.
    Source of truth for "what's set up" — impressions show what's actually serving.

    Returns: list of dicts with keys: campaign_id (str), adv_name, payout (float | None)
    """
    rows = ch.query(
        """
        SELECT
            pc.campaign_id,
            c.adv_name,
            pc.payout
        FROM default.from_airbyte_publisher_campaigns pc
        JOIN default.from_airbyte_campaigns c ON toInt64(pc.campaign_id) = toInt64(c.id)
        WHERE pc.user_id = {pub_id: Int64}
          AND pc.deleted_at IS NULL
          AND pc.is_active = true
          AND c.deleted_at IS NULL
        ORDER BY pc.created_at DESC
        LIMIT 50
        """,
        parameters={"pub_id": pub_id},
    ).result_rows
    return [
        {
            "campaign_id": str(r[0]),
            "adv_name":    r[1],
            "payout":      float(r[2]) if r[2] is not None else None,
        }
        for r in rows
    ]


def publisher_serving_campaign_impressions(
    ch, pub_pid: str, campaign_ids: list[str], days: int = 14
) -> dict[str, int]:
    """
    Impression count per campaign on this publisher over the last `days` days.
    Only campaigns in `campaign_ids` are included (the provisioned set).
    Used to determine which provisioned campaigns are actively serving.

    pub_pid: numeric publisher ID as string (the 'pid' column in impressions table).

    Returns: dict mapping campaign_id_str -> impression_count
    """
    if not campaign_ids:
        return {}
    rows = ch.query(
        """
        SELECT campaign_id, count() AS impressions
        FROM default.adpx_impressions_details
        PREWHERE pid = {pub_pid: String}
        WHERE created_at >= today() - {days: UInt32}
          AND campaign_id IN {cids: Array(String)}
        GROUP BY campaign_id
        """,
        parameters={"pub_pid": pub_pid, "cids": campaign_ids, "days": days},
    ).result_rows
    return {str(r[0]): int(r[1]) for r in rows}


def publisher_campaign_rpms(
    ch, pub_user_id: str, campaign_ids: list[str], days: int = 14
) -> dict[str, float]:
    """
    RPM (revenue per 1,000 impressions) per campaign on this publisher.
    Only campaigns in `campaign_ids` that are actively serving are included.

    pub_user_id: numeric publisher ID as string (the traffic/attribution publisher).

    IMPORTANT — two different ID columns, two different tables:
    - adpx_impressions_details uses `pid` (offer-owner publisher). Correct for impression counts.
    - adpx_conversionsdetails uses `user_id` (traffic publisher). MUST use user_id here —
      filtering on `pid` in conversionsdetails returns offer-owner attribution, which for
      publishers where pid ≠ user_id gives the wrong partner's revenue (e.g. pid=338 returns
      $74K gross but 0 sessions; correct attribution is user_id=953).

    Returns: dict mapping campaign_id_str -> rpm (float, 0.0 if no revenue)
    """
    if not campaign_ids:
        return {}
    rows = ch.query(
        """
        SELECT
            imp.campaign_id,
            round(coalesce(cv.total_revenue, 0) / nullIf(imp.impressions, 0) * 1000, 2) AS rpm
        FROM (
            SELECT campaign_id, count() AS impressions
            FROM default.adpx_impressions_details
            PREWHERE pid = {pub_pid: String}
            WHERE created_at >= today() - {days: UInt32}
              AND campaign_id IN {cids: Array(String)}
            GROUP BY campaign_id
        ) imp
        LEFT JOIN (
            -- Filter on user_id (traffic attribution), NOT pid (offer-owner).
            -- See docstring above for the pid vs user_id attribution distinction.
            SELECT cv.campaign_id,
                   sum(toFloat64OrNull(cv.revenue)) AS total_revenue
            FROM default.adpx_conversionsdetails cv
            PREWHERE cv.user_id = {pub_user_id: String}
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - {extended_days: UInt32})
              AND cv.created_at >= today() - {extended_days: UInt32}
              AND cv.campaign_id IN {cids: Array(String)}
            GROUP BY cv.campaign_id
        ) cv ON toInt64(imp.campaign_id) = toInt64(cv.campaign_id)
        """,
        parameters={
            "pub_pid":       pub_user_id,   # impressions table: pid column (offer-owner, correct here)
            "pub_user_id":   pub_user_id,   # conversions table: user_id column (traffic attribution)
            "cids":          campaign_ids,
            "days":          days,
            "extended_days": days * 2,
        },
    ).result_rows
    return {str(r[0]): float(r[1] or 0) for r in rows}


# ===========================================================================
# Supply/demand gaps (was: get_supply_demand_gaps)
# ===========================================================================

def supply_gap_opportunities(ch, pub_id: int) -> list[dict]:
    """
    Advertisers earning well on 2+ other publishers but NOT provisioned on this publisher.
    Used for publisher-first supply gap analysis (revenue opportunities to pitch).

    Thresholds: revenue_30d > 0, active on 2+ publishers (excludes single-publisher offers).
    Returns top 20 by revenue.

    Returns: list of dicts with keys:
        adv_name, pub_count (int), impressions_30d (int), revenue_30d (float), rpm (float)
    """
    rows = ch.query(
        f"""
        WITH imp_agg AS (
            SELECT campaign_id, count() AS impressions_30d
            FROM adpx_impressions_details
            WHERE created_at >= {_ch_date_filter(30)}
              AND toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(30)})
            GROUP BY campaign_id
        ),
        conv_agg AS (
            SELECT campaign_id, sum(toFloat64OrNull(revenue)) AS revenue_30d
            FROM adpx_conversionsdetails
            WHERE created_at >= {_ch_date_filter(30)}
              AND toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(30)})
            GROUP BY campaign_id
        )
        SELECT
            c.adv_name,
            count(DISTINCT pc.user_id) AS pub_count,
            sum(ia.impressions_30d) AS impressions_30d,
            coalesce(sum(ca.revenue_30d), 0) AS revenue_30d,
            round(coalesce(sum(ca.revenue_30d), 0) /
                  nullIf(sum(ia.impressions_30d), 0) * 1000, 2) AS rpm
        FROM from_airbyte_publisher_campaigns pc
        JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
        LEFT JOIN imp_agg ia ON ia.campaign_id = toUInt64(pc.campaign_id)
        LEFT JOIN conv_agg ca ON ca.campaign_id = toUInt64(pc.campaign_id)
        WHERE pc.is_active = true
          AND pc.deleted_at IS NULL AND c.deleted_at IS NULL
          AND pc.user_id != {{pub_id: Int64}}
        GROUP BY c.adv_name
        HAVING revenue_30d > 0 AND pub_count >= 2
        ORDER BY revenue_30d DESC
        LIMIT 20
        """,
        parameters={"pub_id": pub_id},
    ).result_rows
    return [
        {
            "adv_name":        r[0],
            "pub_count":       int(r[1]),
            "impressions_30d": int(r[2] or 0),
            "revenue_30d":     round(float(r[3] or 0), 2),
            "rpm":             round(float(r[4] or 0), 2),
        }
        for r in rows
    ]


def supply_dead_weight(ch, pub_id: int, pub_pid: str) -> list[dict]:
    """
    Campaigns provisioned on this publisher but serving zero impressions in 30 days.
    These are "dead weight" — setup cost was paid, zero monetization return.

    pub_pid: numeric publisher ID as string (for the impressions table pid column).

    Returns: list of dicts with keys: adv_name, provisioned_since (date | str)
    """
    rows = ch.query(
        f"""
        SELECT c.adv_name, min(pc.created_at) AS provisioned_since
        FROM from_airbyte_publisher_campaigns pc
        JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
        LEFT JOIN adpx_impressions_details i
            ON i.campaign_id = toUInt64(pc.campaign_id)
            AND i.pid = {{pub_pid: String}}
            AND i.created_at >= {_ch_date_filter(30)}
        WHERE pc.user_id = {{pub_id: Int64}}
          AND pc.is_active = true
          AND pc.deleted_at IS NULL AND c.deleted_at IS NULL
          AND c.status = 'Active' AND c.end_date >= today()
        GROUP BY c.adv_name
        HAVING count(i.campaign_id) = 0
        ORDER BY provisioned_since ASC
        LIMIT 10
        """,
        parameters={"pub_id": pub_id, "pub_pid": pub_pid},
    ).result_rows
    return [{"adv_name": r[0], "provisioned_since": r[1]} for r in rows]


def publisher_sessions_30d(ch, pub_id: int) -> int:
    """
    Total SDK sessions for this publisher in the last 30 days.
    Used to compute daily session rate for revenue projection.

    Returns: session count (int)
    """
    rows = ch.query(
        f"""
        SELECT count() AS sessions
        FROM adpx_sdk_sessions
        WHERE user_id = {{pub_id: Int64}}
          AND created_at >= {_ch_date_filter(30)}
          AND toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(30)})
        """,
        parameters={"pub_id": pub_id},
    ).result_rows
    return int(rows[0][0]) if rows else 0


def publisher_existing_advertisers(ch, pub_id: int) -> set[str]:
    """
    Set of lowercase advertiser names already provisioned (active) on this publisher.
    Used for fuzzy-match deduplication when surfacing gap opportunities.

    Returns: set of lowercase adv_name strings
    """
    rows = ch.query(
        """
        SELECT DISTINCT c.adv_name
        FROM from_airbyte_publisher_campaigns pc
        JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
        WHERE pc.user_id = {pub_id: Int64}
          AND pc.is_active = true AND pc.deleted_at IS NULL AND c.deleted_at IS NULL
        """,
        parameters={"pub_id": pub_id},
    ).result_rows
    return {r[0].lower() for r in rows if r[0]}


def advertiser_active_publishers(ch, advertiser_name: str) -> list[dict]:
    """
    Publishers where an advertiser is currently active (provisioned + not deleted).
    Used for advertiser-first gap analysis ("where is X NOT running?").

    Returns: list of dicts with keys: publisher_id (int), organization (str)
    """
    rows = ch.query(
        """
        SELECT DISTINCT pc.user_id, u.organization
        FROM from_airbyte_publisher_campaigns pc
        JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
        JOIN from_airbyte_users u ON pc.user_id = u.id
        WHERE c.adv_name ILIKE {adv: String}
          AND pc.is_active = true AND pc.deleted_at IS NULL AND c.deleted_at IS NULL
        """,
        parameters={"adv": f"%{advertiser_name}%"},
    ).result_rows
    return [{"publisher_id": int(r[0]), "organization": r[1]} for r in rows]


def publishers_missing_advertiser(ch, active_pub_ids: list[int]) -> list[dict]:
    """
    Publishers with >1,000 sessions/30d that are NOT in `active_pub_ids`.
    Used for advertiser-first gap analysis to show where an advertiser is not running.

    Returns: list of dicts with keys: publisher_id (int), organization (str), sessions_30d (int)
    """
    rows = ch.query(
        f"""
        -- mv_adpx_users is a lightweight MV (id, organization, is_test, parent_id only)
        -- — prefer over from_airbyte_users for simple name lookups.
        SELECT s.user_id, coalesce(u.organization, '') AS organization, count() AS sessions_30d
        FROM adpx_sdk_sessions s
        LEFT JOIN mv_adpx_users u ON s.user_id = u.id
        WHERE s.created_at >= {_ch_date_filter(30)}
          AND toYYYYMM(s.created_at) >= toYYYYMM({_ch_date_filter(30)})
          AND s.user_id NOT IN {{active_ids: Array(Int64)}}
        GROUP BY s.user_id, coalesce(u.organization, '')
        HAVING sessions_30d > 1000
        ORDER BY sessions_30d DESC
        LIMIT 20
        """,
        parameters={"active_ids": [int(i) for i in active_pub_ids]},
    ).result_rows
    return [
        {
            "publisher_id":  int(r[0]),
            "organization":  r[1],
            "sessions_30d":  int(r[2]),
        }
        for r in rows
    ]


# ===========================================================================
# Publisher health (was inline in get_publisher_health)
# ===========================================================================

def publisher_health_sessions(
    ch, pid: int, partition: int, days: int, geo_state: Optional[str] = None
) -> list[dict]:
    """
    Session counts by placement and OS for a publisher, over the last `days` days.
    Optionally filtered to a US state (geo_state).

    partition: toYYYYMM(today() - days) — passed from caller to avoid recomputing.

    Returns: list of dicts with keys: placement, os, sessions (int)
    """
    sql = """
    SELECT placement, os, count() AS sessions
    FROM adpx_sdk_sessions
    PREWHERE user_id = {pid: UInt64}
        AND toYYYYMM(created_at) >= {partition: UInt32}
    WHERE created_at >= today() - {days: UInt32}
    """
    params: dict = {"pid": pid, "partition": partition, "days": days}
    if geo_state:
        sql += "  AND state ILIKE {geo_state: String}\n"
        params["geo_state"] = f"%{geo_state}%"
    sql += "GROUP BY placement, os\nORDER BY sessions DESC"
    rows = ch.query(sql, parameters=params).result_rows
    return [{"placement": r[0], "os": r[1], "sessions": int(r[2])} for r in rows]


def publisher_health_ad_metrics(
    ch,
    pid: int,
    pid_str: str,
    partition: int,
    extended_partition: int,
    days: int,
    geo_state: Optional[str] = None,
) -> list[dict]:
    """
    Impressions, conversions, revenue, and payout by placement for a publisher.
    Joins impressions (left) to sessions and conversions.

    pid_str: pid as string (impressions table uses string pid, sessions table uses int user_id).
    extended_partition: partition - 1 month, for conversion downstream lag window.

    Returns: list of dicts with keys:
        placement, impressions (int), conversions (int), revenue (float), payout (float)
    """
    sql = """
    SELECT
        s.placement,
        count(DISTINCT i.id)                                    AS impressions,
        count(DISTINCT cd.id)                                   AS conversions,
        coalesce(sum(toFloat64OrNull(cd.revenue)), 0)           AS revenue,
        coalesce(sum(toFloat64OrNull(cd.payout)), 0)            AS payout
    FROM (
        SELECT session_id, id, campaign_id
        FROM adpx_impressions_details
        PREWHERE pid = {pid_str: String}
            AND toYYYYMM(created_at) >= {partition: UInt32}
        WHERE created_at >= today() - {days: UInt32}
    ) i
    JOIN (
        SELECT session_id, placement
        FROM adpx_sdk_sessions
        PREWHERE user_id = {pid: UInt64}
            AND toYYYYMM(created_at) >= {partition: UInt32}
        WHERE created_at >= today() - {days: UInt32}
    """
    params: dict = {
        "pid":                pid,
        "pid_str":            pid_str,
        "partition":          partition,
        "extended_partition": extended_partition,
        "days":               days,
    }
    if geo_state:
        sql += "      AND state ILIKE {geo_state: String}\n"
        params["geo_state"] = f"%{geo_state}%"
    sql += """
    ) s ON s.session_id = i.session_id
    LEFT JOIN (
        SELECT session_id, id, campaign_id, revenue, payout
        FROM adpx_conversionsdetails
        PREWHERE user_id = {pid: UInt64}
            AND toYYYYMM(created_at) >= {extended_partition: UInt32}
    ) cd ON cd.session_id = i.session_id AND cd.campaign_id = i.campaign_id
    GROUP BY s.placement
    """
    rows = ch.query(sql, parameters=params).result_rows
    return [
        {
            "placement":   r[0],
            "impressions": int(r[1]),
            "conversions": int(r[2]),
            "revenue":     round(float(r[3]), 2),
            "payout":      round(float(r[4]), 2),
        }
        for r in rows
    ]


def publisher_health_click_metrics(
    ch, pid: int, partition: int, days: int, geo_state: Optional[str] = None
) -> list[dict]:
    """
    Click metrics by placement: click count, converted clicks, and avg offer position.
    Joins clicks (left, filtered by user_id) to sessions (hash table).

    Returns: list of dicts with keys:
        placement, clicks (int), converted_clicks (int), avg_position (float)
    """
    sql = """
    SELECT
        s.placement,
        count(tc.id)                            AS clicks,
        countIf(tc.is_converted)               AS converted_clicks,
        round(avg(tc.position), 1)             AS avg_position
    FROM (
        SELECT session_id, id, is_converted, position
        FROM adpx_tracked_clicks
        PREWHERE user_id = {pid: UInt64}
            AND toYYYYMM(created_at) >= {partition: UInt32}
        WHERE created_at >= today() - {days: UInt32}
    ) tc
    JOIN (
        SELECT session_id, placement
        FROM adpx_sdk_sessions
        PREWHERE user_id = {pid: UInt64}
            AND toYYYYMM(created_at) >= {partition: UInt32}
        WHERE created_at >= today() - {days: UInt32}
    """
    params: dict = {"pid": pid, "partition": partition, "days": days}
    if geo_state:
        sql += "      AND state ILIKE {geo_state: String}\n"
        params["geo_state"] = f"%{geo_state}%"
    sql += """
    ) s ON s.session_id = tc.session_id
    GROUP BY s.placement
    """
    rows = ch.query(sql, parameters=params).result_rows
    return [
        {
            "placement":        r[0],
            "clicks":           int(r[1]),
            "converted_clicks": int(r[2]),
            "avg_position":     float(r[3] or 0),
        }
        for r in rows
    ]


def publisher_placement_names(ch, pid: int) -> dict[str, str]:
    """
    Display name overrides for placement slugs (e.g. "order_confirmation" → "Order Confirmation").
    Non-fatal if the from_airbyte_placements table is unavailable — callers use slugs as fallback.

    Returns: dict mapping slug -> display_name (empty dict on error)
    """
    try:
        rows = ch.query(
            "SELECT slug, display_name FROM from_airbyte_placements WHERE user_id = {pid: Int64}",
            parameters={"pid": int(pid)},
        ).result_rows
        return {slug: dn for slug, dn in rows if dn}
    except Exception:
        return {}


# ===========================================================================
# Fill rate (was: get_low_fill_publishers)
# ===========================================================================

def low_fill_publishers(ch, placements: list[str]) -> list[dict]:
    """
    DEPRECATED — use fill_rate_publishers() instead.

    This function uses a 30-day window with a hardcoded 10,000 minimum-session
    threshold, which does not match the signal path (7d / 2,500 sessions defined
    in config/scout_thresholds.json). Retained for wide-window diagnostic queries
    where the 30-day view is intentional (e.g., monthly trend analysis).

    Publishers on post-transaction placements with fill rate < 15% over last 30 days.
    Fill rate = % of sessions that received at least one offer impression.
    Threshold: 10,000+ sessions (excludes low-volume publishers from alert).

    placements: list of placement name strings (see POST_TX_PLACEMENTS constant above).

    Returns: list of dicts with keys:
        publisher_id (int), publisher_name (str), placement (str),
        sessions_30d (int), sessions_with_imps (int), fill_rate_pct (float),
        missed_sessions (int), revenue_30d (float)
    Raises on ClickHouse error — callers must catch.
    """
    rows = ch.query(
        f"""
        WITH sessions_agg AS (
            SELECT
                toInt64(user_id) AS publisher_id,
                placement,
                count() AS sessions_30d
            FROM adpx_sdk_sessions
            PREWHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(30)})
            WHERE created_at >= {_ch_date_filter(30)}
              AND placement IN {{placements: Array(String)}}
            GROUP BY user_id, placement
            HAVING sessions_30d > 10000
        ),
        imps_agg AS (
            SELECT
                toInt64(pid) AS publisher_id,
                count(DISTINCT session_id) AS sessions_with_imps
            FROM adpx_impressions_details
            PREWHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(30)})
            WHERE created_at >= {_ch_date_filter(30)}
            GROUP BY pid
        ),
        rev_agg AS (
            SELECT
                toInt64(user_id) AS publisher_id,
                coalesce(sum(toFloat64OrNull(revenue)), 0) AS revenue_30d,
                count(DISTINCT session_id) AS converting_sessions
            FROM adpx_conversionsdetails
            PREWHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(30)})
            WHERE created_at >= {_ch_date_filter(30)}
            GROUP BY user_id
        )
        SELECT
            s.publisher_id,
            coalesce(u.organization, '') AS publisher_name,
            s.placement,
            s.sessions_30d,
            coalesce(i.sessions_with_imps, 0) AS sessions_with_imps,
            round(100.0 * coalesce(i.sessions_with_imps, 0) / s.sessions_30d, 2) AS fill_rate_pct,
            s.sessions_30d - coalesce(i.sessions_with_imps, 0)                    AS missed_sessions,
            coalesce(r.revenue_30d, 0)                                             AS revenue_30d
        FROM sessions_agg s
        LEFT JOIN imps_agg i ON i.publisher_id = s.publisher_id
        LEFT JOIN rev_agg r  ON r.publisher_id = s.publisher_id
        -- mv_adpx_users is a lightweight MV (id, organization, is_test, parent_id only)
        LEFT JOIN mv_adpx_users u ON toUInt64(s.publisher_id) = u.id
        WHERE coalesce(i.sessions_with_imps, 0) * 100.0 / s.sessions_30d < 15
        ORDER BY missed_sessions DESC
        LIMIT 15
        """,
        parameters={"placements": list(placements)},
    ).result_rows
    return [
        {
            "publisher_id":       int(r[0]),
            "publisher_name":     r[1] or f"Pub #{r[0]}",
            "placement":          r[2],
            "sessions_30d":       int(r[3]),
            "sessions_with_imps": int(r[4]),
            "fill_rate_pct":      round(float(r[5]), 2),
            "missed_sessions":    int(r[6]),
            "revenue_30d":        round(float(r[7]), 2),
        }
        for r in rows
    ]


# ===========================================================================
# Publisher offer recommendations (was: get_offers_for_publisher SQL portions)
# ===========================================================================

def publisher_top_categories(ch, pub_id: int) -> list[str]:
    """
    Top 5 converting offer categories for this publisher over the last 6 months.
    Used to re-rank affiliate offer recommendations by audience fit.
    Non-fatal if query fails — callers fall back to RPM-only ranking.

    Returns: list of category name strings (empty list on error or no data)

    PR 19: previously joined against mv_adpx_campaigns.category which doesn't exist
    (mv_adpx_campaigns has only id/internal_name/is_test). The query has been
    silently returning [] for some time, swallowed by the except handler.
    Now joins directly against from_airbyte_campaigns and uses tags-parsing
    (same fix as performance_benchmarks_raw above).
    """
    try:
        rows = ch.query(
            """
            WITH joined AS (
                SELECT
                    c.tags,
                    cv.revenue
                FROM default.adpx_conversionsdetails cv
                JOIN default.from_airbyte_campaigns c
                  ON toInt64(cv.campaign_id) = toInt64(c.id)
                PREWHERE cv.user_id = {uid: Int64}
                  AND toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL 6 MONTH)
                WHERE cv.created_at >= today() - INTERVAL 6 MONTH
                  AND c.deleted_at IS NULL
                  AND c.tags IS NOT NULL
                  AND c.tags != '[]'
            )
            SELECT
                arrayJoin(arrayFilter(
                    t -> NOT startsWith(t, 'internal-'),
                    JSONExtract(coalesce(tags, '[]'), 'Array(String)')
                )) AS category,
                count() AS conversions,
                sum(toFloat64OrNull(revenue)) AS revenue
            FROM joined
            GROUP BY category
            HAVING category != ''
            ORDER BY revenue DESC NULLS LAST
            LIMIT 5
            """,
            parameters={"uid": int(pub_id)},
        ).result_rows
        return [row[0] for row in rows if row[0]]
    except Exception:
        return []


# ===========================================================================
# Revenue trends (publisher)
# ===========================================================================

def publisher_revenue_trends(ch, days: int = 7, min_periods: int = 4) -> list[dict]:
    """
    DEPRECATED for velocity alerts — use velocity_alerts() instead.

    This function uses a period-median algorithm (sequential N-day windows, median as
    baseline), which does not match the signal path. velocity_alerts() uses the canonical
    annualized comparison formula: ((rev_7d/7)*30 - rev_30d) / rev_30d * 100, with
    threshold -25% as defined in config/scout_thresholds.json.

    Retained for analytical trend visualisation where the period-median view is intentional
    (e.g., smoothed long-run trend charts that are not alert-quality signals).

    Publisher revenue trends: actual revenue vs. historical median for the same period length.

    Algorithm: divide history into sequential `days`-length windows, take the median of 8
    such windows as the expected baseline, then compare the most-recent window (actual).
    This avoids the semantic error of comparing a period total against a median of daily values.

    Returns list of dicts with keys:
        publisher_id (int), publisher_name (str),
        revenue_actual (float), revenue_expected (float), delta_pct (float),
        trend ("up" | "down" | "flat"), sessions_actual (int)
    Raises on ClickHouse error — callers must catch.
    """
    rows = ch.query(
        _revenue_trend_sql(
            entity_col="cv.user_id                                             AS publisher_id",
            group_col="publisher_id",
            actual_metric="count(DISTINCT cv.session_id)                         AS sessions_actual",
            # adpx_conversionsdetails.user_id is the publisher ID — no session join needed.
            actual_join="",
            historical_join="",
            names_cte=""",
        names AS (
            SELECT id AS publisher_id, organization AS publisher_name
            FROM mv_adpx_users
        )""",
            final_select="""SELECT
            b.publisher_id,
            coalesce(n.publisher_name, toString(b.publisher_id))       AS publisher_name,
            coalesce(a.revenue_actual, 0)                              AS revenue_actual,
            b.revenue_expected,
            round((coalesce(a.revenue_actual, 0) - b.revenue_expected) /
                  nullIf(b.revenue_expected, 0) * 100, 1)              AS delta_pct,
            coalesce(a.sessions_actual, 0)                             AS sessions_actual
        FROM baseline b
        LEFT JOIN actual a ON a.publisher_id = b.publisher_id
        LEFT JOIN names n ON n.publisher_id = b.publisher_id
        ORDER BY delta_pct ASC""",
        ),
        parameters={
            "days": int(days),
            "lookback": int(days * 9),
            "min_periods": int(min_periods),
        },
    ).result_rows

    return [
        {
            "publisher_id": int(r[0]),
            "publisher_name": r[1],
            "revenue_actual": float(r[2] or 0),
            "revenue_expected": float(r[3] or 0),
            "delta_pct": float(r[4] or 0),
            "trend": _trend(float(r[4] or 0)),
            "sessions_actual": int(r[5] or 0),
        }
        for r in rows
    ]


def publisher_fleet_health_stats(
    ch,
    days: int = 7,
    min_windows: int = 3,
    min_revenue: float = 500.0,
) -> list[dict]:
    """
    Statistical fleet health baseline: rolling 4-week same-period average + σ-score.

    For each publisher computes:
    - revenue_actual: revenue in the most recent `days`-day window
    - revenue_expected: mean of the 4 prior same-length windows (rolling WoW avg)
    - sigma_score: (actual - mean) / stddev — std deviations from normal
    - dollar_gap: revenue_expected - revenue_actual (positive = shortfall)
    - tier: from from_airbyte_partner_categories (empty string if no entry)

    Gates:
    - min_windows: require at least this many of 4 prior windows (default 3)
    - min_revenue: materiality floor — only publishers with mean >= this ($500)

    Filters applied in Python to avoid ClickHouse Nullable(UInt8) type issues:
    - Excludes publishers whose name ends with ' Super' (super-admin accounts)
    No SQL-side is_test filter (Nullable UInt8 throws exception 349 in WHERE).

    Returns list sorted by dollar_gap DESC (largest shortfall first).
    """
    lookback = days * 5

    rows = ch.query(
        """
        WITH
        -- ── Current window ──────────────────────────────────────────────────────
        current_window AS (
            SELECT
                user_id                                                AS publisher_id,
                round(sum(toFloat64OrZero(revenue)), 2)               AS revenue_actual
            FROM adpx_conversionsdetails
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - INTERVAL {days: Int32} DAY)
            WHERE created_at >= today() - INTERVAL {days: Int32} DAY
            GROUP BY publisher_id
        ),
        -- ── Prior 4 same-length windows ─────────────────────────────────────────
        -- window_idx 0 = most recent prior period, 3 = oldest of the 4
        prior_windows AS (
            SELECT
                user_id                                                AS publisher_id,
                intDiv(
                    dateDiff('day', toDate(created_at, 'America/Chicago'), today())
                    - {days: Int32},
                    {days: Int32}
                )                                                      AS window_idx,
                round(sum(toFloat64OrZero(revenue)), 2)               AS window_revenue
            FROM adpx_conversionsdetails
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - INTERVAL {lookback: Int32} DAY)
            WHERE created_at >= today() - INTERVAL {lookback: Int32} DAY
              AND created_at < today() - INTERVAL {days: Int32} DAY
            GROUP BY publisher_id, window_idx
            HAVING window_idx BETWEEN 0 AND 3
        ),
        -- ── Per-publisher baseline stats ─────────────────────────────────────────
        baseline AS (
            SELECT
                publisher_id,
                round(avg(window_revenue), 2)                         AS revenue_mean,
                round(stddevSamp(window_revenue), 2)                  AS revenue_stddev,
                count()                                               AS window_count
            FROM prior_windows
            GROUP BY publisher_id
            HAVING window_count >= {min_windows: Int32}
        ),
        -- ── Publisher names + tier ───────────────────────────────────────────────
        publishers AS (
            SELECT id AS publisher_id, organization AS publisher_name
            FROM mv_adpx_users
        ),
        tiers AS (
            SELECT toString(user_id) AS publisher_id, toString(tier) AS tier
            FROM from_airbyte_partner_categories
        )
        SELECT
            b.publisher_id,
            coalesce(p.publisher_name, toString(b.publisher_id))      AS publisher_name,
            coalesce(t.tier, '')                                       AS tier,
            coalesce(cw.revenue_actual, 0)                            AS revenue_actual,
            b.revenue_mean                                             AS revenue_expected,
            round(
                (coalesce(cw.revenue_actual, 0) - b.revenue_mean)
                / nullIf(b.revenue_mean, 0) * 100,
                1
            )                                                          AS delta_pct,
            round(
                (coalesce(cw.revenue_actual, 0) - b.revenue_mean)
                / nullIf(b.revenue_stddev, 0),
                2
            )                                                          AS sigma_score
        FROM baseline b
        LEFT JOIN publishers p ON p.publisher_id = b.publisher_id
        LEFT JOIN current_window cw ON cw.publisher_id = b.publisher_id
        LEFT JOIN tiers t ON t.publisher_id = toString(b.publisher_id)
        WHERE b.revenue_mean >= {min_revenue: Float64}
        ORDER BY (b.revenue_mean - coalesce(cw.revenue_actual, 0)) DESC
        """,
        parameters={
            "days": int(days),
            "lookback": int(lookback),
            "min_windows": int(min_windows),
            "min_revenue": float(min_revenue),
        },
    ).result_rows

    _SUPER_SUFFIX = " Super"
    return [
        {
            "publisher_id": int(r[0]),
            "publisher_name": str(r[1]),
            "tier": str(r[2]),
            "revenue_actual": float(r[3] or 0),
            "revenue_expected": float(r[4] or 0),
            "delta_pct": float(r[5] or 0),
            "sigma_score": float(r[6]) if r[6] is not None else -99.0,
            "dollar_gap": float(r[4] or 0) - float(r[3] or 0),
        }
        for r in rows
        if not str(r[1]).endswith(_SUPER_SUFFIX)
    ]


def get_publisher_fleet_health_data(
    ch,
    days: int = 7,
    min_windows: int = 3,
    min_revenue: float = 500.0,
    act_now_sigma: float = -2.0,
    act_now_gap: float = 500.0,
    watch_sigma: float = -1.5,
    watch_gap: float = 200.0,
) -> dict:
    """
    Classify publishers into Act Now / Watch / Healthy using σ-based gates.

    Gates (both conditions must be met):
    - act_now:  sigma_score <= -2.0 AND dollar_gap >= $500
    - watch:    sigma_score <= -1.5 AND dollar_gap >= $200
    - healthy:  delta_pct >= 0

    Platform-wide alarm fires when act_now count > 5.

    Returns:
    {
        as_of, window_days, total_publishers, total_gap,
        act_now, watch, healthy_top5, platform_alarm, insufficient_history
    }
    """
    import sys
    from datetime import timezone, datetime

    # Look up through the module to allow mocking via `patch('queries.publisher_fleet_health_stats')`.
    _fleet_stats_fn = getattr(sys.modules.get("queries", sys.modules[__name__]), "publisher_fleet_health_stats")
    stats = _fleet_stats_fn(
        ch,
        days=days,
        min_windows=min_windows,
        min_revenue=min_revenue,
    )

    as_of = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not stats:
        return {
            "as_of": as_of,
            "window_days": days,
            "total_publishers": 0,
            "total_gap": 0.0,
            "act_now": [],
            "watch": [],
            "healthy_top5": [],
            "platform_alarm": False,
            "insufficient_history": True,
        }

    act_now, watch, healthy = [], [], []
    for pub in stats:
        gap = pub["dollar_gap"]
        sigma = pub["sigma_score"]
        if sigma <= act_now_sigma and gap >= act_now_gap:
            act_now.append(pub)
        elif sigma <= watch_sigma and gap >= watch_gap:
            watch.append(pub)
        elif pub["delta_pct"] >= 0:
            healthy.append(pub)

    healthy_top5 = sorted(
        healthy,
        key=lambda p: p["revenue_actual"] - p["revenue_expected"],
        reverse=True,
    )[:5]

    total_gap = sum(p["dollar_gap"] for p in act_now + watch if p["dollar_gap"] > 0)

    return {
        "as_of": as_of,
        "window_days": days,
        "total_publishers": len(stats),
        "total_gap": round(total_gap, 2),
        "act_now": act_now,
        "watch": watch,
        "healthy_top5": healthy_top5,
        "platform_alarm": len(act_now) > 5,
        "insufficient_history": False,
    }
