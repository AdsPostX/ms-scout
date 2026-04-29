"""
queries.py — All ClickHouse SQL for Scout.

Rules (enforced here, not aspirational):
  - Every function: typed parameters, no f-strings, docstring explaining return shape
  - Returns list[dict] (never raw rows) — callers never unpack tuples
  - Named for what it fetches, not for the tool that uses it
  - Shared functions (used by both agent tools AND Pulse) live here, not in scout_agent.py
  - Any threshold, window, or filter change belongs in this file, not in callers

Import pattern in callers:
    from queries import ghost_campaigns, revenue_opportunities  # etc.
"""

from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Post-transaction placement names (canonical list — single source of truth)
# Used by low_fill_publishers() to filter sessions to monetizable placements.
# Kept here so callers don't need to import from scout_agent.py.
# ---------------------------------------------------------------------------
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
# Ghost campaigns
# ===========================================================================

def ghost_campaigns(ch) -> list[dict]:
    """
    Canonical ghost campaign detection — single source of truth for both the
    agent tool (get_ghost_campaigns) and the 8am Pulse signal (_run_pulse_signals).

    A campaign qualifies as a ghost if ALL of:
    - status = 'Active', non-expired
    - 5,000+ impressions in last 7 days (meaningful traffic volume)
    - 2,000+ impressions in last 48h (actively burning inventory RIGHT NOW)
    - 200+ clicks in 7 days (real engagement, not just display)
    - Zero conversions in 7 days (broken tracking or non-converting offer)
    - Campaign age > 7 days (excludes new launches still warming up)
    - conversion_events configured (CPA/CPS only — excludes CPM/CPC by design)

    Returns: list of dicts with keys:
        campaign_id, adv_name, campaign_title,
        impressions_7d, impressions_2d, clicks_7d, revenue_7d,
        first_impression_date, publisher_ids, publisher_names
    Raises on ClickHouse error — callers must catch.
    """
    sql = """
WITH imp_agg AS (
    SELECT campaign_id, count() AS impressions_7d, min(created_at)::Date AS first_impression_date
    FROM adpx_impressions_details
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
    WHERE created_at >= today() - 7
    GROUP BY campaign_id
    HAVING impressions_7d > 5000
),
recent_imp AS (
    SELECT campaign_id, count() AS impressions_2d
    FROM adpx_impressions_details
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 2)
    WHERE created_at >= today() - 2
    GROUP BY campaign_id
    HAVING impressions_2d >= 2000
),
click_agg AS (
    SELECT campaign_id, count() AS clicks_7d
    FROM adpx_tracked_clicks
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
    WHERE created_at >= today() - 7
    GROUP BY campaign_id
    HAVING clicks_7d > 100
),
rev_agg AS (
    SELECT campaign_id,
           coalesce(sum(toFloat64OrNull(revenue)), 0) AS revenue_7d,
           count()                                    AS conversion_count_7d
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
    WHERE created_at >= today() - 7
    GROUP BY campaign_id
)
SELECT
    c.id                                          AS campaign_id,
    c.adv_name,
    c.title                                       AS campaign_title,
    ia.impressions_7d,
    ri.impressions_2d,
    ca.clicks_7d,
    coalesce(ra.revenue_7d, 0)                    AS revenue_7d,
    toString(ia.first_impression_date)            AS first_impression_date,
    groupArray(toInt64(pc.user_id))               AS publisher_ids,
    groupArray(u.organization)                    AS publisher_names
FROM imp_agg ia
JOIN recent_imp ri ON toString(ri.campaign_id) = toString(ia.campaign_id)
JOIN click_agg ca ON toString(ca.campaign_id) = toString(ia.campaign_id)
JOIN from_airbyte_campaigns c ON toInt64(ia.campaign_id) = c.id
    AND JSONLength(c.conversion_events) > 0
    AND (c.is_test = false OR c.is_test IS NULL)
    AND c.status = 'Active'
    AND (c.end_date IS NULL OR c.end_date >= today())
LEFT JOIN rev_agg ra ON toString(ra.campaign_id) = toString(ia.campaign_id)
LEFT JOIN from_airbyte_publisher_campaigns pc
    ON toString(pc.campaign_id) = toString(ia.campaign_id) AND pc.is_active = 1
LEFT JOIN from_airbyte_users u ON pc.user_id = u.id
WHERE coalesce(ra.conversion_count_7d, 0) = 0
  AND ia.first_impression_date <= today() - 7
  AND c.deleted_at IS NULL
GROUP BY c.id, c.adv_name, c.title, ia.impressions_7d, ri.impressions_2d, ca.clicks_7d, revenue_7d, ia.first_impression_date
HAVING impressions_7d > 5000 AND clicks_7d > 200
ORDER BY impressions_7d DESC
LIMIT 25
"""
    rows = ch.query(sql).result_rows
    return [
        {
            "campaign_id":           r[0],
            "adv_name":              r[1],
            "campaign_title":        r[2],
            "impressions_7d":        int(r[3]),
            "impressions_2d":        int(r[4]),
            "clicks_7d":             int(r[5]),
            "revenue_7d":            round(float(r[6]), 2),
            "first_impression_date": str(r[7])[:10] if r[7] else "unknown",
            "publisher_ids":         list(r[8]),
            "publisher_names":       list(r[9]),
        }
        for r in rows
    ]


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
        FROM default.adpx_impressions_details
        PREWHERE pid IN {pid_list: Array(String)}
        WHERE created_at >= today() - {days: UInt32}
        GROUP BY pid
        ORDER BY impressions DESC
        LIMIT 50
        """,
        parameters={"pid_list": pid_list, "days": days},
    ).result_rows
    return {str(r[0]): int(r[1]) for r in rows}


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
    ch, pub_pid: str, campaign_ids: list[str], days: int = 14
) -> dict[str, float]:
    """
    RPM (revenue per 1,000 impressions) per campaign on this publisher.
    Only campaigns in `campaign_ids` that are actively serving are included.
    Revenue is joined from conversions within sessions that had impressions on this publisher.

    pub_pid: numeric publisher ID as string.

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
            SELECT cv.campaign_id,
                   sum(toFloat64OrNull(cv.revenue)) AS total_revenue
            FROM default.adpx_conversionsdetails cv
            WHERE cv.session_id IN (
                SELECT session_id
                FROM default.adpx_impressions_details
                PREWHERE pid = {pub_pid: String}
                WHERE created_at >= today() - {days: UInt32}
                  AND campaign_id IN {cids: Array(String)}
            )
              AND toYYYYMM(cv.created_at) >= toYYYYMM(today() - {extended_days: UInt32})
            GROUP BY cv.campaign_id
        ) cv ON toInt64(imp.campaign_id) = toInt64(cv.campaign_id)
        """,
        parameters={
            "pub_pid":       pub_pid,
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
        """
        WITH imp_agg AS (
            SELECT campaign_id, count() AS impressions_30d
            FROM adpx_impressions_details
            WHERE created_at >= today() - 30
              AND toYYYYMM(created_at) >= toYYYYMM(today() - 30)
            GROUP BY campaign_id
        ),
        conv_agg AS (
            SELECT campaign_id, sum(toFloat64OrNull(revenue)) AS revenue_30d
            FROM adpx_conversionsdetails
            WHERE created_at >= today() - 30
              AND toYYYYMM(created_at) >= toYYYYMM(today() - 30)
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
          AND pc.user_id != {pub_id: Int64}
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
        """
        SELECT c.adv_name, min(pc.created_at) AS provisioned_since
        FROM from_airbyte_publisher_campaigns pc
        JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
        LEFT JOIN adpx_impressions_details i
            ON i.campaign_id = toUInt64(pc.campaign_id)
            AND i.pid = {pub_pid: String}
            AND i.created_at >= today() - 30
        WHERE pc.user_id = {pub_id: Int64}
          AND pc.is_active = true
          AND pc.deleted_at IS NULL AND c.deleted_at IS NULL
          AND i.campaign_id IS NULL
        GROUP BY c.adv_name
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
        """
        SELECT count() AS sessions
        FROM adpx_sdk_sessions
        WHERE user_id = {pub_id: Int64}
          AND created_at >= today() - 30
          AND toYYYYMM(created_at) >= toYYYYMM(today() - 30)
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
        """
        SELECT u.id, u.organization, count() AS sessions_30d
        FROM adpx_sdk_sessions s
        JOIN from_airbyte_users u ON s.user_id = u.id
        WHERE s.created_at >= today() - 30
          AND toYYYYMM(s.created_at) >= toYYYYMM(today() - 30)
          AND s.user_id NOT IN {active_ids: Array(Int64)}
        GROUP BY u.id, u.organization
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
        """
        WITH sessions_agg AS (
            SELECT
                toInt64(user_id) AS publisher_id,
                placement,
                count() AS sessions_30d
            FROM adpx_sdk_sessions
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
            WHERE created_at >= today() - 30
              AND placement IN {placements: Array(String)}
            GROUP BY user_id, placement
            HAVING sessions_30d > 10000
        ),
        imps_agg AS (
            SELECT
                toInt64(pid) AS publisher_id,
                count(DISTINCT session_id) AS sessions_with_imps
            FROM adpx_impressions_details
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
            WHERE created_at >= today() - 30
            GROUP BY pid
        ),
        rev_agg AS (
            SELECT
                toInt64(user_id) AS publisher_id,
                coalesce(sum(toFloat64OrNull(revenue)), 0) AS revenue_30d,
                count(DISTINCT session_id) AS converting_sessions
            FROM adpx_conversionsdetails
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
            WHERE created_at >= today() - 30
            GROUP BY user_id
        )
        SELECT
            s.publisher_id,
            u.organization AS publisher_name,
            s.placement,
            s.sessions_30d,
            coalesce(i.sessions_with_imps, 0) AS sessions_with_imps,
            round(100.0 * coalesce(i.sessions_with_imps, 0) / s.sessions_30d, 2) AS fill_rate_pct,
            s.sessions_30d - coalesce(i.sessions_with_imps, 0)                    AS missed_sessions,
            coalesce(r.revenue_30d, 0)                                             AS revenue_30d
        FROM sessions_agg s
        LEFT JOIN imps_agg i ON i.publisher_id = s.publisher_id
        LEFT JOIN rev_agg r  ON r.publisher_id = s.publisher_id
        LEFT JOIN from_airbyte_users u ON s.publisher_id = u.id
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
# Revenue opportunities (was: get_top_revenue_opportunities)
# ===========================================================================

def revenue_opportunities(ch) -> list[dict]:
    """
    Cross-publisher revenue gap opportunities: high-performing advertisers (2+ publishers,
    >$10K/30d) NOT active in high-volume publishers (>100K sessions/30d).

    Uses fuzzy-match LEFT JOIN anti-join to suppress variants ("Disney+" vs "Disney+ and Hulu").
    This is the canonical opportunity query — any threshold change belongs here, not in callers.

    Note: The agent tool (get_top_revenue_opportunities) applies additional fuzzy de-duplication
    at the Python level. The Pulse signal (_build_opportunity_signal) does not — known drift,
    tracked in CLAUDE.md. Extract _query_revenue_opportunities when this causes a false recommendation.

    Returns: list of dicts with keys:
        publisher_name (str), publisher_id (int), adv_name (str),
        adv_total_rev_30d (float), est_monthly_rev (float), adv_pub_count (int), sessions_30d (int)
    Raises on ClickHouse error — callers must catch.
    """
    rows = ch.query(
        """
        WITH adv_perf AS (
            SELECT
                c.adv_name,
                count(DISTINCT cv.user_id)                               AS publisher_count,
                round(sum(toFloat64OrNull(cv.revenue)), 2)               AS rev_30d,
                round(sum(toFloat64OrNull(cv.revenue))
                      / nullIf(count(DISTINCT cv.user_id), 0), 2)        AS avg_rev_per_pub
            FROM adpx_conversionsdetails cv
            JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - 30)
              AND cv.created_at >= today() - 30
            GROUP BY c.adv_name
            HAVING publisher_count >= 2 AND rev_30d >= 10000
        ),
        pub_volume AS (
            SELECT
                toInt64(user_id) AS publisher_id,
                u.organization   AS publisher_name,
                count()          AS sessions_30d
            FROM adpx_sdk_sessions s
            JOIN from_airbyte_users u ON toInt64(s.user_id) = u.id
            WHERE toYYYYMM(s.created_at) >= toYYYYMM(today() - 30)
              AND s.created_at >= today() - 30
            GROUP BY publisher_id, publisher_name
            HAVING sessions_30d > 100000
        ),
        active_pairs AS (
            SELECT DISTINCT
                toInt64(pc.user_id) AS publisher_id,
                lower(c.adv_name)   AS adv_name_lower
            FROM from_airbyte_publisher_campaigns pc
            JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
            WHERE pc.is_active = 1 AND pc.deleted_at IS NULL
        ),
        candidates AS (
            SELECT
                pv.publisher_name,
                pv.publisher_id,
                adv.adv_name,
                lower(adv.adv_name) AS adv_name_lower,
                adv.rev_30d         AS adv_total_rev_30d,
                adv.avg_rev_per_pub AS est_monthly_rev,
                adv.publisher_count AS adv_pub_count,
                pv.sessions_30d
            FROM pub_volume pv
            CROSS JOIN adv_perf adv
        )
        SELECT
            c.publisher_name,
            c.publisher_id,
            c.adv_name,
            c.adv_total_rev_30d,
            c.est_monthly_rev,
            c.adv_pub_count,
            c.sessions_30d
        FROM candidates c
        LEFT JOIN active_pairs ap
            ON ap.publisher_id = c.publisher_id
            AND (
                ap.adv_name_lower = c.adv_name_lower
                OR position(c.adv_name_lower, ap.adv_name_lower) > 0
                OR position(ap.adv_name_lower, c.adv_name_lower) > 0
            )
        WHERE ap.publisher_id IS NULL
        ORDER BY c.est_monthly_rev DESC, c.sessions_30d DESC
        LIMIT 20
        """
    ).result_rows
    return [
        {
            "publisher_name":    r[0],
            "publisher_id":      int(r[1]),
            "adv_name":          r[2],
            "adv_total_rev_30d": round(float(r[3] or 0), 2),
            "est_monthly_rev":   round(float(r[4] or 0), 2),
            "adv_pub_count":     int(r[5]),
            "sessions_30d":      int(r[6]),
        }
        for r in rows
    ]


# ===========================================================================
# Performance benchmarks (was: _load_performance_benchmarks)
# ===========================================================================

def performance_benchmarks_raw(ch) -> list[tuple]:
    """
    Raw CVR + RPM benchmark data from MS live conversion history.
    Joins campaigns → conversions → impressions, filtered to campaigns with
    500+ impressions since 2025-01.

    Used by _load_performance_benchmarks() in scout_agent.py to build the
    four-tier lookup (by_offer_impact_id, by_adv_name, by_category_payout, by_payout_type).

    Returns raw tuples — caller is responsible for building the tiered lookup dict.
    Tuple columns: (id, adv_name, impact_id, category, impression_count, cvr_pct, rpm)
    Raises on ClickHouse error — callers must catch and return empty benchmarks.

    PR 19: `c.categories` column is NULL for all rows in production. Real category
    data lives in `c.tags` as a JSON array; we filter out `internal-*` system tags
    (network/channel metadata) and arrayJoin to fan out one row per category.
    Multi-category campaigns contribute to each category's benchmark — the consumer
    in scout_agent._load_performance_benchmarks() accumulates by category.

    The arrayJoin runs in the OUTER select (after the joins are done in the CTE)
    so the impression/conversion JOINs operate on per-campaign cardinality, not
    per-(campaign × tag). Avoids unnecessary cartesian work.
    """
    rows = ch.query(
        """
        WITH joined AS (
            SELECT
                c.id,
                c.adv_name,
                c.tags,
                trim(c.internal_network_name)                                          AS impact_id,
                imp.impression_count                                                   AS impression_count,
                round(conv.conversion_count / nullIf(imp.impression_count, 0) * 100, 4) AS cvr_pct,
                round(conv.total_revenue    / nullIf(imp.impression_count, 0) * 1000, 2) AS rpm
            FROM default.from_airbyte_campaigns c
            JOIN (
                SELECT campaign_id,
                       count()                           AS conversion_count,
                       sum(toFloat64OrNull(revenue))      AS total_revenue
                FROM default.adpx_conversionsdetails
                WHERE toYYYYMM(created_at) >= 202501
                GROUP BY campaign_id
            ) conv ON toInt64(c.id) = toInt64(conv.campaign_id)
            JOIN (
                SELECT campaign_id,
                       count() AS impression_count
                FROM default.adpx_impressions_details
                WHERE toYYYYMM(created_at) >= 202501
                GROUP BY campaign_id
            ) imp ON toInt64(c.id) = toInt64(imp.campaign_id)
            WHERE c.deleted_at IS NULL
              AND imp.impression_count > 500
        )
        SELECT
            id,
            adv_name,
            impact_id,
            arrayJoin(arrayFilter(
                t -> NOT startsWith(lower(t), 'internal-'),
                JSONExtract(coalesce(tags, '[]'), 'Array(String)')
            )) AS category,
            impression_count,
            cvr_pct,
            rpm
        FROM joined
        ORDER BY impression_count DESC
        """
    ).result_rows
    return rows


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
