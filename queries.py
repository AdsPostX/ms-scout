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
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Home scoreboard rollup — scoreboard_rollup()
#
# Single narrow query against adpx_conversionsdetails. No JOINs to
# impression-level tables; those cause OOM on cvr_anomaly today. eCPM and
# Fill are deliberately omitted from v1 — render them as "—" in the UI
# rather than waiting on a heavy MV that may not exist yet.
# ---------------------------------------------------------------------------

@dataclass
class PublisherDelta:
    publisher_id: int
    publisher_name: str
    revenue_today_cents: int
    revenue_baseline_cents: int  # 7-day same-time-of-day average
    delta_pct: float             # signed percentage Δ vs baseline


@dataclass
class ScoreboardRollup:
    revenue_today_cents: int
    revenue_yesterday_same_time_cents: int
    revenue_7d_avg_cents: int
    conversions_today: int
    conversions_yesterday_same_time: int
    conversions_7d_avg: int
    winners: list[PublisherDelta] = field(default_factory=list)   # top 3 by revenue Δ%
    worry:   list[PublisherDelta] = field(default_factory=list)   # bottom 3 by revenue Δ%
    revenue_7d_series: list[int] = field(default_factory=list)    # 8 daily cents [D-7..D-1, today]
    revenue_eod_projection_cents: int = 0                         # linear EOD extrapolation; 0 = too early
    revenue_mtd_cents: int = 0                                    # month-to-date revenue in cents
    generated_at: datetime = field(default_factory=datetime.utcnow)


def scoreboard_rollup(ch) -> ScoreboardRollup:
    """Headline numbers for the App Home scoreboard.

    Returns revenue + conversion totals for three windows (today, yesterday
    same-time-of-day, 7-day same-time-of-day average) plus top/bottom-3
    publisher Δ% lists.

    Source: adpx_conversionsdetails only. All timezone math lives in
    ClickHouse (America/Chicago) so DST doesn't drift the comparison windows.

    Errors propagate — caller (the home cache layer) wraps in try/except
    and renders "—" placeholders if the query fails.
    """
    # ── Headline totals: today / yesterday-same-time / 7d-same-time avg ──
    # Yesterday-same-time = midnight yesterday CT → now-minus-1-day.
    # 7d avg = sum of last 7 same-time windows ÷ 7 (excludes today).
    totals_sql = """
WITH
    toStartOfDay(toTimeZone(now(), 'America/Chicago')) AS today_start_ct,
    toStartOfDay(toTimeZone(now() - INTERVAL 1 DAY, 'America/Chicago')) AS yest_start_ct,
    toTimeZone(now(), 'America/Chicago') AS now_ct,
    (now_ct - today_start_ct) AS elapsed_today,
    yest_start_ct + elapsed_today AS yest_same_time_ct,
    toStartOfMonth(toTimeZone(now(), 'America/Chicago')) AS month_start_ct
SELECT
    coalesce(sum(if(toTimeZone(c.created_at, 'America/Chicago') >= today_start_ct
                    AND toTimeZone(c.created_at, 'America/Chicago') <= now_ct,
                    toFloat64OrNull(c.revenue), 0)), 0) AS rev_today,
    coalesce(sum(if(toTimeZone(c.created_at, 'America/Chicago') >= yest_start_ct
                    AND toTimeZone(c.created_at, 'America/Chicago') <= yest_same_time_ct,
                    toFloat64OrNull(c.revenue), 0)), 0) AS rev_yest,
    coalesce(sum(if(toTimeZone(c.created_at, 'America/Chicago') >= today_start_ct - INTERVAL 7 DAY
                    AND toTimeZone(c.created_at, 'America/Chicago') < today_start_ct
                    AND (toTimeZone(c.created_at, 'America/Chicago') - toStartOfDay(toTimeZone(c.created_at, 'America/Chicago'))) <= elapsed_today,
                    toFloat64OrNull(c.revenue), 0)), 0) / 7 AS rev_7d_avg,
    countIf(toTimeZone(c.created_at, 'America/Chicago') >= today_start_ct
            AND toTimeZone(c.created_at, 'America/Chicago') <= now_ct) AS conv_today,
    countIf(toTimeZone(c.created_at, 'America/Chicago') >= yest_start_ct
            AND toTimeZone(c.created_at, 'America/Chicago') <= yest_same_time_ct) AS conv_yest,
    countIf(toTimeZone(c.created_at, 'America/Chicago') >= today_start_ct - INTERVAL 7 DAY
            AND toTimeZone(c.created_at, 'America/Chicago') < today_start_ct
            AND (toTimeZone(c.created_at, 'America/Chicago') - toStartOfDay(toTimeZone(c.created_at, 'America/Chicago'))) <= elapsed_today) / 7 AS conv_7d_avg,
    coalesce(sum(if(toTimeZone(c.created_at, 'America/Chicago') >= month_start_ct
                    AND toTimeZone(c.created_at, 'America/Chicago') <= now_ct,
                    toFloat64OrNull(c.revenue), 0)), 0) AS rev_mtd
FROM adpx_conversionsdetails c
PREWHERE toYYYYMM(c.created_at) >= toYYYYMM(
    toDate(least(month_start_ct, today_start_ct - INTERVAL 7 DAY))
)
WHERE c.created_at >= least(month_start_ct, today_start_ct - INTERVAL 7 DAY)
""".strip()

    rows = ch.query(totals_sql).result_rows
    if not rows:
        return ScoreboardRollup(0, 0, 0, 0, 0, 0, [], [])

    rev_today, rev_yest, rev_7d_avg, conv_today, conv_yest, conv_7d_avg, rev_mtd = rows[0]

    # ── Per-publisher Δ% (today vs 7d-same-time avg) ──
    # Same source table; group by user_id and align with mv_adpx_users / from_airbyte_users
    # for the friendly name. Cap at top/bottom 3; ignore publishers under $50 today
    # (noise floor — single small conversion shouldn't crown a "winner").
    pub_sql = """
WITH
    toStartOfDay(toTimeZone(now(), 'America/Chicago')) AS today_start_ct,
    toTimeZone(now(), 'America/Chicago') AS now_ct,
    (now_ct - today_start_ct) AS elapsed_today
SELECT
    toInt64(c.user_id) AS uid,
    coalesce(any(u.organization), '') AS publisher_name,
    coalesce(sum(if(toTimeZone(c.created_at, 'America/Chicago') >= today_start_ct
                    AND toTimeZone(c.created_at, 'America/Chicago') <= now_ct,
                    toFloat64OrNull(c.revenue), 0)), 0) AS rev_today,
    coalesce(sum(if(toTimeZone(c.created_at, 'America/Chicago') >= today_start_ct - INTERVAL 7 DAY
                    AND toTimeZone(c.created_at, 'America/Chicago') < today_start_ct
                    AND (toTimeZone(c.created_at, 'America/Chicago') - toStartOfDay(toTimeZone(c.created_at, 'America/Chicago'))) <= elapsed_today,
                    toFloat64OrNull(c.revenue), 0)), 0) / 7 AS rev_baseline
FROM adpx_conversionsdetails c
LEFT JOIN from_airbyte_users u ON u.id = toInt64(c.user_id)
PREWHERE toYYYYMM(c.created_at) >= toYYYYMM(toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 8 DAY)
WHERE c.created_at >= toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 8 DAY
GROUP BY uid
HAVING rev_today >= 50 OR rev_baseline >= 50
""".strip()

    pub_rows = ch.query(pub_sql).result_rows
    deltas: list[PublisherDelta] = []
    for uid, name, today, baseline in pub_rows:
        today_c = int(float(today or 0) * 100)
        base_c  = int(float(baseline or 0) * 100)
        if base_c <= 0:
            pct = 100.0 if today_c > 0 else 0.0
        else:
            pct = round(100.0 * (today_c - base_c) / base_c, 1)
        deltas.append(PublisherDelta(
            publisher_id=int(uid),
            publisher_name=(name or "").strip() or f"pub {uid}",
            revenue_today_cents=today_c,
            revenue_baseline_cents=base_c,
            delta_pct=pct,
        ))

    deltas.sort(key=lambda d: d.delta_pct, reverse=True)
    winners = deltas[:3]
    worry   = list(reversed(deltas[-3:])) if len(deltas) >= 3 else []
    # If winners and worry overlap (very few publishers), prefer winners as-is
    # and trim worry to non-overlapping tail.
    win_ids = {d.publisher_id for d in winners}
    worry = [d for d in worry if d.publisher_id not in win_ids][:3]

    # ── 7-day daily revenue series (sparkline data) ──
    # Fetches one row per completed day (D-7 through D-1); fills missing days
    # with 0; appends today's partial as the 8th point.
    series_sql = """
SELECT
    toDate(toTimeZone(c.created_at, 'America/Chicago')) AS day,
    round(sum(toFloat64OrNull(c.revenue)), 2)           AS daily_rev
FROM adpx_conversionsdetails c
WHERE toDate(toTimeZone(c.created_at, 'America/Chicago'))
          >= toDate(toStartOfDay(toTimeZone(now(), 'America/Chicago'))) - 7
  AND toDate(toTimeZone(c.created_at, 'America/Chicago'))
          < toDate(toStartOfDay(toTimeZone(now(), 'America/Chicago')))
GROUP BY day
ORDER BY day
""".strip()

    series_map: dict[date, int] = {}
    series_query_ok = True
    try:
        for row in ch.query(series_sql).result_rows:
            day_val, rev_val = row
            if hasattr(day_val, "date"):
                day_val = day_val.date()
            series_map[day_val] = int(float(rev_val or 0) * 100)
    except Exception:
        series_query_ok = False
        log.exception("scoreboard_rollup: series query failed, sparkline will be empty")

    # Use CT wall-clock date to match the CH query's toDate(toTimeZone(..., 'America/Chicago'))
    today_ct = datetime.now(ZoneInfo("America/Chicago")).date()
    series: list[int] = []
    if series_query_ok:
        for offset in range(7, 0, -1):
            d = today_ct - timedelta(days=offset)
            series.append(series_map.get(d, 0))
        series.append(int(float(rev_today or 0) * 100))

    # ── EOD pace projection ──
    # Linear extrapolation: cents/second × 86400. Suppressed before the first
    # hour (elapsed < 3600s) — too volatile and not actionable at 12:05am CT.
    today_rev_cents = int(float(rev_today or 0) * 100)
    now_ct_for_proj = datetime.now(ZoneInfo("America/Chicago"))
    midnight_ct     = now_ct_for_proj.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_s       = (now_ct_for_proj - midnight_ct).total_seconds()
    eod_projection  = int(today_rev_cents / elapsed_s * 86400) if elapsed_s >= 3600 else 0

    return ScoreboardRollup(
        revenue_today_cents=today_rev_cents,
        revenue_yesterday_same_time_cents=int(float(rev_yest or 0) * 100),
        revenue_7d_avg_cents=int(float(rev_7d_avg or 0) * 100),
        conversions_today=int(conv_today or 0),
        conversions_yesterday_same_time=int(conv_yest or 0),
        conversions_7d_avg=int(float(conv_7d_avg or 0)),
        winners=winners,
        worry=worry,
        revenue_7d_series=series,
        revenue_eod_projection_cents=eod_projection,
        revenue_mtd_cents=int(float(rev_mtd or 0) * 100),
        generated_at=datetime.utcnow(),
    )

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

def ghost_campaigns(ch, recency_hours: int = 48, as_of_date: str | None = None) -> list[dict]:
    """
    Canonical ghost campaign detection — single source of truth for the
    agent tool (get_ghost_campaigns) and the ghost monitor daemon.

    A campaign qualifies as a ghost if ALL of:
    - status = 'Active', non-expired
    - 5,000+ impressions in last 7 days (meaningful traffic volume)
    - 2,000+ impressions in last recency_hours (actively burning inventory RIGHT NOW)
    - 200+ clicks in 7 days (real engagement, not just display)
    - Zero conversions in 7 days (broken tracking or non-converting offer)
    - Campaign age > 7 days (excludes new launches still warming up)
    - conversion_events configured (CPA/CPS only — excludes CPM/CPC by design)

    recency_hours: rolling window for "actively burning right now" check. Default 48.
    Configured via signals.ghost_recency_hours in config/scout_thresholds.json.

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
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(subtractHours(now(), {recency_hours:UInt32}))
    WHERE created_at >= subtractHours(now(), {recency_hours:UInt32})
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
    if as_of_date:
        _ref = f"toDate('{as_of_date}')"
        sql = sql.replace("today()", _ref)
    rows = ch.query(sql, parameters={"recency_hours": recency_hours}).result_rows
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
        -- mv_adpx_users is a lightweight MV (id, organization, is_test, parent_id only)
        -- — prefer over from_airbyte_users for simple name lookups.
        SELECT s.user_id, coalesce(u.organization, '') AS organization, count() AS sessions_30d
        FROM adpx_sdk_sessions s
        LEFT JOIN mv_adpx_users u ON s.user_id = u.id
        WHERE s.created_at >= today() - 30
          AND toYYYYMM(s.created_at) >= toYYYYMM(today() - 30)
          AND s.user_id NOT IN {active_ids: Array(Int64)}
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
            -- mv_adpx_users is a lightweight MV (id, organization, is_test, parent_id only)
            -- — prefer over from_airbyte_users for simple name lookups.
            SELECT
                toInt64(s.user_id)              AS publisher_id,
                coalesce(u.organization, '')    AS publisher_name,
                count()                         AS sessions_30d
            FROM adpx_sdk_sessions s
            LEFT JOIN mv_adpx_users u ON s.user_id = u.id
            WHERE toYYYYMM(s.created_at) >= toYYYYMM(today() - 30)
              AND s.created_at >= today() - 30
            GROUP BY s.user_id, coalesce(u.organization, '')
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
# CVR anomaly detection
# ===========================================================================

def cvr_anomaly(
    ch,
    drop_pct: float = 30.0,
    min_payout: float = 50.0,
    min_impressions_7d: int = 5000,
) -> list[dict]:
    """
    Publisher-campaign pairs where yesterday's CVR dropped significantly vs. 7d baseline.

    CVR = conversions / impressions (impressions are the exposure unit, not clicks).
    Only fires on high-value campaigns (avg_payout >= min_payout) with enough volume
    (impressions_7d >= min_impressions_7d) to make the signal actionable.

    Returns list of dicts with keys:
        publisher_id (int), publisher_name (str), campaign_id (int), adv_name (str),
        exposure_cvr_7d (float), cvr_yesterday (float), delta_pct (float),
        impressions_7d (int), payout_per_conversion (float)
    Raises on ClickHouse error — callers must catch.
    """
    rows = ch.query(
        """
        WITH imp_7d AS (
                -- adpx_impressions_details.pid is the publisher ID (string) —
                -- no session join needed; eliminates the FillingRightJoinSide OOM.
                SELECT
                    toUInt64OrZero(pid)               AS publisher_id,
                    campaign_id,
                    count()                           AS impressions_7d
                FROM adpx_impressions_details
                WHERE toYYYYMM(created_at) >= toYYYYMM(today() - INTERVAL 8 DAY)
                  AND created_at >= today() - INTERVAL 7 DAY
                GROUP BY publisher_id, campaign_id
                HAVING impressions_7d >= {min_impressions_7d: Int64}
            ),
            imp_yesterday AS (
                SELECT
                    toUInt64OrZero(pid)               AS publisher_id,
                    campaign_id,
                    count()                           AS impressions_yesterday
                FROM adpx_impressions_details
                WHERE toYYYYMM(created_at) >= toYYYYMM(yesterday())
                  AND created_at >= yesterday()
                  AND created_at < today()
                GROUP BY publisher_id, campaign_id
            ),
            conv_7d AS (
                -- adpx_conversionsdetails.user_id is already the publisher ID (UInt64).
                SELECT
                    user_id                           AS publisher_id,
                    campaign_id,
                    count()                           AS conversions_7d,
                    avg(toFloat64OrNull(payout))       AS payout_per_conversion
                FROM adpx_conversionsdetails
                WHERE toYYYYMM(created_at) >= toYYYYMM(today() - INTERVAL 8 DAY)
                  AND created_at >= today() - INTERVAL 7 DAY
                GROUP BY publisher_id, campaign_id
                HAVING payout_per_conversion >= {min_payout: Float64}
            ),
            conv_yesterday AS (
                SELECT
                    user_id                           AS publisher_id,
                    campaign_id,
                    count()                           AS conversions_yesterday
                FROM adpx_conversionsdetails
                WHERE toYYYYMM(created_at) >= toYYYYMM(yesterday())
                  AND created_at >= yesterday()
                  AND created_at < today()
                GROUP BY publisher_id, campaign_id
            ),
            names AS (
                SELECT
                    id                                AS publisher_id,
                    organization                      AS publisher_name
                FROM mv_adpx_users
            ),
            campaigns AS (
                SELECT
                    id                                AS campaign_id,
                    adv_name
                FROM from_airbyte_campaigns
                WHERE deleted_at IS NULL
            ),
            final AS (
                SELECT
                    i7.publisher_id                                       AS publisher_id,
                    coalesce(n.publisher_name, toString(i7.publisher_id)) AS publisher_name,
                    toInt64(i7.campaign_id)                               AS campaign_id,
                    coalesce(ca.adv_name, '')                             AS adv_name,
                    round(coalesce(c7.conversions_7d, 0) /
                          nullIf(i7.impressions_7d, 0), 6)                AS exposure_cvr_7d,
                    round(coalesce(cy.conversions_yesterday, 0) /
                          nullIf(iy.impressions_yesterday, 0), 6)         AS cvr_yesterday,
                    round((coalesce(cy.conversions_yesterday, 0) /
                          nullIf(iy.impressions_yesterday, 0) -
                          coalesce(c7.conversions_7d, 0) /
                          nullIf(i7.impressions_7d, 0)) /
                          nullIf(coalesce(c7.conversions_7d, 0) /
                          nullIf(i7.impressions_7d, 0), 0) * 100, 2)     AS delta_pct,
                    i7.impressions_7d,
                    coalesce(c7.payout_per_conversion, 0)                 AS payout_per_conversion
                FROM imp_7d i7
                LEFT JOIN imp_yesterday iy
                       ON iy.publisher_id = i7.publisher_id
                      AND iy.campaign_id  = i7.campaign_id
                LEFT JOIN conv_7d c7
                       ON c7.publisher_id = i7.publisher_id
                      AND c7.campaign_id  = i7.campaign_id
                LEFT JOIN conv_yesterday cy
                       ON cy.publisher_id = i7.publisher_id
                      AND cy.campaign_id  = i7.campaign_id
                LEFT JOIN names n ON n.publisher_id = i7.publisher_id
                LEFT JOIN campaigns ca ON ca.campaign_id = toInt64(i7.campaign_id)
            )
        SELECT *
        FROM final
        WHERE exposure_cvr_7d > 0
          AND delta_pct <= -{drop_pct: Float64}
        ORDER BY delta_pct ASC
        """,
        parameters={
            "min_impressions_7d": int(min_impressions_7d),
            "min_payout": float(min_payout),
            "drop_pct": float(drop_pct),
        },
    ).result_rows
    return [
        {
            "publisher_id": int(r[0]),
            "publisher_name": r[1],
            "campaign_id": int(r[2]),
            "adv_name": r[3],
            "exposure_cvr_7d": float(r[4] or 0),
            "cvr_yesterday": float(r[5] or 0),
            "delta_pct": float(r[6] or 0),
            "impressions_7d": int(r[7] or 0),
            "payout_per_conversion": float(r[8] or 0),
        }
        for r in rows
    ]


# ===========================================================================
# Expiring campaigns
# ===========================================================================

def expiring_campaigns(ch, warning_days: int = 7) -> list[dict]:
    """
    Active campaigns expiring within warning_days days.

    Includes last-7d impression/publisher activity and revenue so callers can
    surface only campaigns that are actively generating revenue.

    Returns list of dicts with keys:
        campaign_id (int), adv_name (str), end_date (str, YYYY-MM-DD),
        days_remaining (int), impressions_7d (int), publisher_count (int),
        revenue_7d (float)
    Raises on ClickHouse error — callers must catch.
    """
    rows = ch.query(
        """
        WITH expiring_raw AS (
            SELECT
                id                                              AS campaign_id,
                adv_name,
                toDate(end_date)                                AS end_date_dt
            FROM from_airbyte_campaigns
            WHERE toDate(end_date) BETWEEN today() AND today() + INTERVAL {warning_days: Int32} DAY
              AND trim(status) = 'Active'
              AND deleted_at IS NULL
        ),
        expiring AS (
            SELECT
                campaign_id,
                adv_name,
                toString(end_date_dt)                           AS end_date,
                dateDiff('day', today(), end_date_dt)           AS days_remaining
            FROM expiring_raw
        ),
        imp_agg AS (
            -- pid is the publisher ID (string) — no session join needed.
            SELECT
                toInt64(campaign_id)                      AS campaign_id,
                count()                                   AS impressions_7d,
                count(DISTINCT toUInt64OrZero(pid))       AS publisher_count
            FROM adpx_impressions_details
            WHERE toYYYYMM(created_at) >= toYYYYMM(today() - INTERVAL 8 DAY)
              AND created_at >= today() - INTERVAL 7 DAY
            GROUP BY campaign_id
        ),
        rev_agg AS (
            SELECT
                toInt64(cv.campaign_id)               AS campaign_id,
                round(sum(toFloat64OrNull(cv.revenue)), 2) AS revenue_7d
            FROM adpx_conversionsdetails cv
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL 8 DAY)
              AND cv.created_at >= today() - INTERVAL 7 DAY
            GROUP BY campaign_id
        )
        SELECT
            e.campaign_id,
            e.adv_name,
            e.end_date,
            e.days_remaining,
            coalesce(ia.impressions_7d, 0)     AS impressions_7d,
            coalesce(ia.publisher_count, 0)    AS publisher_count,
            coalesce(ra.revenue_7d, 0)         AS revenue_7d
        FROM expiring e
        LEFT JOIN imp_agg ia ON ia.campaign_id = e.campaign_id
        LEFT JOIN rev_agg ra ON ra.campaign_id = e.campaign_id
        ORDER BY e.days_remaining ASC, revenue_7d DESC
        """,
        parameters={"warning_days": int(warning_days)},
    ).result_rows
    return [
        {
            "campaign_id": int(r[0]),
            "adv_name": r[1],
            "end_date": r[2],
            "days_remaining": int(r[3]),
            "impressions_7d": int(r[4] or 0),
            "publisher_count": int(r[5] or 0),
            "revenue_7d": float(r[6] or 0),
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


# ===========================================================================
# Revenue trends (publisher and advertiser)
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
        """
        WITH actual AS (
            -- adpx_conversionsdetails.user_id is the publisher ID — no session join needed.
            SELECT
                cv.user_id                                             AS publisher_id,
                round(sum(toFloat64OrNull(cv.revenue)), 2)            AS revenue_actual,
                count(DISTINCT cv.session_id)                         AS sessions_actual
            FROM adpx_conversionsdetails cv
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL {days: Int32} DAY)
              AND cv.created_at >= today() - INTERVAL {days: Int32} DAY
            GROUP BY publisher_id
        ),
        historical_daily AS (
            SELECT
                cv.user_id                                             AS publisher_id,
                toDate(cv.created_at, 'America/Chicago')              AS rev_date,
                round(sum(toFloat64OrNull(cv.revenue)), 2)            AS daily_revenue,
                intDiv(dateDiff('day',
                       toDate(now(), 'America/Chicago') - INTERVAL {lookback: Int32} DAY,
                       rev_date),
                       {days: Int32}) + 1                              AS period_idx
            FROM adpx_conversionsdetails cv
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL {lookback: Int32} DAY)
              AND cv.created_at >= today() - INTERVAL {lookback: Int32} DAY
              AND cv.created_at < today() - INTERVAL {days: Int32} DAY
            GROUP BY publisher_id, rev_date
        ),
        historical_periods AS (
            SELECT
                publisher_id,
                period_idx,
                sum(daily_revenue)                                     AS period_revenue
            FROM historical_daily
            WHERE period_idx BETWEEN 1 AND 8
            GROUP BY publisher_id, period_idx
        ),
        baseline AS (
            SELECT
                publisher_id,
                round(median(period_revenue), 2)                       AS revenue_expected,
                count()                                                AS period_count
            FROM historical_periods
            GROUP BY publisher_id
            HAVING period_count >= {min_periods: Int32}
        ),
        names AS (
            SELECT id AS publisher_id, organization AS publisher_name
            FROM mv_adpx_users
        )
        SELECT
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
        ORDER BY delta_pct ASC
        """,
        parameters={
            "days": int(days),
            "lookback": int(days * 9),
            "min_periods": int(min_periods),
        },
    ).result_rows

    def _trend(delta: float) -> str:
        if delta <= -15:
            return "down"
        if delta >= 15:
            return "up"
        return "flat"

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


def advertiser_revenue_trends(ch, days: int = 7, min_periods: int = 4) -> list[dict]:
    """
    Advertiser revenue trends: same period-median algorithm as publisher_revenue_trends
    but aggregated by adv_name across all publishers.

    Returns list of dicts with keys:
        adv_name (str), revenue_actual (float), revenue_expected (float),
        delta_pct (float), trend ("up" | "down" | "flat"), conversions_actual (int)
    Raises on ClickHouse error — callers must catch.
    """
    rows = ch.query(
        """
        WITH actual AS (
            SELECT
                c.adv_name,
                round(sum(toFloat64OrNull(cv.revenue)), 2)            AS revenue_actual,
                count()                                               AS conversions_actual
            FROM adpx_conversionsdetails cv
            JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL {days: Int32} DAY)
              AND cv.created_at >= today() - INTERVAL {days: Int32} DAY
            GROUP BY c.adv_name
        ),
        historical_daily AS (
            SELECT
                c.adv_name,
                toDate(cv.created_at, 'America/Chicago')              AS rev_date,
                round(sum(toFloat64OrNull(cv.revenue)), 2)            AS daily_revenue,
                intDiv(dateDiff('day',
                       toDate(now(), 'America/Chicago') - INTERVAL {lookback: Int32} DAY,
                       rev_date),
                       {days: Int32}) + 1                              AS period_idx
            FROM adpx_conversionsdetails cv
            JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL {lookback: Int32} DAY)
              AND cv.created_at >= today() - INTERVAL {lookback: Int32} DAY
              AND cv.created_at < today() - INTERVAL {days: Int32} DAY
            GROUP BY c.adv_name, rev_date
        ),
        historical_periods AS (
            SELECT
                adv_name,
                period_idx,
                sum(daily_revenue)                                     AS period_revenue
            FROM historical_daily
            WHERE period_idx BETWEEN 1 AND 8
            GROUP BY adv_name, period_idx
        ),
        baseline AS (
            SELECT
                adv_name,
                round(median(period_revenue), 2)                       AS revenue_expected,
                count()                                                AS period_count
            FROM historical_periods
            GROUP BY adv_name
            HAVING period_count >= {min_periods: Int32}
        )
        SELECT
            b.adv_name,
            coalesce(a.revenue_actual, 0)                             AS revenue_actual,
            b.revenue_expected,
            round((coalesce(a.revenue_actual, 0) - b.revenue_expected) /
                  nullIf(b.revenue_expected, 0) * 100, 1)             AS delta_pct,
            coalesce(a.conversions_actual, 0)                         AS conversions_actual
        FROM baseline b
        LEFT JOIN actual a ON a.adv_name = b.adv_name
        ORDER BY delta_pct ASC
        """,
        parameters={
            "days": int(days),
            "lookback": int(days * 9),
            "min_periods": int(min_periods),
        },
    ).result_rows

    def _trend(delta: float) -> str:
        if delta <= -15:
            return "down"
        if delta >= 15:
            return "up"
        return "flat"

    return [
        {
            "adv_name": r[0],
            "revenue_actual": float(r[1] or 0),
            "revenue_expected": float(r[2] or 0),
            "delta_pct": float(r[3] or 0),
            "trend": _trend(float(r[3] or 0)),
            "conversions_actual": int(r[4] or 0),
        }
        for r in rows
    ]


# ===========================================================================
# Tier 4 baseline CVR — network-agnostic fallback for unknown advertisers
# ===========================================================================

def benchmark_overall_cvr(ch) -> dict:
    """
    Aggregate CVR + RPM across ALL MS campaigns (500+ impressions, since 2025-01).
    Used as Tier 4 (lowest-confidence) fallback in _scout_score() when an offer has
    no offer/advertiser/category match — e.g. every MaxBounty and FlexOffers offer.

    Returns {"cvr_pct": float, "rpm": float, "campaigns": int} or {} on error.

    Empirically measured 2026-05-19: 0.4514% CVR, $40.29 RPM across 670 campaigns.
    The live query keeps this fresh as MS's book of business evolves.
    """
    try:
        rows = ch.query(
            """
            WITH conv AS (
                SELECT campaign_id,
                       count()                          AS conversions,
                       sum(toFloat64OrNull(revenue))    AS revenue
                FROM adpx_conversionsdetails
                WHERE toYYYYMM(created_at) >= 202501
                GROUP BY campaign_id
            ),
            imp AS (
                SELECT campaign_id, count() AS impressions
                FROM adpx_impressions_details
                WHERE toYYYYMM(created_at) >= 202501
                GROUP BY campaign_id
            )
            SELECT
                count(DISTINCT c.id)                                              AS campaigns,
                round(sum(conv.conversions) / sum(imp.impressions) * 100, 4)     AS cvr_pct,
                round(sum(conv.revenue)     / sum(imp.impressions) * 1000, 2)    AS rpm
            FROM from_airbyte_campaigns c
            JOIN conv ON toInt64(c.id) = toInt64(conv.campaign_id)
            JOIN imp  ON toInt64(c.id) = toInt64(imp.campaign_id)
            WHERE c.deleted_at IS NULL
              AND imp.impressions > 500
            """
        ).result_rows
        if rows and rows[0][0]:
            campaigns, cvr_pct, rpm = rows[0]
            return {"cvr_pct": float(cvr_pct or 0), "rpm": float(rpm or 0), "campaigns": int(campaigns or 0)}
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"benchmark_overall_cvr failed: {e}")
    return {}


# ===========================================================================
# Canonical query contracts (ghost pattern — one function, both signal + NL)
# ===========================================================================

def fill_rate_publishers(
    ch,
    as_of_date=None,
    window_days: int = 7,
    min_sessions: int = 2500,
    threshold_pct: float = 15.0,
    placements=None,
    entity_overrides=None,
    limit: int = 5,
) -> list[dict]:
    """Canonical fill-rate alert query. One function, both signal path and NL path.

    Returns publishers where fill_rate < threshold_pct over the last window_days days.
    Fill rate = (sessions_with_impressions / sessions) * 100.

    Replaces:
      - scout_bot._pulse_signal_fill_rate   — 7d / 2500, f-string placements, from_airbyte_users
      - queries.low_fill_publishers         — 30d / 10000, kept for diagnostic wide-window queries

    Canonical values (from config/scout_thresholds.json):
      window_days=7, min_sessions=2500, threshold_pct=15.0

    Args:
        ch:               ClickHouse client.
        as_of_date:       "YYYY-MM-DD" or None → uses today() in ClickHouse.
        window_days:      Lookback window in days (default 7).
        min_sessions:     Minimum session count to include a publisher (default 2500).
        threshold_pct:    Fill rate below this % triggers the alert (default 15.0).
        placements:       List of placement strings to filter sessions.
                          If None, loads _POST_TX_PLACEMENTS from scout_agent at call time.
        entity_overrides: Dict of {publisher_name: {flag: value}} overrides.
                          If None, loads from scout_state._load_entity_overrides() at call time.
                          Note: loaded inside function body — NOT a default arg — to avoid
                          the mutable-default anti-pattern and to stay testable.
        limit:            Maximum number of publishers to return (default 5).

    Returns:
        List of dicts with keys:
          publisher_id (int), publisher_name (str), sessions_7d (int),
          fill_rate_pct (float), missed_sessions (int)
    """
    if entity_overrides is None:
        try:
            from scout_state import _load_entity_overrides
            entity_overrides = _load_entity_overrides().get("publishers", {})
        except Exception:
            entity_overrides = {}
    if placements is None:
        try:
            from scout_agent import _POST_TX_PLACEMENTS
            placements = list(_POST_TX_PLACEMENTS)
        except Exception:
            placements = []

    date_expr = f"toDate('{as_of_date}')" if as_of_date else "today()"

    try:
        rows = ch.query(
            f"""
            SELECT
                s.user_id                                                   AS publisher_id,
                any(u.organization)                                         AS publisher_name,
                count() AS sessions_{window_days}d,
                coalesce(i.sessions_with_imps, 0)                          AS sessions_with_imps,
                round(
                    coalesce(i.sessions_with_imps, 0) * 100.0 / count(), 2
                )                                                           AS fill_rate_pct,
                count() - coalesce(i.sessions_with_imps, 0)                AS missed_sessions
            FROM adpx_sdk_sessions s
            -- mv_adpx_users columns: id (UInt64), organization — NOT pid/name
            LEFT JOIN mv_adpx_users u ON u.id = toUInt64(s.user_id)
            LEFT JOIN (
                SELECT toInt64(pid) AS user_id, count() AS sessions_with_imps
                FROM adpx_impressions_details
                WHERE toDate(created_at) > {date_expr} - {window_days}
                  AND placement IN {{placements: Array(String)}}
                GROUP BY pid
            ) i ON toInt64(i.user_id) = toInt64(s.user_id)
            WHERE toDate(s.created_at) > {date_expr} - {window_days}
              AND s.placement IN {{placements: Array(String)}}
            GROUP BY s.user_id, i.sessions_with_imps
            HAVING sessions_{window_days}d > {{min_sessions: UInt64}}
               AND fill_rate_pct < {{threshold_pct: Float64}}
            ORDER BY fill_rate_pct ASC
            LIMIT {{limit: UInt64}}
            """,
            parameters={
                "placements": placements,
                "min_sessions": min_sessions,
                "threshold_pct": threshold_pct,
                "limit": limit,
            },
        ).result_rows
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"fill_rate_publishers failed: {e}")
        return []

    results = []
    for row in rows:
        pub_id, pub_name, sessions, _, fill_rate, missed = row
        name = pub_name or str(pub_id)
        # Entity override check: skip publishers explicitly excluded from fill-rate alerts.
        override = entity_overrides.get(name, {})
        if override.get("exclude_from_fill_rate"):
            continue
        results.append(
            {
                "publisher_id": int(pub_id),
                "publisher_name": name,
                f"sessions_{window_days}d": int(sessions),
                "fill_rate_pct": float(fill_rate),
                "missed_sessions": int(missed),
            }
        )

    return results


def velocity_alerts(
    ch,
    as_of_date=None,
    down_threshold_pct: float = -25.0,
    up_threshold_pct: float = 20.0,
    min_rev_30d: float = 5000.0,
    publisher_id=None,
) -> list[dict]:
    """Canonical velocity alert query. One function, both signal path and NL path.

    Computes annualised velocity for each publisher and returns those whose pace has
    moved beyond the configured thresholds (down OR up).

    Formula:
        pct_delta = ((rev_7d / 7) * 30 - rev_30d) / rev_30d * 100

    Replaces:
      - scout_bot._pulse_signal_velocity   — annualised, inline SQL, up+down, two-phase
      - queries.publisher_revenue_trends   — period-median, kept for analytical trend charts

    Canonical values (from config/scout_thresholds.json):
        down_threshold_pct=-25.0, up_threshold_pct=20.0, min_rev_30d=5000.0

    Args:
        ch:                 ClickHouse client.
        as_of_date:         "YYYY-MM-DD" or None → uses today() in ClickHouse.
        down_threshold_pct: Fire when pct_delta <= this value (default -25.0, i.e. ≥25% drop).
        up_threshold_pct:   Fire when pct_delta >= this value (default 20.0).
        min_rev_30d:        Minimum 30-day revenue to include a publisher (default $5,000).
        publisher_id:       Optional int — narrow to a single publisher.

    Returns:
        List of dicts (top 5 by abs(pct_delta), descending), each with keys:
          publisher_id (int), publisher_name (str), rev_30d (float), rev_7d (float),
          pct_delta (float), direction ("up"|"down"),
          advertisers (list[dict]) — top advertisers from Phase 2 attribution enrichment.
    """
    date_expr = f"toDate('{as_of_date}')" if as_of_date else "today()"
    pub_filter = f"AND toInt64(user_id) = {int(publisher_id)}" if publisher_id else ""

    try:
        rows = ch.query(
            f"""
            SELECT
                toInt64(user_id)                                                AS publisher_id,
                any(u.organization)                                             AS publisher_name,
                coalesce(sum(toFloat64OrZero(revenue)), 0)                      AS revenue_30d,
                coalesce(sumIf(
                    toFloat64OrZero(revenue),
                    toDate(created_at) > {date_expr} - 7
                ), 0)                                                           AS revenue_7d
            FROM adpx_conversionsdetails cv
            -- mv_adpx_users columns: id (UInt64), organization — NOT pid/name
            LEFT JOIN mv_adpx_users u ON u.id = toUInt64(cv.user_id)
            WHERE toDate(cv.created_at) > {date_expr} - 30
              {pub_filter}
            GROUP BY user_id
            HAVING revenue_30d >= {{min_rev_30d: Float64}}
            ORDER BY revenue_30d DESC
            LIMIT 200
            """,
            parameters={"min_rev_30d": min_rev_30d},
        ).result_rows
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"velocity_alerts phase-1 failed: {e}")
        return []

    candidates = []
    for row in rows:
        pub_id, pub_name, rev_30d, rev_7d = row
        if rev_30d <= 0:
            continue
        rev_7d_ann = (float(rev_7d) / 7) * 30
        pct_delta = (rev_7d_ann - float(rev_30d)) / float(rev_30d) * 100
        if down_threshold_pct < pct_delta < up_threshold_pct:
            continue  # within normal range — skip
        direction = "up" if pct_delta > 0 else "down"
        candidates.append(
            {
                "publisher_id": int(pub_id),
                "publisher_name": pub_name or str(pub_id),
                "rev_30d": round(float(rev_30d), 2),
                "rev_7d": round(float(rev_7d), 2),
                "pct_delta": round(pct_delta, 1),
                "direction": direction,
                "advertisers": [],
            }
        )

    # Top 5 by absolute delta magnitude.
    candidates.sort(key=lambda x: abs(x["pct_delta"]), reverse=True)
    candidates = candidates[:5]
    if not candidates:
        return []

    # Phase 2 — advertiser attribution enrichment for the top candidates.
    # Filter to candidates with |pct_delta| >= 100 (meaningful enough to warrant attribution).
    enrich_ids = [c["publisher_id"] for c in candidates if abs(c["pct_delta"]) >= 100]
    if enrich_ids:
        pub_id_csv = ", ".join(str(pid) for pid in enrich_ids)
        try:
            adv_rows = ch.query(
                f"""
                SELECT
                    toInt64(cv.user_id)                                     AS publisher_id,
                    c.adv_name,
                    coalesce(sum(toFloat64OrZero(cv.revenue)), 0)           AS rev_30d,
                    coalesce(sumIf(
                        toFloat64OrZero(cv.revenue),
                        toDate(cv.created_at) > {date_expr} - 7
                    ), 0)                                                   AS rev_7d
                FROM adpx_conversionsdetails cv
                JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = toInt64(c.id)
                WHERE toDate(cv.created_at) > {date_expr} - 30
                  AND toInt64(cv.user_id) IN ({pub_id_csv})
                  AND c.deleted_at IS NULL
                GROUP BY cv.user_id, c.adv_name
                HAVING rev_30d > 0
                ORDER BY rev_30d DESC
                """
            ).result_rows
            # Group by publisher.
            adv_by_pub: dict = {}
            for adv_row in adv_rows:
                pid_adv, adv_name, r30, r7 = adv_row
                adv_by_pub.setdefault(int(pid_adv), []).append(
                    {"advertiser": adv_name, "rev_30d": round(float(r30), 2), "rev_7d": round(float(r7), 2)}
                )
            for c in candidates:
                c["advertisers"] = adv_by_pub.get(c["publisher_id"], [])[:5]
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"velocity_alerts phase-2 advertiser enrichment failed: {e}")

    return candidates


def cap_alert_campaigns(
    ch,
    as_of_date=None,
    cap_alert_pct: float = 85.0,
    advertiser_id=None,
) -> list[dict]:
    """Canonical cap-alert query. One function, signal path (and future NL path).

    Returns campaigns whose MTD revenue has reached cap_alert_pct% of their monthly budget.

    Replaces:
      - scout_bot._pulse_signal_cap — inline SQL + Python capping_config JSON parsing

    Canonical value (from config/scout_thresholds.json):
        cap_alert_pct=85.0

    Args:
        ch:             ClickHouse client.
        as_of_date:     "YYYY-MM-DD" or None → uses today() in ClickHouse.
        cap_alert_pct:  Alert threshold as a percentage (default 85.0).
        advertiser_id:  Optional int — narrow to a single advertiser campaign ID.

    Returns:
        List of dicts, each with keys:
          adv_name (str), campaign_id (int), monthly_cap (float), revenue_mtd (float),
          cap_pct (float), days_remaining (int), days_to_cap (float|None)
    """
    import json as _json
    import datetime as _dt

    date_expr = f"toDate('{as_of_date}')" if as_of_date else "today()"
    adv_filter = f"AND c.id = {int(advertiser_id)}" if advertiser_id else ""

    try:
        rows = ch.query(
            f"""
            SELECT
                c.id                                                        AS campaign_id,
                c.adv_name,
                c.capping_config,
                coalesce(sum(toFloat64OrZero(cv.revenue)), 0)               AS revenue_mtd
            FROM from_airbyte_campaigns c
            LEFT JOIN adpx_conversionsdetails cv
                ON toInt64(cv.campaign_id) = toInt64(c.id)
               AND toYYYYMM(toDate(cv.created_at)) = toYYYYMM({date_expr})
            WHERE c.deleted_at IS NULL
              AND c.capping_config IS NOT NULL
              AND c.capping_config != ''
              AND c.capping_config != '{{}}'
              {adv_filter}
            GROUP BY c.id, c.adv_name, c.capping_config
            HAVING revenue_mtd > 0
            ORDER BY revenue_mtd DESC
            """
        ).result_rows
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"cap_alert_campaigns failed: {e}")
        return []

    today = _dt.date.fromisoformat(as_of_date) if as_of_date else _dt.date.today()
    days_in_month = (today.replace(day=28) + _dt.timedelta(days=4)).replace(day=1) - _dt.timedelta(days=1)
    days_remaining = (days_in_month - today).days + 1

    results = []
    for row in rows:
        campaign_id, adv_name, cap_cfg, revenue_mtd = row
        try:
            cfg = _json.loads(cap_cfg)
            month_cfg = cfg.get("month") or cfg.get("monthly") or {}
            mb = float(month_cfg.get("budget") or 0)
        except Exception:
            continue
        if mb <= 0:
            continue
        cap_pct = revenue_mtd / mb
        if cap_pct < cap_alert_pct / 100:
            continue

        daily_run_rate = revenue_mtd / max(today.day, 1)
        days_to_cap = (mb - revenue_mtd) / daily_run_rate if daily_run_rate > 0 else None

        results.append(
            {
                "adv_name": adv_name,
                "campaign_id": int(campaign_id),
                "monthly_cap": round(mb, 2),
                "revenue_mtd": round(float(revenue_mtd), 2),
                "cap_pct": round(cap_pct * 100, 1),
                "days_remaining": int(days_remaining),
                "days_to_cap": round(days_to_cap, 1) if days_to_cap is not None else None,
            }
        )

    results.sort(key=lambda x: x["cap_pct"], reverse=True)
    return results


def earnings_breakdown(
    ch,
    start_date: str,
    end_date: str,
    publisher_id=None,
) -> dict:
    """Returns Earnings and its three components for a publisher (or fleet-wide) over a date range.

    Earnings = Gross Revenue - Partner Revenue + Partner Cost
    IMPORTANT: the formula uses +Partner Cost (not minus).
    The Notion Custom Reports doc is WRONG — it omits the +Partner Cost term.

    All three source tables are filtered by user_id for correct partner attribution.
    See attribution rule: use user_id, never pid, for traffic-publisher attribution.

    Args:
        ch:           ClickHouse client.
        start_date:   "YYYY-MM-DD" inclusive start date.
        end_date:     "YYYY-MM-DD" inclusive end date.
        publisher_id: Optional int. If None, returns fleet-wide (all publishers).

    Returns:
        Dict with keys:
          gross_rev (float), partner_rev (float), partner_cost (float), earnings (float)
        Returns all-zero dict on query failure (logged as warning).
    """
    pub_filter_cv = f"AND toInt64(user_id) = {int(publisher_id)}" if publisher_id else ""
    pub_filter_tc = f"AND toInt64(user_id) = {int(publisher_id)}" if publisher_id else ""

    try:
        rev_rows = ch.query(
            f"""
            SELECT
                coalesce(sum(toFloat64OrZero(revenue)), 0) AS gross_rev,
                coalesce(sum(toFloat64OrZero(payout)), 0)  AS partner_rev
            FROM adpx_conversionsdetails
            WHERE toDate(created_at) BETWEEN {{start_date: String}} AND {{end_date: String}}
              {pub_filter_cv}
            """,
            parameters={"start_date": start_date, "end_date": end_date},
        ).result_rows

        cost_rows = ch.query(
            f"""
            SELECT coalesce(sum(pub_cost_cents) / 100.0, 0) AS partner_cost
            FROM adpx_tracked_clicks
            WHERE toDate(created_at) BETWEEN {{start_date: String}} AND {{end_date: String}}
              {pub_filter_tc}
            """,
            parameters={"start_date": start_date, "end_date": end_date},
        ).result_rows
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"earnings_breakdown failed: {e}")
        return {"gross_rev": 0.0, "partner_rev": 0.0, "partner_cost": 0.0, "earnings": 0.0}

    gross_rev = float(rev_rows[0][0]) if rev_rows else 0.0
    partner_rev = float(rev_rows[0][1]) if rev_rows else 0.0
    partner_cost = float(cost_rows[0][0]) if cost_rows else 0.0
    earnings = gross_rev - partner_rev + partner_cost  # +partner_cost — NOT minus

    return {
        "gross_rev": round(gross_rev, 4),
        "partner_rev": round(partner_rev, 4),
        "partner_cost": round(partner_cost, 4),
        "earnings": round(earnings, 4),
    }
