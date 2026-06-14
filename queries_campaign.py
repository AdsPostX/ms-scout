"""ClickHouse queries — campaign entry, status, ghost detection, expiring campaigns."""
from __future__ import annotations

import re

from queries_revenue import _ch_date_filter


def _validate_as_of_date(as_of_date: str) -> None:
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', as_of_date):
        raise ValueError(f"as_of_date must be YYYY-MM-DD, got: {as_of_date!r}")


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
    sql = f"""
WITH imp_agg AS (
    SELECT campaign_id, count() AS impressions_7d, min(created_at)::Date AS first_impression_date
    FROM adpx_impressions_details
    PREWHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(7)})
    WHERE created_at >= {_ch_date_filter(7)}
    GROUP BY campaign_id
    HAVING impressions_7d > 5000
),
recent_imp AS (
    SELECT campaign_id, count() AS impressions_2d
    FROM adpx_impressions_details
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(subtractHours(now(), {{recency_hours:UInt32}}))
    WHERE created_at >= subtractHours(now(), {{recency_hours:UInt32}})
    GROUP BY campaign_id
    HAVING impressions_2d >= 2000
),
click_agg AS (
    SELECT campaign_id, count() AS clicks_7d
    FROM adpx_tracked_clicks
    PREWHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(7)})
    WHERE created_at >= {_ch_date_filter(7)}
    GROUP BY campaign_id
    HAVING clicks_7d > 100  -- pre-filter; actual 200-click requirement in final HAVING
),
rev_agg AS (
    SELECT campaign_id,
           coalesce(sum(toFloat64OrNull(revenue)), 0) AS revenue_7d,
           count()                                    AS conversion_count_7d
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(7)})
    WHERE created_at >= {_ch_date_filter(7)}
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
  AND ia.first_impression_date <= {_ch_date_filter(7)}
  AND c.deleted_at IS NULL
GROUP BY c.id, c.adv_name, c.title, ia.impressions_7d, ri.impressions_2d, ca.clicks_7d, revenue_7d, ia.first_impression_date
HAVING impressions_7d > 5000 AND clicks_7d > 200
ORDER BY impressions_7d DESC
LIMIT 25
"""
    if as_of_date:
        _validate_as_of_date(as_of_date)
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
        f"""
        WITH expiring_raw AS (
            SELECT
                id                                              AS campaign_id,
                adv_name,
                toDate(end_date)                                AS end_date_dt
            FROM from_airbyte_campaigns
            WHERE toDate(end_date) BETWEEN today() AND today() + INTERVAL {{warning_days: Int32}} DAY
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
            WHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(8)})
              AND created_at >= {_ch_date_filter(7)}
            GROUP BY campaign_id
        ),
        rev_agg AS (
            SELECT
                toInt64(cv.campaign_id)               AS campaign_id,
                round(sum(toFloat64OrNull(cv.revenue)), 2) AS revenue_7d
            FROM adpx_conversionsdetails cv
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM({_ch_date_filter(8)})
              AND cv.created_at >= {_ch_date_filter(7)}
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
