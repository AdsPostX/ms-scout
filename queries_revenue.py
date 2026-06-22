"""ClickHouse queries — revenue, cap, velocity, fill-rate, and shared SQL helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL helpers (shared across all query modules — imported from here)
# ---------------------------------------------------------------------------

def _ch_date_filter(days: int) -> str:
    """Return the ClickHouse SQL fragment for filtering rows within the last *days* days.

    Usage in f-string SQL::

        f"WHERE created_at >= {_ch_date_filter(7)}"
        # → "WHERE created_at >= today() - 7"

    The integer-subtraction form (``today() - N``) is semantically identical to
    ``today() - INTERVAL N DAY`` in ClickHouse and is the canonical form used here.

    Safe for f-string interpolation because *days* is always a hardcoded int literal
    at every call site — never user input or a runtime variable.
    """
    return f"today() - {days}"


def _ct_today() -> str:
    """SQL fragment: today's date in America/Chicago timezone."""
    return "toDate(toTimeZone(now(), 'America/Chicago'))"


def _ct_days_ago(n: int) -> str:
    """SQL fragment: N days ago in America/Chicago timezone."""
    return f"toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL {n} DAY"


def _ct_date(col: str) -> str:
    """SQL fragment: convert a timestamp column to a CT date.

    Usage::

        f"{_ct_date('cv.created_at')} AS rev_date"
        # → "toDate(cv.created_at, 'America/Chicago') AS rev_date"
    """
    return f"toDate({col}, 'America/Chicago')"


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
    revenue_eod_projection_low_cents:  Optional[int] = None      # p25 band from historical distribution
    revenue_eod_projection_high_cents: Optional[int] = None      # p75 band from historical distribution
    revenue_eod_diagnostic:            Optional[str] = None      # one of: "efficiency", "traffic", "traffic_upside", "on_track", or None
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
    pub_sql = f"""
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
PREWHERE toYYYYMM(c.created_at) >= toYYYYMM({_ct_days_ago(8)})
WHERE c.created_at >= {_ct_days_ago(8)}
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
    worry   = list(reversed(deltas[-3:]))
    # If winners and worry overlap (very few publishers), prefer winners as-is
    # and trim worry to non-overlapping tail — only when enough publishers exist
    # to guarantee non-empty worry after dedup.
    if len(deltas) > 3:
        win_ids = {d.publisher_id for d in winners}
        worry = [d for d in worry if d.publisher_id not in win_ids][:3]

    # ── 7-day daily revenue series (sparkline data) ──
    # Fetches one row per completed day (D-7 through D-1); fills missing days
    # with 0; appends today's partial as the 8th point.
    series_sql = f"""
SELECT
    {_ct_date('c.created_at')} AS day,
    round(sum(toFloat64OrNull(c.revenue)), 2)           AS daily_rev
FROM adpx_conversionsdetails c
WHERE {_ct_date('c.created_at')}
          >= toDate(toStartOfDay(toTimeZone(now(), 'America/Chicago'))) - 7
  AND {_ct_date('c.created_at')}
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
        f"""
        WITH adv_perf AS (
            SELECT
                c.adv_name,
                count(DISTINCT cv.user_id)                               AS publisher_count,
                round(sum(toFloat64OrNull(cv.revenue)), 2)               AS rev_30d,
                round(sum(toFloat64OrNull(cv.revenue))
                      / nullIf(count(DISTINCT cv.user_id), 0), 2)        AS avg_rev_per_pub
            FROM adpx_conversionsdetails cv
            JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM({_ch_date_filter(30)})
              AND cv.created_at >= {_ch_date_filter(30)}
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
            WHERE toYYYYMM(s.created_at) >= toYYYYMM({_ch_date_filter(30)})
              AND s.created_at >= {_ch_date_filter(30)}
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
        exposure_cvr_7d (float), exposure_cvr_yesterday (float), delta_pct (float),
        impressions_7d (int), payout_per_conversion (float)
    Raises on ClickHouse error — callers must catch.
    """
    rows = ch.query(
        f"""
        WITH imp_7d AS (
                -- adpx_impressions_details.pid is the publisher ID (string) —
                -- no session join needed; eliminates the FillingRightJoinSide OOM.
                SELECT
                    toUInt64OrZero(pid)               AS publisher_id,
                    campaign_id,
                    count()                           AS impressions_7d
                FROM adpx_impressions_details
                WHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(8)})
                  AND created_at >= {_ch_date_filter(7)}
                GROUP BY publisher_id, campaign_id
                HAVING impressions_7d >= {{min_impressions_7d: Int64}}
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
                WHERE toYYYYMM(created_at) >= toYYYYMM({_ch_date_filter(8)})
                  AND created_at >= {_ch_date_filter(7)}
                GROUP BY publisher_id, campaign_id
                HAVING payout_per_conversion >= {{min_payout: Float64}}
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
                          nullIf(iy.impressions_yesterday, 0), 6)         AS exposure_cvr_yesterday,
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
          AND delta_pct <= -{{drop_pct: Float64}}
        ORDER BY delta_pct ASC
        """,
        parameters={
            "drop_pct": drop_pct,
            "min_payout": min_payout,
            "min_impressions_7d": min_impressions_7d,
        },
    ).result_rows
    return [
        {
            "publisher_id":           int(r[0]),
            "publisher_name":         r[1],
            "campaign_id":            int(r[2]),
            "adv_name":               r[3],
            "exposure_cvr_7d":        float(r[4] or 0),
            "exposure_cvr_yesterday": float(r[5] or 0),
            "delta_pct":              float(r[6] or 0),
            "impressions_7d":         int(r[7]),
            "payout_per_conversion":  float(r[8] or 0),
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
                coalesce(conv.conversions, 0)                                          AS conversions,
                coalesce(conv.revenue, 0)                                              AS revenue
            FROM from_airbyte_campaigns c
            JOIN (
                SELECT campaign_id, count() AS impression_count
                FROM adpx_impressions_details
                WHERE toYYYYMM(created_at) >= 202501
                GROUP BY campaign_id
                HAVING impression_count > 500
            ) imp ON toInt64(c.id) = toInt64(imp.campaign_id)
            LEFT JOIN (
                SELECT campaign_id,
                       count()                          AS conversions,
                       sum(toFloat64OrNull(revenue))    AS revenue
                FROM adpx_conversionsdetails
                WHERE toYYYYMM(created_at) >= 202501
                GROUP BY campaign_id
            ) conv ON toInt64(c.id) = toInt64(conv.campaign_id)
            WHERE c.deleted_at IS NULL
        )
        SELECT
            id,
            adv_name,
            impact_id,
            arrayJoin(arrayFilter(
                t -> NOT startsWith(t, 'internal-'),
                JSONExtract(coalesce(tags, '[]'), 'Array(String)')
            )) AS category,
            impression_count,
            round(conversions / nullIf(impression_count, 0) * 100, 4)  AS cvr_pct,
            round(revenue     / nullIf(impression_count, 0) * 1000, 2) AS rpm
        FROM joined
        WHERE length(JSONExtract(coalesce(tags, '[]'), 'Array(String)')) > 0
        """
    ).result_rows
    return rows


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
                count(DISTINCT c.id)                                                         AS campaigns,
                round(sum(coalesce(conv.conversions, 0)) / nullIf(sum(imp.impressions), 0) * 100, 4)   AS cvr_pct,
                round(sum(coalesce(conv.revenue, 0))     / nullIf(sum(imp.impressions), 0) * 1000, 2)  AS rpm
            FROM from_airbyte_campaigns c
            LEFT JOIN conv ON toInt64(c.id) = toInt64(conv.campaign_id)
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
# Revenue trends helpers
# ===========================================================================

def _trend(delta: float) -> str:
    """Map a revenue delta percentage to a trend label (shared by publisher + advertiser trends)."""
    if delta <= -15:
        return "down"
    if delta >= 15:
        return "up"
    return "flat"


def _revenue_trend_sql(
    entity_col: str,
    group_col: str,
    *,
    actual_metric: str,
    actual_join: str = "",
    historical_join: str = "",
    names_cte: str = "",
    final_select: str,
) -> str:
    """Build the period-median revenue trend SQL for a given entity dimension.

    Generates the full parameterised ClickHouse SQL used by both
    ``publisher_revenue_trends`` and ``advertiser_revenue_trends``.  The
    ClickHouse ``{days: Int32}`` / ``{lookback: Int32}`` / ``{min_periods:
    Int32}`` bind parameters are left as-is for the caller to supply via
    ``ch.query(..., parameters=...)``.

    Args:
        entity_col:     SELECT expression that defines the entity key in the
                        ``actual`` and ``historical_daily`` CTEs, e.g.
                        ``"cv.user_id AS publisher_id"`` or ``"c.adv_name"``.
        group_col:      Column name (no alias) to GROUP BY / JOIN on, e.g.
                        ``"publisher_id"`` or ``"adv_name"``.
        actual_metric:  The count/metric column expression for the ``actual``
                        CTE, e.g.
                        ``"count(DISTINCT cv.session_id) AS sessions_actual"``
                        or ``"count() AS conversions_actual"``.
        actual_join:    Optional JOIN clause appended to the ``actual`` CTE's
                        FROM, e.g.
                        ``"JOIN from_airbyte_campaigns c ON ..."``
        historical_join:
                        Optional JOIN clause appended to the
                        ``historical_daily`` CTE's FROM.
        names_cte:      Optional extra CTE block (WITH body, including the
                        leading comma) inserted before the final SELECT, e.g.
                        the publisher ``names`` CTE.
        final_select:   The final SELECT … FROM baseline … ORDER BY block
                        (excludes the WITH keyword — that is generated here).
    """
    if names_cte and not names_cte.lstrip().startswith(","):
        raise ValueError("names_cte must start with a comma, e.g. \",\\n    names AS (...)\"")
    return f"""
        WITH actual AS (
            SELECT
                {entity_col},
                round(sum(toFloat64OrNull(cv.revenue)), 2)            AS revenue_actual,
                {actual_metric}
            FROM adpx_conversionsdetails cv
            {actual_join}
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL {{days: Int32}} DAY)
              AND cv.created_at >= today() - INTERVAL {{days: Int32}} DAY
            GROUP BY {group_col}
        ),
        historical_daily AS (
            SELECT
                {entity_col},
                toDate(cv.created_at)                                 AS rev_date,
                round(sum(toFloat64OrNull(cv.revenue)), 2)            AS daily_revenue
            FROM adpx_conversionsdetails cv
            {historical_join}
            WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL {{lookback: Int32}} DAY)
              AND cv.created_at >= today() - INTERVAL {{lookback: Int32}} DAY
              AND cv.created_at < today() - INTERVAL {{days: Int32}} DAY
            GROUP BY {group_col}, rev_date
        ),
        historical_periods AS (
            SELECT
                {group_col},
                intDiv(
                    dateDiff('day', toDate(today() - INTERVAL {{lookback: Int32}} DAY), rev_date),
                    {{days: Int32}}
                )                                                      AS period_idx,
                sum(daily_revenue)                                     AS period_revenue
            FROM historical_daily
            GROUP BY {group_col}, period_idx
        ),
        baseline AS (
            SELECT
                {group_col},
                round(median(period_revenue), 2)                       AS revenue_expected,
                count()                                                AS period_count
            FROM historical_periods
            GROUP BY {group_col}
            HAVING period_count >= {{min_periods: Int32}}
        ){names_cte}
        {final_select}
        """


def advertiser_revenue_trends(ch, days: int = 7, min_periods: int = 4) -> list[dict]:
    """
    Advertiser revenue trends: same period-median algorithm as publisher_revenue_trends
    but aggregated by adv_name across all publishers.

    Returns list of dicts with keys:
        adv_name (str), revenue_actual (float), revenue_expected (float),
        delta_pct (float), trend ("up" | "down" | "flat"), conversions_actual (int)
    Raises on ClickHouse error — callers must catch.
    """
    if days <= 0:
        raise ValueError(f"days must be a positive integer, got {days!r}")
    if min_periods <= 0:
        raise ValueError(f"min_periods must be a positive integer, got {min_periods!r}")
    _campaign_join = "JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id"
    rows = ch.query(
        _revenue_trend_sql(
            entity_col="c.adv_name",
            group_col="adv_name",
            actual_metric="count()                                               AS conversions_actual",
            actual_join=_campaign_join,
            historical_join=_campaign_join,
            names_cte="",
            final_select="""SELECT
            b.adv_name,
            coalesce(a.revenue_actual, 0)                             AS revenue_actual,
            b.revenue_expected,
            round((coalesce(a.revenue_actual, 0) - b.revenue_expected) /
                  nullIf(b.revenue_expected, 0) * 100, 1)             AS delta_pct,
            coalesce(a.conversions_actual, 0)                         AS conversions_actual
        FROM baseline b
        LEFT JOIN actual a ON a.adv_name = b.adv_name
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
            "adv_name": r[0],
            "revenue_actual": float(r[1] or 0),
            "revenue_expected": float(r[2] or 0),
            "delta_pct": float(r[3] or 0),
            "trend": _trend(float(r[3] or 0)),
            "conversions_actual": int(r[4] or 0),
        }
        for r in rows
    ]


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
                coalesce(sum(toFloat64OrNull(revenue)), 0) AS gross_rev,
                coalesce(sum(toFloat64OrNull(payout)), 0)  AS partner_rev
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


def get_publisher_drill_summary(pub_id: str) -> dict:
    # Output shape: {pub_id, pub_name, rev_7d, conv_7d, rev_yesterday,
    #                conv_yesterday, top_offer, as_of}
    # Table: adpx_conversionsdetails — user_id column = publisher ID (UInt64)
    from scout_ch import _get_ch_client

    sql = """
SELECT
    toDate(cv.created_at)                           AS day,
    coalesce(any(u.organization), '')               AS pub_name,
    coalesce(any(c.adv_name), '')                   AS adv_name,
    round(sum(toFloat64OrNull(cv.revenue)), 4)      AS rev,
    count()                                         AS convs
FROM adpx_conversionsdetails cv
LEFT JOIN mv_adpx_users u ON u.id = toInt64(cv.user_id)
LEFT JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
WHERE cv.user_id = {pub_id: UInt64}
  AND toYYYYMM(cv.created_at) >= toYYYYMM(today() - INTERVAL 7 DAY)
  AND cv.created_at >= today() - INTERVAL 7 DAY
GROUP BY day, adv_name
ORDER BY day DESC
""".strip()

    ch = _get_ch_client()
    rows = ch.query(sql, parameters={"pub_id": int(pub_id)}).result_rows

    rev_7d = 0.0
    conv_7d = 0
    rev_yesterday = 0.0
    conv_yesterday = 0
    pub_name = str(pub_id)
    adv_rev: dict[str, float] = {}
    yesterday = date.today() - timedelta(days=1)

    for row in rows:
        day, p_name, adv_name, rev, convs = row
        if p_name:
            pub_name = p_name
        rev_7d += float(rev or 0)
        conv_7d += int(convs or 0)
        if isinstance(day, date) and day == yesterday:
            rev_yesterday += float(rev or 0)
            conv_yesterday += int(convs or 0)
        if adv_name:
            adv_rev[adv_name] = adv_rev.get(adv_name, 0.0) + float(rev or 0)

    top_offer = max(adv_rev, key=lambda k: adv_rev[k]) if adv_rev else None

    return {
        "pub_id": str(pub_id),
        "pub_name": pub_name,
        "rev_7d": round(rev_7d, 4),
        "conv_7d": conv_7d,
        "rev_yesterday": round(rev_yesterday, 4),
        "conv_yesterday": conv_yesterday,
        "top_offer": top_offer,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
