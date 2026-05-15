# scout_ch.py — ClickHouse client infrastructure + backward-compat wrappers.
# Canonical SQL lives in queries.py. Do NOT add SQL here.

from __future__ import annotations

import os
import logging

import queries as _q

log = logging.getLogger(__name__)


def _run_parallel(fns: list):
    """Run a list of zero-argument callables sequentially.
    Previously used ThreadPoolExecutor, but clickhouse_connect clients are not thread-safe —
    concurrent queries on the same client raise ProgrammingError. Sequential is correct here;
    ClickHouse queries are fast enough that the ~200ms parallelism gain doesn't justify the risk.
    Returns a list of results in the same order as fns.
    """
    return [fn() for fn in fns]


def _get_ch_client():
    """Create a ClickHouse client from env vars. Import is local so startup never fails."""
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", ""),
        user=os.getenv("CH_USER", "analytics"),
        password=os.getenv("CH_PASSWORD", ""),
        database=os.getenv("CH_DATABASE", "default"),
        secure=True,
    )
    return _LoggingCHClient(client)


class _LoggingCHClient:
    """Thin wrapper that logs every SQL query to the terminal before execution.

    Vamsee's ask: "when running locally, we should be printing all queries to
    the terminal — that's where you'll verify." This satisfies that without
    touching every call site. Logs at INFO so it appears in both local terminal
    and Railway log stream. Truncates to 400 chars to keep it readable.
    """

    def __init__(self, client):
        self._client = client

    def query(self, query: str, parameters=None, **kwargs):
        preview = query.strip().replace("\n", " ")
        preview = " ".join(preview.split())  # collapse whitespace
        log.info(f"[CH] {preview[:400]}{'…' if len(preview) > 400 else ''}")
        return self._client.query(query, parameters=parameters, **kwargs)

    def __getattr__(self, name):
        # Proxy everything else (command, insert, etc.) directly to real client
        return getattr(self._client, name)


def _query_ghost_campaigns(ch) -> list:
    """
    Canonical ghost campaign detection — delegates to queries.ghost_campaigns().
    SQL and thresholds live in queries.py. Any change belongs there.
    This wrapper exists so scout_bot.py can continue to import from scout_agent.
    """
    from scout_agent import SCOUT_THRESHOLDS  # lazy — avoids circular import
    recency_hours = int(SCOUT_THRESHOLDS.get("signals", {}).get("ghost_recency_hours", 48))
    return _q.ghost_campaigns(ch, recency_hours=recency_hours)


def _query_revenue_baseline(ch) -> dict | None:
    """
    Compare yesterday's total platform revenue against the 8-week same-weekday median.

    Fires when yesterday's actuals fall below the tolerance band (default 70% of expected).
    Returns None when revenue is within normal range or history is too thin to trust.

    Returns dict with keys:
        actual (float)            — yesterday's total revenue ($)
        expected (float)          — median revenue for this weekday over last 8 weeks
        pct_of_expected (float)   — actual / expected × 100
        weekday (str)             — e.g. "Monday"
        sample_days (int)         — number of historical same-weekday data points used
    Returns None if no anomaly or insufficient history.
    Raises on ClickHouse error — callers must catch.
    """
    from scout_agent import SCOUT_THRESHOLDS  # lazy — avoids circular import
    tolerance = float(SCOUT_THRESHOLDS.get("signals", {}).get("revenue_baseline_tolerance_pct", 70))
    min_days  = int(SCOUT_THRESHOLDS.get("signals", {}).get("revenue_baseline_min_sample_days", 4))

    sql = """
WITH history AS (
    SELECT
        toDate(created_at, 'America/Chicago') AS day,
        toDayOfWeek(day)                       AS dow,
        sum(toFloat64OrNull(revenue))          AS daily_revenue
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 60)
    WHERE created_at >= today() - 60
      AND created_at < today()
    GROUP BY day, dow
),
baseline AS (
    SELECT
        dow,
        median(daily_revenue) AS expected_revenue,
        count()               AS sample_days
    FROM history
    GROUP BY dow
    HAVING sample_days >= {min_days:UInt8}
),
yesterday_rev AS (
    SELECT coalesce(sum(toFloat64OrNull(revenue)), 0) AS actual
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(yesterday())
    WHERE created_at >= yesterday()
      AND created_at < today()
)
SELECT
    y.actual,
    b.expected_revenue,
    round(100.0 * y.actual / nullIf(b.expected_revenue, 0), 1) AS pct_of_expected,
    b.sample_days,
    toDayOfWeek(yesterday())                                     AS dow_num,
    ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][toDayOfWeek(yesterday())] AS weekday
FROM yesterday_rev y
CROSS JOIN baseline b
WHERE b.dow = toDayOfWeek(yesterday())
LIMIT 1
""".strip()

    rows = ch.query(sql, parameters={"min_days": min_days}).result_rows
    if not rows:
        return None  # not enough history for this weekday

    actual, expected, pct, sample_days, _dow_num, weekday = rows[0]
    actual   = float(actual or 0)
    expected = float(expected or 0)
    pct      = float(pct or 0)

    if pct >= tolerance:
        return None  # within normal range

    return {
        "actual":          actual,
        "expected":        expected,
        "pct_of_expected": pct,
        "weekday":         weekday,
        "sample_days":     int(sample_days),
    }


def _query_intraday_revenue_total(ch) -> dict | None:
    """
    Phase 1 of the revenue tracker daemon.

    Checks whether today's revenue (CT midnight to now) is tracking below the
    8-week same-weekday median when projected to end-of-day via the 70% arrival curve.

    Returns None when revenue is within normal range or history is too thin.
    Returns a dict with total platform numbers when an anomaly is detected.

    Returns dict with keys:
        today_revenue (float)      — revenue since CT midnight to now
        projected_full_day (float) — today_revenue / 0.70 (3pm CT = ~70% of day)
        dow_median (float)         — 8-week same-weekday full-day median
        pct_of_expected (float)    — projected_full_day / dow_median × 100
        weekday (str)              — e.g. "Friday"
        sample_days (int)          — number of historical same-weekday data points
    Returns None if no anomaly or insufficient history.
    Raises on ClickHouse error — callers must catch.
    """
    from scout_agent import SCOUT_THRESHOLDS  # lazy — avoids circular import
    tolerance = float(SCOUT_THRESHOLDS.get("signals", {}).get("revenue_baseline_tolerance_pct", 70))
    min_days  = int(SCOUT_THRESHOLDS.get("signals", {}).get("revenue_baseline_min_sample_days", 4))

    sql = """
WITH today_rev AS (
    SELECT coalesce(sum(toFloat64OrNull(revenue)), 0) AS today_revenue
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today())
    WHERE created_at >= toStartOfDay(now(), 'America/Chicago')
      AND created_at < now()
),
history AS (
    SELECT
        toDate(created_at, 'America/Chicago') AS day,
        toDayOfWeek(toDate(created_at, 'America/Chicago')) AS dow,
        sum(toFloat64OrNull(revenue)) AS daily_revenue
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 60)
    WHERE created_at >= today() - 60
      AND created_at < today()
    GROUP BY day, dow
),
baseline AS (
    SELECT
        dow,
        median(daily_revenue) AS dow_median,
        count()               AS sample_days
    FROM history
    GROUP BY dow
    HAVING sample_days >= {min_days:UInt8}
)
SELECT
    t.today_revenue,
    b.dow_median,
    b.sample_days,
    ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][toDayOfWeek(today())] AS weekday
FROM today_rev t
CROSS JOIN baseline b
WHERE b.dow = toDayOfWeek(today())
LIMIT 1
""".strip()

    rows = ch.query(sql, parameters={"min_days": min_days}).result_rows
    if not rows:
        return None  # insufficient history for today's weekday

    today_revenue, dow_median, sample_days, weekday = rows[0]
    today_revenue = float(today_revenue or 0)
    dow_median    = float(dow_median or 0)

    if dow_median == 0:
        return None

    projected_full_day = today_revenue / 0.70  # 3pm CT ≈ 70% of daily revenue
    pct_of_expected    = round(100.0 * projected_full_day / dow_median, 1)

    if pct_of_expected >= tolerance:
        return None  # within normal range

    return {
        "today_revenue":      today_revenue,
        "projected_full_day": projected_full_day,
        "dow_median":         dow_median,
        "pct_of_expected":    pct_of_expected,
        "weekday":            weekday,
        "sample_days":        int(sample_days),
    }


def _query_intraday_revenue_by_publisher(ch, total_result: dict) -> list[dict]:
    """
    Phase 2 of the revenue tracker daemon — called only when Phase 1 fires.

    For each publisher that significantly contributes to the platform shortfall,
    returns their intraday revenue vs their own 8-week same-DOW median, plus a
    root cause tag based on impressions/sessions cross-reference.

    Root cause tags (applied in priority order):
        "traffic"        — zero sessions today (no upstream traffic)
        "fill_rate"      — sessions present, zero impressions (offers not serving)
        "ghost_campaign" — impressions > ghost_min, revenue = $0 (postback broken)
        "cvr_drop"       — impressions + revenue, but CVR < 50% of historical
        "normal"         — within expected variance (filtered out of returned list)

    Only publishers with abs(delta) >= publisher_min_delta AND root_cause != "normal"
    are included. Cap at 5. Sorted by abs(delta) descending.

    Note on publisher key mismatch: impressions use `pid` (string), sessions/conversions
    use `user_id` (numeric). We query them separately and align via mv_adpx_users.
    """
    from scout_agent import SCOUT_THRESHOLDS  # lazy — avoids circular import
    min_days        = int(SCOUT_THRESHOLDS.get("signals", {}).get("revenue_baseline_min_sample_days", 4))
    min_delta       = float(SCOUT_THRESHOLDS.get("signals", {}).get("revenue_tracker_publisher_min_delta", 500))
    ghost_min_impr  = int(SCOUT_THRESHOLDS.get("signals", {}).get("revenue_tracker_ghost_min_impressions", 100))
    cvr_min_impr    = int(SCOUT_THRESHOLDS.get("signals", {}).get("revenue_tracker_cvr_min_impressions", 500))

    # ── Query 1: today's per-publisher revenue + conversions via user_id ──────
    revenue_sql = """
SELECT
    toString(user_id)                                AS publisher_id,
    coalesce(sum(toFloat64OrNull(revenue)), 0)       AS revenue_today,
    count()                                          AS conversions_today
FROM adpx_conversionsdetails
PREWHERE toYYYYMM(created_at) >= toYYYYMM(today())
WHERE created_at >= toStartOfDay(now(), 'America/Chicago')
  AND created_at < now()
GROUP BY user_id
""".strip()

    # ── Query 2: today's per-publisher impressions via pid ────────────────────
    impressions_sql = """
SELECT
    pid                                              AS publisher_id,
    count()                                          AS impressions_today
FROM adpx_impressions_details
PREWHERE toYYYYMM(created_at) >= toYYYYMM(today())
WHERE created_at >= toStartOfDay(now(), 'America/Chicago')
  AND created_at < now()
GROUP BY pid
""".strip()

    # ── Query 3: today's per-publisher sessions via user_id ───────────────────
    sessions_sql = """
SELECT
    toString(user_id)                                AS publisher_id,
    count()                                          AS sessions_today
FROM adpx_sdk_sessions
PREWHERE toYYYYMM(created_at) >= toYYYYMM(today())
WHERE created_at >= toStartOfDay(now(), 'America/Chicago')
  AND created_at < now()
GROUP BY user_id
""".strip()

    # ── Query 4: 8-week same-DOW per-publisher revenue median ─────────────────
    baseline_sql = """
WITH history AS (
    SELECT
        toString(user_id)                                AS publisher_id,
        toDate(created_at, 'America/Chicago')            AS day,
        toDayOfWeek(toDate(created_at, 'America/Chicago')) AS dow,
        sum(toFloat64OrNull(revenue))                    AS daily_revenue,
        count()                                          AS daily_conversions
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 60)
    WHERE created_at >= today() - 60
      AND created_at < today()
    GROUP BY publisher_id, day, dow
)
SELECT
    publisher_id,
    median(daily_revenue * 0.70)  AS revenue_expected,
    median(daily_conversions * 0.70) AS conversions_expected,
    count()                        AS sample_days
FROM history
WHERE dow = toDayOfWeek(today())
GROUP BY publisher_id
HAVING sample_days >= {min_days:UInt8}
""".strip()

    # ── Query 5: publisher name lookup ────────────────────────────────────────
    names_sql = """
SELECT toString(user_id) AS publisher_id, organization
FROM mv_adpx_users
WHERE user_id > 0
""".strip()

    try:
        rev_rows   = ch.query(revenue_sql).result_rows
        impr_rows  = ch.query(impressions_sql).result_rows
        sess_rows  = ch.query(sessions_sql).result_rows
        base_rows  = ch.query(baseline_sql, parameters={"min_days": min_days}).result_rows
        names_rows = ch.query(names_sql).result_rows
    except Exception as e:
        import logging as _log
        _log.getLogger("scout_agent").warning(f"_query_intraday_revenue_by_publisher query failed: {e}")
        return []

    # Build lookup dicts
    revenue_by_pub     = {r[0]: (float(r[1] or 0), int(r[2] or 0)) for r in rev_rows}
    impressions_by_pub = {r[0]: int(r[1] or 0) for r in impr_rows}
    sessions_by_pub    = {r[0]: int(r[1] or 0) for r in sess_rows}
    baseline_by_pub    = {r[0]: (float(r[1] or 0), float(r[2] or 0), int(r[3] or 0)) for r in base_rows}
    names_by_pub       = {r[0]: str(r[1] or "") for r in names_rows}

    # All publishers with a baseline (those we can evaluate)
    all_pub_ids = set(baseline_by_pub.keys())

    results = []
    for pub_id in all_pub_ids:
        revenue_expected, conv_expected, sample_days = baseline_by_pub[pub_id]
        revenue_today, conv_today = revenue_by_pub.get(pub_id, (0.0, 0))
        impressions_today = impressions_by_pub.get(pub_id, 0)
        sessions_today    = sessions_by_pub.get(pub_id, 0)
        delta             = revenue_today - revenue_expected

        if abs(delta) < min_delta:
            continue  # not a meaningful contributor

        # Root cause tagging (priority order)
        if sessions_today == 0:
            root_cause = "traffic"
        elif impressions_today == 0:
            root_cause = "fill_rate"
        elif revenue_today == 0 and impressions_today > ghost_min_impr:
            root_cause = "ghost_campaign"
        elif (conv_expected > 0 and impressions_today > cvr_min_impr
              and conv_today / max(impressions_today, 1) < (conv_expected / max(revenue_expected / max(delta, 1), 1)) * 0.5):
            # Simple CVR proxy: today's conv/impr vs historical conv_expected/revenue_expected ratio
            root_cause = "cvr_drop"
        else:
            root_cause = "normal"

        if root_cause == "normal":
            continue  # within variance, not the cause

        results.append({
            "publisher_id":       pub_id,
            "publisher_name":     names_by_pub.get(pub_id, f"pub {pub_id}"),
            "revenue_today":      revenue_today,
            "revenue_expected":   revenue_expected,
            "delta":              delta,
            "impressions_today":  impressions_today,
            "sessions_today":     sessions_today,
            "conversions_today":  conv_today,
            "sample_days":        sample_days,
            "root_cause":         root_cause,
        })

    # Sort by absolute delta descending, cap at 5
    results.sort(key=lambda r: abs(r["delta"]), reverse=True)
    return results[:5]


def _query_advertiser_rpm_context(ch, adv_name: str) -> dict:
    """
    Return 30-day platform RPM history for an advertiser across all active campaigns.

    Used at offer approval time to give the team context on how this advertiser
    performs on the MS platform before committing to a queue slot.

    Returns dict with keys:
        has_history (bool)     — False if no campaigns found or query fails
        active_campaigns (int) — number of campaigns with >= 100 impressions in 30d
        impressions_30d (int)  — total impressions across all campaigns
        revenue_30d (float)    — total revenue ($) across all campaigns
        rpm_min (float)        — lowest per-campaign RPM
        rpm_max (float)        — highest per-campaign RPM
        rpm_avg (float)        — blended RPM (total revenue / total impressions * 1000)
    """
    if not adv_name or not adv_name.strip():
        return {"has_history": False}
    try:
        q = """
        WITH
        imp_agg AS (
            SELECT campaign_id, count() AS impressions_30d
            FROM adpx_impressions_details
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(now() - INTERVAL 30 DAY)
            WHERE created_at >= now() - INTERVAL 30 DAY
            GROUP BY campaign_id
        ),
        conv_agg AS (
            SELECT campaign_id,
                   round(sum(toFloat64OrNull(revenue)), 2) AS revenue_30d
            FROM adpx_conversionsdetails
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(now() - INTERVAL 30 DAY)
            WHERE created_at >= now() - INTERVAL 30 DAY
            GROUP BY campaign_id
        )
        SELECT
            count()                                                                              AS active_campaigns,
            sum(imp.impressions_30d)                                                             AS impressions_30d,
            sum(coalesce(conv.revenue_30d, 0))                                                   AS revenue_30d,
            min(round(coalesce(conv.revenue_30d, 0) / nullIf(imp.impressions_30d, 0) * 1000, 1)) AS rpm_min,
            max(round(coalesce(conv.revenue_30d, 0) / nullIf(imp.impressions_30d, 0) * 1000, 1)) AS rpm_max,
            round(sum(coalesce(conv.revenue_30d, 0)) / nullIf(sum(imp.impressions_30d), 0) * 1000, 1) AS rpm_avg
        FROM imp_agg imp
        LEFT JOIN conv_agg conv ON conv.campaign_id = imp.campaign_id
        JOIN from_airbyte_campaigns fc ON toUInt64(fc.id) = imp.campaign_id
        WHERE imp.impressions_30d >= 100
          AND trim(fc.status) = 'active'
          AND fc.adv_name ILIKE {adv_pattern:String}
        """
        rows = ch.query(q, parameters={"adv_pattern": f"%{adv_name}%"}).result_rows
        if not rows or rows[0][0] == 0:
            return {"has_history": False}
        active, imps, rev, rpm_min, rpm_max, rpm_avg = rows[0]
        return {
            "has_history":      True,
            "active_campaigns": int(active or 0),
            "impressions_30d":  int(imps or 0),
            "revenue_30d":      float(rev or 0),
            "rpm_min":          float(rpm_min or 0),
            "rpm_max":          float(rpm_max or 0),
            "rpm_avg":          float(rpm_avg or 0),
        }
    except Exception as e:
        log.warning(f"_query_advertiser_rpm_context failed for {adv_name!r}: {e}")
        return {"has_history": False}
