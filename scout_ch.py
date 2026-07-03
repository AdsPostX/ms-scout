# scout_ch.py — ClickHouse client infrastructure + backward-compat wrappers.
# Canonical SQL lives in queries.py. Do NOT add SQL here.

from __future__ import annotations

import os
import logging
import statistics
import threading
import time

import queries as _q
from scout_thresholds import _manager as _tm

log = logging.getLogger(__name__)


# Global cap on concurrent ClickHouse queries across the whole process.
# Background workers (_run_ghost, _run_fill, etc.) and the agent's parallel
# tool executor can otherwise stack 6-10 queries simultaneously on the same
# CH cluster, which surfaces to users as "ClickHouse is under pressure".
# Queue (block briefly) instead of failing — most queries finish in <2s.
def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        log.warning("[CH] bad %s=%r — falling back to %d", name, raw, default)
        return default
    if value < minimum:
        log.warning("[CH] %s=%d below minimum %d — using %d", name, value, minimum, minimum)
        return minimum
    return value


def _env_float(name: str, default: float, minimum: float = 0.1, maximum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        log.warning("[CH] bad %s=%r — falling back to %s", name, raw, default)
        return default
    if value < minimum:
        log.warning("[CH] %s=%s below minimum %s — using %s", name, value, minimum, minimum)
        return minimum
    if maximum is not None and value > maximum:
        log.warning("[CH] %s=%s above maximum %s — using %s", name, value, maximum, maximum)
        return maximum
    return value


_CH_MAX_CONCURRENT = _env_int("CH_MAX_CONCURRENT", 4, minimum=1)
# Capped at 30s — like the timeouts below, an env misconfiguration must not be
# able to let a single acquire wait outlast ask_timeout_s on its own.
_CH_ACQUIRE_TIMEOUT_S = _env_float("CH_ACQUIRE_TIMEOUT_S", 10.0, minimum=0.1, maximum=30.0)
_CH_QUERY_SEMAPHORE = threading.BoundedSemaphore(_CH_MAX_CONCURRENT)

# Bound the network I/O itself, not just the wait for a semaphore slot.
# clickhouse_connect defaults to connect_timeout=10s, send_receive_timeout=300s —
# a degraded connection can hang a single .query() call for 5 minutes, holding
# both a _CH_QUERY_SEMAPHORE slot and (via _ask_with_timeout) an _ASK_SEMAPHORE
# slot the whole time. Keep this well under _CFG.ask_timeout_s (90s default)
# so a hung query fails fast enough for ask()'s own timeout to mean anything.
# Capped at the same ceiling — an env misconfiguration (e.g. 600s) must not be
# able to silently defeat that bound.
_CH_CONNECT_TIMEOUT_S = _env_float("CH_CONNECT_TIMEOUT_S", 10.0, minimum=1.0, maximum=30.0)
_CH_SEND_RECEIVE_TIMEOUT_S = _env_float("CH_SEND_RECEIVE_TIMEOUT_S", 45.0, minimum=1.0, maximum=60.0)


def _validate_ch_timeout_budget() -> None:
    """Warn if acquire+connect+send_receive can outlast ask()'s own timeout.

    Each piece is individually clamped above, but nothing previously checked
    their sum against _CFG.ask_timeout_s — a hung query could still fail
    later than ask()'s own timeout, defeating the point of bounding it.
    Warns instead of raising: a single bad env var must not crash the
    process. Local import avoids a circular import (scout_handlers imports
    scout_ch, not the reverse).
    """
    from scout_handlers import _CFG as _handlers_cfg
    budget = _CH_ACQUIRE_TIMEOUT_S + _CH_CONNECT_TIMEOUT_S + _CH_SEND_RECEIVE_TIMEOUT_S
    if budget >= _handlers_cfg.ask_timeout_s:
        log.warning(
            "[CH] timeout budget %.1fs (acquire=%.1f + connect=%.1f + send_receive=%.1f) "
            ">= ask_timeout_s %ds — a hung query can outlast ask()'s own timeout",
            budget, _CH_ACQUIRE_TIMEOUT_S, _CH_CONNECT_TIMEOUT_S, _CH_SEND_RECEIVE_TIMEOUT_S,
            _handlers_cfg.ask_timeout_s,
        )


# Revenue deviation threshold used by the intraday diagnostic classifier.
# A projected or actual revenue deviation beyond this fraction triggers
# a "traffic" or "efficiency" diagnostic label.
_REVENUE_DEVIATION_THRESHOLD = 0.08


class CHBusyError(RuntimeError):
    """Raised when the CH concurrency cap is saturated past the acquire timeout.

    Callers (handlers, agent tools) catch this and surface the friendly
    "ClickHouse is under pressure" message instead of an infinite spinner.
    """


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
        connect_timeout=_CH_CONNECT_TIMEOUT_S,
        send_receive_timeout=_CH_SEND_RECEIVE_TIMEOUT_S,
    )
    return _LoggingCHClient(client)


class _LoggingCHClient:
    """Thin wrapper that logs every SQL query to the terminal before execution.

    Log all queries to terminal for local debugging — shows actual queries hitting
    ClickHouse. Logs at INFO so it appears in both local terminal and log stream.
    Truncates to 400 chars to keep output readable.
    """

    def __init__(self, client):
        self._client = client

    def query(self, query: str, parameters=None, **kwargs):
        preview = query.strip().replace("\n", " ")
        preview = " ".join(preview.split())  # collapse whitespace
        log.info(f"[CH] {preview[:400]}{'…' if len(preview) > 400 else ''}")
        t0 = time.monotonic()
        if not _CH_QUERY_SEMAPHORE.acquire(timeout=_CH_ACQUIRE_TIMEOUT_S):
            log.warning(
                f"[CH] busy: gave up after {_CH_ACQUIRE_TIMEOUT_S}s waiting for slot "
                f"(cap={_CH_MAX_CONCURRENT}); query={preview[:160]}"
            )
            raise CHBusyError(
                f"ClickHouse is busy (>{_CH_MAX_CONCURRENT} concurrent queries); try again shortly."
            )
        wait_ms = int((time.monotonic() - t0) * 1000)
        if wait_ms > 500:
            log.info(f"[CH] queued {wait_ms}ms before slot")
        try:
            return self._client.query(query, parameters=parameters, **kwargs)
        finally:
            _CH_QUERY_SEMAPHORE.release()

    def __getattr__(self, name):
        # Proxy everything else (command, insert, etc.) directly to real client
        return getattr(self._client, name)


def _query_ghost_campaigns(ch, as_of_date: str | None = None) -> list:
    """
    Canonical ghost campaign detection — delegates to queries.ghost_campaigns().
    SQL and thresholds live in queries.py. Any change belongs there.
    This wrapper exists so scout_bot.py can continue to import from scout_agent.

    as_of_date: optional ISO date string (e.g. '2026-01-15') for backtest replay.
    When provided, today() in the underlying SQL is substituted with toDate(as_of_date).
    """
    recency_hours = int(_tm.load().get("signals", {}).get("ghost_recency_hours", 48))
    return _q.ghost_campaigns(ch, recency_hours=recency_hours, as_of_date=as_of_date)


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
    tolerance = float(_tm.load().get("signals", {}).get("revenue_baseline_tolerance_pct", 70))
    min_days  = int(_tm.load().get("signals", {}).get("revenue_baseline_min_sample_days", 4))

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
    tolerance = float(_tm.load().get("signals", {}).get("revenue_baseline_tolerance_pct", 70))
    min_days  = int(_tm.load().get("signals", {}).get("revenue_baseline_min_sample_days", 4))

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
        "revenue_down"   — revenue below expected with no single dominant signal

    Only publishers with abs(delta) >= publisher_min_delta are included (all tagged
    publishers are returned — there is no "normal" filter value). Cap at 5. Sorted
    by abs(delta) descending.

    Note on publisher key mismatch: impressions use `pid` (string), sessions/conversions
    use `user_id` (numeric). We query them separately and align via mv_adpx_users.
    """
    min_days        = int(_tm.load().get("signals", {}).get("revenue_baseline_min_sample_days", 4))
    min_delta       = float(_tm.load().get("signals", {}).get("revenue_tracker_publisher_min_delta", 500))
    ghost_min_impr  = int(_tm.load().get("signals", {}).get("revenue_tracker_ghost_min_impressions", 100))
    cvr_min_impr    = int(_tm.load().get("signals", {}).get("revenue_tracker_cvr_min_impressions", 500))

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
    except CHBusyError:
        raise
    except Exception as e:
        log.warning(f"_query_intraday_revenue_by_publisher query failed: {e}")
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
        else:
            root_cause = "revenue_down"

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


# Module-scope cache for the 90-day hour-of-day curve.
# The curve is the same for ~10 min across users; avoids fanning out the
# ~90-day aggregate scan when several @Scout mentions arrive in a burst.
_HOUR_CURVE_CACHE: dict = {"ts": 0.0, "data": None}
_HOUR_CURVE_TTL_SEC = 600  # 10 minutes


def _quantile(data: list[float], q: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = q * (len(s) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (idx - lo) * (s[hi] - s[lo])


def _build_hour_curve(ch) -> dict:
    """Return per-DOW cumulative-share curve, traffic baselines, and per-DOW
    full-day median from the last 90 CT calendar days (excluding today).

    Per-DOW share at hour H is now a band dict {p25, p50, p75, n} across
    qualifying same-weekday days. Median (not mean) + 90d window cleared the
    backtest gate (median |% err| 7.25%, P90 17.3% across 40 cells);
    60d/mean systematically undershot (~12% / 26%).

    Returns:
        {
            "share_by_dow":    { dow_int: { hour_int: {p25, p50, p75, n} } },
            "traffic_by_dow":  { dow_int: { hour_int: {impressions_p50, sessions_p50} } },
            "dow_median":      { dow_int: float },
            "sample_days":     { dow_int: int },  # qualifying full-day samples
        }

    All datetime math is anchored in America/Chicago. dow values match
    ClickHouse `toDayOfWeek`: Monday=1 .. Sunday=7.
    """
    import time as _time

    now = _time.time()
    if _HOUR_CURVE_CACHE["data"] is not None and (now - _HOUR_CURVE_CACHE["ts"]) < _HOUR_CURVE_TTL_SEC:
        return _HOUR_CURVE_CACHE["data"]

    sql = """
SELECT
    toDate(toTimeZone(created_at, 'America/Chicago'))                AS ct_day,
    toDayOfWeek(toDate(toTimeZone(created_at, 'America/Chicago')))   AS dow,
    toHour(toTimeZone(created_at, 'America/Chicago'))                AS ct_hour,
    sum(toFloat64OrNull(revenue))                                    AS hour_rev
FROM adpx_conversionsdetails
PREWHERE toYYYYMM(created_at) >= toYYYYMM(
    toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 95 DAY
)
WHERE toDate(toTimeZone(created_at, 'America/Chicago'))
        >= toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 90 DAY
  AND toDate(toTimeZone(created_at, 'America/Chicago'))
        <  toDate(toTimeZone(now(), 'America/Chicago'))
GROUP BY ct_day, dow, ct_hour
""".strip()

    rows = ch.query(sql).result_rows

    # Aggregate to per-day full + per-day cumulative-through-hour
    days: dict = {}  # ct_day -> {"dow": int, "by_hour": {h: rev}, "full": float}
    for ct_day, dow, ct_hour, hour_rev in rows:
        rec = days.setdefault(ct_day, {"dow": int(dow), "by_hour": {}, "full": 0.0})
        rev = float(hour_rev or 0)
        rec["by_hour"][int(ct_hour)] = rec["by_hour"].get(int(ct_hour), 0.0) + rev
        rec["full"] += rev

    # Per-DOW: for each hour H, collect (cum_through_H / full_day) shares across
    # qualifying days (full_day > 0) in the 90d window.
    share_acc: dict = {dow: {h: [] for h in range(24)} for dow in range(1, 8)}
    # For dow_median we want an 8-week same-weekday baseline (distinct concern
    # from the share curve). Keep (ct_day, full) tuples so we can take the
    # 8 most recent same-weekday samples per DOW.
    full_days_dated: dict = {dow: [] for dow in range(1, 8)}
    sample_days: dict = {dow: 0 for dow in range(1, 8)}

    for ct_day, rec in days.items():
        if rec["full"] <= 0:
            continue
        dow = rec["dow"]
        sample_days[dow] += 1
        full_days_dated[dow].append((ct_day, rec["full"]))
        cum = 0.0
        for h in range(24):
            cum += rec["by_hour"].get(h, 0.0)
            share_acc[dow][h].append(cum / rec["full"])

    def _median(xs: list[float]) -> float:
        if not xs:
            return 0.0
        s = sorted(xs)
        n = len(s)
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    share_by_dow: dict = {}
    for dow, by_hour in share_acc.items():
        share_by_dow[dow] = {}
        for h, shares in by_hour.items():
            share_by_dow[dow][h] = {
                "p25": _quantile(shares, 0.25),
                "p50": statistics.median(shares) if shares else 0.0,
                "p75": _quantile(shares, 0.75),
                "n":   len(shares),
            }

    # 8-week same-weekday baseline: pick the 8 most recent qualifying same-DOW
    # full-day totals (within the 90d window) and median those. Keeps the
    # share curve's 90d statistical bedrock independent from a tighter
    # recency baseline for "what's a normal day".
    dow_median: dict = {}
    for dow, dated in full_days_dated.items():
        recent = sorted(dated, key=lambda t: t[0], reverse=True)[:8]
        dow_median[dow] = _median([v for _, v in recent])

    # ── Traffic baselines: impressions and sessions per (DOW, hour) ──────────
    # Same 90-day window, same DOW/hour grouping as the revenue share scan.
    imp_sql = """
SELECT
    toDate(toTimeZone(created_at, 'America/Chicago'))              AS ct_day,
    toDayOfWeek(toDate(toTimeZone(created_at, 'America/Chicago'))) AS dow,
    toHour(toTimeZone(created_at, 'America/Chicago'))              AS ct_hour,
    count()                                                        AS hour_imps
FROM adpx_impressions_details
PREWHERE toYYYYMM(created_at) >= toYYYYMM(
    toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 95 DAY
)
WHERE toDate(toTimeZone(created_at, 'America/Chicago'))
        >= toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 90 DAY
  AND toDate(toTimeZone(created_at, 'America/Chicago'))
        <  toDate(toTimeZone(now(), 'America/Chicago'))
GROUP BY ct_day, dow, ct_hour
""".strip()

    sess_sql = """
SELECT
    toDate(toTimeZone(created_at, 'America/Chicago'))              AS ct_day,
    toDayOfWeek(toDate(toTimeZone(created_at, 'America/Chicago'))) AS dow,
    toHour(toTimeZone(created_at, 'America/Chicago'))              AS ct_hour,
    count()                                                        AS hour_sess
FROM adpx_sdk_sessions
PREWHERE toYYYYMM(created_at) >= toYYYYMM(
    toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 95 DAY
)
WHERE toDate(toTimeZone(created_at, 'America/Chicago'))
        >= toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 90 DAY
  AND toDate(toTimeZone(created_at, 'America/Chicago'))
        <  toDate(toTimeZone(now(), 'America/Chicago'))
GROUP BY ct_day, dow, ct_hour
""".strip()

    # Accumulate per (day, dow, hour) counts then median by (dow, hour)
    # imp_by_day_hour[(dow, hour)] -> list of daily counts
    imp_acc: dict  = {dow: {h: [] for h in range(24)} for dow in range(1, 8)}
    sess_acc: dict = {dow: {h: [] for h in range(24)} for dow in range(1, 8)}

    try:
        imp_rows  = ch.query(imp_sql).result_rows
        sess_rows = ch.query(sess_sql).result_rows

        # Aggregate per (day, dow, hour) — each row is already one hour bucket
        imp_day: dict = {}   # (ct_day, dow, hour) -> count
        for ct_day, dow, ct_hour, cnt in imp_rows:
            key = (ct_day, int(dow), int(ct_hour))
            imp_day[key] = imp_day.get(key, 0) + int(cnt or 0)

        sess_day: dict = {}
        for ct_day, dow, ct_hour, cnt in sess_rows:
            key = (ct_day, int(dow), int(ct_hour))
            sess_day[key] = sess_day.get(key, 0) + int(cnt or 0)

        # Bucket into accumulators
        for (ct_day, dow, hour), cnt in imp_day.items():
            if 1 <= dow <= 7 and 0 <= hour <= 23:
                imp_acc[dow][hour].append(float(cnt))

        for (ct_day, dow, hour), cnt in sess_day.items():
            if 1 <= dow <= 7 and 0 <= hour <= 23:
                sess_acc[dow][hour].append(float(cnt))

        # Zero-fill: absent (day, hour) pairs had 0 traffic — including full-day
        # outages that produce no rows at all. Build from an authoritative 90-day
        # date list so the median isn't conditioned on "day had at least one event."
        from datetime import datetime as _dtf, timedelta as _tdf
        from zoneinfo import ZoneInfo as _ZIf
        _today_ct = _dtf.now(_ZIf("America/Chicago")).date()
        _all_ct_days = [(_today_ct - _tdf(days=i)) for i in range(1, 91)]
        for _d in _all_ct_days:
            _dow = _d.weekday() + 1  # Mon=1..Sun=7, matching toDayOfWeek()
            if 1 <= _dow <= 7:
                for h in range(24):
                    if (_d, _dow, h) not in imp_day:
                        imp_acc[_dow][h].append(0.0)
                    if (_d, _dow, h) not in sess_day:
                        sess_acc[_dow][h].append(0.0)

    except Exception as exc:
        log.warning("[CH] traffic baseline scan failed (non-fatal): %s", exc)

    from collections import defaultdict as _defaultdict
    traffic_by_dow: dict = _defaultdict(dict)
    for dow in range(1, 8):
        for h in range(24):
            traffic_by_dow[dow][h] = {
                "impressions_p50": _median(imp_acc[dow][h]),
                "sessions_p50":    _median(sess_acc[dow][h]),
            }

    data = {
        "share_by_dow":   share_by_dow,
        "traffic_by_dow": dict(traffic_by_dow),
        "dow_median":     dow_median,
        "sample_days":    sample_days,
    }
    _HOUR_CURVE_CACHE["data"] = data
    _HOUR_CURVE_CACHE["ts"] = now
    return data


def _revenue_at_hour(ch, target_date, max_hour: int | None) -> float:
    """Revenue for target_date up to (not including) max_hour CT; None = full day."""
    from datetime import date as _date
    if max_hour is None:
        hour_clause = ""
    else:
        hour_clause = f"  AND toHour(toTimeZone(created_at, 'America/Chicago')) < {max_hour}"
    sql = f"""
SELECT coalesce(sum(toFloat64OrNull(revenue)), 0) AS rev
FROM adpx_conversionsdetails
PREWHERE toYYYYMM(created_at) >= toYYYYMM(toDate('{target_date.isoformat()}'))
WHERE toDate(toTimeZone(created_at, 'America/Chicago')) = toDate('{target_date.isoformat()}')
{hour_clause}
""".strip()
    rows = ch.query(sql).result_rows
    return float(rows[0][0] or 0) if rows else 0.0


def _query_intraday_traffic(ch, today_ct, max_hour: int) -> dict:
    """Impressions and sessions for today up to max_hour CT."""
    imp_sql = """
SELECT count() AS imps
FROM adpx_impressions_details
PREWHERE toYYYYMM(created_at) >= toYYYYMM(toDate({date_str:String}))
WHERE toDate(toTimeZone(created_at, 'America/Chicago')) = toDate({date_str:String})
  AND toHour(toTimeZone(created_at, 'America/Chicago')) < {max_hour:UInt8}
""".strip()
    sess_sql = """
SELECT count() AS sess
FROM adpx_sdk_sessions
PREWHERE toYYYYMM(created_at) >= toYYYYMM(toDate({date_str:String}))
WHERE toDate(toTimeZone(created_at, 'America/Chicago')) = toDate({date_str:String})
  AND toHour(toTimeZone(created_at, 'America/Chicago')) < {max_hour:UInt8}
""".strip()
    params = {"date_str": today_ct.isoformat(), "max_hour": max_hour}
    imps  = int((ch.query(imp_sql,  parameters=params).result_rows or [[0]])[0][0])
    sess  = int((ch.query(sess_sql, parameters=params).result_rows or [[0]])[0][0])
    return {"impressions": imps, "sessions": sess}


def _classify_revenue_diagnostic(
    traffic: dict | None,
    t_band: dict | None,
    today_revenue: float,
    p50: float,
    dow_median_val,
    threshold: float = _REVENUE_DEVIATION_THRESHOLD,
) -> str | None:
    """Classify intraday revenue deviation as a named diagnostic label.

    Returns one of: "traffic", "efficiency", "traffic_upside", "on_track", or None
    (when traffic data or baseline is unavailable).
    """
    if not (traffic and t_band):
        return None

    imp_baseline = t_band.get("impressions_p50", 0)
    if imp_baseline <= 0:
        return None

    if traffic["impressions"] == 0:
        return "traffic"

    imp_dev = (traffic["impressions"] - imp_baseline) / imp_baseline
    denom = dow_median_val or today_revenue or 1.0
    rev_dev = (today_revenue / denom - p50) / p50

    if rev_dev < -threshold and abs(imp_dev) < threshold:
        return "efficiency"
    if rev_dev < -threshold and imp_dev < -threshold:
        return "traffic"
    if rev_dev > threshold and imp_dev > threshold:
        return "traffic_upside"
    return "on_track"


def project_today_revenue(ch) -> dict:
    """Project today's full-day platform revenue from the intraday total and a
    90-day hour-of-day cumulative-share curve. Purely additive — daemon code
    path `_query_intraday_revenue_total` is unchanged.

    Returns a dict with `status` always set:
        ok                  — projection available
        too_early           — current CT hour < 10 (curve thin at dawn)
        insufficient_history— sample_days for today's DOW < 4
        error               — internal failure (raised by helper to caller normally;
                              this status is reserved for the agent wrapper)

    Numeric fields are None when not applicable.

    Raises on ClickHouse error — callers must catch (see scout_agent wrapper).
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _Zi

    too_early_msg = "Too early to project reliably — ask after 10am CT."

    now_ct = _dt.now(_Zi("America/Chicago"))
    today_ct = now_ct.date()
    hour_ct = now_ct.hour
    as_of_ct = now_ct.strftime("%Y-%m-%d %H:%M %Z")
    # dow matches ClickHouse toDayOfWeek (Mon=1..Sun=7)
    py_weekday = now_ct.weekday()  # Mon=0..Sun=6
    dow = py_weekday + 1
    weekday_name = ["Monday", "Tuesday", "Wednesday", "Thursday",
                    "Friday", "Saturday", "Sunday"][py_weekday]

    base = {
        "status":                    "ok",
        "formatted":                 "",
        "today_revenue":             None,
        "projected_full_day":        None,
        "projected_low":             None,
        "projected_high":            None,
        "dow_median":                None,
        "pct_of_expected":           None,
        "as_of_ct":                  as_of_ct,
        "hour_ct":                   hour_ct,
        "curve_share":               None,
        "curve_source":              None,
        "projection_n":              0,
        "sample_days":               0,
        "warning":                   None,
        "weekday":                   weekday_name,
        "diagnostic":                None,
        "traffic_impressions_today": 0,
        "traffic_sessions_today":    0,
    }

    if hour_ct < 10:
        base["status"] = "too_early"
        base["formatted"] = too_early_msg
        return base

    # Today's revenue from CT midnight to now — via shared helper.
    today_revenue = _revenue_at_hour(ch, today_ct, hour_ct)
    base["today_revenue"] = today_revenue

    curve = _build_hour_curve(ch)
    sample = int(curve["sample_days"].get(dow, 0))
    base["sample_days"] = sample

    # _revenue_at_hour fetches hours < hour_ct (i.e. 0..hour_ct-1).
    # Curve slot [h] = cumulative-through-hour-h (inclusive), so the matching
    # slot is hour_ct-1. Using hour_ct directly would compare incomplete-hour
    # revenue against a curve that includes one extra hour, biasing projections low.
    curve_hour = hour_ct - 1

    band = curve["share_by_dow"].get(dow, {}).get(curve_hour)
    dow_median = float(curve["dow_median"].get(dow, 0) or 0)
    base["dow_median"] = dow_median if dow_median > 0 else None

    if sample < 4:
        base["status"] = "insufficient_history"
        base["formatted"] = (
            f"Not enough same-{weekday_name} history to project reliably "
            f"({sample} qualifying days in last 90; need 4)."
        )
        return base

    # Pick curve band; fall back if missing, p50 implausibly small, or p25/p75
    # are non-positive (would cause ZeroDivisionError in projected_low/high).
    if (band is None or band["p50"] < 0.01
            or band["p25"] <= 0 or band["p75"] <= 0
            or not (band["p25"] <= band["p50"] <= band["p75"])):
        p50, p25, p75 = 0.70, 0.65, 0.75    # conservative fallback band
        curve_source = "fallback_0.70"
        projection_n = 0
        base["warning"] = "Hour-of-day curve unavailable; used fallback 0.70."
    else:
        p50, p25, p75 = band["p50"], band["p25"], band["p75"]
        curve_source = "90d"
        projection_n = band["n"]

    base["curve_source"]  = curve_source
    base["projection_n"]  = projection_n
    base["curve_share"]   = round(float(p50), 4)

    projected_full_day = today_revenue / p50
    projected_low      = today_revenue / p75   # pessimistic: at 75th-pct share pace
    projected_high     = today_revenue / p25   # optimistic: at 25th-pct share pace

    base["projected_full_day"] = projected_full_day
    base["projected_low"]      = projected_low
    base["projected_high"]     = projected_high

    if dow_median > 0:
        base["pct_of_expected"] = round(100.0 * projected_full_day / dow_median, 1)

    # ── Diagnostic classification ─────────────────────────────────────────────
    traffic_impressions_today = 0
    traffic_sessions_today    = 0

    try:
        traffic = _query_intraday_traffic(ch, today_ct, hour_ct)
        traffic_impressions_today = traffic["impressions"]
        traffic_sessions_today    = traffic["sessions"]
    except Exception:
        traffic = None   # CH busy or table unavailable — degrade gracefully

    t_band = curve.get("traffic_by_dow", {}).get(dow, {}).get(curve_hour)
    diagnostic = _classify_revenue_diagnostic(
        traffic, t_band, today_revenue, p50, curve["dow_median"].get(dow)
    )

    base["diagnostic"]                = diagnostic
    base["traffic_impressions_today"] = traffic_impressions_today
    base["traffic_sessions_today"]    = traffic_sessions_today

    return base


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


def _query_cvr_anomaly(
    ch,
    drop_pct: float = None,
    min_payout: float = None,
    min_impressions_7d: int = None,
) -> list[dict]:
    """Thin wrapper — reads thresholds from config; per-call overrides take precedence."""
    t = _tm.load().get("signals", {})
    return _q.cvr_anomaly(
        ch,
        drop_pct=float(drop_pct if drop_pct is not None else t.get("cvr_anomaly_drop_pct", 30)),
        min_payout=float(min_payout if min_payout is not None else t.get("cvr_anomaly_min_payout", 50)),
        min_impressions_7d=int(min_impressions_7d if min_impressions_7d is not None else t.get("cvr_anomaly_min_impressions_7d", 5000)),
    )


def _query_expiring_campaigns(ch, warning_days: int = None) -> list[dict]:
    """Thin wrapper — reads warning_days from config; per-call override takes precedence."""
    t = _tm.load().get("signals", {})
    return _q.expiring_campaigns(
        ch,
        warning_days=int(warning_days if warning_days is not None else t.get("expiration_warning_days", 7)),
    )


def _query_publisher_revenue_trends(ch, days: int = 7) -> list[dict]:
    """Thin wrapper — reads min_periods from config and delegates to queries.publisher_revenue_trends()."""
    t = _tm.load().get("signals", {})
    return _q.publisher_revenue_trends(
        ch,
        days=days,
        min_periods=int(t.get("revenue_trend_min_periods", 4)),
    )


def _query_advertiser_revenue_trends(ch, days: int = 7) -> list[dict]:
    """Thin wrapper — reads min_periods from config and delegates to queries.advertiser_revenue_trends()."""
    t = _tm.load().get("signals", {})
    return _q.advertiser_revenue_trends(
        ch,
        days=days,
        min_periods=int(t.get("revenue_trend_min_periods", 4)),
    )


def _query_revenue_sparkline_series(ch) -> list[tuple]:
    """
    Return 7 days of prior daily revenue totals for sparkline rendering.

    Excludes today — used to show the trailing trend behind today's intraday
    number. Returns list of (date, rev_float) tuples ordered oldest → newest.
    """
    sql = """
SELECT
    toDate(toTimeZone(created_at, 'America/Chicago')) AS day,
    sum(toFloat64OrNull(revenue)) AS day_rev
FROM adpx_conversionsdetails
PREWHERE toYYYYMM(created_at) >= toYYYYMM(
    toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 8 DAY
)
WHERE toDate(toTimeZone(created_at, 'America/Chicago'))
      >= toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 6 DAY
  AND toDate(toTimeZone(created_at, 'America/Chicago'))
      < toDate(toTimeZone(now(), 'America/Chicago'))
GROUP BY day
ORDER BY day ASC
"""
    return ch.query(sql).result_rows
