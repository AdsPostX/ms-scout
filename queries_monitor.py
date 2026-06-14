"""ClickHouse queries — monitor signal queries for the polling loop (cap, velocity, fill-rate)."""
from __future__ import annotations

import logging
import re


def _validate_as_of_date(as_of_date: str) -> None:
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', as_of_date):
        raise ValueError(f"as_of_date must be YYYY-MM-DD, got: {as_of_date!r}")


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

    if as_of_date:
        _validate_as_of_date(as_of_date)
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
                SELECT toInt64(pid) AS user_id, count(DISTINCT session_id) AS sessions_with_imps
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
    if as_of_date:
        _validate_as_of_date(as_of_date)
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
        logging.getLogger(__name__).warning(f"velocity_alerts phase-1 failed: {e}")
        return []

    # Collect as (raw_delta, result_dict) pairs so raw value never enters the public dict.
    pairs: list[tuple[float, dict]] = []
    for row in rows:
        pub_id, pub_name, rev_30d, rev_7d = row
        if rev_30d <= 0:
            continue
        rev_7d_ann = (float(rev_7d) / 7) * 30
        pct_delta_raw = (rev_7d_ann - float(rev_30d)) / float(rev_30d) * 100
        if down_threshold_pct < pct_delta_raw < up_threshold_pct:
            continue  # within normal range — skip
        direction = "up" if pct_delta_raw > 0 else "down"
        pairs.append((
            pct_delta_raw,
            {
                "publisher_id": int(pub_id),
                "publisher_name": pub_name or str(pub_id),
                "rev_30d": round(float(rev_30d), 2),
                "rev_7d": round(float(rev_7d), 2),
                "pct_delta": round(pct_delta_raw, 1),
                "direction": direction,
                "advertisers": [],
            },
        ))

    # Top 5 by absolute delta magnitude (raw so sort is not affected by rounding).
    pairs.sort(key=lambda x: abs(x[0]), reverse=True)
    pairs = pairs[:5]
    if not pairs:
        return []

    candidates = [p for _, p in pairs]

    # Phase 2 — advertiser attribution enrichment for the top candidates.
    # Gate on raw unrounded value so 99.96 does not cross the >=100 threshold after rounding.
    enrich_ids = [p[1]["publisher_id"] for p in pairs if abs(p[0]) >= 100]
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
            logging.getLogger(__name__).warning(f"velocity_alerts phase-2 advertiser enrichment failed: {e}")

    return candidates


def cap_alert_campaigns(
    ch,
    as_of_date=None,
    cap_alert_pct: float = 85.0,
    campaign_id=None,
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
        campaign_id:    Optional int — narrow to a single campaign by its ID.

    Returns:
        List of dicts, each with keys:
          adv_name (str), campaign_id (int), monthly_cap (float), revenue_mtd (float),
          cap_pct (float), days_remaining (int), days_to_cap (float|None)
    """
    import json as _json
    import datetime as _dt

    if as_of_date:
        _validate_as_of_date(as_of_date)
    # Bug C fix: compute effective date once in Python before the SQL call so that
    # SQL date filtering and Python day-math always use the same calendar date.
    today = _dt.date.fromisoformat(as_of_date) if as_of_date else _dt.date.today()
    date_expr = f"toDate('{today.isoformat()}')"
    # Bug B fix: parameter renamed from advertiser_id to campaign_id — c.id is campaign ID.
    adv_filter = f"AND c.id = {int(campaign_id)}" if campaign_id else ""

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
        logging.getLogger(__name__).warning(f"cap_alert_campaigns failed: {e}")
        return []

    days_in_month = (today.replace(day=28) + _dt.timedelta(days=4)).replace(day=1) - _dt.timedelta(days=1)
    days_remaining = (days_in_month - today).days + 1

    results = []
    for row in rows:
        cid, adv_name, cap_cfg, revenue_mtd = row
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
                "campaign_id": int(cid),
                "monthly_cap": round(mb, 2),
                "revenue_mtd": round(float(revenue_mtd), 2),
                "cap_pct": round(cap_pct * 100, 1),
                "days_remaining": int(days_remaining),
                "days_to_cap": round(days_to_cap, 1) if days_to_cap is not None else None,
            }
        )

    results.sort(key=lambda x: x["cap_pct"], reverse=True)
    return results
