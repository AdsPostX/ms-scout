from __future__ import annotations

# Standard library
import calendar
import datetime as _dt_mod
import json
import logging
import re
from datetime import date

# Third-party
from zoneinfo import ZoneInfo

# Local — NO scout_agent imports (circular)
import queries as _q
from scout_ch import (
    _get_ch_client,
    _run_parallel,
    _query_advertiser_revenue_trends,
    _query_revenue_sparkline_series,
    project_today_revenue,
)
from scout_ch import CHBusyError
from scout_thresholds import _manager

log = logging.getLogger("scout_agent")


# ── Revenue formatter ─────────────────────────────────────────────────────────

def _fmt_rev(amount: float | None) -> str:
    """Format a revenue float as a compact human-readable dollar string."""
    if amount is None:
        return "$?"
    if amount >= 10_000:
        return f"${round(amount / 100) * 100 / 1000:.0f}K"
    if amount >= 1_000:
        rounded = round(amount / 100) * 100
        return f"${rounded:,.0f}"
    return f"${amount:,.0f}"


def _fmt_rev_short(v: float) -> str:
    """Compact $XK / $X format for inline table cells (no rounding to nearest $100)."""
    v = float(v)
    return f"${v / 1000:.0f}K" if v >= 1000 else f"${v:.0f}"


def _process_cap_rows(
    cap_rows: list,
    month_end: date,
) -> tuple[list[str], list[str], float | None]:
    """Return (cap_warnings, end_date_warnings, monthly_cap_total) from raw CH rows."""
    cap_warnings: list[str] = []
    end_date_warnings: list[str] = []
    monthly_cap_total: float | None = None

    for row in cap_rows:
        cid, _adv, end_dt, cap_cfg = row
        if end_dt and end_dt < month_end:
            end_date_warnings.append(
                f"Campaign {cid} ends {end_dt} — won't run full month"
            )
        if cap_cfg:
            try:
                cfg = json.loads(cap_cfg) if isinstance(cap_cfg, str) else cap_cfg
                mb = (cfg.get("month") or {}).get("budget")
                if mb and float(mb) > 0:
                    cap_warnings.append(
                        f"Campaign {cid}: ${float(mb):,.0f} monthly budget cap"
                    )
                    monthly_cap_total = (monthly_cap_total or 0) + float(mb)
            except Exception as exc:
                log.debug("_process_cap_rows swallowed: %s", exc)

    return cap_warnings, end_date_warnings, monthly_cap_total


# ── Revenue tool functions ────────────────────────────────────────────────────

def get_pulse_summary() -> dict:
    """
    Return a summary of which monitoring signals have fired today.
    Reads from per-signal state keys written by the individual monitor daemons.

    Returns:
      has_pulse (bool): True if any signal fired today
      had_content (bool): same as has_pulse
      fired_today (dict): per-signal boolean — which fired today
      currently_active (list[str]): alert names currently firing in the registry
      message (str): human-readable summary
    """
    try:
        # alert_registry imported locally — it initialises the registry on import and
        # must not run at module load time (it has side-effects / may not be available
        # in test environments that only import scout_agent without the daemon running).
        import alert_registry as _ar

        state = _manager.load_pulse_state()
        today = _dt_mod.datetime.now(ZoneInfo("America/Chicago")).date().isoformat()

        # Cap: after Phase 2, key is last_cap_alert_slot (YYYY-MM-DDTHH prefix)
        # Before Phase 2, key is last_cap_alert_date (YYYY-MM-DD)
        _cap_slot = state.get("last_cap_alert_slot", "")
        _cap_date = state.get("last_cap_alert_date", "")
        cap_fired = (_cap_slot[:10] == today) if _cap_slot else (_cap_date == today)

        fired_today = {
            "cap": cap_fired,
            "velocity_down": state.get("last_velocity_down_alert_date") == today,
            "ghost": state.get("last_ghost_alert_date") == today,
            "fill_rate": state.get("last_fill_alert_date") == today,
            "cvr_anomaly": state.get("last_cvr_anomaly_alert_date") == today,
            "expiration": state.get("last_expiration_alert_date") == today,
        }

        any_fired = any(fired_today.values())

        try:
            currently_active = [s.alert_name for s in _ar.current_state()]
        except Exception:
            currently_active = []

        if any_fired:
            fired_names = [k for k, v in fired_today.items() if v]
            msg = "Today's signals fired: " + ", ".join(fired_names)
        else:
            msg = "No monitoring signals have fired today yet."

        return {
            "has_pulse": any_fired,
            "had_content": any_fired,
            "fired_today": fired_today,
            "currently_active": currently_active,
            "message": msg,
        }
    except Exception as e:
        log.warning(f"get_pulse_summary failed: {e}")
        return {"has_pulse": False, "message": f"Could not read pulse state: {e}"}


def get_advertiser_revenue_projection(
    advertiser_name: str,
    month: str = None,
) -> dict:
    """
    Project gross revenue for an advertiser across all MS publishers for a target month.

    Uses last 30 days as the baseline (avg daily revenue × days in month).
    Checks:
      - Campaign end dates (excludes/warns on campaigns ending before month)
      - Monthly budget caps from capping_config JSON
    Returns projected totals, publisher breakdown, cap warnings, end-date warnings.
    """
    ch = _get_ch_client()
    today = date.today()

    # ── Parse target month ────────────────────────────────────────────────────
    target_year, target_month_num = today.year, today.month + 1
    if target_month_num > 12:
        target_month_num, target_year = 1, today.year + 1

    if month:
        m = re.search(r'(\d{4})[/-](\d{1,2})', month)
        if m:
            target_year, target_month_num = int(m.group(1)), int(m.group(2))
        else:
            month_map = {n.lower(): i for i, n in enumerate(calendar.month_name) if n}
            for name, num in month_map.items():
                if name in month.lower():
                    target_month_num = num
                    yr = re.search(r'\d{4}', month)
                    if yr:
                        target_year = int(yr.group())
                    break

    days_in_month = calendar.monthrange(target_year, target_month_num)[1]
    month_start   = date(target_year, target_month_num, 1)
    month_end     = date(target_year, target_month_num, days_in_month)
    month_label   = f"{calendar.month_name[target_month_num]} {target_year}"

    # ── Steps 1 + 2 run in parallel — both depend only on advertiser_name ───────
    # Step 1: 30-day baseline — impressions + revenue per publisher
    # Step 2: Campaign end dates + monthly caps
    def _fetch_baseline():
        return ch.query(
            """
            SELECT
                cast(i.pid AS String)          AS publisher_pid,
                any(u.organization)            AS publisher_name,
                count()                        AS impressions_30d,
                count(DISTINCT i.session_id)   AS sessions_30d,
                coalesce(sum(toFloat64OrNull(cd.revenue)), 0) AS revenue_30d,
                coalesce(sum(toFloat64OrNull(cd.payout)),  0) AS payout_30d,
                count(DISTINCT cd.id)           AS conversions_30d,
                coalesce(any(clicks_agg.clicks_30d), 0) AS clicks_30d
            FROM adpx_impressions_details i
            JOIN from_airbyte_campaigns c
                ON i.campaign_id = cast(c.id AS UInt64)
            LEFT JOIN from_airbyte_users u
                ON i.pid = toString(u.id)
            LEFT JOIN adpx_conversionsdetails cd
                ON i.session_id = cd.session_id
                AND cd.campaign_id = i.campaign_id
                AND toYYYYMM(cd.created_at) >= toYYYYMM(today() - 44)
            LEFT JOIN (
                SELECT cast(tc.user_id AS String) AS pub_id, count() AS clicks_30d
                FROM adpx_tracked_clicks tc
                INNER JOIN from_airbyte_campaigns c2 ON tc.campaign_id = cast(c2.id AS UInt64)
                WHERE c2.adv_name ILIKE %(adv)s
                  AND c2.deleted_at IS NULL
                  AND tc.created_at >= today() - 30
                GROUP BY tc.user_id
            ) clicks_agg ON clicks_agg.pub_id = cast(i.pid AS String)
            WHERE c.adv_name ILIKE %(adv)s
              AND c.deleted_at IS NULL
              AND i.created_at >= today() - 30
              AND toYYYYMM(i.created_at) >= toYYYYMM(today() - 30)
            GROUP BY publisher_pid
            ORDER BY revenue_30d DESC
            LIMIT 30
            """,
            parameters={"adv": f"%{advertiser_name}%"},
        ).result_rows

    def _fetch_cap_data():
        return ch.query(
            """
            SELECT id, adv_name, end_date, capping_config
            FROM from_airbyte_campaigns
            WHERE adv_name ILIKE %(adv)s
              AND deleted_at IS NULL
              AND (end_date IS NULL OR end_date >= %(month_start)s)
            """,
            parameters={"adv": f"%{advertiser_name}%", "month_start": str(month_start)},
        ).result_rows

    baseline_rows = []
    cap_rows = []
    try:
        baseline_rows, cap_rows = _run_parallel([_fetch_baseline, _fetch_cap_data])
    except Exception as e:
        log.warning(f"get_advertiser_revenue_projection parallel fetch failed: {e}")
        try:
            baseline_rows = _fetch_baseline()
        except Exception as e2:
            log.warning(f"get_advertiser_revenue_projection baseline failed: {e2}")
            return {"error": str(e2), "advertiser": advertiser_name, "month": month_label}
        try:
            cap_rows = _fetch_cap_data()
        except Exception as e2:
            log.warning(f"get_advertiser_revenue_projection cap query failed: {e2}")

    if not baseline_rows:
        return {
            "advertiser": advertiser_name,
            "month": month_label,
            "error": f"No impression data for '{advertiser_name}' in the last 30 days. Check spelling or try a partial name.",
        }

    # ── Process cap/end-date results ──────────────────────────────────────────
    cap_warnings, end_date_warnings, monthly_cap_total = _process_cap_rows(
        cap_rows, month_end
    )

    # ── Step 3: Projection ────────────────────────────────────────────────────
    total_revenue_30d     = sum(r[4] for r in baseline_rows)
    total_payout_30d      = sum(r[5] for r in baseline_rows)
    total_impressions_30d = sum(r[2] for r in baseline_rows)
    total_sessions_30d    = sum(r[3] for r in baseline_rows)
    total_conversions_30d = sum(r[6] for r in baseline_rows)
    total_clicks_30d      = sum(r[7] for r in baseline_rows)

    uncapped_projected_revenue = round((total_revenue_30d / 30) * days_in_month, 2)
    uncapped_projected_payout  = round((total_payout_30d  / 30) * days_in_month, 2)

    cap_applied = bool(monthly_cap_total and uncapped_projected_revenue > monthly_cap_total)
    projected_revenue = monthly_cap_total if cap_applied else uncapped_projected_revenue
    projected_payout  = uncapped_projected_payout  # payout cap not modeled separately

    by_publisher = []
    for row in baseline_rows[:10]:
        pub_pid, pub_name, impr, sess, rev, pay, convs, clicks = row
        by_publisher.append({
            "publisher":          pub_name or f"Partner {pub_pid}",
            "impressions_30d":    impr,
            "revenue_30d":        round(rev, 2),
            "projected_revenue":  round((rev / 30) * days_in_month, 2),
            "conversions_30d":    convs,
            "share_pct":          round(rev / total_revenue_30d * 100, 1) if total_revenue_30d else 0,
        })

    result = {
        "advertiser":                advertiser_name,
        "month":                     month_label,
        "days_in_month":             days_in_month,
        "publisher_count":           len(baseline_rows),
        # Actuals (30-day)
        "revenue_30d":               round(total_revenue_30d, 2),
        "payout_30d":                round(total_payout_30d, 2),
        "impressions_30d":           total_impressions_30d,
        "conversions_30d":           total_conversions_30d,
        # Projections
        "projected_revenue":         round(projected_revenue, 2),
        "uncapped_projected_revenue": uncapped_projected_revenue,
        "projected_payout":          round(projected_payout, 2),
        "projected_impressions":     int((total_impressions_30d / 30) * days_in_month),
        "cap_applied":               cap_applied,
        "monthly_cap_total":         monthly_cap_total,
        # Performance metrics (used for payout impact math)
        "avg_daily_revenue":         round(total_revenue_30d / 30, 2),
        "avg_rpm":                   round(total_revenue_30d / max(total_impressions_30d, 1) * 1000, 4),
        "avg_cvr":                   round(total_conversions_30d / max(total_clicks_30d, 1) * 100, 4),
        # Breakdown + warnings
        "by_publisher":              by_publisher,
        "cap_warnings":              cap_warnings,
        "end_date_warnings":         end_date_warnings,
        "methodology":               "30-day avg daily revenue × days in month. Cap applied where monthly_cap_total < uncapped projection.",
        "data_quality":              _manager.data_quality_tier(30, total_sessions_30d),
    }

    # Build formatted Slack output for LLM synthesis
    _lines = []
    if result.get("cap_applied"):
        _monthly_cap = result.get("monthly_cap_total", 0)
        _avg_daily = result.get("avg_daily_revenue", 0)
        _uncapped = result.get("uncapped_projected_revenue", 0)
        _delta = (_uncapped or 0) - (_monthly_cap or 0)
        _adv = result.get("advertiser", advertiser_name)
        _lines.append(
            f":red_circle: *Budget cap is the story.* {_adv} capped at "
            f"*${_monthly_cap:,.0f}*/mo — run rate *${_avg_daily:,.0f}/day* "
            f"(~${_uncapped:,.0f} uncapped). "
            f":zap: Lift cap or spin uncapped campaign to unlock ~${_delta:,.0f}."
        )
    else:
        _proj = result.get("projected_revenue", 0)
        _avg = result.get("avg_daily_revenue", 0)
        _adv = result.get("advertiser", advertiser_name)
        _m = result.get("month", month_label)
        _lines.append(
            f"{_adv} projects *${_proj:,.0f}* for {_m} at *${_avg:,.0f}/day*."
        )
    # Publisher breakdown top 5
    _breakdown = result.get("by_publisher", [])
    if _breakdown:
        _lines.append("")
        for _pub in _breakdown[:5]:
            _pname = _pub.get("publisher", "Unknown")
            _prev = _pub.get("revenue_30d", 0)
            _pshare = _pub.get("share_pct", 0)
            _lines.append(f"• {_pname}: ${_prev:,.0f} ({_pshare:.0f}%)")
    result["formatted"] = "\n".join(_lines)

    return result


def get_top_revenue_opportunities() -> str:
    """
    Return the top cross-publisher revenue gap opportunities.
    Finds high-performing advertisers (2+ publishers, >$10K/30d) not active
    in high-volume publishers (>100K sessions/30d). Ranked by estimated monthly revenue.
    """
    ch = _get_ch_client()
    try:
        data = _q.revenue_opportunities(ch)
    except Exception as e:
        return f"Revenue opportunities query failed: {e}"

    if not data:
        return (
            "*Revenue Opportunity Report*\n\n"
            ":white_check_mark: No obvious cross-publisher gaps detected — "
            "all high-performing advertisers appear active in major publishers."
        )

    total_est = sum(row["est_monthly_rev"] for row in data)
    lines = [
        f"*Revenue Opportunity Report — Cross-Publisher Gaps* ({len(data)} opportunities)\n",
        f"Total estimated monthly revenue at risk: *${total_est / 1000:.0f}K/mo*\n",
        "_Advertisers already earning on 2+ publishers — the revenue pattern is proven, "
        "the distribution gap is the opportunity._\n",
    ]

    for row in data:
        pub_name  = row["publisher_name"]
        pub_id    = row["publisher_id"]
        adv_name  = row["adv_name"]
        adv_rev   = row["adv_total_rev_30d"]
        est_rev   = row["est_monthly_rev"]
        pub_count = row["adv_pub_count"]
        sessions  = row["sessions_30d"]
        sessions_str = f"{int(sessions) / 1_000_000:.1f}M" if sessions >= 1_000_000 else f"{int(sessions) / 1000:.0f}K"
        adv_rev_str  = _fmt_rev_short(adv_rev)
        est_rev_str  = _fmt_rev_short(est_rev)
        lines.append(
            f"• Add *{adv_name}* → *{pub_name or f'Pub #{pub_id}'}*\n"
            f"  {adv_name} earns {adv_rev_str}/30d across {pub_count} publishers · est. *{est_rev_str}/mo* if added\n"
            f"  {pub_name}: {sessions_str} sessions/30d · not currently running this advertiser"
        )

    lines.append(
        "\n:zap: Prioritize by session volume × avg revenue. "
        "Confirm no geo/OS exclusions exist before requesting provisioning."
    )
    return "\n".join(lines)


def get_revenue_today() -> dict:
    """
    Return today's intraday revenue by publisher vs 30-day rolling average.
    Returns formatted Slack mrkdwn in the 'formatted' key for LLM synthesis.

    Format spec:
        *$14K today*, 58% of daily avg. Still early.
        ---
        🟢 *AT&T* · $3,400 · 343 conversions
        🟡 *AT&T Buy Flow* · $515 · 41 conversions
        > 4 others · $1,155 combined
        ---
        ⚠️ *TuitionHero*: invalid conversions. $5,500 excluded until ops confirms netting.

    Signal thresholds vs publisher 30-day avg:
        🟢 ≥ 80%   🟡 40–79%   🔴 < 40%

    Revenue rounding:
        ≥ $10K → $XK (nearest $100)   ≥ $1K → $X,X00 (nearest $100)   < $1K → exact
    """
    def _signal(today_rev: float, avg_rev: float) -> str:
        if avg_rev <= 0:
            return "🟢"
        pct = today_rev / avg_rev
        if pct >= 0.80:
            return "🟢"
        elif pct >= 0.40:
            return "🟡"
        return "🔴"

    try:
        ch = _get_ch_client()

        # Let ClickHouse own timezone math — avoids DST drift from Python UTC offset
        today_sql = """
SELECT
    c.user_id,
    u.organization AS publisher_name,
    sum(toFloat64OrNull(c.revenue)) AS today_rev,
    count() AS conversions
FROM adpx_conversionsdetails c
LEFT JOIN from_airbyte_users u ON u.id = toInt64(c.user_id)
PREWHERE toYYYYMM(c.created_at) = toYYYYMM(toDate(toTimeZone(now(), 'America/Chicago')))
WHERE toDate(toTimeZone(c.created_at, 'America/Chicago'))
      = toDate(toTimeZone(now(), 'America/Chicago'))
GROUP BY c.user_id, u.organization
HAVING today_rev > 0
ORDER BY today_rev DESC
"""

        # 30-day avg divided by 30 calendar days (includes zero-revenue days in denominator)
        avg_sql = """
SELECT
    c.user_id,
    sum(toFloat64OrNull(c.revenue)) / 30 AS avg_daily_rev
FROM adpx_conversionsdetails c
PREWHERE toYYYYMM(c.created_at) >= toYYYYMM(
    toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 35 DAY
)
WHERE toDate(toTimeZone(c.created_at, 'America/Chicago'))
      >= toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 30 DAY
  AND toDate(toTimeZone(c.created_at, 'America/Chicago'))
      < toDate(toTimeZone(now(), 'America/Chicago'))
GROUP BY c.user_id
"""

        today_rows = ch.query(today_sql).result_rows
        avg_rows = ch.query(avg_sql).result_rows
        series_rows = _query_revenue_sparkline_series(ch)

        # Build avg lookup: user_id → avg_daily_rev
        avg_lookup: dict[int, float] = {int(r[0]): float(r[1] or 0) for r in avg_rows}

        # Today rows: (user_id, publisher_name, today_rev, conversions)
        publishers = []
        total_today = 0.0
        total_avg = 0.0
        for row in today_rows:
            uid = int(row[0])
            name = (row[1] or "").strip() or "Unknown Partner"
            rev = float(row[2] or 0)
            convs = int(row[3] or 0)
            avg = avg_lookup.get(uid, 0.0)
            publishers.append({
                "uid": uid,
                "name": name,
                "rev": rev,
                "convs": convs,
                "avg": avg,
                "signal": _signal(rev, avg),
            })
            total_today += rev
            total_avg += avg

        # Build sparkline from 7-day series (cents for _build_sparkline_url)
        from scout_ui_kit import _build_sparkline_url
        _series_cents = [int(float(r[1] or 0) * 100) for r in series_rows]
        _sparkline_url = _build_sparkline_url(_series_cents) or ""

        # Empty / early state
        if not publishers:
            return {
                "formatted": "_No revenue data yet today. Check back after 9am CT._",
                "chart_url": _sparkline_url,
            }

        # Headline
        pct_of_avg = (total_today / total_avg * 100) if total_avg > 0 else None
        import datetime as _dt_now
        from zoneinfo import ZoneInfo as _ZoneInfo
        _now_ct = _dt_now.datetime.now(_ZoneInfo("America/Chicago"))
        hour_ct = _now_ct.hour
        time_note = "Still early." if hour_ct < 12 else ("Midday pace." if hour_ct < 17 else "")
        headline_pct = f", {pct_of_avg:.0f}% of daily avg. {time_note}".strip() if pct_of_avg else "."
        headline = f"*{_fmt_rev(total_today)} today*{headline_pct}"

        # Top 3 inline, remainder grouped
        lines: list[str] = [headline, "---"]
        top3 = publishers[:3]
        rest = publishers[3:]
        for p in top3:
            lines.append(f"{p['signal']} *{p['name']}* · {_fmt_rev(p['rev'])} · {p['convs']:,} conversions")

        if rest:
            rest_total = sum(p["rev"] for p in rest)
            lines.append(f"> {len(rest)} others · {_fmt_rev(rest_total)} combined")

        return {"formatted": "\n".join(lines), "chart_url": _sparkline_url}

    except Exception as e:
        if isinstance(e, CHBusyError):
            raise
        log.exception("get_revenue_today failed")
        return {
            "formatted": "⚠️ Revenue data unavailable — query failed. Try again or check ClickHouse.",
            "chart_url": "",
        }


def get_revenue_today_projection() -> dict:
    """
    Project today's end-of-day revenue using a 60-day hour-of-day arrival curve
    and 8-week same-weekday median baseline. Returns formatted Slack mrkdwn
    in the 'formatted' key for LLM synthesis.

    Status dispatch:
        ok                    → "Projected EOD: *$X* (range $Y-$Z based on ±10% pace error).
                                 Currently *$A* — tracking *B%* of typical for this hour.
                                 Typical {weekday} lands ~*$C*."
        too_early             → formatted helper string (before 10am CT)
        insufficient_history  → formatted helper string
        unstable / error      → formatted helper string
    """
    try:
        ch = _get_ch_client()
        result = project_today_revenue(ch)
        status = result.get("status")

        if status != "ok":
            formatted = result.get("formatted") or "⚠️ Projection unavailable."
            return {"formatted": formatted}

        projected = result.get("projected_full_day") or 0.0
        today_rev = result.get("today_revenue") or 0.0
        dow_median = result.get("dow_median")
        pct_expected = result.get("pct_of_expected")
        as_of_ct = result.get("as_of_ct", "")

        # Use p25/p75 historical bands when available; fall back to ±10% arithmetic band
        low  = result.get("projected_low",  projected * 0.90)
        high = result.get("projected_high", projected * 1.10)

        import datetime as _dt
        from zoneinfo import ZoneInfo as _ZI
        weekday = _dt.datetime.now(_ZI("America/Chicago")).strftime("%A")

        # Band sample-size note
        n = result.get("projection_n", 0)
        wd = result.get('weekday', weekday)
        band_note = f" ({n} historical {wd}{'s' if n != 1 else ''})" if n > 0 else ""

        pace_line = (
            f"Currently *{_fmt_rev(today_rev)}* — tracking *{pct_expected:.0f}%* of typical for this hour."
            if pct_expected is not None
            else f"Currently *{_fmt_rev(today_rev)}*."
        )
        median_line = (
            f"Typical {weekday} lands ~*{_fmt_rev(dow_median)}*."
            if dow_median
            else ""
        )

        # Diagnostic labels (experimental)
        diag_labels = {
            "efficiency":     "⚠ Traffic normal, revenue soft — conversion efficiency issue (experimental)",
            "traffic":        "⚠ Both revenue and traffic below baseline — volume issue (experimental)",
            "traffic_upside": "↑ Traffic and revenue both running ahead of baseline (experimental)",
            "on_track":       "✓ Revenue and traffic are tracking close to baseline (experimental)",
        }
        diag = result.get("diagnostic", "")
        diag_line = diag_labels.get(diag, "")

        lines = [
            f"Projected EOD: *{_fmt_rev(projected)}* (range {_fmt_rev(low)}-{_fmt_rev(high)}{band_note}).",
            pace_line,
        ]
        if median_line:
            lines.append(median_line)
        if diag_line:
            lines.append(diag_line)
        if as_of_ct:
            lines.append(f"_As of {as_of_ct} CT._")

        warning = result.get("warning")
        if warning:
            lines.append(f"⚠️ {warning}")

        return {"formatted": "\n".join(lines)}

    except Exception:
        log.exception("get_revenue_today_projection failed")
        return {
            "formatted": "⚠️ Projection unavailable — query failed. Try again or check ClickHouse.",
        }


def get_publisher_revenue_trends(days: int = 7) -> dict:
    """
    Publisher velocity alerts: publishers with significant revenue change vs prior 30-day baseline.
    Uses canonical annualized comparison: ((rev_7d/7)*30 - rev_30d) / rev_30d * 100.
    Threshold: -25% down, +20% up (from config/scout_thresholds.json).

    Replaces period-median algorithm (publisher_revenue_trends deprecated). Both up and
    down publishers are returned with a 'direction' field ('up'|'down').

    Note: the `days` parameter is accepted for backward compatibility but ignored —
    the canonical function uses a fixed 7-day/30-day window.
    """
    try:
        ch = _get_ch_client()
        rows = _q.velocity_alerts(ch)
        if not rows:
            return {"trends": [], "count": 0, "summary": "No publisher velocity anomalies detected."}
        down = [r for r in rows if r["direction"] == "down"]
        up = [r for r in rows if r["direction"] == "up"]
        return {
            "trends": rows,
            "count": len(rows),
            "down_count": len(down),
            "up_count": len(up),
            "summary": f"{len(rows)} publishers with velocity anomalies: {len(down)} down, {len(up)} up.",
        }
    except Exception as e:
        log.exception("get_publisher_revenue_trends failed")
        return {"error": str(e), "trends": []}


def get_advertiser_revenue_trends(days: int = 7) -> dict:
    """
    Advertiser revenue trends: actual vs. historical median, aggregated cross-publisher.
    Uses period-median algorithm (no canonical advertiser velocity function yet; see
    velocity_alerts() in queries.py for publisher-level canonical annualized velocity).
    """
    try:
        ch = _get_ch_client()
        rows = _query_advertiser_revenue_trends(ch, days=int(days))
        if not rows:
            return {"trends": [], "count": 0, "days": days, "summary": "No advertiser revenue trend data available."}
        down = [r for r in rows if r["trend"] == "down"]
        up = [r for r in rows if r["trend"] == "up"]
        return {
            "trends": rows,
            "count": len(rows),
            "days": days,
            "down_count": len(down),
            "up_count": len(up),
            "summary": f"{len(rows)} advertisers with trend data: {len(down)} down, {len(up)} up.",
        }
    except Exception as e:
        log.exception("get_advertiser_revenue_trends failed")
        return {"error": str(e), "trends": []}
