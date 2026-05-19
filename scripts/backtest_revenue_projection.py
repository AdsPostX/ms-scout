#!/usr/bin/env python3
"""
Backtest the revenue projection helper (PLAN.md Step 6).

Replays `project_today_revenue` against the last 10 business days at CT hours
10/12/14/16 by reconstructing what the helper *would* have returned at that
moment, then compares to that day's actual EOD revenue.

Gates public rollout — see PLAN.md Risks: median error < 8% and p90 < 18% before
flipping the tool from dev-Scout to channel-visible.

Usage:
    python3 scripts/backtest_revenue_projection.py

Reads from ClickHouse via the same _get_ch_client() Scout uses. No writes.
"""
from __future__ import annotations

import statistics
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

# Make repo root importable
sys.path.insert(0, str(__file__).rsplit("/scripts/", 1)[0])

from scout_ch import _build_hour_curve, _get_ch_client  # type: ignore  # noqa: E402

CT = ZoneInfo("America/Chicago")
HOURS = [10, 12, 14, 16]
LOOKBACK_DAYS = 14  # to find 10 business days


def _business_days(end: date, n: int) -> list[date]:
    out: list[date] = []
    d = end
    while len(out) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            out.append(d)
    return out


def _revenue_at(ch, target_date: date, max_hour: int | None) -> float:
    """Sum CT revenue for target_date up to (but not including) max_hour. None = full day."""
    if max_hour is None:
        hour_clause = ""
    else:
        hour_clause = f"  AND toHour(toTimeZone(created_at, 'America/Chicago')) < {max_hour}"
    sql = f"""
SELECT coalesce(sum(toFloat64OrNull(revenue)), 0) AS rev
FROM adpx_conversionsdetails
PREWHERE toYYYYMM(created_at) = toYYYYMM(toDate('{target_date.isoformat()}'))
WHERE toDate(toTimeZone(created_at, 'America/Chicago')) = toDate('{target_date.isoformat()}')
{hour_clause}
""".strip()
    rows = ch.query(sql).result_rows
    return float(rows[0][0] or 0) if rows else 0.0


def _project(curve: dict, today_rev: float, dow: int, hour: int) -> tuple[float | None, str]:
    share = curve["share_by_dow"].get(dow, {}).get(hour)
    source = "60d"
    if share is None or share < 0.01:
        share = 0.70
        source = "fallback_0.70"
    if share <= 0:
        return None, source
    return today_rev / float(share), source


def main() -> int:
    ch = _get_ch_client()
    today_ct = datetime.now(CT).date()
    days = _business_days(today_ct, 10)
    curve = _build_hour_curve(ch)

    rows: list[dict] = []
    for d in days:
        actual_eod = _revenue_at(ch, d, None)
        if actual_eod <= 0:
            continue
        dow = d.weekday() + 1
        for h in HOURS:
            rev_so_far = _revenue_at(ch, d, h)
            projected, source = _project(curve, rev_so_far, dow, h)
            if projected is None:
                continue
            abs_err = projected - actual_eod
            pct_err = abs_err / actual_eod * 100.0
            rows.append({
                "date": d.isoformat(),
                "hour": h,
                "rev_so_far": rev_so_far,
                "projected": projected,
                "actual_eod": actual_eod,
                "abs_err": abs_err,
                "pct_err": pct_err,
                "source": source,
            })

    if not rows:
        print("No backtest rows produced — check data window.")
        return 1

    print("| Date | Hour | Rev so far | Projected EOD | Actual EOD | Abs err | % err | Source |")
    print("|------|------|-----------:|--------------:|-----------:|--------:|------:|--------|")
    for r in rows:
        print(
            f"| {r['date']} | {r['hour']:02d}:00 | ${r['rev_so_far']:,.0f} | "
            f"${r['projected']:,.0f} | ${r['actual_eod']:,.0f} | "
            f"${r['abs_err']:+,.0f} | {r['pct_err']:+.1f}% | {r['source']} |"
        )

    pct_errs = sorted(abs(r["pct_err"]) for r in rows)
    median = statistics.median(pct_errs)
    p90_idx = max(0, int(round(0.9 * (len(pct_errs) - 1))))
    p90 = pct_errs[p90_idx]

    print()
    print(f"Samples: {len(rows)}")
    print(f"Median |% err|: {median:.1f}%")
    print(f"P90    |% err|: {p90:.1f}%")
    print()
    gate_median = 8.0
    gate_p90 = 18.0
    if median <= gate_median and p90 <= gate_p90:
        print(f"GATE: PASS (median ≤ {gate_median}%, p90 ≤ {gate_p90}%) — safe to enable channel-wide.")
        return 0
    print(f"GATE: FAIL (need median ≤ {gate_median}%, p90 ≤ {gate_p90}%) — keep tool dev-Scout-only.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
