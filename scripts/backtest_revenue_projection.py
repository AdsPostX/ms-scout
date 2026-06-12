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

from scout_ch import _build_hour_curve, _get_ch_client, _revenue_at_hour  # type: ignore  # noqa: E402

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


def _project(curve: dict, today_rev: float, dow: int, hour: int) -> tuple[float | None, str, float | None, float | None]:
    """Return (projected_p50, source, projected_low, projected_high).

    projected_low  = today_rev / p75  (pessimistic — if we're at a high-share hour pace)
    projected_high = today_rev / p25  (optimistic  — if we're at a low-share hour pace)
    Both are None when the fallback band is used (no p25/p75 available).
    """
    band = curve["share_by_dow"].get(dow, {}).get(hour)
    source = "90d"
    if band is None or band["p50"] < 0.01:
        share = 0.70
        source = "fallback_0.70"
        if share <= 0:
            return None, source, None, None
        projected = today_rev / float(share)
        return projected, source, None, None
    else:
        p50 = band["p50"]
        p25 = band["p25"]
        p75 = band["p75"]
    if p50 <= 0:
        return None, source, None, None
    projected      = today_rev / float(p50)
    projected_low  = today_rev / float(p75) if p75 > 0 else None
    projected_high = today_rev / float(p25) if p25 > 0 else None
    return projected, source, projected_low, projected_high


def main() -> int:
    ch = _get_ch_client()
    today_ct = datetime.now(CT).date()
    days = _business_days(today_ct, 10)
    curve = _build_hour_curve(ch)

    _DOW_NAMES = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}

    rows: list[dict] = []
    sampled_dates: set[str] = set()
    for d in days:
        actual_eod = _revenue_at_hour(ch, d, None)
        if actual_eod <= 0:
            continue
        sampled_dates.add(d.isoformat())
        dow = d.weekday() + 1
        for h in HOURS:
            rev_so_far = _revenue_at_hour(ch, d, h)
            projected, source, proj_low, proj_high = _project(curve, rev_so_far, dow, h)
            if projected is None:
                continue
            abs_err = projected - actual_eod
            pct_err = abs_err / actual_eod * 100.0
            rows.append({
                "date":       d.isoformat(),
                "dow":        dow,
                "hour":       h,
                "rev_so_far": rev_so_far,
                "projected":  projected,
                "actual_eod": actual_eod,
                "abs_err":    abs_err,
                "pct_err":    pct_err,
                "source":     source,
                "proj_low":   proj_low,
                "proj_high":  proj_high,
            })

    if not rows:
        print("No backtest rows produced — check data window.")
        return 1

    print("| Date | DOW | Hour | Rev so far | Projected EOD | Actual EOD | Abs err | % err | Bias | Source |")
    print("|------|-----|------|-----------:|--------------:|-----------:|--------:|------:|------:|--------|")
    for r in rows:
        dow_label = _DOW_NAMES.get(r["dow"], str(r["dow"]))
        print(
            f"| {r['date']} | {dow_label} | {r['hour']:02d}:00 | ${r['rev_so_far']:,.0f} | "
            f"${r['projected']:,.0f} | ${r['actual_eod']:,.0f} | "
            f"${r['abs_err']:+,.0f} | {r['pct_err']:+.1f}% | {r['pct_err']:+.1f}% | {r['source']} |"
        )

    pct_errs_abs = sorted(abs(r["pct_err"]) for r in rows)
    median = statistics.median(pct_errs_abs)
    p90_idx = max(0, int(round(0.9 * (len(pct_errs_abs) - 1))))
    p90 = pct_errs_abs[p90_idx]

    # Bias: signed mean error (positive = systematic over-estimate)
    bias = statistics.mean(r["pct_err"] for r in rows)

    print()
    print(f"Samples: {len(rows)} across {len(sampled_dates)} business days")
    print(f"Median |% err|: {median:.1f}%")
    print(f"P90    |% err|: {p90:.1f}%")
    print(f"Bias   (signed mean % err): {bias:+.1f}%  ({'over-estimates' if bias > 0 else 'under-estimates'} on average)")

    # Per-DOW error summary
    dow_rows: dict[int, list[float]] = {}
    for r in rows:
        dow_rows.setdefault(r["dow"], []).append(abs(r["pct_err"]))
    print()
    print("Per-DOW median |% err|:")
    for dow_int in sorted(dow_rows):
        dow_errs = sorted(dow_rows[dow_int])
        dow_median_err = statistics.median(dow_errs)
        print(f"  {_DOW_NAMES.get(dow_int, str(dow_int))}: {dow_median_err:.1f}%  (n={len(dow_errs)})")

    # p25/p75 coverage gate: how often actual EOD falls inside [proj_low, proj_high]
    band_rows = [r for r in rows if r["proj_low"] is not None and r["proj_high"] is not None]
    if band_rows:
        covered = sum(
            1 for r in band_rows
            if r["proj_low"] <= r["actual_eod"] <= r["proj_high"]
        )
        coverage_pct = covered / len(band_rows) * 100.0
        print()
        print(f"Coverage: {coverage_pct:.1f}% (p25–p75 range, n={len(band_rows)} rows with bands)")
        gate_coverage = 45.0
        if coverage_pct < gate_coverage:
            print(f"COVERAGE GATE: FAIL ({coverage_pct:.1f}% < {gate_coverage}% target) — p25/p75 band too narrow.")
        else:
            print(f"COVERAGE GATE: PASS ({coverage_pct:.1f}% ≥ {gate_coverage}% target)")
    else:
        print()
        print("Coverage: N/A (all rows used fallback band — no p25/p75 available)")

    print()
    gate_median = 8.0
    gate_p90 = 18.0
    gate_min_dates = 10
    if len(sampled_dates) < gate_min_dates:
        print(
            f"GATE: FAIL (only {len(sampled_dates)} business days with EOD > 0; "
            f"need {gate_min_dates}) — extend lookback window."
        )
        return 2
    if median <= gate_median and p90 <= gate_p90:
        print(f"GATE: PASS (median ≤ {gate_median}%, p90 ≤ {gate_p90}%) — safe to enable channel-wide.")
        return 0
    print(f"GATE: FAIL (need median ≤ {gate_median}%, p90 ≤ {gate_p90}%) — keep tool dev-Scout-only.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
