"""
Unit tests for the revenue projection engine in scout_ch.py.

Tests cover:
  T1  _quantile — empty list returns 0.0
  T2  _quantile — single element returns that element
  T3  _quantile — even-length list uses linear interpolation
  T4  project_today_revenue band ordering: projected_low < projected_full_day < projected_high
  T5  Fallback path: p50 < 0.01 → projection_n == 0
  T6  Missing DOW/hour in curve → no KeyError, returns fallback
  T7  Diagnostic "efficiency": revenue soft, traffic normal
  T8  Diagnostic "traffic": both revenue and traffic soft
"""
from __future__ import annotations

import datetime as _datetime_module
import sys
import unittest
from datetime import datetime, date
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scout_ch import _quantile, project_today_revenue  # noqa: E402


def _frozen_datetime_cls(frozen: datetime):
    """Return a datetime subclass whose .now() always returns ``frozen``."""
    class _FrozenDt(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return frozen.astimezone(tz)
            return frozen
    return _FrozenDt


def _make_curve(
    dow: int,
    hour: int,
    p25: float = 0.30,
    p50: float = 0.40,
    p75: float = 0.50,
    n: int = 10,
    dow_median: float = 1000.0,
    impressions_p50: float = 1000.0,
    sessions_p50: float = 500.0,
) -> dict:
    """Build a minimal curve dict matching the structure _build_hour_curve returns."""
    return {
        "share_by_dow": {
            dow: {
                hour: {"p25": p25, "p50": p50, "p75": p75, "n": n}
            }
        },
        "dow_median": {dow: dow_median},
        "sample_days": {dow: 30},
        "traffic_by_dow": {
            dow: {
                hour: {
                    "impressions_p50": impressions_p50,
                    "sessions_p50": sessions_p50,
                }
            }
        },
    }


def _make_ch_mock(today_revenue: float = 400.0) -> MagicMock:
    """CH mock that returns today_revenue for _revenue_at_hour."""
    ch = MagicMock()
    result = MagicMock()
    result.result_rows = [[today_revenue]]
    ch.query.return_value = result
    return ch


# ── T1 ────────────────────────────────────────────────────────────────────────

class TestQuantileEmpty(unittest.TestCase):
    def test_empty_list_returns_zero(self):
        """_quantile with an empty list must return 0.0."""
        self.assertEqual(_quantile([], 0.5), 0.0)
        self.assertEqual(_quantile([], 0.0), 0.0)
        self.assertEqual(_quantile([], 1.0), 0.0)


# ── T2 ────────────────────────────────────────────────────────────────────────

class TestQuantileSingleElement(unittest.TestCase):
    def test_single_element_returns_that_element(self):
        """_quantile with a single-element list returns that element."""
        self.assertAlmostEqual(_quantile([7.5], 0.0), 7.5)
        self.assertAlmostEqual(_quantile([7.5], 0.5), 7.5)
        self.assertAlmostEqual(_quantile([7.5], 1.0), 7.5)


# ── T3 ────────────────────────────────────────────────────────────────────────

class TestQuantileLinearInterpolation(unittest.TestCase):
    def test_even_length_linear_interpolation(self):
        """_quantile interpolates linearly for a 4-element list."""
        data = [10.0, 20.0, 30.0, 40.0]
        # Median (q=0.5): idx = 0.5 * 3 = 1.5 → 20 + 0.5*(30-20) = 25
        self.assertAlmostEqual(_quantile(data, 0.5), 25.0)
        # q=0.25: idx = 0.25 * 3 = 0.75 → 10 + 0.75*(20-10) = 17.5
        self.assertAlmostEqual(_quantile(data, 0.25), 17.5)
        # q=0.75: idx = 0.75 * 3 = 2.25 → 30 + 0.25*(40-30) = 32.5
        self.assertAlmostEqual(_quantile(data, 0.75), 32.5)


# ── T4 ────────────────────────────────────────────────────────────────────────

class TestProjectBandOrdering(unittest.TestCase):
    def test_projected_low_lt_full_lt_high(self):
        """projected_low < projected_full_day < projected_high for a normal curve."""  # noqa: E501
        dow = 3          # Wednesday (CH toDayOfWeek: Mon=1..Sun=7)
        hour = 14        # 2pm CT — well past the too_early=10 guard
        today_rev = 400.0

        curve = _make_curve(
            dow=dow, hour=hour,
            p25=0.30, p50=0.40, p75=0.50,
            n=10, dow_median=1000.0,
        )
        ch = _make_ch_mock(today_rev)

        # Freeze time to Wednesday 14:00 CT
        frozen_dt = datetime(2026, 6, 3, 14, 0, 0, tzinfo=ZoneInfo("America/Chicago"))

        with patch("scout_ch._build_hour_curve", return_value=curve), \
             patch("scout_ch._query_intraday_traffic", return_value={"impressions": 500, "sessions": 200}), \
             patch("scout_ch._revenue_at_hour", return_value=today_rev), \
             patch("datetime.datetime", _frozen_datetime_cls(frozen_dt)):
            result = project_today_revenue(ch)

        self.assertEqual(result["status"], "ok")
        self.assertIsNotNone(result["projected_low"])
        self.assertIsNotNone(result["projected_full_day"])
        self.assertIsNotNone(result["projected_high"])
        self.assertLess(result["projected_low"], result["projected_full_day"])
        self.assertLess(result["projected_full_day"], result["projected_high"])


# ── T5 ────────────────────────────────────────────────────────────────────────

class TestFallbackPathProjectionN(unittest.TestCase):
    def test_fallback_when_p50_tiny_sets_projection_n_zero(self):
        """When band p50 < 0.01, projection_n must be 0 (not band['n'])."""
        from zoneinfo import ZoneInfo

        dow = 3
        hour = 14
        today_rev = 400.0

        # p50 = 0.001 < 0.01 → forces fallback path
        curve = _make_curve(
            dow=dow, hour=hour,
            p25=0.001, p50=0.001, p75=0.001,
            n=15,          # band has n=15, but projection_n should still be 0
            dow_median=1000.0,
        )
        ch = _make_ch_mock(today_rev)

        frozen_dt = datetime(2026, 6, 3, 14, 0, 0, tzinfo=ZoneInfo("America/Chicago"))

        with patch("scout_ch._build_hour_curve", return_value=curve), \
             patch("scout_ch._query_intraday_traffic", return_value={"impressions": 0, "sessions": 0}), \
             patch("scout_ch._revenue_at_hour", return_value=today_rev), \
             patch("datetime.datetime", _frozen_datetime_cls(frozen_dt)):
            result = project_today_revenue(ch)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["projection_n"], 0,
                         "projection_n must be 0 on fallback, not band['n']")
        self.assertEqual(result["curve_source"], "fallback_0.70")


# ── T6 ────────────────────────────────────────────────────────────────────────

class TestMissingDOWHour(unittest.TestCase):
    def test_missing_dow_hour_no_keyerror(self):
        """project_today_revenue returns gracefully when DOW/hour absent from curve."""
        from zoneinfo import ZoneInfo

        # Curve has data for dow=2, but we'll freeze time on dow=3 (Wednesday)
        # so the lookup misses entirely
        dow_in_curve = 2   # Tuesday
        hour = 14
        today_rev = 300.0

        curve = _make_curve(
            dow=dow_in_curve, hour=hour,
            p25=0.35, p50=0.45, p75=0.55,
            n=8, dow_median=900.0,
        )
        # Put dow=3 in sample_days so it doesn't hit insufficient_history
        curve["sample_days"][3] = 12

        ch = _make_ch_mock(today_rev)

        # Freeze to Wednesday 14:00 — dow=3, which has no share_by_dow entry
        frozen_dt = datetime(2026, 6, 3, 14, 0, 0, tzinfo=ZoneInfo("America/Chicago"))

        try:
            with patch("scout_ch._build_hour_curve", return_value=curve), \
                 patch("scout_ch._query_intraday_traffic", return_value={"impressions": 0, "sessions": 0}), \
                 patch("scout_ch._revenue_at_hour", return_value=today_rev), \
                 patch("datetime.datetime", _frozen_datetime_cls(frozen_dt)):
                result = project_today_revenue(ch)
        except KeyError as exc:
            self.fail(f"project_today_revenue raised KeyError for missing DOW/hour: {exc}")

        # Should fall back gracefully — either ok with fallback or insufficient_history
        self.assertIn(result["status"], ("ok", "insufficient_history", "too_early"))


# ── T7 ────────────────────────────────────────────────────────────────────────

class TestDiagnosticEfficiency(unittest.TestCase):
    def test_efficiency_diagnostic_revenue_soft_traffic_normal(self):
        """Diagnostic='efficiency' when revenue is below expected but traffic is normal."""
        from zoneinfo import ZoneInfo

        dow = 3
        hour = 14

        # Revenue pace: today_revenue / dow_median = 400 / 1000 = 0.40
        # p50 share = 0.40, so revenue-relative-to-expected at p50 pace = 0.40/0.40 = 1.0 * p50/p50
        # We want rev_dev < -0.08: today_revenue pace vs DOW median
        # rev_dev = (today_revenue / dow_median - p50) / p50
        # = (300 / 1000 - 0.40) / 0.40 = (0.30 - 0.40)/0.40 = -0.25 → < -0.08 ✓ (revenue soft)
        #
        # imp_dev = (impressions - imp_baseline) / imp_baseline
        # = (950 - 1000) / 1000 = -0.05 → abs < 0.08 ✓ (traffic normal)
        today_rev = 300.0   # soft relative to 1000 median at 40% share pace
        imp_baseline = 1000.0
        actual_impressions = 950  # within ±8% of baseline → traffic normal

        curve = _make_curve(
            dow=dow, hour=hour,
            p25=0.30, p50=0.40, p75=0.50,
            n=10, dow_median=1000.0,
            impressions_p50=imp_baseline,
            sessions_p50=500.0,
        )
        ch = _make_ch_mock(today_rev)

        frozen_dt = datetime(2026, 6, 3, 14, 0, 0, tzinfo=ZoneInfo("America/Chicago"))

        with patch("scout_ch._build_hour_curve", return_value=curve), \
             patch("scout_ch._query_intraday_traffic",
                   return_value={"impressions": actual_impressions, "sessions": 480}), \
             patch("scout_ch._revenue_at_hour", return_value=today_rev), \
             patch("datetime.datetime", _frozen_datetime_cls(frozen_dt)):
            result = project_today_revenue(ch)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["diagnostic"], "efficiency",
                         f"Expected 'efficiency', got {result['diagnostic']!r}. "
                         f"rev={today_rev}, imp={actual_impressions}, "
                         f"projected_full_day={result.get('projected_full_day')}")


# ── T8 ────────────────────────────────────────────────────────────────────────

class TestDiagnosticTraffic(unittest.TestCase):
    def test_traffic_diagnostic_both_soft(self):
        """Diagnostic='traffic' when both revenue and impressions are below expected."""
        from zoneinfo import ZoneInfo

        dow = 3
        hour = 14

        # rev_dev = (today_revenue / dow_median - p50) / p50
        # = (300 / 1000 - 0.40) / 0.40 = -0.25 → < -0.08 ✓ (revenue soft)
        #
        # imp_dev = (impressions - imp_baseline) / imp_baseline
        # = (850 - 1000) / 1000 = -0.15 → < -0.08 ✓ (traffic also soft)
        today_rev = 300.0
        imp_baseline = 1000.0
        actual_impressions = 850   # 15% below baseline → traffic soft

        curve = _make_curve(
            dow=dow, hour=hour,
            p25=0.30, p50=0.40, p75=0.50,
            n=10, dow_median=1000.0,
            impressions_p50=imp_baseline,
            sessions_p50=500.0,
        )
        ch = _make_ch_mock(today_rev)

        frozen_dt = datetime(2026, 6, 3, 14, 0, 0, tzinfo=ZoneInfo("America/Chicago"))

        with patch("scout_ch._build_hour_curve", return_value=curve), \
             patch("scout_ch._query_intraday_traffic",
                   return_value={"impressions": actual_impressions, "sessions": 380}), \
             patch("scout_ch._revenue_at_hour", return_value=today_rev), \
             patch("datetime.datetime", _frozen_datetime_cls(frozen_dt)):
            result = project_today_revenue(ch)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["diagnostic"], "traffic",
                         f"Expected 'traffic', got {result['diagnostic']!r}. "
                         f"rev={today_rev}, imp={actual_impressions}, "
                         f"projected_full_day={result.get('projected_full_day')}")


if __name__ == "__main__":
    unittest.main()
