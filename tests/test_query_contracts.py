"""Tests for Platform Dictionary query contracts.

Verifies that canonical query functions in queries.py return consistent, correctly
computed results regardless of whether they are called from the signal path or the
NL handler path.

T1:  fill_rate_publishers — correct schema + sessions_7d dynamic key
T2:  fill_rate_publishers — entity override suppresses excluded publisher
T3:  fill_rate_publishers — below-threshold publisher excluded (fill_rate_pct >= threshold)
T4:  fill_rate_publishers — CH failure returns empty list, does not raise
T5:  velocity_alerts — pct_delta formula: ((rev_7d/7)*30 - rev_30d) / rev_30d * 100
T6:  velocity_alerts — direction field is "down" for negative delta, "up" for positive
T7:  velocity_alerts — normal-range publisher excluded (down_threshold < delta < up_threshold)
T8:  velocity_alerts — phase-2 NOT called when |pct_delta| < 100 (only 1 ch.query call)
T9:  velocity_alerts — CH failure returns empty list, does not raise
T10: cap_alert_campaigns — cap_pct computed as percentage (not fraction)
T11: cap_alert_campaigns — both "month" and "monthly" config keys accepted
T12: cap_alert_campaigns — campaigns below cap_alert_pct excluded
T13: cap_alert_campaigns — days_to_cap calculation correct
T14: cap_alert_campaigns — CH failure returns empty list, does not raise
T15: earnings_breakdown — Earnings = gross_rev - partner_rev + partner_cost (+, not minus)
T16: earnings_breakdown — makes exactly TWO separate ch.query calls
T17: earnings_breakdown — CH failure returns all-zero dict, does not raise
T18: CVR P0 — get_publisher_health avg_cvr key uses clicks denominator, not sessions
T19: DATA DICTIONARY smoke — prompts/scout_system.md contains "Earnings = Gross Revenue"
T20: NL velocity handler — get_publisher_revenue_trends returns direction field in trend dicts
"""
from __future__ import annotations

import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

_WT_ROOT = Path(__file__).parent.parent
if str(_WT_ROOT) not in sys.path:
    sys.path.insert(0, str(_WT_ROOT))


# ---------------------------------------------------------------------------
# Stub heavy deps that are not available in the test venv
# ---------------------------------------------------------------------------

def _stub(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# anthropic
try:
    importlib.import_module("anthropic")
except ImportError:
    _ant = _stub("anthropic")
    _ant.Anthropic = MagicMock
    _ant.types = _stub("anthropic.types")
    sys.modules["anthropic.types"] = _ant.types

# clickhouse_connect
try:
    importlib.import_module("clickhouse_connect")
except ImportError:
    _stub("clickhouse_connect")

# scout_types
try:
    importlib.import_module("scout_types")
except ImportError:
    _stub("scout_types")

# pytz
try:
    importlib.import_module("pytz")
except ImportError:
    import datetime as _datetime_mod

    class _FakeTZ(_datetime_mod.tzinfo):
        def utcoffset(self, dt):
            return _datetime_mod.timedelta(hours=-6)
        def dst(self, dt):
            return _datetime_mod.timedelta(0)
        def tzname(self, dt):
            return "CST"

    _pytz = _stub("pytz")
    _pytz.timezone = MagicMock(return_value=_FakeTZ())

# slack_sdk
try:
    importlib.import_module("slack_sdk")
except ImportError:
    _slack = _stub("slack_sdk")
    _slack_web = _stub("slack_sdk.web")
    _slack_web.WebClient = MagicMock
    sys.modules["slack_sdk.web"] = _slack_web
    _slack.web = _slack_web

# Other heavy deps scout_agent imports at module level
for _dep in (
    "boto3",
    "botocore",
    "botocore.exceptions",
    "redis",
    "upstash_redis",
):
    if _dep not in sys.modules:
        _stub(_dep)

# scout_state — provide minimal stub so queries.py entity_overrides path doesn't crash
try:
    importlib.import_module("scout_state")
except ImportError:
    _ss = _stub("scout_state")
    _ss._load_entity_overrides = MagicMock(return_value={"publishers": {}})


def _make_ch_result(rows):
    """Return a mock ClickHouse query result with .result_rows = rows."""
    r = MagicMock()
    r.result_rows = rows
    return r


# ---------------------------------------------------------------------------
# Import the modules under test
# ---------------------------------------------------------------------------

import queries as _q  # noqa: E402


# ---------------------------------------------------------------------------
# T1-T4: fill_rate_publishers
# ---------------------------------------------------------------------------

class TestFillRatePublishers(unittest.TestCase):
    """Canonical fill-rate query contract tests."""

    def _make_rows(self):
        """Six-tuple rows: (pub_id, pub_name, sessions, sessions_with_imps, fill_rate, missed)."""
        return [
            (1001, "PublisherA", 5000, 3000, 60.0, 2000),  # fill_rate=60% — above threshold, should appear
            (1002, "PublisherB", 3000, 200, 6.7, 2800),    # fill_rate=6.7% — below threshold, should appear
        ]

    def test_schema_and_dynamic_sessions_key(self):
        """T1: Result dicts contain sessions_7d (not sessions_30d), fill_rate_pct, etc."""
        ch = MagicMock()
        ch.query.return_value = _make_ch_result(self._make_rows())

        results = _q.fill_rate_publishers(ch, entity_overrides={}, placements=["p1"])

        self.assertEqual(len(results), 2)
        first = results[0]
        # Dynamic key must be sessions_7d (window_days=7 default)
        self.assertIn("sessions_7d", first, "Expected sessions_7d key with default window_days=7")
        self.assertIn("fill_rate_pct", first)
        self.assertIn("missed_sessions", first)
        self.assertIn("publisher_id", first)
        self.assertIn("publisher_name", first)
        self.assertIsInstance(first["publisher_id"], int)
        self.assertIsInstance(first["sessions_7d"], int)

    def test_dynamic_key_with_custom_window(self):
        """T1 variant: window_days=14 produces sessions_14d key."""
        ch = MagicMock()
        ch.query.return_value = _make_ch_result([
            (1001, "PublisherA", 5000, 3000, 60.0, 2000),
        ])

        results = _q.fill_rate_publishers(ch, window_days=14, entity_overrides={}, placements=["p1"])

        self.assertEqual(len(results), 1)
        self.assertIn("sessions_14d", results[0], "Expected sessions_14d key with window_days=14")
        self.assertNotIn("sessions_7d", results[0])

    def test_entity_override_suppresses_excluded_publisher(self):
        """T2: Publisher with exclude_from_fill_rate=True is skipped."""
        ch = MagicMock()
        ch.query.return_value = _make_ch_result([
            (1001, "PublisherA", 5000, 3000, 60.0, 2000),
            (1002, "ExcludedPub", 3000, 200, 6.7, 2800),
        ])

        entity_overrides = {"ExcludedPub": {"exclude_from_fill_rate": True}}
        results = _q.fill_rate_publishers(ch, entity_overrides=entity_overrides, placements=["p1"])

        ids = [r["publisher_id"] for r in results]
        self.assertIn(1001, ids)
        self.assertNotIn(1002, ids, "ExcludedPub should be suppressed by entity override")

    def test_ch_failure_returns_empty_list(self):
        """T4: ClickHouse failure returns empty list without raising."""
        ch = MagicMock()
        ch.query.side_effect = RuntimeError("CH connection refused")

        results = _q.fill_rate_publishers(ch, entity_overrides={}, placements=["p1"])

        self.assertEqual(results, [], "CH failure must return empty list, not raise")


# ---------------------------------------------------------------------------
# T5-T9: velocity_alerts
# ---------------------------------------------------------------------------

class TestVelocityAlerts(unittest.TestCase):
    """Canonical velocity alert query contract tests."""

    def test_pct_delta_formula(self):
        """T5: pct_delta = ((rev_7d/7)*30 - rev_30d) / rev_30d * 100."""
        ch = MagicMock()
        # Publisher: rev_30d=$10000, rev_7d=$500 → ann=$2143 → delta=-78.6%
        ch.query.return_value = _make_ch_result([
            (2001, "SlowPub", 10000.0, 500.0),
        ])

        results = _q.velocity_alerts(ch)

        self.assertEqual(len(results), 1)
        expected_ann = (500.0 / 7) * 30  # ≈ 2142.86
        expected_delta = (expected_ann - 10000.0) / 10000.0 * 100  # ≈ -78.57%
        self.assertAlmostEqual(results[0]["pct_delta"], round(expected_delta, 1), places=0)

    def test_direction_down_for_negative_delta(self):
        """T6a: direction="down" when pct_delta < 0."""
        ch = MagicMock()
        ch.query.return_value = _make_ch_result([
            (2001, "SlowPub", 10000.0, 500.0),  # big drop → down
        ])

        results = _q.velocity_alerts(ch)
        self.assertEqual(results[0]["direction"], "down")

    def test_direction_up_for_positive_delta(self):
        """T6b: direction="up" when pct_delta > 0."""
        ch = MagicMock()
        # rev_30d=$5000, rev_7d=$1500 → ann=$6429 → delta=+28.6% (above up threshold of 20%)
        ch.query.return_value = _make_ch_result([
            (2002, "FastPub", 5000.0, 1500.0),
        ])

        results = _q.velocity_alerts(ch)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["direction"], "up")

    def test_normal_range_publisher_excluded(self):
        """T7: Publisher with delta within (-25%, +20%) is skipped."""
        ch = MagicMock()
        # rev_30d=$10000, rev_7d=$1000 → ann=$4286 → delta=-57.1% — fires
        # rev_30d=$8000, rev_7d=$900 → ann=$3857 → delta=-51.8% — fires
        # rev_30d=$6000, rev_7d=$800 → ann=$3429 → delta=-42.9% — fires
        # Publisher in normal range: rev_30d=$5000, rev_7d=$600 → ann=$2571 → delta=-48.6%
        # Actually let's do a publisher clearly in normal range:
        # rev_30d=$5000, rev_7d=$1200 → ann=$5143 → delta=+2.9% — within (-25%, +20%) → excluded
        ch.query.return_value = _make_ch_result([
            (2003, "NormalPub", 5000.0, 1200.0),
        ])

        results = _q.velocity_alerts(ch)
        self.assertEqual(results, [], "Publisher in normal velocity range must be excluded")

    def test_phase_2_not_called_when_delta_below_100(self):
        """T8: Only ONE ch.query call when |pct_delta| < 100 (no Phase 2 enrichment)."""
        ch = MagicMock()
        # rev_30d=$10000, rev_7d=$500 → delta≈-78.6% → |delta|<100 → phase 2 skipped
        ch.query.return_value = _make_ch_result([
            (2001, "SlowPub", 10000.0, 500.0),
        ])

        results = _q.velocity_alerts(ch)

        # Exactly ONE ch.query call — phase 2 never fires
        self.assertEqual(ch.query.call_count, 1, "Phase 2 should not fire when |pct_delta| < 100")
        self.assertEqual(results[0]["advertisers"], [])

    def test_phase_2_called_when_delta_exceeds_100(self):
        """T8 variant: Phase 2 fires when |pct_delta| >= 100 (two ch.query calls)."""
        ch = MagicMock()
        # rev_30d=$10000, rev_7d=$0 → delta=-100% exactly → |delta|>=100 → phase 2 fires
        phase1_result = _make_ch_result([
            (2001, "BigDropPub", 10000.0, 0.0),
        ])
        phase2_result = _make_ch_result([])
        ch.query.side_effect = [phase1_result, phase2_result]

        _q.velocity_alerts(ch)

        self.assertEqual(ch.query.call_count, 2, "Phase 2 should fire when |pct_delta| >= 100")

    def test_ch_failure_returns_empty_list(self):
        """T9: ClickHouse failure returns empty list without raising."""
        ch = MagicMock()
        ch.query.side_effect = RuntimeError("connection timeout")

        results = _q.velocity_alerts(ch)

        self.assertEqual(results, [], "CH failure must return empty list, not raise")


# ---------------------------------------------------------------------------
# T10-T14: cap_alert_campaigns
# ---------------------------------------------------------------------------

class TestCapAlertCampaigns(unittest.TestCase):
    """Canonical cap alert query contract tests."""

    def _rows_month_budget(self, campaign_id, adv_name, budget, revenue_mtd):
        """Single row with {"month": {"budget": budget}} capping_config."""
        import json
        cap_cfg = json.dumps({"month": {"budget": budget}})
        return [(campaign_id, adv_name, cap_cfg, revenue_mtd)]

    def _rows_monthly_budget(self, campaign_id, adv_name, budget, revenue_mtd):
        """Single row with {"monthly": {"budget": budget}} capping_config."""
        import json
        cap_cfg = json.dumps({"monthly": {"budget": budget}})
        return [(campaign_id, adv_name, cap_cfg, revenue_mtd)]

    def test_cap_pct_stored_as_percentage(self):
        """T10: cap_pct is percentage (85.0), not fraction (0.85)."""
        ch = MagicMock()
        # revenue_mtd=$9000, budget=$10000 → cap_pct=90% → above default 85% threshold
        ch.query.return_value = _make_ch_result(
            self._rows_month_budget(301, "AT&T", 10000.0, 9000.0)
        )

        results = _q.cap_alert_campaigns(ch, as_of_date="2026-05-15")

        self.assertEqual(len(results), 1)
        self.assertAlmostEqual(results[0]["cap_pct"], 90.0, places=0)
        self.assertGreater(results[0]["cap_pct"], 1.0, "cap_pct must be a percentage, not a fraction")

    def test_monthly_key_accepted(self):
        """T11: {"monthly": {"budget": X}} config key is accepted (same as "month")."""
        ch = MagicMock()
        ch.query.return_value = _make_ch_result(
            self._rows_monthly_budget(302, "Verizon", 10000.0, 9500.0)
        )

        results = _q.cap_alert_campaigns(ch, as_of_date="2026-05-15")

        self.assertEqual(len(results), 1, '"monthly" config key should be accepted')
        self.assertAlmostEqual(results[0]["monthly_cap"], 10000.0, places=0)

    def test_campaign_below_threshold_excluded(self):
        """T12: Campaign at 70% cap (below 85% default threshold) is excluded."""
        ch = MagicMock()
        # revenue_mtd=$7000, budget=$10000 → cap_pct=70% — below 85% threshold
        ch.query.return_value = _make_ch_result(
            self._rows_month_budget(303, "SprinkleCo", 10000.0, 7000.0)
        )

        results = _q.cap_alert_campaigns(ch, as_of_date="2026-05-15")

        self.assertEqual(results, [], "Campaign below cap_alert_pct should be excluded")

    def test_days_to_cap_calculation(self):
        """T13: days_to_cap = (monthly_cap - revenue_mtd) / daily_run_rate."""
        ch = MagicMock()
        # as_of_date=2026-05-15 (day 15), revenue_mtd=$9000, budget=$10000
        # daily_run_rate = 9000 / 15 = 600
        # days_to_cap = (10000 - 9000) / 600 = 1.667 → rounds to 1.7
        ch.query.return_value = _make_ch_result(
            self._rows_month_budget(304, "QuickCapper", 10000.0, 9000.0)
        )

        results = _q.cap_alert_campaigns(ch, as_of_date="2026-05-15")

        self.assertEqual(len(results), 1)
        expected_dtc = round((10000.0 - 9000.0) / (9000.0 / 15), 1)
        self.assertAlmostEqual(results[0]["days_to_cap"], expected_dtc, places=0)

    def test_ch_failure_returns_empty_list(self):
        """T14: ClickHouse failure returns empty list without raising."""
        ch = MagicMock()
        ch.query.side_effect = RuntimeError("CH unreachable")

        results = _q.cap_alert_campaigns(ch)

        self.assertEqual(results, [], "CH failure must return empty list, not raise")


# ---------------------------------------------------------------------------
# T15-T17: earnings_breakdown
# ---------------------------------------------------------------------------

class TestEarningsBreakdown(unittest.TestCase):
    """Canonical earnings query contract tests."""

    def test_earnings_formula_plus_partner_cost(self):
        """T15: Earnings = gross_rev - partner_rev + partner_cost (NOT minus partner_cost)."""
        ch = MagicMock()
        # Query 1: gross_rev=1000, partner_rev=400
        # Query 2: partner_cost=50
        # Earnings = 1000 - 400 + 50 = 650 (NOT 1000 - 400 - 50 = 550)
        q1 = _make_ch_result([(1000.0, 400.0)])
        q2 = _make_ch_result([(50.0,)])
        ch.query.side_effect = [q1, q2]

        result = _q.earnings_breakdown(ch, start_date="2026-05-01", end_date="2026-05-25")

        self.assertAlmostEqual(result["gross_rev"], 1000.0, places=2)
        self.assertAlmostEqual(result["partner_rev"], 400.0, places=2)
        self.assertAlmostEqual(result["partner_cost"], 50.0, places=2)
        self.assertAlmostEqual(result["earnings"], 650.0, places=2,
                               msg="Earnings must be gross - partner_rev + partner_cost (NOT minus)")

    def test_exactly_two_ch_query_calls(self):
        """T16: earnings_breakdown makes exactly two separate ch.query calls."""
        ch = MagicMock()
        q1 = _make_ch_result([(1000.0, 400.0)])
        q2 = _make_ch_result([(50.0,)])
        ch.query.side_effect = [q1, q2]

        _q.earnings_breakdown(ch, start_date="2026-05-01", end_date="2026-05-25")

        self.assertEqual(ch.query.call_count, 2,
                         "earnings_breakdown must make exactly two separate ch.query calls")

    def test_ch_failure_returns_all_zero_dict(self):
        """T17: Any CH failure returns all-zero dict without raising."""
        ch = MagicMock()
        ch.query.side_effect = RuntimeError("CH timeout")

        result = _q.earnings_breakdown(ch, start_date="2026-05-01", end_date="2026-05-25")

        self.assertEqual(result["gross_rev"], 0.0)
        self.assertEqual(result["partner_rev"], 0.0)
        self.assertEqual(result["partner_cost"], 0.0)
        self.assertEqual(result["earnings"], 0.0)

    def test_optional_publisher_id_none_runs_fleet_wide(self):
        """T17 variant: publisher_id=None (fleet-wide) doesn't crash."""
        ch = MagicMock()
        ch.query.side_effect = [
            _make_ch_result([(50000.0, 20000.0)]),
            _make_ch_result([(1000.0,)]),
        ]

        result = _q.earnings_breakdown(ch, start_date="2026-05-01", end_date="2026-05-25",
                                       publisher_id=None)

        self.assertAlmostEqual(result["earnings"], 50000.0 - 20000.0 + 1000.0, places=2)


# ---------------------------------------------------------------------------
# T18: CVR P0 — get_publisher_health uses clicks not sessions
# ---------------------------------------------------------------------------

class TestCVRClicksDenominator(unittest.TestCase):
    """Verify CVR P0 fix: get_publisher_health uses clicks denominator not sessions."""

    def test_avg_cvr_uses_clicks_denominator(self):
        """T18: avg_cvr in get_publisher_health result uses clicks, not sessions."""
        # We verify by providing different values for clicks vs sessions and checking
        # which denominator was used. This catches regression if someone reverts to /sessions.
        try:
            import scout_agent as _sa
        except Exception:
            self.skipTest("scout_agent not importable in this environment")
            return

        # Guard only the symbol lookup — skip if the function doesn't exist yet.
        # Invocation and assertions are unguarded so AttributeErrors from the
        # implementation propagate as test failures rather than silent skips.
        _get_health = getattr(_sa, "get_publisher_health", None)
        if _get_health is None:
            self.skipTest("get_publisher_health not found on scout_agent")
            return

        # Stub the ClickHouse client so get_publisher_health runs without real CH
        mock_ch = MagicMock()

        # 30-day aggregate row: (total_sessions, total_clicks, total_conversions, ...)
        # sessions=10000, clicks=1000, conversions=50
        # CVR with clicks: 50/1000*100 = 5.0
        # CVR with sessions: 50/10000*100 = 0.5
        mock_agg_row = _make_ch_result([
            (10000, 1000, 50, 5000.0, 400.0, 30)  # sessions, clicks, conversions, rev, payout, days
        ])
        mock_ch.query.return_value = mock_agg_row

        with patch("scout_tools_publisher._get_ch_client", return_value=mock_ch):
            result = _get_health(publisher_id="1001")
            self.assertIsInstance(result, dict, "get_publisher_health must return a dict")
            # CVR lives under result["overall"]["cvr_pct"]
            self.assertIn("overall", result,
                          "get_publisher_health must return a result with 'overall' key")
            overall = result["overall"]
            self.assertIn("cvr_pct", overall,
                          "overall must contain 'cvr_pct'")
            # Should be ~5.0 (clicks-based: 50/1000*100), not ~0.5 (sessions-based: 50/10000*100)
            self.assertGreater(
                overall["cvr_pct"], 1.0,
                f"cvr_pct={overall['cvr_pct']} looks sessions-based (expected ~5.0 clicks-based)"
            )


# ---------------------------------------------------------------------------
# T19: DATA DICTIONARY smoke test
# ---------------------------------------------------------------------------

class TestDataDictionarySmoke(unittest.TestCase):
    """Regression guard: prompts/scout_system.md contains critical formula definitions."""

    def test_earnings_formula_present(self):
        """T19: DATA DICTIONARY contains Earnings = Gross Revenue definition."""
        system_prompt_path = _WT_ROOT / "prompts" / "scout_system.md"
        if not system_prompt_path.exists():
            self.skipTest("prompts/scout_system.md not found — skipping DATA DICTIONARY smoke")
            return

        content = system_prompt_path.read_text(encoding="utf-8")
        # Formula is aligned with spaces: "Earnings          = Gross Revenue − Partner Revenue + Partner Cost"
        # Match on the formula body rather than exact spacing.
        self.assertTrue(
            "Gross Revenue" in content and "Partner Revenue + Partner Cost" in content,
            "DATA DICTIONARY missing Earnings formula — risk of wrong ad-hoc SQL generation",
        )

    def test_cvr_definition_present(self):
        """T19b: DATA DICTIONARY contains CVR = Conversions / Clicks definition."""
        system_prompt_path = _WT_ROOT / "prompts" / "scout_system.md"
        if not system_prompt_path.exists():
            self.skipTest("prompts/scout_system.md not found")
            return

        content = system_prompt_path.read_text(encoding="utf-8")
        # Either "Conversions / Clicks" or "conversions / clicks" should appear
        self.assertTrue(
            "Conversions / Clicks" in content or "conversions / clicks" in content,
            "DATA DICTIONARY missing CVR = Conversions / Clicks definition",
        )


# ---------------------------------------------------------------------------
# T20: NL velocity handler — get_publisher_revenue_trends returns direction field
# ---------------------------------------------------------------------------

class TestNLVelocityHandler(unittest.TestCase):
    """NL velocity handler uses canonical velocity_alerts and surfaces direction field."""

    def test_get_publisher_revenue_trends_returns_direction(self):
        """T20: get_publisher_revenue_trends returns trend dicts with direction field."""
        try:
            import scout_agent as _sa
        except Exception:
            self.skipTest("scout_agent not importable")
            return

        # Guard only the symbol lookup — skip if the function doesn't exist yet.
        _get_trends = getattr(_sa, "get_publisher_revenue_trends", None)
        if _get_trends is None:
            self.skipTest("get_publisher_revenue_trends not found on scout_agent")
            return

        mock_ch = MagicMock()
        # velocity_alerts returns a down publisher — pct_delta=-78.6%, direction="down"
        velocity_row = _make_ch_result([
            (2001, "SlowPub", 10000.0, 500.0),
        ])
        mock_ch.query.return_value = velocity_row

        with patch("scout_agent._get_ch_client", return_value=mock_ch):
            result = _get_trends()
            self.assertIsInstance(result, dict,
                                  "get_publisher_revenue_trends must return a dict")
            self.assertIn("trends", result, "result must contain 'trends' key")
            self.assertTrue(result["trends"],
                            "trends list must not be empty given mock velocity data")
            trend = result["trends"][0]
            self.assertIn(
                "direction", trend,
                "get_publisher_revenue_trends must surface 'direction' field from velocity_alerts()",
            )
            self.assertIn(trend["direction"], ("up", "down"),
                          "direction must be 'up' or 'down'")


if __name__ == "__main__":
    unittest.main()
