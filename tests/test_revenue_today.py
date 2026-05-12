"""
get_revenue_today() formatter tests.

These tests do not hit ClickHouse. They patch _get_ch_client and
_load_entity_overrides to return controlled data, then assert on the
pre-formatted mrkdwn string produced by get_revenue_today().
"""
import sys
import pathlib
import unittest
import unittest.mock

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _make_ch_mock(today_rows, avg_rows):
    """Return a mock ClickHouse client that returns given rows for execute()."""
    mock_ch = unittest.mock.MagicMock()
    mock_ch.execute.side_effect = [today_rows, avg_rows]
    return mock_ch


def _run(today_rows, avg_rows, overrides=None):
    """Helper: patch dependencies and call get_revenue_today()."""
    import scout_agent
    overrides = overrides or {"publishers": {}, "advertisers": {}}
    mock_ch = _make_ch_mock(today_rows, avg_rows)
    with unittest.mock.patch.object(scout_agent, "_get_ch_client", return_value=mock_ch), \
         unittest.mock.patch.object(scout_agent, "_load_entity_overrides", return_value=overrides):
        return scout_agent.get_revenue_today()


class TestRevenueToday(unittest.TestCase):

    def test_pre_formatted_flag_in_return(self):
        result = _run(
            today_rows=[(1, "AT&T", 5000.0, 200)],
            avg_rows=[(1, 6000.0)],
        )
        self.assertIsInstance(result, dict)
        self.assertTrue(result.get("pre_formatted"), "pre_formatted must be True")
        self.assertIn("formatted", result)
        self.assertIsInstance(result["formatted"], str)

    def test_no_publisher_ids_in_output(self):
        """No user_id, publisher_id, or numeric IDs should appear in the output."""
        result = _run(
            today_rows=[(953, "AT&T", 3392.0, 343), (1572, "Ifficient", 2513.0, 198)],
            avg_rows=[(953, 4000.0), (1572, 3000.0)],
        )
        formatted = result["formatted"]
        self.assertNotIn("953", formatted)
        self.assertNotIn("1572", formatted)
        self.assertNotIn("publisher_id", formatted)
        self.assertNotIn("user_id", formatted)

    def test_top_3_inline_remainder_grouped(self):
        """5 publishers → top 3 named, remainder as "> N others · $X combined"."""
        rows = [
            (1, "Alpha", 10000.0, 100),
            (2, "Beta",   8000.0,  80),
            (3, "Gamma",  6000.0,  60),
            (4, "Delta",  2000.0,  20),
            (5, "Epsilon", 1000.0, 10),
        ]
        avg_rows = [(i, 8000.0) for i in range(1, 6)]
        result = _run(today_rows=rows, avg_rows=avg_rows)
        formatted = result["formatted"]
        self.assertIn("Alpha", formatted)
        self.assertIn("Beta", formatted)
        self.assertIn("Gamma", formatted)
        self.assertNotIn("Delta", formatted)
        self.assertNotIn("Epsilon", formatted)
        self.assertIn("2 others", formatted)

    def test_single_publisher_no_others_line(self):
        """Only 1 publisher — no "> 0 others" line should appear."""
        result = _run(
            today_rows=[(1, "SoloPublisher", 5000.0, 50)],
            avg_rows=[(1, 6000.0)],
        )
        self.assertNotIn("0 others", result["formatted"])
        self.assertNotIn("others", result["formatted"])

    def test_entity_override_flag_appears(self):
        """Publisher with entity override note should appear as ⚠️ line."""
        overrides = {
            "publishers": {
                "TuitionHero": {
                    "note": "invalid conversions",
                    "added_by": "scout-agent",
                }
            },
            "advertisers": {},
        }
        result = _run(
            today_rows=[(1, "AT&T", 5000.0, 100)],
            avg_rows=[(1, 6000.0)],
            overrides=overrides,
        )
        formatted = result["formatted"]
        self.assertIn("⚠️", formatted)
        self.assertIn("TuitionHero", formatted)
        self.assertIn("invalid conversions", formatted)

    def test_empty_state_early_morning(self):
        """No rows from ClickHouse returns the empty-state message."""
        result = _run(today_rows=[], avg_rows=[])
        self.assertTrue(result.get("pre_formatted"))
        self.assertIn("No revenue data", result["formatted"])

    def test_green_yellow_red_thresholds(self):
        """Signal emoji matches threshold: ≥80% → 🟢, 40-79% → 🟡, <40% → 🔴."""
        rows = [
            (1, "GreenPub",  9000.0, 90),   # 90% of 10K avg → 🟢
            (2, "YellowPub", 6000.0, 60),   # 60% of 10K avg → 🟡
            (3, "RedPub",    3000.0, 30),   # 30% of 10K avg → 🔴
        ]
        avg_rows = [
            (1, 10000.0),
            (2, 10000.0),
            (3, 10000.0),
        ]
        result = _run(today_rows=rows, avg_rows=avg_rows)
        formatted = result["formatted"]
        lines = formatted.split("\n")
        green_line = next((l for l in lines if "GreenPub" in l), "")
        yellow_line = next((l for l in lines if "YellowPub" in l), "")
        red_line = next((l for l in lines if "RedPub" in l), "")
        self.assertIn("🟢", green_line)
        self.assertIn("🟡", yellow_line)
        self.assertIn("🔴", red_line)

    def test_revenue_rounding(self):
        """$14,041 → $14K; $3,392 → $3,400; small amounts stay exact."""
        rows = [
            (1, "BigPub",  14041.0, 100),
            (2, "MidPub",  3392.0,  50),
            (3, "SmallPub", 450.0,  10),
        ]
        avg_rows = [(i, 12000.0) for i in range(1, 4)]
        result = _run(today_rows=rows, avg_rows=avg_rows)
        formatted = result["formatted"]
        self.assertIn("$14K", formatted)
        self.assertIn("$3,400", formatted)
        self.assertIn("$450", formatted)


if __name__ == "__main__":
    unittest.main()
