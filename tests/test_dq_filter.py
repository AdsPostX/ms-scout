"""
DQ pre-filter tests — Phase 1.5-lite.

Pins the three known-bad postback patterns the filter must drop before
threshold logic runs. No ClickHouse, no Slack, no Anthropic — exercises the
pure helper directly. Mirrors the style of test_ask_contract.py.
"""
import os
import sys
import unittest
import unittest.mock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scout_bot import _row_is_dq_bad, _apply_dq_filter, _post_dq_advisory  # noqa: E402


class TestRowIsDqBad(unittest.TestCase):
    def test_clean_row_passes(self):
        row = {"publisher_name": "P", "cvr_today": 0.04, "sessions_today": 1200,
               "rpc": 1.20, "conversions": 50}
        self.assertIsNone(_row_is_dq_bad(row))

    def test_cvr_100_percent_fraction_form(self):
        row = {"cvr_today": 1.0, "sessions_today": 500}
        reason = _row_is_dq_bad(row)
        self.assertIsNotNone(reason)
        self.assertIn("postback misfire", reason)

    def test_cvr_100_percent_percent_form(self):
        row = {"cvr_today": 100.0, "sessions_today": 500}
        reason = _row_is_dq_bad(row)
        self.assertIsNotNone(reason)
        self.assertIn("postback misfire", reason)

    def test_zero_sessions(self):
        row = {"cvr_today": 0.02, "sessions_today": 0}
        reason = _row_is_dq_bad(row)
        self.assertIsNotNone(reason)
        self.assertIn("no traffic", reason)

    def test_postback_storm(self):
        # rpc < $0.50 AND conversions > 1000 → storm
        row = {"rpc": 0.10, "conversions": 5000, "sessions_today": 200}
        reason = _row_is_dq_bad(row)
        self.assertIsNotNone(reason)
        self.assertIn("postback storm", reason)

    def test_low_rpc_alone_is_fine(self):
        # Low RPC without huge conversions should NOT trip the storm rule.
        row = {"rpc": 0.10, "conversions": 5, "sessions_today": 200}
        self.assertIsNone(_row_is_dq_bad(row))

    def test_high_conversions_alone_is_fine(self):
        # High conversions without low RPC should NOT trip the storm rule.
        row = {"rpc": 5.00, "conversions": 5000, "sessions_today": 200}
        self.assertIsNone(_row_is_dq_bad(row))

    def test_non_dict_row_returns_none(self):
        self.assertIsNone(_row_is_dq_bad("garbage"))  # type: ignore[arg-type]
        self.assertIsNone(_row_is_dq_bad(None))  # type: ignore[arg-type]

    def test_string_values_coerced(self):
        # ClickHouse sometimes returns numeric fields as strings; coerce safely.
        row = {"cvr_today": "1.0", "sessions_today": "500"}
        self.assertIsNotNone(_row_is_dq_bad(row))


class TestApplyDqFilter(unittest.TestCase):
    def test_empty_input(self):
        clean, dropped = _apply_dq_filter([])
        self.assertEqual(clean, [])
        self.assertEqual(dropped, [])

    def test_splits_clean_and_dropped(self):
        rows = [
            {"publisher_name": "OK", "cvr_today": 0.04, "sessions_today": 1000},
            {"publisher_name": "Misfire", "cvr_today": 1.0, "sessions_today": 100},
            {"publisher_name": "Storm", "rpc": 0.10, "conversions": 5000},
        ]
        clean, dropped = _apply_dq_filter(rows)
        self.assertEqual(len(clean), 1)
        self.assertEqual(clean[0]["publisher_name"], "OK")
        self.assertEqual(len(dropped), 2)
        names = {r["publisher_name"] for r, _ in dropped}
        self.assertEqual(names, {"Misfire", "Storm"})

    def test_preserves_row_order_in_clean(self):
        rows = [
            {"publisher_name": "A", "sessions_today": 100},
            {"publisher_name": "Bad", "sessions_today": 0},
            {"publisher_name": "B", "sessions_today": 100},
        ]
        clean, _ = _apply_dq_filter(rows)
        self.assertEqual([r["publisher_name"] for r in clean], ["A", "B"])


class TestPostDqAdvisory(unittest.TestCase):
    def test_no_post_when_dropped_empty(self):
        web = unittest.mock.MagicMock()
        _post_dq_advisory(web, "cap-monitor", [])
        web.chat_postMessage.assert_not_called()

    def test_posts_to_shadow_channel_when_dropped(self):
        web = unittest.mock.MagicMock()
        dropped = [({"publisher_name": "Misfire"}, "cvr_today=100% (postback misfire)")]
        _post_dq_advisory(web, "cap-monitor", dropped)
        web.chat_postMessage.assert_called_once()
        kwargs = web.chat_postMessage.call_args.kwargs
        self.assertIn("Misfire", kwargs["text"])
        self.assertIn("cap-monitor", kwargs["text"])
        # Channel must be the shadow (HQ) channel — not the production routing.
        from scout_bot import _SCOUT_HQ_CHANNEL
        self.assertEqual(kwargs["channel"], _SCOUT_HQ_CHANNEL)

    def test_swallows_post_errors(self):
        web = unittest.mock.MagicMock()
        web.chat_postMessage.side_effect = RuntimeError("slack down")
        dropped = [({"publisher_name": "P"}, "sessions_today=0 (no traffic)")]
        # Must not raise.
        _post_dq_advisory(web, "fill-monitor", dropped)


if __name__ == "__main__":
    unittest.main()
