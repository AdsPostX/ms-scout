"""Deterministic intent pre-router — exercise the keyword/regex routes
that short-circuit the LLM call for control-surface verbs.

No ClickHouse, no Anthropic. Pure routing logic.
"""
import os
import sys
import unittest
import unittest.mock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import scout_agent  # noqa: E402
from scout_agent import (  # noqa: E402
    AskResult,
    _coerce_threshold_value,
    _route_deterministic,
    _split_dotted_key,
    set_threshold,
)


class TestKeywordRoutes(unittest.TestCase):
    """Each phrase in the footer-trained vocabulary must route to its tool."""

    def _route(self, msg: str):
        with unittest.mock.patch.object(scout_agent, "TOOL_MAP", {
            "list_thresholds":       lambda: {"thresholds": {"signals": {"cap_alert_pct": 85}}, "overridden": {}},
            "get_scout_config":      lambda: {"version": "test"},
            "get_scout_status":      lambda: {"status": "ok"},
            "get_threshold_history": lambda: {"entries": [], "count": 0},
        }):
            return _route_deterministic(msg, user_id="U1")

    def test_alert_thresholds_routes_to_list_thresholds(self):
        r = self._route("alert thresholds")
        self.assertIsInstance(r, AskResult)
        self.assertEqual(r.tools_called, ("list_thresholds",))

    def test_thresholds_routes_to_list_thresholds(self):
        self.assertEqual(self._route("thresholds").tools_called, ("list_thresholds",))

    def test_settings_routes_to_list_thresholds(self):
        self.assertEqual(self._route("settings").tools_called, ("list_thresholds",))

    def test_config_routes_to_get_scout_config(self):
        self.assertEqual(self._route("config").tools_called, ("get_scout_config",))

    def test_status_routes_to_get_scout_status(self):
        self.assertEqual(self._route("status").tools_called, ("get_scout_status",))

    def test_threshold_history_routes_correctly(self):
        self.assertEqual(self._route("threshold history").tools_called, ("get_threshold_history",))

    def test_unknown_phrase_falls_through_to_llm(self):
        self.assertIsNone(self._route("how is revenue trending"))

    def test_mention_token_is_stripped(self):
        r = self._route("<@U123ABC> alert thresholds")
        self.assertIsNotNone(r)
        self.assertEqual(r.tools_called, ("list_thresholds",))

    def test_case_insensitive(self):
        self.assertEqual(self._route("ALERT THRESHOLDS").tools_called, ("list_thresholds",))

    def test_whitespace_trimmed(self):
        self.assertEqual(self._route("  thresholds  ").tools_called, ("list_thresholds",))


class TestSetThresholdRegex(unittest.TestCase):
    """`set X to Y because Z` parses deterministically."""

    def _route(self, msg: str, user_id: str = "U_admin"):
        # Provide an admin-like user_id; the underlying set_threshold call still
        # gates on the env var, so we patch that for the with-reason case.
        return _route_deterministic(msg, user_id=user_id)

    def test_full_regex_with_reason_calls_set_threshold(self):
        with unittest.mock.patch.dict(os.environ, {"SCOUT_THRESHOLD_ADMINS": "U_admin"}):
            with unittest.mock.patch.object(scout_agent, "set_threshold") as mock_set:
                mock_set.return_value = {"ok": True, "prior": 85, "value": 80, "reason": "burst caps"}
                r = self._route("set signals.cap_alert_pct to 80 because burst caps")
                self.assertIsNotNone(r)
                self.assertEqual(r.tools_called, ("set_threshold",))
                mock_set.assert_called_once()
                kwargs = mock_set.call_args.kwargs
                self.assertEqual(kwargs["section"], "signals")
                self.assertEqual(kwargs["key"], "cap_alert_pct")
                self.assertEqual(kwargs["value"], 80)
                self.assertEqual(kwargs["reason"], "burst caps")

    def test_missing_reason_returns_usage_hint(self):
        r = self._route("set signals.cap_alert_pct to 80")
        self.assertIsNotNone(r)
        self.assertEqual(r.tools_called, ())
        self.assertIn("Reason required", r.text)

    def test_negative_float_parsed(self):
        with unittest.mock.patch.dict(os.environ, {"SCOUT_THRESHOLD_ADMINS": "U_admin"}):
            with unittest.mock.patch.object(scout_agent, "set_threshold") as mock_set:
                mock_set.return_value = {"ok": True, "prior": -40, "value": -25.5, "reason": "tune"}
                self._route("set signals.velocity_down_threshold_pct to -25.5 because tune")
                kwargs = mock_set.call_args.kwargs
                self.assertEqual(kwargs["value"], -25.5)

    def test_bool_value_parsed(self):
        with unittest.mock.patch.dict(os.environ, {"SCOUT_THRESHOLD_ADMINS": "U_admin"}):
            with unittest.mock.patch.object(scout_agent, "set_threshold") as mock_set:
                mock_set.return_value = {"ok": True, "prior": True, "value": False, "reason": "pause"}
                self._route("set signals.cap_monitor_enabled to false because pause")
                kwargs = mock_set.call_args.kwargs
                self.assertIs(kwargs["value"], False)

    def test_bare_key_defaults_to_signals_section(self):
        section, key = _split_dotted_key("cap_alert_pct")
        self.assertEqual((section, key), ("signals", "cap_alert_pct"))

    def test_dotted_key_split(self):
        self.assertEqual(_split_dotted_key("digest.min_rpm_floor"), ("digest", "min_rpm_floor"))


class TestValueCoercion(unittest.TestCase):
    def test_int(self):
        self.assertEqual(_coerce_threshold_value("80"), 80)

    def test_negative_int(self):
        self.assertEqual(_coerce_threshold_value("-25"), -25)

    def test_float(self):
        self.assertEqual(_coerce_threshold_value("0.5"), 0.5)

    def test_negative_float(self):
        self.assertEqual(_coerce_threshold_value("-25.5"), -25.5)

    def test_true(self):
        self.assertIs(_coerce_threshold_value("true"), True)
        self.assertIs(_coerce_threshold_value("TRUE"), True)

    def test_false(self):
        self.assertIs(_coerce_threshold_value("false"), False)


class TestUnknownKeyRejection(unittest.TestCase):
    """set_threshold rejects unknown keys with closest-match suggestion."""

    def test_unknown_section_rejected(self):
        with unittest.mock.patch.dict(os.environ, {"SCOUT_THRESHOLD_ADMINS": "U_admin"}):
            r = set_threshold(section="signalz", key="cap_alert_pct", value=80,
                              reason="typo test", _caller_user_id="U_admin")
        self.assertFalse(r["ok"])
        self.assertEqual(r["error"], "unknown_section")
        self.assertIn("signals", r["message"])

    def test_unknown_key_rejected(self):
        with unittest.mock.patch.dict(os.environ, {"SCOUT_THRESHOLD_ADMINS": "U_admin"}):
            r = set_threshold(section="signals", key="cap_alert_pcT_typo", value=80,
                              reason="typo test", _caller_user_id="U_admin")
        self.assertFalse(r["ok"])
        self.assertEqual(r["error"], "unknown_key")

    def test_unknown_key_suggests_closest(self):
        with unittest.mock.patch.dict(os.environ, {"SCOUT_THRESHOLD_ADMINS": "U_admin"}):
            r = set_threshold(section="signals", key="cap_alrt_pct", value=80,
                              reason="typo test", _caller_user_id="U_admin")
        self.assertFalse(r["ok"])
        self.assertIn("cap_alert_pct", r["message"])


class TestRawMessageRouting(unittest.TestCase):
    """Pre-router must run on RAW user_message — context prefix would break it."""

    def test_context_prefix_does_not_route(self):
        # The wrapped form (with date/caller prefix) is what messages[] gets;
        # the pre-router must operate on the raw text only. We assert by
        # ensuring that prefix-wrapped text does NOT exact-match.
        prefixed = (
            "[Business date: Monday, May 17, 2026 (America/Chicago) — ...]\n"
            "[Caller Slack user_id: U1]\n"
            "alert thresholds"
        )
        with unittest.mock.patch.object(scout_agent, "TOOL_MAP", {
            "list_thresholds": lambda: {"thresholds": {}, "overridden": {}},
        }):
            r = _route_deterministic(prefixed, user_id="U1")
        # Prefixed text is not equal to "alert thresholds" → falls through to LLM
        self.assertIsNone(r)


if __name__ == "__main__":
    unittest.main()
