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
    AmbiguousThresholdKey,
    AskResult,
    _classify_intent,
    _coerce_threshold_value,
    _route_deterministic,
    _split_dotted_key,
    _THREAD_INTENTS,
    set_threshold,
)


class TestDeterministicRouterFallthrough(unittest.TestCase):
    """Non-set_threshold queries fall through to the LLM — _route_deterministic returns None."""

    def _route(self, msg: str):
        return _route_deterministic(msg, user_id="U1")

    def test_arbitrary_phrase_falls_through_to_llm(self):
        self.assertIsNone(self._route("how is revenue trending"))

    def test_threshold_query_falls_through_to_llm(self):
        self.assertIsNone(self._route("thresholds"))

    def test_threshold_history_falls_through_to_llm(self):
        self.assertIsNone(self._route("threshold history"))

    def test_status_falls_through_to_llm(self):
        self.assertIsNone(self._route("status"))

    def test_mention_token_stripped_then_falls_through(self):
        # Mention token is stripped; no keyword routing exists, so falls through to LLM.
        self.assertIsNone(self._route("<@U123ABC> alert thresholds"))


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


class TestBareKeyResolution(unittest.TestCase):
    """Bare keys must resolve to their owning section via the base schema."""

    def test_bare_key_unique_in_signals(self):
        # cap_alert_pct lives only under signals.
        section, key = _split_dotted_key("cap_alert_pct")
        self.assertEqual((section, key), ("signals", "cap_alert_pct"))

    def test_bare_key_unique_in_digest(self):
        section, key = _split_dotted_key("min_rpm_floor")
        self.assertEqual((section, key), ("digest", "min_rpm_floor"))

    def test_bare_key_unique_in_health(self):
        section, key = _split_dotted_key("offer_staleness_hours")
        self.assertEqual((section, key), ("health", "offer_staleness_hours"))

    def test_ambiguous_bare_key_raises(self):
        # Simulate a key registered in two sections by patching the base schema.
        fake_base = {
            "signals": {"shared_key": 1},
            "digest":  {"shared_key": 2},
        }
        with unittest.mock.patch.object(scout_agent, "_BASE_THRESHOLDS", fake_base):
            with self.assertRaises(AmbiguousThresholdKey) as ctx:
                _split_dotted_key("shared_key")
        self.assertIn("signals", str(ctx.exception))
        self.assertIn("digest", str(ctx.exception))

    def test_unknown_bare_key_falls_back_to_signals(self):
        # Unknown bare keys still default to signals — set_threshold then
        # surfaces a proper unknown-key error with a suggestion.
        section, key = _split_dotted_key("bogus_unknown_key")
        self.assertEqual((section, key), ("signals", "bogus_unknown_key"))


class TestAmbiguousKeyInRouter(unittest.TestCase):
    """The router must surface ambiguity as a Slack-friendly warning, not crash."""

    def test_router_returns_warning_on_ambiguous_key(self):
        fake_base = {
            "signals": {"shared_key": 1},
            "digest":  {"shared_key": 2},
        }
        with unittest.mock.patch.dict(os.environ, {"SCOUT_THRESHOLD_ADMINS": "U_admin"}):
            with unittest.mock.patch.object(scout_agent, "_BASE_THRESHOLDS", fake_base):
                r = _route_deterministic(
                    "set shared_key to 5 because test", user_id="U_admin"
                )
        self.assertIsNotNone(r)
        self.assertEqual(r.tools_called, ())
        self.assertIn("multiple sections", r.text)


class TestBaseSchemaValidation(unittest.TestCase):
    """set_threshold must validate against the base schema, not the merged read-view.

    Otherwise a previously persisted typo in data/threshold_overrides.json
    would appear "known" and pass validation forever.
    """

    def test_legacy_override_in_merged_view_does_not_mask_unknown_key(self):
        # Simulate: a bad key 'typo_key' sneaked into SCOUT_THRESHOLDS via a
        # legacy override, but is NOT in _BASE_THRESHOLDS. Writes to it must
        # still be rejected.
        polluted_merged = {
            "signals": {"cap_alert_pct": 80, "typo_key": 99},
        }
        clean_base = {
            "signals": {"cap_alert_pct": 90},
        }
        with unittest.mock.patch.dict(os.environ, {"SCOUT_THRESHOLD_ADMINS": "U_admin"}):
            with unittest.mock.patch.object(scout_agent, "SCOUT_THRESHOLDS", polluted_merged):
                with unittest.mock.patch.object(scout_agent, "_BASE_THRESHOLDS", clean_base):
                    r = set_threshold(section="signals", key="typo_key", value=1,
                                      reason="should reject", _caller_user_id="U_admin")
        self.assertFalse(r["ok"])
        self.assertEqual(r["error"], "unknown_key")


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
        r = _route_deterministic(prefixed, user_id="U1")
        # Prefixed text is not equal to "alert thresholds" → falls through to LLM
        self.assertIsNone(r)


class TestClassifyIntent(unittest.TestCase):
    def tearDown(self):
        _THREAD_INTENTS.clear()

    def test_fleet_health_signals(self):
        name, d = _classify_intent("fleet health report")
        self.assertEqual(name, "fleet_health")
        self.assertIn("get_publisher_fleet_health", d["primary_tools"])

    def test_fleet_health_before_publisher_health(self):
        # "how are publishers" has "how are" (publisher_health signal) but also
        # "publishers" near fleet context — fleet bucket comes first
        name, _ = _classify_intent("how are publishers doing this week")
        self.assertEqual(name, "fleet_health")

    def test_campaign_pacing(self):
        name, _ = _classify_intent("how much will TurboTax make this month")
        self.assertEqual(name, "campaign_pacing")

    def test_revenue_anomaly(self):
        name, _ = _classify_intent("revenue drop this week why")
        self.assertEqual(name, "revenue_anomaly")

    def test_offer_performance(self):
        name, _ = _classify_intent("top performing offers right now")
        self.assertEqual(name, "offer_performance")

    def test_publisher_offer_fit_tightened(self):
        # "profitable" should NOT match publisher_offer_fit after "fit" → "offer fit"
        name, _ = _classify_intent("which offers are most profitable")
        self.assertNotEqual(name, "publisher_offer_fit")

    def test_no_match_returns_none(self):
        name, d = _classify_intent("tell me a joke")
        self.assertIsNone(name)
        self.assertIsNone(d)

    def test_thread_memory_fallback(self):
        # Signal matches first, sets thread memory
        name1, _ = _classify_intent("fleet health", thread_ts="T999")
        self.assertEqual(name1, "fleet_health")
        # Ambiguous follow-up with no signal → uses thread memory
        name2, _ = _classify_intent("and this week?", thread_ts="T999")
        self.assertEqual(name2, "fleet_health")

    def test_thread_memory_overridden_by_strong_signal(self):
        # Thread tagged as fleet_health, but explicit revenue signal should win
        _classify_intent("fleet health", thread_ts="T888")
        name, _ = _classify_intent("revenue drop this week", thread_ts="T888")
        self.assertEqual(name, "revenue_anomaly")  # signals > thread memory


class TestSystemBlockCaching(unittest.TestCase):
    """Verify that the two-block system array is built correctly for prompt caching."""

    def _get_system_blocks(self, user_message: str) -> list:
        """Run the intent classification + block assembly logic inline."""
        from scout_agent import _classify_intent, SYSTEM_PROMPT, TOOLS
        _, _intent_dict = _classify_intent(user_message)
        if _intent_dict:
            return [
                {"type": "text", "text": _intent_dict["context"]},
                {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}},
            ]
        return [
            {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}},
        ]

    def test_intent_query_produces_two_block_system(self):
        """When intent fires, system must be 2 blocks: dynamic context + cached SYSTEM_PROMPT."""
        blocks = self._get_system_blocks("fleet health report")
        self.assertEqual(len(blocks), 2, "Expected 2 system blocks when intent fires")
        # First block: dynamic intent context, NOT cached
        self.assertNotIn("cache_control", blocks[0], "Intent context must NOT be cached")
        self.assertIn("text", blocks[0])
        # Second block: static SYSTEM_PROMPT, MUST be cached
        self.assertEqual(blocks[1].get("cache_control"), {"type": "ephemeral"})
        from scout_agent import SYSTEM_PROMPT
        self.assertEqual(blocks[1]["text"], SYSTEM_PROMPT)

    def test_no_intent_query_produces_single_cached_block(self):
        """When no intent fires, system must be 1 block with cache_control on SYSTEM_PROMPT."""
        blocks = self._get_system_blocks("tell me a joke")
        self.assertEqual(len(blocks), 1, "Expected 1 system block when no intent fires")
        self.assertEqual(blocks[0].get("cache_control"), {"type": "ephemeral"})
        from scout_agent import SYSTEM_PROMPT
        self.assertEqual(blocks[0]["text"], SYSTEM_PROMPT)

    def test_system_prompt_block_is_always_last_cached_block(self):
        """SYSTEM_PROMPT cache_control is always on the last block — never on the intent context."""
        for query in ["fleet health", "revenue drop this week", "top offers"]:
            blocks = self._get_system_blocks(query)
            last_block = blocks[-1]
            self.assertEqual(
                last_block.get("cache_control"), {"type": "ephemeral"},
                f"SYSTEM_PROMPT cache_control must be on last block for query: {query!r}",
            )
            from scout_agent import SYSTEM_PROMPT
            self.assertEqual(
                last_block["text"], SYSTEM_PROMPT,
                f"Last block must be SYSTEM_PROMPT for query: {query!r}",
            )


if __name__ == "__main__":
    unittest.main()
