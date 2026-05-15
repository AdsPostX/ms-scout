"""
ask() return-type contract tests — Part 4 (Usage Tracking Bug Fix).

Pins the AskResult dataclass so the telemetry gate at scout_handlers.py
(formerly `isinstance(response, dict)`) cannot regress to an untyped shape
that silently drops `tools_called` from `usage_log.jsonl`.

These tests do NOT call the live Anthropic API. They exercise the typed
boundary contract directly: ask() must return AskResult on every code
path, including the no-API-key early return.
"""
import dataclasses
import os
import sys
import unittest
import unittest.mock

# scout_agent.py lives one level up from tests/
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scout_agent import AskResult, ask, _validate_sql_query  # noqa: E402


class TestAskContract(unittest.TestCase):
    def test_ask_returns_typed_result_with_no_api_key(self):
        """Early return path (no ANTHROPIC_API_KEY) must still yield AskResult."""
        with unittest.mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            r = ask("status", user_id="test-contract")
        self.assertIsInstance(r, AskResult)
        self.assertIsInstance(r.text, str)
        # tools_called is coerced to tuple by __post_init__ for deep immutability.
        self.assertIsInstance(r.tools_called, tuple)
        self.assertIsInstance(r.duration_ms, int)

    def test_askresult_is_frozen(self):
        """AskResult must be immutable so handlers can't mutate telemetry mid-flight."""
        r = AskResult(text="x", tools_called=(), duration_ms=0)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            r.text = "y"  # type: ignore[misc]

    def test_askresult_tools_called_is_immutable_sequence(self):
        """Passing a list at construction must be coerced to a tuple (no .append())."""
        r = AskResult(text="x", tools_called=["a", "b"], duration_ms=0)
        self.assertIsInstance(r.tools_called, tuple)
        self.assertEqual(r.tools_called, ("a", "b"))
        with self.assertRaises(AttributeError):
            r.tools_called.append("c")  # type: ignore[attr-defined]

    def test_askresult_payload_is_read_only_mapping(self):
        """payload dict must be wrapped in MappingProxyType so handlers can't mutate it."""
        r = AskResult(text="x", tools_called=(), duration_ms=0, payload={"k": "v"})
        # Still dict-like for read access.
        self.assertEqual(r.payload["k"], "v")
        # But mutation is blocked at runtime.
        with self.assertRaises(TypeError):
            r.payload["k"] = "z"  # type: ignore[index]

    def test_askresult_payload_defaults_to_none(self):
        """Payload field is optional — plain-text responses must not require it."""
        r = AskResult(text="hello", tools_called=("q",), duration_ms=42)
        self.assertIsNone(r.payload)
        self.assertEqual(r.tools_called, ("q",))
        self.assertEqual(r.duration_ms, 42)

    def test_askresult_payload_round_trips(self):
        """Structured-dispatch payload must survive untouched for handler routing."""
        payload = {"type": "brief", "brief_data": {"advertiser": "ACME"}}
        r = AskResult(text="brief ready", tools_called=(), duration_ms=0, payload=payload)
        self.assertEqual(r.payload["type"], "brief")
        self.assertEqual(r.payload["brief_data"]["advertiser"], "ACME")


class TestValidateSqlQuery(unittest.TestCase):
    """Safety-gate validator for run_sql_query — logs warnings, never blocks."""

    def test_clean_query_with_prewhere_date_and_limit_has_no_warnings(self):
        sql = (
            "SELECT count() FROM adpx_sdk_sessions "
            "PREWHERE user_id = 1 "
            "WHERE created_at >= today() - 7 "
            "LIMIT 10"
        )
        self.assertEqual(_validate_sql_query(sql), [])

    def test_missing_prewhere_on_large_table_warns(self):
        sql = (
            "SELECT count() FROM adpx_sdk_sessions "
            "WHERE user_id = 1 AND created_at >= today() - 7 "
            "LIMIT 10"
        )
        warnings = _validate_sql_query(sql)
        self.assertTrue(
            any("missing PREWHERE" in w and "adpx_sdk_sessions" in w for w in warnings),
            f"expected PREWHERE warning, got {warnings!r}",
        )

    def test_missing_date_filter_on_large_table_warns(self):
        sql = (
            "SELECT count() FROM adpx_tracked_clicks "
            "PREWHERE user_id = 1 "
            "LIMIT 10"
        )
        warnings = _validate_sql_query(sql)
        self.assertTrue(
            any("no date filter" in w and "adpx_tracked_clicks" in w for w in warnings),
            f"expected date-filter warning, got {warnings!r}",
        )

    def test_missing_limit_warns(self):
        sql = (
            "SELECT count() FROM adpx_conversionsdetails "
            "PREWHERE user_id = 1 "
            "WHERE created_at >= today() - 7"
        )
        warnings = _validate_sql_query(sql)
        self.assertIn("no LIMIT clause", warnings)

    def test_small_table_query_without_filters_only_warns_about_limit(self):
        # Queries against tables not in the large-table list shouldn't trigger
        # PREWHERE/date warnings — only the LIMIT check applies.
        sql = "SELECT * FROM mv_adpx_users"
        warnings = _validate_sql_query(sql)
        self.assertEqual(warnings, ["no LIMIT clause"])

    def test_case_insensitive_table_detection(self):
        sql = "SELECT * FROM ADPX_IMPRESSIONS_DETAILS LIMIT 10"
        warnings = _validate_sql_query(sql)
        self.assertTrue(
            any("adpx_impressions_details" in w for w in warnings),
            f"case-insensitive match failed, got {warnings!r}",
        )

    def test_empty_or_invalid_input_returns_empty_list(self):
        self.assertEqual(_validate_sql_query(""), [])
        self.assertEqual(_validate_sql_query(None), [])  # type: ignore[arg-type]

    def test_created_at_in_select_only_does_not_satisfy_date_filter(self):
        # Regression: an earlier draft used `\bCREATED_AT\b` anywhere in the SQL,
        # which matched the SELECT column list and silently suppressed the
        # "no date filter" warning. The scoped pattern must only count
        # created_at when it appears in PREWHERE/WHERE.
        sql = "SELECT created_at FROM adpx_tracked_clicks LIMIT 10"
        warnings = _validate_sql_query(sql)
        self.assertTrue(
            any("no date filter" in w and "adpx_tracked_clicks" in w for w in warnings),
            f"date-filter warning missing — created_at in SELECT must not count, got {warnings!r}",
        )


if __name__ == "__main__":
    unittest.main()
