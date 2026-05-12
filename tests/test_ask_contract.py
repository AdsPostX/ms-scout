"""
ask() return-type contract tests — Part 4 (Usage Tracking Bug Fix).

Pins the AskResult dataclass so the telemetry gate at scout_handlers.py
(formerly `isinstance(response, dict)`) cannot regress to an untyped shape
that silently drops `tools_called` from `usage_log.jsonl`.

These tests do NOT call the live Anthropic API. They exercise the typed
boundary contract directly: ask() must return AskResult on every code
path, including the no-API-key early return.
"""
import os
import sys
import unittest
import unittest.mock

# scout_agent.py lives one level up from tests/
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scout_agent import AskResult, ask  # noqa: E402


class TestAskContract(unittest.TestCase):
    def test_ask_returns_typed_result_with_no_api_key(self):
        """Early return path (no ANTHROPIC_API_KEY) must still yield AskResult."""
        with unittest.mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            r = ask("status", user_id="test-contract")
        self.assertIsInstance(r, AskResult)
        self.assertIsInstance(r.text, str)
        self.assertIsInstance(r.tools_called, list)
        self.assertIsInstance(r.duration_ms, int)

    def test_askresult_is_frozen(self):
        """AskResult must be immutable so handlers can't mutate telemetry mid-flight."""
        r = AskResult(text="x", tools_called=[], duration_ms=0)
        with self.assertRaises(Exception):
            r.text = "y"  # type: ignore[misc]

    def test_askresult_payload_defaults_to_none(self):
        """Payload field is optional — plain-text responses must not require it."""
        r = AskResult(text="hello", tools_called=["q"], duration_ms=42)
        self.assertIsNone(r.payload)
        self.assertEqual(r.tools_called, ["q"])
        self.assertEqual(r.duration_ms, 42)

    def test_askresult_payload_round_trips(self):
        """Structured-dispatch payload must survive untouched for handler routing."""
        payload = {"type": "brief", "brief_data": {"advertiser": "ACME"}}
        r = AskResult(text="brief ready", tools_called=[], duration_ms=0, payload=payload)
        self.assertEqual(r.payload["type"], "brief")
        self.assertEqual(r.payload["brief_data"]["advertiser"], "ACME")


if __name__ == "__main__":
    unittest.main()
