"""
Boot card renderer tests — migrated from smoke_test.py (PR 23).
Tests the format_slack_blocks() and post_to_slack() renderer in smoke_test.py.
All tests are pure unit tests: no external calls, no env var requirements.
"""
import sys
import os
import unittest
import unittest.mock

# smoke_test.py lives one level up from tests/
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from smoke_test import format_slack_blocks, post_to_slack  # noqa: E402


def _extract_text(blocks: list) -> str:
    """Flatten all text content from a Block Kit block list into a single string."""
    parts = []
    for b in blocks:
        if isinstance(b.get("text"), dict):
            parts.append(b["text"].get("text", ""))
        for e in b.get("elements", []):
            if isinstance(e, dict):
                t = e.get("text", "")
                parts.append(t if isinstance(t, str) else "")
    return " ".join(parts)


class TestBootCardRenderer(unittest.TestCase):

    def test_zero_tests_renders_as_failure(self):
        blocks, _ = format_slack_blocks([], 0)
        text = _extract_text(blocks)
        self.assertNotIn(":white_check_mark:", text, "0-test case rendered as pass")
        self.assertNotIn(":warning:", text, "0-test case rendered as warning, not failure")
        has_failure_signal = (
            ":x:" in text or "0 tests" in text or "no checks" in text.lower()
        )
        self.assertTrue(has_failure_signal, f"0-test card missing ❌ marker: {text[:120]}")

    def test_all_pass_collapses_to_summary_card(self):
        results = [{"name": f"test_{i}", "passed": True, "detail": "ok"} for i in range(10)]
        blocks, _ = format_slack_blocks(results, 10)
        self.assertLessEqual(len(blocks), 3, f"all-pass rendered {len(blocks)} blocks, expected ≤3")
        text = _extract_text(blocks)
        self.assertIn(":white_check_mark:", text, "all-pass card missing ✅")

    def test_failure_surfaced_and_passing_collapsed(self):
        results = [{"name": "bad_check", "passed": False, "detail": "it broke"}]
        results += [{"name": f"good_{i}", "passed": True, "detail": "ok"} for i in range(5)]
        blocks, _ = format_slack_blocks(results, 5)
        text = _extract_text(blocks)
        self.assertIn("bad_check", text, "failure name not surfaced in card")
        self.assertIn("it broke", text, "failure detail not surfaced in card")
        has_collapsed = "+5" in text or "5 other" in text
        self.assertTrue(has_collapsed, "passing count not collapsed into summary")
        for i in range(5):
            self.assertNotIn(f"good_{i}", text, f"good_{i} listed individually — should be collapsed")

    def test_failures_capped_at_10_with_overflow_note(self):
        results = [{"name": f"fail_{i}", "passed": False, "detail": f"err {i}"} for i in range(15)]
        blocks, _ = format_slack_blocks(results, 0)
        text = _extract_text(blocks)
        shown = sum(1 for i in range(15) if f"fail_{i}" in text)
        self.assertLessEqual(shown, 10, f"showed {shown} failures — must cap at 10")
        has_overflow = "5 more" in text or "and 5" in text
        self.assertTrue(has_overflow, f"overflow note missing — expected '5 more': {text[:200]}")

    def test_renderer_crash_falls_back_to_plain_text(self):
        results = [{"name": "check_a", "passed": True, "detail": "ok"}]
        with unittest.mock.patch(
            "smoke_test.format_slack_blocks",
            side_effect=RuntimeError("renderer exploded"),
        ):
            orig_token = os.environ.get("SLACK_BOT_TOKEN")
            try:
                os.environ["SLACK_BOT_TOKEN"] = "xoxb-fake-token-for-test"
                with unittest.mock.patch("slack_sdk.web.WebClient.chat_postMessage") as mock_post:
                    post_to_slack(results, 1)
                    self.assertTrue(mock_post.called, "chat_postMessage never called after crash")
                    call_kwargs = mock_post.call_args
                    kwargs = call_kwargs.kwargs or (call_kwargs[1] if len(call_kwargs) > 1 else {})
                    blocks_sent = kwargs.get("blocks")
                    self.assertIsNone(blocks_sent, f"crash still sent blocks: {blocks_sent}")
                    text = kwargs.get("text", "")
                    self.assertTrue(text, "fallback text was empty")
            finally:
                if orig_token is None:
                    os.environ.pop("SLACK_BOT_TOKEN", None)
                else:
                    os.environ["SLACK_BOT_TOKEN"] = orig_token


if __name__ == "__main__":
    unittest.main()
