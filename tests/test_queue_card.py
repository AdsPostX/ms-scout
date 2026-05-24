"""
Queue card renderer tests for _build_queue_card() in scout_slack_ui.py.
Tests _build_queue_card() directly — NOT _build_home_view() — to avoid the deferred
scout_agent import at line ~1556 of scout_slack_ui.py pulling in ClickHouse/.env state.
All tests are pure unit tests: no external calls, no env var requirements.
"""
import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-fake")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-fake")
os.environ.setdefault("CLICKHOUSE_HOST", "localhost")

from scout_ui_kit import _build_queue_card, _MAX_QUEUE_ITEMS_RENDERED  # noqa: E402


def _all_text(blocks: list) -> str:
    """Flatten all mrkdwn/plain_text content from blocks into one string."""
    parts = []
    for b in blocks:
        t = b.get("text")
        if isinstance(t, dict):
            parts.append(t.get("text", ""))
        for f in b.get("fields", []):
            if isinstance(f, dict):
                parts.append(f.get("text", ""))
        for e in b.get("elements", []):
            if isinstance(e, dict):
                et = e.get("text", "")
                parts.append(et if isinstance(et, str) else "")
    return " ".join(parts)


def _make_item(advertiser: str, status: str, network: str = "Impact", payout: float = 50.0) -> dict:
    return {
        "page_id": "abc123",
        "advertiser": advertiser,
        "payout": payout,
        "payout_type": "CPA",
        "network": network,
        "status": status,
        "notion_url": "https://www.notion.so/abc123",
        "approved_by": "",
        "approved_at": "",
        "category": "",
    }


class TestBuildQueueCard(unittest.TestCase):

    def test_empty_state_shows_queue_clear_message(self):
        blocks = _build_queue_card([])
        text = _all_text(blocks)
        self.assertIn("clear", text.lower(), "empty queue should mention 'clear'")
        self.assertNotIn("unavailable", text.lower(), "empty state confused with error state")

    def test_error_state_shows_notion_unavailable_not_clear(self):
        blocks = _build_queue_card(None)
        text = _all_text(blocks)
        self.assertIn("unavailable", text.lower(), "error state should say Notion unavailable")
        self.assertNotIn("clear", text.lower(), "error state confused with empty state")

    def test_items_grouped_by_status_with_correct_emoji(self):
        items = [
            _make_item("Nike", "Awaiting Entry"),
            _make_item("Adidas", "In Platform"),
            _make_item("Puma", "Test Offer ON"),
            _make_item("Reebok", "Live"),
        ]
        blocks = _build_queue_card(items)
        text = _all_text(blocks)
        # Each advertiser should appear
        for brand in ["Nike", "Adidas", "Puma", "Reebok"]:
            self.assertIn(brand, text, f"{brand} missing from queue card")
        # Status emojis
        self.assertIn("🟡", text, "Awaiting Entry emoji 🟡 missing")
        self.assertIn("🔵", text, "In Platform emoji 🔵 missing")
        self.assertIn("🟠", text, "Test Offer ON emoji 🟠 missing")
        self.assertIn("✅", text, "Live emoji ✅ missing")

    def test_unknown_status_falls_back_gracefully(self):
        items = [_make_item("Mystery Co", "Some New Status")]
        blocks = _build_queue_card(items)
        text = _all_text(blocks)
        self.assertIn("Mystery Co", text, "item with unknown status should still render")
        self.assertIn("Some New Status", text, "unknown status label should appear")

    def test_queue_card_block_count_under_budget_with_12_offers(self):
        items = [
            _make_item(f"Advertiser {i}", "Awaiting Entry")
            for i in range(_MAX_QUEUE_ITEMS_RENDERED)
        ]
        blocks = _build_queue_card(items)
        # Budget: 36 blocks for queue card + ~14 fixed Home blocks = 50 max
        # This test covers the queue card alone — must stay well under 36
        self.assertLessEqual(
            len(blocks), 36,
            f"queue card with {_MAX_QUEUE_ITEMS_RENDERED} offers produced {len(blocks)} blocks (budget: 36)"
        )


if __name__ == "__main__":
    unittest.main()
