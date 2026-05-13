"""
tests/test_text_to_blocks.py — Unit tests for _text_to_blocks() pipe table fallback.

Tests describe behavior contracts, not implementation history.
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scout_slack_ui import _text_to_blocks


class TestPipeTableFallback(unittest.TestCase):

    def test_pipe_table_becomes_preformatted(self):
        """Pipe tables are converted to rich_text_preformatted blocks."""
        text = "| Col1 | Col2 | Col3 |\n|---|---|---|\n| A | B | C |"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        self.assertEqual(rt["type"], "rich_text")
        el = rt["elements"][0]
        self.assertEqual(el["type"], "rich_text_preformatted")
        self.assertIn("Col1", el["elements"][0]["text"])
        # Separator row is skipped
        self.assertNotIn("---|---", el["elements"][0]["text"])

    def test_pipe_table_separator_rows_omitted(self):
        """Separator rows (|---|---|) are silently dropped from the output."""
        text = "| A | B | C |\n|:--|:--|:--|\n| 1 | 2 | 3 |"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        el = rt["elements"][0]
        self.assertEqual(el["type"], "rich_text_preformatted")
        rendered = el["elements"][0]["text"]
        self.assertNotIn("---", rendered)
        self.assertIn("| A | B | C |", rendered)
        self.assertIn("| 1 | 2 | 3 |", rendered)

    def test_single_pipe_line_not_treated_as_table(self):
        """A line with only one pipe (single-column) is not treated as a table row."""
        text = "| Active |"
        blocks = _text_to_blocks(text)
        # Should NOT produce a rich_text_preformatted block
        rt = blocks[0]
        has_preformatted = any(
            el.get("type") == "rich_text_preformatted"
            for el in rt.get("elements", [])
        )
        self.assertFalse(has_preformatted)

    def test_fenced_code_block_unchanged(self):
        """Fenced code blocks remain as rich_text_preformatted (existing behavior)."""
        text = "```\nSELECT * FROM table\n```"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        el = rt["elements"][0]
        self.assertEqual(el["type"], "rich_text_preformatted")
        self.assertIn("SELECT", el["elements"][0]["text"])

    def test_bullet_list_renders_as_list(self):
        """Bullet lists render as rich_text_list (existing behavior)."""
        text = "• Publisher A\n• Publisher B"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        el = rt["elements"][0]
        self.assertEqual(el["type"], "rich_text_list")


if __name__ == "__main__":
    unittest.main()
