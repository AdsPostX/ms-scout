"""
tests/test_text_to_blocks.py — Unit tests for _text_to_blocks() pipe table handling.

Tests describe behavior contracts, not implementation history.
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scout_ui_kit import _text_to_blocks


class TestPipeTableFallback(unittest.TestCase):

    def test_pipe_table_becomes_preformatted(self):
        """3-col pipe tables are converted to rich_text_section rows (mobile-safe)."""
        text = "| Col1 | Col2 | Col3 |\n|---|---|---|\n| A | B | C |"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        self.assertEqual(rt["type"], "rich_text")
        # Narrow table (≤3 cols) → rich_text_section rows, not preformatted
        has_section = any(
            el.get("type") == "rich_text_section" for el in rt.get("elements", [])
        )
        self.assertTrue(has_section, "Expected rich_text_section elements for 3-col table")
        # Verify the header cell content is present (as bold text)
        all_text = " ".join(
            e.get("text", "") for el in rt["elements"]
            for e in el.get("elements", [])
        )
        self.assertIn("Col1", all_text)
        self.assertNotIn("---|---", all_text)

    def test_pipe_table_separator_rows_omitted(self):
        """Separator rows are dropped; data rows render as rich_text_section."""
        text = "| A | B | C |\n|:--|:--|:--|\n| 1 | 2 | 3 |"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        all_text = " ".join(
            e.get("text", "") for el in rt["elements"]
            for e in el.get("elements", [])
        )
        self.assertNotIn("---", all_text)
        self.assertIn("A", all_text)
        self.assertIn("1", all_text)

    def test_wide_pipe_table_stays_preformatted(self):
        """4+ col tables stay as rich_text_preformatted with a mobile warning section."""
        text = "| A | B | C | D |\n|---|---|---|---|\n| 1 | 2 | 3 | 4 |"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        elements = rt["elements"]
        # First element: preformatted block for the wide table
        self.assertEqual(elements[0]["type"], "rich_text_preformatted",
                         "Wide table must stay preformatted")
        # Second element: mobile warning
        self.assertEqual(elements[1]["type"], "rich_text_section",
                         "Mobile warning section must follow the preformatted block")
        warning_text = elements[1]["elements"][0].get("text", "")
        self.assertIn("mobile", warning_text.lower())

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
