"""
tests/test_text_to_blocks.py — Unit tests for _text_to_blocks() and inline element parsing.

Tests describe behavior contracts, not implementation history.
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scout_ui_kit import _text_to_blocks, _parse_inline_elements


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


class TestRichTextSpecGaps(unittest.TestCase):

    def test_blockquote_produces_rich_text_quote(self):
        """> lines emit rich_text_quote elements, not context blocks."""
        text = "> This is a quoted line"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        self.assertEqual(rt["type"], "rich_text")
        rt_types = [el["type"] for el in rt["elements"]]
        self.assertIn("rich_text_quote", rt_types)
        self.assertNotIn("context", [b["type"] for b in blocks])

    def test_strike_style_for_double_tilde(self):
        """~~text~~ produces a text element with style.strike = True."""
        elements = _parse_inline_elements("~~deprecated~~")
        strike = next((e for e in elements if e.get("style", {}).get("strike")), None)
        self.assertIsNotNone(strike, "No strike element found")
        self.assertEqual(strike["text"], "deprecated")

    def test_fenced_code_block_with_language(self):
        """Opening fence with language tag sets the language field on the preformatted block."""
        text = "```python\nprint('hello')\n```"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        pre = next((el for el in rt["elements"] if el["type"] == "rich_text_preformatted"), None)
        self.assertIsNotNone(pre)
        self.assertEqual(pre.get("language"), "python")

    def test_fenced_code_block_without_language(self):
        """Opening fence without language tag omits the language field entirely."""
        text = "```\nSELECT 1\n```"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        pre = next((el for el in rt["elements"] if el["type"] == "rich_text_preformatted"), None)
        self.assertIsNotNone(pre)
        self.assertNotIn("language", pre)

    def test_ordered_list_produces_ordered_rich_text_list(self):
        """Numbered list items produce rich_text_list with style='ordered'."""
        text = "1. First item\n2. Second item\n3. Third item"
        blocks = _text_to_blocks(text)
        rt = blocks[0]
        ol = next(
            (el for el in rt["elements"]
             if el["type"] == "rich_text_list" and el["style"] == "ordered"),
            None,
        )
        self.assertIsNotNone(ol, "No ordered list found")
        self.assertEqual(len(ol["elements"]), 3)


if __name__ == "__main__":
    unittest.main()
