"""
Lint tests for scout_ui_kit migration invariants.

Enforces that block builders do not contain legacy severity string literals
("WARNING:" / "CRITICAL:") that the kit Severity enum replaced, and verifies
that high-traffic surfaces (monitor alarms) apply enforce() budget caps.
Fails if a contributor bypasses the enum, hard-codes the old strings, or
returns unbudgeted blocks on a paging surface.
"""
import os
import pathlib
import sys
import unittest

_REPO = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_REPO))

_BUILDER_FILES = [
    _REPO / "scout_ui_kit.py",
    _REPO / "scout_bot.py",
    _REPO / "scout_handlers.py",
]
# Match the bold-formatted severity labels as they would appear in Slack mrkdwn.
# The legacy pattern is `*WARNING:*` / `*CRITICAL:*` (bold + colon).
# This avoids false positives from docstrings that reference the pattern by name.
_BANNED_PATTERNS = ["*WARNING:*", "*CRITICAL:*"]


class TestKitLint(unittest.TestCase):
    def test_no_legacy_severity_labels_in_block_builders(self):
        violations = []
        for path in _BUILDER_FILES:
            if not path.exists():
                continue
            lines = path.read_text().splitlines()
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                for banned in _BANNED_PATTERNS:
                    if banned in line:
                        violations.append(f"{path.name}:{i}: {stripped!r}")
        self.assertFalse(
            violations,
            "Legacy bold severity labels found in block builders — use Severity enum:\n"
            + "\n".join(violations),
        )

    def test_monitor_alert_enforces_budget_when_kit_enabled(self):
        """Monitor alert surface has a strict 6-block budget. wrap_response must
        enforce it even when card body + facts would otherwise exceed the cap."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response, BUDGETS
        # body + 10 facts would produce >6 blocks without enforcement
        facts = [(f"Pub {i}", f"${i*10}K") for i in range(10)]
        card = Card(Severity.WARN, "Enforce test", body="• alert\n• detail", facts=facts)
        _fallback, blocks = wrap_response(card=card, surface=Surface.MONITOR_ALARM, feedback="none")
        cap = BUDGETS[Surface.MONITOR_ALARM]
        self.assertLessEqual(
            len(blocks), cap,
            f"wrap_response returned {len(blocks)} blocks; MONITOR_ALARM budget is {cap}",
        )

    def test_home_view_action_ids_are_unique(self):
        """Slack mobile silently drops clicks when action_ids repeat in a
        single view. Regression guard for the 5-button Home that all shared
        action_id='home_try_query' and broke on iOS."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import _build_home_view

        view = _build_home_view()
        seen: dict[str, int] = {}
        for block in view.get("blocks", []):
            # actions block elements
            for el in block.get("elements", []) or []:
                aid = el.get("action_id") if isinstance(el, dict) else None
                if aid:
                    seen[aid] = seen.get(aid, 0) + 1
            # section.accessory
            acc = block.get("accessory") if isinstance(block, dict) else None
            if isinstance(acc, dict) and acc.get("action_id"):
                aid = acc["action_id"]
                seen[aid] = seen.get(aid, 0) + 1
        dupes = {k: v for k, v in seen.items() if v > 1}
        self.assertFalse(
            dupes,
            f"Duplicate action_ids in Home view (mobile drops clicks): {dupes}",
        )

    def test_monitor_alert_blocks_basic_sanity(self):
        """Sanity: wrap_response on MONITOR_ALARM must not crash and must return blocks."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        card = Card(Severity.WARN, "Passthrough test", body="• a\n• b")
        _fallback, blocks = wrap_response(card=card, surface=Surface.MONITOR_ALARM, feedback="none")
        self.assertGreater(len(blocks), 0)

    def test_no_section_accessory_buttons(self):
        """Primary CTAs must be in actions blocks, never section.accessory (mobile-first rule 1).
        Renders all Card severity levels and asserts no section contains an accessory button."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        for sev in Severity:
            _, blocks = wrap_response(
                card=Card(sev, "Test headline", body="Test body"),
                surface=Surface.CHANNEL_ROOT,
            )
            for block in blocks:
                acc = block.get("accessory") if isinstance(block, dict) else None
                if isinstance(acc, dict):
                    self.assertNotEqual(
                        acc.get("type"), "button",
                        f"section.accessory button found in {sev} render — use actions block instead",
                    )

    def test_no_fenced_code_in_response(self):
        """Triple-backtick blocks horizontal-scroll on mobile (rule 2).
        wrap_response output must never contain ``` in any mrkdwn text field."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        card = Card(Severity.INFO, "Result", body="Here is the data:\n```sql\nSELECT 1\n```")
        _, blocks = wrap_response(card=card, surface=Surface.CHANNEL_ROOT)
        for block in blocks:
            text_val = ""
            if isinstance(block.get("text"), dict):
                text_val += block["text"].get("text", "")
            for el in block.get("elements", []) or []:
                if isinstance(el, dict):
                    text_val += el.get("text", "") if isinstance(el.get("text"), str) else ""
            # Also check section.fields (facts block) — same mobile rule applies
            for field in block.get("fields", []) or []:
                if isinstance(field, dict):
                    text_val += field.get("text", "")
            self.assertNotIn(
                "```", text_val,
                "Triple-backtick fenced code found in wrap_response output — use inline `code` instead",
            )

    def test_no_danger_style_on_feedback(self):
        """style:danger is for destructive actions only (rule 4).
        Feedback and suggestion buttons must not use danger styling."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        _, blocks = wrap_response(
            card=Card(Severity.WARN, "Something is off"),
            surface=Surface.CHANNEL_ROOT,
            suggestions=["Try this", "Or this"],
            feedback="button",
            query_hash="fake_ts_123",
        )
        for block in blocks:
            for el in block.get("elements", []) or []:
                if isinstance(el, dict) and el.get("type") == "button":
                    self.assertNotEqual(
                        el.get("style"), "danger",
                        f"Feedback/suggestion button has style:danger — remove it: {el}",
                    )

    def test_suggestions_capped_at_max_actions(self):
        """More suggestions than MAX_ACTIONS[surface] must be silently trimmed."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, MAX_ACTIONS, wrap_response
        many = [f"suggestion {i}" for i in range(10)]
        for surface in (Surface.CHANNEL_ROOT, Surface.DM, Surface.THREAD):
            _, blocks = wrap_response(
                card=Card(Severity.INFO, "headline"),
                surface=surface,
                suggestions=many,
            )
            action_blocks = [b for b in blocks if b.get("type") == "actions"]
            total_buttons = sum(len(b.get("elements", [])) for b in action_blocks)
            self.assertLessEqual(
                total_buttons, MAX_ACTIONS[surface],
                f"{surface}: {total_buttons} buttons exceeds MAX_ACTIONS={MAX_ACTIONS[surface]}",
            )

    def test_long_body_preserves_feedback(self):
        """enforce() must not truncate the feedback row when blocks exceed the surface budget.
        Uses Surface.DM (budget=6) and a card with body + facts + card.actions + elapsed +
        suggestions — 7 blocks total — so enforce() actually fires. Feedback is placed before
        suggestions in composition order specifically to survive truncation; this test verifies
        that guarantee holds under real budget pressure."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        # Build a card that produces 7 blocks on Surface.DM (budget=6):
        # header + body + facts + feedback + suggestions + elapsed + card.actions = 7
        card = Card(
            Severity.INFO,
            "Revenue summary",
            body="x" * 200,
            facts=[("Publisher", "Pub 1247"), ("Fill %", "62%")],
            actions=[("View full", "drill_down_view", "val", "")],
        )
        _, blocks = wrap_response(
            card=card,
            surface=Surface.DM,
            feedback="button",
            query_hash="ts_123",
            suggestions=["Follow-up 1", "Follow-up 2"],
            elapsed_seconds=3,
        )
        # Budget is 6 — enforce() must have fired (truncated to 5 + 1 overflow context)
        self.assertLessEqual(len(blocks), 6, "enforce() did not fire — test setup is invalid")
        has_feedback = any(
            b.get("type") == "actions" and any(
                e.get("action_id", "").startswith("scout_feedback")
                for e in b.get("elements", [])
            )
            for b in blocks
        )
        self.assertTrue(
            has_feedback,
            "Feedback row was truncated by enforce() — check composition order in wrap_response",
        )

    def test_empty_suggestions_no_actions_block(self):
        """suggestions=[] must produce zero actions blocks.
        Slack rejects messages with actions blocks containing empty elements arrays."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        for empty in ([], None):
            _, blocks = wrap_response(
                card=Card(Severity.INFO, "headline"),
                surface=Surface.CHANNEL_ROOT,
                suggestions=empty,
                feedback="none",
            )
            action_blocks = [b for b in blocks if b.get("type") == "actions"]
            self.assertEqual(
                action_blocks, [],
                f"suggestions={empty!r} produced actions blocks: {action_blocks}",
            )

    def test_monitor_alarm_warn_gets_native_header_block(self):
        """MONITOR_ALARM + WARN severity must emit a native header block (not mrkdwn section).
        Regression guard: wrap_response previously excluded MONITOR_ALARM from native headers
        even when the surface was explicitly passed."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response, BUDGETS
        _, blocks = wrap_response(
            card=Card(Severity.WARN, "Cap alert", body="• Advertiser A: 85% of cap"),
            surface=Surface.MONITOR_ALARM,
            feedback="none",
        )
        self.assertEqual(blocks[0]["type"], "header", "First block must be native header")
        self.assertEqual(blocks[1]["type"], "divider", "Second block must be divider")
        self.assertLessEqual(
            len(blocks), BUDGETS[Surface.MONITOR_ALARM],
            f"Block count {len(blocks)} exceeds MONITOR_ALARM budget {BUDGETS[Surface.MONITOR_ALARM]}",
        )

    def test_fallback_text_nonempty(self):
        """Every wrap_response return must have a non-empty fallback string.
        Mobile push previews go blank when fallback is empty."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        fixtures = [
            Card(Severity.CRITICAL, "Revenue dropped 40%"),
            Card(Severity.WARN, "Fill rate low", body="Below 60% threshold"),
            Card(Severity.INFO, "Status OK"),
            Card(Severity.POSITIVE, "Revenue up"),
            Card(Severity.INFO, "", body="Body only"),
        ]
        for card in fixtures:
            fallback, _ = wrap_response(card=card, surface=Surface.CHANNEL_ROOT)
            self.assertTrue(
                fallback and fallback.strip(),
                f"Empty fallback for card headline={card.headline!r} body={card.body!r}",
            )


    def test_facts_render_as_rich_text(self):
        """card.facts must render as a rich_text block, not section.fields.
        Verifies _build_facts_blocks is wired into wrap_response."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        card = Card(Severity.INFO, "Publisher rev",
                    facts=[("Pub A", "$42K"), ("Pub B", "$38K"), ("Pub C", "$21K")])
        _f, blocks = wrap_response(card=card, surface=Surface.CHANNEL_ROOT, feedback="none")
        facts_block = next((b for b in blocks if b.get("type") == "rich_text"), None)
        self.assertIsNotNone(facts_block, "Expected a rich_text block for facts")

    def test_facts_overflow_context(self):
        """12 facts: first 10 visible, remaining 2 trigger a context overflow block."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response
        card = Card(Severity.INFO, "All pubs",
                    facts=[(f"Pub {i}", f"${i*1000}") for i in range(12)])
        _f, blocks = wrap_response(card=card, surface=Surface.CHANNEL_ROOT, feedback="none")
        ctx_blocks = [b for b in blocks if b.get("type") == "context"]
        overflow_text = any("2 more" in str(b) for b in ctx_blocks)
        self.assertTrue(overflow_text, "Expected context block mentioning '2 more'")

    def test_facts_budget_is_one_block(self):
        """10 facts on MONITOR_ALARM must occupy exactly 1 rich_text block, not 10 sections.
        This keeps us inside the 6-block budget regardless of fact count."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit",):
            sys.modules.pop(mod, None)
        from scout_ui_kit import Card, Severity, Surface, wrap_response, BUDGETS
        card = Card(Severity.WARN, "Cap alert", facts=[(f"Pub {i}", "12%") for i in range(10)])
        _f, blocks = wrap_response(card=card, surface=Surface.MONITOR_ALARM, feedback="none")
        rich_text_blocks = [b for b in blocks if b.get("type") == "rich_text"]
        self.assertEqual(len(rich_text_blocks), 1,
                         "All 10 facts must collapse into exactly 1 rich_text block")
        self.assertLessEqual(len(blocks), BUDGETS[Surface.MONITOR_ALARM],
                             f"Block count {len(blocks)} exceeds MONITOR_ALARM budget")


    def test_footer_elapsed_under_60s(self):
        """elapsed_seconds < 60 renders as Ns."""
        from scout_ui_kit import Surface, _build_footer_block
        blocks = _build_footer_block(elapsed_seconds=42, surface=Surface.CHANNEL_ROOT)
        self.assertEqual(len(blocks), 1)
        self.assertIn("42s", blocks[0]["elements"][0]["text"])

    def test_footer_elapsed_over_60s(self):
        """elapsed_seconds >= 60 renders as XmYs."""
        from scout_ui_kit import Surface, _build_footer_block
        blocks = _build_footer_block(elapsed_seconds=95, surface=Surface.CHANNEL_ROOT)
        self.assertEqual(len(blocks), 1)
        self.assertIn("1m 35s", blocks[0]["elements"][0]["text"])

    def test_footer_suppressed_on_dm(self):
        """Elapsed-only footer is suppressed on DM surface."""
        from scout_ui_kit import Surface, _build_footer_block
        blocks = _build_footer_block(elapsed_seconds=10, surface=Surface.DM)
        self.assertEqual(blocks, [])

    def test_footer_suppressed_on_ephemeral(self):
        """Elapsed-only footer is suppressed on EPHEMERAL surface."""
        from scout_ui_kit import Surface, _build_footer_block
        blocks = _build_footer_block(elapsed_seconds=10, surface=Surface.EPHEMERAL)
        self.assertEqual(blocks, [])

    def test_footer_interpretation_with_elapsed(self):
        """interpretation + elapsed_seconds combines into one context block."""
        from scout_ui_kit import Surface, _build_footer_block
        blocks = _build_footer_block(
            interpretation="fill rate for Acme", elapsed_seconds=30, surface=Surface.CHANNEL_ROOT
        )
        self.assertEqual(len(blocks), 1)
        text = blocks[0]["elements"][0]["text"]
        self.assertIn("Interpreted as: fill rate for Acme", text)
        self.assertIn("30s", text)

    def test_footer_interpretation_suppresses_elapsed_only_path(self):
        """When interpretation is set, the elapsed-only branch is never reached."""
        from scout_ui_kit import Surface, _build_footer_block
        # interpretation present on DM — should still emit (interpretation path ignores surface)
        blocks = _build_footer_block(interpretation="revenue query", surface=Surface.DM)
        self.assertEqual(len(blocks), 1)
        self.assertIn("Interpreted as: revenue query", blocks[0]["elements"][0]["text"])

    def test_footer_empty_when_no_args(self):
        """No args → empty list."""
        from scout_ui_kit import _build_footer_block
        self.assertEqual(_build_footer_block(), [])

    def test_markdown_block_emitted_when_flag_enabled(self):
        """When SCOUT_MARKDOWN_BLOCKS=true, body uses native markdown block instead of rich_text."""
        import os
        import importlib
        os.environ["SCOUT_MARKDOWN_BLOCKS"] = "true"
        import scout_ui_kit as kit
        importlib.reload(kit)
        card = kit.Card(severity=kit.Severity.INFO, headline="Test", body="## Heading\nSome text.")
        _, blocks = kit.wrap_response(card=card, surface=kit.Surface.CHANNEL_ROOT, pattern=kit.ResponsePattern.ANSWER)
        md_blocks = [b for b in blocks if b.get("type") == "markdown"]
        self.assertEqual(len(md_blocks), 1)
        self.assertIn("## Heading", md_blocks[0]["text"])
        os.environ.pop("SCOUT_MARKDOWN_BLOCKS", None)
        importlib.reload(kit)


if __name__ == "__main__":
    unittest.main()
