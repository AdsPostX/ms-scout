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

    def test_handlers_no_direct_builder_calls(self):
        """scout_handlers.py must not call _build_feedback_buttons or _build_suggestion_buttons
        in active (kit-enabled) code paths. Uses AST walk with if/else awareness so that
        calls inside `else:` branches of `if _KIT_ENABLED:` guards are treated as
        intentional legacy rollback paths and are not flagged.

        Calls outside any _KIT_ENABLED guard are violations.
        """
        import ast

        handlers_path = _REPO / "scout_handlers.py"
        if not handlers_path.exists():
            self.skipTest("scout_handlers.py not found")

        tree = ast.parse(handlers_path.read_text())
        banned = {"_build_feedback_buttons", "_build_suggestion_buttons"}

        def _is_kit_enabled_test(node: ast.expr) -> bool:
            """Return True if node is `_KIT_ENABLED` (Name) or `not _KIT_ENABLED`."""
            if isinstance(node, ast.Name) and node.id == "_KIT_ENABLED":
                return True
            if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
                return _is_kit_enabled_test(node.operand)
            return False

        def _collect_legacy_nodes(stmts) -> set[int]:
            """Collect line numbers of all AST nodes in legacy (else) branches."""
            legacy: set[int] = set()
            for stmt in stmts:
                for n in ast.walk(stmt):
                    legacy.add(id(n))
            return legacy

        # Collect all node ids that live inside legacy else: branches
        legacy_node_ids: set[int] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.If) and _is_kit_enabled_test(node.test):
                # orelse is the legacy fallback path — exclude it from violation checks
                legacy_node_ids |= _collect_legacy_nodes(node.orelse)
            elif isinstance(node, ast.If):
                # Also handle `if not _KIT_ENABLED:` → body is legacy, orelse is active
                if (isinstance(node.test, ast.UnaryOp)
                        and isinstance(node.test.op, ast.Not)
                        and _is_kit_enabled_test(node.test.operand)):
                    legacy_node_ids |= _collect_legacy_nodes(node.body)

        violations = []
        for node in ast.walk(tree):
            if id(node) in legacy_node_ids:
                continue
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in banned:
                    violations.append(f"line {node.lineno}: direct call to {node.func.id}()")
                elif isinstance(node.func, ast.Attribute) and node.func.attr in banned:
                    violations.append(f"line {node.lineno}: direct call to {node.func.attr}()")

        self.assertFalse(
            violations,
            "Direct calls to legacy builder functions found outside _KIT_ENABLED guard "
            "— route through wrap_response:\n" + "\n".join(violations),
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


if __name__ == "__main__":
    unittest.main()
