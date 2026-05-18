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
    _REPO / "scout_slack_ui.py",
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
        """Monitor alerts are a paging surface. When the kit is on, output
        MUST be capped at BUDGETS[MONITOR_ALARM] so we never overflow Slack
        on a noisy day. Regression guard for the PR-2 budget wrapping."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        # Force re-import so _KIT_ENABLED picks up the env var
        for mod in ("scout_ui_kit", "scout_slack_ui"):
            sys.modules.pop(mod, None)
        from scout_ui_kit import BUDGETS, Surface
        from scout_slack_ui import _build_monitor_alert_blocks

        # 50 items would otherwise render 50+ blocks; we cap at 8 bullets per
        # section, but the surrounding header+context+section still need budget.
        many_items = [f"item {i}" for i in range(50)]
        _fallback, blocks = _build_monitor_alert_blocks(
            ":warning:", "Stress test", many_items, "test cta"
        )
        cap = BUDGETS[Surface.MONITOR_ALARM]
        self.assertLessEqual(
            len(blocks), cap,
            f"_build_monitor_alert_blocks returned {len(blocks)} blocks; "
            f"MONITOR_ALARM budget is {cap}. enforce() not wired correctly.",
        )

    def test_home_view_action_ids_are_unique(self):
        """Slack mobile silently drops clicks when action_ids repeat in a
        single view. Regression guard for the 5-button Home that all shared
        action_id='home_try_query' and broke on iOS."""
        os.environ["SCOUT_KIT_ENABLED"] = "true"
        for mod in ("scout_ui_kit", "scout_slack_ui"):
            sys.modules.pop(mod, None)
        from scout_slack_ui import _build_home_view

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

    def test_monitor_alert_passthrough_when_kit_disabled(self):
        """Kill switch sanity: SCOUT_KIT_ENABLED=false must not crash and
        must return blocks (no enforcement, legacy behavior)."""
        os.environ["SCOUT_KIT_ENABLED"] = "false"
        for mod in ("scout_ui_kit", "scout_slack_ui"):
            sys.modules.pop(mod, None)
        from scout_slack_ui import _build_monitor_alert_blocks

        _fallback, blocks = _build_monitor_alert_blocks(
            ":warning:", "Kill-switch test", ["a", "b"], ""
        )
        self.assertGreater(len(blocks), 0)
        # Restore default
        os.environ["SCOUT_KIT_ENABLED"] = "true"


if __name__ == "__main__":
    unittest.main()
