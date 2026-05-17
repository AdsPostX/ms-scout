"""
Lint tests for scout_ui_kit migration invariants.

Enforces that block builders do not contain legacy severity string literals
("WARNING:" / "CRITICAL:") that the kit Severity enum replaced.
Fails if a contributor bypasses the enum and hard-codes the old strings.
"""
import pathlib
import unittest

_REPO = pathlib.Path(__file__).parent.parent
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


if __name__ == "__main__":
    unittest.main()
