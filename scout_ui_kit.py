"""
scout_ui_kit — shared Block Kit rendering primitives.

Pure module: no Slack API calls, no ClickHouse calls, no file I/O.
Data-in, blocks-out. All primitives are @dataclass.

Kill switch: set SCOUT_KIT_ENABLED=false in Render env vars to fall back
to legacy scout_slack_ui.py paths. Import _KIT_ENABLED from here — never
read the env var directly in other modules.

Import DAG: scout_ui_kit imports stdlib only. Nothing else imports from it
before scout_slack_ui.py. Do NOT import from scout_handlers, scout_bot, or
scout_slack_ui — circular import.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

# ---------------------------------------------------------------------------
# Kill switch — import from here, never re-read the env var elsewhere
# ---------------------------------------------------------------------------
_KIT_ENABLED: bool = os.getenv("SCOUT_KIT_ENABLED", "true").lower() == "true"


# ---------------------------------------------------------------------------
# Severity vocabulary — 4 levels, one source of truth
# ---------------------------------------------------------------------------
class Severity(Enum):
    CRITICAL = ("🔴", "critical")  # Revenue burning right now
    WARN = ("🟠", "warn")          # Action needed within 24h
    INFO = ("🔵", "info")          # No action, just FYI
    POSITIVE = ("🟢", "ok")        # Recovery / momentum

    @property
    def emoji(self) -> str:
        return self.value[0]

    @property
    def label(self) -> str:
        return self.value[1]


# ---------------------------------------------------------------------------
# Surface enum + budgets
# ---------------------------------------------------------------------------
class Surface(Enum):
    CHANNEL_ROOT = "channel_root"
    THREAD = "thread"
    DM = "dm"
    MONITOR_ALARM = "monitor_alarm"
    HOME = "home"
    EPHEMERAL = "ephemeral"


BUDGETS: dict[Surface, int] = {
    Surface.CHANNEL_ROOT: 8,
    Surface.THREAD: 50,
    Surface.DM: 6,
    Surface.MONITOR_ALARM: 6,
    Surface.HOME: 30,
    Surface.EPHEMERAL: 6,
}


# ---------------------------------------------------------------------------
# ts() — Slack-native relative time helper
# ---------------------------------------------------------------------------
def ts(unix_seconds: int, fallback: str = "") -> str:
    """
    Return a <!date^TS^format> string that Slack renders as timezone-aware
    relative time. Falls back to `fallback` if unix_seconds is falsy.

    Usage in mrkdwn: f"Last seen {ts(timestamp, '?')}"
    """
    if not unix_seconds:
        return fallback or "unknown"
    return f"<!date^{int(unix_seconds)}^{{date_short_pretty}} at {{time}}|{fallback}>"


# ---------------------------------------------------------------------------
# Card — canonical rendering unit
# ---------------------------------------------------------------------------
@dataclass
class Card:
    """
    Canonical rendering unit. Renders to a list of Block Kit blocks.

    severity:  Severity enum value
    headline:  Short title (≤150 chars, no mrkdwn needed — rendered bold)
    body:      Optional main body text (mrkdwn supported)
    facts:     Optional list of (label, value) pairs rendered as section.fields
    actions:   Optional list of (label, value, style) button tuples
               style: "primary" | "danger" | "" (default/unstyled)

    IMPORTANT: actions uses field(default_factory=list) — never pass actions=[]
    as a default argument in a caller function (mutable default bug).
    """

    severity: Severity
    headline: str
    body: str = ""
    facts: list[tuple[str, str]] = field(default_factory=list)
    actions: list[tuple[str, str, str]] = field(default_factory=list)

    def render(self, surface: Surface) -> list[dict]:
        """Render to Block Kit blocks. Caller must pass through enforce() before send."""
        blocks: list[dict] = []

        # Header section: severity emoji + bold headline
        header_text = f"{self.severity.emoji} *{self.headline}*"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": header_text},
        })

        # Body section (optional)
        if self.body:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": self.body},
            })

        # Facts as section.fields (max 10 fields — Slack limit)
        if self.facts:
            fields = []
            for label, value in self.facts[:10]:
                fields.append({"type": "mrkdwn", "text": f"*{label}*\n{value}"})
            blocks.append({"type": "section", "fields": fields})

        # Actions row (optional)
        if self.actions:
            elements = []
            for label, value, style in self.actions[:5]:  # Slack max 5 per actions block
                btn: dict = {
                    "type": "button",
                    "text": {"type": "plain_text", "text": label},
                    "value": value,
                }
                if style in ("primary", "danger"):
                    btn["style"] = style
                elements.append(btn)
            blocks.append({"type": "actions", "elements": elements})

        return blocks


# ---------------------------------------------------------------------------
# enforce() — hard block budget enforcement
# ---------------------------------------------------------------------------
def enforce(
    blocks: list[dict],
    surface: Surface,
    thread_ts: Optional[str] = None,
) -> list[dict]:
    """
    Enforce block budget for the target surface. Never silently truncates.

    When over budget:
    - If thread_ts provided: truncate and append a "View full →" context block
      pointing to the thread.
    - If thread_ts is None (e.g. fresh monitor alarm): truncate and append a
      "Results too large" context block. Caller should post as normal alarm.

    Returns a list guaranteed to be within BUDGETS[surface].
    """
    limit = BUDGETS.get(surface, 8)
    if len(blocks) <= limit:
        return blocks

    # Reserve 1 slot for the overflow indicator
    truncated = blocks[: limit - 1]

    if thread_ts:
        overflow_text = f"View full results in <slack://channel?thread_ts={thread_ts}|thread>"
    else:
        overflow_text = "Response too large for this surface. Narrow the query or ask in a thread."

    truncated.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": overflow_text}],
    })
    return truncated
