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

────────────────────────────────────────────────────────────────────────────
MOBILE-FIRST RULES (every Scout Slack surface)
────────────────────────────────────────────────────────────────────────────
Sidd reads Scout on iPhone as often as desktop. Every renderer in
scout_slack_ui.py must follow these — enforced by tests/test_kit_lint.py:

1. **Primary CTAs go in `actions` blocks, never `section.accessory`.**
   Accessory buttons clip on narrow iOS widths and get tap-eaten by the
   surrounding section. Dedicated `actions` blocks render reliably.

2. **No fenced code blocks (```) in user-facing copy.**
   Triple-backtick blocks horizontal-scroll on mobile. Use inline `code`
   spans for short snippets; for longer payloads use a `section` with
   wrapped mrkdwn text.

3. **`action_id` must be unique within a single view/message.**
   Slack mobile silently drops clicks when ids repeat. Namespace ids by
   purpose + index (e.g. `home_try_query_hero`, `home_try_query_0`).

4. **`style: "danger"` only for destructive actions.**
   👎 is feedback, not destruction. Pink-styled feedback buttons visually
   dominate the response. Use unstyled buttons for non-destructive UX.

5. **Stay under the Surface budget.**
   See BUDGETS dict below. `enforce(blocks, surface)` truncates and adds
   a human-readable overflow line. Use it on paging surfaces (monitor
   alarms, channel root) — never let a noisy day blow past 100 blocks.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Optional

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
    actions:   Optional list of (label, action_id, value, style) button tuples
               action_id: Slack action_id for block_action routing
               style: "primary" | "danger" | "" (default/unstyled)

    IMPORTANT: actions uses field(default_factory=list) — never pass actions=[]
    as a default argument in a caller function (mutable default bug).
    """

    severity: Severity
    headline: str
    body: str = ""
    facts: list[tuple[str, str]] = field(default_factory=list)
    actions: list[tuple[str, str, str, str]] = field(default_factory=list)

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
                "text": {"type": "mrkdwn", "text": _escape_md_code(self.body)},
            })

        # Facts as section.fields (max 10 fields — Slack limit)
        if self.facts:
            fields = []
            for label, value in self.facts[:10]:
                fields.append({"type": "mrkdwn", "text": f"*{label}*\n{_escape_md_code(str(value))}"})
            blocks.append({"type": "section", "fields": fields})

        # Actions row (optional)
        if self.actions:
            elements = []
            for label, action_id, value, style in self.actions[:25]:  # Slack Block Kit limit: 25
                btn: dict = {
                    "type": "button",
                    "text": {"type": "plain_text", "text": label},
                    "action_id": action_id,
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
    shown = limit - 1
    total = len(blocks)

    if thread_ts:
        overflow_text = f"Showing {shown} of {total} items. Rest is in the thread above."
    else:
        overflow_text = f"Too long for this view ({total} items). Narrow the query or ask in a thread."

    truncated.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": overflow_text}],
    })
    return truncated


# ---------------------------------------------------------------------------
# MAX_ACTIONS — per-surface button budget (mobile-first defaults)
# ---------------------------------------------------------------------------
MAX_ACTIONS: dict[Surface, int] = {
    Surface.CHANNEL_ROOT: 2,
    Surface.THREAD: 3,
    Surface.DM: 2,
    Surface.MONITOR_ALARM: 0,
    Surface.HOME: 2,
    Surface.EPHEMERAL: 1,
}


# ---------------------------------------------------------------------------
# _escape_md_code — strip fenced code blocks (mobile horizontal-scroll) and
#                   protect underscores inside backtick spans from italic.
# ---------------------------------------------------------------------------
_CODE_SPAN_RE = re.compile(r"`([^`]+)`")
_FENCED_BLOCK_RE = re.compile(r"```[a-z]*\n?(.*?)```", re.DOTALL)


def _escape_md_code(text: str) -> str:
    """Convert fenced code blocks to inline code and escape underscores in spans.

    1. Triple-backtick fenced blocks cause horizontal scroll on mobile (Slack rule 2).
       They are collapsed to a single inline ``code`` span containing just the first
       non-blank line of the block, so context is preserved without the scroll trap.

    2. Slack's mrkdwn parser treats _word_ as italic even inside inline code spans.
       Underscores inside backtick spans are escaped so that ``cap_alert_pct``
       renders as literal text rather than ``cap<em>alert</em>pct``.
    """
    # Step 1: replace fenced blocks with inline code (first non-blank content line)
    def _collapse_fenced(m: re.Match) -> str:
        inner = m.group(1).strip()
        first_line = next((ln for ln in inner.splitlines() if ln.strip()), inner)
        return f"`{first_line.strip()}`"

    text = _FENCED_BLOCK_RE.sub(_collapse_fenced, text)

    # Step 2: escape underscores inside remaining inline code spans
    def _escape_underscores(m: re.Match) -> str:
        return "`" + m.group(1).replace("_", r"\_") + "`"

    return _CODE_SPAN_RE.sub(_escape_underscores, text)


# ---------------------------------------------------------------------------
# wrap_response — single mobile-tuned chokepoint for all ask() exits
# ---------------------------------------------------------------------------
def wrap_response(
    *,
    card: "Card",
    surface: Surface,
    suggestions: Optional[list[str]] = None,
    feedback: Literal["reaction", "button", "none"] = "reaction",
    query_hash: Optional[str] = None,
    elapsed_seconds: Optional[int] = None,
) -> tuple[str, list[dict]]:
    """Single entry-point for every ask() reply surface.

    Composition order (earlier items are protected from enforce() truncation):
        headline → body → feedback → suggestions → footer → enforce()

    Args:
        card:             Card to render (severity + headline + optional body/facts/actions).
        surface:          Target Slack surface — drives budget and button caps.
        suggestions:      Follow-up query strings. Capped at MAX_ACTIONS[surface].
                          Pass [] or None to emit zero actions blocks.
        feedback:         "reaction" — no button row, caller should seed 👎 reaction.
                          "button"   — include 👎 Off + ✏️ Correct this actions block.
                                       NOTE: requires query_hash; silently omitted if None.
                          "none"     — omit feedback entirely.
        query_hash:       Message ts / hash used as button value for feedback routing.
                          Required when feedback="button"; pass None to suppress buttons.
        elapsed_seconds:  If provided, appended as a context footer (ops surfaces only;
                          omit on DM to keep output clean).

    Returns:
        (fallback_text, blocks) — fallback is always non-empty (mobile push previews).
    """
    suggestions = suggestions or []
    max_btn = MAX_ACTIONS.get(surface, 2)

    # 1. Headline + body from Card
    blocks: list[dict] = []
    # Skip the header section when headline is empty so body-only cards don't
    # render as "🔵 **" (empty bold) — callers pass headline="" for plain-text
    # responses where the full answer goes into body.
    if card.headline:
        header_text = f"{card.severity.emoji} *{card.headline}*"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": header_text}})

    if card.body:
        body_text = _escape_md_code(card.body)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": body_text}})

    if card.facts:
        fields = [
            {"type": "mrkdwn", "text": f"*{lbl}*\n{_escape_md_code(str(val))}"}
            for lbl, val in card.facts[:10]
        ]
        blocks.append({"type": "section", "fields": fields})

    # 2. Feedback row (protected — placed before suggestions so enforce() keeps it)
    if feedback == "button" and query_hash:
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👎 Off", "emoji": True},
                    "action_id": "scout_feedback_bad",
                    "value": query_hash,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✏️ Correct this", "emoji": True},
                    "action_id": "scout_feedback_correct",
                    "value": query_hash,
                },
            ],
        })

    # 3. Suggestion buttons — capped at MAX_ACTIONS[surface]; omit block entirely if empty
    capped = [s for s in suggestions[:max_btn] if isinstance(s, str) and s.strip()]
    if capped:
        def _fit(s: str, max_len: int = 25) -> str:
            if len(s) <= max_len:
                return s
            cut = s[:max_len].rsplit(" ", 1)[0]
            return cut if cut else s[:max_len]

        elements = [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": _fit(s), "emoji": False},
                "value": s,
                "action_id": f"scout_suggestion_{i}",
            }
            for i, s in enumerate(capped)
        ]
        blocks.append({"type": "actions", "elements": elements})

    # 4. Elapsed footer (ops surfaces; skip on DM)
    if elapsed_seconds is not None and surface not in (Surface.DM, Surface.EPHEMERAL):
        elapsed_str = (
            f"{elapsed_seconds}s" if elapsed_seconds < 60
            else f"{elapsed_seconds // 60}m {elapsed_seconds % 60}s"
        )
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_Scout · {elapsed_str}_"}],
        })

    # 5. Card-level extra actions (e.g. drill-down CTAs from Card.actions)
    # Intentionally not subject to MAX_ACTIONS: these are specific CTAs the caller
    # attached to the Card (e.g. "View in ClickHouse"), not open-ended suggestions.
    # Budget enforcement via enforce() is the final backstop.
    if card.actions:
        elements = []
        for label, action_id, value, style in card.actions[:25]:
            btn: dict = {
                "type": "button",
                "text": {"type": "plain_text", "text": label},
                "action_id": action_id,
                "value": value,
            }
            if style in ("primary", "danger"):
                btn["style"] = style
            elements.append(btn)
        blocks.append({"type": "actions", "elements": elements})

    # 6. Budget enforcement — always last
    blocks = enforce(blocks, surface)

    # Fallback text for push previews — always non-empty; strip markdown for clean preview
    _raw_fallback = card.headline or card.body or f"{card.severity.emoji} Scout update"
    fallback = _raw_fallback[:200].strip() or f"{card.severity.emoji} Scout update"

    return fallback, blocks
