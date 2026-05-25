"""
scout_ui_kit — shared Block Kit rendering primitives + all Slack Block Kit builders.

Pure module: no Slack API calls, no ClickHouse calls, no file I/O.
Data-in, blocks-out.

Import DAG: scout_ui_kit imports stdlib only. Do NOT import from scout_handlers,
scout_bot, scout_agent, scout_state, or scout_notion — circular import.

────────────────────────────────────────────────────────────────────────────
MOBILE-FIRST RULES (every Scout Slack surface)
────────────────────────────────────────────────────────────────────────────
Sidd reads Scout on iPhone as often as desktop. Every renderer must follow these
— enforced by tests/test_kit_lint.py:

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

import json
import logging
import os
import pathlib
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
# ResponsePattern enum — canonical Slack response patterns
# ---------------------------------------------------------------------------
class ResponsePattern(str, Enum):
    """Canonical Slack response patterns for Scout.

    Pass as ``pattern=ResponsePattern.ALERT`` to ``wrap_response()`` to opt-in to
    surface validation. Omitting ``pattern`` (existing callers) gets current behaviour.

    Valid surface pairs are enforced at call time via ValueError.
    """
    ALERT   = "alert"    # monitor alarm  → Surface.MONITOR_ALARM, Severity.WARN/CRITICAL
    ANSWER  = "answer"   # ask() reply    → Surface.CHANNEL_ROOT / THREAD / DM, Severity.INFO
    STATUS  = "status"   # health check   → Surface.CHANNEL_ROOT / THREAD / DM, Severity.INFO/WARN
    CONFIRM = "confirm"  # action ack     → Surface.EPHEMERAL, Severity.POSITIVE, 0 buttons
    EMPTY   = "empty"    # no data        → same as ANSWER
    ERROR   = "error"    # CH failure     → Surface.EPHEMERAL, Severity.CRITICAL


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
    MODAL = "modal"


BUDGETS: dict[Surface, int] = {
    Surface.CHANNEL_ROOT: 8,
    Surface.THREAD: 50,
    Surface.DM: 6,
    Surface.MONITOR_ALARM: 6,
    Surface.HOME: 30,
    Surface.EPHEMERAL: 6,
    Surface.MODAL: 45,
}

_PATTERN_VALID_SURFACES: dict[ResponsePattern, set[Surface]] = {
    ResponsePattern.ALERT:   {Surface.MONITOR_ALARM},
    ResponsePattern.ANSWER:  {Surface.CHANNEL_ROOT, Surface.THREAD, Surface.DM},
    ResponsePattern.STATUS:  {Surface.CHANNEL_ROOT, Surface.THREAD, Surface.DM},
    ResponsePattern.CONFIRM: {Surface.EPHEMERAL},
    ResponsePattern.EMPTY:   {Surface.CHANNEL_ROOT, Surface.THREAD, Surface.DM},
    ResponsePattern.ERROR:   {Surface.EPHEMERAL},
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
    Surface.MODAL: 0,
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
    pattern: "ResponsePattern | None" = None,
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
        pattern:          Optional ResponsePattern for surface validation. Raises ValueError
                          if the surface is incompatible with the pattern. Existing callers
                          that omit pattern= are unaffected.

    Returns:
        (fallback_text, blocks) — fallback is always non-empty (mobile push previews).
    """
    if pattern is not None:
        if not isinstance(pattern, ResponsePattern):
            raise TypeError(
                f"pattern must be a ResponsePattern enum value, got {type(pattern).__name__!r}"
            )
        valid = _PATTERN_VALID_SURFACES[pattern]
        if surface not in valid:
            raise ValueError(
                f"ResponsePattern.{pattern.value} requires surface in "
                f"{sorted(s.value for s in valid)}, got Surface.{surface.value}"
            )
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


# ---------------------------------------------------------------------------
# context_block() / divider_block() — lightweight block helpers
# ---------------------------------------------------------------------------
def context_block(
    queried_at: str | None = None,
    period: str | None = None,
    latency_ms: int | None = None,
) -> dict:
    """Return a Slack context block with query metadata.

    Typical usage — append after wrap_response blocks::

        _, blocks = wrap_response(card=card, surface=Surface.CHANNEL_ROOT, feedback="none")
        meta = context_block(queried_at="just now", period="7d")
        web.chat_postMessage(channel=channel, text="...", blocks=[*blocks, meta])

    All params are optional — omit any you don't have.
    """
    parts: list[str] = []
    if queried_at:
        parts.append(f"queried {queried_at}")
    if period:
        parts.append(f"{period} lookback")
    if latency_ms is not None:
        parts.append(f"{latency_ms}ms")
    text = " · ".join(parts) if parts else "Scout"
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": f"_{text}_"}]}


def divider_block() -> dict:
    """Return a Slack divider block."""
    return {"type": "divider"}


# =============================================================================
# Block Kit renderers — migrated from scout_slack_ui.py (legacy file deleted)
# All functions below are pure: data-in, blocks-out, no I/O.
# =============================================================================

log = logging.getLogger("scout_ui_kit")


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------
def _load_ui_thresholds() -> dict:
    try:
        p = pathlib.Path(__file__).parent / "config" / "scout_thresholds.json"
        return json.loads(p.read_text()) if p.exists() else {}
    except Exception:
        return {}


SCOUT_THRESHOLDS: dict = _load_ui_thresholds()


# ---------------------------------------------------------------------------
# App Home content constants
# ---------------------------------------------------------------------------
_HOME_HERO = {
    "jtbd":        "Prep for a publisher call",
    "description": "Publisher health, provisioned offers, and what to pitch.",
    "query":       "Give me a health check on AT&T",
    "cta":         "Health check on AT&T",
}

_HOME_SECONDARY = [
    {"jtbd": "Morning triage",            "query": "What happened today?"},
    {"jtbd": "Understand a revenue drop", "query": "What happened to Pinger this week?"},
    {"jtbd": "Top publishers today",      "query": "Who are the top publishers by revenue today?"},
    {"jtbd": "Find better payouts",
     "query": "Find Capital One Shopping on other networks — is there a better payout?"},
]


# ---------------------------------------------------------------------------
# Help / capability detection
# ---------------------------------------------------------------------------
_HELP_TRIGGERS = {
    "help", "commands", "capabilities", "what can you do", "how do you work",
    "what do you know", "what do you do", "?", "who are you", "teach me",
    "show me what you can do", "options",
}

_EMOJI_ALIASES: dict[str, str] = {
    "yellow_circle": "large_yellow_circle",
}

# Tokenizer for inline elements within a single text line.
_INLINE_RE = re.compile(
    r'\*\*(?P<bold_d>[^*]+?)\*\*'
    r'|\*(?P<bold_s>[^*\n]+?)\*'
    r'|_(?P<italic>[^_\n]+?)_'
    r'|`(?P<code>[^`\n]+?)`'
    r'|:(?P<emoji>[a-z0-9_\-+]+?):'
    r'|<(?P<url>[^|>]+)\|(?P<url_text>[^>]*)>'
    r'|<@(?P<user>[A-Z0-9]+)>'
    r'|(?P<plain>[^*_`:<\n]+|\n|[*_`:<])'
)

# Pipe table fallback: requires ≥2 columns to avoid false-positives on single-pipe lines.
_TABLE_ROW_RE = re.compile(r'^\|(.+\|){2,}\s*$')
_TABLE_SEP_RE = re.compile(r'^\|[-:\s|]+\|?\s*$')

_SOLO_HEADER_RE = re.compile(r'^\*[^*]{15,}\*\s*')


# ---------------------------------------------------------------------------
# Pitch readiness helper
# ---------------------------------------------------------------------------
def _pitch_signal(score: float) -> str:
    """Return pitch-readiness emoji + label based on Scout RPM score."""
    if score >= 2.00:
        return "✅ Pitch-ready"
    if score > 0:
        return "⚠️ Low signal"
    return "🔍 Rate TBD"


# ---------------------------------------------------------------------------
# Alert block (severity-labelled section)
# ---------------------------------------------------------------------------
def _build_alert_block(severity: str, title: str, body: str = "") -> list[dict]:
    """
    Build an Alert block with severity levels for visual hierarchy.

    severity: "danger" | "warning" | "info"
    Returns list of blocks for consistent stacking.
    """
    _KIT_MAP = {
        "danger": Severity.CRITICAL,
        "warning": Severity.WARN,
        "info": Severity.INFO,
    }
    kit_sev = _KIT_MAP.get(severity, Severity.INFO)
    return Card(severity=kit_sev, headline=title, body=body).render(Surface.EPHEMERAL)


# ---------------------------------------------------------------------------
# Card with hero image
# ---------------------------------------------------------------------------
def _build_card_with_image(
    title: str,
    subtitle: str,
    hero_url: str = "",
    body: str = "",
    buttons: list[dict] = None,
    fields: list[dict] = None,
) -> list[dict]:
    """Build a visual card with hero image, title, subtitle, body text, and action buttons."""
    blocks = []

    header_text = f"*{title}*"
    if subtitle:
        header_text += f"\n_{subtitle}_"

    section: dict = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": header_text},
    }

    if hero_url and hero_url.startswith("http"):
        section["accessory"] = {
            "type": "image",
            "image_url": hero_url,
            "alt_text": title,
        }

    blocks.append(section)

    if body:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": body},
        })

    if fields:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ""},
            "fields": [
                {"type": "mrkdwn", "text": f"*{f['label']}*\n{f['value']}"}
                for f in fields if f.get("label") and f.get("value")
            ],
        })

    if buttons:
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": btn.get("text", "Action"), "emoji": True},
                    "style": btn.get("style", "primary"),
                    "action_id": btn.get("action_id", "action"),
                    "value": btn.get("value", ""),
                }
                for btn in buttons
            ],
        })

    return blocks


# ---------------------------------------------------------------------------
# Rich text list
# ---------------------------------------------------------------------------
def _build_rich_text_list(items: list[str], ordered: bool = False, indent: int = 0) -> list[dict]:
    """Build a native rich_text_list block."""
    if not items:
        return []

    return [{
        "type": "rich_text",
        "elements": [
            {
                "type": "rich_text_list",
                "style": "ordered" if ordered else "bullet",
                "indent": indent,
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [{"type": "text", "text": item}]
                    }
                    for item in items
                ],
            }
        ],
    }]


# ---------------------------------------------------------------------------
# Queue confirm blocks
# ---------------------------------------------------------------------------
def _queue_confirm_blocks(
    advertiser: str,
    network: str,
    payout_display: str,
    user_id: str,
    score: float,
    notion_url: "str | None",
) -> list[dict]:
    """Block Kit card for queue confirmation — enhanced visual treatment."""
    signal    = _pitch_signal(score)
    score_str = f"${score:.2f} RPM" if score else "Rate TBD"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"✅ {advertiser} Queued"},
        },
    ]

    section_text = f"{network} · {payout_display}"
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": section_text},
    })

    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"Added by <@{user_id}>"},
            {"type": "mrkdwn", "text": score_str},
            {"type": "mrkdwn", "text": signal},
        ],
    })

    if notion_url:
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "View Brief →", "emoji": True},
                "url": notion_url,
                "action_id": "queue_view_brief",
            }],
        })

    return blocks


# ---------------------------------------------------------------------------
# Advertiser RPM context
# ---------------------------------------------------------------------------
def _build_advertiser_rpm_context_blocks(ctx: dict, scout_estimate: float = 0) -> list[dict]:
    """Return a context block showing the advertiser's 30-day platform RPM history."""
    if not ctx.get("has_history"):
        return []

    active    = ctx["active_campaigns"]
    imps      = ctx["impressions_30d"]
    rev       = ctx["revenue_30d"]
    rpm_min   = ctx["rpm_min"]
    rpm_max   = ctx["rpm_max"]
    rpm_avg   = ctx["rpm_avg"]

    campaign_str = f"{active} active campaign{'s' if active != 1 else ''}"
    imps_str     = f"{imps / 1_000_000:.1f}M" if imps >= 1_000_000 else f"{imps / 1000:.0f}K"
    rev_str      = f"${rev / 1000:.0f}K" if rev >= 1000 else f"${rev:.0f}"

    if rpm_min == rpm_max or active == 1:
        rpm_str = f"${rpm_avg:.0f} platform RPM"
    else:
        rpm_str = f"${rpm_min:.0f}–${rpm_max:.0f} platform RPM range"

    estimate_str = f"Scout estimate: ${scout_estimate:.0f} RPM" if scout_estimate else ""

    parts = [f"{campaign_str} · {imps_str} impressions", f"{rev_str} revenue · {rpm_str}"]
    if estimate_str:
        parts.append(estimate_str)

    return [
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f":bar_chart:  *{' · '.join(parts[:2])}*"},
            ] + ([{"type": "mrkdwn", "text": estimate_str}] if estimate_str else []),
        }
    ]


# ---------------------------------------------------------------------------
# Campaign brief blocks
# ---------------------------------------------------------------------------
def _build_brief_blocks(brief_data: dict, copy: dict, thread_ts: str = "") -> list:  # noqa: ARG001
    """Build a Slack Block Kit message for a campaign brief."""
    advertiser   = brief_data.get("advertiser", "Offer")
    network      = brief_data.get("network", "").title()
    payout       = brief_data.get("payout", "Rate TBD")
    geo          = brief_data.get("geo", "")
    tracking_url = brief_data.get("tracking_url", "")
    offer_id     = brief_data.get("offer_id", "")
    performance  = brief_data.get("performance_context", "")
    hero_url     = brief_data.get("hero_url", "")
    icon_url     = brief_data.get("icon_url", "")
    ms_status    = brief_data.get("ms_status", "")
    score_rpm    = brief_data.get("scout_score_rpm", 0)
    portal_url   = brief_data.get("portal_url", "")
    risk_flag    = brief_data.get("risk_flag", "")
    restrictions = brief_data.get("restrictions", "")

    titles       = copy.get("titles", [])
    ctas         = copy.get("ctas", [])
    title        = copy.get("title", "") or (titles[0] if titles else "")
    title_backup = copy.get("title_backup", "") or (titles[1] if len(titles) > 1 else "")
    description  = copy.get("description", "")
    short_desc   = copy.get("short_desc", "")
    cta          = copy.get("cta") or (ctas[0] if ctas else None)
    targeting    = copy.get("targeting", "")
    bottom       = copy.get("bottom_line", "")

    blocks = []

    status_tag = {"Not in System": " · New", "Live": " · Already Live", "In System": " · In System"}.get(ms_status, "")
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"Campaign Brief — {advertiser}{status_tag}", "emoji": False},
    })

    _HIGH_FRICTION_TAGS = ("B2B intent", "Loan/credit", "Medical program", "Biz-opp", "Insurance")
    is_high_friction = any(tag in (risk_flag or "") for tag in _HIGH_FRICTION_TAGS)

    if not score_rpm and is_high_friction:
        rpm_display = "Not estimated\n_conversion complexity too high_"
    elif not score_rpm:
        rpm_display = "N/A\n_no MS data at any tier_"
    elif performance and "Real MS data" in performance:
        rpm_display = f"${score_rpm:,.0f}"
    elif performance and "advertiser benchmark" in performance:
        rpm_display = f"~${score_rpm:,.0f} est."
    elif performance and "benchmark" in performance:
        rpm_display = f"~${score_rpm:,.0f} est."
    else:
        rpm_display = f"~${score_rpm:,.0f} est.\n_broad avg_"

    stat_fields = [
        {"type": "mrkdwn", "text": f"*Network*\n{network}"},
        {"type": "mrkdwn", "text": f"*Payout*\n{payout}"},
        {"type": "mrkdwn", "text": f"*Geo*\n{geo or 'Not specified'}"},
        {"type": "mrkdwn", "text": f"*Est. RPM*\n{rpm_display}"},
    ]
    stats_block: dict = {"type": "section", "fields": stat_fields}
    if icon_url and icon_url.startswith("http"):
        stats_block["accessory"] = {
            "type": "image",
            "image_url": icon_url,
            "alt_text": advertiser,
        }
    blocks.append(stats_block)

    if risk_flag:
        blocks.extend(_build_alert_block("warning", f"Fit note: {risk_flag}", ""))

    blocks.append({"type": "divider"})

    _PROHIBITED_CHARS = ("—", "–", "™", "®")

    def _copy_qa(text: str, max_len: int) -> str:
        length = len(text)
        has_prohibited = any(c in text for c in _PROHIBITED_CHARS)
        if has_prohibited:
            flagged = [c for c in _PROHIBITED_CHARS if c in text]
            return f"⚠ prohibited chars: {', '.join(repr(c) for c in flagged)}"
        if length > max_len:
            return f"⚠ {length} chars (max {max_len})"
        return f"✓ {length} chars"

    if title:
        title_qa  = _copy_qa(title, 58)
        title_text = f"*Headline:* {title}  _{title_qa}_"
        if title_backup:
            backup_qa = _copy_qa(title_backup, 58)
            title_text += f"\n_A/B: {title_backup}  {backup_qa}_"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": title_text},
        })

    if description:
        desc_qa = _copy_qa(description, 170)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Description:* {description}  _{desc_qa}_"},
        })

    if short_desc:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Short:* {short_desc}"},
        })

    if cta:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*CTA:* \"{cta.get('yes', '')}\" / \"{cta.get('no', '')}\""},
        })

    detail_parts = []
    if restrictions:
        r = " · ".join(line.strip() for line in restrictions.splitlines() if line.strip())
        detail_parts.append(f":warning: *Restrictions:* {r}")
    if tracking_url and tracking_url != "Not available — pull from network portal":
        detail_parts.append(f"*Tracking URL:* `{tracking_url}`")
    if offer_id:
        if portal_url:
            detail_parts.append(f"*Creatives:* <{portal_url}|View on {network}> · Offer ID: `{offer_id}`")
        else:
            detail_parts.append(f"*Creatives:* Pull from {network} portal · Offer ID: `{offer_id}`")
    if detail_parts:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(detail_parts)},
        })

    blocks.append({"type": "divider"})

    context_elements = []
    footer_parts = []
    if bottom:
        footer_parts.append(f"_{bottom}_")

    if footer_parts:
        context_elements.append({"type": "mrkdwn", "text": "\n".join(footer_parts)})
    if context_elements:
        blocks.append({"type": "context", "elements": context_elements})

    if thread_ts:
        cta_obj = copy.get("cta") or {}
        _btn_json = json.dumps({
            "advertiser":   advertiser,
            "offer_id":     offer_id,
            "payout":       payout,
            "network":      network,
            "tracking_url": tracking_url,
            "thread_ts":    thread_ts,
            "t":   (copy.get("title", ""))[:120],
            "d":   (copy.get("description", ""))[:200],
            "cy":  (cta_obj.get("yes", ""))[:60],
            "cn":  (cta_obj.get("no", ""))[:60],
            "rpm": brief_data.get("scout_score_rpm", 0),
            "pf":  (brief_data.get("performance_context", ""))[:120],
            "rf":  (brief_data.get("risk_flag", ""))[:80],
            "pt":  (brief_data.get("payout_type", "CPA"))[:10],
        }, separators=(",", ":"))
        try:
            json.loads(_btn_json[:2900])
            btn_val = _btn_json[:2900]
        except json.JSONDecodeError:
            btn_val = json.dumps({
                "advertiser":   advertiser,
                "offer_id":     offer_id,
                "payout":       payout,
                "network":      network,
                "tracking_url": tracking_url[:200],
                "thread_ts":    thread_ts,
            }, separators=(",", ":"))[:2900]
        blocks.append({
            "type": "actions",
            "elements": [{
                "type":      "button",
                "text":      {"type": "plain_text", "text": "✓  Add to Queue", "emoji": True},
                "style":     "primary",
                "action_id": "scout_brief_queue",
                "value":     btn_val,
            }],
        })

    return blocks


# ---------------------------------------------------------------------------
# Opportunity cards
# ---------------------------------------------------------------------------
def _build_opportunity_cards(offers: list, thread_ts: str = "") -> list:
    """Render a list of formatted offer dicts as visual Slack cards."""
    blocks: list = []

    if len(offers) >= 5:
        blocks.append({
            "type": "header",
            "text": {"type": "plain_text", "text": f"📋 Top Opportunities ({len(offers)})"},
        })
        blocks.append({"type": "divider"})

    for offer in offers[:10]:
        advertiser = offer.get("advertiser", "Unknown")
        payout     = offer.get("payout", "Rate TBD")
        category   = offer.get("category", "")
        network    = offer.get("network", "")
        geo        = offer.get("geo", "")
        perf_note  = offer.get("performance_context", "")
        score      = offer.get("scout_score_rpm", 0)
        ms_status  = offer.get("ms_status", "")

        meta_parts = [p for p in [payout, category, geo] if p]
        meta_str = "  ·  ".join(meta_parts) if meta_parts else ""

        detail_parts = []
        if perf_note:
            detail_parts.append(perf_note)
        if score:
            detail_parts.append(f"Scout: ${score:.2f} RPM")
        if ms_status and ms_status != "Not in System":
            detail_parts.append(ms_status)
        detail_str = "  ·  ".join(detail_parts) if detail_parts else ""

        text = f"*{advertiser}*"
        if meta_str:
            text += f"\n{meta_str}"
        if detail_str:
            text += f"\n_{detail_str}_"

        icon_url = offer.get("icon_url", "") or offer.get("hero_url", "")
        section: dict = {"type": "section", "text": {"type": "mrkdwn", "text": text}}
        if icon_url and icon_url.startswith("http"):
            section["accessory"] = {"type": "image", "image_url": icon_url, "alt_text": advertiser}
        blocks.append(section)

        risk_flag = offer.get("risk_flag", "")
        if risk_flag:
            blocks.extend(_build_alert_block("warning", risk_flag, ""))

        blocks.append({"type": "divider"})

        if thread_ts:
            btn_val = json.dumps({
                "advertiser": advertiser,
                "offer_id":   offer.get("offer_id", ""),
                "payout":     payout,
                "network":    network,
                "thread_ts":  thread_ts,
            }, separators=(",", ":"))[:2900]
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type":      "button",
                    "text":      {"type": "plain_text", "text": "✓  Add to Queue", "emoji": True},
                    "style":     "primary",
                    "action_id": "scout_brief_queue",
                    "value":     btn_val,
                }],
            })

    return blocks


# ---------------------------------------------------------------------------
# Help query detection
# ---------------------------------------------------------------------------
def _is_help_query(query: str) -> bool:
    """True if the query is asking Scout to explain itself."""
    lower = query.lower().strip()
    if lower in _HELP_TRIGGERS:
        return True
    if len(lower) < 30 and any(t in lower for t in ("help", "command", "capabilit", "what can", "how do")):
        return True
    return False


# ---------------------------------------------------------------------------
# Inline element parser + text-to-blocks converter
# ---------------------------------------------------------------------------
def _parse_inline_elements(text: str) -> list:
    """Convert a plain-text line into Slack rich_text inline element objects."""
    elements = []
    for m in _INLINE_RE.finditer(text):
        if m.group("bold_d") is not None:
            elements.append({"type": "text", "text": m.group("bold_d"), "style": {"bold": True}})
        elif m.group("bold_s") is not None:
            elements.append({"type": "text", "text": m.group("bold_s"), "style": {"bold": True}})
        elif m.group("italic") is not None:
            elements.append({"type": "text", "text": m.group("italic"), "style": {"italic": True}})
        elif m.group("code") is not None:
            elements.append({"type": "text", "text": m.group("code"), "style": {"code": True}})
        elif m.group("emoji") is not None:
            name = _EMOJI_ALIASES.get(m.group("emoji"), m.group("emoji"))
            elements.append({"type": "emoji", "name": name})
        elif m.group("url") is not None:
            elements.append({"type": "link", "url": m.group("url"), "text": m.group("url_text")})
        elif m.group("user") is not None:
            elements.append({"type": "user", "user_id": m.group("user")})
        elif m.group("plain") is not None:
            t = m.group("plain")
            if elements and elements[-1].get("type") == "text" and "style" not in elements[-1]:
                elements[-1]["text"] += t
            else:
                elements.append({"type": "text", "text": t})
    return elements or [{"type": "text", "text": text}]


def _text_to_blocks(text: str) -> list:
    """
    Convert Claude's markdown response text into Block Kit blocks using native rich_text.

    Structure:
    - '---' separators → divider blocks between sections
    - Lines starting with '>' → mrkdwn context block
    - Bullet lines (•, -, *) → rich_text_list element
    - Triple-backtick fences → rich_text_preformatted element
    - Everything else → rich_text_section with typed inline elements

    Falls back to a single mrkdwn section block on any parse failure.
    """
    _BULLET_RE = re.compile(r'^[•\-\*]\s+')
    _FENCE_RE  = re.compile(r'^```')

    def _flush_section(line_buf: list) -> "list | None":
        joined = "\n".join(line_buf).strip()
        if not joined:
            return None
        inline = _parse_inline_elements(joined)
        return {"type": "rich_text_section", "elements": inline}

    def _flush_list(items: list) -> "dict | None":
        if not items:
            return None
        return {
            "type": "rich_text_list",
            "style": "bullet",
            "indent": 0,
            "elements": [
                {"type": "rich_text_section", "elements": _parse_inline_elements(item)}
                for item in items
            ],
        }

    def _part_to_rt_elements(part: str) -> "tuple[list, list]":
        rt_elems: list = []
        ctx_lines: list = []
        line_buf: list = []
        list_buf: list = []
        table_buf: list = []
        in_fence = False
        fence_buf: list = []

        for raw_line in part.split('\n'):
            if _FENCE_RE.match(raw_line):
                if in_fence:
                    in_fence = False
                    code_text = "\n".join(fence_buf)
                    fence_buf = []
                    if list_buf:
                        el = _flush_list(list_buf)
                        list_buf = []
                        if el:
                            rt_elems.append(el)
                    if line_buf:
                        el = _flush_section(line_buf)
                        line_buf = []
                        if el:
                            rt_elems.append(el)
                    rt_elems.append({
                        "type": "rich_text_preformatted",
                        "elements": [{"type": "text", "text": code_text}],
                    })
                else:
                    in_fence = True
                continue

            if in_fence:
                fence_buf.append(raw_line)
                continue

            if raw_line.startswith('>'):
                ctx_lines.append(raw_line[1:].strip())
                continue

            stripped = raw_line.strip()

            if _TABLE_ROW_RE.match(stripped):
                if _TABLE_SEP_RE.match(stripped):
                    continue
                table_buf.append(stripped)
                continue

            if table_buf:
                table_text = '\n'.join(table_buf)
                log.debug("[text_to_blocks] pipe table fallback triggered: %d rows", len(table_buf))
                rt_elems.append({
                    "type": "rich_text_preformatted",
                    "elements": [{"type": "text", "text": table_text}],
                })
                table_buf = []

            if _BULLET_RE.match(stripped):
                item_text = _BULLET_RE.sub('', stripped)
                if line_buf:
                    el = _flush_section(line_buf)
                    line_buf = []
                    if el:
                        rt_elems.append(el)
                list_buf.append(item_text)
                continue

            if list_buf:
                el = _flush_list(list_buf)
                list_buf = []
                if el:
                    rt_elems.append(el)

            if not stripped:
                if line_buf:
                    el = _flush_section(line_buf)
                    line_buf = []
                    if el:
                        rt_elems.append(el)
            else:
                line_buf.append(stripped)

        if table_buf:
            table_text = '\n'.join(table_buf)
            log.debug("[text_to_blocks] pipe table fallback triggered: %d rows", len(table_buf))
            rt_elems.append({
                "type": "rich_text_preformatted",
                "elements": [{"type": "text", "text": table_text}],
            })
        if list_buf:
            el = _flush_list(list_buf)
            if el:
                rt_elems.append(el)
        if line_buf:
            el = _flush_section(line_buf)
            if el:
                rt_elems.append(el)

        return rt_elems, ctx_lines

    _SOLO_HEADER_RE_LOCAL = re.compile(r'^\*[^*]{15,}\*\s*$')

    def _inject_section_dividers(raw: str) -> str:
        lines = raw.strip().split('\n')
        out: list[str] = []
        saw_content = False
        for line in lines:
            stripped = line.strip()
            if (
                _SOLO_HEADER_RE_LOCAL.match(stripped)
                and saw_content
                and (not out or out[-1].strip() not in ('---', ''))
            ):
                out.append('---')
            out.append(line)
            if stripped and not stripped.startswith('>') and stripped != '---':
                saw_content = True
        return '\n'.join(out)

    try:
        parts = re.split(r'\n+\s*---\s*\n+', _inject_section_dividers(text.strip()))
        blocks: list = []

        for i, part in enumerate(parts):
            part = part.strip()
            if not part:
                if i < len(parts) - 1:
                    blocks.append({"type": "divider"})
                continue

            rt_elems, ctx_lines = _part_to_rt_elements(part)

            if rt_elems:
                blocks.append({"type": "rich_text", "elements": rt_elems})
            if ctx_lines:
                ctx_text = " · ".join(ctx_lines)
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": ctx_text}],
                })
            if i < len(parts) - 1:
                blocks.append({"type": "divider"})

        return blocks or [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]

    except Exception:
        return [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]


# ---------------------------------------------------------------------------
# Suggestion buttons (legacy wrapper — kit's wrap_response is the primary path)
# ---------------------------------------------------------------------------
def _build_suggestion_buttons(suggestions: list) -> list:
    """Build a Slack actions block with 2-3 contextual follow-up suggestion buttons."""
    def _fit(s: str, max_len: int = 25) -> str:
        if len(s) <= max_len:
            return s
        cut = s[:max_len].rsplit(' ', 1)[0]
        return cut if cut else s[:max_len]

    if not suggestions:
        return []
    buttons = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": _fit(s), "emoji": False},
            "value": s,
            "action_id": f"scout_suggestion_{i}",
        }
        for i, s in enumerate(suggestions[:3])
        if isinstance(s, str) and s.strip()
    ]
    return [{"type": "actions", "elements": buttons}] if buttons else []


# ---------------------------------------------------------------------------
# Help blocks
# ---------------------------------------------------------------------------
def _build_help_blocks() -> list:
    """JTBD-organized capabilities card."""
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "What Scout can do for you"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "Scout pulls from live Impact inventory, MS platform data, "
                    "and real ClickHouse performance benchmarks. "
                    "Ask me anything in plain English — no special syntax needed."
                ),
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*🔍 Research a specific offer*\n"
                    "`@Scout tell me about Checkr`\n"
                    "`@Scout what's the Impact offer for Progressive Insurance?`\n"
                    "`@Scout is HelloPrenup already live on the network?`"
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*📊 Gauge category or payout performance*\n"
                    "`@Scout how have fintech CPL offers performed on the network?`\n"
                    "`@Scout what's the average RPM for Health & Wellness?`\n"
                    "`@Scout is $150 CPS for a water filter brand a good deal?`"
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*🗺️ Find gaps and net-new opportunities*\n"
                    "`@Scout what verticals are we missing in the current inventory?`\n"
                    "`@Scout any travel offers on Impact that aren't already live?`\n"
                    "`@Scout find me something endemic to Q4 holiday shopping`"
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*📋 Get a full campaign brief*\n"
                    "`@Scout build a brief for Checkr`\n"
                    "Scout generates copy, tracking URL, RPM estimate, and a "
                    "pre-filled queue record — then posts *Add to Queue* buttons "
                    "so you can send it straight to the Pipeline."
                ),
            },
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    "_What Scout can't do yet: publisher-specific targeting recommendations "
                    "(needs vertical mapping data). Coming when we have it. "
                    "For now — ask about the offer, not the publisher._"
                ),
            }],
        },
    ]


# ---------------------------------------------------------------------------
# Feedback buttons
# ---------------------------------------------------------------------------
def _build_feedback_buttons(query_hash: str) -> list:
    """Adds 👎 / ✏️ feedback buttons + microcopy to Scout text responses."""
    return [
        {
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
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "_React 👎 if this is off — I'll retry. Or hit ✏️ Correct this._",
            }],
        },
    ]


# ---------------------------------------------------------------------------
# Pulse signal rendering primitives
# ── RENDERING CONTRACT ────────────────────────────────────────────────────
# All Pulse signal rendering MUST use these primitives.
# Primitives:
#   _build_signal_header(emoji, title, context="") → list[dict]
#   _build_item_card(name, left_body, right_body="", context="") → list[dict]
#   _build_action_row(buttons) → dict
#   _build_monitor_alert_blocks(emoji, title, items, cta_query) → monitors + revenue tracker
# ---------------------------------------------------------------------------

def _build_signal_header(emoji: str, title: str, context: str = "") -> list[dict]:
    """Canonical Pulse signal group header. 1 section + optional context."""
    blocks: list[dict] = [{"type": "section", "text": {"type": "mrkdwn", "text": f"{emoji}  *{title}*"}}]
    if context:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": context}]})
    return blocks


def _build_item_card(
    name: str,
    left_body: str,
    right_body: str = "",
    context: str = "",
    action_button: "dict | None" = None,
) -> list[dict]:
    """Canonical per-item card. section.fields when right_body is set; plain section otherwise."""
    if right_body:
        card: dict = {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{name}*\n{left_body}"},
                {"type": "mrkdwn", "text": right_body},
            ],
        }
    else:
        card = {"type": "section", "text": {"type": "mrkdwn", "text": f"*{name}*\n{left_body}"}}
    blocks: list[dict] = [card]
    if context:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": context}]})
    if action_button:
        blocks.append({"type": "actions", "elements": [action_button]})
    return blocks


def _build_action_row(buttons: list[dict]) -> dict:
    """Canonical actions block. Pass pre-built button element dicts."""
    return {"type": "actions", "elements": buttons}


def _build_publisher_card(
    name: str,
    delta_pct: "float | int | str",
    revenue_str: str,
    attribution: str = "",
    hypothesis: str = "",
    gaps: "list | None" = None,
    flag_count: int = 0,
) -> list[dict]:
    """Canonical publisher card: 2-col fields + combined context."""
    pct = float(delta_pct)
    left = f"*{pct:+.0f}%*  ·  {revenue_str}/mo"
    right = f"*Top Advertiser*\n{attribution}" if attribution else ""
    context_parts: list[str] = []
    if hypothesis:
        context_parts.append(hypothesis)
    if gaps:
        gap_strs = [f"{adv} (${rpm:.2f} RPM)" for adv, rpm in gaps]
        context_parts.append(f"↳ Missing: {', '.join(gap_strs)}")
    if flag_count >= 4:
        context_parts.append(f"_flagged {flag_count}d_")
    return _build_item_card(name, left, right, "  \n".join(context_parts))


def _build_monitor_alert_blocks(
    emoji: str,
    title: str,
    items: list[str],
    cta_query: str = "",
) -> "tuple[str, list[dict]]":
    """Canonical Block Kit alert for all silent monitors and revenue tracker."""
    fallback = f"{emoji} {title}"
    blocks: list[dict] = [*_build_signal_header(emoji, title)]
    if items:
        bullet_text = "\n".join(f"• {item}" for item in items[:8])
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": bullet_text}})
    if cta_query:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_`@Scout {cta_query}` for the full breakdown_"}],
        })
    blocks = enforce(blocks, Surface.MONITOR_ALARM)
    return fallback, blocks


# ---------------------------------------------------------------------------
# Queue card helpers
# ---------------------------------------------------------------------------
_MAX_QUEUE_ITEMS_RENDERED = 12

_QUEUE_STATUS_EMOJI: dict = {
    "Awaiting Entry": "🟡",
    "In Platform":    "🔵",
    "Test Offer ON":  "🟠",
    "Live":           "✅",
}

_QUEUE_STATUS_ORDER = ["Awaiting Entry", "In Platform", "Test Offer ON", "Live"]


def _normalise_payout_type(raw: str) -> str:
    """'$ PER LEAD' → 'per lead', 'CPA' → 'CPA', '' → ''"""
    if not raw:
        return ""
    cleaned = raw.lstrip("$").strip()
    upper = cleaned.upper()
    if upper in ("CPA", "CPL", "CPC", "CPM", "CPS", "REV SHARE"):
        return upper
    return cleaned.lower()


def _queue_item_context(approved_at: str) -> str:
    """'2026-04-28' → 'Approved 2d ago'  |  '' → ''"""
    if not approved_at:
        return ""
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(approved_at.replace("Z", "+00:00"))
        days = (datetime.now(timezone.utc) - dt).days
        return "Approved today" if days == 0 else f"Approved {days}d ago"
    except Exception:
        return ""


def _build_queue_card(items: "list[dict] | None") -> list:
    """
    Build Block Kit blocks for the offer pipeline queue sourced from Notion.
    items=None  → Notion unreachable (error state).
    items=[]    → queue genuinely empty.
    items=[...] → rendered grouped by status.
    """
    header = [{"type": "header", "text": {"type": "plain_text", "text": ":inbox_tray: Offer Queue", "emoji": True}}]

    if items is None:
        return header + [{"type": "section", "text": {"type": "mrkdwn", "text": ":warning: Could not reach Notion — queue data unavailable."}}]

    if not items:
        return header + [{"type": "section", "text": {"type": "mrkdwn", "text": ":white_check_mark: Queue is clear — nothing awaiting entry or in platform."}}]

    groups: dict = {s: [] for s in _QUEUE_STATUS_ORDER}
    for item in items:
        status = item.get("status", "Unknown")
        if status not in groups:
            groups[status] = []
        groups[status].append(item)

    blocks = list(header)
    rendered = 0

    for status in _QUEUE_STATUS_ORDER:
        group = groups.get(status, [])
        if not group:
            continue
        emoji = _QUEUE_STATUS_EMOJI.get(status, "⚪")
        blocks += _build_signal_header(emoji, f"{status} ({len(group)})")
        for item in group:
            if rendered >= _MAX_QUEUE_ITEMS_RENDERED:
                remaining = sum(len(g) for g in groups.values()) - rendered
                blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"_+ {remaining} more — view full queue in Notion_"}]})
                return blocks
            adv         = item.get("advertiser", "Unknown")
            network     = item.get("network", "")
            payout      = item.get("payout", 0.0)
            payout_type = item.get("payout_type", "")
            notion_url  = item.get("notion_url", "")
            approved_at = item.get("approved_at", "")
            pt          = _normalise_payout_type(payout_type)
            if payout and pt:
                payout_str = f"${payout:,.2f} {pt}"
            elif payout:
                payout_str = f"${payout:,.2f}"
            elif pt:
                payout_str = pt
            else:
                payout_str = "—"
            left_body   = f"{payout_str} · {network}" if network else payout_str
            right_body  = f"<{notion_url}|View in Notion>" if notion_url else ""
            context     = _queue_item_context(approved_at)
            blocks += _build_item_card(adv, left_body, right_body=right_body, context=context)
            rendered += 1

    for status, group in groups.items():
        if status in _QUEUE_STATUS_ORDER or not group:
            continue
        emoji = _QUEUE_STATUS_EMOJI.get(status, "⚪")
        blocks += _build_signal_header(emoji, f"{status} ({len(group)})")
        for item in group:
            if rendered >= _MAX_QUEUE_ITEMS_RENDERED:
                break
            adv         = item.get("advertiser", "Unknown")
            network     = item.get("network", "")
            payout      = item.get("payout", 0.0)
            payout_type = item.get("payout_type", "")
            notion_url  = item.get("notion_url", "")
            approved_at = item.get("approved_at", "")
            pt          = _normalise_payout_type(payout_type)
            if payout and pt:
                payout_str = f"${payout:,.2f} {pt}"
            elif payout:
                payout_str = f"${payout:,.2f}"
            elif pt:
                payout_str = pt
            else:
                payout_str = "—"
            left_body   = f"{payout_str} · {network}" if network else payout_str
            right_body  = f"<{notion_url}|View in Notion>" if notion_url else ""
            context     = _queue_item_context(approved_at)
            blocks += _build_item_card(adv, left_body, right_body=right_body, context=context)
            rendered += 1

    return blocks


# ---------------------------------------------------------------------------
# Money / delta formatting helpers
# ---------------------------------------------------------------------------
def _fmt_money_short(cents: int) -> str:
    """Compact dollar string for scoreboard headlines: $42.1K, $1.2M, $312."""
    dollars = (cents or 0) / 100.0
    if abs(dollars) >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    if abs(dollars) >= 1_000:
        return f"${dollars / 1_000:.1f}K"
    return f"${int(round(dollars))}"


def _fmt_delta_pct(today: int, baseline: int) -> str:
    """Signed Δ% with arrow glyph. Returns '—' when baseline is zero."""
    if not baseline:
        return "—"
    pct = round(100.0 * (today - baseline) / baseline, 1)
    arrow = "↗" if pct > 0 else ("↘" if pct < 0 else "→")
    sign = "+" if pct > 0 else ""
    return f"{arrow} {sign}{pct}%"


def _build_sparkline_url(series: "list[int]") -> "str | None":
    """Return a quickchart.io chart URL for the 7-day revenue sparkline."""
    if not series or all(v == 0 for v in series):
        return None
    import urllib.parse as _up
    data_vals = [round(v / 100, 2) for v in series]
    chart_cfg = {
        "type": "line",
        "data": {
            "labels": [""] * len(data_vals),
            "datasets": [{
                "data": data_vals,
                "fill": True,
                "borderColor": "#00875a",
                "backgroundColor": "rgba(0,135,90,0.12)",
                "borderWidth": 2,
                "pointRadius": 0,
                "tension": 0.4,
            }],
        },
        "options": {
            "legend": {"display": False},
            "scales": {
                "xAxes": [{"display": False}],
                "yAxes": [{"display": False}],
            },
        },
    }
    cfg_enc = _up.quote(json.dumps(chart_cfg, separators=(",", ":")))
    return f"https://quickchart.io/chart?c={cfg_enc}&w=600&h=120&bkg=transparent"


# ---------------------------------------------------------------------------
# App Home scoreboard + view
# ---------------------------------------------------------------------------
def _build_home_scoreboard_blocks(rollup, alerts) -> list:
    """Headline pulse + alert health line for App Home."""
    blocks: list = []

    if rollup is None:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*Today's revenue:* —\n_Data temporarily unavailable._"},
        })
    else:
        rev = _fmt_money_short(rollup.revenue_today_cents)
        d_yest = _fmt_delta_pct(rollup.revenue_today_cents,
                                rollup.revenue_yesterday_same_time_cents)
        d_7d   = _fmt_delta_pct(rollup.revenue_today_cents,
                                rollup.revenue_7d_avg_cents)
        blocks.append({
            "type": "header",
            "text": {"type": "plain_text", "text": f"{rev} today", "emoji": False},
        })

        sparkline_url = _build_sparkline_url(getattr(rollup, "revenue_7d_series", []))
        if sparkline_url:
            blocks.append({
                "type": "image",
                "image_url": sparkline_url,
                "alt_text": "7-day revenue trend",
            })

        proj_cents = getattr(rollup, "revenue_eod_projection_cents", 0)
        if proj_cents > 0:
            proj_str = _fmt_money_short(proj_cents)
            if rollup.revenue_today_cents > rollup.revenue_yesterday_same_time_cents:
                pace_arrow = "↗"
            elif rollup.revenue_today_cents < rollup.revenue_yesterday_same_time_cents:
                pace_arrow = "↘"
            else:
                pace_arrow = "→"
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"On pace for *{proj_str}* by end of day {pace_arrow}",
                },
            })

        from datetime import timezone as _tz
        generated_at = getattr(rollup, "generated_at", None)
        if generated_at is not None:
            _ts = int(generated_at.replace(tzinfo=_tz.utc).timestamp())
            _fallback = generated_at.strftime("%-I:%M %p UTC")
            freshness = f"<!date^{_ts}^Data from {{time}}|{_fallback}>"
        else:
            freshness = "just now"
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"{d_yest} vs yesterday · {d_7d} vs 7d avg · {freshness}",
            }],
        })

        # ── MTD goal + pace line ──────────────────────────────────────────────
        # Reads monthly_revenue_target from scout_thresholds.json (top-level key).
        # Omits the block entirely when the key is 0, None, or missing — fail-closed.
        _monthly_target = SCOUT_THRESHOLDS.get("monthly_revenue_target") or 0
        if _monthly_target > 0:
            from datetime import datetime as _dt2, timezone as _tz2
            from zoneinfo import ZoneInfo as _ZI
            _mtd_cents = getattr(rollup, "revenue_mtd_cents", 0) or 0
            _target_cents = int(_monthly_target * 100)
            _now_ct = _dt2.now(_ZI("America/Chicago"))
            _month_start = _now_ct.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            import calendar as _cal
            _days_in_month = _cal.monthrange(_now_ct.year, _now_ct.month)[1]
            _days_elapsed = max((_now_ct - _month_start).total_seconds() / 86400, 0.01)
            _days_left = max(_days_in_month - _days_elapsed, 0)
            _pct = round(100.0 * _mtd_cents / _target_cents) if _target_cents else 0
            _running_per_day = _mtd_cents / _days_elapsed  # cents/day so far
            _needed_per_day  = (_target_cents - _mtd_cents) / _days_left if _days_left > 0 else 0
            _mtd_str    = _fmt_money_short(_mtd_cents)
            _target_str = _fmt_money_short(_target_cents)
            _need_str   = _fmt_money_short(int(_needed_per_day))
            _run_str    = _fmt_money_short(int(_running_per_day))
            _days_left_int = max(int(_days_left), 0)
            # Pace indicator: 🔴 badly behind, 🟡 slightly behind, 🟢 on track
            if _needed_per_day > 0 and _running_per_day < _needed_per_day * 0.8:
                _pace_icon = "🔴"
            elif _needed_per_day > 0 and _running_per_day < _needed_per_day:
                _pace_icon = "🟡"
            else:
                _pace_icon = "🟢"
            _pace_line = (
                f"{_mtd_str} / {_target_str} MTD · {_pct}% · "
                f"{_days_left_int} days left · "
                f"need {_need_str}/day · "
                f"running {_run_str}/day {_pace_icon}"
            )
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": _pace_line}],
            })

    firing = list(alerts or [])
    if not firing:
        health_text = "🟢 *All systems normal.*"
    elif len(firing) == 1:
        name = firing[0].alert_name.replace("_", " ")
        health_text = f"🟠 *1 alert firing:* {name}"
    else:
        health_text = f"🔴 *{len(firing)} alerts firing.*"

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": health_text},
    })

    if firing:
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "See details →", "emoji": False},
                "action_id": "home_alert_drill",
                "style": "primary",
            }],
        })

    blocks.append({"type": "divider"})
    blocks = enforce(blocks, Surface.HOME)
    return blocks


def _build_home_view(queue_items: "list[dict] | None" = None,
                     rollup=None,
                     alerts=None) -> dict:
    """
    App Home — activation surface, NOT a dashboard.

    JTBD: get a first-timer to click an example and have the magic moment.
    Mobile-first: CTAs render in dedicated `actions` blocks (NOT
    section.accessory, which clips on narrow iOS widths). Queries use inline
    `code` (NOT fenced ```, which horizontal-scrolls on mobile). action_ids
    are unique within the view — verified by test_kit_lint.py.

    `queue_items` is accepted for backwards compatibility but no longer rendered on Home.
    """
    del queue_items  # intentionally unused — queue moved off Home

    blocks: list = []

    if rollup is not None or alerts is not None:
        blocks.extend(_build_home_scoreboard_blocks(rollup, alerts))

    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                "*Revenue intelligence — plain English.*\n"
                "Mention `@Scout` in any channel. Context carries through the thread."
            ),
        },
    })

    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"⭐ *{_HOME_HERO['jtbd']}*\n{_HOME_HERO['description']}",
        },
    })
    blocks.append({
        "type": "actions",
        "elements": [{
            "type": "button",
            "text": {"type": "plain_text",
                     "text": f"Try: {_HOME_HERO['cta']} →", "emoji": False},
            "style": "primary",
            "action_id": "home_try_query_hero",
            "value": _HOME_HERO["query"],
        }],
    })
    blocks.append({"type": "divider"})

    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": ex["jtbd"], "emoji": False},
                "action_id": f"home_try_query_{idx}",
                "value": ex["query"],
            }
            for idx, ex in enumerate(_HOME_SECONDARY)
        ],
    })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": "`/scout-help` for all commands · `/scout-status` for system health",
        }],
    })

    return {"type": "home", "blocks": blocks}
