"""
scout_slack_ui.py — All Slack Block Kit builders for Scout.

Pure functions: take Python data, return Slack blocks (list[dict]).
Zero ClickHouse, Notion, or Slack API calls here.

Conditional rendering based on caller-provided data is OK — e.g. showing
a risk_flag warning when non-empty, hiding a button when a field is absent.
The caller decides what's true; scout_slack_ui decides how to display it.

Button value contract:
  _build_opportunity_cards() sets:  {"offer_id": ..., "advertiser": ..., ...}
  _handle_approve() reads:           v.get("offer_id") and v.get("advertiser")
  Changing key names in one file REQUIRES updating the other — no type enforcement.
"""

from __future__ import annotations

import json
import logging
import random
import re

from scout_state import _load_launched_offers

log = logging.getLogger("scout_slack_ui")

log_ref = None  # populated at import time by scout_bot

# NOTE: _SOLO_HEADER_RE is promoted to module level (was inside _text_to_blocks)
_SOLO_HEADER_RE = re.compile(r'^\*[^*]{15,}\*\s*')

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
    r'|(?P<plain>[^*_`:<\n]+|[*_`:<])'
)


_HOME_EXAMPLES = [
    {
        "jtbd":        "Morning triage — what needs my attention?",
        "description": "Get a plain-English summary of what moved overnight and who needs a call.",
        "query":       "What happened today?",
    },
    {
        "jtbd":        "Prep for a publisher call",
        "description": "Full account picture: provisioned offers, what's serving, revenue health, what to pitch.",
        "query":       "Give me a health check on TuitionHero",
    },
    {
        "jtbd":        "Understand a revenue drop",
        "description": "Diagnose why a publisher's revenue fell — which advertiser pulled back and when.",
        "query":       "What happened to Pinger this week?",
    },
    {
        "jtbd":        "Build a campaign brief",
        "description": "Get campaign-ready copy, tracking URL, and RPM estimate. One click to add to the queue.",
        "query":       "Build a brief for Square",
    },
    {
        "jtbd":        "Find better payouts",
        "description": "Check if an advertiser exists on other networks at a higher payout rate.",
        "query":       "Find Capital One Shopping on other networks — is there a better payout?",
    },
]


def _pitch_signal(score: float) -> str:
    """Return pitch-readiness emoji + label based on Scout RPM score."""
    if score >= 2.00:
        return "✅ Pitch-ready"
    if score > 0:
        return "⚠️ Low signal"
    return "🔍 Rate TBD"


def _build_alert_block(severity: str, title: str, body: str = "") -> list[dict]:
    """
    Build an Alert block with severity levels for visual hierarchy.

    severity: "danger" | "warning" | "info"
    - danger: 🔴 Ghost campaigns, caps near limit, critical issues
    - warning: 🟡 Fill rate, velocity drops, warnings
    - info: ℹ️ General context, non-blocking info

    Returns list of blocks for consistent stacking.
    """
    _SEVERITY_MAP = {
        "danger": {"emoji": "🔴", "label": "CRITICAL"},
        "warning": {"emoji": "🟡", "label": "WARNING"},
        "info": {"emoji": "ℹ️", "label": "INFO"},
    }
    sev = _SEVERITY_MAP.get(severity, _SEVERITY_MAP["info"])
    emoji = sev["emoji"]
    label = sev["label"]

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *{label}:* {title}",
            },
        },
    ]
    if body:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": body}],
        })
    return blocks


def _build_card_with_image(
    title: str,
    subtitle: str,
    hero_url: str = "",
    body: str = "",
    buttons: list[dict] = None,
    fields: list[dict] = None,
) -> list[dict]:
    """
    Build a visual card with hero image, title, subtitle, body text, and action buttons.

    Layout:
      [section] Title · Subtitle
              [hero image accessory]
      [section] Body text (if present)
      [fields] Stats grid (if present)
      [actions] Buttons (if present)
    """
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
                {"type": "mrkdwn", "text": f"*{f['label']}|\n{f['value']}"}
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


def _build_data_table(headers: list[str], rows: list[list[str]]) -> list[dict]:
    """
    Build a structured data table for metrics display.

    Uses section.fields for 2-column grid layout.
    Headers should be ≤4 for readable display.
    """
    if not headers or not rows:
        return []

    if len(headers) > 4:
        headers = headers[:4]

    fields = [{"type": "mrkdwn", "text": f"*{h}*"} for h in headers]

    for row in rows[:8]:
        row_text = "  ·  ".join(str(cell) for cell in row[:len(headers)])
        fields.append({"type": "mrkdwn", "text": row_text})

    return [{"type": "section", "text": {"type": "mrkdwn", "text": ""}, "fields": fields}]


def _build_rich_text_list(items: list[str], ordered: bool = False, indent: int = 0) -> list[dict]:
    """
    Build a rich text bulleted or numbered list.

    Uses native rich_text_list for proper Slack rendering.
    """
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

def _queue_confirm_blocks(
    advertiser: str,
    network: str,
    payout_display: str,
    user_id: str,
    score: float,
    notion_url: str | None,
) -> list[dict]:
    """
    Block Kit card for queue confirmation — enhanced visual treatment.

    Layout:
      [header]  ✅ Advertiser Added to Queue
      [section] Network · Payout · Status
      [context] Added by @user  ·  $X RPM  ·  Pitch-ready
    """
    signal   = _pitch_signal(score)
    score_str = f"${score:.2f} RPM" if score else "Rate TBD"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"✅ {advertiser} Queued"},
        },
    ]

    section_text = f"{network} · {payout_display}"
    section_block: dict = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": section_text},
    }
    if notion_url:
        section_block["accessory"] = {
            "type": "button",
            "text": {"type": "plain_text", "text": "View Brief →", "emoji": True},
            "url": notion_url,
        }
    blocks.append(section_block)

    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"Added by <@{user_id}>"},
            {"type": "mrkdwn", "text": score_str},
            {"type": "mrkdwn", "text": signal},
        ],
    })

    return blocks

def _build_advertiser_rpm_context_blocks(ctx: dict, scout_estimate: float = 0) -> list[dict]:
    """
    Return a context block showing the advertiser's 30-day platform RPM history.

    Only called when ctx["has_history"] is True — caller is responsible for the check.
    Shows campaign count, impressions, revenue, and RPM range vs Scout's estimate.

    Labels RPM as "platform RPM" (pre-publisher multiplier) so the team has the right frame.
    """
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

    # Support both old schema (titles/ctas lists) and new schema (title/cta single)
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

    # Header — include MS status so decision context is instant
    # No hero_url full-width image — too much scroll cost in a channel with 6-8 briefs.
    # icon_url (brand mark) becomes an accessory on the stats section: instant brand
    # recognition right next to the numbers where it helps, without the scroll tax.
    status_tag = {"Not in System": " · New", "Live": " · Already Live", "In System": " · In System"}.get(ms_status, "")
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"Campaign Brief — {advertiser}{status_tag}", "emoji": False},
    })

    # ── 2-col stats grid ──────────────────────────────────────────────────────
    # RPM display reflects the confidence tier from _scout_score():
    #   score=0 + risk_flag present  → "Not estimated" (high-friction offer suppressed)
    #   score=0, no risk flag        → "N/A" (no data at any tier)
    #   real MS data                 → "$X,XXX" (no qualifier — it's measured)
    #   same-advertiser benchmark    → "~$X,XXX est." (1 step removed)
    #   category×payout benchmark    → "~$X,XXX est." (grounded but indirect)
    #   payout-type-only fallback    → "~$X,XXX est. (broad avg)" (lowest real signal)
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
    # Performance field omitted — RPM already carries the confidence qualifier (est./no prior data)
    # icon_url as accessory: brand mark right-aligned on the stats grid — brand recognition
    # at the decision point without adding scroll. Falls back gracefully when absent.
    stats_block: dict = {"type": "section", "fields": stat_fields}
    if icon_url and icon_url.startswith("http"):
        stats_block["accessory"] = {
            "type": "image",
            "image_url": icon_url,
            "alt_text": advertiser,
        }
    blocks.append(stats_block)

    # Risk flag — surface before copy using Alert block for visibility
    if risk_flag:
        blocks.extend(_build_alert_block("warning", f"Fit note: {risk_flag}", ""))

    blocks.append({"type": "divider"})

    # ── Copy QA ───────────────────────────────────────────────────────────────
    _PROHIBITED_CHARS = ("—", "–", "™", "®")

    def _copy_qa(text: str, max_len: int) -> str:
        """Return a ✓/⚠ QA badge: char count, and flag if prohibited chars found."""
        length = len(text)
        has_prohibited = any(c in text for c in _PROHIBITED_CHARS)
        if has_prohibited:
            flagged = [c for c in _PROHIBITED_CHARS if c in text]
            return f"⚠ prohibited chars: {', '.join(repr(c) for c in flagged)}"
        if length > max_len:
            return f"⚠ {length} chars (max {max_len})"
        return f"✓ {length} chars"

    # ── Copy ─────────────────────────────────────────────────────────────────
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

    # ── Details ───────────────────────────────────────────────────────────────
    # Targeting omitted — geo is in stats, category in header, score in RPM.
    # Only surface what isn't already visible above.
    detail_parts = []
    if restrictions:
        # Normalize multi-line internal_notes into a single line for scannability
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

    # ── Bottom line + handoff ─────────────────────────────────────────────────
    # icon_url moved to stats section accessory — not repeated here.
    context_elements = []
    footer_parts = []
    if bottom:
        footer_parts.append(f"_{bottom}_")
    # "Ready to build?" removed — Creatives field already tells you exactly what to do

    if footer_parts:
        context_elements.append({"type": "mrkdwn", "text": "\n".join(footer_parts)})
    if context_elements:
        blocks.append({"type": "context", "elements": context_elements})

    # ── Add to Queue button ───────────────────────────────────────────────────
    # Only rendered when thread_ts is known (i.e., a real @Scout mention, not a preview).
    # Packs enough data in value so the handler can write the queue item without
    # re-fetching the brief — keeps the click instant.
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
            # Truncation split a unicode escape — fall back to minimal safe payload
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

def _build_opportunity_cards(offers: list, thread_ts: str = "") -> list:
    """
    Render a list of formatted offer dicts as visual Slack cards.
    Enhanced version with Alert blocks for risk flags and richer formatting.

    When 5+ offers, renders as a virtual Carousel (consecutive cards).
    Each card: section with risk Alert block + action button.
    """
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

        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

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

def _is_help_query(query: str) -> bool:
    """True if the query is asking Scout to explain itself."""
    lower = query.lower().strip()
    if lower in _HELP_TRIGGERS:
        return True
    # Short questions that are clearly meta, not about a specific offer
    if len(lower) < 30 and any(t in lower for t in ("help", "command", "capabilit", "what can", "how do")):
        return True
    return False

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
    - Lines starting with '>' → mrkdwn context block (Slack disallows rich_text in context)
    - Bullet lines (•, -, *) → rich_text_list element
    - Triple-backtick fences → rich_text_preformatted element
    - Everything else → rich_text_section with typed inline elements

    Falls back to a single mrkdwn section block on any parse failure.
    """
    _BULLET_RE = re.compile(r'^[•\-\*]\s+')
    _FENCE_RE  = re.compile(r'^```')

    def _flush_section(line_buf: list) -> list | None:
        """Emit a rich_text_section from accumulated lines, or None if empty."""
        joined = "\n".join(line_buf).strip()
        if not joined:
            return None
        inline = _parse_inline_elements(joined)
        return {"type": "rich_text_section", "elements": inline}

    def _flush_list(items: list) -> dict | None:
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

    def _part_to_rt_elements(part: str) -> tuple[list, list]:
        """
        Parse one section (between --- dividers) into:
          (rich_text_elements, context_lines)
        rich_text_elements go into a single rich_text block.
        context_lines are rendered as a separate mrkdwn context block.
        """
        rt_elems: list = []
        ctx_lines: list = []
        line_buf: list = []
        list_buf: list = []
        in_fence = False
        fence_buf: list = []

        for raw_line in part.split('\n'):
            # ── Code fence toggle ────────────────────────────────────────────
            if _FENCE_RE.match(raw_line):
                if in_fence:
                    # Close fence
                    in_fence = False
                    code_text = "\n".join(fence_buf)
                    fence_buf = []
                    if list_buf:
                        el = _flush_list(list_buf); list_buf = []
                        if el: rt_elems.append(el)
                    if line_buf:
                        el = _flush_section(line_buf); line_buf = []
                        if el: rt_elems.append(el)
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

            # ── Context line ('>') ───────────────────────────────────────────
            if raw_line.startswith('>'):
                ctx_lines.append(raw_line[1:].strip())
                continue

            stripped = raw_line.strip()

            # ── Bullet line ──────────────────────────────────────────────────
            if _BULLET_RE.match(stripped):
                item_text = _BULLET_RE.sub('', stripped)
                if line_buf:
                    el = _flush_section(line_buf); line_buf = []
                    if el: rt_elems.append(el)
                list_buf.append(item_text)
                continue

            # ── Regular line ─────────────────────────────────────────────────
            if list_buf:
                el = _flush_list(list_buf); list_buf = []
                if el: rt_elems.append(el)

            if not stripped:
                # Blank line → flush current section paragraph
                if line_buf:
                    el = _flush_section(line_buf); line_buf = []
                    if el: rt_elems.append(el)
            else:
                line_buf.append(stripped)

        # Flush remaining buffers
        if list_buf:
            el = _flush_list(list_buf)
            if el: rt_elems.append(el)
        if line_buf:
            el = _flush_section(line_buf)
            if el: rt_elems.append(el)

        return rt_elems, ctx_lines

    _SOLO_HEADER_RE = re.compile(r'^\*[^*]{15,}\*\s*$')

    def _inject_section_dividers(raw: str) -> str:
        """Insert --- before standalone bold section headers that follow content."""
        lines = raw.strip().split('\n')
        out: list[str] = []
        saw_content = False
        for line in lines:
            stripped = line.strip()
            if (
                _SOLO_HEADER_RE.match(stripped)
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

def _build_help_blocks() -> list:
    """
    JTBD-organized capabilities card.
    Organized by job-to-be-done, not by command syntax.
    Examples are copy-pasteable, honest about limits.
    """
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

def _build_feedback_buttons(query_hash: str) -> list:
    """
    Adds 👍 / 👎 / ✏️ feedback buttons to Scout text responses.
    query_hash: short identifier for the query (for learnings tracking).
    """
    return [
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👍 Accurate", "emoji": True},
                    "action_id": "scout_feedback_good",
                    "value": query_hash,
                    "style": "primary",
                },
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
        }
    ]

# ── RENDERING CONTRACT ────────────────────────────────────────────────────────
# All Pulse signal rendering MUST use these primitives. Never inline Block Kit
# construction for signal headers or per-item cards — wrong patterns become
# harder than right patterns when the interface doesn't permit them.
#
# Primitives:
#   _build_signal_header(emoji, title, context="") → list[dict]
#   _build_item_card(name, left_body, right_body="", context="") → list[dict]
#   _build_action_row(buttons) → dict
#
# Prohibited patterns:
#   ❌  "  ·  ".join([items])          — use one card per item
#   ❌  "  •   *Name*"       — use _build_item_card
#   ❌  _build_alert_block() for Pulse — use _build_signal_header
#   ❌  Separate context blocks per field — merge into one context string
# ─────────────────────────────────────────────────────────────────────────────

def _build_signal_header(emoji: str, title: str, context: str = "") -> list[dict]:
    """Canonical Pulse signal group header. 1 section + optional context.
    No 'WARNING:'/'CRITICAL:' label — emoji communicates severity.
    RENDERING CONTRACT: all new Pulse signals MUST use this for headers."""
    blocks: list[dict] = [{"type": "section", "text": {"type": "mrkdwn", "text": f"{emoji}  *{title}*"}}]
    if context:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": context}]})
    return blocks


def _build_item_card(
    name: str,
    left_body: str,
    right_body: str = "",
    context: str = "",
    action_button: dict | None = None,
) -> list[dict]:
    """Canonical per-item card. section.fields when right_body is set; plain section otherwise.
    RENDERING CONTRACT: one call per item — never join multiple items on one line.
    For per-item action buttons, pass a pre-built button element dict via action_button."""
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
    delta_pct: float | int | str,
    revenue_str: str,
    attribution: str = "",
    hypothesis: str = "",
    gaps: list | None = None,
    flag_count: int = 0,
) -> list[dict]:
    """Canonical publisher card: 2-col fields + combined context.
    Used by _format_pulse_blocks (NEEDS ATTENTION, MOMENTUM) and agent tool responses."""
    pct = float(delta_pct)   # guard: pipeline may return int or string
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


def _format_pulse_blocks(
    signals: dict,
    is_weekend: bool = False,
    flagged_history: dict | None = None,
    chronic: list | None = None,
    _today=None,  # Change H: test-injection seam for Monday path — do NOT remove
) -> tuple[str, list]:
    """
    Format pulse signals into a Slack Block Kit message.
    Returns (fallback_text, blocks).

    Design principles:
    - Ghost campaigns first — P0, burning inventory, team needs to act immediately
    - Urgency-first: NEEDS ATTENTION (downs) before MOMENTUM (ups)
    - One line per publisher — name, %, current rate, attribution all inline
    - Standing checks (caps, overnight) compact at bottom
    - Non-events are context blocks (gray, small) — don't compete with real signals
    - Weekend mode: higher thresholds (noise suppression), different title
    - Signal fatigue: annotate persistent issues, demote 7d+ to standing checks
    """
    from datetime import date as _date, timedelta as _td

    # Rotating NEEDS ATTENTION section hints — actionable nudge at the bottom of the
    # NEEDS ATTENTION block. Separate from the footer. Different day offset so they
    # don't cycle in sync and feel independent.
    _NA_HINTS = [
        "`@Scout dig into [partner]` → detailed breakdown + action plan",
        "Reply with a partner name for deeper analysis",
        "`@Scout why is [partner] down?` → root cause + fix",
        "`@Scout gaps for [partner]` → what advertisers are missing",
        "`@Scout what should I fix first?` → prioritized action list",
    ]
    _na_hint_idx  = (_date.today().timetuple().tm_yday + 3) % len(_NA_HINTS)
    na_section_hint = _NA_HINTS[_na_hint_idx]

    # Rotating footer hints — picks a different prompt each day so the pulse
    # never looks stale. Weekday and weekend lists are separate.
    _WEEKDAY_HINTS = [
        "`@Scout what happened to [partner]?`",
        "`@Scout gaps for [partner]`",
        "`@Scout health brief on [campaign]`",
        "`@Scout why is [partner] down?`",
        "`@Scout top performers this week`",
        "`@Scout what's missing from [partner]?`",
        "`@Scout any ghost campaigns today?`",
    ]
    _WEEKEND_HINTS = [
        "`@Scout check this over the weekend`",
        "`@Scout any ghost campaigns this weekend?`",
        "`@Scout what needs attention before Monday?`",
    ]
    _hint_pool = _WEEKEND_HINTS if is_weekend else _WEEKDAY_HINTS
    _hint_idx  = _date.today().timetuple().tm_yday % len(_hint_pool)
    footer_hint = _hint_pool[_hint_idx]

    def _fmt_k(n: float) -> str:
        if abs(n) >= 1000:
            k = n / 1000
            return f"${k:.0f}K" if k == int(k) else f"${k:.1f}K"
        return f"${n:.0f}"

    def _inline_attr(v: dict) -> str:
        """One-line attribution label — fits inline on the publisher signal line."""
        advs      = v.get("top_advertisers", [])
        direction = v.get("direction", "up")
        parts = []
        for a in advs:
            delta = a["delta_ann"]
            if direction == "up" and delta < 0:
                continue
            if direction == "down" and delta > 0:
                continue
            if a["rev_7d"] == 0 and delta < 0:
                parts.append(f"{a['adv_name']} inactive")
            else:
                sign = "+" if delta >= 0 else "-"
                parts.append(f"{a['adv_name']} {sign}{_fmt_k(abs(delta))}")
        return "  ·  ".join(parts)

    today_d      = _today if _today is not None else _date.today()
    today_label  = today_d.strftime("%B %-d, %Y")
    cap_alerts   = signals.get("cap_alerts", [])
    vel_shifts   = signals.get("velocity_shifts", [])
    night_events = signals.get("overnight_events", [])
    ghost_camps  = signals.get("ghost_campaigns", [])
    fill_rate    = signals.get("fill_rate", [])
    opportunities = signals.get("opportunities", [])

    # Sort by absolute dollar impact (not % change) — a -48% drop on $29K/mo
    # outranks a -98% drop on $129/mo. Magnitude matters more than ratio.
    downs = sorted(
        [v for v in vel_shifts if v["direction"] == "down"],
        key=lambda x: abs(x["revenue_7d_ann"] - x["revenue_30d"]),
        reverse=True,
    )
    ups = sorted(
        [v for v in vel_shifts if v["direction"] == "up"],
        key=lambda x: abs(x["revenue_7d_ann"] - x["revenue_30d"]),
        reverse=True,
    )

    # Weekend mode: raise thresholds to suppress noise — only high-magnitude moves
    # Team response is slower on weekends; false urgency trains people to ignore it.
    if is_weekend:
        downs = [d for d in downs if abs(d["revenue_7d_ann"] - d["revenue_30d"]) >= 15000]
        ups   = [u for u in ups   if abs(u["revenue_7d_ann"] - u["revenue_30d"]) >= 15000]

    # Cap alerts — urgent vs routine
    urgent_caps  = [a for a in cap_alerts if a["cap_pct"] >= 90 and a["days_to_cap"] < a["days_remaining"]]
    # Weekend: skip routine caps entirely — not actionable until Monday
    routine_caps = [] if is_weekend else [a for a in cap_alerts if a not in urgent_caps]

    # Signal fatigue: separate persistent (7d+) downs from regular downs
    # P9-2: chronic partners are excluded from NEEDS ATTENTION entirely
    fh = flagged_history or {}
    chronic_set = set(chronic or [])
    persistent_downs: list[tuple] = []  # (v, flag_count) — demoted to standing
    regular_downs:    list[tuple] = []  # (v, flag_count) — rendered normally
    for v in downs[:3]:
        pname = v["publisher_name"]
        if pname in chronic_set:
            continue  # chronic — shown in CHRONIC ISSUES footer instead
        flag_count = fh.get(pname, {}).get("count", 0)
        if flag_count >= 7:
            persistent_downs.append((v, flag_count))
        else:
            regular_downs.append((v, flag_count))

    blocks: list = []

    # ── Title ─────────────────────────────────────────────────────────────────
    if is_weekend:
        sat = today_d if today_d.weekday() == 5 else today_d - _td(days=1)
        sun = sat + _td(days=1)
        header_text = f"Weekend Watchdog  ·  Sat–Sun, {sat.strftime('%b %-d')}–{sun.strftime('%-d')}"
    else:
        header_text = f"Pulse  ·  {today_d.strftime('%A, %b %-d')}"
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": header_text},
    })
    # Change M: weekend context banner
    if is_weekend:
        blocks.append({"type": "context", "elements": [
            {"type": "mrkdwn", "text": ":calendar:  Weekend mode — showing high-magnitude moves only"}
        ]})

    # ── Ghost campaigns ──────────────────────────────────────────────────────
    # ── Ghost campaigns (Change A header + Change C per-item) ───────────────
    if ghost_camps:
        blocks.append({"type": "divider"})
        impr_total = sum(g["impressions_7d"] for g in ghost_camps)
        impr_str = f"{impr_total / 1000:.0f}K" if impr_total >= 1000 else str(impr_total)
        blocks.extend(_build_signal_header(
            "🔴",
            f"DARK OFFERS — {len(ghost_camps)} active with $0 revenue",
            f"{impr_str} impressions burning, zero conversion",
        ))
        for g in ghost_camps[:5]:
            imp_str = f"{g['impressions_7d'] / 1000:.0f}K" if g["impressions_7d"] >= 1000 else str(g["impressions_7d"])
            rev_str = f"${g['revenue_7d']:,.0f}" if g.get("revenue_7d", 0) > 0 else "$0"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{g['adv_name']}*  ·  {imp_str} impressions/7d  ·  {rev_str} revenue"},
            })
        if len(ghost_camps) > 5:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"+{len(ghost_camps) - 5} more"}]})
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "Get Ghost Brief", "emoji": True},
                "action_id": "pulse_ghost_brief",
                "style": "primary",
            }],
        })

    # ── Low fill rate (Change A header + Change B per-publisher) ─────────────────────
    if fill_rate:
        blocks.append({"type": "divider"})
        total_missed = sum(f["missed_sessions"] for f in fill_rate)
        missed_str = f"{total_missed / 1_000_000:.1f}M" if total_missed >= 1_000_000 else f"{total_missed / 1000:.0f}K"
        blocks.extend(_build_signal_header(
            "🟡",
            f"LOW FILL RATE — {len(fill_rate)} publisher{'s' if len(fill_rate) != 1 else ''}",
            f"{missed_str} missed sessions/7d",
        ))
        for f in fill_rate[:4]:
            sess_str = f"{f['sessions_7d'] / 1_000_000:.1f}M" if f["sessions_7d"] >= 1_000_000 else f"{f['sessions_7d'] / 1000:.0f}K"
            rev_at_risk = f.get("revenue_at_risk", 0)
            risk_str = f"  ·  ~{_fmt_k(rev_at_risk)}/wk at risk" if rev_at_risk else ""
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{f['publisher_name']}*  ·  {f['fill_rate_pct']:.0f}% fill  ·  {sess_str} sessions/7d{risk_str}"},
            })
        if len(fill_rate) > 4:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"+{len(fill_rate) - 4} more publishers"}]})
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "Get Fill Rate Brief", "emoji": True},
                "action_id": "pulse_fill_rate_brief",
                "style": "primary",
            }],
        })

    # ── Revenue opportunities (Change A header + Change F per-opp, Mondays only) ─
    if opportunities and today_d.weekday() == 0:
        blocks.append({"type": "divider"})
        total_est = sum(o["est_monthly_rev"] for o in opportunities)
        total_str = f"${total_est / 1000:.0f}K" if total_est >= 1000 else f"${total_est:.0f}"
        blocks.extend(_build_signal_header(
            "💡",
            f"REVENUE OPPORTUNITIES — {len(opportunities)} gaps, est. {total_str}/mo",
        ))
        for o in opportunities[:3]:
            sess_str = f"{o['sessions_30d'] / 1_000_000:.1f}M" if o["sessions_30d"] >= 1_000_000 else f"{o['sessions_30d'] / 1000:.0f}K"
            est_str  = f"${o['est_monthly_rev'] / 1000:.0f}K" if o["est_monthly_rev"] >= 1000 else f"${o['est_monthly_rev']:.0f}"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{o['adv_name']}*  →  {o['publisher_name']}\n{sess_str} sessions  ·  {est_str}/mo est."},
            })
        if len(opportunities) > 3:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"+{len(opportunities) - 3} more opportunities"}]})
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "Top Opportunities →"},
                "action_id": "pulse_top_opps",
                "style": "primary",
            }],
        })

    # ── Change I: zero-signal all-clear ──────────────────────────────────
    _has_signals = bool(
        ghost_camps or fill_rate or regular_downs or urgent_caps or ups
        or (opportunities and today_d.weekday() == 0)
    )
    if not _has_signals:
        blocks.append({"type": "context", "elements": [
            {"type": "mrkdwn", "text": ":white_check_mark:  All clear — no signals above threshold today"}
        ]})

    # ── NEEDS ATTENTION (Change A header + Change D fields + G1 caps + G3 hint) ─
    if regular_downs or urgent_caps:
        blocks.append({"type": "divider"})
        blocks.extend(_build_signal_header("⬇️", "NEEDS ATTENTION", "Revenue down — action required"))
        for v, flag_count in regular_downs:
            attr = _inline_attr(v)
            pct  = float(v.get("pct_delta", 0))
            left = f"*{pct:+.0f}%*  ·  {_fmt_k(v['revenue_7d_ann'])}/mo"
            # Change J: fall back to plain section when no attribution
            if attr:
                card: dict = {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*{v['publisher_name']}*\n{left}"},
                        {"type": "mrkdwn", "text": f"*Top Advertiser*\n{attr}"},
                    ],
                }
            else:
                card = {"type": "section", "text": {"type": "mrkdwn", "text": f"*{v['publisher_name']}*\n{left}"}}
            blocks.append(card)

            # Change D + K: combined context, \n separator
            ctx_parts: list[str] = []
            if v.get("hypothesis"):
                ctx_parts.append(v["hypothesis"])
            if v.get("gaps"):
                gap_parts = [f"{adv} (${rpm:.2f} RPM)" for adv, rpm in v["gaps"]]
                ctx_parts.append(f"↳ Missing: {', '.join(gap_parts)}")
            if flag_count >= 4:
                ctx_parts.append(f"_(flagged {flag_count}d)_")
            if ctx_parts:
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": "  \n".join(ctx_parts)}],
                })

            pub_name = v["publisher_name"]
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": f"Scout offers for {pub_name}", "emoji": True},
                        "action_id": "pulse_scout_offers",
                        "value": pub_name,
                        "style": "primary",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Dig deeper", "emoji": True},
                        "action_id": "pulse_dig_in",
                        "value": pub_name,
                    },
                ],
            })

        # Change G1: urgent caps — no NBSP padding
        for a in urgent_caps[:3]:
            hit_note = f"~{a['days_to_cap']:.0f}d to cap"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{a['adv_name']}*  ·  *{a['cap_pct']}% of cap*  ·  {hit_note}"},
            })

        # Change G3: NA hint — no NBSP
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f":speech_balloon:  {na_section_hint}"}],
        })

    # ── MOMENTUM (Change A header + Change E fields + Change J empty attr) ──────
    if ups:
        blocks.append({"type": "divider"})
        blocks.extend(_build_signal_header(
            "⬆️",
            f"MOMENTUM — {len(ups)} publisher{'s' if len(ups) != 1 else ''} up",
        ))
        for v in ups[:3]:
            attr = _inline_attr(v)
            pct  = float(v.get("pct_delta", 0))
            left = f"*{pct:+.0f}%*  ·  {_fmt_k(v['revenue_7d_ann'])}/mo"
            # Change J: fall back to plain section when no attribution
            if attr:
                m_card: dict = {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*{v['publisher_name']}*\n{left}"},
                        {"type": "mrkdwn", "text": f"*Top Advertiser*\n{attr}"},
                    ],
                }
            else:
                m_card = {"type": "section", "text": {"type": "mrkdwn", "text": f"*{v['publisher_name']}*\n{left}"}}
            blocks.append(m_card)

    # ── Standing checks: routine caps + overnight + persistent down partners ───
    standing: list = []

    # Change G2: persistent down partners — no leading NBSP
    for v, flag_count in persistent_downs:
        standing.append(
            f"⚠️  *{v['publisher_name']}*  {v['pct_delta']:.0f}%  ·  {_fmt_k(v['revenue_7d_ann'])}/mo"
            f"  ·  _flagged {flag_count}d — escalate or investigate_"
        )

    if routine_caps:
        for a in routine_caps[:3]:
            hit_note = (
                f"~{a['days_to_cap']:.0f}d to cap"
                if a["days_to_cap"] < a["days_remaining"]
                else f"{a['days_remaining']}d left"
            )
            status_emoji = "🔴" if a["cap_pct"] >= 90 else "🟡"
            standing.append(f"{status_emoji}  *{a['adv_name']}* {a['cap_pct']}% of cap   {hit_note}")
    elif not urgent_caps and not is_weekend:
        standing.append("🟢  No caps at risk")

    for e in night_events[:4]:
        raw_ts = e.get("timestamp", "")
        try:
            from zoneinfo import ZoneInfo as _ZI
            _utc_dt = datetime.fromisoformat(raw_ts)
            if _utc_dt.tzinfo is None:
                _utc_dt = _utc_dt.replace(tzinfo=timezone.utc)
            ts = _utc_dt.astimezone(_ZI("America/Chicago")).strftime("%-I:%M %p CT")
        except Exception:
            ts = raw_ts[11:16] + " UTC" if len(raw_ts) >= 16 else ""
        name   = e["adv_name"] or "Unknown"
        icon   = "⏸" if e["type"] == "pause" else "▶"
        action = "paused" if e["type"] == "pause" else "resumed"
        standing.append(f"{icon}  *{name}* {action} {ts}")

    # Block count guard (precise formula) — _ALWAYS_TAIL = chronic(2) + footer(2)
    _ALWAYS_TAIL = 4
    if standing:
        if len(blocks) + 1 + len(standing) + _ALWAYS_TAIL <= 50:
            blocks.append({"type": "divider"})
            for line in standing:
                blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": line}]})
        else:
            log.warning(
                "[pulse] standing section suppressed — %d base + %d standing + %d tail = %d > 50",
                len(blocks), 1 + len(standing), _ALWAYS_TAIL,
                len(blocks) + 1 + len(standing) + _ALWAYS_TAIL,
            )

    # ── Chronic issues (P9-2) ────────────────────────────────────────────
    if chronic_set:
        chronic_names = ", ".join(sorted(chronic_set))
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f":rotating_light:  *Chronic Issues*   {chronic_names} — ongoing structural issues, tracked."}],
        })

    # ── Footer ────────────────────────────────────────────────────────────
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f":speech_balloon:  {footer_hint}   ·   :lock: Only you see slash command responses",
        }],
    })

    log.debug("[pulse] block count: %d", len(blocks))
    fallback = f"{'Weekend Watchdog' if is_weekend else 'Pulse'} — {today_label}: {len(ghost_camps)} ghost, {len(downs)} need attention, {len(ups)} in momentum."
    return fallback, blocks

def _build_home_queue_section() -> list:
    """Build queue status blocks for the App Home dashboard. Reads from disk — no network calls."""
    from datetime import datetime, timezone

    state = _load_launched_offers()
    queued = [
        (adv, entry) for adv, entry in state.items()
        if entry.get("status") == "queued"
    ]

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": ":inbox_tray: Offer Queue", "emoji": True}},
    ]

    if not queued:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":white_check_mark: Queue is clear — nothing pending entry."},
        })
        return blocks

    now = datetime.now(timezone.utc)
    for adv, entry in sorted(queued, key=lambda x: x[1].get("approved_at", ""), reverse=False):
        payout     = entry.get("payout", "")
        network    = entry.get("network", "")
        notion_url = entry.get("notion_url", "")
        approved_at = entry.get("approved_at", "")
        days_str   = ""
        if approved_at:
            try:
                approved_dt = datetime.fromisoformat(approved_at).replace(tzinfo=timezone.utc)
                days = (now - approved_dt).days
                days_str = f" · {days}d waiting"
            except Exception:
                pass
        notion_link = f" · <{notion_url}|View in Notion>" if notion_url else ""
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{adv}* — {payout} · {network}{days_str}{notion_link}"},
        })

    return blocks

def _build_home_view() -> dict:
    """
    App Home dashboard — live queue at the top, system health strip, then examples.
    Refreshed every time the user opens the App Home tab.
    """
    # ── Queue section ─────────────────────────────────────────────────────────
    blocks: list = _build_home_queue_section()

    # ── System health strip ───────────────────────────────────────────────────
    try:
        from scout_agent import _BENCHMARKS_LOADED_AT, _load_offers
        import time as _time
        age_secs = _time.time() - _BENCHMARKS_LOADED_AT if _BENCHMARKS_LOADED_AT else None
        bm_str = (f"{int(age_secs / 60)}m ago" if age_secs and age_secs < 3600
                  else (f"{int(age_secs)}s ago" if age_secs and age_secs < 120
                        else ("not loaded" if age_secs is None else f"{age_secs/3600:.1f}h ago")))
        offers_count = len(_load_offers())
        health_text = f"_Benchmarks: {bm_str}  ·  Offers: {offers_count:,}  ·  Networks: Impact · MaxBounty · FlexOffers_"
    except Exception:
        health_text = "_Networks: Impact · MaxBounty · FlexOffers · Data refreshes daily_"

    blocks += [
        {"type": "divider"},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": health_text}]},
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*Ask @Scout anything in plain English.*\n"
                    "Mention @Scout in any channel or thread. Scout remembers context within a thread.\n\n"
                    "*Slash commands — responses are only visible to you:*\n"
                    "• `/scout-pub [publisher name]` — revenue health, active offers, what to pitch\n"
                    "• `/scout-enter [advertiser or URL]` — campaign entry card for the MS platform\n"
                    "• `/scout-queue` — what's pending in the pipeline\n"
                    "• `/scout-status` — system health + data freshness\n\n"
                    ":lock: _Slash command responses are private — only you can see them. Great for quick lookups mid-call._\n\n"
                    "*Try one →*"
                ),
            },
        },
    ]

    # ── Example "Try it" buttons (unchanged) ─────────────────────────────────
    for ex in _HOME_EXAMPLES:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{ex['jtbd']}*\n{ex['description']}\n```{ex['query']}```",
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "Try it →", "emoji": False},
                "action_id": "home_try_query",
                "value":     ex["query"],
            },
        })

    blocks += [
        {"type": "divider"},
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_Networks: Impact · MaxBounty · FlexOffers · Data refreshes daily_"}],
        },
    ]

    return {"type": "home", "blocks": blocks}

