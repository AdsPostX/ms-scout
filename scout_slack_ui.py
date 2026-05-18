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
import pathlib
import random
import re

from scout_state import _load_launched_offers

try:
    from scout_ui_kit import Card, Severity, Surface, BUDGETS, enforce, ts, _KIT_ENABLED
    _KIT_AVAILABLE = True
except Exception as _e:
    logging.getLogger("scout_slack_ui").warning("scout_ui_kit import failed, disabling kit: %s", _e)
    _KIT_AVAILABLE = False
    _KIT_ENABLED = False

# Load Scout thresholds directly (avoids circular import with scout_agent).
def _load_ui_thresholds() -> dict:
    try:
        p = pathlib.Path(__file__).parent / "config" / "scout_thresholds.json"
        return json.loads(p.read_text()) if p.exists() else {}
    except Exception:
        return {}

SCOUT_THRESHOLDS = _load_ui_thresholds()

log = logging.getLogger("scout_slack_ui")

log_ref = None  # populated at import time by scout_bot

# NOTE: _SOLO_HEADER_RE is promoted to module level (was inside _text_to_blocks)
_SOLO_HEADER_RE = re.compile(r'^\*[^*]{15,}\*\s*')

# Pipe table fallback: requires ≥2 columns to avoid false-positives on single-pipe lines.
_TABLE_ROW_RE = re.compile(r'^\|(.+\|){2,}\s*$')
_TABLE_SEP_RE = re.compile(r'^\|[-:\s|]+\|?\s*$')

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


# App Home content — mobile-first activation surface.
# JTBD: get a first-timer to click and have their "magic moment" within seconds.
# Hero = the most compelling single example (call prep). Secondary = 4 quick
# tries. All CTAs render via dedicated `actions` blocks (NOT section.accessory)
# so iOS doesn't clip them; queries use inline `code` (NOT fenced ```) so
# narrow widths don't horizontal-scroll. action_ids must be unique within
# the view — see tests/test_kit_lint.py.
_HOME_HERO = {
    "jtbd":        "Prep for a publisher call",
    "description": "Full account picture — provisioned offers, what's serving, "
                   "revenue health, what to pitch.",
    "query":       "Give me a health check on TuitionHero",
    "cta":         "Health check on TuitionHero",
}

_HOME_SECONDARY = [
    {"jtbd": "Morning triage",            "query": "What happened today?"},
    {"jtbd": "Understand a revenue drop", "query": "What happened to Pinger this week?"},
    {"jtbd": "Build a campaign brief",    "query": "Build a brief for Square"},
    {"jtbd": "Find better payouts",
     "query": "Find Capital One Shopping on other networks — is there a better payout?"},
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
    - warning: 🟠 Fill rate, velocity drops, warnings
    - info: 🔵 General context, non-blocking info

    Returns list of blocks for consistent stacking.
    """
    if _KIT_AVAILABLE and _KIT_ENABLED:
        _KIT_MAP = {
            "danger": Severity.CRITICAL,
            "warning": Severity.WARN,
            "info": Severity.INFO,
        }
        kit_sev = _KIT_MAP.get(severity, Severity.INFO)
        return Card(severity=kit_sev, headline=title, body=body).render(Surface.EPHEMERAL)

    # Legacy path — active when SCOUT_KIT_ENABLED=false
    _SEVERITY_MAP = {
        "danger": {"emoji": "🔴", "label": "CRITICAL"},
        "warning": {"emoji": "🟡", "label": "WARNING"},
        "info": {"emoji": "ℹ️", "label": "INFO"},
    }
    sev = _SEVERITY_MAP.get(severity, _SEVERITY_MAP["info"])
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{sev['emoji']} *{sev['label']}:* {title}",
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

    # Primary CTA in a dedicated actions block — section.accessory buttons
    # clip on narrow iOS widths and get tap-eaten by the surrounding section.
    # See MOBILE-FIRST RULES in scout_ui_kit.py.
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
        table_buf: list = []
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

            # ── Pipe table fallback ──────────────────────────────────────────
            if _TABLE_ROW_RE.match(stripped):
                if _TABLE_SEP_RE.match(stripped):
                    continue  # skip separator rows silently
                table_buf.append(stripped)
                continue

            # Flush table_buf before processing non-table lines
            if table_buf:
                table_text = '\n'.join(table_buf)
                log.debug("[text_to_blocks] pipe table fallback triggered: %d rows", len(table_buf))
                rt_elems.append({
                    "type": "rich_text_preformatted",
                    "elements": [{"type": "text", "text": table_text}],
                })
                table_buf = []

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
        if table_buf:
            table_text = '\n'.join(table_buf)
            log.debug("[text_to_blocks] pipe table fallback triggered: %d rows", len(table_buf))
            rt_elems.append({
                "type": "rich_text_preformatted",
                "elements": [{"type": "text", "text": table_text}],
            })
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
    Adds 👎 / ✏️ feedback buttons + microcopy to Scout text responses.

    👍 was removed because no one mines positive signals operationally —
    keeping it created noise without payoff. 👎 fires an automatic retry;
    ✏️ captures a correction Scout remembers.
    """
    return [
        {
            "type": "actions",
            "elements": [
                {
                    # No style: "danger" — feedback is not destructive.
                    # Pink-styled feedback dominated the response visually
                    # and violated the MOBILE-FIRST RULE on danger usage.
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

# ── RENDERING CONTRACT ────────────────────────────────────────────────────────
# All Pulse signal rendering MUST use these primitives. Never inline Block Kit
# construction for signal headers or per-item cards — wrong patterns become
# harder than right patterns when the interface doesn't permit them.
#
# Primitives:
#   _build_signal_header(emoji, title, context="") → list[dict]
#   _build_item_card(name, left_body, right_body="", context="") → list[dict]
#   _build_action_row(buttons) → dict
#   _build_monitor_alert_blocks(emoji, title, items, cta_query) → all silent monitors + revenue tracker
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


def _build_monitor_alert_blocks(
    emoji: str,
    title: str,
    items: list[str],
    cta_query: str = "",
) -> tuple[str, list[dict]]:
    """Canonical Block Kit alert for all silent monitors and revenue tracker.

    Budget enforcement: output is capped at BUDGETS[Surface.MONITOR_ALARM] via
    enforce(); overflow gets a context indicator. Topical emoji (ghost/droplet/
    hourglass) is preserved in the header — Severity.emoji standardization is
    PR-3's job, not this surface's.
    """
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
    if _KIT_AVAILABLE and _KIT_ENABLED:
        blocks = enforce(blocks, Surface.MONITOR_ALARM)
    return fallback, blocks

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
        from datetime import datetime, timezone, date as _date
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

    # Group by status preserving canonical order
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

    # Unknown statuses (not in canonical order)
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


def _build_home_scoreboard_blocks(rollup, alerts) -> list:
    """Headline pulse + alert health line for App Home.

    `rollup` is a ScoreboardRollup (or None on data failure).
    `alerts` is a list[AlertState] of currently-firing alerts (or [] / None).

    PR 1 scope: headline revenue + two deltas + one health line. No quad
    tile, no Winners/Worry, no drill modals — see plan PR 2 for the rest.
    """
    blocks: list = []

    # ── Headline pulse ───────────────────────────────────────────────────
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
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"{d_yest} vs yesterday  ·  {d_7d} vs 7d avg",
            }],
        })

    # ── Alert health line ────────────────────────────────────────────────
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
    blocks.append({"type": "divider"})
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

    The queue lives on `/scout-queue`, not here. Status lives on
    `/scout-status`. Help lives on `/scout-help`. This view stays minimal
    so the magic-moment click is unambiguous.

    `queue_items` is accepted for backwards compatibility with bot callers
    but no longer rendered on Home.
    """
    del queue_items  # intentionally unused — queue moved off Home

    blocks: list = []

    # ── Scoreboard (PR 1 thin slice) ──────────────────────────────────────────
    # Prepended only when rollup is provided. Anonymous opens (no rollup) fall
    # through to the existing activation surface so first-timers aren't blocked
    # on a CH query failure.
    if rollup is not None or alerts is not None:
        blocks.extend(_build_home_scoreboard_blocks(rollup, alerts))

    # ── Value prop ────────────────────────────────────────────────────────────
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                "*Ask Scout anything about your publishers, advertisers, "
                "and revenue — in plain English.*\n"
                "Mention `@Scout` in any channel or thread. Scout remembers "
                "context within the thread, so you can follow up."
            ),
        },
    })
    blocks.append({"type": "divider"})

    # ── Hero example (primary CTA) ────────────────────────────────────────────
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

    # ── Secondary examples ────────────────────────────────────────────────────
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*Other things to try*"},
    })
    for idx, ex in enumerate(_HOME_SECONDARY):
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"*{ex['jtbd']}*\n`{ex['query']}`"},
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "Try →", "emoji": False},
                "action_id": f"home_try_query_{idx}",
                "value": ex["query"],
            }],
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": "Need more? Type `/scout-help` for the full command list, "
                    "or `/scout-status` for system health.",
        }],
    })

    return {"type": "home", "blocks": blocks}

