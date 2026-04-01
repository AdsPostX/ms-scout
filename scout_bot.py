"""
Scout — Slack Bot (Socket Mode)
Listens for @Scout mentions and responds with offer intelligence.
Run as a persistent background process: python scout_bot.py
"""

import json
import logging
import os
import pathlib
import random
import re
import threading
import time

import requests

from dotenv import load_dotenv
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.web import WebClient

from scout_agent import ask

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scout_bot")

BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
APP_TOKEN = os.getenv("SLACK_APP_TOKEN")

# ── Persistent brief state ────────────────────────────────────────────────────
# Briefs are written to disk so process restarts (launchd restarts, deploys, etc.)
# never cause "No brief found" on the Launch button click.
_STATE_FILE = pathlib.Path(__file__).parent / "data" / "pending_briefs.json"
_LAST_THREAD_PER_CHANNEL: dict = {}  # channel → thread_ts
_LAST_THREAD_LOCK = threading.Lock()
_BOT_USER_ID: str = ""  # cached at startup — never changes


def _atomic_write(path: pathlib.Path, data: dict) -> None:
    """Write JSON atomically — temp file + os.replace prevents partial writes on crash."""
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _load_briefs() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_briefs(briefs: dict):
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_STATE_FILE, briefs)
    except Exception as e:
        log.warning(f"Could not persist brief state: {e}")


def _store_brief(thread_ts: str, brief_data: dict, copy: dict):
    briefs = _load_briefs()
    briefs[thread_ts] = {"brief_data": brief_data, "copy": copy}
    _save_briefs(briefs)


def _get_brief(thread_ts: str) -> dict | None:
    return _load_briefs().get(thread_ts)


def _delete_brief(thread_ts: str):
    briefs = _load_briefs()
    briefs.pop(thread_ts, None)
    _save_briefs(briefs)


# ── Thread entity context (survives restarts, immune to history trimming) ─────
# Stores structured entities extracted from tool results — publisher, offer,
# payout, category, scenarios run — keyed by thread_ts.
# Injected at position 0 in history so follow-ups like "@Scout yes, $50 CPA"
# always have the entities from earlier in the thread available.

_THREAD_CTX_FILE = pathlib.Path(__file__).parent / "data" / "thread_context.json"


def _load_thread_contexts() -> dict:
    try:
        if _THREAD_CTX_FILE.exists():
            return json.loads(_THREAD_CTX_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_thread_contexts(contexts: dict):
    try:
        _THREAD_CTX_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_THREAD_CTX_FILE, contexts)
    except Exception as e:
        log.warning(f"Could not persist thread context: {e}")


def _get_thread_context(thread_ts: str) -> dict | None:
    return _load_thread_contexts().get(thread_ts)


def _merge_thread_context(thread_ts: str, new_data: dict):
    """Merge new entity data into existing thread context.
    Accumulates scenarios_run as a list — never overwrites prior values.
    """
    if not new_data:
        return
    existing = _load_thread_contexts()
    ctx = existing.get(thread_ts) or {}
    # Accumulate scenarios_run rather than overwrite
    incoming_scenarios = new_data.pop("scenarios_run", [])
    ctx.update({k: v for k, v in new_data.items() if v is not None})
    if incoming_scenarios:
        seen = ctx.get("scenarios_run") or []
        for s in incoming_scenarios:
            if s not in seen:
                seen.append(s)
        ctx["scenarios_run"] = seen
    from datetime import datetime, timezone
    ctx["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    existing[thread_ts] = ctx
    _save_thread_contexts(existing)


_LOADING_MESSAGES = [
    # Data ops
    "_Interrogating ClickHouse until it confesses..._",
    "_Bribing the affiliate networks for better rates..._",
    "_Vigorously cross-referencing things..._",
    "_Doing math so advanced it scared the last analyst..._",
    "_Asking MaxBounty to explain itself..._",
    "_Aggressively filtering out the garbage..._",
    "_Speed-running the Impact catalog..._",
    "_Telepathically downloading CVR benchmarks..._",
    "_Professionally judging low-payout offers..._",
    "_Refusing to guess and actually querying the data..._",
    "_Converting raw SQL into something humans can feel..._",
    # MomentScience flavor
    "_Turning post-transaction moments into money (allegedly)..._",
    "_Manifesting a 15% RPM lift for you specifically..._",
    "_Making the RPM go brrr..._",
    "_Finding what your publishers are leaving on the table..._",
    "_Quietly outperforming every other SDK on the confirmation page..._",
    "_Doing what the SDK does but with words..._",
    "_Treating your thank-you page like a revenue line item..._",
    "_AdOps is sleeping. Scout is not._",
    "_Making MomentScience look good, one query at a time..._",
    # Self-aware / personality
    "_One moment — Scout is having a moment..._",
    "_Thinking very hard thoughts about offer performance..._",
    "_Approximately 40% confident this will be good news..._",
    "_Running this through seven layers of analysis (three are real)..._",
    "_Lovingly nagging ClickHouse for one more row..._",
    "_Sorting by RPM, not vibes (vibes are terrible analytics)..._",
    "_Gently terrorizing the affiliate APIs..._",
    "_This is taking longer than expected, which means it's thorough..._",
    "_Consulting 47 data sources and their weird rate limits..._",
    "_Forensic accounting but for ad performance..._",
    "_Reverse-engineering what competitors are too slow to notice..._",
    "_Asking the data what it wants to be when it grows up..._",
    "_Pretending this is easy (it is not)..._",
    "_Almost done — and by 'almost' Scout means it just started..._",
]


def _rotating_status(web: WebClient, channel: str, ts: str, interval: float = 4.0):
    """
    Rotates the loading placeholder message every `interval` seconds.
    Returns a stop() callable — call it when the real response is ready.
    Each update appends elapsed time so the user knows Scout isn't frozen.
    """
    stop_event = threading.Event()
    start = time.monotonic()
    msgs = _LOADING_MESSAGES[:]
    random.shuffle(msgs)
    idx = [0]

    def _run():
        while not stop_event.wait(interval):
            elapsed = int(time.monotonic() - start)
            msg = msgs[idx[0] % len(msgs)]
            # Strip trailing _ for italic, append elapsed, re-wrap
            core = msg.strip("_")
            try:
                web.chat_update(channel=channel, ts=ts, text=f"_{core}_ · {elapsed}s")
            except Exception:
                pass
            idx[0] += 1

    threading.Thread(target=_run, daemon=True).start()
    return stop_event.set


def _strip_mention(text: str) -> str:
    """Remove @mention tokens so the agent sees the clean query."""
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()


# ── Block Kit brief builder ───────────────────────────────────────────────────

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

    # Hero image
    if hero_url and hero_url.startswith("http"):
        blocks.append({
            "type": "image",
            "image_url": hero_url,
            "alt_text": advertiser,
        })

    # Header — include MS status so decision context is instant
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
    blocks.append({"type": "section", "fields": stat_fields})

    # Risk flag — surface before copy so it's not missed
    if risk_flag:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":warning: *Fit note:* _{risk_flag}_"},
        })

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
        # Quick HEAD check — inline ✓/⚠ without blocking the render
        url_qa = ""
        try:
            import urllib.request
            req = urllib.request.Request(tracking_url, method="HEAD",
                                         headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=4) as r:
                url_qa = " _✓ resolves_" if r.status < 400 else f" _⚠ HTTP {r.status}_"
        except Exception:
            url_qa = " _⚠ did not resolve_"
        detail_parts.append(f"*Tracking URL:* `{tracking_url}`{url_qa}")
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
    context_elements = []
    if icon_url and icon_url.startswith("http"):
        context_elements.append({"type": "image", "image_url": icon_url, "alt_text": advertiser})

    footer_parts = []
    if bottom:
        footer_parts.append(f"_{bottom}_")
    # "Ready to build?" removed — Creatives field already tells you exactly what to do

    context_elements.append({"type": "mrkdwn", "text": "\n".join(footer_parts)})
    blocks.append({"type": "context", "elements": context_elements})

    # ── Add to Queue button ───────────────────────────────────────────────────
    # Only rendered when thread_ts is known (i.e., a real @Scout mention, not a preview).
    # Packs enough data in value so the handler can write the queue item without
    # re-fetching the brief — keeps the click instant.
    if thread_ts:
        cta_obj = copy.get("cta") or {}
        btn_val = json.dumps({
            "advertiser":   advertiser,
            "offer_id":     offer_id,
            "payout":       payout,
            "network":      network,
            "tracking_url": tracking_url,
            "thread_ts":    thread_ts,
            # Copy fields for Notion queue write (short keys to stay within 2900-char limit)
            "t":   (copy.get("title", ""))[:120],
            "d":   (copy.get("description", ""))[:200],
            "cy":  (cta_obj.get("yes", ""))[:60],
            "cn":  (cta_obj.get("no", ""))[:60],
            "rpm": brief_data.get("scout_score_rpm", 0),
            "pf":  (brief_data.get("performance_context", ""))[:120],
            "rf":  (brief_data.get("risk_flag", ""))[:80],
            "pt":  (brief_data.get("payout_type", "CPA"))[:10],
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


# ── Help / capabilities card ──────────────────────────────────────────────────

_HELP_TRIGGERS = {
    "help", "commands", "capabilities", "what can you do", "how do you work",
    "what do you know", "what do you do", "?", "who are you", "teach me",
    "show me what you can do", "options",
}

def _is_help_query(query: str) -> bool:
    """True if the query is asking Scout to explain itself."""
    lower = query.lower().strip()
    if lower in _HELP_TRIGGERS:
        return True
    # Short questions that are clearly meta, not about a specific offer
    if len(lower) < 30 and any(t in lower for t in ("help", "command", "capabilit", "what can", "how do")):
        return True
    return False


def _text_to_blocks(text: str) -> list:
    """
    Convert mrkdwn response text into Block Kit blocks.
    - Lines of '---' → divider block between sections
    - Lines starting with '>' → context block (gray, smaller)
    - Everything else → section block
    Falls back to a single section block if no --- separators found.
    """
    parts = re.split(r'\n\s*---\s*\n', text.strip())
    if len(parts) == 1:
        # No separators — single section block
        return [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]

    blocks = []
    for i, part in enumerate(parts):
        part = part.strip()
        if not part:
            continue
        body_lines, context_lines = [], []
        for line in part.split('\n'):
            if line.startswith('>'):
                context_lines.append(line[1:].strip())
            else:
                body_lines.append(line)
        body = '\n'.join(body_lines).strip()
        if body:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": body}})
        if context_lines:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": ' · '.join(context_lines)}],
            })
        if i < len(parts) - 1:
            blocks.append({"type": "divider"})
    return blocks or [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]


def _build_suggestion_buttons(suggestions: list) -> list:
    """Build a Slack actions block with 2-3 contextual follow-up suggestion buttons."""
    if not suggestions:
        return []
    buttons = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": s[:30], "emoji": False},
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
                    "so you can send it straight to the Demand Queue."
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


# ── SCOUT Sniper: approve / reject handlers ───────────────────────────────────

_SCOUT_HQ_CHANNEL  = "C0AQEECF800"   # #scout-hq


# ── Approve helpers ───────────────────────────────────────────────────────────

def _fetch_brief_for_approve(advertiser: str, offer_payload: dict) -> dict:
    """
    Fetch rich brief data for an approved offer.
    Tries draft_campaign_brief first (full Impact data + OG image scrape).
    Falls back to synthesizing from the offer payload if that fails.
    """
    try:
        from scout_agent import draft_campaign_brief
        result = draft_campaign_brief(advertiser)
        if result and "error" not in result:
            log.info(f"draft_campaign_brief succeeded for {advertiser}")
            return result
    except Exception as e:
        log.warning(f"draft_campaign_brief failed for {advertiser}: {e}")

    # Fallback: construct brief_data from the approve button payload
    offer_id    = str(offer_payload.get("offer_id", ""))
    payout_type = offer_payload.get("payout_type", "")
    payout_raw  = offer_payload.get("payout", "")
    try:
        payout_num = float(payout_raw)
        payout_str = f"${payout_num:,.2f} {payout_type}".strip()
    except (ValueError, TypeError):
        payout_str = payout_raw or "TBD"

    portal_url = (
        f"https://app.impact.com/secure/mediapartner/viewDetails.user?programId={offer_id}"
        if offer_id else ""
    )
    return {
        "advertiser":          advertiser,
        "network":             "Impact",
        "offer_id":            offer_id,
        "payout":              payout_str,
        "payout_type":         payout_type,
        "geo":                 offer_payload.get("geo", "US"),
        "tracking_url":        offer_payload.get("tracking_url", ""),
        "description":         offer_payload.get("description", ""),
        "category":            offer_payload.get("category", ""),
        "ms_status":           "Not in System",
        "hero_url":            "",
        "icon_url":            "",
        "portal_url":          portal_url,
        "scout_score_rpm":     0,
        "performance_context": "",
        "risk_flag":           "",
    }


def _make_copy_for_brief(brief_data: dict, offer_payload: dict) -> dict:
    """
    Build copy dict in the format _build_brief_blocks() expects.
    Derives a practical title option from the offer description.
    """
    advertiser  = brief_data.get("advertiser", "")
    description = (brief_data.get("description") or offer_payload.get("description") or "").strip()
    payout_type = brief_data.get("payout_type") or offer_payload.get("payout_type", "")
    portal_url  = brief_data.get("portal_url", "")
    network     = brief_data.get("network", "Impact")

    first_sent = description.split(".")[0].strip()[:60] if description else ""
    titles = [first_sent] if first_sent else [f"Exclusive offer from {advertiser}"]

    cta_map = {
        "CPL":        {"yes": "Get started", "no": "Maybe later"},
        "CPS":        {"yes": "Shop now", "no": "Maybe later"},
        "CPA":        {"yes": "Claim offer", "no": "Maybe later"},
        "MOBILE_APP": {"yes": "Download now", "no": "Maybe later"},
        "APP_INSTALL":{"yes": "Download now", "no": "Maybe later"},
    }
    cta = cta_map.get(payout_type, {"yes": "Learn more", "no": "Maybe later"})

    if portal_url:
        bottom_line = f"Ready to build? <{portal_url}|View on {network}> to pull creatives, then add to the MS platform."
    else:
        bottom_line = "Ready to build? Pull creatives from the network portal and add to the MS platform."

    return {
        "titles":     titles,
        "ctas":       [cta],
        "targeting":  "",
        "bottom_line": bottom_line,
    }


def _slack_thread_url(channel: str, thread_ts: str) -> str:
    """Build a direct link to a Slack thread message."""
    ts_nodot = thread_ts.replace(".", "")
    return f"https://momentscience.slack.com/archives/{channel}/p{ts_nodot}"


_LAUNCHED_OFFERS_FILE = pathlib.Path(__file__).parent / "data" / "launched_offers.json"


def _load_launched_offers() -> dict:
    try:
        if _LAUNCHED_OFFERS_FILE.exists():
            return json.loads(_LAUNCHED_OFFERS_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_launched_offers(state: dict):
    try:
        _LAUNCHED_OFFERS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_LAUNCHED_OFFERS_FILE, state)
    except Exception as e:
        log.warning(f"Could not persist launched_offers: {e}")


def _record_queued_offer(
    advertiser: str,
    brief_data: dict,
    user_id: str,
    thread_url: str,
    notion_url: str = "",
):
    """Persist approval state so the lifecycle (queue → live → notify) can close the loop.

    Stores scout_score_estimated so the 14-day recap can compare prediction vs. actual.
    This is the training signal: every validated offer becomes a calibration data point.
    """
    from datetime import datetime, timezone
    state = _load_launched_offers()
    state[advertiser] = {
        "payout":                 brief_data.get("payout", ""),
        "payout_num":             brief_data.get("payout_num", 0),
        "network":                (brief_data.get("network") or "").title(),
        "approved_by":            user_id,
        "approved_at":            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "thread_url":             thread_url,
        "notion_url":             notion_url or "",
        "status":                 "queued",
        # Snapshot the estimate at approval time — compared against actual at 14 days
        "scout_score_estimated":  brief_data.get("scout_score_rpm", 0),
        "performance_context":    brief_data.get("performance_context", ""),
        "performance_recap_sent": False,
    }
    _save_launched_offers(state)


def _write_to_notion_queue(
    brief_data: dict,
    copy_data: dict,
    user_id: str,
    thread_url: str,
) -> str | None:
    """
    Create a Notion page in the Scout Demand Queue DB.
    Properties: machine-readable filtering/sorting/Kanban data.
    Page body: human-readable MS entry checklist (Ivan Zhao principle).
    Returns the Notion page URL on success, None on failure.
    """
    from datetime import datetime, timezone

    notion_token  = os.environ.get("NOTION_TOKEN", "")
    queue_db_id   = os.environ.get("NOTION_QUEUE_DB_ID", "")
    if not notion_token or not queue_db_id:
        log.warning("Notion queue write skipped — NOTION_TOKEN or NOTION_QUEUE_DB_ID not set")
        return None

    advertiser   = brief_data.get("advertiser", "Offer")
    payout_str   = brief_data.get("payout", "")
    payout_num   = float(brief_data.get("payout_num") or 0)
    payout_type  = (brief_data.get("payout_type") or "CPA").upper()
    network      = (brief_data.get("network") or "").title()
    tracking_url = brief_data.get("tracking_url", "")
    rpm          = float(copy_data.get("rpm") or 0)
    perf_ctx     = copy_data.get("pf", "") or brief_data.get("performance_context", "")
    risk_flag    = copy_data.get("rf", "") or brief_data.get("risk_flag", "")

    title_copy  = copy_data.get("t", "") or copy_data.get("title", "")
    desc_copy   = copy_data.get("d", "") or copy_data.get("description", "")
    cta_yes     = copy_data.get("cy", "") or copy_data.get("cta_yes", "")
    cta_no      = copy_data.get("cn", "") or copy_data.get("cta_no", "")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── Copy QA checks ────────────────────────────────────────────────────────
    _PROHIBITED = ("—", "–", "™", "®")
    title_len   = len(title_copy)
    desc_len    = len(desc_copy)
    title_ok    = title_len <= 58 and not any(c in title_copy for c in _PROHIBITED)
    desc_ok     = desc_len  <= 170 and not any(c in desc_copy for c in _PROHIBITED)
    title_qa    = f"✓ {title_len} chars" if title_ok else f"⚠ {title_len} chars (max 58)" if title_len > 58 else "⚠ prohibited chars"
    desc_qa     = f"✓ {desc_len} chars" if desc_ok else f"⚠ {desc_len} chars (max 170)" if desc_len > 170 else "⚠ prohibited chars"

    # ── Properties ────────────────────────────────────────────────────────────
    page_name = f"{advertiser} — {payout_str} · {network}"

    properties = {
        "Name":           {"title": [{"text": {"content": page_name}}]},
        "Status":         {"select": {"name": "Awaiting Entry"}},
        "Network":        {"select": {"name": network}} if network else {},
        "Payout":         {"number": payout_num} if payout_num else {},
        "Payout Type":    {"select": {"name": payout_type}},
        "Scout Score RPM": {"number": rpm} if rpm else {},
        "Date Approved":  {"date": {"start": now_iso}},
        "Approved By":    {"rich_text": [{"text": {"content": user_id}}]},
        "Brief Link":     {"url": thread_url} if thread_url else {},
    }
    # Remove empty property blocks (Notion rejects empty select/number/url)
    properties = {k: v for k, v in properties.items() if v}

    # ── Page body ─────────────────────────────────────────────────────────────
    def _rt(text: str) -> dict:
        return {"object": "block", "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": text}}]}}

    def _heading(text: str, level: int = 2) -> dict:
        h = f"heading_{level}"
        return {"object": "block", "type": h,
                h: {"rich_text": [{"type": "text", "text": {"content": text}}]}}

    def _divider() -> dict:
        return {"object": "block", "type": "divider", "divider": {}}

    children = [
        _heading("Copy", 2),
        _rt(f"Headline    {title_copy}    {title_qa}"),
        _rt(f"Description    {desc_copy}    {desc_qa}"),
        _rt(f'CTA    Yes: "{cta_yes}"  /  No: "{cta_no}"'),
        _divider(),
        _heading("Offer Details", 2),
        _rt(f"Payout: {payout_str}  ·  Network: {network}  ·  Payout Type: {payout_type}"),
        _rt(f"Tracking URL: {tracking_url}" if tracking_url else "Tracking URL: pull from network portal"),
        _divider(),
        _heading("Scout Analysis", 2),
        _rt(f"Est. RPM: ${rpm:,.0f}" if rpm else "Est. RPM: N/A"),
        _rt(f"Benchmark basis: {perf_ctx}" if perf_ctx else "Benchmark basis: No MS data"),
        _rt(f"Fit note: {risk_flag}" if risk_flag else "Fit note: None flagged"),
        _divider(),
        _heading("Brief Thread", 2),
        _rt(f"Approved by: {user_id}  ·  {now_iso}"),
        {"object": "block", "type": "bookmark",
         "bookmark": {"url": thread_url, "caption": [{"type": "text", "text": {"content": "View brief thread in Slack →"}}]}}
        if thread_url else _rt("Brief thread: not available"),
    ]

    payload = {
        "parent": {"database_id": queue_db_id},
        "properties": properties,
        "children": children,
    }

    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers={
                "Authorization": f"Bearer {notion_token}",
                "Content-Type":  "application/json",
                "Notion-Version": "2022-06-28",
            },
            json=payload,
            timeout=10,
        )
        if resp.status_code == 200:
            page_id = resp.json().get("id", "").replace("-", "")
            notion_url = f"https://www.notion.so/{page_id}"
            log.info(f"Notion queue page created: {notion_url}")
            return notion_url
        else:
            log.warning(f"Notion queue write failed {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        log.warning(f"Notion queue write error: {e}")
        return None


def _update_brief_card_queued(
    web: WebClient,
    channel: str,
    message_ts: str,
    advertiser: str,
    user_id: str,
    notion_url: str | None,
) -> bool:
    """
    Update the brief card in-place: replace the 'Add to Queue' button block
    with a ⏳ Awaiting Entry context block showing who queued it and a Notion link.
    Returns True on success.
    """
    try:
        hist = web.conversations_history(
            channel=channel,
            latest=message_ts,
            limit=1,
            inclusive=True,
        )
        messages = (hist.get("messages") or [])
        if not messages:
            log.warning(f"_update_brief_card_queued: no message found at {message_ts}")
            return False

        msg = messages[0]
        blocks = list(msg.get("blocks") or [])

        # Remove the actions block that contains scout_brief_queue button
        blocks = [
            b for b in blocks
            if not (b.get("type") == "actions" and
                    any(e.get("action_id") == "scout_brief_queue"
                        for e in (b.get("elements") or [])))
        ]

        # Append queued status
        notion_link = f" · <{notion_url}|View in Notion →>" if notion_url else ""
        status_text = f":hourglass_flowing_sand: *Awaiting Entry* — queued by <@{user_id}>{notion_link}"
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": status_text}],
        })

        web.chat_update(
            channel=channel,
            ts=message_ts,
            text=f"Campaign Brief — {advertiser} · ⏳ Awaiting Entry",
            blocks=blocks,
        )
        return True
    except Exception as e:
        log.warning(f"_update_brief_card_queued failed: {e}")
        return False


def _try_add_to_demand_queue(
    web: WebClient,
    brief_data: dict,
    user_id: str,
    thread_url: str,
    copy_data: dict | None = None,
    brief_channel: str = "",
    brief_ts: str = "",
) -> str | None:
    """
    Write offer to Notion Queue DB and update the brief card in-place.
    Returns the Notion page URL on success, None otherwise.
    State is persisted to launched_offers.json by the caller (_record_queued_offer).
    """
    notion_url = _write_to_notion_queue(brief_data, copy_data or {}, user_id, thread_url)

    if brief_channel and brief_ts:
        _update_brief_card_queued(
            web, brief_channel, brief_ts,
            brief_data.get("advertiser", "Offer"),
            user_id, notion_url,
        )

    return notion_url


def _handle_approve(action: dict, payload: dict, web: WebClient):
    """
    Handle ✓ Add to Queue button click from SCOUT Sniper digest.

    One-click flow:
      1. Record approval (won't resurface in future digests)
      2. Fetch full brief via draft_campaign_brief — images, tracking URL, performance context
      3. Post rich _build_brief_blocks() card in thread (same format as @Scout briefs)
      4. Try to write item to Slack Demand Queue list (best-effort, requires lists:write scope)
      5. Post confirmation with queue link if auto-write failed
    """
    import scout_digest

    channel    = (payload.get("channel") or {}).get("id", "")
    message_ts = (payload.get("message") or {}).get("ts", "")
    user       = payload.get("user", {})
    user_id    = user.get("id", "unknown")

    try:
        offer = json.loads(action.get("value", "{}"))
    except (json.JSONDecodeError, TypeError):
        log.warning("scout_approve: could not parse action value")
        return

    offer_id   = offer.get("offer_id", "")
    advertiser = offer.get("advertiser", "")
    payout     = offer.get("payout", "")

    # 1. Persist approval — excludes from future digests
    scout_digest.record_approval(offer_id, advertiser, payout, user_id)

    # 2. Fetch full brief (OG image scrape happens inside draft_campaign_brief)
    brief_data = _fetch_brief_for_approve(advertiser, offer)
    copy       = _make_copy_for_brief(brief_data, offer)

    # 3. Build thread URL — used in Notion page body as brief reference
    thread_url = _slack_thread_url(channel, message_ts)

    # 4. Post rich brief in thread — no queue button (auto-queued immediately)
    brief_blocks = _build_brief_blocks(brief_data, copy, thread_ts="")
    brief_resp = web.chat_postMessage(
        channel=channel,
        thread_ts=message_ts,
        text=f":clipboard: Campaign brief for {advertiser}",
        blocks=brief_blocks,
        unfurl_links=False,
    )
    brief_ts = (brief_resp.get("ts") or "")
    brief_channel = channel

    # 5. Write to Notion queue + update brief card in-place with ⏳ status
    copy_data = {
        "t":   copy.get("title", ""),
        "d":   copy.get("description", ""),
        "cy":  (copy.get("cta") or {}).get("yes", ""),
        "cn":  (copy.get("cta") or {}).get("no", ""),
        "rpm": brief_data.get("scout_score_rpm", 0),
        "pf":  brief_data.get("performance_context", ""),
        "rf":  brief_data.get("risk_flag", ""),
        "pt":  brief_data.get("payout_type", "CPA"),
    }
    notion_url = _try_add_to_demand_queue(
        web, brief_data, user_id, thread_url,
        copy_data=copy_data,
        brief_channel=brief_channel,
        brief_ts=brief_ts,
    )

    # 6. Persist approval state (for lifecycle tracking + launch notification)
    _record_queued_offer(advertiser, brief_data, user_id, thread_url, notion_url=notion_url or "")

    # 7. Confirm in the digest thread
    notion_link = f" · <{notion_url}|View in Notion>" if notion_url else ""
    confirm = f":white_check_mark: *{advertiser}* added to queue by <@{user_id}>{notion_link}"
    web.chat_postMessage(channel=channel, thread_ts=message_ts, text=confirm)
    log.info(f"Approved: {advertiser} ({offer_id}) by {user_id}")


def _handle_brief_queue(action: dict, payload: dict, web: WebClient):
    """
    Handle 'Add to Queue' click from an @Scout-built brief card.

    The brief is already in Slack — we just need to:
      1. Guard against double-queueing (idempotent)
      2. Write to Slack Demand Queue list
      3. Record in launched_offers.json for lifecycle tracking
      4. Post a confirmation in-thread
    """
    try:
        data = json.loads(action.get("value", "{}"))
    except (json.JSONDecodeError, TypeError):
        log.warning("scout_brief_queue: could not parse action value")
        return

    channel    = (payload.get("container") or {}).get("channel_id") or \
                 (payload.get("channel") or {}).get("id", "")
    message_ts = (payload.get("container") or {}).get("message_ts") or \
                 (payload.get("message") or {}).get("ts", "")
    user_id    = (payload.get("user") or {}).get("id", "unknown")

    advertiser   = data.get("advertiser", "Offer")
    thread_ts    = data.get("thread_ts") or message_ts
    thread_url   = _slack_thread_url(channel, thread_ts)

    # Idempotent: don't double-queue the same advertiser
    state = _load_launched_offers()
    if advertiser in state and state[advertiser].get("status") == "queued":
        web.chat_postMessage(
            channel=channel, thread_ts=thread_ts,
            text=f":information_source: *{advertiser}* is already in the queue.",
        )
        return

    brief_data = {
        "advertiser":   advertiser,
        "payout":       data.get("payout", ""),
        "payout_num":   0,
        "network":      data.get("network", ""),
        "tracking_url": data.get("tracking_url", ""),
        "payout_type":  data.get("pt", "CPA"),
        "scout_score_rpm":    data.get("rpm", 0),
        "performance_context": data.get("pf", ""),
        "risk_flag":    data.get("rf", ""),
    }

    # Copy data (packed into button value with short keys)
    copy_data = {
        "t":   data.get("t", ""),
        "d":   data.get("d", ""),
        "cy":  data.get("cy", ""),
        "cn":  data.get("cn", ""),
        "rpm": data.get("rpm", 0),
        "pf":  data.get("pf", ""),
        "rf":  data.get("rf", ""),
        "pt":  data.get("pt", "CPA"),
    }

    # Write to Notion + update brief card in-place with ⏳ status
    notion_url = _try_add_to_demand_queue(
        web, brief_data, user_id, thread_url,
        copy_data=copy_data,
        brief_channel=channel,
        brief_ts=message_ts,
    )
    _record_queued_offer(advertiser, brief_data, user_id, thread_url, notion_url=notion_url or "")

    notion_link = f" · <{notion_url}|View in Notion>" if notion_url else ""
    confirm = f":white_check_mark: *{advertiser}* added to queue by <@{user_id}>{notion_link}"
    web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=confirm)
    log.info(f"Brief queued: {advertiser} by {user_id}")


def _handle_reject(action: dict, payload: dict, web: WebClient):
    """Handle ✕ Skip button click from SCOUT Sniper digest."""
    import scout_digest

    channel    = (payload.get("channel") or {}).get("id", "")
    message_ts = (payload.get("message") or {}).get("ts", "")
    user       = payload.get("user", {})
    user_id    = user.get("id", "unknown")

    try:
        offer = json.loads(action.get("value", "{}"))
    except (json.JSONDecodeError, TypeError):
        log.warning("scout_reject: could not parse action value")
        return

    offer_id   = offer.get("offer_id", "")
    advertiser = offer.get("advertiser", "")
    payout     = offer.get("payout", "")

    # Persist rejection — resurfaces only if payout improves ≥15%
    scout_digest.record_rejection(offer_id, advertiser, payout, user_id)

    web.chat_postMessage(
        channel=channel,
        thread_ts=message_ts,
        text=f":x: *{advertiser}* skipped by <@{user_id}>",
    )
    log.info(f"Rejected: {advertiser} ({offer_id}) by {user_id}")


def _handle_suggestion(action: dict, payload: dict, web: WebClient):
    """User clicked a suggestion button — run it as a Scout query in the same thread."""
    channel   = (payload.get("channel") or {}).get("id", "")
    msg       = payload.get("message", {})
    thread_ts = msg.get("thread_ts") or msg.get("ts", "")
    query     = action.get("value", "").strip()
    if not query or not channel or not thread_ts:
        return

    placeholder = web.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=random.choice(_LOADING_MESSAGES),
    )
    stop_rotating = _rotating_status(web, channel, placeholder["ts"])

    # Build thread history (mirrors handle_event)
    history = []
    try:
        replies = web.conversations_replies(channel=channel, ts=thread_ts, limit=50)
        bot_id  = _BOT_USER_ID
        for m in replies.get("messages", []):
            role = "assistant" if (m.get("bot_id") or m.get("user") == bot_id) else "user"
            txt  = _strip_mention(m.get("text", "")).strip()
            if txt:
                if len(txt) > 800:
                    txt = txt[:800] + "…[trimmed]"
                history.append({"role": role, "content": txt})
    except Exception as e:
        log.warning(f"suggestion handler: could not fetch history: {e}")

    _MAX_HISTORY = 20
    if len(history) > _MAX_HISTORY:
        history = history[:2] + history[-(_MAX_HISTORY - 2):]

    thread_ctx = _get_thread_context(thread_ts)
    if thread_ctx:
        parts = []
        if thread_ctx.get("publisher"):
            pub_str = thread_ctx["publisher"]
            if thread_ctx.get("publisher_id"):
                pub_str += f" (id={thread_ctx['publisher_id']})"
            parts.append(f"publisher={pub_str}")
        if thread_ctx.get("offer"):
            parts.append(f"offer={thread_ctx['offer']}")
        if thread_ctx.get("payout") is not None:
            parts.append(f"payout=${thread_ctx['payout']} {thread_ctx.get('payout_type', 'CPA')}")
        if thread_ctx.get("scenarios_run"):
            parts.append("scenarios already run: " + ", ".join(f"${s}" for s in thread_ctx["scenarios_run"]))
        if parts:
            history = [
                {"role": "user",      "content": "[Thread context: " + ", ".join(parts) + "]"},
                {"role": "assistant", "content": "Understood — I have this thread context loaded."},
            ] + history

    try:
        response = ask(query, history=history)
    except Exception as e:
        log.error(f"suggestion ask failed: {e}")
        web.chat_update(channel=channel, ts=placeholder["ts"], text=f"Something went wrong: {e}")
        return
    finally:
        stop_rotating()

    with _LAST_THREAD_LOCK:
        _LAST_THREAD_PER_CHANNEL[channel] = thread_ts

    if isinstance(response, dict) and response.get("type") == "brief":
        brief_data = response["brief_data"]
        copy       = response["copy"]
        _store_brief(thread_ts, brief_data, copy)
        _merge_thread_context(thread_ts, {
            "offer":       brief_data.get("advertiser"),
            "payout":      brief_data.get("payout_num"),
            "payout_type": (brief_data.get("payout_type") or "CPA").upper(),
        })
        blocks = _build_brief_blocks(brief_data, copy, thread_ts=thread_ts)
        web.chat_update(channel=channel, ts=placeholder["ts"],
                        text=response.get("fallback_text", "Campaign Brief ready."), blocks=blocks)
        return

    sugg: list = []
    launched_offer_sg: dict | None = None
    if isinstance(response, dict) and response.get("type") == "text_with_context":
        extracted = response.get("extracted_context", {})
        if extracted:
            launched_offer_sg = extracted.pop("launched_offer", None)
            _merge_thread_context(thread_ts, extracted)
        sugg = response.get("suggestions", [])
        response_text = response["text"]
    else:
        response_text = response if isinstance(response, str) else str(response)

    # Launch notification (same logic as handle_event)
    if launched_offer_sg:
        adops_uid   = os.getenv("ADOPS_NOTIFY_USER_ID", "")
        approved_by = launched_offer_sg.get("approved_by", "")
        advertiser  = launched_offer_sg.get("advertiser", "")
        payout      = launched_offer_sg.get("payout", "")
        network     = launched_offer_sg.get("network", "")
        t_url       = launched_offer_sg.get("thread_url", "")
        tags = f"<@{approved_by}>" if approved_by else ""
        if adops_uid and adops_uid != approved_by:
            tags += f" <@{adops_uid}>"
        brief_link = f" · <{t_url}|brief>" if t_url else ""
        msg = f":rocket: *{advertiser}* is live. {payout} · {network}{brief_link}"
        if tags:
            msg += f"\n{tags}"
        web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)

    suggestion_blocks = _build_suggestion_buttons(sugg)
    if suggestion_blocks:
        web.chat_update(
            channel=channel, ts=placeholder["ts"], text=response_text,
            blocks=[
                {"type": "section", "text": {"type": "mrkdwn", "text": response_text}},
                *suggestion_blocks,
            ],
        )
    else:
        web.chat_update(channel=channel, ts=placeholder["ts"], text=response_text, mrkdwn=True)
    log.info(f"Suggestion answered in {channel} (thread {thread_ts}): {query!r}")


# ── Interactive (button click) handler ───────────────────────────────────────

def _handle_block_action(req: SocketModeRequest, web: WebClient):
    """Handle Slack interactive button clicks (block_actions)."""
    payload = req.payload
    if payload.get("type") != "block_actions":
        return

    actions = payload.get("actions", [])
    if not actions:
        return

    action    = actions[0]
    action_id = action.get("action_id", "")
    channel   = (payload.get("channel") or {}).get("id", "")

    log.info(f"Block action: {action_id!r} in {channel}")

    # ── Suggestion button clicks ──────────────────────────────────────────────
    if action_id.startswith("scout_suggestion"):
        _handle_suggestion(action, payload, web)
        return

    # ── SCOUT Sniper digest actions ───────────────────────────────────────────
    if action_id == "scout_approve":
        _handle_approve(action, payload, web)
        return
    if action_id == "scout_reject":
        _handle_reject(action, payload, web)
        return
    # ── Brief "Add to Queue" button (from @Scout build a brief for X) ─────────
    if action_id == "scout_brief_queue":
        _handle_brief_queue(action, payload, web)
        return

    # ── App Home "Try it" buttons ─────────────────────────────────────────────
    if action_id == "home_try_query":
        user_id = payload.get("user", {}).get("id", "")
        query   = action.get("value", "").strip()
        if user_id and query:
            _handle_home_try_query(web, user_id, query)
        return


# ── App Home tutorial ─────────────────────────────────────────────────────────

# Five real, working queries organized by JTBD.
# Values are real advertisers/partners confirmed in the MS platform.
_HOME_EXAMPLES = [
    {
        "jtbd":        "Prep for a publisher call",
        "description": "Get the full account picture: every provisioned offer, which are serving, what to pitch.",
        "query":       "What's provisioned and running for Constant Contact (partner 6103)?",
    },
    {
        "jtbd":        "Find better payouts for an advertiser we're already running",
        "description": "Check if Capital One Shopping exists on MaxBounty or FlexOffers at a higher rate.",
        "query":       "Find Capital One Shopping on other networks — is there a better payout?",
    },
    {
        "jtbd":        "Build a campaign brief",
        "description": "Get campaign-ready copy, tracking URL, and RPM estimate. One click to add to the queue.",
        "query":       "Build a brief for Square",
    },
    {
        "jtbd":        "Browse top offers in a vertical",
        "description": "Find the highest Scout Score Finance offers available right now.",
        "query":       "What are the top Finance offers by Scout Score?",
    },
    {
        "jtbd":        "Check the demand pipeline",
        "description": "See what's pending approval or waiting to go live.",
        "query":       "What's in the demand queue?",
    },
]


def _build_home_view() -> dict:
    """Build the Slack App Home tab — persistent tutorial + interactive examples."""
    blocks: list = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Scout — MomentScience Offer Intelligence", "emoji": False},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "Mention *@Scout* in any channel and ask in plain English. "
                    "Scout has access to the full offer inventory across Impact, MaxBounty, and FlexOffers "
                    "— plus live performance data and publisher account details from the MS platform.\n\n"
                    "*New here? Click any* *→ Try it* *button below to see a real answer in your DMs.*"
                ),
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*5 things you can do right now*"},
        },
    ]

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
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*How Scout works*\n"
                    "• Ask anything in plain English — no special syntax or commands\n"
                    "• Scout remembers context *within a thread* — ask follow-ups naturally\n"
                    "• After any result, Scout surfaces suggested next steps as clickable buttons\n"
                    "• Type `@Scout help` from any channel for a quick reference card\n"
                    "• Data: offer inventory refreshes daily · performance data is live from MS"
                ),
            },
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "_Networks: Impact · MaxBounty · FlexOffers · Publisher data from the MS platform_",
            }],
        },
    ]

    return {"type": "home", "blocks": blocks}


def _handle_home_try_query(web: WebClient, user_id: str, query: str):
    """
    Execute an example query from App Home.
    Opens a DM with the user, posts the query for context, then runs it through Scout.
    The user sees a real answer — learns by doing, not by reading docs.
    """
    try:
        dm_channel = web.conversations_open(users=[user_id])["channel"]["id"]

        # Post the query as context so the user knows what was asked
        intro = web.chat_postMessage(
            channel=dm_channel,
            text=f"Try it: {query}",
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Example query:*\n_{query}_"},
            }],
        )
        thread_ts = intro["ts"]

        placeholder = web.chat_postMessage(
            channel=dm_channel,
            thread_ts=thread_ts,
            text=random.choice(_LOADING_MESSAGES),
        )
        stop_rotating = _rotating_status(web, dm_channel, placeholder["ts"])

        try:
            response = ask(query)
        finally:
            stop_rotating()

        if isinstance(response, dict) and response.get("type") == "brief":
            brief_data = response["brief_data"]
            copy       = response["copy"]
            blocks     = _build_brief_blocks(brief_data, copy, thread_ts=thread_ts)
            web.chat_update(
                channel=dm_channel, ts=placeholder["ts"],
                text="Campaign Brief", blocks=blocks,
            )
        elif isinstance(response, dict) and response.get("type") == "text_with_context":
            response_text    = response.get("text", "")
            suggestions      = response.get("suggestions", [])
            content_blocks   = _text_to_blocks(response_text)
            suggestion_blocks = _build_suggestion_buttons(suggestions)
            web.chat_update(
                channel=dm_channel, ts=placeholder["ts"],
                text=response_text,
                blocks=[*content_blocks, *suggestion_blocks],
            )
        else:
            response_text = response if isinstance(response, str) else str(response)
            web.chat_update(
                channel=dm_channel, ts=placeholder["ts"],
                text=response_text,
                blocks=_text_to_blocks(response_text),
            )
        log.info(f"App Home try-it: ran '{query[:50]}' for {user_id}")
    except Exception as e:
        log.warning(f"_handle_home_try_query failed for {user_id}: {e}")


# ── Main event handler ────────────────────────────────────────────────────────

def handle_event(client: SocketModeClient, req: SocketModeRequest):
    # Acknowledge immediately — Slack requires <3s ack
    client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))

    web = WebClient(token=BOT_TOKEN)

    # ── Button clicks ─────────────────────────────────────────────────────────
    if req.type == "interactive":
        _handle_block_action(req, web)
        return

    if req.type != "events_api":
        return

    event = req.payload.get("event", {})

    # ── App Home tab opened ───────────────────────────────────────────────────
    if event.get("type") == "app_home_opened":
        user_id = event.get("user", "")
        if user_id:
            try:
                web.views_publish(user_id=user_id, view=_build_home_view())
            except Exception as e:
                log.warning(f"app_home_opened: views_publish failed: {e}")
        return

    if event.get("type") != "app_mention":
        return

    # Skip bot's own messages
    if event.get("bot_id"):
        return

    channel  = event.get("channel")
    msg_ts   = event.get("ts")
    thread_ts = event.get("thread_ts") or msg_ts
    raw_text = event.get("text", "")
    query    = _strip_mention(raw_text)

    if not query:
        return

    log.info(f"Query from {event.get('user')}: {query!r}")

    lower = query.lower()

    # ── Special commands (handled before agent) ───────────────────────────────

    # Help / capabilities discovery — no need to spin up the agent for this
    if _is_help_query(query):
        web.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text="Here's what Scout can help with:",
            blocks=_build_help_blocks(),
        )
        return

    # "launch this", "launch it", etc. — redirect to the Approve button flow
    if re.search(r"\blaunch\b", lower) and not re.search(r"\bbuild\b|\bcreate\b|\bbrief\b", lower):
        pending = _get_brief(thread_ts)
        if pending:
            msg = "Brief is ready — click *Approve* in the card above to add it to the queue."
        else:
            msg = "No brief here yet. Ask me to build one: `@Scout build a brief for [offer]`"
        web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)
        return

    # ── Thread history for context ────────────────────────────────────────────
    history = []
    is_thread_reply = event.get("thread_ts") and event.get("thread_ts") != msg_ts
    # For top-level messages (no thread), check if there's a recent active thread
    # in this channel so "yes" / "do that" follow-ups retain context.
    with _LAST_THREAD_LOCK:
        _last_thread = _LAST_THREAD_PER_CHANNEL.get(channel)
    effective_thread_ts = thread_ts if is_thread_reply else _last_thread

    if effective_thread_ts:
        try:
            replies = web.conversations_replies(channel=channel, ts=effective_thread_ts, limit=50)
            bot_id  = _BOT_USER_ID
            for msg in replies.get("messages", []):
                if msg.get("ts") == msg_ts:
                    break
                role = "assistant" if (msg.get("bot_id") or msg.get("user") == bot_id) else "user"
                text = _strip_mention(msg.get("text", "")).strip()
                if not text:
                    continue
                # Trim long messages (especially Scout's own verbose responses) to keep
                # context lean — the key signal is the last few turns, not every word.
                if len(text) > 800:
                    text = text[:800] + "…[trimmed]"
                history.append({"role": role, "content": text})
        except Exception as e:
            log.warning(f"Could not fetch thread history: {e}")

    # Smart trim: keep first 2 + last 18 messages — drops verbose middle content
    # (Scout's own long responses) without losing the opening context or recent turns.
    # Context block is injected AFTER this trim so it always lands at position 0.
    _MAX_HISTORY = 20
    if len(history) > _MAX_HISTORY:
        history = history[:2] + history[-(_MAX_HISTORY - 2):]

    # Inject persisted thread entities at position 0 — immune to trimming.
    # Resolves follow-ups like "@Scout yes, $50 CPA" without restating publisher/offer.
    thread_ctx = _get_thread_context(thread_ts)
    if thread_ctx:
        parts = []
        if thread_ctx.get("publisher"):
            pub_str = thread_ctx["publisher"]
            if thread_ctx.get("publisher_id"):
                pub_str += f" (id={thread_ctx['publisher_id']})"
            parts.append(f"publisher={pub_str}")
        if thread_ctx.get("offer"):
            parts.append(f"offer={thread_ctx['offer']}")
        if thread_ctx.get("payout") is not None:
            parts.append(f"payout=${thread_ctx['payout']} {thread_ctx.get('payout_type', 'CPA')}")
        if thread_ctx.get("category"):
            parts.append(f"category={thread_ctx['category']}")
        if thread_ctx.get("scenarios_run"):
            scens = ", ".join(f"${s}" for s in thread_ctx["scenarios_run"])
            parts.append(f"scenarios already run: {scens}")
        if parts:
            ctx_line = "[Thread context: " + ", ".join(parts) + "]"
            history = [
                {"role": "user",      "content": ctx_line},
                {"role": "assistant", "content": "Understood — I have this thread context loaded."},
            ] + history
            log.info(f"Injected thread context for {thread_ts}: {ctx_line}")

    # Post placeholder — rotates + shows elapsed time while ask() runs
    placeholder = web.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=random.choice(_LOADING_MESSAGES),
    )
    stop_rotating = _rotating_status(web, channel, placeholder["ts"])

    try:
        response = ask(query, history=history)
    except Exception as e:
        log.error(f"Agent error: {e}")
        response = f"Something went wrong: {e}"
    finally:
        stop_rotating()

    # ── Route response: brief (Block Kit) vs text_with_context vs plain text ────
    # Track the active thread per channel so top-level follow-ups retain context
    with _LAST_THREAD_LOCK:
        _LAST_THREAD_PER_CHANNEL[channel] = thread_ts

    # Block B: extract entities + suggestions from text_with_context responses
    suggestions: list = []
    launched_offer: dict | None = None
    if isinstance(response, dict) and response.get("type") == "text_with_context":
        extracted = response.get("extracted_context", {})
        if extracted:
            launched_offer = extracted.pop("launched_offer", None)
            _merge_thread_context(thread_ts, extracted)
            log.info(f"Saved thread context for {thread_ts}: {list(extracted.keys())}")
        suggestions = response.get("suggestions", [])
        response = response["text"]

    # Launch notification — thread-only, targeted tags, no channel noise
    if launched_offer:
        adops_uid   = os.getenv("ADOPS_NOTIFY_USER_ID", "")
        approved_by = launched_offer.get("approved_by", "")
        advertiser  = launched_offer.get("advertiser", "")
        payout      = launched_offer.get("payout", "")
        network     = launched_offer.get("network", "")
        t_url       = launched_offer.get("thread_url", "")

        tags = f"<@{approved_by}>" if approved_by else ""
        if adops_uid and adops_uid != approved_by:
            tags += f" <@{adops_uid}>"

        brief_link = f" · <{t_url}|brief>" if t_url else ""
        msg = f":rocket: *{advertiser}* is live. {payout} · {network}{brief_link}"
        if tags:
            msg += f"\n{tags}"
        web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)

    if isinstance(response, dict) and response.get("type") == "brief":
        brief_data = response["brief_data"]
        copy       = response["copy"]

        # Persist brief to disk — survives Scout restarts and dual-instance races
        _store_brief(thread_ts, brief_data, copy)
        log.info(f"Stored pending brief for {brief_data.get('advertiser')} in thread {thread_ts}")

        # Save offer/payout entities from brief so follow-up "@Scout launch at $X" works
        _merge_thread_context(thread_ts, {
            "offer":       brief_data.get("advertiser"),
            "payout":      brief_data.get("payout_num"),
            "payout_type": (brief_data.get("payout_type") or "CPA").upper(),
        })

        blocks        = _build_brief_blocks(brief_data, copy, thread_ts=thread_ts)
        fallback_text = response.get("fallback_text", "Campaign Brief ready.")

        web.chat_update(
            channel=channel,
            ts=placeholder["ts"],
            text=fallback_text,
            blocks=blocks,
        )
        log.info(f"Posted Block Kit brief for {brief_data.get('advertiser')} in {channel}")

    else:
        # Plain text response — with optional suggestion buttons
        response_text = response if isinstance(response, str) else str(response)
        content_blocks = _text_to_blocks(response_text)
        suggestion_blocks = _build_suggestion_buttons(suggestions)
        web.chat_update(
            channel=channel,
            ts=placeholder["ts"],
            text=response_text,  # fallback for notifications
            blocks=[*content_blocks, *suggestion_blocks],
        )
        log.info(f"Responded in {channel} (thread {thread_ts}), suggestions={len(suggestions)}")


def _check_stale_queue(web: WebClient) -> None:
    """
    Daily background check: any offer approved 7+ days ago with status 'queued'
    (never went live) gets a Slack nudge in the original approval thread.

    Runs every 24h via a daemon thread started in main(). Silent on failures
    so a bad entry never crashes the bot.
    """
    STALE_DAYS = 7
    while True:
        try:
            time.sleep(86_400)  # 24 hours
            from datetime import datetime, timezone
            state = _load_launched_offers()
            now = datetime.now(timezone.utc)
            for advertiser, entry in state.items():
                if entry.get("status") != "queued":
                    continue
                approved_at_str = entry.get("approved_at", "")
                if not approved_at_str:
                    continue
                try:
                    approved_at = datetime.fromisoformat(approved_at_str).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                age_days = (now - approved_at).days
                if age_days < STALE_DAYS:
                    continue

                thread_url = entry.get("thread_url", "")
                payout     = entry.get("payout", "")
                network    = entry.get("network", "")

                # Find the original approval channel + thread from the URL
                # thread_url format: https://momentscience.slack.com/archives/C.../p...
                import re as _re
                m = _re.search(r'/archives/([A-Z0-9]+)/p(\d+)', thread_url)
                if not m:
                    log.debug(f"Stale queue: can't parse thread URL for {advertiser}")
                    continue

                channel   = m.group(1)
                ts_raw    = m.group(2)
                thread_ts = f"{ts_raw[:10]}.{ts_raw[10:]}"

                msg = (
                    f":hourglass: *{advertiser}* has been in the queue for *{age_days} days* "
                    f"({payout} · {network}) with no impressions detected.\n"
                    f"Still in progress? Reply to confirm — or Reject to free the slot."
                )
                web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)
                log.info(f"Stale queue nudge sent for {advertiser} ({age_days} days)")

        except Exception as e:
            log.error(f"[stale_queue] cycle failed, will retry in 24h: {e}", exc_info=True)


def _performance_recap(web: WebClient) -> None:
    """
    14-day post-launch performance recap daemon.

    Runs daily. For every queued/live offer where:
      - approved_at is 14+ days ago
      - performance_recap_sent is False

    Pulls actual RPM from ClickHouse, compares to Scout's estimate at approval time,
    posts a 3-line recap in the original approval thread, marks recap as sent.

    This is the feedback loop that makes the model legible:
      Scout estimated $X → actual came in at $Y (+/-Z%)
    The ClickHouse benchmarks already self-improve as offers accumulate data.
    This thread post makes that improvement visible and builds team trust.
    """
    RECAP_DAYS = 14

    while True:
        try:
            time.sleep(86_400)
            from datetime import datetime, timezone
            from scout_agent import _get_ch_client
            state   = _load_launched_offers()
            now     = datetime.now(timezone.utc)
            updated = False

            for advertiser, entry in list(state.items()):
                if entry.get("performance_recap_sent"):
                    continue
                approved_at_str = entry.get("approved_at", "")
                if not approved_at_str:
                    continue
                try:
                    approved_at = datetime.fromisoformat(approved_at_str).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if (now - approved_at).days < RECAP_DAYS:
                    continue

                thread_url = entry.get("thread_url", "")
                import re as _re
                m = _re.search(r'/archives/([A-Z0-9]+)/p(\d+)', thread_url)
                if not m:
                    continue
                channel   = m.group(1)
                ts_raw    = m.group(2)
                thread_ts = f"{ts_raw[:10]}.{ts_raw[10:]}"

                # Pull actual impressions + revenue since approval date
                try:
                    ch = _get_ch_client()
                    q  = """
                    SELECT
                        count()                          AS impressions,
                        sum(toFloat64OrNull(revenue))    AS total_revenue
                    FROM default.adpx_conversionsdetails conv
                    JOIN default.mv_adpx_campaigns c
                      ON toInt64(conv.campaign_id) = toInt64(c.id)
                    WHERE c.adv_name ILIKE {adv_pattern:String}
                      AND conv.created_at >= toDateTime({approved_at_str:String})
                      AND toYYYYMM(conv.created_at) >= toYYYYMM(toDate({approved_at_date:String}))
                    """
                    rows = ch.query(q, parameters={
                        "adv_pattern":      f"%{advertiser}%",
                        "approved_at_str":  approved_at_str,
                        "approved_at_date": approved_at_str[:10],
                    }).result_rows
                    impressions   = int((rows[0][0] if rows else 0) or 0)
                    total_revenue = float((rows[0][1] if rows else 0) or 0)
                except Exception as ch_err:
                    log.warning(f"Recap ClickHouse query failed for {advertiser}: {ch_err}")
                    continue

                # Build the recap message
                estimated = entry.get("scout_score_estimated", 0)
                payout    = entry.get("payout", "")
                network   = entry.get("network", "")

                if impressions < 100:
                    # Not enough data yet — skip, will catch next cycle
                    log.info(f"Recap skipped for {advertiser}: only {impressions} impressions at 14d")
                    continue

                actual_rpm = round(total_revenue / impressions * 1000, 0) if impressions else 0

                if estimated and actual_rpm:
                    delta_pct = round((actual_rpm - estimated) / estimated * 100)
                    direction = f"+{delta_pct}%" if delta_pct >= 0 else f"{delta_pct}%"
                    accuracy  = "on the money" if abs(delta_pct) <= 15 else ("above estimate" if delta_pct > 0 else "below estimate")
                    score_line = f"Scout estimated *${estimated:,.0f} RPM* → actual *${actual_rpm:,.0f} RPM* ({direction}, {accuracy})"
                elif actual_rpm:
                    score_line = f"Actual RPM at 14 days: *${actual_rpm:,.0f}* ({impressions:,} impressions)"
                else:
                    score_line = f"No conversions detected at 14 days ({impressions:,} impressions)"

                msg = (
                    f":bar_chart: *{advertiser}* — 14-day performance recap\n"
                    f"{score_line}\n"
                    f"_{payout} · {network} · {impressions:,} impressions_"
                )
                web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)

                # Mark sent — won't re-post
                state[advertiser]["performance_recap_sent"] = True
                state[advertiser]["actual_rpm_14d"]         = actual_rpm
                state[advertiser]["impressions_14d"]        = impressions
                updated = True
                log.info(f"14-day recap sent for {advertiser}: est=${estimated} actual=${actual_rpm}")

            if updated:
                _save_launched_offers(state)

        except Exception as e:
            log.error(f"[performance_recap] cycle failed, will retry in 24h: {e}", exc_info=True)


def _cleanup_state() -> None:
    """
    Nightly cleanup of state files to prevent unbounded growth.
    - pending_briefs.json: drop entries > 30 days old
    - thread_context.json: keep last 500 by last_updated
    - launched_offers.json: keep all (lifecycle data, small)
    - digest_state.json: keep approved forever, drop rejected > 90 days
    Also called once at startup to recover from accumulated debt.
    """
    from datetime import datetime, timezone, timedelta

    def _parse_ts_age(ts_str: str, now: datetime) -> int:
        """Return age in days of a Slack thread timestamp (epoch.microseconds format)."""
        try:
            epoch = float(ts_str.replace(".", "")[:10])
            created = datetime.fromtimestamp(epoch, tz=timezone.utc)
            return (now - created).days
        except Exception:
            return 0  # unknown age → keep

    def _run_cleanup():
        now = datetime.now(timezone.utc)

        # 1. pending_briefs.json — drop entries older than 30 days
        try:
            briefs = _load_briefs()
            pruned_briefs = {
                ts: data for ts, data in briefs.items()
                if _parse_ts_age(ts, now) < 30
            }
            if len(pruned_briefs) < len(briefs):
                _atomic_write(_STATE_FILE, pruned_briefs)
                log.info(f"Cleanup: pruned {len(briefs) - len(pruned_briefs)} old brief entries")
        except Exception as e:
            log.warning(f"Cleanup: briefs prune failed: {e}")

        # 2. thread_context.json — LRU eviction, keep last 500
        try:
            ctx = _load_thread_contexts()
            if len(ctx) > 500:
                sorted_keys = sorted(
                    ctx.keys(),
                    key=lambda k: ctx[k].get("last_updated", ""),
                    reverse=True,
                )
                pruned_ctx = {k: ctx[k] for k in sorted_keys[:500]}
                _atomic_write(_THREAD_CTX_FILE, pruned_ctx)
                log.info(f"Cleanup: evicted {len(ctx) - 500} old thread contexts")
        except Exception as e:
            log.warning(f"Cleanup: thread context eviction failed: {e}")

        # 3. digest_state.json — keep approved forever, drop rejected > 90 days
        try:
            digest_state_path = pathlib.Path(__file__).parent / "data" / "digest_state.json"
            if digest_state_path.exists():
                state = json.loads(digest_state_path.read_text())
                rejected = state.get("rejected", {})
                cutoff_90 = (now - timedelta(days=90)).strftime("%Y-%m-%d")
                pruned_rejected = {
                    k: v for k, v in rejected.items()
                    if v.get("actioned_at", "9999") > cutoff_90
                }
                if len(pruned_rejected) < len(rejected):
                    state["rejected"] = pruned_rejected
                    _atomic_write(digest_state_path, state)
                    log.info(f"Cleanup: pruned {len(rejected) - len(pruned_rejected)} old rejections")
        except Exception as e:
            log.warning(f"Cleanup: digest state prune failed: {e}")

    # Run once at startup
    _run_cleanup()

    # Then nightly
    while True:
        try:
            time.sleep(86_400)
            _run_cleanup()
        except Exception as e:
            log.error(f"[cleanup_state] cycle failed: {e}", exc_info=True)


def main():
    global _BOT_USER_ID
    if not BOT_TOKEN or not APP_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env")

    web_client    = WebClient(token=BOT_TOKEN)
    _BOT_USER_ID  = web_client.auth_test()["user_id"]
    socket_client = SocketModeClient(app_token=APP_TOKEN, web_client=web_client)
    socket_client.socket_mode_request_listeners.append(handle_event)

    # Background: daily stale queue alerts (daemon thread dies cleanly on exit)
    threading.Thread(target=_check_stale_queue, args=(web_client,), daemon=True).start()
    # Background: 14-day performance recap — compares Scout estimates to actual ClickHouse RPM
    threading.Thread(target=_performance_recap, args=(web_client,), daemon=True).start()
    # Background: nightly cleanup of state files to prevent unbounded growth
    threading.Thread(target=_cleanup_state, daemon=True).start()

    log.info("Scout is online — listening for @mentions via Socket Mode")
    socket_client.connect()

    import signal
    signal.pause()


if __name__ == "__main__":
    main()
