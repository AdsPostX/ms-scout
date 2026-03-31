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
        _STATE_FILE.write_text(json.dumps(briefs, indent=2))
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
        _THREAD_CTX_FILE.write_text(json.dumps(contexts, indent=2))
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
    from datetime import datetime
    ctx["last_updated"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    existing[thread_ts] = ctx
    _save_thread_contexts(existing)


_LOADING_MESSAGES = [
    "_Digging through 700+ offers..._",
    "_Checking if anyone's already running this..._",
    "_Asking ClickHouse nicely..._",
    "_Cross-referencing payout data..._",
    "_Doing the math so you don't have to..._",
    "_Scanning the networks..._",
    "_Checking what AT&T is serving right now..._",
    "_Pulling real CVR data..._",
    "_Sorting by RPM, not vibes..._",
    "_One sec — running the numbers..._",
    "_Consulting the oracle (ClickHouse)..._",
    "_Finding what AdOps should look at this week..._",
]


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

    titles    = copy.get("titles", [])
    ctas      = copy.get("ctas", [])
    targeting = copy.get("targeting", "")
    bottom    = copy.get("bottom_line", "")

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
    # RPM confidence: only show a number when it's grounded in real MS data
    # or a category benchmark. Never show a bare estimate as if it's a fact.
    has_real_data    = performance and "Real MS data" in performance
    has_cat_benchmark = performance and "benchmark" in performance
    if score_rpm and has_real_data:
        rpm_display = f"${score_rpm:,.0f}"
    elif score_rpm and has_cat_benchmark:
        rpm_display = f"~${score_rpm:,.0f} est."
    elif score_rpm:
        rpm_display = f"~${score_rpm:,.0f} est.\n_no prior data_"
    else:
        rpm_display = "N/A"

    stat_fields = [
        {"type": "mrkdwn", "text": f"*Network*\n{network}"},
        {"type": "mrkdwn", "text": f"*Payout*\n{payout}"},
        {"type": "mrkdwn", "text": f"*Geo*\n{geo or 'Not specified'}"},
        {"type": "mrkdwn", "text": f"*Est. RPM*\n{rpm_display}"},
    ]
    if performance:
        stat_fields.append({"type": "mrkdwn", "text": f"*Performance*\n{performance}"})
    blocks.append({"type": "section", "fields": stat_fields})

    # Risk flag — surface before copy so it's not missed
    if risk_flag:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":warning: *Fit note:* _{risk_flag}_"},
        })

    blocks.append({"type": "divider"})

    # ── Copy ─────────────────────────────────────────────────────────────────
    if titles:
        title_lines = "\n".join(f"{i+1}. {t}" for i, t in enumerate(titles))
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Title options:*\n{title_lines}"},
        })

    if ctas:
        cta_lines = "\n".join(f'• Yes: "{c.get("yes","")}" / No: "{c.get("no","")}"' for c in ctas)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*CTA options:*\n{cta_lines}"},
        })

    # ── Details ───────────────────────────────────────────────────────────────
    detail_parts = []
    if targeting:
        detail_parts.append(f"*Targeting:* {targeting}")
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
    context_elements = []
    if icon_url and icon_url.startswith("http"):
        context_elements.append({"type": "image", "image_url": icon_url, "alt_text": advertiser})

    footer_parts = []
    if bottom:
        footer_parts.append(f"_{bottom}_")

    # Clean handoff line — no automation promise, just the next step
    if portal_url:
        footer_parts.append(f"Ready to build? <{portal_url}|Open in {network}> to pull creatives, then add to the MS platform.")
    else:
        footer_parts.append("Ready to build? Pull creatives from the network portal and add to the MS platform.")

    context_elements.append({"type": "mrkdwn", "text": "\n".join(footer_parts)})
    blocks.append({"type": "context", "elements": context_elements})

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

QUEUE_LIST_URL = "https://momentscience.slack.com/lists/T03Q93Q96UD/F07QCKFP0RM"
_DEMAND_QUEUE_LIST_ID = "F07QCKFP0RM"


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
        _LAUNCHED_OFFERS_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        log.warning(f"Could not persist launched_offers: {e}")


def _record_queued_offer(advertiser: str, brief_data: dict, user_id: str, thread_url: str):
    """Persist approval state so the lifecycle (queue → live → notify) can close the loop."""
    from datetime import datetime, timezone
    state = _load_launched_offers()
    state[advertiser] = {
        "payout":      brief_data.get("payout", ""),
        "network":     (brief_data.get("network") or "").title(),
        "approved_by": user_id,
        "approved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "thread_url":  thread_url,
        "status":      "queued",
    }
    _save_launched_offers(state)


def _try_add_to_demand_queue(
    web: WebClient,
    brief_data: dict,
    user_id: str,
    thread_url: str,
) -> bool:
    """
    Write an approved offer to the Demand Queue Slack List.
    Queue item name includes the direct thread link so AdOps has the full brief
    without hunting Slack — no canvas, no copy-paste required.
    Returns True if successful, False on any error (graceful fallback).
    """
    advertiser = brief_data.get("advertiser", "Unknown")
    payout     = brief_data.get("payout", "")
    network    = (brief_data.get("network") or "").title()

    item_name = f"{advertiser} — {payout} · {network} | Brief: {thread_url}"
    try:
        resp = web.api_call(
            "lists.items.add",
            json={
                "list_id": _DEMAND_QUEUE_LIST_ID,
                "item": {"name": item_name},
            },
        )
        if resp.get("ok"):
            log.info(f"Demand Queue: added '{advertiser}'")
            return True
        log.warning(f"lists.items.add not-ok: {resp.get('error', 'unknown')}")
    except Exception as e:
        log.warning(f"lists.items.add failed (scope missing?): {e}")
    return False


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

    # 3. Post rich brief in thread (same Block Kit as @Scout "build a brief" flow)
    brief_blocks = _build_brief_blocks(brief_data, copy, thread_ts=message_ts)
    web.chat_postMessage(
        channel=channel,
        thread_ts=message_ts,
        text=f":clipboard: Campaign brief for {advertiser}",
        blocks=brief_blocks,
        unfurl_links=False,
    )

    # 4. Build thread URL — this IS the brief; queue item points to it
    thread_url = _slack_thread_url(channel, message_ts)

    # 5. Persist approval state (for lifecycle tracking + launch notification)
    _record_queued_offer(advertiser, brief_data, user_id, thread_url)

    # 6. Try to write directly to the Slack Demand Queue list
    added_to_list = _try_add_to_demand_queue(web, brief_data, user_id, thread_url)

    # 7. Confirm — include thread link so the approval message is self-contained
    if added_to_list:
        confirm = (
            f":white_check_mark: *{advertiser}* added to queue by <@{user_id}> — "
            f"<{thread_url}|brief is in this thread>"
        )
    else:
        confirm = (
            f":white_check_mark: *{advertiser}* approved by <@{user_id}> · "
            f"<{QUEUE_LIST_URL}|Add to Demand Queue →>"
        )
    web.chat_postMessage(channel=channel, thread_ts=message_ts, text=confirm)
    log.info(f"Approved: {advertiser} ({offer_id}) by {user_id}, list_write={added_to_list}")


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

    # Build thread history (mirrors handle_event)
    history = []
    try:
        replies = web.conversations_replies(channel=channel, ts=thread_ts, limit=50)
        bot_id  = web.auth_test()["user_id"]
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
    effective_thread_ts = thread_ts if is_thread_reply else _LAST_THREAD_PER_CHANNEL.get(channel)

    if effective_thread_ts:
        try:
            replies = web.conversations_replies(channel=channel, ts=effective_thread_ts, limit=50)
            bot_id  = web.auth_test()["user_id"]
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

    # Post placeholder — rotates through loading messages so it doesn't feel like a spinner
    placeholder = web.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=random.choice(_LOADING_MESSAGES),
    )

    try:
        response = ask(query, history=history)
    except Exception as e:
        log.error(f"Agent error: {e}")
        response = f"Something went wrong: {e}"

    # ── Route response: brief (Block Kit) vs text_with_context vs plain text ────
    # Track the active thread per channel so top-level follow-ups retain context
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


def main():
    if not BOT_TOKEN or not APP_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env")

    web_client    = WebClient(token=BOT_TOKEN)
    socket_client = SocketModeClient(app_token=APP_TOKEN, web_client=web_client)
    socket_client.socket_mode_request_listeners.append(handle_event)

    log.info("Scout is online — listening for @mentions via Socket Mode")
    socket_client.connect()

    import signal
    signal.pause()


if __name__ == "__main__":
    main()
