"""
scout_handlers.py — Slack event handlers for Scout.

All _handle_* functions live here. Clients (WebClient, ClickHouse) are
passed as parameters — never imported from scout_bot.py (would be circular).

Import DAG: scout_handlers → scout_slack_ui, scout_notion, scout_state, scout_agent
            scout_handlers does NOT import from scout_bot
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time

from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.web import WebClient

from scout_agent import ask
from scout_notion import (
    _generate_offer_copy, _write_to_notion_queue, _update_notion_status,
    _queue_copy_enrichment,
    _patch_notion_copy, _copy_cache_key, _copy_cache_get, _copy_cache_set,
)
from scout_slack_ui import (
    _build_brief_blocks, _queue_confirm_blocks, _build_opportunity_cards,
    _build_suggestion_buttons, _build_help_blocks, _build_feedback_buttons,
    _build_home_view, _text_to_blocks, _is_help_query,
)
from scout_state import (
    _store_brief, _get_brief, _delete_brief,
    _merge_thread_context, _get_thread_context,
    _load_launched_offers, _save_launched_offers,
    _load_learnings, _save_learnings,
    _log_usage,
    _DATA_DIR,
    _strip_mention, _sanitize_slack, _slack_thread_url,
    _route_channel,
    _pick_loading_message,
    _rotating_status,
    _smart_history,
    _post_error_update,
)

log = logging.getLogger("scout_handlers")

# Injected at startup by scout_bot.main() — avoids circular import
_BOT_USER_ID: str = ""
_LAST_THREAD_PER_CHANNEL: dict = {}
_LAST_THREAD_LOCK = None  # threading.Lock, set by scout_bot
_PULSE_RUNNER = None       # _run_pulse_once from scout_bot; injected to avoid circular import

def _set_bot_user_id(uid: str) -> None:
    global _BOT_USER_ID
    _BOT_USER_ID = uid

def _set_thread_state(per_channel: dict, lock) -> None:
    global _LAST_THREAD_PER_CHANNEL, _LAST_THREAD_LOCK
    _LAST_THREAD_PER_CHANNEL = per_channel
    _LAST_THREAD_LOCK = lock

def _set_pulse_runner(fn) -> None:
    global _PULSE_RUNNER
    _PULSE_RUNNER = fn

def _run_preflight_qa(  # replaces _check_url_async (removed — this is a strict superset)
    web: WebClient,
    channel: str,
    thread_ts: str,
    brief_data: dict,
) -> None:
    """
    Run pre-flight quality checks in a background thread and post consolidated
    results as a single follow-up message. Never blocks brief display.

    Checks:
      1. Tracking URL resolution
      2. Advertiser history on MS platform (from ClickHouse benchmarks)
    """
    def _run():
        import urllib.request
        checks: list[str] = []

        # 1. URL resolution
        tracking_url = (brief_data.get("tracking_url") or "").strip()
        if tracking_url and not tracking_url.startswith("Not available"):
            try:
                req = urllib.request.Request(
                    tracking_url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"}
                )
                with urllib.request.urlopen(req, timeout=5) as r:
                    if r.status < 400:
                        checks.append(":white_check_mark: URL resolves")
                    else:
                        checks.append(f":warning: URL returned HTTP {r.status}")
            except Exception:
                checks.append(":warning: URL did not resolve — verify tracking link before entry")

        # 2. Advertiser history on MS platform
        try:
            from scout_agent import _get_benchmarks
            benchmarks = _get_benchmarks()
            adv_key = (brief_data.get("advertiser") or "").lower().strip()
            by_adv = benchmarks.get("by_adv_name", {})
            if adv_key and adv_key in by_adv:
                hist = by_adv[adv_key]
                rpm = hist.get("rpm", 0)
                cvr = hist.get("cvr_pct", 0)
                checks.append(
                    f":bar_chart: MS history: ${rpm:,.0f} RPM · {cvr:.2f}% CVR "
                    f"({hist.get('impressions', 0):,} impressions)"
                )
            else:
                checks.append(":new: No prior MS data — first run for this advertiser")
        except Exception:
            pass

        if not checks:
            return
        try:
            web.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=":mag: *Pre-flight:* " + "  ·  ".join(checks),
                unfurl_links=False,
            )
        except Exception:
            pass

    threading.Thread(target=_run, daemon=True).start()

def _post_offer_queue_card(
    web: WebClient,
    brief_data: dict,
    copy: dict,
    user_id: str,
    digest_thread_url: str,
    notion_url: str | None,
    score: float,
) -> None:
    """
    Post a structured offer card to #scout-offers when an offer is approved.
    This IS the brief — all components Gordon needs to enter the campaign in MS platform.
    One message. No thread noise.
    """
    offers_channel = _route_channel("offers")
    advertiser  = brief_data.get("advertiser", "Offer")
    network     = brief_data.get("network", "").title()
    payout      = brief_data.get("payout", "Rate TBD")
    payout_type = brief_data.get("payout_type", "CPA")
    tracking_url = brief_data.get("tracking_url", "Not available")
    title       = copy.get("title", "")
    description = copy.get("description", "")
    cta_yes     = (copy.get("cta") or {}).get("yes", "")
    cta_no      = (copy.get("cta") or {}).get("no", "")

    # Compact header line
    score_str = f" · est. ${score:.2f} RPM" if score else ""
    payout_str = payout if payout and payout != "Rate TBD" else ""
    ptype_str  = payout_type if payout_type and payout_type.lower() not in ("unknown", "") else ""
    payout_display = " · ".join(filter(None, [payout_str, ptype_str])) or "Rate TBD"
    header = f":white_check_mark: *{advertiser}* approved by <@{user_id}> · {network} · {payout_display}{score_str}"

    # Body: everything needed to enter in MS platform
    lines = [header, ""]
    if title:
        lines.append(f"*Title:* {title}")
    if description:
        lines.append(f"*Description:* {description}")
    if cta_yes or cta_no:
        cta_parts = []
        if cta_yes:
            cta_parts.append(f"Yes: _{cta_yes}_")
        if cta_no:
            cta_parts.append(f"No: _{cta_no}_")
        lines.append(f"*CTA:* " + "  ·  ".join(cta_parts))
    if tracking_url and not tracking_url.startswith("Not available"):
        lines.append(f"*Tracking URL:* {tracking_url}")
    lines.append("")
    footer_parts = [f"<{digest_thread_url}|Digest thread>"]
    if notion_url:
        footer_parts.append(f"<{notion_url}|Notion queue entry>")
    lines.append("_" + "  ·  ".join(footer_parts) + "_")

    try:
        web.chat_postMessage(
            channel=offers_channel,
            text=header,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(lines)},
                }
            ],
            unfurl_links=False,
        )
    except Exception as e:
        log.warning(f"[approve] failed to post queue card to #scout-offers: {e}")

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
    Build baseline copy dict for _handle_approve / _post_offer_queue_card / _write_to_notion_queue.
    Returns keys: title, description, cta (dict), short_headline, short_desc, bottom_line.

    This is the SYNCHRONOUS fallback. AI-quality copy is generated asynchronously by
    _generate_offer_copy() and PATCHed onto the Notion page within ~10 seconds of approval.
    Callers must use: copy.get("title"), copy.get("cta", {}).get("yes"), etc.
    """
    advertiser  = brief_data.get("advertiser", "")
    description = (brief_data.get("description") or offer_payload.get("description") or "").strip()
    payout_type = (brief_data.get("payout_type") or offer_payload.get("payout_type", "")).upper()
    portal_url  = brief_data.get("portal_url", "")
    network     = brief_data.get("network", "Impact")

    # Build a safe title from the first complete sentence (never truncate mid-word)
    first_sent = description.split(".")[0].strip() if description else ""
    if len(first_sent) > 90:
        first_sent = first_sent[:87].rsplit(" ", 1)[0] + "..."
    title = first_sent if first_sent else f"Exclusive offer from {advertiser}"

    # Short headline: title truncated to 60 at a word boundary
    if len(title) > 60:
        short_headline = title[:57].rsplit(" ", 1)[0] + "..."
    else:
        short_headline = title

    # Description and short_desc from offer description
    desc = description[:220] if description else ""
    if len(description) > 140:
        short_desc = description[:137].rsplit(" ", 1)[0] + "..."
    else:
        short_desc = description[:140]

    # CTAs: specific to commitment level, not just payout type
    cta_map = {
        "CPL":        {"yes": "Get my free quote", "no": "Not now"},
        "CPS":        {"yes": "Shop now", "no": "Not now"},
        "CPA":        {"yes": "Claim offer", "no": "Not now"},
        "MOBILE_APP": {"yes": "Download free", "no": "Not now"},
        "APP_INSTALL":{"yes": "Download free", "no": "Not now"},
        "CPC":        {"yes": "Learn more", "no": "Not now"},
    }
    cta = cta_map.get(payout_type, {"yes": "Get started", "no": "Not now"})

    if portal_url:
        bottom_line = f"Ready to build? <{portal_url}|View on {network}> to pull creatives, then add to the MS platform."
    else:
        bottom_line = "Ready to build? Pull creatives from the network portal and add to the MS platform."

    return {
        "title":          title,
        "short_headline": short_headline,
        "description":    desc,
        "short_desc":     short_desc,
        "cta":            cta,          # single dict {yes: ..., no: ...}
        "bottom_line":    bottom_line,
        # Legacy list keys kept for _build_brief_blocks() compatibility
        "titles":         [title],
        "ctas":           [cta],
        "targeting":      "",
    }

def _record_queued_offer(
    advertiser: str,
    brief_data: dict,
    user_id: str,
    thread_url: str,
    notion_url: str = "",
    copy_data: dict | None = None,
):
    """Persist approval state so the lifecycle (queue → live → notify) can close the loop.

    Stores scout_score_estimated so the 14-day recap can compare prediction vs. actual.
    This is the training signal: every validated offer becomes a calibration data point.
    """
    from datetime import datetime, timezone
    state = _load_launched_offers()
    existing = state.get(advertiser, {})
    if existing.get("status") == "queued":
        log.info(f"_record_queued_offer: {advertiser} already queued — skipping overwrite")
        return
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
        # Campaign Builder fields — stored so /scout-enter can reconstruct the form
        # without re-fetching or requiring the original brief thread to still exist.
        "tracking_url":           brief_data.get("tracking_url", ""),
        "offer_id":               str(brief_data.get("offer_id", "")),
        "payout_type":            brief_data.get("payout_type", "CPA"),
        "risk_flag":              brief_data.get("risk_flag", ""),
        "title":                  (copy_data or {}).get("t") or (copy_data or {}).get("title", ""),
        "description":            (copy_data or {}).get("d") or (copy_data or {}).get("description", ""),
        "cta_yes":                (copy_data or {}).get("cy") or (copy_data or {}).get("cta_yes", ""),
        "cta_no":                 (copy_data or {}).get("cn") or (copy_data or {}).get("cta_no", ""),
    }
    _save_launched_offers(state)

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

        # Remove the actions block that contains the queue button
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
    Handle ✓ Add to Queue button click from Scout Signal digest.

    Flow (Slack ack sent before this runs — no 3s timeout):
      1. Record approval (excludes from future digests)
      2. Fetch full brief — tracking URL, performance context
      3. Build copy_data (metadata for Notion properties)
      4. Generate AI copy synchronously — all 7 fields baked into the page at creation
      5. Write to Notion queue (complete page, no patching needed)
      6. Thread reply in digest — one terse ack where the user clicked
      7. Persist state (lifecycle tracking + launch notification)
      8. Block Kit confirmation card to #scout-offers — canonical pipeline entry
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

    # 2. Fetch full brief
    brief_data = _fetch_brief_for_approve(advertiser, offer)
    score      = brief_data.get("scout_score_rpm", 0) or 0

    # 3. Build copy_data (Notion property metadata — not the display copy)
    copy_data = {
        "rpm": score,
        "pf":  brief_data.get("performance_context", ""),
        "rf":  brief_data.get("risk_flag", ""),
        "pt":  brief_data.get("payout_type", "CPA"),
        "oid": brief_data.get("offer_id", ""),
    }

    # Resolve Slack user_id → display name for Notion (avoids raw "<@U08SLE7M0RH>" in page)
    user_display = user_id
    try:
        _uinfo = web.users_info(user=user_id)
        _profile = (_uinfo.get("user") or {}).get("profile", {})
        user_display = (
            _profile.get("display_name")
            or _profile.get("real_name")
            or user_id
        )
    except Exception:
        pass  # fallback to raw user_id — better than crashing approval flow

    # 4. Generate AI copy synchronously — page will be complete at creation time.
    # Slack already sent the ack, so there is no 3-second constraint here.
    ai_copy = None
    try:
        ai_copy = _generate_offer_copy(
            advertiser   = brief_data.get("advertiser", advertiser),
            description  = brief_data.get("description", offer.get("description", "")),
            payout_type  = brief_data.get("payout_type", offer.get("payout_type", "CPA")),
            category     = brief_data.get("category", offer.get("category", "")),
            payout       = offer.get("payout", ""),
            geo          = brief_data.get("geo", offer.get("geo", "US")),
        )
    except Exception as e:
        log.warning(f"AI copy sync generation failed for {advertiser}: {e}")

    # 5. Write to Notion — ai_copy baked in if available, placeholder if not
    thread_url = _slack_thread_url(channel, message_ts)
    notion_url = _write_to_notion_queue(brief_data, copy_data, user_id, thread_url, ai_copy=ai_copy, user_display=user_display)

    # Fallback: if sync generation failed, enrich async so the page eventually fills in
    if notion_url and not ai_copy:
        log.warning(f"AI copy sync failed for {advertiser} — falling back to async enrichment")
        _queue_copy_enrichment(
            notion_url,
            brief_data.get("advertiser", advertiser),
            brief_data.get("description", offer.get("description", "")),
            brief_data.get("payout_type", offer.get("payout_type", "CPA")),
            brief_data.get("category", offer.get("category", "")),
            offer.get("payout", ""),
            brief_data.get("geo", offer.get("geo", "US")),
        )

    # 6. Thread reply in the digest card where the user clicked
    _notion_link = f" · <{notion_url}|Brief in Notion →>" if notion_url else ""
    try:
        web.chat_postMessage(
            channel=channel,
            thread_ts=message_ts,
            text=f"✅ Added to Pipeline — {advertiser}{_notion_link}",
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"✅ Added to Pipeline{_notion_link}"},
            }],
            unfurl_links=False,
        )
    except Exception as e:
        log.warning(f"[approve] thread reply failed: {e}")

    # 7. Persist approval state (lifecycle tracking + launch notification)
    _record_queued_offer(
        advertiser, brief_data, user_id, thread_url,
        notion_url=notion_url or "", copy_data=copy_data,
    )

    # 8. Block Kit confirmation card to #scout-offers — canonical pipeline entry
    _network     = (brief_data.get("network") or "").title()
    _payout      = brief_data.get("payout", "")
    _payout_type = (brief_data.get("payout_type") or "").upper()
    _payout_disp = " · ".join(filter(None, [_payout, _payout_type])) or "Rate TBD"
    web.chat_postMessage(
        channel=_route_channel("offers"),
        text=f"✅ {advertiser} added to Pipeline",
        blocks=_queue_confirm_blocks(advertiser, _network, _payout_disp, user_id, score, notion_url),
        unfurl_links=False,
    )

    # Update the original digest card — replace only this offer's actions block
    # with a confirmation line. Other offer cards in the same message stay intact.
    try:
        orig_blocks = (payload.get("message") or {}).get("blocks", [])
        _notion_badge = f" · <{notion_url}|Notion →>" if notion_url else ""
        confirm_block = {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"✅ *Added to Pipeline* by <@{user_id}>{_notion_badge}"}],
        }
        updated_blocks = []
        replaced = False
        for block in orig_blocks:
            if block.get("type") == "actions" and not replaced:
                is_clicked = False
                for el in block.get("elements", []):
                    try:
                        v = json.loads(el.get("value", "{}"))
                        if v.get("offer_id") == offer_id or v.get("advertiser") == advertiser:
                            is_clicked = True
                            break
                    except (json.JSONDecodeError, TypeError):
                        pass
                if is_clicked:
                    updated_blocks.append(confirm_block)
                    replaced = True
                    continue
            updated_blocks.append(block)
        web.chat_update(
            channel=channel,
            ts=message_ts,
            text=f"✅ {advertiser} added to Pipeline",
            blocks=updated_blocks,
        )
    except Exception as e:
        log.warning(f"[approve] digest card update failed: {e}")

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
        "sh":  data.get("sh", ""),
        "d":   data.get("d", ""),
        "sd":  data.get("sd", ""),
        "cy":  data.get("cy", ""),
        "cn":  data.get("cn", ""),
        "rpm": data.get("rpm", 0),
        "pf":  data.get("pf", ""),
        "rf":  data.get("rf", ""),
        "pt":  data.get("pt", "CPA"),
        "oid": data.get("offer_id", ""),
    }

    # Write to Notion + update brief card in-place with ⏳ status
    notion_url = _try_add_to_demand_queue(
        web, brief_data, user_id, thread_url,
        copy_data=copy_data,
        brief_channel=channel,
        brief_ts=message_ts,
    )
    _record_queued_offer(
        advertiser, brief_data, user_id, thread_url,
        notion_url=notion_url or "", copy_data=copy_data,
    )

    # Enrich Notion page with AI-generated copy via coalescing queue (non-blocking)
    if notion_url:
        _queue_copy_enrichment(
            notion_url,
            brief_data.get("advertiser", advertiser),
            data.get("d", ""),
            brief_data.get("payout_type", data.get("pt", "CPA")),
            brief_data.get("category", data.get("category", "")),
            brief_data.get("payout", ""),
            brief_data.get("geo", "US"),
        )

    # Pre-flight QA in background — URL check + MS history, posts consolidated result
    _run_preflight_qa(web, channel, thread_ts, brief_data)

    # Block Kit confirmation in #scout-offers — standalone, not threaded
    _bq_network     = (brief_data.get("network") or "").title()
    _bq_payout      = brief_data.get("payout", "")
    _bq_payout_type = (brief_data.get("payout_type") or data.get("pt", "")).upper()
    _bq_payout_disp = " · ".join(filter(None, [_bq_payout, _bq_payout_type])) or "Rate TBD"
    _bq_score       = float(data.get("rpm", 0) or 0)
    web.chat_postMessage(
        channel=_route_channel("offers"),
        text=f"✅ {advertiser} added to queue",
        blocks=_queue_confirm_blocks(
            advertiser, _bq_network, _bq_payout_disp, user_id, _bq_score, notion_url
        ),
        unfurl_links=False,
    )
    log.info(f"Brief queued: {advertiser} by {user_id}")

def _handle_reject(action: dict, payload: dict, web: WebClient):
    """Handle ✕ Skip button click from Scout Signal digest."""
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
    user_id   = (payload.get("user") or {}).get("id", "")
    if not query or not channel or not thread_ts:
        return

    try:
        web.chat_typing(channel=channel)
    except Exception:
        pass
    _msg_text = _pick_loading_message(query)
    placeholder = web.chat_postMessage(
        channel=channel, thread_ts=thread_ts, text=_msg_text,
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
    )
    _placeholder_ts_sg = placeholder["ts"]
    stop_rotating = _rotating_status(web, channel, _placeholder_ts_sg)

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

    history = _smart_history(history)

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
        _t0 = time.monotonic()
        response = ask(query, history=history, user_id=user_id)
        _elapsed = int(time.monotonic() - _t0)
        _elapsed_str = f"{_elapsed}s" if _elapsed < 60 else f"{_elapsed // 60}m {_elapsed % 60}s"
    except Exception as e:
        log.error(f"suggestion ask failed: {e}")
        stop_rotating()
        _post_error_update(web, channel, placeholder["ts"], e)
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

    content_blocks_sg = _text_to_blocks(response_text)
    suggestion_blocks = _build_suggestion_buttons(sugg)
    web.chat_update(
        channel=channel, ts=_placeholder_ts_sg, text=response_text,
        blocks=[
            *content_blocks_sg,
            *suggestion_blocks,
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"_Scout · {_elapsed_str}_"}]},
        ],
    )
    log.info(f"Suggestion answered in {channel} (thread {thread_ts}): {query!r}")

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

    # ── Scout Signal digest actions ───────────────────────────────────────────
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

    # ── Feedback buttons (👍 / 👎 / ✏️) ──────────────────────────────────────
    if action_id in ("scout_feedback_good", "scout_feedback_bad", "scout_feedback_correct"):
        _handle_feedback(action, payload, web)
        return

    # ── Pulse interactive buttons ─────────────────────────────────────────────
    if action_id == "pulse_ghost_brief":
        user_id = payload.get("user", {}).get("id", "")
        msg_ts  = (payload.get("message") or {}).get("ts", "")
        def _run_ghost(u=user_id, t=msg_ts):
            resp = ask("ghost campaigns", history=[], user_id=u)
            text = resp if isinstance(resp, str) else resp.get("text", str(resp))
            web.chat_postMessage(channel=channel, thread_ts=t, text=f"<@{u}> {text}")
        threading.Thread(target=_run_ghost, daemon=True).start()
        return

    if action_id == "pulse_fill_rate_brief":
        user_id = payload.get("user", {}).get("id", "")
        msg_ts  = (payload.get("message") or {}).get("ts", "")
        def _run_fill(u=user_id, t=msg_ts):
            resp = ask("fill rate brief", history=[], user_id=u)
            text = resp if isinstance(resp, str) else resp.get("text", str(resp))
            web.chat_postMessage(channel=channel, thread_ts=t, text=f"<@{u}> {text}")
        threading.Thread(target=_run_fill, daemon=True).start()
        return

    if action_id == "pulse_top_opps":
        user_id = payload.get("user", {}).get("id", "")
        msg_ts  = (payload.get("message") or {}).get("ts", "")
        def _run_opps(u=user_id, t=msg_ts):
            resp = ask("top revenue opportunities", history=[], user_id=u)
            text = resp if isinstance(resp, str) else resp.get("text", str(resp))
            web.chat_postMessage(channel=channel, thread_ts=t, text=f"<@{u}> {_sanitize_slack(str(text))}")
        threading.Thread(target=_run_opps, daemon=True).start()
        return

    if action_id in ("pulse_scout_offers", "pulse_dig_in"):
        pub     = action.get("value", "").strip()
        user_id = payload.get("user", {}).get("id", "")
        msg_ts  = (payload.get("message") or {}).get("ts", "")
        query   = f"offers for {pub}" if action_id == "pulse_scout_offers" else f"dig into {pub}"
        def _run_pub(q=query, u=user_id, t=msg_ts):
            resp = ask(q, history=[], user_id=u)
            text = resp if isinstance(resp, str) else resp.get("text", str(resp))
            web.chat_postMessage(channel=channel, thread_ts=t, text=f"<@{u}> {text}")
        threading.Thread(target=_run_pub, daemon=True).start()
        return

def _handle_feedback(action: dict, payload: dict, web: WebClient) -> None:
    """
    Handle 👍 / 👎 / ✏️ feedback buttons on Scout responses.
    Stores to data/learnings.json for future prompt injection.
    """
    import uuid as _uuid
    from datetime import datetime as _dt, timezone as _tz

    action_id   = action.get("action_id", "")
    query_hash  = action.get("value", "")
    user_id     = (payload.get("user") or {}).get("id", "")
    channel     = (payload.get("channel") or {}).get("id", "")
    message     = payload.get("message", {})
    thread_ts   = message.get("thread_ts") or message.get("ts", "")
    msg_ts      = message.get("ts", "")

    learnings = _load_learnings()
    now_str   = _dt.now(_tz.utc).isoformat()

    if action_id == "scout_feedback_good":
        learnings.setdefault("positive_signals", []).append({
            "id":         str(_uuid.uuid4())[:8],
            "created_at": now_str,
            "query_hash": query_hash,
            "user":       user_id,
        })
        _save_learnings(learnings)
        # Acknowledge with an ephemeral message (visible only to the clicker)
        try:
            web.chat_postEphemeral(
                channel=channel, user=user_id, thread_ts=thread_ts,
                text=":white_check_mark: Got it — noted as accurate.",
            )
        except Exception:
            pass

    elif action_id == "scout_feedback_bad":
        learnings.setdefault("negative_signals", []).append({
            "id":         str(_uuid.uuid4())[:8],
            "created_at": now_str,
            "query_hash": query_hash,
            "user":       user_id,
        })
        _save_learnings(learnings)
        try:
            web.chat_postEphemeral(
                channel=channel, user=user_id, thread_ts=thread_ts,
                text=":pencil: Got it — marked as off. Use :pencil2: *Correct this* to add the right answer so Scout remembers.",
            )
        except Exception:
            pass

    elif action_id == "scout_feedback_correct":
        # Store a pending correction keyed by msg_ts — _handle_event will capture the follow-up reply
        corr_id = str(_uuid.uuid4())[:8]
        learnings.setdefault("pending_corrections", {})[msg_ts] = {
            "id":          corr_id,
            "created_at":  now_str,
            "query_hash":  query_hash,
            "correction_by": user_id,
            "channel":     channel,
            "thread_ts":   thread_ts,
            "msg_ts":      msg_ts,
        }
        _save_learnings(learnings)
        try:
            web.chat_postMessage(
                channel=channel, thread_ts=thread_ts,
                text=f"<@{user_id}> What's the correct answer? Reply here and I'll remember it. :memo:",
            )
        except Exception:
            pass

    log.info(f"Feedback recorded: {action_id} query={query_hash} user={user_id}")

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

        try:
            web.chat_typing(channel=dm_channel)
        except Exception:
            pass
        _msg_text = _pick_loading_message(query)
        placeholder = web.chat_postMessage(
            channel=dm_channel, thread_ts=thread_ts, text=_msg_text,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
        )
        _placeholder_ts_ah = placeholder["ts"]
        stop_rotating = _rotating_status(web, dm_channel, _placeholder_ts_ah)

        try:
            _t0 = time.monotonic()
            response = ask(query)
            _elapsed = int(time.monotonic() - _t0)
            _elapsed_str = f"{_elapsed}s" if _elapsed < 60 else f"{_elapsed // 60}m {_elapsed % 60}s"
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
        else:
            if isinstance(response, dict) and response.get("type") == "text_with_context":
                response_text     = response.get("text", "")
                suggestions       = response.get("suggestions", [])
                suggestion_blocks = _build_suggestion_buttons(suggestions)
            else:
                response_text     = response if isinstance(response, str) else str(response)
                suggestion_blocks = []
            content_blocks = _text_to_blocks(response_text)
            web.chat_update(
                channel=dm_channel, ts=_placeholder_ts_ah,
                text=response_text,
                blocks=[*content_blocks, *suggestion_blocks,
                        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"_Scout · {_elapsed_str}_"}]}],
            )
        log.info(f"App Home try-it: ran '{query[:50]}' for {user_id}")
    except Exception as e:
        log.warning(f"_handle_home_try_query failed for {user_id}: {e}")

def _handle_slash_command(req: SocketModeRequest, web: WebClient) -> None:
    """
    Handle Scout slash commands. All responses are ephemeral — only the caller sees them.
    Commands must be registered at api.slack.com/apps → Scout → Slash Commands.

    /scout-pub    — Publisher performance card (ClickHouse, no AI)
    /scout-queue  — Show the current demand queue with Notion links
    /scout-enter  — MS Platform entry card for a queued offer
    /scout-status — System health: benchmark freshness, offer count, ClickHouse status
    """
    from scout_agent import get_demand_queue_status, get_scout_status, get_publisher_competitive_landscape

    payload  = req.payload
    command  = payload.get("command", "")
    user_id  = payload.get("user_id", "")
    channel  = payload.get("channel_id", "")

    try:
        if command == "/scout-queue":
            result = get_demand_queue_status()
            items  = result.get("pending", [])
            if not items:
                text = ":white_check_mark: Queue is clear — nothing pending entry."
            else:
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                lines = [f":hourglass_flowing_sand: *{result['count']} offer{'s' if result['count'] != 1 else ''} in queue*"]
                for item in items:
                    adv         = item["advertiser"]
                    payout      = item.get("payout", "")
                    network     = item.get("network", "")
                    notion_url  = item.get("notion_url", "")
                    approved_at = item.get("approved_at", "")
                    is_live     = item.get("status") == "likely_live"
                    badge       = ":large_green_circle: likely live" if is_live else ":white_circle: pending"
                    notion_link = f" · <{notion_url}|Notion>" if notion_url else ""
                    # Days waiting
                    days_str = ""
                    if approved_at:
                        try:
                            approved_dt = datetime.fromisoformat(approved_at).replace(tzinfo=timezone.utc)
                            days = (now - approved_dt).days
                            days_str = f" · {days}d"
                        except Exception:
                            pass
                    lines.append(f"{badge} *{adv}* — {payout} · {network}{days_str}{notion_link}")
                text = "\n".join(lines)
            web.chat_postEphemeral(channel=channel, user=user_id, text=text)

        elif command == "/scout-status":
            s       = get_scout_status()
            ch_stat = s.get("clickhouse", "unknown")
            ch_icon = ":white_check_mark:" if ch_stat == "ok" else ":warning:"
            bm_age  = s.get("benchmarks", "unknown")
            offers  = s.get("offer_inventory", 0)
            queue   = s.get("queue_depth", 0)
            warns   = s.get("warnings", [])
            lines   = [
                ":satellite: *Scout Status*",
                f"Benchmarks: `{bm_age}`  ·  Offers: `{offers:,}`  ·  Queue: `{queue} pending`  ·  ClickHouse: {ch_icon}",
            ]
            for w in warns:
                lines.append(f":warning: {w}")
            web.chat_postEphemeral(channel=channel, user=user_id, text="\n".join(lines))

        elif command == "/scout-enter":
            # Formatted entry card for a queued offer — all fields pre-formatted
            # for easy copy into MS Platform. No Playwright; human does the entry.
            # Usage: /scout-enter TurboTax
            # NOTE: Register at api.slack.com/apps → Scout → Slash Commands
            text_arg = payload.get("text", "").strip()
            if not text_arg:
                web.chat_postEphemeral(
                    channel=channel, user=user_id,
                    text="Usage: `/scout-enter TurboTax` or `/scout-enter https://tracking.link/...`",
                )
                return
            state = _load_launched_offers()
            # Accept a tracking URL or an advertiser name
            if text_arg.startswith("http"):
                key = next(
                    (k for k, v in state.items() if text_arg in (v.get("tracking_url") or "")),
                    None,
                )
            else:
                key = next(
                    (k for k in state if text_arg.lower() in k.lower() or k.lower() in text_arg.lower()),
                    None,
                )
            if not key:
                web.chat_postEphemeral(
                    channel=channel, user=user_id,
                    text=f":x: No queued offer found matching `{text_arg}`. Run `/scout-queue` to see exact names, or paste the tracking URL.",
                )
                return
            entry     = state[key]
            status    = entry.get("status", "unknown")
            notion_lk = f" · <{entry['notion_url']}|Notion page>" if entry.get("notion_url") else ""
            title       = entry.get("title", "_not saved_")
            description = entry.get("description", "_not saved_")
            cta_yes     = entry.get("cta_yes", "_not saved_")
            cta_no      = entry.get("cta_no", "_not saved_")
            tracking    = entry.get("tracking_url", "_not saved_")
            offer_id    = entry.get("offer_id", "_not saved_")
            network     = entry.get("network", "")
            payout      = entry.get("payout", "")
            payout_type = entry.get("payout_type", "CPA")
            risk_flag   = entry.get("risk_flag", "")

            lines = [
                f":clipboard: *MS Entry Card — {key}* ({status}){notion_lk}",
                "",
                f"*Internal Name:* `{key} — {network} — (today's date)`",
                f"*Network:* `{network}`  *Offer ID:* `{offer_id}`",
                f"*Goal Type:* `{payout_type}`  *Payout:* `{payout}`",
                f"*Destination:* `{tracking}`",
                "",
                f"*Headline:* `{title}`",
                f"*Description:* `{description}`",
                f"*Positive CTA:* `{cta_yes}`",
                f"*Negative CTA:* `{cta_no}`",
            ]
            if risk_flag:
                lines.append(f"\n:warning: *Risk flag:* {risk_flag}")
            lines.append("\n_Copy each field above into MS Platform. Toggle Test Offer ON until reviewed._")
            entry_text = "\n".join(lines)
            web.chat_postEphemeral(
                channel=channel, user=user_id,
                text=entry_text,
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": entry_text}}],
            )

        elif command == "/scout-pub":
            # Publisher performance terminal — direct ClickHouse, no AI.
            # Usage: /scout-pub AT&T   or   /scout-pub 953
            # Register at api.slack.com/apps → Scout → Slash Commands
            text_arg = payload.get("text", "").strip()
            if not text_arg:
                web.chat_postEphemeral(
                    channel=channel, user=user_id,
                    text="Usage: `/scout-pub AT&T` or `/scout-pub 953` (publisher ID)",
                )
                return

            # Numeric → publisher_id; otherwise → name fuzzy match
            pub_kwargs = (
                {"publisher_id": int(text_arg)} if text_arg.isdigit()
                else {"publisher_name": text_arg}
            )
            try:
                data = get_publisher_competitive_landscape(**pub_kwargs)
            except Exception as e:
                log.warning(f"/scout-pub lookup failed for {text_arg!r}: {e}")
                web.chat_postEphemeral(
                    channel=channel, user=user_id,
                    text=f":warning: Publisher data unavailable right now — try `@Scout {text_arg} performance` instead.",
                )
                return

            if not data or not data.get("publisher"):
                web.chat_postEphemeral(
                    channel=channel, user=user_id,
                    text=f":x: No publisher found matching `{text_arg}`. Try the ID (e.g. `953`) or `/scout-queue` to check names.",
                )
                return

            pub_name     = data["publisher"]
            pub_id       = data.get("publisher_id", "")
            weekly_impr  = data.get("weekly_impressions_avg", 0)
            serving      = data.get("active_competitors", [])
            provisioned  = len(data.get("provisioned_campaigns", []))
            serving_cnt  = data.get("serving_count", len(serving))

            # Format weekly impressions
            def _fmt_num(n):
                if n >= 1_000_000:
                    return f"{n/1_000_000:.1f}M"
                if n >= 1_000:
                    return f"{n/1_000:.0f}K"
                return str(int(n))

            header = (
                f":bar_chart: *{pub_name}* (ID: {pub_id})\n"
                f"~{_fmt_num(weekly_impr)} impr/week  ·  "
                f"{provisioned} provisioned  ·  {serving_cnt} serving"
            )

            # Top serving campaigns ranked by RPM
            campaign_lines = []
            for camp in serving[:8]:
                adv   = camp.get("advertiser", "Unknown")
                rpm   = camp.get("rpm") or 0
                impr  = camp.get("impressions_2w") or 0
                pay   = camp.get("payout") or camp.get("provisioned", "")
                rpm_s = f"${rpm:,.0f} RPM" if rpm else "no conv. data"
                line  = f":large_green_circle: {adv} — {rpm_s} · {_fmt_num(impr)} impr"
                if pay:
                    line += f" · {pay}"
                campaign_lines.append(line)

            extra = serving_cnt - len(campaign_lines)
            if extra > 0:
                campaign_lines.append(f"_{extra} more serving_")

            if not campaign_lines:
                campaign_lines = ["_No campaigns serving in last 14 days_"]

            body = "\n".join(campaign_lines)
            tip  = f"_Tip: `@Scout rank [offer] on {pub_name.split()[0]} at $X` for payout scenarios_"
            full_text = f"{header}\n\n{body}\n\n{tip}"

            web.chat_postEphemeral(
                channel=channel, user=user_id,
                text=full_text,
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": full_text}}],
            )

        else:
            web.chat_postEphemeral(
                channel=channel, user=user_id,
                text=f"Unknown command `{command}`. Try `/scout-pub`, `/scout-queue`, `/scout-enter`, or `/scout-status`.",
            )
    except Exception as e:
        log.error(f"_handle_slash_command error ({command}): {e}")
        try:
            web.chat_postEphemeral(channel=channel, user=user_id,
                                   text=f":warning: Scout command failed: {e}")
        except Exception:
            pass

def handle_event(client: SocketModeClient, req: SocketModeRequest):
    # Acknowledge immediately — Slack requires <3s ack
    client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))

    web = WebClient(token=os.getenv("SLACK_BOT_TOKEN", ""), retry_handlers=[RateLimitErrorRetryHandler(max_retry_count=3)])

    # ── Button clicks ─────────────────────────────────────────────────────────
    if req.type == "interactive":
        _handle_block_action(req, web)
        return

    # ── Slash commands ────────────────────────────────────────────────────────
    # NOTE: /scout-queue and /scout-status must be registered at api.slack.com/apps
    #       → Scout app → Slash Commands (Socket Mode). One-time manual step.
    if req.type == "slash_commands":
        _handle_slash_command(req, web)
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

    # ── 🗑️ reaction → delete Scout's own message ─────────────────────────────
    # Any team member can add a :wastebasket: reaction to a Scout message to delete it.
    # Scout only deletes messages it posted (bot_id check). Works on any channel.
    if event.get("type") == "reaction_added" and event.get("reaction") == "wastebasket":
        item = event.get("item", {})
        if item.get("type") == "message":
            try:
                msg = web.conversations_replies(
                    channel=item["channel"],
                    ts=item["ts"],
                    limit=1,
                ).get("messages", [{}])[0]
                if msg.get("bot_id"):  # only delete Scout's own messages
                    web.chat_delete(channel=item["channel"], ts=item["ts"])
                    log.info(f"[delete] removed Scout message {item['ts']} in {item['channel']}")
            except Exception as e:
                log.warning(f"[delete] failed to delete {item.get('ts')}: {e}")
        return

    is_mention = event.get("type") == "app_mention"
    is_dm      = event.get("type") == "message" and event.get("channel_type") == "im"

    if not is_mention and not is_dm:
        return

    # Skip bot's own messages and message edits/deletions (subtypes)
    if event.get("bot_id") or event.get("subtype"):
        return

    channel  = event.get("channel")
    msg_ts   = event.get("ts")
    raw_text = event.get("text", "")

    if is_mention:
        thread_ts = event.get("thread_ts") or msg_ts
        query     = _strip_mention(raw_text)
    else:  # DM
        thread_ts = event.get("thread_ts")  # None for top-level DM — reply flat, not in a sub-thread
        query     = raw_text.strip()

    if not query:
        return

    log.info(f"Query from {event.get('user')}: {query!r}")
    user_id_event = event.get("user", "")
    user_id = user_id_event  # alias used by ask() and usage logging below

    # ── Correction capture — if this thread has a pending correction, store it ─
    learnings_state = _load_learnings()
    pending_corrs   = learnings_state.get("pending_corrections", {})
    if pending_corrs:
        # Check if any pending correction belongs to this thread
        matched_key = None
        for key, corr in pending_corrs.items():
            if corr.get("thread_ts") == thread_ts:
                matched_key = key
                break
        if matched_key:
            corr = pending_corrs.pop(matched_key)
            import uuid as _uuid
            learnings_state.setdefault("corrections", []).append({
                "id":            corr.get("id", str(_uuid.uuid4())[:8]),
                "created_at":    corr.get("created_at", ""),
                "query_hash":    corr.get("query_hash", ""),
                "correction":    query,
                "corrected_by":  user_id_event,
                "confidence":    "high",
            })
            learnings_state["pending_corrections"] = pending_corrs
            _save_learnings(learnings_state)
            web.chat_postMessage(
                channel=channel, thread_ts=thread_ts,
                text=":white_check_mark: Got it — I'll remember that.",
            )
            log.info(f"Correction captured for query_hash={corr.get('query_hash')}: {query[:80]!r}")
            return  # don't process this as a normal query

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

    # "force pulse" — admin command to trigger the pulse immediately to #scout-qa
    if re.search(r'\bforce\s+pulse\b', lower):
        web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                             text=":hourglass_flowing_sand: Running pulse signals now — will post to #scout-qa...")
        def _run_force_pulse():
            try:
                if _PULSE_RUNNER is None:
                    web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                         text=":x: Force pulse unavailable — pulse runner not initialized.")
                    return
                _PULSE_RUNNER(web, force=True)
                web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                     text=":white_check_mark: Force pulse complete — check #scout-qa.")
            except Exception as e:
                log.error(f"[force pulse] failed: {e}", exc_info=True)
                web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                     text=f":x: Force pulse failed: {e}")
        threading.Thread(target=_run_force_pulse, daemon=True).start()
        return

    # "force signal" / "force sniper" — run the offer digest immediately, posts to #scout-qa
    if re.search(r'\bforce\s+s(?:ignal|niper)\b', lower):
        web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                             text=":hourglass_flowing_sand: Running Scout Signal digest now — offer cards will post to #scout-qa...")
        def _run_force_sniper():
            try:
                import scout_digest
                offers_file = _DATA_DIR / "offers_latest.json"

                # If no offer data yet, run the scraper first so there's something to post
                if not offers_file.exists() or offers_file.stat().st_size < 100:
                    web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                         text=":screwdriver: No offer data yet — running scraper first (~60s)...")
                    try:
                        import offer_scraper
                        offer_scraper.run_all()
                    except Exception as scrape_err:
                        web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                             text=f":x: Scraper failed: `{scrape_err}`")
                        return

                # Check offer count before calling post_digest
                try:
                    import json as _j
                    offer_count = len(_j.loads(offers_file.read_text()))
                except Exception:
                    offer_count = 0

                if offer_count == 0:
                    web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                         text=":warning: Scraper ran but returned 0 offers — check network credentials in env vars.")
                    return

                scout_digest.post_digest(is_force=True)
                web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                     text=":white_check_mark: Signal digest posted to #scout-qa — click *Add to Queue* on any offer to test the flow.")
            except RuntimeError as e:
                # post_digest raises RuntimeError with filter breakdown when 0 offers pass
                log.warning(f"[force signal] 0 offers posted: {e}")
                web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                     text=f":warning: Force signal ran but no offers posted.\n{e}")
            except Exception as e:
                log.error(f"[force signal] failed: {e}", exc_info=True)
                web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                     text=f":x: Force signal failed: `{e}`")
        threading.Thread(target=_run_force_sniper, daemon=True).start()
        return

    # "QA yourself" / "self test" — run the QA suite with live per-question posting
    _QA_TRIGGERS = ("qa yourself", "self test", "run qa", "test yourself",
                    "run the qa suite", "scout qa", "run self-qa", "check yourself",
                    "run self qa", "qa suite")
    if any(t in lower for t in _QA_TRIGGERS):
        from scout_agent import _QA_SUITE
        import time as _time

        def _run_live_qa():
            try:
                import random as _random
                web.chat_postMessage(
                    channel=channel, thread_ts=thread_ts,
                    text=":test_tube: Scout Self-QA — 15 questions, live results",
                    blocks=[
                        {"type": "header", "text": {"type": "plain_text", "text": "Scout Self-QA"}},
                        {"type": "section", "text": {"type": "mrkdwn", "text": "Testing every major intent. Pass = responded + expected content present.\nPosting each result as it completes…"}},
                        {"type": "divider"},
                    ],
                )

                results = []
                groups = {
                    "Core Health": ["System status", "Dark offers"],
                    "Offer Intelligence": [
                        "Offer search — finance vertical",
                        "Offers for named publisher",
                        "Supply demand gaps",
                        "Offer inventory count",
                        "Pipeline health",
                    ],
                    "Revenue & Publisher": [
                        "WoW revenue drop",
                        "Publisher health",
                        "Campaign status check",
                        "Revenue projection",
                        "Perkswall engagement",
                        "Multi-part question decomposition",
                    ],
                    "Data Boundaries": [
                        "Data boundary — SOV",
                        "Data boundary — strategic intent",
                    ],
                }

                # Shuffle order each run so live results stream differently
                # and the suite clearly feels live rather than replaying cached output.
                qa_suite = list(_QA_SUITE)
                _random.shuffle(qa_suite)

                for label, question, pass_hints in qa_suite:
                    t0 = _time.monotonic()
                    try:
                        response = ask(question, history=[], user_id="self-qa")
                        elapsed = _time.monotonic() - t0
                        if isinstance(response, dict):
                            text = response.get("fallback_text") or response.get("text") or str(response)
                        else:
                            text = str(response)
                        responded = len(text.strip()) > 40
                        hint_match = any(h.lower() in text.lower() for h in pass_hints)
                        passed = responded and hint_match
                        snippet = text.strip()[:300].replace("\n", " ")
                    except Exception as e:
                        elapsed = _time.monotonic() - t0
                        passed = False
                        snippet = f"ERROR: {e}"

                    emoji_name = "white_check_mark" if passed else "x"
                    results.append({"label": label, "passed": passed, "elapsed": round(elapsed, 1), "snippet": snippet})

                    web.chat_postMessage(
                        channel=channel, thread_ts=thread_ts,
                        text=f"{'✅' if passed else '❌'} {label} · {round(elapsed, 1)}s",
                        blocks=[
                            {
                                "type": "rich_text",
                                "elements": [{
                                    "type": "rich_text_section",
                                    "elements": [
                                        {"type": "emoji", "name": emoji_name},
                                        {"type": "text", "text": f"  {label}", "style": {"bold": True}},
                                        {"type": "text", "text": f"  ·  {round(elapsed, 1)}s"},
                                    ],
                                }],
                            },
                            {
                                "type": "context",
                                "elements": [
                                    {"type": "mrkdwn", "text": f"Q: _{question[:80]}_"},
                                    {"type": "mrkdwn", "text": f"A: {snippet}{'…' if len(text.strip()) > 300 else ''}"},
                                ],
                            },
                        ],
                    )

                # Final scorecard — Block Kit
                passed_count = sum(1 for r in results if r["passed"])
                total = len(results)
                if passed_count >= 13:
                    overall = ":large_green_circle:"
                elif passed_count >= 9:
                    overall = ":large_yellow_circle:"
                else:
                    overall = ":red_circle:"

                scorecard_blocks: list = [
                    {"type": "divider"},
                    {"type": "section", "text": {"type": "mrkdwn", "text": f"{overall} *{passed_count}/{total} passed* — Scout self-QA complete."}},
                ]
                for group, labels in groups.items():
                    group_lines = []
                    for r in results:
                        if r["label"] in labels:
                            icon = ":white_check_mark:" if r["passed"] else ":x:"
                            group_lines.append(f"{icon}  {r['label']}  ·  {r['elapsed']}s")
                    if group_lines:
                        scorecard_blocks.append({
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": f"*{group}*\n" + "\n".join(group_lines)},
                        })

                failed = [r for r in results if not r["passed"]]
                action_line = (
                    f":zap: *Action:* {len(failed)} test(s) failed — check snippets above."
                    if failed else ":zap: All systems nominal."
                )
                scorecard_blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": action_line}],
                })

                web.chat_postMessage(
                    channel=channel, thread_ts=thread_ts,
                    text=f"{overall.strip(':')} {passed_count}/{total} passed — Scout self-QA complete.",
                    blocks=scorecard_blocks,
                )

            except Exception as e:
                log.error(f"[self-qa] failed: {e}", exc_info=True)
                web.chat_postMessage(channel=channel, thread_ts=thread_ts,
                                     text=f":x: Self-QA error: {e}")

        threading.Thread(target=_run_live_qa, daemon=True).start()
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

    # Smart trim: keep last 4 messages verbatim; summarize older ones into a single
    # entity-extraction line so context is preserved without ballooning token count.
    # Context block is injected AFTER this trim so it always lands at position 0.
    history = _smart_history(history)

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

    # ── DM path: emoji-reaction, no placeholder, no GIF, no spinner ─────────────
    if is_dm:
        # Add 🤔 reaction to the user's message — the "I saw it, thinking" signal.
        # Appears on their message specifically, not as a bot post. Disappears when ready.
        try:
            web.reactions_add(channel=channel, timestamp=msg_ts, name="thinking_face")
        except Exception:
            pass  # reactions:write scope may not be set yet — degrade gracefully

        try:
            _t0 = time.monotonic()
            response = ask(query, history=history, user_id=user_id)
            _elapsed = int(time.monotonic() - _t0)
            _elapsed_str = f"{_elapsed}s" if _elapsed < 60 else f"{_elapsed // 60}m {_elapsed % 60}s"
            _tools_called = response.get("tools_called", []) if isinstance(response, dict) else []
            try:
                user_info = web.users_info(user=user_id)
                _uname = (user_info.get("user", {}).get("profile", {}).get("display_name", "")
                          or user_info.get("user", {}).get("name", user_id))
            except Exception:
                _uname = user_id
            _log_usage(user_id, _uname, query, _tools_called, _elapsed * 1000)
        except Exception as e:
            log.error(f"Agent error (DM): {e}", exc_info=True)
            try:
                web.reactions_remove(channel=channel, timestamp=msg_ts, name="thinking_face")
            except Exception:
                pass
            web.chat_postMessage(channel=channel, text=f":warning: Something went wrong — `{e}`")
            return
        finally:
            # Always remove the 🤔 — even on error — so it doesn't hang
            try:
                web.reactions_remove(channel=channel, timestamp=msg_ts, name="thinking_face")
            except Exception:
                pass

        # Track active thread for follow-up context retention
        with _LAST_THREAD_LOCK:
            _LAST_THREAD_PER_CHANNEL[channel] = thread_ts or msg_ts

        # Extract structured context + suggestion buttons
        suggestions: list = []
        if isinstance(response, dict) and response.get("type") == "text_with_context":
            extracted = response.get("extracted_context", {})
            if extracted:
                launched_offer_dm = extracted.pop("launched_offer", None)
                _merge_thread_context(thread_ts or msg_ts, extracted)
            suggestions = response.get("suggestions", [])
            response = response["text"]

        # Post reply — flat DM message (thread_ts=None) or in-thread if user was already in one
        if isinstance(response, dict) and response.get("type") == "brief":
            brief_data = response["brief_data"]
            copy       = response["copy"]
            _store_brief(thread_ts or msg_ts, brief_data, copy)
            _merge_thread_context(thread_ts or msg_ts, {
                "offer":       brief_data.get("advertiser"),
                "payout":      brief_data.get("payout_num"),
                "payout_type": (brief_data.get("payout_type") or "CPA").upper(),
            })
            blocks = _build_brief_blocks(brief_data, copy, thread_ts=thread_ts)
            web.chat_postMessage(
                channel=channel, thread_ts=thread_ts,
                text=response.get("fallback_text", "Campaign Brief ready."),
                blocks=blocks,
                unfurl_links=False,
            )
        elif isinstance(response, dict) and response.get("type") == "opportunities":
            header_text       = _sanitize_slack(response.get("text", ""))
            offer_cards       = _build_opportunity_cards(response.get("offers", []), thread_ts=thread_ts)
            suggestion_blocks = _build_suggestion_buttons(response.get("suggestions", []))
            all_blocks        = [*(_text_to_blocks(header_text) if header_text else []), *offer_cards, *suggestion_blocks]
            web.chat_postMessage(
                channel=channel, thread_ts=thread_ts,
                text=header_text or "Top opportunities",
                blocks=all_blocks,
                unfurl_links=False,
            )
        else:
            response_text     = _sanitize_slack(response if isinstance(response, str) else str(response))
            content_blocks    = _text_to_blocks(response_text)
            suggestion_blocks = _build_suggestion_buttons(suggestions)
            # No elapsed-time footer in DMs — the reaction disappearing IS the signal
            web.chat_postMessage(
                channel=channel, thread_ts=thread_ts,
                text=response_text,
                blocks=[*content_blocks, *suggestion_blocks],
                unfurl_links=False,
            )
        return
    # ── END DM path ──────────────────────────────────────────────────────────────

    try:
        web.chat_typing(channel=channel)
    except Exception:
        pass
    _msg_text = _pick_loading_message(query)
    placeholder = web.chat_postMessage(
        channel=channel, thread_ts=thread_ts, text=_msg_text,
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
    )
    _placeholder_ts = placeholder["ts"]
    stop_rotating = _rotating_status(web, channel, _placeholder_ts)

    try:
        _t0 = time.monotonic()
        response = ask(query, history=history, user_id=user_id)
        _elapsed = int(time.monotonic() - _t0)
        _elapsed_str = f"{_elapsed}s" if _elapsed < 60 else f"{_elapsed // 60}m {_elapsed % 60}s"
        # Log usage for admin reporting
        _tools_called = response.get("tools_called", []) if isinstance(response, dict) else []
        try:
            user_info = web.users_info(user=user_id)
            _uname = (user_info.get("user", {}).get("profile", {}).get("display_name", "")
                      or user_info.get("user", {}).get("name", user_id))
        except Exception:
            _uname = user_id
        _log_usage(user_id, _uname, query, _tools_called, _elapsed * 1000)
    except Exception as e:
        log.error(f"Agent error: {e}")
        stop_rotating()
        _post_error_update(web, channel, placeholder["ts"], e)
        return
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

        # Sync Notion Demand Queue page to "Live" status
        _notion_url = launched_offer.get("notion_url", "")
        if _notion_url:
            threading.Thread(
                target=_update_notion_status,
                args=(_notion_url, "Live"),
                daemon=True,
            ).start()

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

        # Async tracking URL check — fires when brief is first shown, before any queue action
        _real_url = (brief_data.get("tracking_url", "") or "").strip()
        if _real_url and not _real_url.startswith("Not available"):
            _run_preflight_qa(web, channel, thread_ts, brief_data)

    elif isinstance(response, dict) and response.get("type") == "opportunities":
        header_text       = _sanitize_slack(response.get("text", ""))
        offer_cards       = _build_opportunity_cards(response.get("offers", []), thread_ts=thread_ts)
        suggestion_blocks = _build_suggestion_buttons(response.get("suggestions", []))
        elapsed_ctx       = {"type": "context", "elements": [{"type": "mrkdwn", "text": f"_Scout · {_elapsed_str}_"}]}
        all_blocks        = [*(_text_to_blocks(header_text) if header_text else []), *offer_cards, *suggestion_blocks, elapsed_ctx]
        web.chat_update(
            channel=channel,
            ts=_placeholder_ts,
            text=header_text or "Top opportunities",
            blocks=all_blocks,
        )
        log.info(f"Posted opportunity cards ({len(response.get('offers', []))} offers) in {channel}")

    else:
        # Plain text response — clean text only at reveal, no GIF (GIF was shown during loading)
        response_text     = _sanitize_slack(response if isinstance(response, str) else str(response))
        content_blocks    = _text_to_blocks(response_text)
        suggestion_blocks = _build_suggestion_buttons(suggestions)
        web.chat_update(
            channel=channel,
            ts=_placeholder_ts,
            text=response_text,
            blocks=[*content_blocks, *suggestion_blocks,
                    {"type": "context", "elements": [{"type": "mrkdwn", "text": f"_Scout · {_elapsed_str}_"}]}],
        )
        log.info(f"Responded in {channel} (thread {thread_ts}), suggestions={len(suggestions)}")

