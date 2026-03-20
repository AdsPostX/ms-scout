"""
Scout — Slack Bot (Socket Mode)
Listens for @Scout mentions and responds with offer intelligence.
Run as a persistent background process: python scout_bot.py
"""

import logging
import os
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

# ── In-memory state ───────────────────────────────────────────────────────────
# Keyed by thread_ts. Stores the last brief + copy so "launch this" / "approve"
# have context without re-querying the agent.
_PENDING_BRIEFS: dict = {}      # thread_ts → {"brief_data": {...}, "copy": {...}}
_PENDING_LAUNCHES: dict = {}    # thread_ts → {"brief_data": {...}, "copy": {...}} (pre-approval)


def _strip_mention(text: str) -> str:
    """Remove @mention tokens so the agent sees the clean query."""
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()


# ── Block Kit brief builder ───────────────────────────────────────────────────

def _build_brief_blocks(brief_data: dict, copy: dict) -> list:
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

    titles    = copy.get("titles", [])
    ctas      = copy.get("ctas", [])
    targeting = copy.get("targeting", "")
    bottom    = copy.get("bottom_line", "")

    blocks = []

    # Hero image (if available and publicly accessible)
    if hero_url and hero_url.startswith("http"):
        blocks.append({
            "type": "image",
            "image_url": hero_url,
            "alt_text": advertiser,
        })

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"Campaign Brief — {advertiser}", "emoji": False},
    })

    # Key stats row
    stat_fields = [
        {"type": "mrkdwn", "text": f"*Network*\n{network}"},
        {"type": "mrkdwn", "text": f"*Payout*\n{payout}"},
        {"type": "mrkdwn", "text": f"*Geo*\n{geo or 'Not specified'}"},
    ]
    if performance:
        stat_fields.append({"type": "mrkdwn", "text": f"*Performance*\n{performance}"})
    blocks.append({"type": "section", "fields": stat_fields})

    blocks.append({"type": "divider"})

    # Title options
    if titles:
        title_lines = "\n".join(f"{i+1}. {t}" for i, t in enumerate(titles))
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Title options:*\n{title_lines}"},
        })

    # CTA options
    if ctas:
        cta_lines = "\n".join(f'• Yes: "{c.get("yes","")}" / No: "{c.get("no","")}"' for c in ctas)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*CTA options:*\n{cta_lines}"},
        })

    # Targeting + tracking URL
    detail_parts = []
    if targeting:
        detail_parts.append(f"*Targeting:* {targeting}")
    if tracking_url:
        detail_parts.append(f"*Tracking URL:* `{tracking_url}`")
    if offer_id:
        detail_parts.append(f"*Creatives:* Pull from {network} portal · Offer ID: `{offer_id}`")
    if detail_parts:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(detail_parts)},
        })

    blocks.append({"type": "divider"})

    # Bottom line + launch instruction
    footer_text = ""
    if bottom:
        footer_text = f"_{bottom}_\n"
    footer_text += "Reply `@Scout launch this` to build in the MS platform."

    context_elements = []
    if icon_url and icon_url.startswith("http"):
        context_elements.append({"type": "image", "image_url": icon_url, "alt_text": advertiser})
    context_elements.append({"type": "mrkdwn", "text": footer_text})
    blocks.append({"type": "context", "elements": context_elements})

    return blocks


# ── Command handlers ──────────────────────────────────────────────────────────

def _handle_launch(channel: str, thread_ts: str, web: WebClient):
    """Trigger Playwright campaign builder for the last brief in this thread."""
    pending = _PENDING_BRIEFS.get(thread_ts)
    if not pending:
        web.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text="_No brief found in this thread. Ask me to build an offer first._",
        )
        return

    brief_data = pending["brief_data"]
    copy       = pending["copy"]
    advertiser = brief_data.get("advertiser", "Offer")

    # Store for approval
    _PENDING_LAUNCHES[thread_ts] = pending

    # Post acknowledgement
    web.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=f"_Building *{advertiser}* in the MS platform... filling all 4 tabs. I'll post a screenshot when it's ready for approval._",
    )

    try:
        from campaign_builder import launch_offer_in_background
        launch_offer_in_background(brief_data, copy, channel, thread_ts, web)
    except ImportError:
        web.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=(
                "_Campaign Builder not configured yet. "
                "Run `python campaign_builder.py --login` to set up MS platform access, "
                "then restart Scout._"
            ),
        )
    except Exception as e:
        log.error(f"Campaign builder error: {e}")
        web.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f"Campaign builder error: {e}",
        )


def _handle_approve(channel: str, thread_ts: str, web: WebClient):
    """Submit the pending offer after human approval."""
    try:
        from campaign_builder import submit_pending_offer
        submitted = submit_pending_offer(thread_ts, channel, web)
        if not submitted:
            web.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text="_Nothing pending approval in this thread. Use `@Scout launch this` first._",
            )
    except ImportError:
        web.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text="_Campaign Builder not configured yet._",
        )
    except Exception as e:
        log.error(f"Approval error: {e}")
        web.chat_postMessage(channel=channel, thread_ts=thread_ts, text=f"Approval error: {e}")


# ── Main event handler ────────────────────────────────────────────────────────

def handle_event(client: SocketModeClient, req: SocketModeRequest):
    # Acknowledge immediately — Slack requires <3s ack
    client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))

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

    web = WebClient(token=BOT_TOKEN)
    lower = query.lower()

    # ── Special commands (handled before agent) ───────────────────────────────
    if re.search(r"\blaunch\s*(this|it)\b", lower):
        _handle_launch(channel, thread_ts, web)
        return

    if re.search(r"^\s*approve\s*$", lower):
        _handle_approve(channel, thread_ts, web)
        return

    # ── Thread history for context ────────────────────────────────────────────
    history = []
    if event.get("thread_ts") and event.get("thread_ts") != msg_ts:
        try:
            replies = web.conversations_replies(channel=channel, ts=thread_ts, limit=20)
            bot_id  = web.auth_test()["user_id"]
            for msg in replies.get("messages", []):
                if msg.get("ts") == msg_ts:
                    break
                role = "assistant" if (msg.get("bot_id") or msg.get("user") == bot_id) else "user"
                text = _strip_mention(msg.get("text", "")).strip()
                if text:
                    history.append({"role": role, "content": text})
        except Exception as e:
            log.warning(f"Could not fetch thread history: {e}")

    # Post placeholder
    placeholder = web.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text="_Searching inventory..._",
    )

    try:
        response = ask(query, history=history)
    except Exception as e:
        log.error(f"Agent error: {e}")
        response = f"Something went wrong: {e}"

    # ── Route response: brief (Block Kit) vs plain text ───────────────────────
    if isinstance(response, dict) and response.get("type") == "brief":
        brief_data = response["brief_data"]
        copy       = response["copy"]

        # Store in state so "launch this" works later
        _PENDING_BRIEFS[thread_ts] = {"brief_data": brief_data, "copy": copy}

        blocks       = _build_brief_blocks(brief_data, copy)
        fallback_text = response.get("fallback_text", "Campaign Brief ready.")

        web.chat_update(
            channel=channel,
            ts=placeholder["ts"],
            text=fallback_text,
            blocks=blocks,
        )
        log.info(f"Posted Block Kit brief for {brief_data.get('advertiser')} in {channel}")

    else:
        # Plain text response
        text = response if isinstance(response, str) else str(response)
        web.chat_update(
            channel=channel,
            ts=placeholder["ts"],
            text=text,
            mrkdwn=True,
        )
        log.info(f"Responded in {channel} (thread {thread_ts})")


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
