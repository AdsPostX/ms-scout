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


def _strip_mention(text: str) -> str:
    """Remove leading @mention so the agent gets the clean query."""
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()


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

    channel = event.get("channel")
    thread_ts = event.get("thread_ts") or event.get("ts")
    raw_text = event.get("text", "")
    query = _strip_mention(raw_text)

    if not query:
        return

    log.info(f"Query from {event.get('user')}: {query!r}")

    web = WebClient(token=BOT_TOKEN)

    # Post a placeholder, then replace it with the real response
    placeholder = web.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text="_Searching inventory..._",
    )

    try:
        response = ask(query)
    except Exception as e:
        log.error(f"Agent error: {e}")
        response = f"Something went wrong: {e}"

    # Update the placeholder in-place — single clean message, no double-post
    web.chat_update(
        channel=channel,
        ts=placeholder["ts"],
        text=response,
        mrkdwn=True,
    )
    log.info(f"Responded in {channel} (thread {thread_ts})")


def main():
    if not BOT_TOKEN or not APP_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env")

    web_client = WebClient(token=BOT_TOKEN)
    socket_client = SocketModeClient(app_token=APP_TOKEN, web_client=web_client)
    socket_client.socket_mode_request_listeners.append(handle_event)

    log.info("Scout is online — listening for @mentions via Socket Mode")
    socket_client.connect()

    # Keep running
    import signal
    signal.pause()


if __name__ == "__main__":
    main()
