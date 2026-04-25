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
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler
from slack_sdk.web import WebClient

from scout_agent import ask
from scout_notion import (
    _COPY_CACHE, _COPY_CACHE_TTL, _COPY_CACHE_LOCK,
    _COPY_QUEUE, _COPY_QUEUE_LOCK, _COPY_QUEUE_EVENT, _COPY_COALESCE_WINDOW,
    _copy_cache_key, _copy_cache_get, _copy_cache_set,
    _generate_offer_copy, _patch_notion_copy, _enrich_notion_with_ai_copy,
    _generate_offer_copy_batch, _queue_copy_enrichment, _copy_coalescer_loop,
    _write_to_notion_queue, _update_notion_status,
    _check_notion_queue_changes, _notion_watcher_loop,
)
from scout_slack_ui import (
    _pitch_signal, _queue_confirm_blocks, _build_brief_blocks,
    _build_opportunity_cards, _is_help_query, _parse_inline_elements,
    _text_to_blocks, _build_suggestion_buttons, _build_help_blocks,
    _build_feedback_buttons, _format_pulse_blocks,
    _build_home_queue_section, _build_home_view,
    _HELP_TRIGGERS, _EMOJI_ALIASES, _INLINE_RE, _HOME_EXAMPLES,
)
from scout_state import (
    _DATA_DIR,
    _atomic_write,
    _load_briefs, _save_briefs, _store_brief, _get_brief, _delete_brief,
    _load_thread_contexts, _save_thread_contexts, _get_thread_context, _merge_thread_context,
    _load_launched_offers, _save_launched_offers,
    _load_pulse_state, _save_pulse_state,
    _load_watchdog_state, _save_watchdog_state,
    _load_learnings, _save_learnings,
    _load_notion_notified, _save_notion_notified,
    _log_usage, _update_benchmark_from_actuals,
    _LAUNCHED_OFFERS_FILE, _PULSE_STATE_FILE, _NOTION_NOTIFIED_FILE,
    _LEARNINGS_FILE, _LEARNED_BENCHMARKS_FILE, _WATCHDOG_STATE_PATH,
)

load_dotenv()  # plist env vars (SCOUT_ENV, PULSE_CHANNEL, etc.) take precedence over .env

# Mutex: prevents daemon and on-demand trigger from running run_headless() concurrently.
# On-demand run_offer_scraper() checks this before calling run_headless().
_SCRAPER_RUNNING = threading.Event()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scout_bot")

BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
APP_TOKEN = os.getenv("SLACK_APP_TOKEN")

_LAST_THREAD_PER_CHANNEL: dict = {}  # channel → thread_ts
_LAST_THREAD_LOCK = threading.Lock()
_BOT_USER_ID: str = ""  # cached at startup — never changes


# ── Loading message pools ─────────────────────────────────────────────────────
# Each entry: {"text": "...", "tone": "grind" | "late"}
# tone="late" messages only appear after 9 PM Chicago time.
# Pools are selected by query context; Giphy tag varies per pool.

_MESSAGE_POOLS: dict[str, list[dict]] = {
    # Brief / campaign building requests
    "pool_brief": [
        {"text": "_🎯 Matching offer to user at the exact moment they're most likely to convert..._", "tone": "grind"},
        {"text": "_Treating your thank-you page like a revenue line item..._",                        "tone": "grind"},
        {"text": "_The confirmation page is prime real estate. Treating it accordingly..._",          "tone": "grind"},
        {"text": "_📈 Quietly outperforming every other SDK on the confirmation page..._",            "tone": "grind"},
        {"text": "_Doing what the SDK does but with words..._",                                       "tone": "grind"},
        {"text": "_The sale just closed. The real work just started..._",                             "tone": "grind"},
        {"text": "_Finding what your publishers are leaving on the table..._",                        "tone": "grind"},
        {"text": "_💰 Manifesting a 15% RPM lift for you specifically..._",                          "tone": "grind"},
        {"text": "_Building this brief so good it'll basically sell itself..._",                      "tone": "late"},
        {"text": "_It's late. The confirmation page does not care. Neither does Scout..._",           "tone": "late"},
    ],
    # Data / performance queries
    "pool_data": [
        {"text": "_🔍 Interrogating ClickHouse until it confesses..._",                              "tone": "grind"},
        {"text": "_Doing math so advanced it scared the last analyst..._",                           "tone": "grind"},
        {"text": "_📊 Converting raw SQL into something humans can feel..._",                        "tone": "grind"},
        {"text": "_🕵️ Forensic accounting but for ad performance..._",                               "tone": "grind"},
        {"text": "_Consulting 47 data sources and their weird rate limits..._",                      "tone": "grind"},
        {"text": "_Refusing to guess and actually querying the data..._",                            "tone": "grind"},
        {"text": "_Sorting by RPM, not vibes (vibes are terrible analytics)..._",                   "tone": "grind"},
        {"text": "_Running this through seven layers of analysis (three are real)..._",              "tone": "grind"},
        {"text": "_Making decisions your BI dashboard is too cowardly to make..._",                  "tone": "grind"},
        {"text": "_🧠 The AI is confident. The AI is always confident. Question the AI..._",        "tone": "late"},
        {"text": "_Lovingly nagging ClickHouse for one more row at this hour..._",                   "tone": "late"},
    ],
    # Queue / ops / status queries
    "pool_ops": [
        {"text": "_AdOps is sleeping. Scout is not._",                                               "tone": "grind"},
        {"text": "_📡 Pinging every network so you don't have to..._",                              "tone": "grind"},
        {"text": "_Making MomentScience look good, one query at a time..._",                         "tone": "grind"},
        {"text": "_Reverse-engineering what competitors are too slow to notice..._",                 "tone": "grind"},
        {"text": "_AdOps is definitely asleep. Scout is very much not._",                            "tone": "late"},
        {"text": "_The queue doesn't close at 5. Neither does Scout..._",                            "tone": "late"},
    ],
    # Publisher / partner / network queries
    "pool_publisher": [
        {"text": "_Asking MaxBounty to explain itself..._",                                          "tone": "grind"},
        {"text": "_Speed-running the Impact catalog..._",                                            "tone": "grind"},
        {"text": "_⏳ Waiting for FlexOffers to respond. This is normal..._",                       "tone": "grind"},
        {"text": "_Bribing the affiliate networks for better rates..._",                             "tone": "grind"},
        {"text": "_🤝 Diplomatically informing the affiliate networks their tracking is... fine..._","tone": "grind"},
        {"text": "_Aggressively filtering out the garbage..._",                                      "tone": "grind"},
        {"text": "_Professionally judging low-payout offers and finding them wanting..._",           "tone": "grind"},
        {"text": "_The affiliate networks are asleep. Their APIs unfortunately are not..._",         "tone": "late"},
    ],
    # Generic catch-all
    "pool_generic": [
        {"text": "_One moment — Scout is having a moment..._",                                       "tone": "grind"},
        {"text": "_Approximately 40% confident this will be good news..._",                          "tone": "grind"},
        {"text": "_Pretending this is easy (it is not)..._",                                         "tone": "grind"},
        {"text": "_Almost done — and by 'almost' Scout means it just started..._",                   "tone": "grind"},
        {"text": "_This is taking longer than expected, which means it's thorough..._",              "tone": "grind"},
        {"text": "_Vigorously cross-referencing things..._",                                         "tone": "grind"},
        {"text": "_Turning post-transaction moments into money (allegedly)..._",                     "tone": "grind"},
        {"text": "_Telepathically downloading CVR benchmarks..._",                                   "tone": "grind"},
        {"text": "_Gently terrorizing the affiliate APIs..._",                                       "tone": "grind"},
        {"text": "_⚡ Running night queries. Scout doesn't have a bedtime..._",                     "tone": "late"},
        {"text": "_Probably should have waited until morning. Scout disagrees..._",                  "tone": "late"},
    ],
}

# Flat list for _rotating_status (all grind-tone messages, pool-agnostic)
_LOADING_MESSAGES = [e["text"] for pool in _MESSAGE_POOLS.values() for e in pool if e["tone"] == "grind"]

# ── Giphy integration ─────────────────────────────────────────────────────────
_GIPHY_CACHE: dict[str, tuple[str, float]] = {}  # tag → (url, fetched_at)
_GIPHY_CACHE_TTL = 600  # 10 minutes — keeps well under 100 calls/hr free limit


def _fetch_giphy_url(tag: str) -> str | None:
    """Fetch a GIF URL via Giphy search. Returns None on any failure."""
    api_key = os.getenv("GIPHY_API_KEY", "")
    if not api_key:
        return None
    cached_url, cached_at = _GIPHY_CACHE.get(tag, ("", 0.0))
    if cached_url and (time.time() - cached_at) < _GIPHY_CACHE_TTL:
        return cached_url
    try:
        import urllib.request as _ur
        import urllib.parse as _up
        import json as _json
        offset = random.randint(0, 8)  # variety without staleness
        url = (
            f"https://api.giphy.com/v1/gifs/search"
            f"?api_key={api_key}&q={_up.quote(tag)}&rating=pg-13&limit=10&offset={offset}"
        )
        with _ur.urlopen(url, timeout=3) as r:
            data = _json.loads(r.read())
        results = data.get("data", [])
        if not results:
            return None
        gif_url = results[0]["images"]["fixed_height"]["url"]
        _GIPHY_CACHE[tag] = (gif_url, time.time())
        return gif_url
    except Exception:
        return None


def _pick_loading_message(query: str = "") -> tuple[str, str]:
    """
    Pick a context-aware loading message + Giphy tag based on query content and time of day.
    Late-night (9 PM–6 AM Chicago): pull from late-tone entries.
    Returns (message_text, giphy_tag).
    """
    from datetime import datetime
    import pytz

    try:
        chicago = pytz.timezone("America/Chicago")
        hour = datetime.now(chicago).hour
        is_late = hour >= 21 or hour < 6
    except Exception:
        is_late = False

    q = (query or "").lower()

    if any(w in q for w in ("brief", "campaign", "build a brief", "draft", "write")):
        pool_key, giphy_tag = "pool_brief", "wolf of wall street working"
    elif any(w in q for w in ("queue", "status", "pending", "live", "launch", "enter")):
        pool_key, giphy_tag = "pool_ops", "mission impossible"
    elif any(w in q for w in ("revenue", "projection", "gross", "cap", "budget", "forecast")):
        pool_key, giphy_tag = "pool_data", "breaking bad i am the one who knocks"
    elif any(w in q for w in ("performance", "rpm", "cvr", "data", "benchmark", "report", "rank")):
        pool_key, giphy_tag = "pool_data", "moneyball"
    elif any(w in q for w in ("publisher", "partner", "at&t", "pch", "offerup", "integration", "network")):
        pool_key, giphy_tag = "pool_publisher", "succession"
    elif any(w in q for w in ("find", "opportunity", "untapped", "gap", "search", "what are")):
        pool_key, giphy_tag = "pool_generic", "indiana jones searching"
    else:
        pool_key, giphy_tag = "pool_generic", "sherlock holmes thinking"

    pool = _MESSAGE_POOLS[pool_key]
    tone = "late" if is_late else "grind"
    candidates = [e for e in pool if e["tone"] == tone] or pool  # fallback to all if tone not found
    return random.choice(candidates)["text"], giphy_tag


def _clean_error(err: Exception) -> tuple[str, str]:
    """
    Convert a raw exception into a (human_message, giphy_tag) pair.
    Strips Anthropic API JSON blobs — never dumps them to Slack.
    """
    s = str(err)
    if "429" in s or "rate_limit" in s:
        msg = "Scout hit the rate limit — give it 60 seconds and try again."
        tag = "office michael scott too much"
    elif "credit balance" in s.lower() or ("400" in s and "credit" in s.lower()):
        msg = "Scout is out of Anthropic credits — ping Sidd to top up at console.anthropic.com."
        tag = "office michael scott money"
    elif "529" in s or "overloaded" in s:
        msg = "Anthropic is slammed right now — try again in a minute."
        tag = "it crowd have you tried turning it off"
    elif "timeout" in s.lower() or "timed out" in s.lower():
        msg = "Scout timed out — try a narrower question."
        tag = "arrested development im on it"
    elif "connection" in s.lower() or "network" in s.lower():
        msg = "Scout just restarted (deploy or crash) — please resend your message."
        tag = "it crowd internet"
    else:
        msg = "Something broke — try again, or rephrase the question."
        tag = "arrested development but why"
    return msg, tag


def _post_error_update(web: WebClient, channel: str, ts: str, err: Exception) -> None:
    """Replace the loading placeholder with a clean error block."""
    msg, _ = _clean_error(err)
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f":warning: *Scout hit a snag* — {msg}"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": "If this keeps happening, check Render logs."}]},
    ]
    try:
        web.chat_update(channel=channel, ts=ts, text=msg, blocks=blocks)
    except Exception as _e:
        log.warning(f"_post_error_update: could not update {channel}:{ts}: {_e}")


def _rotating_status(
    web: WebClient,
    channel: str,
    ts: str,
    gif_block: list | None = None,
    interval: float = 4.0,
):
    """
    Rotates the loading placeholder message every `interval` seconds.
    Returns a stop() callable — call it when the real response is ready.
    gif_block: shared list reference — populated async by _inject_loading_gif.
               Each rotation update re-reads it so the GIF persists once it arrives.
    """
    stop_event = threading.Event()
    start = time.monotonic()
    msgs = _LOADING_MESSAGES[:]
    random.shuffle(msgs)
    idx = [0]
    _gif = gif_block if gif_block is not None else []

    def _run():
        while not stop_event.wait(interval):
            elapsed = int(time.monotonic() - start)
            msg = msgs[idx[0] % len(msgs)]
            core = msg.strip("_")
            update_text = f"_{core}_ · {elapsed}s"
            try:
                web.chat_update(
                    channel=channel, ts=ts, text=update_text,
                    blocks=[*_gif, {"type": "section", "text": {"type": "mrkdwn", "text": update_text}}],
                )
            except Exception:
                log.debug("suppressed: rotating status update failed", exc_info=True)
            idx[0] += 1

    threading.Thread(target=_run, daemon=True).start()
    return stop_event.set


def _strip_mention(text: str) -> str:
    """Remove @mention tokens so the agent sees the clean query."""
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()


def _sanitize_slack(text: str) -> str:
    import re as _re
    text = _re.sub(r'\*\*(.+?)\*\*', r'*\1*', text)
    text = _re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)', r'<\2|\1>', text)
    text = _re.sub(r'^#{1,3} (.+)$', r'*\1*', text, flags=_re.MULTILINE)
    text = _re.sub(r'^---+$', '', text, flags=_re.MULTILINE)
    return text


# ── Queue confirmation Block Kit helpers ─────────────────────────────────────





# ── Block Kit brief builder ───────────────────────────────────────────────────

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
            log.debug("suppressed: pre-flight benchmark lookup failed", exc_info=True)

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
            log.warning("Failed to post pre-flight check to Slack")

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




# ── Opportunity cards ─────────────────────────────────────────────────────────



# ── Help / capabilities card ──────────────────────────────────────────────────





# Tokenizer for inline elements within a single text line.
# Groups: bold_d (**), bold_s (*), italic, code, emoji, link, user, plain










# ── Scout Signal: approve / reject handlers ───────────────────────────────────

_SCOUT_HQ_CHANNEL  = "C0AQEECF800"   # #scout-qa (was #scout-hq)


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


# ── AI copy coalescer: cache + batch queue ────────────────────────────────────
# Keyed by (advertiser.lower(), payout_type, category) → copy dict + expiry

# Pending enrichment jobs: list of (notion_url, offer_kwargs_dict)




















def _slack_thread_url(channel: str, thread_ts: str) -> str:
    """Build a direct link to a Slack thread message."""
    ts_nodot = thread_ts.replace(".", "")
    return f"https://momentscience.slack.com/archives/{channel}/p{ts_nodot}"


# Fill rate exclusions are now managed dynamically via data/entity_overrides.json.
# Use _load_entity_overrides() at pulse time (imported from scout_agent).
# Seeded with Button on first deploy by _seed_entity_overrides() in main().
_PULSE_CHANNEL               = os.getenv("PULSE_CHANNEL", "")  # kept for backwards compat
_PULSE_ENABLED               = os.getenv("PULSE_ENABLED", "true").lower() == "true"

# ── Environment-aware channel routing ─────────────────────────────────────────
# SCOUT_ENV=production → messages go to production channels (set in launchd plist)
# Anything else (unset, "development") → everything goes to #scout-qa
# force=True → always #scout-qa regardless of environment
_SCOUT_ENV = os.getenv("SCOUT_ENV", "development")
_PRODUCTION_CHANNELS = {
    "pulse":    os.getenv("PULSE_CHANNEL", _SCOUT_HQ_CHANNEL),          # #revenue-operations
    "watchdog": os.getenv("PULSE_CHANNEL", _SCOUT_HQ_CHANNEL),          # #revenue-operations
    "offers":   os.getenv("SCOUT_DIGEST_CHANNEL", _SCOUT_HQ_CHANNEL),   # #scout-offers
}

def _route_channel(purpose: str, force: bool = False) -> str:
    """
    Return the correct Slack channel for a given message purpose.
    Foolproof: force=True OR non-production env always routes to #scout-qa.
    Production channels require SCOUT_ENV=production (set in launchd plist only).
    """
    if force or _SCOUT_ENV != "production":
        return _SCOUT_HQ_CHANNEL
    return _PRODUCTION_CHANNELS.get(purpose, _SCOUT_HQ_CHANNEL)


# ── Feedback buttons ──────────────────────────────────────────────────────────



# ── Pulse signal helpers (one per signal, each owns its own ch connection) ────

def _pulse_signal_cap(ch) -> list:
    import json as _json
    from datetime import date as _date
    import calendar as _cal
    results = []
    try:
        cap_rows = ch.query(
            """
            SELECT
                c.id          AS campaign_id,
                c.adv_name,
                c.capping_config,
                coalesce(sum(toFloat64OrNull(cv.revenue)), 0) AS revenue_this_month
            FROM from_airbyte_campaigns c
            LEFT JOIN adpx_conversionsdetails cv
                ON toInt64(cv.campaign_id) = c.id
                AND toYYYYMM(cv.created_at) = toYYYYMM(today())
            WHERE c.deleted_at IS NULL
              AND c.capping_config IS NOT NULL
              AND c.capping_config != ''
              AND c.capping_config != 'null'
            GROUP BY c.id, c.adv_name, c.capping_config
            """
        ).result_rows
        today_d = _date.today()
        days_in_month = _cal.monthrange(today_d.year, today_d.month)[1]
        days_remaining = days_in_month - today_d.day + 1
        for camp_id, adv_name, cap_cfg, revenue_mtd in cap_rows:
            try:
                cfg = _json.loads(cap_cfg) if isinstance(cap_cfg, str) else (cap_cfg or {})
                mb  = float((cfg.get("month") or {}).get("budget") or 0)
            except Exception:
                mb = 0.0
            if mb <= 0:
                continue
            cap_pct = revenue_mtd / mb
            if cap_pct < 0.70:
                continue
            daily_run_rate = revenue_mtd / max(today_d.day, 1)
            days_to_cap    = (mb - revenue_mtd) / daily_run_rate if daily_run_rate > 0 else 999
            results.append({
                "adv_name":       adv_name,
                "campaign_id":    int(camp_id) if camp_id else None,
                "monthly_cap":    mb,
                "revenue_mtd":    round(revenue_mtd, 2),
                "cap_pct":        round(cap_pct * 100, 1),
                "days_remaining": days_remaining,
                "days_to_cap":    round(days_to_cap, 1),
            })
        results.sort(key=lambda x: x["cap_pct"], reverse=True)
    except Exception as e:
        log.warning(f"Pulse cap signal failed: {e}")
    return results


def _pulse_signal_velocity(ch) -> list:
    results: list = []
    try:
        vel_rows = ch.query(
            """
            SELECT
                user_id,
                sum(toFloat64OrNull(revenue))                                           AS revenue_30d,
                sumIf(toFloat64OrNull(revenue), created_at >= today() - 7)              AS revenue_7d
            FROM adpx_conversionsdetails
            PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
            WHERE created_at >= today() - 30
            GROUP BY user_id
            HAVING revenue_30d > 5000
            ORDER BY revenue_30d DESC
            LIMIT 200
            """
        ).result_rows

        uid_list = [str(r[0]) for r in vel_rows if r[0]]
        org_map: dict = {}
        if uid_list:
            try:
                id_csv = ",".join(uid_list[:200])
                name_rows = ch.query(
                    f"SELECT id, organization FROM from_airbyte_users WHERE id IN ({id_csv}) LIMIT 200"
                ).result_rows
                org_map = {str(r[0]): r[1] for r in name_rows}
            except Exception:
                log.debug("suppressed: publisher name enrichment query failed", exc_info=True)

        for user_id, rev_30d, rev_7d in vel_rows:
            rev_7d_ann = (rev_7d / 7) * 30 if rev_7d else 0
            if rev_30d <= 0:
                continue
            pct_delta = (rev_7d_ann - rev_30d) / rev_30d * 100
            if abs(pct_delta) < 40:
                continue
            results.append({
                "publisher_name":  org_map.get(str(user_id), f"Partner {user_id}"),
                "publisher_id":    int(user_id) if user_id else None,
                "revenue_30d":     round(rev_30d, 2),
                "revenue_7d_ann":  round(rev_7d_ann, 2),
                "pct_delta":       round(pct_delta, 1),
                "direction":       "up" if pct_delta > 0 else "down",
                "top_advertisers": [],
            })
        results.sort(key=lambda x: abs(x["pct_delta"]), reverse=True)
        results = results[:5]

        vel_pub_ids = [v["publisher_id"] for v in results if v["publisher_id"]]
        if vel_pub_ids:
            try:
                pub_id_csv = ",".join(str(p) for p in vel_pub_ids)
                attr_rows = ch.query(
                    f"""
                    SELECT
                        cv.user_id,
                        c.adv_name,
                        sum(toFloat64OrNull(cv.revenue))                                       AS rev_30d,
                        sumIf(toFloat64OrNull(cv.revenue), cv.created_at >= today() - 7)       AS rev_7d,
                        (sumIf(toFloat64OrNull(cv.revenue), cv.created_at >= today() - 7)
                            / 7 * 30) - sum(toFloat64OrNull(cv.revenue))                      AS delta_ann
                    FROM adpx_conversionsdetails cv
                    JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
                    PREWHERE cv.user_id IN ({pub_id_csv})
                        AND toYYYYMM(cv.created_at) >= toYYYYMM(today() - 30)
                    WHERE cv.created_at >= today() - 30
                      AND c.deleted_at IS NULL
                    GROUP BY cv.user_id, c.adv_name
                    ORDER BY cv.user_id, abs(delta_ann) DESC
                    """
                ).result_rows
                attr_map: dict = {}
                for uid, adv_name, rev_30d_a, rev_7d_a, delta_a in attr_rows:
                    key = int(uid) if uid else None
                    if key not in attr_map:
                        attr_map[key] = []
                    delta_rounded = round(delta_a or 0, 0)
                    if abs(delta_rounded) < 100:
                        continue
                    if len(attr_map[key]) < 2:
                        attr_map[key].append({
                            "adv_name": adv_name,
                            "delta_ann": delta_rounded,
                            "rev_7d":    round(rev_7d_a or 0, 0),
                        })
                for v in results:
                    v["top_advertisers"] = attr_map.get(v["publisher_id"], [])
            except Exception as e:
                log.warning(f"Pulse advertiser attribution failed: {e}")

        # ── Batch: fetch existing advertisers for all down publishers in one query ──
        for v in results:
            v["hypothesis"] = ""
            v["gaps"] = []

        down_entries = [
            (v, v["publisher_id"], v.get("publisher_name", ""),
             next((a for a in v.get("top_advertisers", []) if a.get("delta_ann", 0) < 0), None))
            for v in results
            if v["direction"] == "down" and v.get("publisher_id")
        ]

        existing_by_pub: dict[int, set] = {}
        if down_entries:
            try:
                uid_csv = ",".join(str(e[1]) for e in down_entries)
                batch_existing = ch.query(
                    f"SELECT pc.user_id, c.adv_name "
                    f"FROM from_airbyte_publisher_campaigns pc "
                    f"JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = toInt64(c.id) "
                    f"WHERE pc.user_id IN ({uid_csv}) AND pc.is_active = true AND pc.deleted_at IS NULL"
                ).result_rows
                for uid, adv in batch_existing:
                    existing_by_pub.setdefault(int(uid), set()).add(adv)
            except Exception as e:
                log.warning(f"Pulse batch existing-advertisers fetch failed: {e}")

        def _hyp_and_gap(pub_id, pub_name, top_adv):
            from scout_agent import _get_ch_client as _gcc
            _ch = _gcc()
            hyp = ""
            gaps = []
            if top_adv:
                try:
                    hyp_rows = _ch.query(
                        """
                        SELECT
                            u.organization,
                            sum(toFloat64OrNull(cv.revenue))                                    AS rev_30d,
                            sumIf(toFloat64OrNull(cv.revenue), cv.created_at >= today() - 7)   AS rev_7d
                        FROM adpx_conversionsdetails cv
                        JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
                        JOIN from_airbyte_users u ON toInt64(cv.user_id) = u.id
                        PREWHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - 30)
                        WHERE cv.created_at >= today() - 30
                          AND c.adv_name ILIKE %(adv)s
                          AND cv.user_id != %(pub_id)s
                        GROUP BY u.organization
                        HAVING rev_30d > 500
                        ORDER BY rev_30d DESC
                        LIMIT 5
                        """,
                        parameters={"adv": f"%{top_adv['adv_name']}%", "pub_id": pub_id},
                    ).result_rows
                    also_down = [r[0] for r in hyp_rows
                                 if r[2] > 0 and (r[2] / 7 * 30) < r[1] * 0.80][:2]
                    adv_abs   = abs(top_adv["delta_ann"])
                    delta_fmt = f"${adv_abs/1000:.0f}K" if adv_abs >= 1000 else f"${adv_abs:.0f}"
                    if also_down:
                        hyp = (
                            f"_{top_adv['adv_name']} dropped {delta_fmt} — "
                            f"also down at {' & '.join(also_down)}. "
                            f"Likely advertiser-side cap, not a {pub_name} issue._"
                        )
                    else:
                        hyp = (
                            f"_{top_adv['adv_name']} dropped {delta_fmt} here but holding elsewhere — "
                            f"check {pub_name} provisioning or targeting config._"
                        )
                except Exception as e:
                    log.warning(f"Pulse hypothesis failed for {pub_name}: {e}")

            existing = existing_by_pub.get(pub_id, set())
            try:
                gap_rows = _ch.query(f"""
                    WITH imp_agg AS (
                        SELECT campaign_id, count() AS imp_30d
                        FROM adpx_impressions_details
                        PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
                        WHERE created_at >= today() - 30
                        GROUP BY campaign_id
                    ),
                    conv_agg AS (
                        SELECT campaign_id, sum(toFloat64OrNull(revenue)) AS rev_30d
                        FROM adpx_conversionsdetails
                        PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
                        WHERE created_at >= today() - 30
                        GROUP BY campaign_id
                    )
                    SELECT
                        c.adv_name,
                        count(DISTINCT pc.user_id) AS pub_count,
                        sum(ca.rev_30d)             AS revenue_30d,
                        round(sum(ca.rev_30d) / nullIf(sum(ia.imp_30d), 0) * 1000, 2) AS rpm
                    FROM from_airbyte_publisher_campaigns pc
                    JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = toInt64(c.id)
                    LEFT JOIN imp_agg ia ON toString(ia.campaign_id) = toString(pc.campaign_id)
                    LEFT JOIN conv_agg ca ON toString(ca.campaign_id) = toString(pc.campaign_id)
                    WHERE pc.is_active = true AND pc.deleted_at IS NULL
                      AND pc.user_id != {pub_id}
                    GROUP BY c.adv_name
                    HAVING pub_count >= 2 AND sum(ca.rev_30d) > 0
                    ORDER BY sum(ca.rev_30d) DESC
                    LIMIT 20
                """).result_rows
                gaps = [
                    (adv, rpm) for adv, _cnt, _rev, rpm in gap_rows
                    if adv not in existing and rpm and rpm > 0
                ][:3]
            except Exception as e:
                log.warning(f"Pulse gap check failed for {pub_name}: {e}")
            return hyp, gaps

        if down_entries:
            from concurrent.futures import ThreadPoolExecutor as _TPEX, as_completed as _ac
            with _TPEX(max_workers=min(len(down_entries), 5), thread_name_prefix="pulse-hyp") as pool:
                futs = {
                    pool.submit(_hyp_and_gap, pub_id, pub_name, top_adv): v
                    for v, pub_id, pub_name, top_adv in down_entries
                }
                for fut in _ac(futs):
                    v = futs[fut]
                    try:
                        hyp, gaps = fut.result()
                        v["hypothesis"] = hyp
                        v["gaps"] = gaps
                    except Exception as e:
                        log.warning(f"Pulse hyp+gap future failed: {e}")

    except Exception as e:
        log.warning(f"Pulse velocity signal failed: {e}")
    return results


def _pulse_signal_overnight(ch) -> list:
    import json as _json
    results = []
    try:
        event_rows = ch.query(
            """
            SELECT type, old_data, created_at
            FROM adpx_system_activity_logs
            WHERE created_at >= now() - INTERVAL 24 HOUR
              AND type IN ('pause', 'resume')
              AND entity = 'campaigns'
            ORDER BY created_at DESC
            LIMIT 15
            """
        ).result_rows
        for ev_type, old_data_str, created_at in event_rows:
            adv_name = ""
            try:
                od = _json.loads(old_data_str) if old_data_str else {}
                adv_name = od.get("adv_name") or od.get("name") or ""
            except Exception:
                log.debug("suppressed: overnight event old_data JSON parse failed", exc_info=True)
            results.append({
                "type":      ev_type,
                "adv_name":  adv_name,
                "timestamp": str(created_at) if created_at else "",
            })
    except Exception as e:
        log.warning(f"Pulse events signal failed: {e}")
    return results


def _pulse_signal_ghost(ch) -> list:
    results = []
    try:
        from scout_agent import _query_ghost_campaigns
        ghost_detail_rows = _query_ghost_campaigns(ch)
        by_adv: dict = {}
        for r in ghost_detail_rows:
            adv = r["adv_name"]
            by_adv.setdefault(adv, {"impressions_7d": 0, "impressions_2d": 0})
            by_adv[adv]["impressions_7d"] += r["impressions_7d"]
            by_adv[adv]["impressions_2d"] += r["impressions_2d"]
        for adv, agg in sorted(by_adv.items(), key=lambda x: -x[1]["impressions_7d"])[:10]:
            results.append({"adv_name": adv, **agg})
    except Exception as e:
        log.warning(f"Pulse ghost campaign signal failed: {e}")
    return results


def _pulse_signal_fill_rate(ch) -> list:
    results = []
    try:
        from scout_agent import _POST_TX_PLACEMENTS, _load_entity_overrides as _load_eo
        placements_sql = ", ".join(_POST_TX_PLACEMENTS)
        fill_rows = ch.query(
            f"""
            WITH sessions_agg AS (
                SELECT
                    toInt64(user_id) AS publisher_id,
                    count()          AS sessions_7d
                FROM adpx_sdk_sessions
                PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
                WHERE created_at >= today() - 7
                  AND placement IN ({placements_sql})
                GROUP BY user_id
                HAVING sessions_7d > 5000
            ),
            imps_agg AS (
                SELECT
                    toInt64(pid) AS publisher_id,
                    count(DISTINCT session_id) AS sessions_with_imps
                FROM adpx_impressions_details
                PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
                WHERE created_at >= today() - 7
                GROUP BY pid
            )
            SELECT
                s.publisher_id,
                u.organization AS publisher_name,
                s.sessions_7d,
                coalesce(i.sessions_with_imps, 0) AS sessions_with_imps,
                round(100.0 * coalesce(i.sessions_with_imps, 0) / s.sessions_7d, 2) AS fill_rate_pct,
                s.sessions_7d - coalesce(i.sessions_with_imps, 0) AS missed_sessions
            FROM sessions_agg s
            LEFT JOIN imps_agg i ON i.publisher_id = s.publisher_id
            LEFT JOIN from_airbyte_users u ON s.publisher_id = u.id
            WHERE coalesce(i.sessions_with_imps, 0) * 100.0 / s.sessions_7d < 15
            ORDER BY missed_sessions DESC
            LIMIT 5
            """
        ).result_rows
        _pub_overrides = _load_eo().get("publishers", {})
        for pub_id, pub_name, sessions_7d, with_imps, fill_pct, missed in fill_rows:
            name = pub_name or f"Pub #{pub_id}"
            _override = _pub_overrides.get(name, {})
            if _override.get("exclude_from_fill_rate"):
                log.info(f"[pulse] fill rate: skipping {name!r} — {_override.get('note', '')[:60]}...")
                continue
            results.append({
                "publisher_id":   int(pub_id),
                "publisher_name": name,
                "sessions_7d":    int(sessions_7d),
                "fill_rate_pct":  round(float(fill_pct), 1),
                "missed_sessions": int(missed),
            })
    except Exception as e:
        log.warning(f"Pulse fill rate signal failed: {e}")
    return results


def _pulse_signal_opportunities(ch) -> list:
    results = []
    try:
        opp_rows = ch.query(
            """
            WITH adv_perf AS (
                SELECT
                    c.adv_name,
                    count(DISTINCT cv.user_id)                   AS publisher_count,
                    round(sum(toFloat64OrNull(cv.revenue)), 2)   AS rev_30d,
                    round(sum(toFloat64OrNull(cv.revenue))
                          / nullIf(count(DISTINCT cv.user_id), 0), 2) AS avg_rev_per_pub
                FROM adpx_conversionsdetails cv
                JOIN from_airbyte_campaigns c ON toInt64(cv.campaign_id) = c.id
                WHERE toYYYYMM(cv.created_at) >= toYYYYMM(today() - 30)
                  AND cv.created_at >= today() - 30
                GROUP BY c.adv_name
                HAVING publisher_count >= 2 AND rev_30d >= 10000
            ),
            pub_volume AS (
                SELECT
                    toInt64(user_id) AS publisher_id,
                    u.organization   AS publisher_name,
                    count()          AS sessions_30d
                FROM adpx_sdk_sessions s
                JOIN from_airbyte_users u ON toInt64(s.user_id) = u.id
                WHERE toYYYYMM(s.created_at) >= toYYYYMM(today() - 30)
                  AND s.created_at >= today() - 30
                GROUP BY publisher_id, publisher_name
                HAVING sessions_30d > 100000
            ),
            active_pairs AS (
                SELECT DISTINCT
                    toInt64(pc.user_id) AS publisher_id,
                    c.adv_name
                FROM from_airbyte_publisher_campaigns pc
                JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
                WHERE pc.is_active = 1 AND pc.deleted_at IS NULL
            ),
            candidates AS (
                SELECT
                    pv.publisher_name,
                    pv.publisher_id,
                    ap.adv_name,
                    ap.avg_rev_per_pub AS est_monthly_rev,
                    pv.sessions_30d
                FROM pub_volume pv
                CROSS JOIN adv_perf ap
            )
            SELECT
                c.publisher_name,
                c.adv_name,
                c.est_monthly_rev,
                c.sessions_30d
            FROM candidates c
            LEFT JOIN active_pairs ap
                ON ap.publisher_id = c.publisher_id
               AND ap.adv_name = c.adv_name
            WHERE ap.publisher_id IS NULL
            ORDER BY c.est_monthly_rev DESC, c.sessions_30d DESC
            LIMIT 5
            """
        ).result_rows
        for pub_name, adv_name, est_rev, sessions in opp_rows:
            results.append({
                "publisher_name": pub_name or "Unknown Publisher",
                "adv_name":       adv_name,
                "est_monthly_rev": round(float(est_rev), 0),
                "sessions_30d":   int(sessions),
            })
    except Exception as e:
        log.warning(f"Pulse opportunities signal failed: {e}")
    return results


# ── Pulse signal orchestrator ─────────────────────────────────────────────────

def _run_pulse_signals() -> dict:
    """
    Run all 6 Pulse signals in parallel against ClickHouse.
    Each signal owns its own connection — no shared state, no lock needed.
    Returns a dict with cap_alerts, velocity_shifts, overnight_events,
    ghost_campaigns, fill_rate, opportunities.
    """
    from scout_agent import _get_ch_client
    from concurrent.futures import ThreadPoolExecutor, as_completed

    signals: dict = {"cap_alerts": [], "velocity_shifts": [], "overnight_events": [], "ghost_campaigns": [], "fill_rate": [], "opportunities": []}

    _signal_fns = [
        ("cap_alerts",       _pulse_signal_cap),
        ("velocity_shifts",  _pulse_signal_velocity),
        ("overnight_events", _pulse_signal_overnight),
        ("ghost_campaigns",  _pulse_signal_ghost),
        ("fill_rate",        _pulse_signal_fill_rate),
        ("opportunities",    _pulse_signal_opportunities),
    ]

    def _run_one(key, fn):
        ch = _get_ch_client()
        return key, fn(ch)

    with ThreadPoolExecutor(max_workers=6, thread_name_prefix="pulse") as pool:
        futures = {pool.submit(_run_one, key, fn): key for key, fn in _signal_fns}
        for future in as_completed(futures):
            try:
                key, result = future.result()
                signals[key] = result
            except Exception as e:
                log.warning(f"Pulse {futures[future]} signal failed unexpectedly: {e}")

    return signals




def _check_campaign_health(adv_name: str, launched_at) -> dict | None:
    """Query impressions, clicks, revenue since launch. Returns alert dict or None."""
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime as _dt, timezone as _utc

    try:
        from scout_agent import _get_ch_client
        ch = _get_ch_client()
        launched_str = launched_at.strftime("%Y-%m-%d %H:%M:%S")
        partition = launched_at.strftime("%Y%m")

        def q_impressions():
            rows = ch.query("""
                SELECT count() AS impressions
                FROM adpx_impressions_details i
                JOIN from_airbyte_campaigns c ON i.campaign_id = toUInt64(c.id)
                WHERE c.adv_name ILIKE %(adv)s
                  AND i.created_at >= %(launched_at)s
                  AND toYYYYMM(i.created_at) >= %(partition)s
            """, parameters={"adv": adv_name, "launched_at": launched_str, "partition": int(partition)}).result_rows
            return rows[0][0] if rows else 0

        def q_clicks():
            rows = ch.query("""
                SELECT count() AS clicks
                FROM adpx_tracked_clicks tc
                JOIN from_airbyte_campaigns c ON tc.campaign_id = toUInt64(c.id)
                WHERE c.adv_name ILIKE %(adv)s
                  AND tc.created_at >= %(launched_at)s
                  AND toYYYYMM(tc.created_at) >= %(partition)s
            """, parameters={"adv": adv_name, "launched_at": launched_str, "partition": int(partition)}).result_rows
            return rows[0][0] if rows else 0

        def q_revenue():
            rows = ch.query("""
                SELECT sum(toFloat64OrNull(revenue)) AS revenue
                FROM adpx_conversionsdetails cd
                JOIN from_airbyte_campaigns c ON cd.campaign_id = toUInt64(c.id)
                WHERE c.adv_name ILIKE %(adv)s
                  AND cd.created_at >= %(launched_at)s
                  AND toYYYYMM(cd.created_at) >= %(partition)s
            """, parameters={"adv": adv_name, "launched_at": launched_str, "partition": int(partition)}).result_rows
            return (rows[0][0] or 0) if rows else 0

        with ThreadPoolExecutor(max_workers=3) as ex:
            f_imp = ex.submit(q_impressions)
            f_clk = ex.submit(q_clicks)
            f_rev = ex.submit(q_revenue)
            impressions = f_imp.result()
            clicks = f_clk.result()
            revenue = f_rev.result()

        # Alert conditions
        hours_since = (_dt.now(_utc.utc) - launched_at).total_seconds() / 3600
        alert = None

        if impressions > 1000 and clicks == 0 and hours_since >= 3:
            alert = {
                "impressions": impressions, "clicks": clicks, "revenue": revenue,
                "hypothesis": "CTA not rendering or link broken — no clicks despite high impression volume."
            }
        elif impressions > 5000 and revenue == 0 and clicks > 0 and hours_since >= 6:
            alert = {
                "impressions": impressions, "clicks": clicks, "revenue": revenue,
                "hypothesis": "Tracking pixel not firing or landing page failure — clicks present but no conversions."
            }
        elif impressions == 0 and hours_since >= 3:
            alert = {
                "impressions": 0, "clicks": 0, "revenue": 0,
                "hypothesis": "Not serving at all — check geo/OS restrictions or provisioning config."
            }

        return alert

    except Exception as e:
        log.error(f"[watchdog] health check failed for {adv_name}: {e}")
        return None


def _post_watchdog_alert(web: WebClient, adv_name: str, result: dict, hours_since: float) -> None:
    """Post launch health alert to #revenue-operations (production) or #scout-qa (dev/force)."""
    channel = _route_channel("watchdog")
    hours_str = f"{int(hours_since)}h" if hours_since < 24 else f"{hours_since / 24:.1f}d"
    imp = f"{result['impressions']:,}"
    clk = f"{result['clicks']:,}"
    rev = f"${result['revenue']:,.2f}"

    text = (
        f":rotating_light: *Launch Health Alert — {adv_name}*\n"
        f"Launched {hours_str} ago · {imp} impressions · "
        f"*{clk} clicks · {rev} revenue*\n\n"
        f"Likely cause: {result['hypothesis']}\n\n"
        f":zap: Reply `@Scout health brief on {adv_name}` for a full breakdown."
    )
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    try:
        web.chat_postMessage(channel=channel, text=text, blocks=blocks)
        log.info(f"[watchdog] alert posted for {adv_name}")
    except Exception as e:
        log.error(f"[watchdog] failed to post alert: {e}")


def _run_watchdog_checks(web: WebClient, state: dict) -> None:
    """Check recently launched campaigns for zero-engagement patterns."""
    from datetime import datetime as _dt, timezone as _utc, timedelta
    import pytz

    alerted = set(state.get("alerted", []))
    new_alerts = []

    # --- Source A: Scout-tracked launches from launched_offers.json ---
    offers = _load_launched_offers()
    now_utc = _dt.now(_utc.utc)

    for adv_name, offer in offers.items():
        if offer.get("status") != "launched":
            continue
        launched_at_str = offer.get("launched_at")
        if not launched_at_str:
            continue
        try:
            launched_at = _dt.fromisoformat(launched_at_str.replace("Z", "+00:00"))
            if launched_at.tzinfo is None:
                launched_at = launched_at.replace(tzinfo=_utc.utc)
        except Exception:
            continue
        hours_since = (now_utc - launched_at).total_seconds() / 3600
        if hours_since < 3 or hours_since > 48:
            continue  # too early or too old
        if adv_name in alerted:
            continue

        result = _check_campaign_health(adv_name, launched_at)
        if result:
            _post_watchdog_alert(web, adv_name, result, hours_since)
            new_alerts.append(adv_name)

    # --- Source B: Platform-launched campaigns (not in Scout queue) ---
    # Query from_airbyte_publisher_campaigns for new entries in last 48h
    try:
        from scout_agent import _get_ch_client
        ch = _get_ch_client()
        rows = ch.query("""
            SELECT DISTINCT c.adv_name, min(pc.created_at) AS first_seen
            FROM from_airbyte_publisher_campaigns pc
            JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
            WHERE pc.created_at >= now() - INTERVAL 48 HOUR
              AND pc.is_active = true
              AND pc.deleted_at IS NULL
              AND c.deleted_at IS NULL
            GROUP BY c.adv_name
        """).result_rows
    except Exception as e:
        log.error(f"[watchdog] platform launch query failed: {e}")
        rows = []

    for (adv_name, first_seen) in rows:
        if adv_name in offers:
            continue  # already handled by Source A
        if adv_name in alerted:
            continue
        now_utc = _dt.now(_utc.utc)
        if hasattr(first_seen, 'tzinfo') and first_seen.tzinfo is None:
            import pytz as _pytz
            first_seen = _pytz.utc.localize(first_seen)
        hours_since = (now_utc - first_seen).total_seconds() / 3600
        if hours_since < 3 or hours_since > 48:
            continue

        result = _check_campaign_health(adv_name, first_seen)
        if result:
            _post_watchdog_alert(web, adv_name, result, hours_since)
            new_alerts.append(adv_name)

    if new_alerts:
        state.setdefault("alerted", [])
        state["alerted"].extend(new_alerts)
        _save_watchdog_state(state)

    log.info(f"[watchdog] checked launches, fired {len(new_alerts)} alert(s)")


def _launch_watchdog(web: WebClient) -> None:
    """
    Launch health watchdog daemon.

    Runs daily at 10:00 AM Chicago time. Check-first pattern — fires immediately
    on startup if today's run was missed (e.g. Mac was off at 10am).
    Catches broken campaign launches within hours, not days.
    Posts alerts to #revenue-operations (no @mentions).
    """
    import pytz
    from datetime import datetime as _dt, timedelta

    while True:
        try:
            chicago = pytz.timezone("America/Chicago")
            now_chi = _dt.now(chicago)
            today_str = now_chi.strftime("%Y-%m-%d")

            # Load state
            state = _load_watchdog_state()

            # CHECK FIRST: if past 10am and haven't run today, fire immediately
            if state.get("last_run_date") != today_str and now_chi.hour >= 10:
                _run_watchdog_checks(web, state)
                state["last_run_date"] = today_str
                _save_watchdog_state(state)

            # Sleep until next 10am
            target = now_chi.replace(hour=10, minute=0, second=0, microsecond=0)
            if now_chi >= target:
                target += timedelta(days=1)
            sleep_secs = (target - now_chi).total_seconds()
            log.info(f"[watchdog] sleeping {sleep_secs / 3600:.1f}h until next run at {target}")
            time.sleep(sleep_secs)

        except Exception as e:
            log.error(f"[watchdog] cycle failed: {e}", exc_info=True)
            time.sleep(3600)


def _run_pulse_once(web: WebClient, force: bool = False) -> None:
    """
    Execute one pulse run immediately. If force=True, always routes to #scout-qa
    and skips the idempotency state write (so the scheduled pulse still fires today).
    Called by _proactive_pulse daemon and by the @Scout force pulse admin command.
    """
    import pytz
    from datetime import datetime as _dt

    chicago = pytz.timezone("America/Chicago")
    now_chi = _dt.now(chicago)
    today_str = now_chi.strftime("%Y-%m-%d")
    is_weekend = now_chi.weekday() >= 5

    state = _load_pulse_state()
    flagged_history: dict = state.get("flagged_history", {})
    today_s = today_str
    from datetime import datetime as _fdt

    signals = _run_pulse_signals()

    for v in signals.get("velocity_shifts", []):
        if v.get("direction") != "down":
            continue
        pname = v["publisher_name"]
        rec   = flagged_history.get(pname, {})
        last  = rec.get("last_flagged", "")
        if last and (_fdt.strptime(today_s, "%Y-%m-%d") - _fdt.strptime(last, "%Y-%m-%d")).days <= 2:
            rec["count"] = rec.get("count", 1) + 1
        else:
            rec = {"count": 1, "first_flagged": today_s}
        rec["last_flagged"] = today_s
        flagged_history[pname] = rec
    state["flagged_history"] = flagged_history

    chronic: list = state.get("chronic", [])
    today_dt = _fdt.strptime(today_s, "%Y-%m-%d")
    evicted = [
        pname for pname in chronic
        if (today_dt - _fdt.strptime(
            flagged_history.get(pname, {}).get("last_flagged") or "2000-01-01",
            "%Y-%m-%d",
        )).days > 14
    ]
    for pname in evicted:
        chronic.remove(pname)
    for pname, rec in flagged_history.items():
        if pname in chronic:
            continue
        count = rec.get("count", 0)
        first = rec.get("first_flagged", "")
        last  = rec.get("last_flagged", "")
        if not first or not last:
            continue
        span_days = (_fdt.strptime(last, "%Y-%m-%d") - _fdt.strptime(first, "%Y-%m-%d")).days
        if count >= 3 and span_days <= 14:
            chronic.append(pname)
    state["chronic"] = chronic

    has_content = (
        signals.get("cap_alerts")
        or signals.get("velocity_shifts")
        or signals.get("overnight_events")
        or signals.get("ghost_campaigns")
        or signals.get("fill_rate")
        or signals.get("opportunities")
    )
    # Force pulse always routes to #scout-qa; normal pulse uses _route_channel
    channel = _route_channel("pulse", force=force)
    if has_content:
        fallback, blocks = _format_pulse_blocks(
            signals,
            is_weekend=is_weekend,
            flagged_history=flagged_history,
            chronic=chronic,
        )
        web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)
        log.info(f"[pulse{'|force' if force else ''}] posted to {channel}")
    else:
        log.info("[pulse] no signals — skipping post")

    # Only update state for scheduled (non-force) runs
    if not force:
        state["last_pulse_date"] = today_str
        _save_pulse_state(state)


def _proactive_pulse(web: WebClient) -> None:
    """
    Daily proactive intelligence briefing daemon.

    Posts once per day at 8:00 AM Chicago time to the pulse channel.
    Idempotent — uses pulse_state.json to avoid double-posting.
    Surfaces: cap proximity alerts, revenue velocity shifts, overnight events.
    """
    import pytz
    from datetime import datetime as _dt, timedelta

    while True:
        try:
            chicago = pytz.timezone("America/Chicago")
            now_chi = _dt.now(chicago)
            today_str = now_chi.strftime("%Y-%m-%d")

            # CHECK FIRST: fire immediately if past 8am and haven't run today.
            # This handles: Mac was off at 8am, just resumed.
            state = _load_pulse_state()
            if state.get("last_pulse_date") != today_str and now_chi.hour >= 8:
                is_weekend = now_chi.weekday() >= 5  # Saturday=5, Sunday=6

                # Run signals
                signals = _run_pulse_signals()

                # ── Update signal fatigue tracking ────────────────────────────
                # flagged_history: publisher_name → {count, first_flagged, last_flagged}
                # Uses "last_flagged within 48h" window — resilient to Scout being
                # offline for a day (strict consecutive counter would reset on gap).
                flagged_history: dict = state.get("flagged_history", {})
                today_s = today_str
                from datetime import datetime as _fdt
                for v in signals.get("velocity_shifts", []):
                    if v.get("direction") != "down":
                        continue
                    pname = v["publisher_name"]
                    rec   = flagged_history.get(pname, {})
                    last  = rec.get("last_flagged", "")
                    # Within 48h = either yesterday or today (handles one offline day)
                    if last and (_fdt.strptime(today_s, "%Y-%m-%d") - _fdt.strptime(last, "%Y-%m-%d")).days <= 2:
                        rec["count"] = rec.get("count", 1) + 1
                    else:
                        rec = {"count": 1, "first_flagged": today_s}
                    rec["last_flagged"] = today_s
                    flagged_history[pname] = rec
                state["flagged_history"] = flagged_history

                # ── Chronic classification (P9-2) ─────────────────────────────────
                # Threshold: count >= 3 AND (last_flagged - first_flagged) <= 14 days
                # Eviction: today - last_flagged > 14 days → remove from chronic list
                chronic: list = state.get("chronic", [])
                today_dt = _fdt.strptime(today_s, "%Y-%m-%d")

                # Evict stale chronic partners
                evicted = [
                    pname for pname in chronic
                    if (today_dt - _fdt.strptime(
                        flagged_history.get(pname, {}).get("last_flagged") or "2000-01-01",
                        "%Y-%m-%d",
                    )).days > 14
                ]
                for pname in evicted:
                    chronic.remove(pname)
                    log.info(f"[pulse] {pname} evicted from chronic list (no flag in 14d)")

                # Promote new chronic partners
                for pname, rec in flagged_history.items():
                    if pname in chronic:
                        continue
                    count = rec.get("count", 0)
                    first = rec.get("first_flagged", "")
                    last  = rec.get("last_flagged", "")
                    if not first or not last:
                        continue
                    span_days = (_fdt.strptime(last, "%Y-%m-%d") - _fdt.strptime(first, "%Y-%m-%d")).days
                    if count >= 3 and span_days <= 14:
                        chronic.append(pname)
                        log.info(f"[pulse] {pname} classified as chronic ({count} flags in {span_days}d)")

                state["chronic"] = chronic

                # Only post if there's something to say
                has_content = (
                    signals.get("cap_alerts")
                    or signals.get("velocity_shifts")
                    or signals.get("overnight_events")
                    or signals.get("ghost_campaigns")
                    or signals.get("fill_rate")
                    or signals.get("opportunities")
                )
                channel = _route_channel("pulse")
                if has_content:
                    fallback, blocks = _format_pulse_blocks(
                        signals,
                        is_weekend=is_weekend,
                        flagged_history=flagged_history,
                        chronic=chronic,
                    )
                    web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)
                    log.info(f"[pulse] posted to {channel}: {len(signals['cap_alerts'])} caps, "
                             f"{len(signals['velocity_shifts'])} velocity, "
                             f"{len(signals['overnight_events'])} events, "
                             f"{len(signals['ghost_campaigns'])} ghosts, "
                             f"{len(signals['fill_rate'])} low-fill, "
                             f"{len(signals['opportunities'])} opportunities"
                             f"{' [weekend]' if is_weekend else ''}")
                else:
                    log.info("[pulse] no signals today — skipping post")

                # Record posted
                state["last_pulse_date"] = today_str
                _save_pulse_state(state)

            # SLEEP until next 8am
            target = now_chi.replace(hour=8, minute=0, second=0, microsecond=0)
            if now_chi >= target:
                target += timedelta(days=1)
            sleep_secs = (target - now_chi).total_seconds()
            log.info(f"[pulse] sleeping {sleep_secs / 3600:.1f}h until next pulse at {target}")
            time.sleep(sleep_secs)

        except Exception as e:
            log.error(f"[pulse] cycle failed: {e}", exc_info=True)
            time.sleep(3600)  # back off 1h on error


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
        log.debug("suppressed: Slack users_info lookup failed, using raw user_id", exc_info=True)

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
                        log.debug("suppressed: block element value JSON parse failed", exc_info=True)
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

    _msg_text, _giphy_tag = _pick_loading_message(query)
    placeholder = web.chat_postMessage(
        channel=channel, thread_ts=thread_ts, text=_msg_text,
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
    )
    _placeholder_ts_sg = placeholder["ts"]
    _gif_block_sg: list = []

    def _inject_gif_sg():
        gif_url = _fetch_giphy_url(_giphy_tag)
        if not gif_url:
            return
        _gif_block_sg.append({"type": "image", "image_url": gif_url, "alt_text": "Scout"})
        try:
            web.chat_update(
                channel=channel, ts=_placeholder_ts_sg, text=_msg_text,
                blocks=[{"type": "image", "image_url": gif_url, "alt_text": "Scout"},
                        {"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
            )
        except Exception:
            log.debug("suppressed: GIF inject chat_update failed (scout-guided)", exc_info=True)
    threading.Thread(target=_inject_gif_sg, daemon=True).start()
    stop_rotating = _rotating_status(web, channel, _placeholder_ts_sg, gif_block=_gif_block_sg)

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


# ── Feedback handler ─────────────────────────────────────────────────────────

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
            log.warning("Failed to post feedback acknowledgement (good) to Slack")

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
            log.warning("Failed to post feedback acknowledgement (bad) to Slack")

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
            log.warning("Failed to post correction prompt to Slack")

    log.info(f"Feedback recorded: {action_id} query={query_hash} user={user_id}")


# ── App Home tutorial ─────────────────────────────────────────────────────────

# Five real, working queries organized by JTBD.
# Values are real advertisers/partners confirmed in the MS platform.






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

        _msg_text, _giphy_tag = _pick_loading_message(query)
        placeholder = web.chat_postMessage(
            channel=dm_channel, thread_ts=thread_ts, text=_msg_text,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
        )
        _placeholder_ts_ah = placeholder["ts"]
        _gif_block_ah: list = []

        def _inject_gif_ah():
            gif_url = _fetch_giphy_url(_giphy_tag)
            if not gif_url:
                return
            _gif_block_ah.append({"type": "image", "image_url": gif_url, "alt_text": "Scout"})
            try:
                web.chat_update(
                    channel=dm_channel, ts=_placeholder_ts_ah, text=_msg_text,
                    blocks=[{"type": "image", "image_url": gif_url, "alt_text": "Scout"},
                            {"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
                )
            except Exception:
                log.debug("suppressed: GIF inject chat_update failed (ad-hoc)", exc_info=True)
        threading.Thread(target=_inject_gif_ah, daemon=True).start()
        stop_rotating = _rotating_status(web, dm_channel, _placeholder_ts_ah, gif_block=_gif_block_ah)

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


# ── Main event handler ────────────────────────────────────────────────────────

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
                            log.debug("suppressed: approved_at datetime parse failed (queue status)", exc_info=True)
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
            log.warning("Failed to post slash command error to Slack")


def handle_event(client: SocketModeClient, req: SocketModeRequest):
    # Acknowledge immediately — Slack requires <3s ack
    client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))

    web = WebClient(token=BOT_TOKEN, retry_handlers=[RateLimitErrorRetryHandler(max_retry_count=3)])

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
                _run_pulse_once(web, force=True)
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
            log.debug("suppressed: reactions_add thinking_face failed (DM)", exc_info=True)

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
                log.debug("suppressed: reactions_remove thinking_face failed (DM error path)", exc_info=True)
            web.chat_postMessage(channel=channel, text=f":warning: Something went wrong — `{e}`")
            return
        finally:
            # Always remove the 🤔 — even on error — so it doesn't hang
            try:
                web.reactions_remove(channel=channel, timestamp=msg_ts, name="thinking_face")
            except Exception:
                log.debug("suppressed: reactions_remove thinking_face failed (DM finally)", exc_info=True)

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

    # Post placeholder immediately — GIF injected async so there's no Giphy latency on first render
    _msg_text, _giphy_tag = _pick_loading_message(query)
    placeholder = web.chat_postMessage(
        channel=channel, thread_ts=thread_ts, text=_msg_text,
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
    )
    _placeholder_ts = placeholder["ts"]
    _gif_block_he: list = []

    def _inject_gif_he():
        gif_url = _fetch_giphy_url(_giphy_tag)
        if not gif_url:
            return
        _gif_block_he.append({"type": "image", "image_url": gif_url, "alt_text": "Scout"})
        try:
            web.chat_update(
                channel=channel, ts=_placeholder_ts, text=_msg_text,
                blocks=[{"type": "image", "image_url": gif_url, "alt_text": "Scout"},
                        {"type": "section", "text": {"type": "mrkdwn", "text": _msg_text}}],
            )
        except Exception:
            log.debug("suppressed: GIF inject chat_update failed (handle_event)", exc_info=True)
    threading.Thread(target=_inject_gif_he, daemon=True).start()
    stop_rotating = _rotating_status(web, channel, _placeholder_ts, gif_block=_gif_block_he)

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

                # Also DM the approver — they won't be watching a 2-week-old thread
                approved_by = entry.get("approved_by", "")
                if approved_by:
                    try:
                        dm_ch = web.conversations_open(users=[approved_by])["channel"]["id"]
                        dm_body = (
                            f":bar_chart: *{advertiser}* — 14-day recap\n"
                            f"{score_line}\n"
                            f"_{payout} · {network} · {impressions:,} impressions_"
                        )
                        if thread_url:
                            dm_body += f" · <{thread_url}|view brief>"
                        web.chat_postMessage(channel=dm_ch, text=dm_body)
                    except Exception as _dm_err:
                        log.warning(f"Recap DM failed for {approved_by}: {_dm_err}")

                # Mark sent — won't re-post
                state[advertiser]["performance_recap_sent"] = True
                state[advertiser]["actual_rpm_14d"]         = actual_rpm
                state[advertiser]["impressions_14d"]        = impressions
                updated = True
                log.info(f"14-day recap sent for {advertiser}: est=${estimated} actual=${actual_rpm}")

                # Feed actuals back into learned benchmarks
                if actual_rpm > 0:
                    _update_benchmark_from_actuals(
                        advertiser, actual_rpm,
                        payout_type=entry.get("payout_type", ""),
                    )

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
            digest_state_path = _DATA_DIR / "digest_state.json"
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


def _nightly_harvest():
    """Background daemon: harvest Slack channel context once per day at midnight CT."""
    import zoneinfo
    from context_harvester import harvest, is_stale

    ct = zoneinfo.ZoneInfo("America/Chicago")
    while True:
        try:
            from datetime import datetime as _dt
            now = _dt.now(ct)
            # Run at midnight CT — calculate seconds until next midnight
            from datetime import timedelta as _td
            tomorrow_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + _td(days=1)
            sleep_secs = (tomorrow_midnight - now).total_seconds()

            # On startup, if context is stale, harvest immediately
            if is_stale():
                log.info("[harvest] context stale or missing — running immediate harvest")
                result = harvest()
                _post_harvest_audit(result)
            else:
                log.info(f"[harvest] context is fresh — sleeping {sleep_secs / 3600:.1f}h until midnight CT")

            time.sleep(sleep_secs)
            # After sleep, harvest
            log.info("[harvest] midnight CT — running nightly harvest")
            result = harvest()
            _post_harvest_audit(result)
        except Exception as e:
            log.error(f"[harvest] cycle failed: {e}", exc_info=True)
            time.sleep(3600)  # retry in 1 hour on failure


def _post_harvest_audit(harvest_result: dict) -> None:
    """Post a brief audit summary to #scout-qa if the harvester learned any entity facts."""
    try:
        audit = harvest_result.get("audit", []) if isinstance(harvest_result, dict) else []
        if not audit:
            return  # nothing to report

        written = [e for e in audit if e.get("action") == "written"]
        skipped = [e for e in audit if e.get("action") == "skipped"]

        if not written and not skipped:
            return

        lines = [f":newspaper: *Scout learned overnight* ({len(written)} fact{'s' if len(written) != 1 else ''} added to entity knowledge)"]
        for e in written:
            icon = ":office:" if e.get("type") == "publisher" else ":chart_with_upwards_trend:"
            lines.append(f"{icon} *{e['name']}* ({e['type']}) — {e.get('note', '')[:80]}")
        for e in skipped:
            lines.append(f":grey_exclamation: *{e['name']}* — skipped: {e.get('reason', 'manual entry exists')}")
        lines.append("_To correct anything: `@Scout, actually [entity] does X` — I'll overwrite it._")

        web_client.chat_postMessage(
            channel=_SCOUT_HQ_CHANNEL,
            text="\n".join(lines),
        )
        log.info(f"[harvest] audit posted — {len(written)} written, {len(skipped)} skipped")
    except Exception as e:
        log.warning(f"[harvest] audit post failed (non-fatal): {e}")


def _run_scraper_daemon() -> None:
    """
    Offer scraper daemon — fetches affiliate inventory (Impact/FlexOffers/MaxBounty)
    once per day at 6:00 AM CT, then posts the Scout Signal digest.

    First-boot behaviour: if scraper_state.json doesn't exist, run immediately
    regardless of time of day. This ensures Render deployments (which can happen
    any time) don't leave offer inventory empty for hours.

    Check-first on subsequent starts: if past 6am and haven't run today, fire now.
    State: data/scraper_state.json
    """
    import pytz
    from datetime import datetime as _dt, timedelta
    from offer_scraper import run_headless as _run_scraper

    _SCRAPER_STATE = _DATA_DIR / "scraper_state.json"

    def _load_state():
        try:
            return json.loads(_SCRAPER_STATE.read_text())
        except Exception:
            return {}

    def _save_state(s):
        _atomic_write(_SCRAPER_STATE, s)

    while True:
        try:
            chicago = pytz.timezone("America/Chicago")
            now_chi = _dt.now(chicago)
            today_str = now_chi.strftime("%Y-%m-%d")
            state = _load_state()

            # FIRST BOOT: no state file → run immediately regardless of hour.
            # Prevents "offer inventory at 0" immediately after a Render deploy.
            is_first_boot = not _SCRAPER_STATE.exists() or not state

            # CHECK FIRST: past 6am and haven't run today → fire immediately.
            should_run = is_first_boot or (
                state.get("last_run_date") != today_str and now_chi.hour >= 6
            )

            if should_run:
                reason = "first boot" if is_first_boot else "daily run"
                log.info(f"[scraper] running offer fetch ({reason})")
                _SCRAPER_RUNNING.set()
                try:
                    _run_scraper()
                finally:
                    _SCRAPER_RUNNING.clear()
                state["last_run_date"] = today_str
                _save_state(state)
                log.info("[scraper] done — offers_latest.json updated")

            # Sleep until next 6am CT
            target = now_chi.replace(hour=6, minute=0, second=0, microsecond=0)
            if now_chi >= target:
                target += timedelta(days=1)
            sleep_secs = (target - now_chi).total_seconds()
            log.info(f"[scraper] sleeping {sleep_secs / 3600:.1f}h until next run at {target}")
            time.sleep(sleep_secs)

        except Exception as e:
            log.error(f"[scraper] cycle failed: {e}", exc_info=True)
            time.sleep(3600)  # retry in 1 hour on failure


_PID_FILE = _DATA_DIR / "scout.pid"


def _check_singleton() -> None:
    """Prevent two Scout processes from running simultaneously and double-posting.

    On Render, Background Workers are single-instance by platform design — skip
    the PID check entirely.  Render recycles small container PIDs (1-10) between
    restarts, so os.kill(stale_pid, 0) would hit an unrelated system process,
    return successfully, and cause a false-positive sys.exit(1) crash loop.
    """
    import atexit, sys

    # Render sets RENDER=true automatically; trust the platform for single-instance.
    if os.getenv("RENDER"):
        log.info("[main] Running on Render — skipping singleton PID check")
        _PID_FILE.write_text(str(os.getpid()))
        atexit.register(lambda: _PID_FILE.unlink(missing_ok=True))
        return

    # Local: check for an already-running Scout process via PID file
    if _PID_FILE.exists():
        try:
            existing_pid = int(_PID_FILE.read_text().strip())
            os.kill(existing_pid, 0)   # raises ProcessLookupError if dead
            log.error(
                "[main] Scout already running (PID %s). "
                "Kill it first or delete data/scout.pid. Exiting.",
                existing_pid,
            )
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            pass   # stale PID file — safe to overwrite
    _PID_FILE.write_text(str(os.getpid()))
    atexit.register(lambda: _PID_FILE.unlink(missing_ok=True))


def _seed_entity_overrides() -> None:
    """Ensure Button fill-rate exclusion exists in data/entity_overrides.json on first deploy."""
    from scout_agent import _load_entity_overrides, _save_entity_overrides
    import datetime as _dt
    overrides = _load_entity_overrides()
    pubs = overrides.setdefault("publishers", {})
    if "Button" not in pubs:
        pubs["Button"] = {
            "note": (
                "Pre-purchase SDK calls — Button cannot detect the purchase page, so they fire "
                "SDK calls early in the user journey before a purchase is confirmed. "
                "High session counts with low fill rate are expected behavior, not a signal failure."
            ),
            "exclude_from_fill_rate": True,
            "added": _dt.date.today().isoformat(),
            "added_by": "seed",
        }
        _save_entity_overrides(overrides)
        log.info("[startup] seeded Button exclusion into data/entity_overrides.json")


def _run_startup_smoke_test(web: WebClient) -> None:
    """
    Run smoke tests on every startup and post results to #scout-qa.
    Non-blocking — runs in a background thread so it doesn't delay bot startup.
    Catches the class of bug that just burned us: bad model name, broken import,
    ClickHouse down, etc. — all invisible until someone @mentions Scout.
    """
    try:
        import smoke_test as _st
        results, pass_count = _st.run_tests(quiet=True)
        total = len(results)
        blocks, fallback = _st.format_slack_blocks(results, pass_count)
        web.chat_postMessage(channel=_SCOUT_HQ_CHANNEL, text=fallback, blocks=blocks, unfurl_links=False)
        log.info(f"[smoke] {pass_count}/{total} checks passed — posted to #scout-qa")
    except Exception as e:
        log.warning(f"[smoke] startup smoke test failed to run: {e}")
        try:
            web.chat_postMessage(
                channel=_SCOUT_HQ_CHANNEL,
                text=f":red_circle: *Scout startup smoke test crashed* — `{e}`\nCheck Render logs.",
            )
        except Exception:
            log.warning("Failed to post smoke test crash notification to Slack")


# ── Notion → Slack status watcher ────────────────────────────────────────────





def main():
    global _BOT_USER_ID
    _check_singleton()
    _seed_entity_overrides()  # ensure Button exclusion survives fresh Render deploys
    if not BOT_TOKEN or not APP_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env")

    web_client    = WebClient(token=BOT_TOKEN, retry_handlers=[RateLimitErrorRetryHandler(max_retry_count=3)])
    _BOT_USER_ID  = web_client.auth_test()["user_id"]
    socket_client = SocketModeClient(app_token=APP_TOKEN, web_client=web_client)
    socket_client.socket_mode_request_listeners.append(handle_event)

    # Startup: smoke test — runs immediately in background, posts pass/fail to #scout-qa.
    # Catches bad model names, broken imports, ClickHouse down, etc. before anyone @mentions Scout.
    threading.Thread(target=_run_startup_smoke_test, args=(web_client,), daemon=True, name="smoke-test").start()
    # Background: daily stale queue alerts (daemon thread dies cleanly on exit)
    threading.Thread(target=_check_stale_queue, args=(web_client,), daemon=True, name="stale-queue-checker").start()
    # Background: 14-day performance recap — compares Scout estimates to actual ClickHouse RPM
    threading.Thread(target=_performance_recap, args=(web_client,), daemon=True, name="perf-recap").start()
    # Background: nightly cleanup of state files to prevent unbounded growth
    threading.Thread(target=_cleanup_state, daemon=True, name="state-cleanup").start()
    # Background: daily proactive pulse — cap alerts, velocity shifts, overnight events
    # PULSE_ENABLED=false on local (LaunchAgent) to avoid double-posting when Render is also live
    if _PULSE_ENABLED:
        threading.Thread(target=_proactive_pulse, args=(web_client,), daemon=True, name="proactive-pulse").start()
    else:
        log.info("[pulse] disabled via PULSE_ENABLED=false — skipping pulse thread")
    # Background: daily launch health watchdog — catches broken campaigns within hours
    threading.Thread(target=_launch_watchdog, args=(web_client,), daemon=True, name="launch-watchdog").start()
    # Background: nightly channel context harvest — reads Slack channels, compresses to notes
    threading.Thread(target=_nightly_harvest, daemon=True, name="context-harvest").start()
    # Background: daily offer scraper — keeps offers_latest.json fresh (6am CT, or immediately on first boot)
    threading.Thread(target=_run_scraper_daemon, daemon=True, name="scraper").start()
    # Background: Notion → Slack watcher — posts when queue page Status changes from "Awaiting Entry"
    threading.Thread(target=_notion_watcher_loop, args=(web_client,), daemon=True, name="notion-watcher").start()
    # Background: AI copy coalescer — batches enrichment requests with 10s window + 24h cache
    threading.Thread(target=_copy_coalescer_loop, daemon=True, name="copy-coalescer").start()

    log.info("Scout is online — listening for @mentions via Socket Mode")
    socket_client.connect()

    import signal
    signal.pause()


if __name__ == "__main__":
    main()
