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

load_dotenv()  # plist env vars (SCOUT_ENV, PULSE_CHANNEL, etc.) take precedence over .env

# ── Data directory — created here so it exists on first boot (Render, fresh clone, etc.) ──
_DATA_DIR = pathlib.Path(__file__).parent / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

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

# ── Persistent brief state ────────────────────────────────────────────────────
# Briefs are written to disk so process restarts (launchd restarts, deploys, etc.)
# never cause "No brief found" on the Launch button click.
_STATE_FILE = _DATA_DIR / "pending_briefs.json"
_LAST_THREAD_PER_CHANNEL: dict = {}  # channel → thread_ts
_LAST_THREAD_LOCK = threading.Lock()
_BOT_USER_ID: str = ""  # cached at startup — never changes


def _smart_history(history: list, max_full: int = 4) -> list:
    """Keep last max_full messages verbatim; summarize older ones as a single context line."""
    if len(history) <= max_full:
        return history
    older, recent = history[:-max_full], history[-max_full:]
    entities: set = set()
    for msg in older:
        content = msg.get("content", "")
        if isinstance(content, str):
            entities.update(re.findall(r'\b[A-Z][a-zA-Z+]{2,}\b', content))
    summary = (
        f"[Earlier context: {', '.join(list(entities)[:8])}]"
        if entities
        else "[Earlier messages truncated]"
    )
    return [
        {"role": "user", "content": summary},
        {"role": "assistant", "content": "Understood."},
    ] + recent


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

_THREAD_CTX_FILE = _DATA_DIR / "thread_context.json"


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
    """Replace the loading placeholder with a GIF + clean human error message."""
    msg, tag = _clean_error(err)
    gif_url  = _fetch_giphy_url(tag)
    blocks: list = []
    if gif_url:
        blocks.append({"type": "image", "image_url": gif_url, "alt_text": "oops"})
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f":warning: {msg}"}})
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
                pass
            idx[0] += 1

    threading.Thread(target=_run, daemon=True).start()
    return stop_event.set


def _strip_mention(text: str) -> str:
    """Remove @mention tokens so the agent sees the clean query."""
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()


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
    # "Ready to build?" removed — Creatives field already tells you exactly what to do

    context_elements.append({"type": "mrkdwn", "text": "\n".join(footer_parts)})
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
    parts = re.split(r'\n+\s*---\s*\n+', text.strip())
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


_LAUNCHED_OFFERS_FILE        = _DATA_DIR / "launched_offers.json"
_PULSE_STATE_FILE            = _DATA_DIR / "pulse_state.json"
_LEARNINGS_FILE              = _DATA_DIR / "learnings.json"
_LEARNED_BENCHMARKS_FILE     = _DATA_DIR / "learned_benchmarks.json"
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


# ── Pulse state ───────────────────────────────────────────────────────────────

def _load_pulse_state() -> dict:
    try:
        if _PULSE_STATE_FILE.exists():
            return json.loads(_PULSE_STATE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_pulse_state(state: dict):
    try:
        _PULSE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_PULSE_STATE_FILE, state)
    except Exception as e:
        log.warning(f"Could not persist pulse_state: {e}")


# ── Watchdog state ────────────────────────────────────────────────────────────

_WATCHDOG_STATE_PATH = _DATA_DIR / "watchdog_state.json"


def _load_watchdog_state() -> dict:
    try:
        if _WATCHDOG_STATE_PATH.exists():
            return json.loads(_WATCHDOG_STATE_PATH.read_text())
    except Exception:
        pass
    return {}


def _save_watchdog_state(state: dict) -> None:
    _WATCHDOG_STATE_PATH.write_text(json.dumps(state, indent=2))


# ── Learnings store ───────────────────────────────────────────────────────────

def _load_learnings() -> dict:
    try:
        if _LEARNINGS_FILE.exists():
            return json.loads(_LEARNINGS_FILE.read_text())
    except Exception:
        pass
    return {"corrections": [], "positive_signals": []}


def _save_learnings(data: dict):
    try:
        _LEARNINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_LEARNINGS_FILE, data)
    except Exception as e:
        log.warning(f"Could not persist learnings: {e}")


# ── Benchmark recalibration from 14-day actuals ───────────────────────────────

def _update_benchmark_from_actuals(advertiser: str, actual_rpm: float, payout_type: str = "") -> None:
    """
    After a 14-day recap, fold the actual RPM into learned_benchmarks.json.
    Stored as a rolling average per (advertiser, payout_type).
    Scout loads this on startup to improve future estimates.
    """
    try:
        key = f"{advertiser.lower()}:{payout_type.lower()}" if payout_type else advertiser.lower()
        data: dict = {}
        if _LEARNED_BENCHMARKS_FILE.exists():
            try:
                data = json.loads(_LEARNED_BENCHMARKS_FILE.read_text())
            except Exception:
                data = {}

        entry = data.get(key, {"rpm_actual_avg": 0.0, "sample_count": 0})
        n     = entry["sample_count"]
        avg   = entry["rpm_actual_avg"]
        # Rolling average (max 20 samples — recent data is more relevant)
        n_new = min(n + 1, 20)
        w     = 1 / n_new  # weight for new sample
        new_avg = avg * (1 - w) + actual_rpm * w
        data[key] = {"rpm_actual_avg": round(new_avg, 2), "sample_count": n_new}

        _LEARNED_BENCHMARKS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_LEARNED_BENCHMARKS_FILE, data)
        log.info(f"Learned benchmark updated: {key} → avg RPM ${new_avg:.2f} (n={n_new})")
    except Exception as e:
        log.warning(f"_update_benchmark_from_actuals failed for {advertiser}: {e}")


# ── Feedback buttons ──────────────────────────────────────────────────────────

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


# ── Pulse signal runners ──────────────────────────────────────────────────────

def _run_pulse_signals() -> dict:
    """
    Run the three proactive pulse signals against ClickHouse.
    Returns a dict with cap_alerts, velocity_shifts, overnight_events.
    Each is a list (may be empty if no signal or query failed).
    """
    from scout_agent import _get_ch_client
    import json as _json
    ch = _get_ch_client()

    signals: dict = {"cap_alerts": [], "velocity_shifts": [], "overnight_events": [], "ghost_campaigns": [], "fill_rate": [], "opportunities": []}

    # ── Signal 1: Cap proximity (campaigns ≥70% of monthly cap) ──────────────
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
        from datetime import date as _date
        import calendar as _cal
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
            signals["cap_alerts"].append({
                "adv_name":       adv_name,
                "campaign_id":    int(camp_id) if camp_id else None,
                "monthly_cap":    mb,
                "revenue_mtd":    round(revenue_mtd, 2),
                "cap_pct":        round(cap_pct * 100, 1),
                "days_remaining": days_remaining,
                "days_to_cap":    round(days_to_cap, 1),
            })
        signals["cap_alerts"].sort(key=lambda x: x["cap_pct"], reverse=True)
    except Exception as e:
        log.warning(f"Pulse cap signal failed: {e}")

    # ── Signal 2: Revenue velocity (7d vs 30d run rate, ≥40% delta, >$5K/mo) ─
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
            HAVING revenue_30d > 5000  -- exclude ramp-ups from near-zero baseline
            ORDER BY revenue_30d DESC
            LIMIT 200
            """
        ).result_rows

        # Resolve publisher names in one batch query
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
                pass

        # Build velocity shifts list (filter to ≥40% delta)
        for user_id, rev_30d, rev_7d in vel_rows:
            rev_7d_ann = (rev_7d / 7) * 30 if rev_7d else 0
            if rev_30d <= 0:
                continue
            pct_delta = (rev_7d_ann - rev_30d) / rev_30d * 100
            if abs(pct_delta) < 40:
                continue
            signals["velocity_shifts"].append({
                "publisher_name":  org_map.get(str(user_id), f"Partner {user_id}"),
                "publisher_id":    int(user_id) if user_id else None,
                "revenue_30d":     round(rev_30d, 2),
                "revenue_7d_ann":  round(rev_7d_ann, 2),
                "pct_delta":       round(pct_delta, 1),
                "direction":       "up" if pct_delta > 0 else "down",
                "top_advertisers": [],
            })
        signals["velocity_shifts"].sort(key=lambda x: abs(x["pct_delta"]), reverse=True)
        signals["velocity_shifts"] = signals["velocity_shifts"][:5]

        # ── Advertiser attribution: top-2 per publisher by revenue delta ──────
        vel_pub_ids = [v["publisher_id"] for v in signals["velocity_shifts"] if v["publisher_id"]]
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
                # Group by publisher_id → top 2 advertisers by |delta_ann|
                attr_map: dict = {}
                for uid, adv_name, rev_30d_a, rev_7d_a, delta_a in attr_rows:
                    key = int(uid) if uid else None
                    if key not in attr_map:
                        attr_map[key] = []
                    delta_rounded = round(delta_a or 0, 0)
                    # Skip flat advertisers — no meaningful signal
                    if abs(delta_rounded) < 100:
                        continue
                    if len(attr_map[key]) < 2:
                        attr_map[key].append({
                            "adv_name": adv_name,
                            "delta_ann": delta_rounded,
                            "rev_7d":    round(rev_7d_a or 0, 0),
                        })
                # Attach to velocity shifts
                for v in signals["velocity_shifts"]:
                    v["top_advertisers"] = attr_map.get(v["publisher_id"], [])
            except Exception as e:
                log.warning(f"Pulse advertiser attribution failed: {e}")

    except Exception as e:
        log.warning(f"Pulse velocity signal failed: {e}")

    # ── Causal hypothesis + gap opportunities for each down publisher ──────────
    # Runs after velocity + attribution so top_advertisers is already populated.
    for v in signals["velocity_shifts"]:
        v["hypothesis"] = ""
        v["gaps"] = []
        if v["direction"] != "down":
            continue
        pub_id   = v.get("publisher_id")
        pub_name = v.get("publisher_name", "")
        top_advs = v.get("top_advertisers", [])

        # ── Causal hypothesis: is the top-drop advertiser also down elsewhere? ─
        top_adv = next((a for a in top_advs if a.get("delta_ann", 0) < 0), None)
        if top_adv and pub_id:
            try:
                hyp_rows = ch.query(
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
                # A publisher "also down" = 7d annualized < 80% of 30d run rate
                also_down = [r[0] for r in hyp_rows
                             if r[2] > 0 and (r[2] / 7 * 30) < r[1] * 0.80][:2]
                adv_abs   = abs(top_adv["delta_ann"])
                delta_fmt = f"${adv_abs/1000:.0f}K" if adv_abs >= 1000 else f"${adv_abs:.0f}"
                if also_down:
                    also_str = " & ".join(also_down)
                    v["hypothesis"] = (
                        f"_{top_adv['adv_name']} dropped {delta_fmt} — "
                        f"also down at {also_str}. Likely advertiser-side cap, not a {pub_name} issue._"
                    )
                else:
                    v["hypothesis"] = (
                        f"_{top_adv['adv_name']} dropped {delta_fmt} here but holding elsewhere — "
                        f"check {pub_name} provisioning or targeting config._"
                    )
            except Exception as e:
                log.warning(f"Pulse hypothesis failed for {pub_name}: {e}")

        # ── Quick gap check: top earners on 2+ publishers not yet in this one ──
        if pub_id:
            try:
                existing_rows = ch.query(
                    f"SELECT DISTINCT c.adv_name "
                    f"FROM from_airbyte_publisher_campaigns pc "
                    f"JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = toInt64(c.id) "
                    f"WHERE pc.user_id = {pub_id} AND pc.is_active = true AND pc.deleted_at IS NULL"
                ).result_rows
                existing = {r[0] for r in existing_rows}

                gap_rows = ch.query(f"""
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
                v["gaps"] = [
                    (adv, rpm) for adv, _cnt, _rev, rpm in gap_rows
                    if adv not in existing and rpm and rpm > 0
                ][:3]
            except Exception as e:
                log.warning(f"Pulse gap check failed for {pub_name}: {e}")

    # ── Signal 3: Overnight campaign events (last 24h pauses/resumes) ─────────
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
                pass
            signals["overnight_events"].append({
                "type":      ev_type,
                "adv_name":  adv_name,
                "timestamp": str(created_at) if created_at else "",
            })
    except Exception as e:
        log.warning(f"Pulse events signal failed: {e}")

    # ── Signal 4: Ghost campaigns (serving + clicking but near-zero revenue) ────
    # Detects campaigns with broken tracking/pixels that all other signals miss.
    # All other signals assume revenue exists — ghost campaigns earn $0 and are
    # invisible to velocity shifts, cap alerts, and the watchdog.
    try:
        ghost_rows = ch.query(
            """
            WITH imp_agg AS (
                SELECT
                    i.campaign_id,
                    count()                           AS impressions_7d,
                    min(i.created_at)::Date           AS first_impression_date
                FROM adpx_impressions_details i
                PREWHERE toYYYYMM(i.created_at) >= toYYYYMM(today() - 7)
                WHERE i.created_at >= today() - 7
                GROUP BY i.campaign_id
            ),
            click_agg AS (
                SELECT campaign_id, count() AS clicks_7d
                FROM adpx_tracked_clicks
                PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
                WHERE created_at >= today() - 7
                GROUP BY campaign_id
            ),
            rev_agg AS (
                SELECT
                    campaign_id,
                    coalesce(sum(toFloat64OrNull(revenue)), 0) AS revenue_7d,
                    count()                                    AS conversion_count_7d
                FROM adpx_conversionsdetails
                PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
                WHERE created_at >= today() - 7
                GROUP BY campaign_id
            ),
            joined AS (
                SELECT
                    c.adv_name,
                    ia.impressions_7d,
                    coalesce(ca.clicks_7d, 0)              AS clicks_7d,
                    coalesce(ra.revenue_7d, 0)             AS revenue_7d,
                    coalesce(ra.conversion_count_7d, 0)    AS conversion_count_7d,
                    ia.first_impression_date
                FROM imp_agg ia
                JOIN from_airbyte_campaigns c ON toInt64(ia.campaign_id) = c.id
                    AND JSONLength(c.conversion_events) > 0
                    AND (c.is_test = false OR c.is_test IS NULL)
                    AND c.deleted_at IS NULL
                LEFT JOIN click_agg ca ON ca.campaign_id = ia.campaign_id
                LEFT JOIN rev_agg ra  ON ra.campaign_id  = ia.campaign_id
                WHERE coalesce(ra.conversion_count_7d, 0) = 0
                  AND ia.first_impression_date <= today() - 7
            )
            SELECT
                adv_name,
                sum(impressions_7d)       AS total_impressions,
                sum(clicks_7d)            AS total_clicks,
                sum(revenue_7d)           AS total_revenue
            FROM joined
            GROUP BY adv_name
            HAVING total_impressions > 5000 AND total_clicks > 200
            ORDER BY total_impressions DESC
            LIMIT 10
            """
        ).result_rows
        for adv_name, impressions, clicks, revenue in ghost_rows:
            if revenue == 0:
                hypothesis = "Zero conversions in 7 days — postback not firing post-click. Check pixel/postback URL config."
            else:
                hypothesis = "Near-zero RPM — may be a test campaign or fundamentally non-converting offer. Review or pause."
            signals["ghost_campaigns"].append({
                "adv_name":       adv_name,
                "impressions_7d": int(impressions),
                "clicks_7d":      int(clicks),
                "revenue_7d":     round(float(revenue), 2),
                "hypothesis":     hypothesis,
            })
    except Exception as e:
        log.warning(f"Pulse ghost campaign signal failed: {e}")

    # ── Signal 5: Low fill rate on post-transaction placements ───────────────────
    # Detects publishers on checkout/receipt/order-confirmation pages where
    # fewer than 15% of sessions receive an offer. These are the highest-value
    # real estate in MomentScience's inventory — burned sessions = burned revenue.
    try:
        from scout_agent import _POST_TX_PLACEMENTS
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
        for pub_id, pub_name, sessions_7d, with_imps, fill_pct, missed in fill_rows:
            signals["fill_rate"].append({
                "publisher_id":   int(pub_id),
                "publisher_name": pub_name or f"Pub #{pub_id}",
                "sessions_7d":    int(sessions_7d),
                "fill_rate_pct":  round(float(fill_pct), 1),
                "missed_sessions": int(missed),
            })
    except Exception as e:
        log.warning(f"Pulse fill rate signal failed: {e}")

    # ── Signal 6: Revenue opportunities (cross-publisher gaps) ───────────────
    # Finds high-performing advertisers not active in high-volume publishers.
    # Shown weekly (Mondays) to surface proactive expansion opportunities —
    # separate from the reactive gaps embedded in velocity-shift downs.
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
            signals["opportunities"].append({
                "publisher_name": pub_name or "Unknown Publisher",
                "adv_name":       adv_name,
                "est_monthly_rev": round(float(est_rev), 0),
                "sessions_30d":   int(sessions),
            })
    except Exception as e:
        log.warning(f"Pulse opportunities signal failed: {e}")

    return signals


def _format_pulse_blocks(
    signals: dict,
    is_weekend: bool = False,
    flagged_history: dict | None = None,
    chronic: list | None = None,
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

    today_d      = _date.today()
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

    # ── Ghost campaigns (P0 — compact inline, all same cause, details on demand) ─
    if ghost_camps:
        blocks.append({"type": "divider"})
        # Inline compact list: names + impression scale only — hypothesis is uniform
        # so it's noise inline; available via @Scout ghost brief on demand.
        top_g    = ghost_camps[:5]
        remainder = len(ghost_camps) - len(top_g)
        g_parts  = []
        for g in top_g:
            imp_str = f"{g['impressions_7d'] / 1000:.0f}K" if g["impressions_7d"] >= 1000 else str(g["impressions_7d"])
            g_parts.append(f"*{g['adv_name']}* {imp_str}")
        ghost_inline = "  ·  ".join(g_parts)
        if remainder > 0:
            ghost_inline += f"  +{remainder} more"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":ghost:  *GHOST CAMPAIGNS*  ({len(ghost_camps)} active · high impressions · $0 revenue)"},
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"\u00a0\u00a0\u00a0\u00a0{ghost_inline}"}],
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "\u00a0\u00a0\u00a0\u00a0`@Scout ghost brief` \u2192 full list + pixel/postback diagnosis per campaign"}],
        })

    # ── Low fill rate (P0 — burned post-transaction traffic) ─────────────────
    if fill_rate:
        blocks.append({"type": "divider"})
        total_missed = sum(f["missed_sessions"] for f in fill_rate)
        missed_str   = f"{total_missed / 1_000_000:.1f}M" if total_missed >= 1_000_000 else f"{total_missed / 1000:.0f}K"
        fill_parts   = []
        for f in fill_rate[:4]:
            sess_str = f"{f['sessions_7d'] / 1_000_000:.1f}M" if f["sessions_7d"] >= 1_000_000 else f"{f['sessions_7d'] / 1000:.0f}K"
            fill_parts.append(f"*{f['publisher_name']}* {f['fill_rate_pct']:.0f}% ({sess_str} sessions)")
        fill_inline = "  ·  ".join(fill_parts)
        if len(fill_rate) > 4:
            fill_inline += f"  +{len(fill_rate) - 4} more"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":bar_chart:  *LOW FILL RATE*  ({len(fill_rate)} publisher{'s' if len(fill_rate) != 1 else ''} · {missed_str} sessions/7d with no offer shown)"},
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"\u00a0\u00a0\u00a0\u00a0{fill_inline}"}],
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "\u00a0\u00a0\u00a0\u00a0`@Scout fill rate brief` \u2192 breakdown by publisher + root cause diagnosis"}],
        })

    # ── Revenue opportunities (weekly, Mondays only) ──────────────────────────
    # Proactive cross-publisher gap intelligence — not tied to any publisher being down.
    # Reactive gaps (per down-publisher) are already embedded in velocity shifts.
    if opportunities and today_d.weekday() == 0:  # Monday only
        blocks.append({"type": "divider"})
        total_est = sum(o["est_monthly_rev"] for o in opportunities)
        total_str = f"${total_est / 1000:.0f}K" if total_est >= 1000 else f"${total_est:.0f}"
        opp_parts = []
        for o in opportunities[:4]:
            sess_str = f"{o['sessions_30d'] / 1_000_000:.1f}M" if o["sessions_30d"] >= 1_000_000 else f"{o['sessions_30d'] / 1000:.0f}K"
            est_str  = f"${o['est_monthly_rev'] / 1000:.0f}K" if o["est_monthly_rev"] >= 1000 else f"${o['est_monthly_rev']:.0f}"
            opp_parts.append(f"*{o['adv_name']}* → {o['publisher_name']} · {sess_str} sessions · est. {est_str}/mo")
        if len(opportunities) > 4:
            opp_parts.append(f"+{len(opportunities) - 4} more")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":chart_with_upwards_trend:  *REVENUE OPPORTUNITIES*  ({len(opportunities)} gaps · est. {total_str}/mo combined)"},
        })
        for part in opp_parts:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"\u00a0\u00a0\u00a0\u00a0{part}"}],
            })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "\u00a0\u00a0\u00a0\u00a0`@Scout revenue opportunities` \u2192 full ranked list + estimated impact"}],
        })

    # ── NEEDS ATTENTION (downs) ───────────────────────────────────────────────
    if regular_downs or urgent_caps:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":rotating_light:  *NEEDS ATTENTION*"},
        })
        for v, flag_count in regular_downs:
            attr      = _inline_attr(v)
            attr_part = f"   ·   {attr}" if attr else ""
            flag_note = f"   ·   _(flagged {flag_count}d)_" if flag_count >= 4 else ""
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"\u00a0\u00a0\u00a0\u00a0•   *{v['publisher_name']}*   "
                        f"*{v['pct_delta']:.0f}%*   "
                        f"{_fmt_k(v['revenue_7d_ann'])}/mo"
                        f"{attr_part}{flag_note}"
                    ),
                },
            })
            # Causal hypothesis: why this is happening
            if v.get("hypothesis"):
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0{v['hypothesis']}"}],
                })
            # Gap opportunities: top earners not yet provisioned here
            if v.get("gaps"):
                gap_parts = [f"{adv} (${rpm:.2f} RPM)" for adv, rpm in v["gaps"]]
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0↳ *Missing:* {',  '.join(gap_parts)}"}],
                })

        for a in urgent_caps[:3]:
            hit_note = f"~{a['days_to_cap']:.0f}d to cap"
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"\u00a0\u00a0\u00a0\u00a0•   *{a['adv_name']}*   *{a['cap_pct']}% of cap*   {hit_note}",
                },
            })

        # Rotating action nudge — bottom of NEEDS ATTENTION section
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"\u00a0\u00a0\u00a0\u00a0:speech_balloon:  {na_section_hint}"}],
        })

    # ── MOMENTUM (ups) ────────────────────────────────────────────────────────
    if ups:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":chart_with_upwards_trend:  *MOMENTUM*"},
        })
        for v in ups[:3]:
            attr = _inline_attr(v)
            attr_part = f"   ·   {attr}" if attr else ""
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"\u00a0\u00a0\u00a0\u00a0•   *{v['publisher_name']}*   "
                        f"*+{v['pct_delta']:.0f}%*   "
                        f"{_fmt_k(v['revenue_7d_ann'])}/mo"
                        f"{attr_part}"
                    ),
                },
            })

    # ── Standing checks: routine caps + overnight + persistent down partners ───
    standing: list = []

    # Persistent down partners (7+ days flagged) — demoted here with escalation note
    for v, flag_count in persistent_downs:
        standing.append(
            f"⚠️  *{v['publisher_name']}* {v['pct_delta']:.0f}%   {_fmt_k(v['revenue_7d_ann'])}/mo"
            f"   _(flagged {flag_count}d — persistent, escalate or investigate)_"
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
        ts     = e["timestamp"][11:16] if len(e.get("timestamp", "")) >= 16 else ""
        name   = e["adv_name"] or "Unknown"
        icon   = "⏸" if e["type"] == "pause" else "▶"
        action = "paused" if e["type"] == "pause" else "resumed"
        standing.append(f"{icon}  *{name}* {action} {ts} UTC")

    if standing:
        blocks.append({"type": "divider"})
        for line in standing:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": line}],
            })

    # ── Chronic issues (P9-2) ────────────────────────────────────────────────
    if chronic_set:
        chronic_names = ", ".join(sorted(chronic_set))
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f":rotating_light:  *Chronic Issues*   {chronic_names} — ongoing structural issues, tracked."}],
        })

    # ── Footer ────────────────────────────────────────────────────────────────
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f":speech_balloon:  {footer_hint}   ·   :lock: Only you see slash command responses",
        }],
    })

    fallback = f"{'Weekend Watchdog' if is_weekend else 'Pulse'} — {today_label}: {len(ghost_camps)} ghost, {len(downs)} need attention, {len(ups)} in momentum."
    return fallback, blocks


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

    def _callout(text: str, emoji: str = "📋") -> dict:
        return {
            "object": "block", "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": text}}],
                "icon": {"emoji": emoji},
                "color": "blue_background",
            }
        }

    children = [
        _heading("Copy", 2),
        _callout(title_copy, "✏️"),
        _rt(title_qa),
        _callout(desc_copy, "📝"),
        _rt(desc_qa),
        _callout(f'Yes: "{cta_yes}"  /  No: "{cta_no}"', "👆"),
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


def _update_notion_status(notion_url: str, new_status: str) -> bool:
    """
    PATCH a Notion page's Status select property to new_status.
    Called when an offer is marked live — keeps Notion in sync with launched_offers.json.
    Returns True on success. Best-effort — failure logged, never raises.
    """
    if not notion_url:
        return False
    notion_token = os.environ.get("NOTION_TOKEN", "")
    if not notion_token:
        return False
    # Extract page ID from URL: https://www.notion.so/{32-char-id} or with hyphens
    page_id = notion_url.rstrip("/").split("/")[-1].replace("-", "")
    if len(page_id) != 32:
        log.warning(f"_update_notion_status: unexpected page_id format: {page_id!r}")
        return False
    # Notion API requires hyphenated UUID: 8-4-4-4-12
    hyphenated = f"{page_id[:8]}-{page_id[8:12]}-{page_id[12:16]}-{page_id[16:20]}-{page_id[20:]}"
    try:
        resp = requests.patch(
            f"https://api.notion.com/v1/pages/{hyphenated}",
            headers={
                "Authorization": f"Bearer {notion_token}",
                "Content-Type":  "application/json",
                "Notion-Version": "2022-06-28",
            },
            json={"properties": {"Status": {"select": {"name": new_status}}}},
            timeout=8,
        )
        if resp.status_code == 200:
            log.info(f"Notion status updated to '{new_status}': {notion_url}")
            return True
        else:
            log.warning(f"Notion status update failed {resp.status_code}: {resp.text[:120]}")
            return False
    except Exception as e:
        log.warning(f"_update_notion_status error: {e}")
        return False


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
    Handle ✓ Add to Queue button click from SCOUT Sniper digest.

    One-click flow:
      1. Record approval (won't resurface in future digests)
      2. Fetch full brief via draft_campaign_brief — images, tracking URL, performance context
      3. Post rich _build_brief_blocks() card in thread (same format as @Scout briefs)
      4. Try to write item to Slack Demand Queue list (best-effort, requires lists:write scope)
      5. Post confirmation with queue link if auto-write failed
    """
    # NOTE: Same data path as _handle_brief_queue / scout_brief_queue button.
    # Full copy comes from _make_copy_for_brief (not a truncated button value).
    # Both flows write to launched_offers.json + Notion via _try_add_to_demand_queue.
    # _write_to_notion_queue handles both short-key schema (t/d/cy/cn from button value)
    # and long-key schema (title/description from _make_copy_for_brief) transparently.
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
    _record_queued_offer(
        advertiser, brief_data, user_id, thread_url,
        notion_url=notion_url or "", copy_data=copy_data,
    )

    # Pre-flight QA in background — URL check + MS history, posts consolidated result
    _run_preflight_qa(web, channel, brief_ts, brief_data)

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
    _record_queued_offer(
        advertiser, brief_data, user_id, thread_url,
        notion_url=notion_url or "", copy_data=copy_data,
    )

    # Pre-flight QA in background — URL check + MS history, posts consolidated result
    _run_preflight_qa(web, channel, thread_ts, brief_data)

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
            pass
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
        response = ask(query, history=history)
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

    # ── Feedback buttons (👍 / 👎 / ✏️) ──────────────────────────────────────
    if action_id in ("scout_feedback_good", "scout_feedback_bad", "scout_feedback_correct"):
        _handle_feedback(action, payload, web)
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


# ── App Home tutorial ─────────────────────────────────────────────────────────

# Five real, working queries organized by JTBD.
# Values are real advertisers/partners confirmed in the MS platform.
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
                    "• `/scout-queue` — what's pending in the demand queue\n"
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
                pass
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
    user_id_event = event.get("user", "")

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
            pass
    threading.Thread(target=_inject_gif_he, daemon=True).start()
    stop_rotating = _rotating_status(web, channel, _placeholder_ts, gif_block=_gif_block_he)

    try:
        _t0 = time.monotonic()
        response = ask(query, history=history)
        _elapsed = int(time.monotonic() - _t0)
        _elapsed_str = f"{_elapsed}s" if _elapsed < 60 else f"{_elapsed // 60}m {_elapsed % 60}s"
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

    else:
        # Plain text response — clean text only at reveal, no GIF (GIF was shown during loading)
        response_text     = response if isinstance(response, str) else str(response)
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
                harvest()
            else:
                log.info(f"[harvest] context is fresh — sleeping {sleep_secs / 3600:.1f}h until midnight CT")

            time.sleep(sleep_secs)
            # After sleep, harvest
            log.info("[harvest] midnight CT — running nightly harvest")
            harvest()
        except Exception as e:
            log.error(f"[harvest] cycle failed: {e}", exc_info=True)
            time.sleep(3600)  # retry in 1 hour on failure


def _run_scraper_daemon() -> None:
    """
    Offer scraper daemon — fetches affiliate inventory (Impact/FlexOffers/MaxBounty)
    once per day at 6:00 AM CT, then posts the Scout Sniper digest.

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


def main():
    global _BOT_USER_ID
    _check_singleton()
    if not BOT_TOKEN or not APP_TOKEN:
        raise RuntimeError("SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env")

    web_client    = WebClient(token=BOT_TOKEN, retry_handlers=[RateLimitErrorRetryHandler(max_retry_count=3)])
    _BOT_USER_ID  = web_client.auth_test()["user_id"]
    socket_client = SocketModeClient(app_token=APP_TOKEN, web_client=web_client)
    socket_client.socket_mode_request_listeners.append(handle_event)

    # Background: daily stale queue alerts (daemon thread dies cleanly on exit)
    threading.Thread(target=_check_stale_queue, args=(web_client,), daemon=True).start()
    # Background: 14-day performance recap — compares Scout estimates to actual ClickHouse RPM
    threading.Thread(target=_performance_recap, args=(web_client,), daemon=True).start()
    # Background: nightly cleanup of state files to prevent unbounded growth
    threading.Thread(target=_cleanup_state, daemon=True).start()
    # Background: daily proactive pulse — cap alerts, velocity shifts, overnight events
    # PULSE_ENABLED=false on local (LaunchAgent) to avoid double-posting when Render is also live
    if _PULSE_ENABLED:
        threading.Thread(target=_proactive_pulse, args=(web_client,), daemon=True).start()
    else:
        log.info("[pulse] disabled via PULSE_ENABLED=false — skipping pulse thread")
    # Background: daily launch health watchdog — catches broken campaigns within hours
    threading.Thread(target=_launch_watchdog, args=(web_client,), daemon=True, name="launch-watchdog").start()
    # Background: nightly channel context harvest — reads Slack channels, compresses to notes
    threading.Thread(target=_nightly_harvest, daemon=True, name="context-harvest").start()
    # Background: daily offer scraper — keeps offers_latest.json fresh (6am CT, or immediately on first boot)
    threading.Thread(target=_run_scraper_daemon, daemon=True, name="scraper").start()

    log.info("Scout is online — listening for @mentions via Socket Mode")
    socket_client.connect()

    import signal
    signal.pause()


if __name__ == "__main__":
    main()
