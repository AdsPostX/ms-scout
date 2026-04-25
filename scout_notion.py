"""
scout_notion.py — All Notion API interactions for Scout.

- Notion client is passed as a parameter (never imported globally)
- Zero Slack API calls in this module
- _patch_notion_copy is a live async fallback — do NOT delete
"""

import json
import logging
import os
import threading
import time

import requests

from scout_state import (
    _DATA_DIR, _load_notion_notified, _save_notion_notified,
)

log = logging.getLogger("scout_notion")

# ── AI copy coalescer: cache + batch queue ────────────────────────────────────
# Keyed by (advertiser.lower(), payout_type, category) → copy dict + expiry
_COPY_CACHE: dict[tuple, tuple] = {}   # key → (copy_dict, expires_at_monotonic)
_COPY_CACHE_TTL = 86_400               # 24h — same advertiser+config reuses copy
_COPY_CACHE_LOCK = threading.Lock()

# Pending enrichment jobs: list of (notion_url, offer_kwargs_dict)
_COPY_QUEUE: list[tuple[str, dict]] = []
_COPY_QUEUE_LOCK = threading.Lock()
_COPY_QUEUE_EVENT = threading.Event()
_COPY_COALESCE_WINDOW = 10            # seconds to wait before flushing the queue

def _copy_cache_key(advertiser: str, payout_type: str, category: str) -> tuple:
    return (advertiser.lower().strip(), (payout_type or "").upper(), (category or "").lower())

def _copy_cache_get(key: tuple) -> dict | None:
    with _COPY_CACHE_LOCK:
        entry = _COPY_CACHE.get(key)
        if entry and entry[1] > time.monotonic():
            return entry[0]
        if entry:
            del _COPY_CACHE[key]
    return None

def _copy_cache_set(key: tuple, copy_dict: dict) -> None:
    with _COPY_CACHE_LOCK:
        _COPY_CACHE[key] = (copy_dict, time.monotonic() + _COPY_CACHE_TTL)

def _generate_offer_copy(
    advertiser: str,
    description: str,
    payout_type: str,
    category: str,
    payout: str = "",
    geo: str = "US",
) -> dict | None:
    """
    Use Claude Haiku to generate platform-ready copy for all 7 MS platform copy fields.
    Called synchronously in _handle_approve (after Slack ack) so the Notion page is
    complete at creation time — no async patching required.
    Returns a dict with keys: headline, short_headline, description, short_desc,
    cta_yes, cta_no, goal_title.
    Returns None on failure — callers fall back to async enrichment.
    """
    import json as _json
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.warning("_generate_offer_copy: ANTHROPIC_API_KEY not set, skipping AI copy")
        return None

    prompt = f"""You are a world-class direct response copywriter specializing in post-transaction offers — ads that appear right after someone completes a purchase. The user just transacted. Your copy should feel like a valuable follow-on, not an interruption.

Context:
- Advertiser: {advertiser}
- Description: {description}
- Payout: {payout} {payout_type}
- Category: {category}
- Geo: {geo}

Motivation framework:
- Post-transaction users are in ACTION MODE. Their decision muscle is warm. Lead with the SPECIFIC OUTCOME they get, not the brand name. "$50 back" beats "Join {advertiser}."
- The offer is a REWARD, not an ad. Frame it as value delivered, not as an ask.
- Match confidence to payout size: high payout = be specific about the dollar reward. Low payout = emphasize convenience and speed over the dollar amount.

Category to primary motivator (use this to pick the emotional angle):
- Financial/loans/insurance: control, savings, security. "Lock in your rate" not "Sign up."
- Shopping/cashback/rewards: deal-seeking, FOMO. "Claim your savings" not "Learn more."
- Health/wellness: transformation, aspiration. "Start your journey" not "Try it."
- Entertainment/streaming: convenience, discovery. "Watch free" not "Subscribe."
- Travel: escape, possibility. "Book your next trip" not "Sign up for deals."
- Apps/software: productivity, speed. "Try it free" not "Download now."

CTA design rules:
- cta_yes: Match commitment level. High commitment (purchase) = "Shop now". Low commitment (lead) = "See my rate" or "Get your quote". If payout has a specific dollar amount, use it: "Claim $15" for a $15 CPA. Never: "Learn more", "Click here", "Submit".
- cta_no: Make it feel like a timing issue, not a hard rejection. "Not now" or "Maybe later" — not "No thanks" or "Skip."

Return ONLY a JSON object. Enforce char limits precisely — count every character:
- headline: max 90 chars. Specific benefit first. Does NOT start with the advertiser name.
- short_headline: max 60 chars. Distilled. Every word earns its place.
- description: max 220 chars. Expands the headline. Answers "why now?" or "why me?"
- short_desc: max 140 chars. The single most compelling sentence from the description.
- cta_yes: max 25 chars. Action verb first. Specific to this offer type.
- cta_no: max 25 chars. Timing language, not rejection language.
- goal_title: max 128 chars. Plain language description of the conversion event for the MS platform Goal field. Examples: "Membership signup", "Insurance quote request", "Free trial activation", "Cashback account opening". Derives from advertiser + payout type + category. No brand names, no punctuation.

Hard rules — enforce without exception:
- No em dashes (—), en dashes (–), trademark (TM), registered (R), copyright (C) symbols — these break platform rendering
- No exclamation marks — they read as spam in confirmation page contexts and reduce CTR
- No "Free" unless the offer description explicitly confirms no cost to user
- Verify char counts before outputting. A 91-char headline when 90 is the limit is a failure.
- Do NOT include publisher, website, or platform names in any copy field. Copy is for the advertiser's offer only, shown on any publisher's confirmation page.

JSON only, no explanation:
{{"headline":"...","short_headline":"...","description":"...","short_desc":"...","cta_yes":"...","cta_no":"...","goal_title":"..."}}"""

    try:
        import requests as _req
        resp = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5",
                "max_tokens": 600,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if resp.status_code != 200:
            log.warning(f"_generate_offer_copy: Anthropic API {resp.status_code}: {resp.text[:200]}")
            return None

        text = resp.json().get("content", [{}])[0].get("text", "").strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]

        data = _json.loads(text)

        # Hard-enforce char limits — if Claude exceeded, truncate at word boundary
        def _trunc(s: str, n: int) -> str:
            s = str(s or "").strip()
            if len(s) <= n:
                return s
            return s[:n - 1].rsplit(" ", 1)[0].rstrip(",.:;") + "..."

        return {
            "headline":       _trunc(data.get("headline", ""), 90),
            "short_headline": _trunc(data.get("short_headline", ""), 60),
            "description":    _trunc(data.get("description", ""), 220),
            "short_desc":     _trunc(data.get("short_desc", ""), 140),
            "cta_yes":        _trunc(data.get("cta_yes", ""), 25),
            "cta_no":         _trunc(data.get("cta_no", ""), 25),
            "goal_title":     _trunc(data.get("goal_title", ""), 128),
        }
    except Exception as e:
        log.warning(f"_generate_offer_copy failed: {e}")
        return None

def _patch_notion_copy(notion_url: str, ai_copy: dict) -> None:
    """
    Fallback: PATCH an existing Notion queue page with AI copy when sync generation failed.
    Appends copy callouts to the page root — they appear under the Copy heading which is
    the last section, so ordering is correct.
    Called only when _generate_offer_copy failed synchronously in _handle_approve.
    """
    import requests as _req
    notion_token = os.environ.get("NOTION_TOKEN", "")
    if not notion_token or not notion_url:
        return

    # Extract page ID from URL
    page_id_raw = notion_url.rstrip("/").split("/")[-1].replace("-", "")
    if len(page_id_raw) != 32:
        log.warning(f"_patch_notion_copy: unexpected page_id from {notion_url}")
        return
    page_id = f"{page_id_raw[:8]}-{page_id_raw[8:12]}-{page_id_raw[12:16]}-{page_id_raw[16:20]}-{page_id_raw[20:]}"

    def _callout(text: str, emoji: str, color: str = "green_background") -> dict:
        return {
            "object": "block", "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": text}}],
                "icon": {"emoji": emoji},
                "color": color,
            },
        }

    def _rt_muted(text: str) -> dict:
        return {"object": "block", "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": text},
                                              "annotations": {"color": "gray"}}]}}

    def _copy_callout(value: str, emoji: str, label: str, max_chars: int) -> list:
        n = len(value)
        ok = n <= max_chars and n > 0
        color = "green_background" if ok else "red_background" if n > max_chars else "yellow_background"
        qa = f"{n}/{max_chars}" if ok else f"{n}/{max_chars} — over limit" if n > max_chars else "empty"
        return [_callout(value or "(empty)", emoji, color), _rt_muted(f"{label} · {qa}")]

    headline       = ai_copy.get("headline", "")
    short_headline = ai_copy.get("short_headline", "")
    description    = ai_copy.get("description", "")
    short_desc     = ai_copy.get("short_desc", "")
    cta_yes        = ai_copy.get("cta_yes", "")
    cta_no         = ai_copy.get("cta_no", "")
    goal_title     = ai_copy.get("goal_title", "")

    new_blocks = [
        # No heading — Copy h3 already exists as last section of the page
        *_copy_callout(headline,       "✏️", "Headline · 90 chars",       90),
        *_copy_callout(short_headline, "🔤", "Short Headline · 60 chars", 60),
        *_copy_callout(description,    "📝", "Description · 220 chars",  220),
        *_copy_callout(short_desc,     "📋", "Short Desc · 140 chars",   140),
        *_copy_callout(cta_yes,        "👍", "CTA Yes · 25 chars",        25),
        *_copy_callout(cta_no,         "👎", "CTA No · 25 chars",         25),
        *(_copy_callout(goal_title,    "🎯", "Goal Title · 128 chars",   128) if goal_title else []),
    ]

    try:
        resp = _req.patch(
            f"https://api.notion.com/v1/blocks/{page_id}/children",
            headers={
                "Authorization": f"Bearer {notion_token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28",
            },
            json={"children": new_blocks},
            timeout=10,
        )
        if resp.status_code == 200:
            log.info(f"AI copy patched onto Notion page {page_id}")
        else:
            log.warning(f"_patch_notion_copy failed {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.warning(f"_patch_notion_copy error: {e}")

def _enrich_notion_with_ai_copy(
    notion_url: str,
    advertiser: str,
    description: str,
    payout_type: str,
    category: str,
    payout: str = "",
    geo: str = "US",
) -> None:
    """
    Background-thread target: generate AI copy and patch it onto a Notion queue page.
    Safe to call from threading.Thread — never raises.
    """
    try:
        ai_copy = _generate_offer_copy(advertiser, description, payout_type, category, payout, geo)
        if ai_copy and notion_url:
            _patch_notion_copy(notion_url, ai_copy)
            log.info(f"AI copy enrichment complete for {advertiser}")
        else:
            log.info(f"AI copy skipped for {advertiser} (no copy returned)")
    except Exception as e:
        log.warning(f"AI copy enrichment error for {advertiser}: {e}")

def _generate_offer_copy_batch(offers: list[dict]) -> list[dict | None]:
    """
    Generate copy for 1–N offers in a single Claude Haiku API call.
    Returns a list in the same order as input; None for any offer that failed.
    Falls back to sequential individual calls if batch parse fails.
    """
    import json as _json
    if not offers:
        return []
    if len(offers) == 1:
        return [_generate_offer_copy(**offers[0])]

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return [_generate_offer_copy(**o) for o in offers]

    offer_lines = "\n".join(
        f"{i+1}. Advertiser={o['advertiser']} | Payout={o.get('payout','')} {o.get('payout_type','CPA')} "
        f"| Category={o.get('category','')} | Geo={o.get('geo','US')} | Desc={o.get('description','')[:120]}"
        for i, o in enumerate(offers)
    )

    prompt = (
        "You are a world-class direct response copywriter for post-transaction offers. "
        "Generate platform-ready copy for each offer below.\n\n"
        "Copy field rules (enforce char limits precisely):\n"
        "- headline: max 90 chars. Benefit first, not brand name.\n"
        "- short_headline: max 60 chars.\n"
        "- description: max 220 chars. Answers 'why now?'.\n"
        "- short_desc: max 140 chars. Single most compelling sentence.\n"
        "- cta_yes: max 25 chars. Action verb first, specific to offer.\n"
        "- cta_no: max 25 chars. Timing language ('Not now'), not rejection.\n"
        "No em dashes, no exclamation marks, no 'Free' unless explicitly free.\n"
        "Do NOT include publisher, website, or platform names — copy runs on any publisher's page.\n\n"
        f"Offers ({len(offers)} total):\n{offer_lines}\n\n"
        f"Return ONLY a JSON array of exactly {len(offers)} objects in the same order:\n"
        '[{"headline":"...","short_headline":"...","description":"...","short_desc":"...","cta_yes":"...","cta_no":"..."}, ...]'
    )

    def _trunc(s: str, n: int) -> str:
        s = str(s or "").strip()
        return s if len(s) <= n else s[:n - 1].rsplit(" ", 1)[0].rstrip(",.:;") + "..."

    try:
        import requests as _req
        resp = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-3-5-haiku-20241022",
                "max_tokens": 512 * len(offers),
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if resp.status_code != 200:
            log.warning(f"_generate_offer_copy_batch: API {resp.status_code}")
            return [_generate_offer_copy(**o) for o in offers]

        text = resp.json().get("content", [{}])[0].get("text", "").strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]

        parsed = _json.loads(text)
        if not isinstance(parsed, list) or len(parsed) != len(offers):
            log.warning("_generate_offer_copy_batch: unexpected response shape, falling back")
            return [_generate_offer_copy(**o) for o in offers]

        results = []
        for item in parsed:
            results.append({
                "headline":       _trunc(item.get("headline", ""), 90),
                "short_headline": _trunc(item.get("short_headline", ""), 60),
                "description":    _trunc(item.get("description", ""), 220),
                "short_desc":     _trunc(item.get("short_desc", ""), 140),
                "cta_yes":        _trunc(item.get("cta_yes", ""), 25),
                "cta_no":         _trunc(item.get("cta_no", ""), 25),
            })
        return results
    except Exception as e:
        log.warning(f"_generate_offer_copy_batch failed ({e}), falling back to sequential")
        return [_generate_offer_copy(**o) for o in offers]

def _queue_copy_enrichment(
    notion_url: str,
    advertiser: str,
    description: str,
    payout_type: str,
    category: str,
    payout: str = "",
    geo: str = "US",
) -> None:
    """Push an enrichment job to the coalescing queue (non-blocking)."""
    offer_kwargs = dict(
        advertiser=advertiser, description=description,
        payout_type=payout_type, category=category, payout=payout, geo=geo,
    )
    with _COPY_QUEUE_LOCK:
        _COPY_QUEUE.append((notion_url, offer_kwargs))
    _COPY_QUEUE_EVENT.set()

def _copy_coalescer_loop() -> None:
    """
    Daemon: drains the enrichment queue every _COPY_COALESCE_WINDOW seconds.
    Cache hit → instant Notion patch, no API call.
    Cache miss → batch all misses into one Claude call, then patch and cache.
    """
    while True:
        _COPY_QUEUE_EVENT.wait(timeout=_COPY_COALESCE_WINDOW)
        _COPY_QUEUE_EVENT.clear()

        with _COPY_QUEUE_LOCK:
            pending = _COPY_QUEUE[:]
            _COPY_QUEUE.clear()

        if not pending:
            continue

        urls      = [p[0] for p in pending]
        offer_kws = [p[1] for p in pending]
        keys      = [_copy_cache_key(o["advertiser"], o["payout_type"], o["category"]) for o in offer_kws]

        # Serve cache hits immediately
        results: list[dict | None] = [_copy_cache_get(k) for k in keys]

        # Batch only the cache misses
        miss_idx   = [i for i, r in enumerate(results) if r is None]
        miss_offers = [offer_kws[i] for i in miss_idx]

        if miss_offers:
            batch_results = _generate_offer_copy_batch(miss_offers)
            for i, copy_dict in zip(miss_idx, batch_results):
                results[i] = copy_dict
                if copy_dict:
                    _copy_cache_set(keys[i], copy_dict)

        for notion_url, copy_dict, offer in zip(urls, results, offer_kws):
            if copy_dict and notion_url:
                try:
                    _patch_notion_copy(notion_url, copy_dict)
                    log.info(f"AI copy enrichment complete for {offer['advertiser']}")
                except Exception as e:
                    log.warning(f"Notion copy patch failed for {offer['advertiser']}: {e}")
            elif not copy_dict:
                log.info(f"AI copy skipped for {offer['advertiser']} (no copy returned)")

def _write_to_notion_queue(
    brief_data: dict,
    copy_data: dict,
    user_id: str,
    thread_url: str,
    ai_copy: dict | None = None,
    user_display: str = "",
) -> str | None:
    """
    Create a Notion page in the Scout Demand Queue DB.
    Properties: machine-readable filtering/sorting/Kanban data.
    Page body: MS platform entry checklist, ordered for ops workflow:
      Campaign Config → Platform Settings → Scout Intelligence → Copy.
    Copy goes last so _patch_notion_copy (async fallback) appends in the right position.
    ai_copy: if provided (sync generation succeeded), copy callouts are baked in.
    If None, copy section shows a pending placeholder; _patch_notion_copy fills it async.
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
    offer_id     = copy_data.get("oid", "") or brief_data.get("offer_id", "")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")

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

    # ── Page body helpers ─────────────────────────────────────────────────────
    def _rt(text: str) -> dict:
        return {"object": "block", "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": text}}]}}

    def _rt_muted(text: str) -> dict:
        return {"object": "block", "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": text},
                                              "annotations": {"color": "gray"}}]}}

    def _rt_code(text: str) -> dict:
        """Inline code block — triple-click selects exactly the value. Use for URLs."""
        return {"object": "block", "type": "code",
                "code": {"rich_text": [{"type": "text", "text": {"content": text}}],
                         "language": "plain text"}}

    def _heading(text: str, level: int = 2) -> dict:
        h = f"heading_{level}"
        return {"object": "block", "type": h,
                h: {"rich_text": [{"type": "text", "text": {"content": text}}]}}

    def _divider() -> dict:
        return {"object": "block", "type": "divider", "divider": {}}

    def _callout(text: str, emoji: str = "📋", color: str = "gray_background") -> dict:
        return {
            "object": "block", "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": text or "(pending)"}}],
                "icon": {"emoji": emoji},
                "color": color,
            }
        }

    def _copy_field(label: str, emoji: str, value: str, max_chars: int) -> list:
        """Label above callout — label establishes context before content appears."""
        n = len(value)
        ok = n <= max_chars and n > 0
        color = "green_background" if ok else "red_background" if n > max_chars else "yellow_background"
        qa = f"{n}/{max_chars}" if ok else f"{n}/{max_chars} — over limit" if n > max_chars else "pending"
        return [
            _rt_muted(f"{label} · {qa}"),
            _callout(value or "(pending)", emoji, color),
        ]

    # ── Build copy section ────────────────────────────────────────────────────
    if ai_copy:
        copy_blocks = [
            *_copy_field("Headline · 90 chars",       "✏️", ai_copy.get("headline", ""),       90),
            *_copy_field("Short Headline · 60 chars", "🔤", ai_copy.get("short_headline", ""), 60),
            *_copy_field("Description · 220 chars",   "📝", ai_copy.get("description", ""),   220),
            *_copy_field("Short Desc · 140 chars",    "📋", ai_copy.get("short_desc", ""),    140),
            *_copy_field("CTA Yes · 25 chars",        "👍", ai_copy.get("cta_yes", ""),        25),
            *_copy_field("CTA No · 25 chars",         "👎", ai_copy.get("cta_no", ""),         25),
        ]
    else:
        # Sync generation failed — placeholders shown, _patch_notion_copy fills in async
        copy_blocks = [
            _callout("(AI copy generating — check back in ~30 seconds)", "⏳", "yellow_background"),
        ]

    # ── Platform config values ────────────────────────────────────────────────
    internal_name = f"{advertiser} — {network} — {now_iso}"
    goal_type     = "CPC" if "click" in payout_type.lower() else "CPA"
    goal_title    = (ai_copy or {}).get("goal_title", "")
    adv_name_len  = len(advertiser[:28])

    children = [
        _heading("Platform Entry Checklist", 2),
        _rt_muted("Copy and paste into the MS platform. Page 1: Campaign Config + Copy. Page 2: Platform Settings."),
        _divider(),

        # ── Campaign Config ───────────────────────────────────────────────────
        _heading("Campaign Config", 3),
        _rt(f"Internal Offer Name:   {internal_name[:100]}"),
        _rt(f"Partner Offer Name:    {advertiser[:80]}"),
        _rt(f"Advertiser Name:       {advertiser[:28]}  ({adv_name_len}/28)"),
        *(
            [_rt("Destination URL:"), _rt_code(tracking_url)]
            if tracking_url else
            [_rt("Destination URL:       pull from network portal")]
        ),
        _rt(f"Goal Type:             {goal_type}"),
        _rt(f"Payout ($):            {payout_str}"),
        *([_rt(f"Goal Title:            {goal_title}")] if goal_title else []),
        _divider(),

        # ── Platform Settings — scraped offer data only ───────────────────────
        # Ops config (Test Offer ON/OFF, Perkswall toggle) omitted — ops knows their workflow.
        _heading("Platform Settings", 3),
        _rt(f"Network:               {network}" if network else "Network:               set in platform"),
        _rt(f"Network Offer ID:      {offer_id}" if offer_id else "Network Offer ID:      pull from network portal"),
        _divider(),

        # ── Scout Intelligence ────────────────────────────────────────────────
        _heading("Scout Intelligence", 3),
        _rt(f"Est. RPM:   ${rpm:,.2f}" if rpm else "Est. RPM:   N/A"),
        *([
            _rt(f"MS history: {perf_ctx}")
            if perf_ctx.startswith(("Real MS data", "Same advertiser"))
            else _rt(f"Category benchmark: {perf_ctx}")
        ] if perf_ctx and perf_ctx != "No MS performance data at any tier" else [
            _rt("MS history: No MS data — going in cold")
        ]),
        *([_rt(f"Category:   {brief_data.get('category')}")] if brief_data.get("category") else []),
        _rt(f"Risk note:  {risk_flag}" if risk_flag else "Risk note:  None flagged"),
        _rt(f"Approved:   {user_display or user_id}  ·  {now_iso}"),
        {"object": "block", "type": "bookmark",
         "bookmark": {"url": thread_url, "caption": [{"type": "text", "text": {"content": "Brief thread in Slack"}}]}}
        if thread_url else _rt("Brief thread: not available"),
        _divider(),

        # ── Copy — LAST so _patch_notion_copy appends in the correct position ─
        # ai_copy present → callouts baked in at creation (sync generation succeeded).
        # ai_copy absent  → single placeholder; _patch_notion_copy fills this section async.
        _heading("Copy", 3),
        # Creative image — prefer banner_url (actual ad creative) over hero_url (may be brand logo).
        # Notion renders external images inline; if the URL is behind network auth it shows a
        # broken placeholder — no data loss, no page creation failure.
        *([{
            "object": "block", "type": "image",
            "image": {"type": "external", "external": {"url": (
                brief_data.get("banner_url") or brief_data.get("hero_url") or ""
            )}},
        }] if (brief_data.get("banner_url") or brief_data.get("hero_url") or "").startswith("http") else []),
        *copy_blocks,
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

def _check_notion_queue_changes(web: WebClient, notion_token: str, queue_db_id: str, notified: dict) -> None:
    """
    Query the Notion Demand Queue DB for pages where Status != 'Awaiting Entry'.
    For each unseen page, post a status update to the Scout offers channel and tag the approver.
    """
    offers_channel = _route_channel("offers")

    try:
        resp = requests.post(
            f"https://api.notion.com/v1/databases/{queue_db_id}/query",
            headers={
                "Authorization": f"Bearer {notion_token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            json={
                "filter": {
                    "property": "Status",
                    "select": {"does_not_equal": "Awaiting Entry"},
                }
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning(f"[notion-watcher] query failed {resp.status_code}: {resp.text[:200]}")
            return

        results = resp.json().get("results", [])
    except Exception as e:
        log.warning(f"[notion-watcher] request error: {e}")
        return

    changed = False
    for page in results:
        page_id = page.get("id", "").replace("-", "")
        if not page_id or page_id in notified:
            continue

        props       = page.get("properties", {})
        status      = (props.get("Status") or {}).get("select", {}).get("name", "Unknown")
        adv_raw     = (props.get("Name") or {}).get("title", [{}])
        adv_name    = adv_raw[0].get("plain_text", "Offer") if adv_raw else "Offer"
        approved_by = ""
        ab_rt       = (props.get("Approved By") or {}).get("rich_text", [])
        if ab_rt:
            approved_by = ab_rt[0].get("plain_text", "")
        notion_url  = f"https://www.notion.so/{page_id}"

        _STATUS_EMOJI = {
            "Live":       ":white_check_mark:",
            "Needs Work": ":warning:",
            "Rejected":   ":x:",
        }
        emoji = _STATUS_EMOJI.get(status, ":bell:")

        lines = [f"{emoji} *{adv_name}* status changed to *{status}*"]
        if status == "Live":
            lines.append("Offer is live in the MS platform.")
        elif status == "Needs Work":
            lines.append("Review the Notion page and revise copy before going live.")
        elif status == "Rejected":
            lines.append("Offer will not be entered — marked rejected.")
        if approved_by:
            lines.append(f"Originally approved by <@{approved_by}>")
        lines.append(f"<{notion_url}|View in Notion>")

        try:
            web.chat_postMessage(channel=offers_channel, text="\n".join(lines))
            log.info(f"[notion-watcher] posted status change: {adv_name} → {status}")
        except Exception as e:
            log.warning(f"[notion-watcher] post error for {adv_name}: {e}")
            continue

        notified[page_id] = status
        changed = True

    if changed:
        _save_notion_notified(notified)

def _notion_watcher_loop(web: WebClient) -> None:
    """
    Background daemon: polls the Notion Demand Queue DB every 5 minutes.
    When a page's Status changes from 'Awaiting Entry', posts to #scout-offers
    and tags the original approver. State stored in notion_notified.json so
    each transition fires exactly once.
    """
    import time

    POLL_INTERVAL = 300  # 5 minutes — fast enough for same-day awareness

    notion_token = os.environ.get("NOTION_TOKEN", "")
    queue_db_id  = os.environ.get("NOTION_QUEUE_DB_ID", "")
    if not notion_token or not queue_db_id:
        log.info("[notion-watcher] disabled — NOTION_TOKEN or NOTION_QUEUE_DB_ID not set")
        return

    log.info("[notion-watcher] started — polling every 5 min")
    notified = _load_notion_notified()

    while True:
        try:
            _check_notion_queue_changes(web, notion_token, queue_db_id, notified)
        except Exception as e:
            log.warning(f"[notion-watcher] loop error: {e}")
        time.sleep(POLL_INTERVAL)

