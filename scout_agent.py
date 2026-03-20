"""
Scout — MomentScience Offer Intelligence Agent
Answers natural language questions about the offer inventory using Claude + tools.
Data source: data/offers_latest.json (written by offer_scraper.py after each daily run)
Performance benchmarks: queried from ClickHouse at startup, cached in memory.
"""

import json
import logging
import os
import pathlib
import re
import time
import urllib.request
from html.parser import HTMLParser
from typing import Optional

import anthropic
from dotenv import load_dotenv

load_dotenv(override=True)

log = logging.getLogger("scout_agent")

SNAPSHOT_PATH = pathlib.Path(__file__).parent / "data" / "offers_latest.json"

# ── Performance benchmark cache (loaded once at startup) ─────────────────────
# Maps: category → {cvr_pct, rpm, sample_size}
#        offer_impact_id → {cvr_pct, rpm, adv_name}
_BENCHMARKS: dict = {}

def _load_performance_benchmarks() -> dict:
    """
    Query ClickHouse for real CVR + RPM benchmarks from MS's live campaigns.
    Called once at startup. Returns a dict with category and offer-level data.
    Gracefully returns empty dict if ClickHouse is unavailable.
    """
    try:
        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=os.getenv("CH_HOST", ""),
            user=os.getenv("CH_USER", "analytics"),
            password=os.getenv("CH_PASSWORD", ""),
            database=os.getenv("CH_DATABASE", "default"),
            secure=True,
        )

        # Offer-level: CVR + RPM for campaigns with enough data
        offer_query = """
        SELECT
            c.id,
            c.adv_name,
            trim(c.internal_network_name) AS impact_id,
            conv.conversion_count,
            conv.total_revenue,
            imp.impression_count,
            round(conv.conversion_count / nullIf(imp.impression_count, 0) * 100, 4) AS cvr_pct,
            round(conv.total_revenue / nullIf(imp.impression_count, 0) * 1000, 2) AS rpm
        FROM default.from_airbyte_campaigns c
        JOIN (
            SELECT campaign_id, count() AS conversion_count,
                   sum(toFloat64OrNull(revenue)) AS total_revenue
            FROM default.adpx_conversionsdetails
            WHERE toYYYYMM(created_at) >= 202501
            GROUP BY campaign_id
        ) conv ON toInt64(c.id) = toInt64(conv.campaign_id)
        JOIN (
            SELECT campaign_id, count() AS impression_count
            FROM default.adpx_impressions_details
            WHERE toYYYYMM(created_at) >= 202501
            GROUP BY campaign_id
        ) imp ON toInt64(c.id) = toInt64(imp.campaign_id)
        WHERE c.deleted_at IS NULL
          AND imp.impression_count > 500
        ORDER BY imp.impression_count DESC
        """

        rows = ch.query(offer_query).result_rows
        offer_benchmarks = {}
        for row in rows:
            _, adv_name, impact_id, _, _, impressions, cvr_pct, rpm = row
            entry = {"adv_name": adv_name, "cvr_pct": float(cvr_pct or 0), "rpm": float(rpm or 0), "impressions": impressions}
            if impact_id:
                offer_benchmarks[impact_id] = entry

        # Category-level aggregate benchmarks
        # Use adv_name patterns to map to categories (since categories field is null in CH)
        # This is a simplification — we use offer-level data grouped by adv_name patterns
        category_data = {}
        for entry in offer_benchmarks.values():
            # Map known advertisers to categories
            name = (entry["adv_name"] or "").lower()
            if any(k in name for k in ["turbotax", "capital one", "moneylion", "credit", "loan", "finance", "bank", "cash"]):
                cat = "Finance"
            elif any(k in name for k in ["hulu", "spotify", "apple music", "disney", "paramount", "starz", "siriusxm"]):
                cat = "Entertainment"
            elif any(k in name for k in ["health", "glp", "weight", "med", "rx", "care", "aspca"]):
                cat = "Health & Wellness"
            elif any(k in name for k in ["shop", "walmart", "amazon", "rakuten", "retail", "coupon"]):
                cat = "Retail"
            elif any(k in name for k in ["grammarly", "wix", "docusign", "semrush", "hubspot"]):
                cat = "Business Services"
            else:
                continue  # skip unmapped

            if cat not in category_data:
                category_data[cat] = {"total_cvr": 0, "total_rpm": 0, "count": 0}
            category_data[cat]["total_cvr"] += entry["cvr_pct"]
            category_data[cat]["total_rpm"] += entry["rpm"]
            category_data[cat]["count"] += 1

        category_benchmarks = {
            cat: {
                "avg_cvr_pct": round(v["total_cvr"] / v["count"], 4),
                "avg_rpm": round(v["total_rpm"] / v["count"], 2),
                "sample_campaigns": v["count"],
            }
            for cat, v in category_data.items()
            if v["count"] > 0
        }

        result = {
            "by_offer_impact_id": offer_benchmarks,
            "by_category": category_benchmarks,
        }
        log.info(f"Performance benchmarks loaded: {len(offer_benchmarks)} offers, {len(category_benchmarks)} categories")
        return result

    except Exception as e:
        log.warning(f"Could not load performance benchmarks from ClickHouse: {e}")
        return {"by_offer_impact_id": {}, "by_category": {}}


def _get_benchmarks() -> dict:
    global _BENCHMARKS
    if not _BENCHMARKS:
        _BENCHMARKS = _load_performance_benchmarks()
    return _BENCHMARKS


# ── Scout Score ───────────────────────────────────────────────────────────────

def _scout_score(offer: dict, benchmarks: dict) -> float:
    """
    Composite score: estimated RPM = payout × predicted_CVR.
    This is what actually matters for MS — revenue per 1000 impressions.

    For Impact offers with known CVR history: use actual CVR.
    For others: use category benchmark CVR, falling back to a base estimate.

    Also weights:
      - Payout reliability: CPL (known $) > CPS (% of sale, rate unknown) > Unknown
      - Brand signal: Impact offers tend to be more established than MaxBounty unknowns
    """
    payout = offer.get("_payout_num") or 0
    if payout == 0:
        return 0.0

    offer_id = str(offer.get("offer_id", ""))
    network = offer.get("network", "").lower()
    payout_type = (offer.get("_payout_type_norm") or "").lower()
    category = offer.get("category", "")

    # 1. Get predicted CVR
    by_offer = benchmarks.get("by_offer_impact_id", {})
    by_cat = benchmarks.get("by_category", {})

    if offer_id in by_offer:
        # Best: we've run this exact offer
        predicted_cvr = by_offer[offer_id]["cvr_pct"] / 100
    elif category in by_cat:
        # Good: we know how this category converts
        predicted_cvr = by_cat[category]["avg_cvr_pct"] / 100
    else:
        # Fallback: base estimate by payout type
        # CPL offers tend to be higher-intent (easier conversion event)
        base_cvr = {"$ per lead": 0.03, "$ per click": 0.08, "% of sale": 0.015, "fixed": 0.02}
        predicted_cvr = base_cvr.get(payout_type, 0.02)

    # 2. Payout reliability multiplier
    reliability = {"$ per lead": 1.0, "$ per click": 0.9, "% of sale": 0.6, "fixed": 0.8, "unknown": 0.5}
    reliability_mult = reliability.get(payout_type, 0.6)

    # 3. Estimated RPM = payout × CVR × 1000 × reliability
    estimated_rpm = payout * predicted_cvr * 1000 * reliability_mult

    return round(estimated_rpm, 4)


SYSTEM_PROMPT = """You are Scout — MomentScience's offer intelligence assistant.
MomentScience runs affiliate offers at post-transaction moments (right after a user completes a purchase or action). This context matters enormously: offers that work here are low-friction, from recognizable brands, with simple conversion events (email/signup/free trial). High-intent or complex offers (medical programs, loan applications, insurance sign-ups) convert poorly regardless of payout.

You have access to the full offer inventory (700+ offers from Impact, FlexOffers, MaxBounty) AND real performance data from MS's live campaigns — actual CVR and RPM from ClickHouse.

Your job is not to list offers. It is to help the team make confident decisions fast.

HOW TO ANSWER:
- Lead with THE recommendation (1 best option), not a ranked list
- Use performance data to explain WHY: "this category converts at X% for MS, at $Y payout = Z RPM"
- Show 2-3 alternatives at most, briefly
- Flag restrictions or gotchas inline (geo-limited, prelanders only, etc.)
- End with a pointed follow-up question or next action — move the conversation forward
- Max 5 offers total. If you have more, pick the best ones and say so.

SCORING LOGIC:
Offers are ranked by Scout Score = estimated RPM (payout × predicted CVR × 1000).
This is more useful than raw payout because a $375 offer at 1% CVR = $3.75 RPM,
while a $100 offer at 5% CVR = $5.00 RPM. Higher RPM wins.
When you have actual MS performance data for an offer or category, use it.
When you don't, use the category benchmark. When you have neither, note the uncertainty.

SLACK FORMATTING — this is Slack mrkdwn, NOT GitHub markdown. Follow exactly.

WRONG — never produce this:
| Advertiser | Payout | CVR |
|---|---|---|
| Hulu | $2/lead | 6% |
**Bold**  ## Headers  ← both wrong

RIGHT — always produce this:
*Advertiser Name* · $X/lead · Network · Geo
_One-line reason with RPM or CVR data_
• Alternative 1 — brief note
• Alternative 2 — brief note
*Bottom line:* one sentence + follow-up question

Rules (no exceptions):
- *single asterisk* = bold. _underscore_ = italic.
- NEVER use double asterisks, hash headers, or pipe tables.
- Max 5 offers. Lead with 1 best pick, 2-3 bullets for alternatives.
- No emojis. No "Great question!" openers. No trailing summaries.

CAMPAIGN BRIEF MODE:
When asked to "build", "create", "draft a brief for", or "I like [offer], build it":
1. Call draft_campaign_brief() with the advertiser name
2. Generate exactly 3 headline variants — post-transaction tone, under 60 chars
   Good: "You just unlocked 3 months free"  Bad: "Get SiriusXM today"
3. Generate 2 CTA Yes/No pairs (4-6 words max each, within 25-char platform limit)
4. One targeting note using geo + ClickHouse CVR data if available
5. Output ONLY the JSON block below — no other text before or after it

<<<BRIEF_JSON
{
  "titles": ["title 1", "title 2", "title 3"],
  "ctas": [
    {"yes": "CTA option A", "no": "No Thanks"},
    {"yes": "CTA option B", "no": "Skip"}
  ],
  "targeting": "one-line targeting recommendation with CVR data if available",
  "bottom_line": "one sentence on why this offer is worth running right now"
}
BRIEF_JSON>>>"""


TOOLS = [
    {
        "name": "search_offers",
        "description": (
            "Full-text search across advertiser name and description. "
            "Use for specific advertiser lookups or keyword searches. "
            "Optional filters: network, category, min_payout, ms_status. "
            "Returns results ranked by Scout Score (estimated RPM), not raw payout."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term — advertiser name or keyword"},
                "network": {"type": "string", "description": "impact, flexoffers, or maxbounty"},
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness, Retail"},
                "min_payout": {"type": "number", "description": "Minimum payout amount"},
                "ms_status": {"type": "string", "description": "Live, In System, or Not in System"},
                "limit": {"type": "integer", "description": "Max results (default 5)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_top_opportunities",
        "description": (
            "Returns best untapped offers MS is NOT running (MS Status = Not in System), "
            "ranked by Scout Score (estimated RPM = payout × predicted CVR). "
            "Use for prospecting: 'what should we go after?', 'best opportunities in X vertical'. "
            "Optional filters: category, geo."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness"},
                "geo": {"type": "string", "description": "e.g. US Only, Global"},
                "limit": {"type": "integer", "description": "Max results (default 5)"},
            },
        },
    },
    {
        "name": "get_running_offers",
        "description": (
            "Returns offers MS is currently running (MS Status = Live) with real CVR + RPM data where available. "
            "Use to benchmark payouts, see what verticals are covered, check if MS has an offer from a specific advertiser, "
            "or understand what's actually performing. Optional filter: category."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness"},
            },
        },
    },
    {
        "name": "get_category_performance",
        "description": (
            "Returns real CVR and RPM benchmarks from MS's live ClickHouse data, by category and by specific offer. "
            "Use this to answer questions about what performs well for MS, "
            "to contextualize a new offer's expected value, or to compare verticals. "
            "This is the most data-driven signal available — prioritize it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Optional: filter to a specific category"},
            },
        },
    },
    {
        "name": "get_offer_stats",
        "description": (
            "Returns aggregate inventory stats: count and avg Scout Score by network and category, "
            "MS Status breakdown, and top 5 highest-value offers. "
            "Use for strategic / high-level questions about the inventory."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "draft_campaign_brief",
        "description": (
            "Fetch all offer details needed to generate a campaign brief: tracking URL, payout, geo, "
            "description, network, offer ID, and real MS performance data. "
            "Use when asked to 'build', 'create a brief for', 'I like [offer], build it', or similar. "
            "Returns structured data — you then generate the copy, titles, and CTAs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser": {"type": "string", "description": "Advertiser name (partial match OK)"},
                "network": {"type": "string", "description": "Optional: impact, flexoffers, or maxbounty"},
            },
            "required": ["advertiser"],
        },
    },
]


def _load_offers() -> list:
    if not SNAPSHOT_PATH.exists():
        return []
    with open(SNAPSHOT_PATH) as f:
        return json.load(f)


def _norm(s: str) -> str:
    return s.lower().strip() if s else ""


# ── Tool implementations ─────────────────────────────────────────────────────

def search_offers(
    query: str,
    network: str = None,
    category: str = None,
    min_payout: float = None,
    ms_status: str = None,
    limit: int = 5,
) -> list:
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    q = _norm(query)
    results = []
    for o in offers:
        text = _norm(o.get("advertiser", "")) + " " + _norm(o.get("description", ""))
        if q and q not in text:
            continue
        if network and _norm(o.get("network", "")) != _norm(network):
            continue
        if category and _norm(category) not in _norm(o.get("category", "")):
            continue
        payout_num = o.get("_payout_num") or 0
        if min_payout and payout_num < min_payout:
            continue
        if ms_status and _norm(o.get("_ms_status", "")) != _norm(ms_status):
            continue
        results.append(o)

    results.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    return _format_offers(results[:limit], benchmarks)


def get_top_opportunities(category: str = None, geo: str = None, limit: int = 5) -> list:
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    results = []
    for o in offers:
        if o.get("_ms_status") != "Not in System":
            continue
        if category and _norm(category) not in _norm(o.get("category", "")):
            continue
        if geo and _norm(geo) not in _norm(o.get("geo", "")):
            continue
        results.append(o)

    results.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    return _format_offers(results[:limit], benchmarks)


def get_running_offers(category: str = None) -> list:
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    results = [
        o for o in offers
        if o.get("_ms_status") == "Live"
        and (not category or _norm(category) in _norm(o.get("category", "")))
    ]
    results.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    return _format_offers(results, benchmarks)


def get_category_performance(category: str = None) -> dict:
    benchmarks = _get_benchmarks()
    by_cat = benchmarks.get("by_category", {})
    by_offer = benchmarks.get("by_offer_impact_id", {})

    if category:
        cat_key = next((k for k in by_cat if _norm(category) in _norm(k)), None)
        cat_data = {cat_key: by_cat[cat_key]} if cat_key else {}
    else:
        cat_data = by_cat

    # Also include top individual offer benchmarks
    top_offers = sorted(by_offer.items(), key=lambda x: x[1].get("rpm", 0), reverse=True)[:10]

    return {
        "category_benchmarks": cat_data,
        "note": "CVR and RPM are real MS performance data from ClickHouse (Jan 2025+). Use RPM to estimate expected value of new offers: RPM = payout × (CVR/100) × 1000.",
        "top_performing_offers_by_rpm": [
            {"impact_id": k, **v} for k, v in top_offers
        ],
    }


def get_offer_stats() -> dict:
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    if not offers:
        return {"error": "No offer data available"}

    by_network: dict = {}
    by_category: dict = {}
    by_ms_status: dict = {}

    for o in offers:
        net = o.get("network", "unknown")
        score = _scout_score(o, benchmarks)
        payout = o.get("_payout_num") or 0
        cats = o.get("_categories") or [o.get("category", "Other")]
        ms = o.get("_ms_status", "Unknown")

        by_network.setdefault(net, {"count": 0, "total_score": 0, "total_payout": 0})
        by_network[net]["count"] += 1
        by_network[net]["total_score"] += score
        by_network[net]["total_payout"] += payout

        for cat in (cats if isinstance(cats, list) else [cats]):
            by_category.setdefault(cat, {"count": 0, "total_score": 0})
            by_category[cat]["count"] += 1
            by_category[cat]["total_score"] += score

        by_ms_status[ms] = by_ms_status.get(ms, 0) + 1

    top5 = sorted(offers, key=lambda x: _scout_score(x, benchmarks), reverse=True)[:5]

    return {
        "total_offers": len(offers),
        "by_network": {
            k: {
                "count": v["count"],
                "avg_scout_score_rpm": round(v["total_score"] / v["count"], 2),
                "avg_payout": round(v["total_payout"] / v["count"], 2),
            }
            for k, v in sorted(by_network.items(), key=lambda x: -x[1]["count"])
        },
        "by_category": {
            k: {
                "count": v["count"],
                "avg_scout_score_rpm": round(v["total_score"] / v["count"], 2),
            }
            for k, v in sorted(by_category.items(), key=lambda x: -x[1]["count"])
            if k and k != "Other"
        },
        "ms_status_breakdown": by_ms_status,
        "top_5_by_scout_score": _format_offers(top5, benchmarks),
    }


def _format_offers(offers: list, benchmarks: dict) -> list:
    """Return a compact, readable version of each offer for the LLM, including Scout Score context."""
    by_offer = benchmarks.get("by_offer_impact_id", {})
    by_cat = benchmarks.get("by_category", {})
    out = []
    for o in offers:
        offer_id = str(o.get("offer_id", ""))
        category = o.get("category", "")
        score = _scout_score(o, benchmarks)

        # Performance context
        if offer_id in by_offer:
            perf = by_offer[offer_id]
            perf_note = f"Real MS data: {perf['cvr_pct']}% CVR, ${perf['rpm']} RPM"
        elif category in by_cat:
            cat_perf = by_cat[category]
            perf_note = f"Category benchmark: {cat_perf['avg_cvr_pct']}% CVR avg, ${cat_perf['avg_rpm']} RPM avg"
        else:
            perf_note = "No MS performance data for this category yet"

        out.append({
            "advertiser": o.get("advertiser", ""),
            "network": o.get("network", ""),
            "payout": o.get("_raw_payout") or o.get("payout") or "Rate TBD",
            "payout_num": o.get("_payout_num"),
            "payout_type": o.get("_payout_type_norm") or o.get("payout_type", ""),
            "category": category,
            "geo": o.get("geo", ""),
            "ms_status": o.get("_ms_status", ""),
            "ms_internal_name": o.get("_ms_internal_name", ""),
            "scout_score_rpm": score,
            "performance_context": perf_note,
        })
    return out


def _scrape_og_image(url: str) -> str:
    """Scrape og:image from a landing page. Used for FlexOffers/MaxBounty offers."""
    if not url or not url.startswith("http"):
        return ""
    try:
        class _OGParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.og_image = ""
            def handle_starttag(self, tag, attrs):
                if tag == "meta" and not self.og_image:
                    d = dict(attrs)
                    if d.get("property") == "og:image" or d.get("name") == "og:image":
                        self.og_image = d.get("content", "")

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            html = resp.read(20000).decode("utf-8", errors="ignore")
        parser = _OGParser()
        parser.feed(html)
        return parser.og_image
    except Exception:
        return ""


def draft_campaign_brief(advertiser: str, network: str = None) -> dict:
    """
    Fetch all offer details needed to generate a campaign brief.
    Matches by partial advertiser name (case-insensitive), picks highest Scout Score match.
    """
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    q = _norm(advertiser)
    matches = [
        o for o in offers
        if q in _norm(o.get("advertiser", ""))
        and (not network or _norm(network) == _norm(o.get("network", "")))
    ]
    if not matches:
        return {"error": f"No offer found matching '{advertiser}'. Try a partial name or different spelling."}

    matches.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    o = matches[0]
    offer_id = str(o.get("offer_id", ""))
    by_offer = benchmarks.get("by_offer_impact_id", {})
    by_cat = benchmarks.get("by_category", {})

    if offer_id in by_offer:
        perf = by_offer[offer_id]
        perf_context = f"Real MS data: {perf['cvr_pct']}% CVR, ${perf['rpm']} RPM"
    elif o.get("category") in by_cat:
        cat = by_cat[o["category"]]
        perf_context = f"Category benchmark ({o['category']}): {cat['avg_cvr_pct']}% avg CVR, ${cat['avg_rpm']} avg RPM"
    else:
        perf_context = "No MS performance data yet for this advertiser/category"

    # Creative URLs: Impact has them from scraper; FlexOffers/MaxBounty use OG scrape
    icon_url = o.get("icon_url", "")
    hero_url = o.get("hero_url", "")
    if not icon_url and o.get("tracking_url"):
        log.info(f"draft_campaign_brief: scraping OG image for {o.get('advertiser')}")
        og = _scrape_og_image(o.get("tracking_url", ""))
        icon_url = og
        hero_url = og

    return {
        "advertiser": o.get("advertiser"),
        "network": o.get("network"),
        "offer_id": offer_id,
        "payout": o.get("_raw_payout") or o.get("payout") or "Rate TBD",
        "payout_num": o.get("_payout_num"),
        "payout_type": o.get("_payout_type_norm") or "",
        "geo": o.get("geo"),
        "tracking_url": o.get("tracking_url") or "Not available — pull from network portal",
        "description": (o.get("description") or "")[:300],
        "category": o.get("category"),
        "ms_status": o.get("_ms_status"),
        "performance_context": perf_context,
        "scout_score_rpm": _scout_score(o, benchmarks),
        "icon_url": icon_url,
        "hero_url": hero_url,
    }


# ── Tool dispatch ─────────────────────────────────────────────────────────────

TOOL_MAP = {
    "search_offers": search_offers,
    "get_top_opportunities": get_top_opportunities,
    "get_running_offers": get_running_offers,
    "get_category_performance": get_category_performance,
    "get_offer_stats": get_offer_stats,
    "draft_campaign_brief": draft_campaign_brief,
}


def _run_tool(name: str, inputs: dict):
    fn = TOOL_MAP.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    return fn(**inputs)


# ── Agent loop ────────────────────────────────────────────────────────────────

def ask(user_message: str, history: list = None) -> str:
    """
    Send a message to Scout and get a response.
    history: optional list of prior {"role": "user"/"assistant", "content": str} messages
             from the Slack thread, providing conversation context.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return "ANTHROPIC_API_KEY not set — Scout can't respond."

    client = anthropic.Anthropic(api_key=api_key)
    messages = list(history or []) + [{"role": "user", "content": user_message}]
    _brief_data = {}  # captured when draft_campaign_brief tool runs

    while True:
        for attempt in range(4):
            try:
                response = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=2048,
                    system=SYSTEM_PROMPT,
                    tools=TOOLS,
                    messages=messages,
                )
                break
            except anthropic.APIStatusError as e:
                if e.status_code == 529 and attempt < 3:
                    wait = 2 ** attempt
                    log.warning(f"Anthropic overloaded (529), retrying in {wait}s (attempt {attempt + 1}/3)")
                    time.sleep(wait)
                else:
                    raise

        if response.stop_reason == "end_turn":
            text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    text = block.text
                    break

            # Detect structured brief JSON output
            if _brief_data and "<<<BRIEF_JSON" in text and "BRIEF_JSON>>>" in text:
                try:
                    json_str = text.split("<<<BRIEF_JSON")[1].split("BRIEF_JSON>>>")[0].strip()
                    copy_data = json.loads(json_str)
                    return {
                        "type": "brief",
                        "brief_data": _brief_data,
                        "copy": copy_data,
                        "fallback_text": (
                            f"Campaign Brief — {_brief_data.get('advertiser', 'Offer')} "
                            f"({_brief_data.get('network', '').title()}, "
                            f"{_brief_data.get('payout', 'Rate TBD')}, "
                            f"{_brief_data.get('geo', '')})"
                        ),
                    }
                except Exception as e:
                    log.warning(f"Failed to parse BRIEF_JSON: {e} — falling back to plain text")

            return text or "(no response)"

        # Process tool calls
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _run_tool(block.name, block.input)
                if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                    _brief_data = result  # capture for Block Kit
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result),
                })

        if not tool_results:
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return "(no response)"

        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What are the top finance opportunities we don't run yet?"
    print(f"\nQuery: {query}\n")
    print(ask(query))
