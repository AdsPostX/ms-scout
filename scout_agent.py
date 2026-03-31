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
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Optional

import anthropic
from dotenv import load_dotenv

load_dotenv(override=True)

log = logging.getLogger("scout_agent")

SNAPSHOT_PATH = pathlib.Path(__file__).parent / "data" / "offers_latest.json"

# ── Performance benchmark cache (refreshed hourly) ───────────────────────────
# Maps: category → {cvr_pct, rpm, sample_size}
#        offer_impact_id → {cvr_pct, rpm, adv_name}
_BENCHMARKS: dict = {}
_BENCHMARKS_LOADED_AT: float = 0.0
_BENCHMARKS_TTL = 3600  # 1 hour

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
    global _BENCHMARKS, _BENCHMARKS_LOADED_AT
    if not _BENCHMARKS or (time.time() - _BENCHMARKS_LOADED_AT) > _BENCHMARKS_TTL:
        _BENCHMARKS = _load_performance_benchmarks()
        _BENCHMARKS_LOADED_AT = time.time()
    return _BENCHMARKS


# Compiled once at module level — strips <<<SUGGESTIONS [...]  SUGGESTIONS>>> blocks from responses
_SUGG_RE = re.compile(r'<<<SUGGESTIONS\s*(\[.*?\])\s*SUGGESTIONS>>>', re.DOTALL)

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

MomentScience runs affiliate offers at post-transaction moments (right after a user completes a purchase or action). This context matters enormously: offers that work here are low-friction, recognizable brands, simple conversion events (email/signup/free trial). High-intent or complex offers (loans, insurance, medical programs) convert poorly regardless of payout.

You have access to 700+ offers across Impact, FlexOffers, MaxBounty AND real performance data — actual CVR and RPM from ClickHouse. Your job: help the team make confident offer decisions fast. No clarifying questions. Ever.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INTENT RECOGNITION — resolve every query to one of these intents, then act immediately.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. OPEN PROSPECTING (default for anything vague)
   Signals: greetings, "what's new", "what should we look at", "holler", "show me something", "what's good", "any ideas", no clear subject
   → Call get_top_opportunities() immediately. Lead with top 2-3 untapped offers by Scout Score.

2. INVENTORY STATUS — "what are we running?"
   Signals: "what's live", "what are we running", "what's in the platform", "what are we on", "what offers are active"
   → Call get_running_offers(). Show top performers with real CVR/RPM.

3. SPECIFIC OFFER RESEARCH — "tell me about X"
   Signals: advertiser name + any question, "tell me about", "what do we know about", "research X", "look up X"
   → Call search_offers(query=advertiser_name). Give full picture: payout, status, performance, fit note.

4. EXISTENCE CHECK — "do we have X?"
   Signals: "do we have", "do we run", "are we on", "is X live", "is X in the platform"
   → Call search_offers(query=X). Answer yes/no + status immediately. If live, show performance. If not, show payout + opportunity signal.

5. PERFORMANCE INTELLIGENCE — "what's working?"
   Signals: "what's performing", "what's converting", "what's our best", "what works for us", "top performers", "best RPM"
   → Call get_category_performance(). Lead with highest-RPM categories, then top individual offers.

6. VERTICAL / CATEGORY PROSPECTING — "fintech options?", "any health CPL?"
   Signals: category or vertical name + any qualifier ("options", "offers", "available", "show me", "find me", "what's out there")
   → Call get_top_opportunities(category=X). Show best untapped by Scout Score.

7. PAYOUT BENCHMARK — "is $25 CPL good for background checks?"
   Signals: dollar amount + payout type + context, "is this a good deal", "fair rate", "worth it", "good payout"
   → Call get_category_performance() for the relevant category. Compare stated payout to benchmark. Give a clear verdict.

8. GAP / PORTFOLIO ANALYSIS — "what are we missing?"
   Signals: "what gaps", "what are we missing", "what verticals", "diversify", "coverage", "what don't we have"
   → Call get_offer_stats() then get_category_performance(). Map what's covered vs. what's available. Highlight the highest-value gaps.

9. BRIEF BUILDING — "build a brief for X"
   Signals: "build", "create a brief", "draft", "let's do", "I like X", "set up X", "I want to run X"
   → Call draft_campaign_brief(advertiser=X). Generate copy, CTAs, targeting note. Output ONLY the JSON block (see format below).

10. SEASONAL / ENDEMIC — "any Q4 ideas?", "tax season?"
    Signals: season/holiday/calendar reference near offer context ("Q4", "holiday", "tax season", "back to school", "summer")
    → Call get_top_opportunities(). Filter mentally for seasonal fit. Note timing context explicitly.

11. PUBLISHER COMPETITIVE INTELLIGENCE — "would a higher payout win more AT&T impressions?"
    Signals: publisher name + payout change + impression share/volume/allocation/compete/win
    Examples: "would $40 CPA let TurboTax compete on AT&T?", "how much inventory would X get on Y?",
              "if we raise payout to $40 what happens on AT&T first 2 weeks of April?"
    → Call get_publisher_competitive_landscape(publisher_name=Y, offer_name=X, hypothetical_payout=N).
      Lead with the rank change and projected impressions. Be direct: "At $40, TurboTax ranks #3 of 8 — ~12% share = ~22K impressions over 2 weeks."
      Always compare current vs. hypothetical. Include the weekly impression volume so RevOps can size the opportunity.

12. FALLBACK / CONTINGENCY PLANNING — "what if X goes dark?", "what's our backup for Y?"
    Signals: "fallback", "backup", "alternative", "if X goes dark", "if X runs out of budget",
             "if we lose X", "what do we replace X with", "contingency", "if budget runs out"
    → Call get_fallback_candidates(offer_name=X). Lead with same-brand alternatives first
      ("Same brand, different network — plug-and-play swap"), then category subs.
      Frame as a ranked plan: "If Sam's Club goes dark: #1 swap is Sam's Club on MaxBounty
      (same brand, different source). If that's also unavailable, next best is..."

13. PAYOUT-BOUNDED PROSPECTING — "find offers with payout under X", "advertisers at $0.05 or less", "low-cost offers for partner Y"
    Signals: payout ceiling + browsing intent ("under", "at most", "≤", "or less", "no more than") with or without a publisher/partner qualifier
    Examples: "find advertisers with payout ≤ $0.05", "what offers are under a dollar?", "find low-payout options for partner 6103"
    → Step 1: If a publisher name or partner ID is given, call get_publisher_competitive_landscape(publisher_id=N or publisher_name=X) to understand what's running there and what categories they serve.
      Step 2: Call search_offers(query='', max_payout=X) — empty query browses the full inventory filtered by payout ceiling.
              Add network or category filters if given.
      Lead with count + top results by Scout Score. Note which are already in System vs. new opportunities.
      If a publisher was specified, frame results as "fits partner [X]'s profile" based on the categories they run.

14. PUBLISHER CONTEXT LOOKUP — "what does partner 6103 run?", "what's on publisher X?", "what's live on AT&T?"
    Signals: publisher name or ID + "what's running", "what's live", "what offers", "what do they run"
    → Call get_publisher_competitive_landscape(publisher_id=N or publisher_name=X).
      Lead with what's running and the competitive set. Include weekly impression volume.

15. DEMAND QUEUE STATUS — "what's in the queue?", "what's pending?"
    Signals: "queue", "pending", "pipeline", "what's approved", "what's waiting to go live", "what's been approved"
    → Call get_demand_queue_status(). Lead with count and any likely-live flags.
      "2 in queue: TurboTax and H&R Block.
       TurboTax looks live — ~12K impressions since approval. Confirm it: @Scout confirm TurboTax is live"
    If queue is empty: "Queue is clear — nothing pending."

16. CONFIRM LIVE — "TurboTax is live", "confirm X is live", "mark X as launched"
    Signals: "is live", "went live", "launched", "confirm live", "mark as launched", "is running"
    → Call mark_offer_launched(advertiser=X). Thread-only notification, no channel broadcast.
      "TurboTax confirmed live."

DEFAULT RULE: When the intent is unclear, always default to Intent 1 (open prospecting). Call get_top_opportunities() and show results. A confident answer to a slightly wrong interpretation is infinitely more useful than asking "what do you mean?"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUDIENCE FIT + PROJECTION RULE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Before citing any RPM or impression estimate for a publisher query, reason about fit first:
1. State the fit judgment as an opinion: "AT&T Payment Confirmation is a financial services
   publisher — TurboTax is a natural fit here, expect above-category CVR." Or: "This food
   delivery publisher is a stretch for a financial offer — expect below-category CVR."
2. Then cite the number with ~ approximation: "~22K impressions over 2 weeks"
3. If using a category benchmark (no live MS CVR for this exact offer), say it once:
   "Category estimate — no live CVR data for this offer yet."
4. One sharp insight about the biggest real-world variable, not a list of hedges:
   "Seasonal timing matters — tax season peaks through April, CVR will be higher right now."
Do NOT add boilerplate caveat lists. One confident insight beats five defensive hedges.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE STYLE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Lead with the verdict. Always.
"At $40 TurboTax jumps to #6 on AT&T — worth doing." Then the facts that back it up.
Never lead with data and bury the answer at the end.

Cut these phrases entirely: "Based on the data...", "Looking at this...", "It's worth noting...",
"I can see that...", "This suggests...", "Interestingly...", "To summarize..."

Short sentences. Conversational, not academic. Use ~ not false precision.
No preamble. No trailing summary — you already led with the verdict.
Max 5 offers. Flag gotchas inline: geo-limited, complex conversion, high-friction.

SLACK FORMATTING — strict mrkdwn only, never GitHub markdown:
RIGHT:  *Advertiser* · $X CPL · Impact · US
        _One-line reason with real data_
        • Alt 1 — brief note
        • Alt 2 — brief note

WRONG: | tables | **double asterisks** | ## headers | emojis | "Great question!" | "Based on the data..."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FOLLOW-UP SUGGESTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

After every non-brief response, append a suggestions block — these become clickable buttons:

<<<SUGGESTIONS
["short query 1", "short query 2", "short query 3"]
SUGGESTIONS>>>

Rules:
- Always 2-3 suggestions, never more or fewer
- Each is a short, complete query — specific to what was just answered, not generic
- After competitive landscape: "Run it at $50 CPA", "Fallback if [offer] hits cap?", "Who leads [publisher]?"
- After top opportunities: "Build a brief for [top offer name]", "What's live in [category]?"
- After offer search: "Build a brief for [offer]", "Fallback if this goes dark?", "More [category] options?"
- Max 50 characters per suggestion — they render as buttons
- Do NOT add suggestions after <<<BRIEF_JSON>>> responses — Approve/Reject are already there
- No double quotes inside suggestion strings

CAMPAIGN BRIEF MODE (Intent 9 only):
1. Call draft_campaign_brief() with the advertiser name
2. Generate 3 headline variants — post-transaction tone, under 60 chars
   Good: "You just unlocked 3 months free"  Bad: "Get SiriusXM today"
3. Generate 2 CTA Yes/No pairs (4-6 words max, 25-char platform limit)
4. One targeting note with CVR data if available
5. Output ONLY this JSON — no other text:
6. After the JSON, if fallback_same_brand is non-empty, add ONE line:
   "Backup plan: [advertiser] also on [network] — plug-and-play if this source hits cap."
   If only fallback_category_subs, add: "If this goes dark, next best in [category]: [name] ($X payout)."
   Skip entirely if both are empty.

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
            "Leave query empty ('') to browse all offers with only filters applied. "
            "Optional filters: network, category, min_payout, max_payout, ms_status. "
            "Returns results ranked by Scout Score (estimated RPM), not raw payout."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term — advertiser name or keyword. Use '' to browse all offers."},
                "network": {"type": "string", "description": "impact, flexoffers, or maxbounty"},
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness, Retail"},
                "min_payout": {"type": "number", "description": "Minimum payout amount (floor)"},
                "max_payout": {"type": "number", "description": "Maximum payout amount (ceiling), e.g. 0.05 for ≤$0.05"},
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
        "name": "get_publisher_competitive_landscape",
        "description": (
            "Query ClickHouse for what's currently running on a specific publisher (e.g. AT&T, TXB, MLB, or partner ID 6103), "
            "ranked by RPM. Answers questions like: 'would a higher payout help us win more AT&T impressions?', "
            "'how competitive is the TurboTax offer on AT&T?', 'how many impressions would we get?', "
            "'what does partner 6103 run?'. "
            "Supply offer_name + hypothetical_payout to get a rank-change + impression share projection. "
            "Use publisher_id when a numeric partner ID is given (e.g. 6103); use publisher_name otherwise."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK) e.g. 'AT&T', 'TXB'. Omit if using publisher_id."},
                "publisher_id": {"type": "integer", "description": "Numeric publisher/partner ID (e.g. 6103). Use when the user provides a partner number."},
                "offer_name": {"type": "string", "description": "Optional: offer/advertiser to rank in the competitive set e.g. 'TurboTax'"},
                "hypothetical_payout": {"type": "number", "description": "Optional: new payout to test (e.g. 40.0 for $40 CPA)"},
                "weeks": {"type": "integer", "description": "Optional: projection window in weeks (default 2)"},
            },
        },
    },
    {
        "name": "get_fallback_candidates",
        "description": (
            "When an offer might go dark (budget cap, advertiser pause, network issue), find the best replacement. "
            "Returns (1) same advertiser on a different network — plug-and-play swap, "
            "and (2) top category substitutes not currently live in MS, ranked by Scout Score. "
            "Use for: 'what's our fallback if X goes dark?', 'backup for Y', 'if X hits cap what do we run?', "
            "'what do we replace X with?', 'contingency plan for Y'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "offer_name": {"type": "string", "description": "The offer that may go dark"},
                "category": {"type": "string", "description": "Override category if needed"},
                "payout_type": {"type": "string", "description": "Optional: filter subs by payout type (CPA, CPL, etc.)"},
            },
            "required": ["offer_name"],
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
    {
        "name": "get_demand_queue_status",
        "description": (
            "Read the MS Demand Queue — approved offers waiting to go live. "
            "Cross-references ClickHouse to auto-detect if any queued offer is already live "
            "(impressions > 0 since approval date). "
            "Use for: 'what's in the queue?', 'what's pending?', 'what's waiting to go live?', "
            "'what's been approved?', 'queue status', 'pipeline'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "mark_offer_launched",
        "description": (
            "Mark an approved offer as live. Updates queue state and triggers a notification "
            "to the person who approved it + AdOps. Thread-only — no channel noise. "
            "Use when: 'TurboTax is live', 'confirm X is live', 'mark X as launched', "
            "'X went live', 'X is running'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser": {"type": "string", "description": "Advertiser name (partial match OK)"},
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
    max_payout: float = None,
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
        if max_payout is not None and payout_num > max_payout:
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


def _format_payout(payout_num, payout_type_norm: str, raw_payout: str) -> str:
    """Normalize payout display: '$300 / lead' not '300 $ per lead'."""
    if not payout_num:
        return raw_payout or "Rate TBD"
    ptype = (payout_type_norm or "").lower()
    num = float(payout_num)
    fmt = f"{num:,.0f}" if num >= 1 and num == int(num) else f"{num:,.2f}"
    if "lead" in ptype or "cpl" in ptype:
        return f"${fmt} / lead"
    elif "click" in ptype or "cpc" in ptype:
        return f"${fmt} / click"
    elif "sale" in ptype or "%" in ptype:
        return f"{num}% of sale"
    elif "install" in ptype or "cpi" in ptype:
        return f"${fmt} / install"
    elif "impression" in ptype or "cpm" in ptype:
        return f"${fmt} CPM"
    return f"${fmt}"


# Risk flags: keyed by trigger keyword lists.
# Shown on the brief to flag post-transaction fit issues before launch.
_RISK_PATTERNS = [
    (["employer", "hiring", "recruitment", "recruiter", "payroll", "crm", "erp", "b2b", "business banking"],
     "B2B intent — post-transaction CVR typically 50-70% lower than consumer offers; test conservatively"),
    (["loan", "lending", "mortgage", "refinance", "credit repair", "debt consolidation"],
     "Loan/credit offers are high-friction; monitor CVR closely and verify compliance"),
    (["glp-1", "weight loss program", "prescription", "telehealth", "medical weight"],
     "Medical program — high-intent required; verify geo/age compliance before launch"),
    (["insurance", "life insurance", "auto insurance", "home insurance"],
     "Insurance sign-ups are high-friction; expect below-category CVR post-transaction"),
    (["work from home", "earn from home", "make money online", "business opportunity", "profit scaling"],
     "Biz-opp offer — elevated brand risk; evaluate publisher fit carefully"),
]


def _get_risk_flag(advertiser: str, category: str, description: str) -> str:
    """Return a one-line risk warning if the offer is a poor post-transaction fit."""
    combined = f"{advertiser} {category} {description}".lower()
    for keywords, flag in _RISK_PATTERNS:
        if any(kw in combined for kw in keywords):
            return flag
    return ""


def _network_portal_url(network: str, offer_id: str) -> str:
    """Construct a direct link to the offer in the network's portal."""
    n = network.lower()
    if n == "maxbounty" and offer_id:
        return f"https://www.maxbounty.com/campaigns/detail/{offer_id}"
    elif n == "impact":
        return "https://app.impact.com"
    elif n == "flexoffers" and offer_id:
        return f"https://www.flexoffers.com/affiliate-programs/{offer_id}/"
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
    net = (o.get("network") or "").lower()
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

    # Creative URLs: MaxBounty/FlexOffers have CDN thumbnails; Impact falls back to OG scrape
    icon_url = o.get("icon_url", "")
    hero_url = o.get("hero_url", "")
    if not icon_url and o.get("tracking_url"):
        log.info(f"draft_campaign_brief: scraping OG image for {o.get('advertiser')}")
        og = _scrape_og_image(o.get("tracking_url", ""))
        icon_url = og
        hero_url = og

    score = _scout_score(o, benchmarks)

    # Proactive fallback intelligence — surface at brief creation (highest-intent moment)
    fallback = get_fallback_candidates(o.get("advertiser", ""), category=o.get("category"))
    fallback_same_brand = fallback.get("same_brand_alts", [])[:1]
    fallback_category_subs = fallback.get("category_alts", [])[:2]

    return {
        "advertiser": o.get("advertiser"),
        "network": o.get("network"),
        "offer_id": offer_id,
        "payout": _format_payout(o.get("_payout_num"), o.get("_payout_type_norm"), o.get("_raw_payout") or str(o.get("payout", ""))),
        "payout_num": o.get("_payout_num"),
        "payout_type": o.get("_payout_type_norm") or "",
        "geo": o.get("geo"),
        "tracking_url": o.get("tracking_url") or "Not available — pull from network portal",
        "description": (o.get("description") or "")[:300],
        "category": o.get("category"),
        "ms_status": o.get("_ms_status"),
        "performance_context": perf_context,
        "scout_score_rpm": score,
        "portal_url": _network_portal_url(net, offer_id),
        "risk_flag": _get_risk_flag(
            o.get("advertiser", ""),
            o.get("category", ""),
            o.get("description", ""),
        ),
        "icon_url": icon_url,
        "hero_url": hero_url,
        "fallback_same_brand": fallback_same_brand,
        "fallback_category_subs": fallback_category_subs,
    }


def get_publisher_competitive_landscape(
    publisher_name: str = None,
    publisher_id: int = None,
    offer_name: str = None,
    hypothetical_payout: float = None,
    weeks: int = 2,
) -> dict:
    """
    Query ClickHouse for what's competing on a given publisher right now.
    Returns: ranked offer list by RPM, publisher weekly impression volume,
    and (if offer_name + hypothetical_payout supplied) where that offer would rank
    at current vs. hypothetical payout.

    BLUF output: "At $40 CPA, TurboTax would rank #3 of 8 on AT&T (~12% share).
                  At $35 it ranks #5 (~7% share). AT&T runs ~180K impressions/week."
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

        # Step 1: find publisher — by numeric ID or name.
        # IMPORTANT: adpx_impressions_details.pid stores the numeric user id as a string,
        # NOT the hex sdk_id. Always use str(id) as the pid for impression queries.
        if not publisher_name and not publisher_id:
            return {"error": "Provide either publisher_name or publisher_id."}

        if publisher_id:
            pub_rows = ch.query(f"""
                SELECT id, organization, sdk_id
                FROM default.from_airbyte_users
                WHERE id = {int(publisher_id)}
                  AND deletedAt IS NULL
                  AND sdk_id IS NOT NULL
                  AND sdk_id != ''
                LIMIT 1
            """).result_rows
            if not pub_rows:
                return {"error": f"No publisher found with ID {publisher_id}."}
        else:
            pub_rows = ch.query(f"""
                SELECT id, organization, sdk_id
                FROM default.from_airbyte_users
                WHERE (lower(organization) LIKE lower('%{publisher_name}%')
                    OR lower(username) LIKE lower('%{publisher_name}%'))
                  AND deletedAt IS NULL
                  AND sdk_id IS NOT NULL
                  AND sdk_id != ''
                ORDER BY createdAt ASC
                LIMIT 10
            """).result_rows

        if not pub_rows:
            return {"error": f"No publisher found matching '{publisher_name}'. Try a shorter name e.g. 'AT&T' or 'TXB'."}

        # Pick the best row in a single pass — no extra ClickHouse roundtrips.
        # Priority: non-test/demo row whose id has the most recent impressions.
        # We rank candidates by a single aggregate query instead of probing one-by-one.
        candidate_ids = [str(row[0]) for row in pub_rows]
        id_list = ", ".join(f"'{i}'" for i in candidate_ids)
        volume_rows = ch.query(f"""
            SELECT pid, count() AS impressions
            FROM default.adpx_impressions_details
            PREWHERE pid IN ({id_list})
            WHERE created_at >= today() - 7
            GROUP BY pid
            ORDER BY impressions DESC
            LIMIT 10
        """).result_rows
        # Build a rank map: pid → impression count (0 if absent)
        vol_map = {str(r[0]): int(r[1]) for r in volume_rows}

        # Score each candidate: deprioritize test/demo, then rank by volume
        def _candidate_score(row) -> tuple:
            org = (row[1] or "").lower()
            is_noise = int("test" in org or "demo" in org)
            volume = vol_map.get(str(row[0]), 0)
            return (is_noise, -volume)  # lower is better

        chosen = sorted(pub_rows, key=_candidate_score)[0]

        pub_id_int, pub_full_name, pub_sdk_id = chosen[0], chosen[1], chosen[2]
        # pid in impressions table = numeric user id as string
        pub_pid = str(pub_id_int)

        # Step 2: weekly impression volume on this publisher (last 4 weeks avg)
        vol_rows = ch.query(f"""
            SELECT
                toStartOfWeek(i.created_at) AS week,
                count() AS impressions
            FROM default.adpx_impressions_details i
            PREWHERE i.pid = '{pub_pid}'
            WHERE i.created_at >= today() - 28
            GROUP BY week
            ORDER BY week DESC
        """).result_rows

        weekly_impressions = int(sum(r[1] for r in vol_rows) / max(len(vol_rows), 1)) if vol_rows else 0

        # Step 3: campaigns currently running on this publisher with RPM.
        # IMPORTANT: aggregate impressions and conversions independently before joining.
        # Row-level JOIN (impressions × conversions on session_id) causes fan-out:
        # a session with 3 impressions + 1 conversion produces count(cv.id)=3, not 1.
        camp_rows = ch.query(f"""
            SELECT
                c.adv_name,
                imp.campaign_id,
                imp.impressions,
                coalesce(cv.conversions, 0) AS conversions,
                coalesce(cv.total_revenue, 0) AS total_revenue,
                round(coalesce(cv.total_revenue, 0) / nullIf(imp.impressions, 0) * 1000, 2) AS rpm
            FROM (
                SELECT campaign_id, count() AS impressions
                FROM default.adpx_impressions_details
                PREWHERE pid = '{pub_pid}'
                WHERE created_at >= today() - 14
                GROUP BY campaign_id
            ) imp
            JOIN default.from_airbyte_campaigns c ON toInt64(imp.campaign_id) = toInt64(c.id)
            LEFT JOIN (
                SELECT cv.campaign_id,
                       count() AS conversions,
                       sum(toFloat64OrNull(cv.revenue)) AS total_revenue
                FROM default.adpx_conversionsdetails cv
                WHERE cv.session_id IN (
                    SELECT session_id
                    FROM default.adpx_impressions_details
                    PREWHERE pid = '{pub_pid}'
                    WHERE created_at >= today() - 14
                )
                  AND toYYYYMM(cv.created_at) >= toYYYYMM(today() - 28)
                GROUP BY cv.campaign_id
            ) cv ON toInt64(imp.campaign_id) = toInt64(cv.campaign_id)
            WHERE c.deleted_at IS NULL
              AND imp.impressions >= 50
            ORDER BY rpm DESC NULLS LAST
            LIMIT 20
        """).result_rows

        competitors = []
        for row in camp_rows:
            adv_name, campaign_id, impressions, conversions, _revenue, rpm = row
            competitors.append({
                "advertiser": adv_name,
                "campaign_id": str(campaign_id),
                "impressions_2w": int(impressions),
                "conversions_2w": int(conversions),
                "rpm": float(rpm or 0),
            })

        result = {
            "publisher": pub_full_name,
            "publisher_id": pub_id_int,
            "publisher_pid": pub_pid,  # numeric string — used as pid in impressions table
            "weekly_impressions_avg": weekly_impressions,
            "projected_impressions_2w": weekly_impressions * weeks,
            "active_competitors": competitors,
            "competitor_count": len(competitors),
        }

        # Step 4: if offer + payout provided, compute rank scenarios
        if offer_name and hypothetical_payout is not None:
            benchmarks = _get_benchmarks()

            def _est_rpm(payout: float) -> float:
                """Estimate RPM for the offer at a given payout using benchmark CVR."""
                by_cat = benchmarks.get("by_category", {})
                # TurboTax → Finance category
                for cat_name, cat_data in by_cat.items():
                    if "finance" in cat_name.lower() or "tax" in cat_name.lower():
                        cvr = cat_data.get("avg_cvr_pct", 0) / 100
                        return round(payout * cvr * 1000, 2)
                return round(payout * 0.02 * 1000, 2)  # fallback 2% CVR

            # Find the offer's current payout from offer snapshot
            offers = _load_offers()
            current_payout = None
            for o in offers:
                if offer_name.lower() in (o.get("advertiser") or "").lower():
                    current_payout = o.get("_payout_num")
                    break

            hyp_rpm = _est_rpm(hypothetical_payout)
            cur_rpm = _est_rpm(current_payout) if current_payout else None

            rpms = sorted([c["rpm"] for c in competitors if c["rpm"] > 0], reverse=True)

            def _rank(rpm: float) -> int:
                return sum(1 for r in rpms if r > rpm) + 1

            def _share_pct(rank: int, total: int) -> float:
                if total == 0:
                    return 0.0
                # Simple linear decay: rank 1 gets 2x share of last, proportional
                weight = max(total - rank + 1, 1)
                total_weight = sum(range(1, total + 2))
                return round(weight / total_weight * 100, 1)

            n = len(rpms)
            hyp_rank = _rank(hyp_rpm)
            hyp_share = _share_pct(hyp_rank, n)

            result["payout_scenario"] = {
                "offer": offer_name,
                "hypothetical_payout": hypothetical_payout,
                "hypothetical_rpm": hyp_rpm,
                "hypothetical_rank": hyp_rank,
                "hypothetical_impression_share_pct": hyp_share,
                "projected_impressions_2w": round(weekly_impressions * weeks * hyp_share / 100),
            }

            if cur_rpm is not None:
                cur_rank = _rank(cur_rpm)
                cur_share = _share_pct(cur_rank, n)
                result["payout_scenario"]["current_payout"] = current_payout
                result["payout_scenario"]["current_rpm"] = cur_rpm
                result["payout_scenario"]["current_rank"] = cur_rank
                result["payout_scenario"]["current_impression_share_pct"] = cur_share
                result["payout_scenario"]["current_impressions_2w"] = round(
                    weekly_impressions * weeks * cur_share / 100
                )

        return result

    except Exception as e:
        log.warning(f"get_publisher_competitive_landscape failed: {e}")
        return {"error": str(e)}


def get_fallback_candidates(
    offer_name: str,
    category: str = None,
    payout_type: str = None,
    limit: int = 4,
) -> dict:
    """
    Given an offer that's live (or might go dark), find the best replacements.

    Priority order:
    1. Same advertiser on a different network (Sam's Club on MaxBounty when Rakuten hits cap)
    2. Same category + similar payout type, not currently live in MS, ranked by Scout Score

    Returns two lists — 'same_brand_alts' and 'category_alts' — so Scout can present
    a tiered answer: same brand first (plug-and-play), then category substitutes.
    """
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    q = _norm(offer_name)

    # Find the primary offer to infer category/payout_type/network
    primary = None
    for o in offers:
        if q in _norm(o.get("advertiser", "")):
            primary = o
            break

    inferred_category = category or (primary.get("category") if primary else None)
    inferred_ptype = payout_type or (primary.get("_payout_type_norm") if primary else None)
    primary_network = _norm(primary.get("network") or "") if primary else None

    # Tier 1: same advertiser, different network
    same_brand = [
        o for o in offers
        if q in _norm(o.get("advertiser", ""))
        and _norm(o.get("network") or "") != primary_network
    ]
    same_brand.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)

    # Tier 2: same category, not already live, ranked by Scout Score
    cat_subs = [
        o for o in offers
        if o.get("_ms_status") != "Live"
        and inferred_category and _norm(inferred_category) in _norm(o.get("category") or "")
        and q not in _norm(o.get("advertiser") or "")
        and (not inferred_ptype or _norm(inferred_ptype) in _norm(o.get("_payout_type_norm") or ""))
    ]
    cat_subs.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)

    return {
        "primary_offer": offer_name,
        "primary_network": primary_network,
        "primary_category": inferred_category,
        "same_brand_alts": _format_offers(same_brand[:limit], benchmarks),
        "category_alts": _format_offers(cat_subs[:limit], benchmarks),
        "note": "same_brand_alts = same advertiser on a different network (plug-and-play swap). category_alts = next best in vertical if brand unavailable on any network.",
    }


# ── Demand Queue lifecycle tools ─────────────────────────────────────────────

_LAUNCHED_OFFERS_PATH = pathlib.Path(__file__).parent / "data" / "launched_offers.json"


def _load_launched_offers_state() -> dict:
    try:
        if _LAUNCHED_OFFERS_PATH.exists():
            return json.loads(_LAUNCHED_OFFERS_PATH.read_text())
    except Exception:
        pass
    return {}


def get_demand_queue_status() -> dict:
    """
    Read the MS Demand Queue state from launched_offers.json.
    Cross-references ClickHouse for impressions since each offer's approved_at date —
    if impressions > 0 the offer is likely live. Returns pending items with status.
    """
    state = _load_launched_offers_state()
    pending = [
        {**{"advertiser": k}, **v}
        for k, v in state.items()
        if v.get("status") == "queued"
    ]

    if not pending:
        return {"pending": [], "count": 0}

    # Batch ClickHouse check: impressions per advertiser since approved_at
    impression_counts: dict = {}
    try:
        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=os.getenv("CH_HOST", ""),
            user=os.getenv("CH_USER", "analytics"),
            password=os.getenv("CH_PASSWORD", ""),
            database=os.getenv("CH_DATABASE", "default"),
            secure=True,
        )
        # Find campaign IDs matching each advertiser name, then count impressions
        # since that offer's approved_at date
        for item in pending:
            adv = item["advertiser"]
            approved_at = item.get("approved_at", "2000-01-01T00:00:00")
            try:
                rows = ch.query(
                    """
                    SELECT count() AS imp_count
                    FROM default.adpx_impressions_details imp
                    JOIN default.mv_adpx_campaigns c ON toInt64(imp.campaign_id) = toInt64(c.id)
                    WHERE c.adv_name ILIKE %(adv)s
                      AND imp.created_at >= parseDateTimeBestEffort(%(approved_at)s)
                      AND toYYYYMM(imp.created_at) >= toYYYYMM(parseDateTimeBestEffort(%(approved_at)s))
                    """,
                    parameters={"adv": f"%{adv}%", "approved_at": approved_at},
                ).result_rows
                impression_counts[adv] = rows[0][0] if rows else 0
            except Exception as e:
                log.debug(f"CH impression check failed for {adv}: {e}")
                impression_counts[adv] = 0
    except Exception as e:
        log.warning(f"get_demand_queue_status: ClickHouse unavailable: {e}")

    result_items = []
    for item in pending:
        adv = item["advertiser"]
        imp = impression_counts.get(adv, 0)
        result_items.append({
            "advertiser": adv,
            "payout":     item.get("payout", ""),
            "network":    item.get("network", ""),
            "brief_url":  item.get("thread_url", ""),
            "approved_by": item.get("approved_by", ""),
            "approved_at": item.get("approved_at", ""),
            "status":     "likely_live" if imp > 0 else "pending",
            "impressions_since_approval": imp,
        })

    return {"pending": result_items, "count": len(result_items)}


def mark_offer_launched(advertiser: str) -> dict:
    """
    Mark an approved offer as live. Updates launched_offers.json status to 'launched'.
    scout_bot.py reads the result and sends a targeted notification to the approver + AdOps.
    """
    state = _load_launched_offers_state()

    # Fuzzy match — handles "TurboTax" matching "TurboTax 2025" etc.
    key = next(
        (k for k in state if advertiser.lower() in k.lower() or k.lower() in advertiser.lower()),
        advertiser,
    )

    entry = state.get(key, {})
    entry.update({"status": "launched", "launched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")})
    state[key] = entry

    try:
        _LAUNCHED_OFFERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        _LAUNCHED_OFFERS_PATH.write_text(json.dumps(state, indent=2))
    except Exception as e:
        log.warning(f"mark_offer_launched write failed: {e}")

    return {
        "status":      "launched",
        "advertiser":  key,
        "approved_by": entry.get("approved_by"),
        "thread_url":  entry.get("thread_url"),
        "payout":      entry.get("payout"),
        "network":     entry.get("network"),
    }


# ── Tool dispatch ─────────────────────────────────────────────────────────────

TOOL_MAP = {
    "search_offers": search_offers,
    "get_top_opportunities": get_top_opportunities,
    "get_running_offers": get_running_offers,
    "get_category_performance": get_category_performance,
    "get_offer_stats": get_offer_stats,
    "draft_campaign_brief": draft_campaign_brief,
    "get_publisher_competitive_landscape": get_publisher_competitive_landscape,
    "get_fallback_candidates": get_fallback_candidates,
    "get_demand_queue_status": get_demand_queue_status,
    "mark_offer_launched": mark_offer_launched,
}


def _run_tool(name: str, inputs: dict):
    fn = TOOL_MAP.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    return fn(**inputs)


# ── Agent loop ────────────────────────────────────────────────────────────────

def _extract_copy_from_text(text: str) -> dict:
    """
    Parse titles, CTAs, targeting, and bottom line from Claude's plain-text brief output.
    Used as fallback when Claude doesn't emit <<<BRIEF_JSON>>>.
    """
    copy: dict = {"titles": [], "ctas": [], "targeting": "", "bottom_line": ""}

    # Titles: numbered list — "1. text" or "1) text"
    title_matches = re.findall(r'(?:^|\n)\s*\d+[.)]\s+(.+?)(?=\n\s*\d+[.)]|\n\n|$)', text, re.MULTILINE)
    copy["titles"] = [t.strip() for t in title_matches[:3] if t.strip()]

    # CTAs: Yes: "..." / No: "..."
    cta_matches = re.findall(r'[Yy]es:\s*["\u201c]([^"\u201d]+)["\u201d]\s*/\s*[Nn]o:\s*["\u201c]([^"\u201d]+)["\u201d]', text)
    copy["ctas"] = [{"yes": y.strip(), "no": n.strip()} for y, n in cta_matches[:2]]

    # Targeting: line starting with "Targeting:"
    targeting_match = re.search(r'[Tt]argeting:\s*(.+?)(?=\n[A-Z*_]|\n\n|$)', text, re.DOTALL)
    if targeting_match:
        copy["targeting"] = targeting_match.group(1).strip()[:300]

    # Bottom line: last substantive paragraph (not the "Reply @Scout..." instruction)
    paragraphs = [p.strip() for p in text.split("\n") if p.strip() and "Reply" not in p and "launch this" not in p.lower()]
    if paragraphs:
        copy["bottom_line"] = paragraphs[-1][:200]

    return copy


def _extract_thread_entities(tool_results: list) -> dict:
    """
    Extract named entities from tool results accumulated during an agent turn.
    Tool-agnostic — works across get_publisher_competitive_landscape,
    draft_campaign_brief, search_offers, get_running_offers, etc.
    Returns a flat dict of resolved entities (empty if nothing found).
    scout_bot.py merges this into thread_context.json so follow-ups like
    "@Scout yes, $50 CPA" work without repeating publisher/offer/payout.
    """
    ctx: dict = {}
    for result in tool_results:
        if not isinstance(result, dict):
            # List results (e.g. search_offers) — grab category from first item
            if isinstance(result, list) and result:
                first = result[0] if isinstance(result[0], dict) else {}
                if first.get("category"):
                    ctx["category"] = first["category"]
            continue

        # Publisher competitive landscape
        if "publisher_id" in result:
            if result.get("publisher"):
                ctx["publisher"]    = result["publisher"]
            if result.get("publisher_id") is not None:
                ctx["publisher_id"] = result["publisher_id"]
            scenario = result.get("payout_scenario") or {}
            if scenario.get("offer"):
                ctx["offer"] = scenario["offer"]
            if scenario.get("current_payout") is not None:
                ctx["payout"]      = scenario["current_payout"]
                ctx["payout_type"] = "CPA"
            if scenario.get("hypothetical_payout") is not None:
                ctx.setdefault("scenarios_run", [])
                hyp = scenario["hypothetical_payout"]
                if hyp not in ctx["scenarios_run"]:
                    ctx["scenarios_run"].append(hyp)

        # Campaign brief (draft_campaign_brief result)
        if result.get("advertiser"):
            ctx["offer"] = result["advertiser"]
            if result.get("payout_num") is not None:
                ctx["payout"] = result["payout_num"]
            ptype = (result.get("payout_type") or "").upper()
            if ptype:
                ctx["payout_type"] = ptype

        # Offer search / running offers — capture category signal
        if result.get("category"):
            ctx["category"] = result["category"]

        # mark_offer_launched result — scout_bot.py posts the launch notification
        if result.get("status") == "launched" and result.get("advertiser"):
            ctx["launched_offer"] = result

    return {k: v for k, v in ctx.items() if v is not None}


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
    # List of brief results — append each draft_campaign_brief call result.
    # We use the FIRST successful result as the primary (handles multi-brief
    # requests where Claude calls the tool multiple times in one turn).
    _brief_results: list = []
    # All tool results from every loop iteration — used for entity extraction.
    _all_tool_results: list = []

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

            # If draft_campaign_brief was called at any point, always return a
            # structured brief dict — regardless of whether Claude used <<<BRIEF_JSON>>>.
            # This ensures _PENDING_BRIEFS is populated in scout_bot.py so "launch this" works.
            if _brief_results:
                brief_data = _brief_results[0]  # use first result (primary offer)
                copy_data: dict = {}

                # Preferred: Claude emitted structured JSON
                if "<<<BRIEF_JSON" in text and "BRIEF_JSON>>>" in text:
                    try:
                        json_str = text.split("<<<BRIEF_JSON")[1].split("BRIEF_JSON>>>")[0].strip()
                        copy_data = json.loads(json_str)
                        log.info(f"Parsed BRIEF_JSON for {brief_data.get('advertiser')}")
                    except Exception as e:
                        log.warning(f"Failed to parse BRIEF_JSON: {e} — extracting from plain text")

                # Fallback: extract copy from Claude's plain-text response
                if not copy_data or not copy_data.get("titles"):
                    copy_data = _extract_copy_from_text(text)
                    log.info(f"Extracted copy from plain text for {brief_data.get('advertiser')}: "
                             f"{len(copy_data.get('titles', []))} titles, {len(copy_data.get('ctas', []))} CTAs")

                return {
                    "type": "brief",
                    "brief_data": brief_data,
                    "copy": copy_data,
                    # Full Claude text as fallback so Slack shows something useful
                    # even if Block Kit rendering fails
                    "fallback_text": text or (
                        f"Campaign Brief — {brief_data.get('advertiser', 'Offer')} "
                        f"({brief_data.get('network', '').title()}, "
                        f"{brief_data.get('payout', 'Rate TBD')}, "
                        f"{brief_data.get('geo', '')})"
                    ),
                }

            # Parse and strip <<<SUGGESTIONS [...]  SUGGESTIONS>>> block from text.
            # Claude appends this to every non-brief response; scout_bot.py renders
            # them as Slack buttons so the user can explore without retyping.
            suggestions: list = []
            sugg_match = _SUGG_RE.search(text)
            if sugg_match:
                try:
                    suggestions = json.loads(sugg_match.group(1))
                except Exception:
                    suggestions = []
                text = text[:sugg_match.start()].rstrip()

            # General entity extraction — runs over all tool results from this turn.
            # Tool-agnostic: picks up publisher, offer, payout, category from any tool.
            # Returns {"type": "text_with_context", ...} so scout_bot.py can persist
            # the entities to thread_context.json for follow-up queries.
            if not _brief_results:
                extracted = _extract_thread_entities(_all_tool_results)
                if extracted or suggestions:
                    return {
                        "type": "text_with_context",
                        "text": text or "(no response)",
                        "extracted_context": extracted,
                        "suggestions": suggestions,
                    }

            return text or "(no response)"

        # Process tool calls
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _run_tool(block.name, block.input)
                if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                    _brief_results.append(result)  # collect all, use first for primary
                _all_tool_results.append(result)  # accumulate all for entity extraction
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
