"""
Scout — MomentScience Offer Intelligence Agent
Answers natural language questions about the offer inventory using Claude + 4 tools.
Data source: data/offers_latest.json (written by offer_scraper.py after each daily run)
"""

import json
import os
import pathlib
from typing import Optional

import anthropic
from dotenv import load_dotenv

load_dotenv(override=True)

SNAPSHOT_PATH = pathlib.Path(__file__).parent / "data" / "offers_latest.json"

SYSTEM_PROMPT = """You are Scout — MomentScience's offer intelligence assistant.
You know the company's full affiliate offer inventory: 700+ offers scraped daily from Impact, FlexOffers, and MaxBounty.
Each offer includes advertiser, network, payout, payout type, category, geo, and whether MomentScience is already running it (MS Status: Live / In System / Not in System).

Be direct and specific. Lead with the best options, not a list of everything.
When someone asks for offers, give the top picks with payout, network, and a one-line reason why.
If MS already runs an offer (MS Status = Live), flag it — don't pitch it as a new opportunity unless asked.
Speak like a sharp colleague who knows the inventory cold, not like a search engine.
Keep responses concise — this is Slack, not a report.

SLACK FORMATTING RULES — follow these exactly:
- Use *text* for bold (NOT **text**)
- Use _text_ for italic
- NEVER use markdown tables (| col | col |) — Slack does not render them
- For lists of offers, use this format per item:
  *1. Advertiser Name* — $X/lead (Network · Geo)
  _One-line reason why_
- Separate items with a blank line
- End with a short punchy summary or follow-up question on its own line"""

TOOLS = [
    {
        "name": "search_offers",
        "description": (
            "Full-text search across advertiser name and description. "
            "Use this for specific advertiser lookups or keyword searches. "
            "Optional filters: network, category, min_payout, ms_status. "
            "Returns top matches sorted by payout descending."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term — advertiser name or keyword"},
                "network": {"type": "string", "description": "impact, flexoffers, or maxbounty"},
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness, Retail"},
                "min_payout": {"type": "number", "description": "Minimum payout amount"},
                "ms_status": {"type": "string", "description": "Live, In System, or Not in System"},
                "limit": {"type": "integer", "description": "Max results (default 8)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_top_opportunities",
        "description": (
            "Returns highest-payout offers MS is NOT currently running (MS Status = Not in System). "
            "Use this for prospecting questions: 'what should we go after?', 'best untapped offers', etc. "
            "Optional filters: category, geo."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness"},
                "geo": {"type": "string", "description": "e.g. US Only, Global"},
                "limit": {"type": "integer", "description": "Max results (default 10)"},
            },
        },
    },
    {
        "name": "get_running_offers",
        "description": (
            "Returns offers MS is currently running (MS Status = Live). "
            "Use this to benchmark payouts, see what verticals are covered, "
            "or check if MS already has an offer from a specific advertiser. "
            "Optional filter: category."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness"},
            },
        },
    },
    {
        "name": "get_offer_stats",
        "description": (
            "Returns aggregate inventory stats: count and avg payout by network, "
            "count and avg payout by category, MS Status breakdown, and top 5 highest payout offers. "
            "Use this for strategic / high-level questions about the inventory."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
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


# ── Tool implementations ────────────────────────────────────────────────────

def search_offers(
    query: str,
    network: str = None,
    category: str = None,
    min_payout: float = None,
    ms_status: str = None,
    limit: int = 8,
) -> list:
    offers = _load_offers()
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

    results.sort(key=lambda x: x.get("_payout_num") or 0, reverse=True)
    return _format_offers(results[:limit])


def get_top_opportunities(category: str = None, geo: str = None, limit: int = 10) -> list:
    offers = _load_offers()
    results = []
    for o in offers:
        if o.get("_ms_status") != "Not in System":
            continue
        if category and _norm(category) not in _norm(o.get("category", "")):
            continue
        if geo and _norm(geo) not in _norm(o.get("geo", "")):
            continue
        results.append(o)

    results.sort(key=lambda x: x.get("_payout_num") or 0, reverse=True)
    return _format_offers(results[:limit])


def get_running_offers(category: str = None) -> list:
    offers = _load_offers()
    results = [
        o for o in offers
        if o.get("_ms_status") == "Live"
        and (not category or _norm(category) in _norm(o.get("category", "")))
    ]
    results.sort(key=lambda x: x.get("_payout_num") or 0, reverse=True)
    return _format_offers(results)


def get_offer_stats() -> dict:
    offers = _load_offers()
    if not offers:
        return {"error": "No offer data available"}

    by_network: dict = {}
    by_category: dict = {}
    by_ms_status: dict = {}

    for o in offers:
        net = o.get("network", "unknown")
        payout = o.get("_payout_num") or 0
        cats = o.get("_categories") or [o.get("category", "Other")]
        ms = o.get("_ms_status", "Unknown")

        by_network.setdefault(net, {"count": 0, "total_payout": 0})
        by_network[net]["count"] += 1
        by_network[net]["total_payout"] += payout

        for cat in (cats if isinstance(cats, list) else [cats]):
            by_category.setdefault(cat, {"count": 0, "total_payout": 0})
            by_category[cat]["count"] += 1
            by_category[cat]["total_payout"] += payout

        by_ms_status[ms] = by_ms_status.get(ms, 0) + 1

    top5 = sorted(offers, key=lambda x: x.get("_payout_num") or 0, reverse=True)[:5]

    return {
        "total_offers": len(offers),
        "by_network": {
            k: {"count": v["count"], "avg_payout": round(v["total_payout"] / v["count"], 2)}
            for k, v in sorted(by_network.items(), key=lambda x: -x[1]["count"])
        },
        "by_category": {
            k: {"count": v["count"], "avg_payout": round(v["total_payout"] / v["count"], 2)}
            for k, v in sorted(by_category.items(), key=lambda x: -x[1]["count"])
            if k and k != "Other"
        },
        "ms_status_breakdown": by_ms_status,
        "top_5_by_payout": _format_offers(top5),
    }


def _format_offers(offers: list) -> list:
    """Return a compact, readable version of each offer for the LLM."""
    out = []
    for o in offers:
        out.append({
            "advertiser": o.get("advertiser", ""),
            "network": o.get("network", ""),
            "payout": o.get("_raw_payout") or o.get("payout") or "Rate TBD",
            "payout_num": o.get("_payout_num"),
            "payout_type": o.get("_payout_type_norm") or o.get("payout_type", ""),
            "category": o.get("category", ""),
            "geo": o.get("geo", ""),
            "ms_status": o.get("_ms_status", ""),
            "ms_internal_name": o.get("_ms_internal_name", ""),
        })
    return out


# ── Tool dispatch ───────────────────────────────────────────────────────────

TOOL_MAP = {
    "search_offers": search_offers,
    "get_top_opportunities": get_top_opportunities,
    "get_running_offers": get_running_offers,
    "get_offer_stats": get_offer_stats,
}


def _run_tool(name: str, inputs: dict):
    fn = TOOL_MAP.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    return fn(**inputs)


# ── Agent loop ──────────────────────────────────────────────────────────────

def ask(user_message: str) -> str:
    """Send a message to Scout and get a response."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return "ANTHROPIC_API_KEY not set — Scout can't respond."

    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": user_message}]

    while True:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return "(no response)"

        # Process tool calls
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _run_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result),
                })

        if not tool_results:
            # No tool calls but not end_turn — return whatever text we have
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return "(no response)"

        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})


# ── CLI test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What are the top 5 finance opportunities we don't run yet?"
    print(f"\nQuery: {query}\n")
    print(ask(query))
