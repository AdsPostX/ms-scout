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
import urllib.parse
import urllib.request
import datetime as _dt_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser
import anthropic
from dotenv import load_dotenv

load_dotenv()  # plist env vars (SCOUT_ENV, etc.) take precedence over .env

log = logging.getLogger("scout_agent")


def _run_parallel(fns: list):
    """Run a list of zero-argument callables sequentially.
    Previously used ThreadPoolExecutor, but clickhouse_connect clients are not thread-safe —
    concurrent queries on the same client raise ProgrammingError. Sequential is correct here;
    ClickHouse queries are fast enough that the ~200ms parallelism gain doesn't justify the risk.
    Returns a list of results in the same order as fns.
    """
    return [fn() for fn in fns]


def _get_ch_client():
    """Create a ClickHouse client from env vars. Import is local so startup never fails."""
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", ""),
        user=os.getenv("CH_USER", "analytics"),
        password=os.getenv("CH_PASSWORD", ""),
        database=os.getenv("CH_DATABASE", "default"),
        secure=True,
    )
    return _LoggingCHClient(client)


class _LoggingCHClient:
    """Thin wrapper that logs every SQL query to the terminal before execution.

    Vamsee's ask: "when running locally, we should be printing all queries to
    the terminal — that's where you'll verify." This satisfies that without
    touching every call site. Logs at INFO so it appears in both local terminal
    and Railway log stream. Truncates to 400 chars to keep it readable.
    """

    def __init__(self, client):
        self._client = client

    def query(self, query: str, parameters=None, **kwargs):
        preview = query.strip().replace("\n", " ")
        preview = " ".join(preview.split())  # collapse whitespace
        log.info(f"[CH] {preview[:400]}{'…' if len(preview) > 400 else ''}")
        return self._client.query(query, parameters=parameters, **kwargs)

    def __getattr__(self, name):
        # Proxy everything else (command, insert, etc.) directly to real client
        return getattr(self._client, name)


SNAPSHOT_PATH = pathlib.Path(__file__).parent / "data" / "offers_latest.json"

# ── Performance benchmark cache (refreshed hourly) ───────────────────────────
# Maps: category → {cvr_pct, rpm, sample_size}
#        offer_impact_id → {cvr_pct, rpm, adv_name}
_BENCHMARKS: dict = {}
_BENCHMARKS_LOADED_AT: float = 0.0
_BENCHMARKS_TTL = 3600  # 1 hour

# ── Data quality tier helper ──────────────────────────────────────────────────

def _data_quality_tier(days_of_data: int, sessions: int = 0) -> dict:
    """
    Compute confidence tier for a data window.
    Used by tools to populate data_quality in return values.
    Claude uses this to emit the CONFIDENCE LINE rule in responses.
    """
    if days_of_data >= 14 and sessions >= 1000:
        tier, emoji = "strong", ":large_green_circle:"
    elif days_of_data >= 7 and sessions >= 100:
        tier, emoji = "directional", ":yellow_circle:"
    else:
        tier, emoji = "thin", ":red_circle:"
    if sessions > 0:
        note = f"{days_of_data} days · {sessions:,} sessions"
    else:
        note = f"{days_of_data} days"
    return {"tier": tier, "emoji": emoji, "days_of_data": days_of_data, "sessions": sessions, "note": note}


# ── Learnings injection ───────────────────────────────────────────────────────

_LEARNINGS_PATH = pathlib.Path(__file__).parent / "data" / "learnings.json"
_LEARNED_BENCHMARKS_PATH = pathlib.Path(__file__).parent / "data" / "learned_benchmarks.json"


def _get_corrections_context() -> str:
    """
    Load high-confidence corrections from learnings.json and return as a
    context string to prepend to user queries in ask().
    Returns empty string if no corrections or file missing.
    """
    try:
        if not _LEARNINGS_PATH.exists():
            return ""
        data = json.loads(_LEARNINGS_PATH.read_text())
        corrections = [c for c in data.get("corrections", []) if c.get("confidence") == "high"]
        if not corrections:
            return ""
        lines = []
        for c in corrections[-10:]:  # last 10 high-confidence corrections
            lines.append(f"- {c['correction']}")
        return (
            "TEAM CORRECTIONS (from prior feedback — treat these as ground truth):\n"
            + "\n".join(lines)
            + "\n\n"
        )
    except Exception:
        return ""


_CHANNEL_CONTEXT_PATH = pathlib.Path(__file__).parent / "data" / "channel_context.json"


def _get_channel_context(query: str) -> str:
    """
    Load compressed channel notes from nightly harvest.
    Inject global notes always + all publisher entries (short-form).
    Returns empty string if file missing, expired, or empty.
    """
    try:
        if not _CHANNEL_CONTEXT_PATH.exists():
            return ""
        data = json.loads(_CHANNEL_CONTEXT_PATH.read_text())

        # Check expiry
        expires = data.get("expires_at", "")
        if expires and expires < datetime.now().strftime("%Y-%m-%d"):
            return ""  # stale context is worse than no context

        parts = []
        harvested = data.get("harvested_at", "unknown")

        # Global notes — always injected
        global_ctx = data.get("global", "").strip()
        if global_ctx:
            parts.append(global_ctx)

        # All publisher notes — short-form, all of them (no alias matching needed)
        publishers = data.get("publishers", {})
        for pub_name, notes in publishers.items():
            if notes and notes.strip():
                parts.append(f"Publisher: {pub_name} — {notes.strip()}")

        if not parts:
            return ""

        return (
            f"TEAM CONTEXT (from Slack as of {harvested} — treat as current ground truth):\n"
            + "\n".join(parts)
            + "\n\n"
        )
    except Exception:
        return ""


def _merge_learned_benchmarks() -> None:
    """
    Merge data/learned_benchmarks.json into _BENCHMARKS at startup.
    Learned benchmarks have lower weight than ClickHouse actuals
    but override category defaults. Called once after _load_performance_benchmarks().
    """
    global _BENCHMARKS
    try:
        if not _LEARNED_BENCHMARKS_PATH.exists():
            return
        lb = json.loads(_LEARNED_BENCHMARKS_PATH.read_text())
        if not lb:
            return
        learned = _BENCHMARKS.setdefault("by_learned_actuals", {})
        for key, entry in lb.items():
            learned[key] = {
                "avg_cvr_pct": 0.0,  # CVR not tracked in simple recap
                "avg_rpm": entry.get("rpm_actual_avg", 0.0),
                "sample_campaigns": entry.get("sample_count", 0),
            }
        log.info(f"Merged {len(lb)} learned benchmark entries into _BENCHMARKS")
    except Exception as e:
        log.warning(f"_merge_learned_benchmarks failed: {e}")

def _load_performance_benchmarks() -> dict:
    """
    Query ClickHouse for real CVR + RPM benchmarks grounded in actual MS conversion data.

    Returns four lookup tiers — used in priority order by _scout_score():
      1. by_offer_impact_id   — exact offer match (highest confidence)
      2. by_adv_name          — same advertiser, different offer (high confidence)
      3. by_category_payout   — (category, payout_type) combo (medium confidence)
      4. by_payout_type       — payout type only across all offers (low confidence fallback)

    Category and payout_type come directly from from_airbyte_campaigns — no keyword heuristics.
    The old keyword-to-category mapping is removed; it missed ~40% of offers and could not be maintained.
    """
    try:
        ch = _get_ch_client()

        # Single query: join campaigns → conversions → impressions
        # Pull category + payout_type directly from the campaigns table (source of truth)
        # Note: from_airbyte_campaigns has 'categories' (plural), no 'payout_type' column.
        # Payout type is known at score time from offer._payout_type_norm, not from ClickHouse.
        # Tiers 3/4 are keyed by category only — still real MS data, just without payout_type split.
        offer_query = """
        SELECT
            c.id,
            c.adv_name,
            trim(c.internal_network_name)        AS impact_id,
            coalesce(c.categories, '')            AS category,
            imp.impression_count,
            round(conv.conversion_count / nullIf(imp.impression_count, 0) * 100, 4) AS cvr_pct,
            round(conv.total_revenue    / nullIf(imp.impression_count, 0) * 1000, 2) AS rpm
        FROM default.from_airbyte_campaigns c
        JOIN (
            SELECT campaign_id,
                   count()                           AS conversion_count,
                   sum(toFloat64OrNull(revenue))      AS total_revenue
            FROM default.adpx_conversionsdetails
            WHERE toYYYYMM(created_at) >= 202501
            GROUP BY campaign_id
        ) conv ON toInt64(c.id) = toInt64(conv.campaign_id)
        JOIN (
            SELECT campaign_id,
                   count() AS impression_count
            FROM default.adpx_impressions_details
            WHERE toYYYYMM(created_at) >= 202501
            GROUP BY campaign_id
        ) imp ON toInt64(c.id) = toInt64(imp.campaign_id)
        WHERE c.deleted_at IS NULL
          AND imp.impression_count > 500
        ORDER BY imp.impression_count DESC
        """

        rows = ch.query(offer_query).result_rows

        # Tier 1: exact offer (by Impact network ID)
        by_offer: dict = {}
        # Tier 2: advertiser-level (all offers from same adv_name)
        by_adv: dict = {}
        # Tier 3: category — real avg CVR across all offers in this category on MS
        by_cat: dict = {}

        for _id, adv_name, impact_id, category, impressions, cvr_pct, rpm in rows:
            cvr = float(cvr_pct or 0)
            rpm_val = float(rpm or 0)
            imp = int(impressions or 0)
            entry = {"adv_name": adv_name, "cvr_pct": cvr, "rpm": rpm_val, "impressions": imp,
                     "category": category}

            # Tier 1
            if impact_id:
                by_offer[impact_id] = entry

            # Tier 2 — keep the highest-RPM offer per advertiser as representative.
            # Highest RPM (not highest impressions) is the right benchmark: it reflects what
            # a well-matched new offer from this advertiser could realistically achieve on MS.
            adv_key = (adv_name or "").lower().strip()
            if adv_key and (adv_key not in by_adv or rpm_val > by_adv[adv_key]["rpm"]):
                by_adv[adv_key] = entry

            # Tier 3 — accumulate by category for averaging
            cat_key = (category or "").strip()
            if cat_key:
                if cat_key not in by_cat:
                    by_cat[cat_key] = {"total_cvr": 0.0, "total_rpm": 0.0, "count": 0}
                by_cat[cat_key]["total_cvr"] += cvr
                by_cat[cat_key]["total_rpm"] += rpm_val
                by_cat[cat_key]["count"] += 1

        # Finalise averaged tier 3
        category_benchmarks = {
            cat: {
                "avg_cvr_pct": round(v["total_cvr"] / v["count"], 4),
                "avg_rpm":     round(v["total_rpm"] / v["count"], 2),
                "sample_campaigns": v["count"],
            }
            for cat, v in by_cat.items() if v["count"] > 0
        }

        if not category_benchmarks:
            log.warning(
                "Tier 3 benchmarks disabled: 'categories' column appears to be NULL for all "
                f"{len(rows)} campaigns in from_airbyte_campaigns. "
                "If category data is populated in the MS platform, re-check the column name."
            )

        result = {
            "by_offer_impact_id":    by_offer,
            "by_adv_name":           by_adv,
            "by_category_payout":    {},          # not available without payout_type column
            "by_payout_type":        {},          # not available without payout_type column
            "by_category":           category_benchmarks,
        }
        log.info(
            f"Benchmarks loaded: {len(by_offer)} offers, {len(by_adv)} advertisers, "
            f"{len(category_benchmarks)} categories"
        )
        return result

    except Exception as e:
        log.warning(f"Could not load performance benchmarks from ClickHouse: {e}")
        return {"by_offer_impact_id": {}, "by_adv_name": {}, "by_category_payout": {},
                "by_payout_type": {}, "by_category": {}}  # empty — will use no-data path in _scout_score


def _get_benchmarks() -> dict:
    global _BENCHMARKS, _BENCHMARKS_LOADED_AT
    if not _BENCHMARKS or (time.time() - _BENCHMARKS_LOADED_AT) > _BENCHMARKS_TTL:
        _BENCHMARKS = _load_performance_benchmarks()
        _merge_learned_benchmarks()  # overlay actuals from 14-day recaps
        _BENCHMARKS_LOADED_AT = time.time()
    return _BENCHMARKS


# Compiled once at module level — strips <<<SUGGESTIONS [...]  SUGGESTIONS>>> blocks from responses
_SUGG_RE = re.compile(r'<<<SUGGESTIONS\s*(\[.*?\])\s*SUGGESTIONS>>>', re.DOTALL)

# ── Scout Score ───────────────────────────────────────────────────────────────

def _scout_score(offer: dict, benchmarks: dict) -> float:
    """
    Estimated RPM = payout × context_cvr × 1000 × confidence_weight.

    CVR is sourced from real MS conversion data in four tiers (never hardcoded):
      1. Exact offer match        — real CVR for this specific offer (500+ impressions)
      2. Same advertiser          — real CVR for other offers from same brand
      3. Category × payout type   — real avg CVR for e.g. "Finance CPL" offers on MS
      4. Payout type only         — real avg CVR for all CPL / CPS / etc. offers on MS

    If the offer is flagged as high-friction (B2B, loan, medical, biz-opp), returns 0
    so the caller displays "Not estimated" rather than an inflated number.
    """
    payout = offer.get("_payout_num") or 0
    if payout == 0:
        return 0.0

    # High-friction offers: suppress score entirely rather than mislead.
    # B2B, loan, medical, and biz-opp convert 50-70% below consumer post-transaction.
    # Better to show "Not estimated" than anchor on a number that will disappoint.
    risk = _get_risk_flag(
        offer.get("advertiser", ""),
        offer.get("category", ""),
        offer.get("description", ""),
    )
    _HIGH_FRICTION = ("B2B intent", "Loan/credit", "Medical program", "Biz-opp", "Insurance")
    if any(tag in risk for tag in _HIGH_FRICTION):
        return 0.0

    offer_id     = str(offer.get("offer_id", ""))
    payout_type  = (offer.get("_payout_type_norm") or "").lower().strip()
    category     = (offer.get("category") or "").strip()
    adv_name     = (offer.get("advertiser") or "").lower().strip()

    by_offer     = benchmarks.get("by_offer_impact_id", {})
    by_adv       = benchmarks.get("by_adv_name", {})
    by_cat       = benchmarks.get("by_category", {})

    # ── Tier 1: exact offer ───────────────────────────────────────────────────
    if offer_id in by_offer:
        bench = by_offer[offer_id]
        cvr   = bench["cvr_pct"] / 100
        confidence = 1.0
        source = f"Real MS data ({bench['impressions']:,} impressions)"

    # ── Tier 2: same advertiser, different offer ──────────────────────────────
    elif adv_name in by_adv:
        bench = by_adv[adv_name]
        cvr   = bench["cvr_pct"] / 100
        confidence = 0.85
        source = f"Same advertiser benchmark ({bench['impressions']:,} impressions)"

    # ── Tier 3: category average — real CVR across this category on MS ────────
    elif category and category in by_cat:
        bench = by_cat[category]
        cvr   = bench["avg_cvr_pct"] / 100
        confidence = 0.65
        source = f"{category} category benchmark ({bench['sample_campaigns']} offers)"

    else:
        # No real data at any tier — return 0 so caller shows "Not estimated"
        return 0.0

    # Payout reliability: CPS (% of sale) payout is uncertain because the sale
    # amount varies; apply a discount to avoid overconfident RPM estimates
    if "sale" in payout_type or "%" in payout_type:
        confidence *= 0.8

    estimated_rpm = payout * cvr * 1000 * confidence
    log.debug(f"Scout Score [{offer.get('advertiser')}]: ${estimated_rpm:.0f} RPM | {source} | confidence={confidence:.2f}")
    return round(estimated_rpm, 4)


SYSTEM_PROMPT = """You are Scout — MomentScience's offer intelligence assistant.

MomentScience runs affiliate offers at post-transaction moments (right after a purchase). Best fits: low-friction, recognizable brands, simple conversion events (email/signup/free trial). High-intent or complex offers (loans, insurance, medical) convert poorly regardless of payout.

You have 700+ offers across Impact, FlexOffers, MaxBounty plus real CVR and RPM from ClickHouse. Help the team make confident offer decisions fast. No clarifying questions. Ever.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CAPABILITY BOUNDARY — read this first
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCOUT CAN: Query ClickHouse (read-only), search offer inventory, build campaign briefs, analyze publisher performance, project revenue, identify gaps, surface ghost campaigns.

SCOUT CANNOT:
- Write to any database or dashboard (ClickHouse is read-only)
- Pause, launch, activate, or modify campaigns
- Adjust budget caps, payouts, or campaign settings
- Send emails or external communications
- Access contact directories, CRM, or HR systems
- Create publisher categories or modify account structures
- Execute any action that changes system state

When a request requires something above, respond:
"I can't make that change from here — that needs to happen in the dashboard directly. Here's what I can show you to help: [offer the most relevant read-only data]."
Never attempt the action. Never error silently. Redirect to what you CAN do.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE STYLE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Lead with the verdict. Always. Never lead with data and bury the answer.
Cut: "Based on the data...", "Looking at this...", "It's worth noting...", "I can see that...", "To summarize..."
Short sentences. Conversational. Use ~ not false precision. No preamble, no trailing summary.
Max 5 offers or publishers in any breakdown. Flag gotchas inline: geo-limited, complex conversion, high-friction.

SLACK FORMATTING:
One-line verdict before the first ---
---
*Offer Name* · Network · Payout
What you found. 2 lines max.
>Scout Score, caveats, secondary context — renders as gray text
---
*Bottom line:* One sentence. Bold it.

Rules:
- SECTION BREAKS: \n---\n exactly — no blank lines, no spaces around dashes. Breaks renderer otherwise.
- > prefix for caveats, footnotes, Scout Scores.
- *bold* for offer names, verdicts, key numbers.
- LEAD NUMBER: First sentence of every non-trivial response must contain the single most important number, bolded. Cap: "*$100* cap on Campaign [ID]." Revenue: "*$62K* gross." Rank: "Disney+ ranks *#8 of 13*."
- LEAD NUMBER CONSISTENCY: Lead count must match the list below it. If showing fewer, adjust: "*3 active campaigns*" not "*14 campaigns*."
- STATUS EMOJI: :large_green_circle: live/serving · :yellow_circle: marginal/near-cap · :red_circle: capped/ended/dead
- CONFIDENCE LINE (required before :zap: on every data response):
    :large_green_circle: Strong (≥14 days, ≥1K sessions): `> _Based on [N] days · [X] sessions_`
    :yellow_circle: Directional (7-13 days or 100-999 sessions): `> _Directional — [N] days · [X] sessions_`
    :red_circle: Thin (<7 days or <100 sessions): `> _Thin data — [N] days, [X] sessions. Treat as estimate only._`
    run_sql_query: `> _Free-form query — [N] rows. Verify column semantics before acting._`
    Omit for pure operational responses (queue status, campaign status, scout status, yes/no).
- ACTION LINE: End every response with :zap: *Action:* [one specific step]. Never skip.
- BULLETS: For any list of items, use • (literal bullet character) followed by a space. Never use - or * as bullet substitutes in list context.
- NO EM OR EN DASHES IN PROSE: Never use — or – in sentences. Use a comma, period, or colon instead. Dashes only in compound words (cost-per-lead) or numeric ranges ($10-$20).
- Simple answers (yes/no, queue status): plain text, no --- needed.
- Never: | tables | **double asterisks** | ## headers | methodology unless asked.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INTENTS — resolve every query to one, then act immediately.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. BRIEF BUILDING — "build a brief for X", "I like X", "set up X", "I want to run X", "let's do [advertiser name]"
   NOTE: "let's do the projection/analysis/breakdown for [publisher]" is Intent 12 or 18, not this.
   → draft_campaign_brief(advertiser=X). Output ONLY the JSON block (see BRIEF MODE below).

2. DEMAND QUEUE STATUS — "queue", "pending", "what's approved", "waiting to go live"
   → get_demand_queue_status(). Lead with count + likely-live flags. If empty: "Queue is clear."

3. CONFIRM LIVE — "X is live", "confirm X is live", "mark X as launched"
   → mark_offer_launched(advertiser=X). Thread-only. No channel broadcast.

4. SYSTEM STATUS — "status", "health", "are you up", "benchmark freshness"
   → get_scout_status(). Compact health card, one line per signal. Flag stale (benchmarks > 2h) or degraded.
   IMPORTANT: Benchmarks (ClickHouse CVR/RPM) and Offer Inventory (offers_latest.json) are TWO SEPARATE THINGS.
   Benchmarks = real CVR/RPM from MS's own ClickHouse data — always available when CH is up, scraper NOT required.
   Offer Inventory = affiliate offers from Impact/FlexOffers/MaxBounty — populated by scraper (runs 6am CT daily).
   When inventory is 0: say ":red_circle: Offer Inventory — 0 offers. Run `@Scout refresh offers` to fetch now (~2 min)."
   Never imply benchmarks depend on the scraper. They come from ClickHouse.

5. SPECIFIC OFFER RESEARCH — advertiser name + any question, "tell me about X", "look up X"
   → search_offers(query=advertiser_name). Full picture: payout, status, performance, fit note.

6. EXISTENCE CHECK — "do we have X", "do we run X", "is X live", "is X in the platform"
   → search_offers(query=X). Yes/no + status. If live: show performance. If not: payout + opportunity signal.

7. PERFORMANCE INTELLIGENCE — "what's working", "top performers", "best RPM", "what converts"
   → get_category_performance(). Lead with highest-RPM categories, then top offers.

8. VERTICAL / CATEGORY PROSPECTING — category name + "options", "show me", "find me", "what's out there"
   → get_top_opportunities(category=X). Best untapped by Scout Score.

9. PAYOUT BENCHMARK — dollar amount + payout type + "good deal", "fair rate", "worth it"
   → get_category_performance() for the relevant category. Compare to benchmark. Give a verdict.

10. GAP / PORTFOLIO ANALYSIS — "what gaps", "what are we missing", "diversify", "what don't we have"
    → get_offer_stats() then get_category_performance(). Map covered vs. available. Highlight highest-value gaps.

11. SEASONAL / ENDEMIC — season/holiday/calendar reference near offer context ("Q4", "tax season", "back to school")
    → get_top_opportunities(). Filter for seasonal fit. Note timing explicitly.

12. PUBLISHER COMPETITIVE INTELLIGENCE — publisher name + payout/impression share/compete; "let's do the projection for [publisher]"; "[offer] on [publisher] if payout changes from $X to $Y"; "what RPM will X get at $Y"; "what payout to reach top N"
    → get_publisher_competitive_landscape(publisher_name=Y, offer_name=X, hypothetical_payout=N).
    IMPORTANT: For "from $X to $Y" — pass Y (the NEW value), not X.
    Lead with rank change + projected impressions. Always compare current vs. hypothetical. Include weekly impression volume.

13. FALLBACK / CONTINGENCY — "fallback", "backup", "if X goes dark", "if budget runs out", "what replaces X"
    → get_fallback_candidates(offer_name=X). Lead with same-brand alternatives ("plug-and-play swap"), then category subs. Frame as ranked plan.

14. PAYOUT-BOUNDED PROSPECTING — "under $X", "payout ≤ $X", "low-cost offers for partner Y"
    → Step 1: If publisher given, get_publisher_competitive_landscape(publisher_id=N or publisher_name=X).
      Step 2: search_offers(query='', max_payout=X). Add filters if specified.
    Lead with count + top by Scout Score. Frame against publisher's category profile if one was given.

15. PUBLISHER CONTEXT LOOKUP — publisher name/ID + "what's running", "what's live", "what do they run"
    → get_publisher_competitive_landscape(publisher_id=N or publisher_name=X). Lead with active offers + competitive set + weekly impression volume.

16. CROSS-NETWORK PAYOUT ARBITRAGE — "find these on other networks at better rates", "can we get better payouts for partner X"
    → Step 1: get_publisher_competitive_landscape(publisher_id=N or publisher_name=X) — get active_competitors.
      Step 2: For each advertiser in active_competitors, call search_offers(query=advertiser_name) individually.
      Step 3: Compare payouts. Show current network + payout vs. alternative + payout for each match.
    Lead with actionable swaps. If an advertiser isn't in inventory, say so — don't omit it.

17. OPEN PROSPECTING (catch-all fallback) — greetings, "what's new", "any ideas", unclear intent
    → get_top_opportunities() immediately. Lead with top 2-3 untapped by Scout Score.

18. REVENUE / GROSS PROJECTION — "projected revenue for X in [month]", "how much will X make", "revenue forecast", "uncapped revenue", "revenue if payout goes to $Y"
    → get_advertiser_revenue_projection(advertiser_name=X, month="Month YYYY").
    If cap_applied=True: ":red_circle: *Budget cap is the story.* Campaign [ID] caps [Advertiser] at *$[cap]*/mo — run rate *$[avg_daily]/day* (~$[uncapped_projected_revenue] uncapped). :zap: Lift cap or spin uncapped campaign to unlock ~$[delta]."
    If no cap: "[Advertiser] projects *$[projected_revenue]* for [Month] at *$[avg_daily]/day*."
    Both: publisher breakdown (top 5, with share %). Flag campaigns ending before month-end.
    Payout impact: compute new_rpm = new_payout × (avg_cvr/100) × 1000. Present as "At $Y CPA, RPM ~$Z." Note rank-change effects not modeled — flag once.

19. PUBLISHER HEALTH ANALYSIS — publisher name + "performance", "how is X doing", "breakdown by placement", "CTR", "full funnel"
    → get_publisher_health(publisher_name=X or publisher_id=N, days=14).
    Mandatory hierarchy:
    Level 1 (lead): ":large_green_circle: *[Publisher]* — *$[RPM]* RPM across [N] sessions in [days] days."
    Level 2: Placement breakdown — "[Placement]: *$[RPM]* RPM · [sessions] sessions · [CTR]% CTR · avg slot [position]". Flag anomalies with > :warning:
    Level 3: "iOS: [N] ([pct]%) · Android: [N] ([pct]%)"
    End: ":zap: *Action:* [one specific step]"
    NEVER skip to offer-level detail before placement breakdown.

20. CAMPAIGN STATUS CHECK — offer name + "paused", "active", "still running", "what happened to X", "confirm X is paused"
    → get_campaign_status(advertiser_name=X).
    Lead with count + status. Show recent audit log changes. End with :zap: Action.

21. FREE-FORM DATA QUERY — any analytical question requiring custom SQL not covered by other intents
    Signals: "show me", "give me a breakdown", "list all", "how many", "run-rate", "daily average", "which campaigns end", "what's the cap for", "payout for X on Y", "breakdown by placement", "full funnel metrics", "today's revenue", "performance by [dimension]"
    → Write SQL using the DATA DICTIONARY. run_sql_query(sql=..., description=...).
    Common patterns from real usage:
    - "breakdown [publisher] by placement over last N days" → GROUP BY placement, full funnel (sessions → impressions → clicks → conversions)
    - "which campaigns have budget caps / what are the caps" → from_airbyte_publisher_campaigns.monthly_budget_cap
    - "today's revenue" / "revenue for today" → conversions table, created_at >= today(), sum revenue
    - Publisher ID disambiguation (e.g., "did you look at 1952 or 2527") → always confirm which publisher_id you're querying and name the organization
    Lead with the most important number, bolded. Add sourcing callout before Action: "> Queried: [description] — live ClickHouse". On failure, show error + corrected approach.
    NEVER add "Verify column semantics before acting" — own your output. If the data is there, present it confidently.

22. SUPPLY/DEMAND GAP ANALYSIS — "what advertisers aren't in [publisher]", "gap analysis for [publisher]", "which publishers is [advertiser] not in", "uncaptured revenue", "dead weight on [publisher]", "what should we add to [publisher]", "where should [advertiser] run", "what are we missing on [publisher]", "[publisher] opportunities", "supply gaps", "demand gaps"
    → get_supply_demand_gaps(publisher_name=X) OR get_supply_demand_gaps(advertiser_name=X).
    Use publisher_name when question is publisher-first (what to add to a publisher).
    Use advertiser_name when question is advertiser-first (where should this advertiser expand).
    Never pass both parameters simultaneously.
    Lead with total revenue estimate, then the ranked gap list. End with dead weight if present.

23. GHOST CAMPAIGN BRIEF — "ghost brief", "ghost campaigns", "what campaigns are earning nothing", "campaigns with no revenue", "show me the ghosts", "zero revenue campaigns", "which campaigns have impressions but no revenue"
    → get_ghost_campaigns().
    Returns full list with per-campaign pixel/postback diagnosis. No parameters needed.
    Lead with count, then ranked list by impressions. End with :zap: action prompt.
    NEVER suggest action buttons (pause, check, fallback) — Scout cannot execute campaign operations from Slack.
    Each row includes campaign_id and publisher name + ID — surface both so the reader can open the exact right account.

24. FILL RATE — "fill rate", "low fill rate", "publishers not serving offers", "which publishers have low fill", "offer fill", "session fill", "checkout page fill rate", "confirmation page fill", "sessions not getting offers", "what publishers are underserving"
    → get_low_fill_publishers().
    Returns publishers on post-transaction placements (checkout confirmation, receipt, order confirmation, etc.) with fill rate below 15%.
    Fill rate = % of sessions that received at least one offer impression.
    Lead with total missed sessions and estimated revenue at risk. Then ranked publisher list. End with :zap: action note.

25. REVENUE OPPORTUNITIES — "revenue opportunities", "what are we missing", "what should we add", "net-new revenue", "supply gaps", "where should we add advertisers", "largest gaps", "uncaptured revenue", "what advertisers should we add to which publishers"
    → get_top_revenue_opportunities().
    Returns top cross-publisher advertiser gaps: high-performing advertisers (2+ publishers, >$10K/30d) not yet active in high-volume publishers (>100K sessions/30d).
    Lead with total estimated monthly revenue at risk. Then ranked list by est. revenue. End with :zap: action note.

26. PARTNER OFFER RECOMMENDATIONS — "offers for [partner]", "what should we add to [partner]", "recommend offers for [partner]", "what can we run on [partner]", "what's a good fit for [partner]", "pitch ideas for [partner]", "affiliate offers for [partner]", "new offers for [partner]"
    → get_offers_for_publisher(publisher_name=<partner>).
    Returns top affiliate network offers (not yet provisioned) scored by estimated RPM using real MS conversion benchmarks.
    DIFFERENT from get_supply_demand_gaps (which shows MS advertisers already on the platform) — this surfaces net-new affiliate inventory.
    Lead with partner name and candidate count. Ranked list. End with :zap: demand queue CTA.

27. RUN SCRAPER / REFRESH OFFERS — "refresh offers", "run scraper", "update offer inventory", "load benchmarks", "inventory is empty", "reload offers", "fetch latest offers", "scraper"
    → run_offer_scraper().
    Triggers an immediate affiliate network fetch (~2 min). Returns count of offers loaded.
    Use when Scout reports "offer inventory at 0" or benchmarks are stale.

28. PERKSWALL ENGAGEMENT — "perkswall engagement for [partner]", "perkswall stats for [partner]", "how is [partner]'s perkswall doing", "perkswall performance for [partner]", "perkswall clicks for [partner]", "what's the engagement on [partner]'s perkswall", "perkswall metrics"
    → get_perkswall_engagement(publisher_name=<partner>).
    Returns click-through rate, session engagement, and offer interaction breakdown for the partner's Perkswall placement.
    Lead with publisher name + total sessions. Highlight CTR and top-performing offer slots. Flag low-engagement placements.

29. PIPELINE HEALTH — "pipeline health", "how many offers went live", "what's stuck in the queue", "are we launching offers", "pipeline status", "offer queue status", "how many approved offers", "what's pending"
    → get_pipeline_health().
    Returns total approved offers, stale count (>7 days without Live status), oldest pending offers.
    Lead with total count and pass/fail signal. If stale offers exist: ends with action to mark Live or ping Gordon.
    Different from get_demand_queue_status (which is real-time queue for the current digest session).

30. USAGE REPORT — "scout usage", "usage report", "who uses scout", "usage stats", "how often is scout used", "who asks the most questions", "scout analytics"
    → get_usage_report(requesting_user_id=<caller's Slack user_id>).
    Pass the requesting user's Slack user_id — the tool enforces admin authorization check.
    Returns: queries per period (7d + 30d), top users, most-called tools, avg response time.
    If not admin: returns lock message.

DEFAULT: Unclear intent → Intent 17. Call get_top_opportunities(). A confident answer to a slightly wrong interpretation is better than asking "what do you mean?"
EXCEPTION: If the query clearly asks Scout to CHANGE something (pause, launch, adjust, create, modify, send) → apply the CAPABILITY BOUNDARY. Redirect to what you CAN show.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUDIENCE FIT + PROJECTION RULE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Before citing RPM or impression estimates for a publisher query:
1. State fit as an opinion: "AT&T Payment Confirmation is financial — TurboTax fits, expect above-category CVR."
2. Cite numbers with ~: "~22K impressions over 2 weeks."
3. If using category benchmark (no live CVR): say it once — "Category estimate — no live CVR yet."
4. One sharp insight on the biggest variable: "Tax season peaks through April — CVR is elevated right now."
No boilerplate caveat lists.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FOLLOW-UP SUGGESTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

After every non-brief response:
<<<SUGGESTIONS
["short query 1", "short query 2", "short query 3"]
SUGGESTIONS>>>

Rules:
- Always 2-3 suggestions. Max 30 chars each. Verb-first. Specific to what was shown.
- After arbitrage: "Build brief for [offer]", "Fallback for [offer]", "[category] gaps"
- After competitive landscape: "Run at $[N] CPA", "Fallback if [offer] caps", "[publisher] top offers"
- After offer research: "Build brief for [offer]", "Fallback if this goes dark"
- After top opportunities: "Build brief for [top offer]", "[category] gaps"
- After revenue query: "Top publishers for [offer]", "Compare to [category]"
- BAD: "Find more Finance offers for partner 6103" — too long, generic. GOOD: "Finance gaps on 6103"
- No suggestions after <<<BRIEF_JSON>>> — Approve/Reject buttons already exist.
- No double quotes inside suggestion strings.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BRIEF MODE (Intent 1 only)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Call draft_campaign_brief(advertiser=X).

COPY SOURCING (highest priority):
- platform_title non-empty → use verbatim as title. Do NOT rephrase or shorten.
- platform_cta_yes non-empty → use verbatim as cta.yes.
- platform_cta_no non-empty → use verbatim as cta.no.
- Generate from scratch only when fields are empty.

COPY RULES:
- Value Clarity > Cleverness: incentive obvious in ≤3 seconds.
- Subtle urgency only: "Today", "Start now", "Risk-free" OK. Countdown/false scarcity: never.
- Trust First: legit brand tone. No hype, no hidden conditions.
- Mobile-first: reads instantly on small screen.
- No em dashes (—) or en dashes (–). Use period, comma, or rewrite.

FIELDS:
- title (~50 chars, benefit-driven, post-transaction tone). If platform_title set: use it; add title_backup only if substantially different.
- description (150-170 chars EXACTLY): what user gets + hook + risk removal if applicable. No countdown language.
- short_desc (~50 chars, punchy, factual — for tiles/cards, must work without context).
- cta.yes (4-6 words, 25-char limit): desire/action. "Claim Free Reader", "Get Started Free".
- cta.no (4-6 words, 25-char limit): loss aversion — leaving something behind. "I'll miss out", "Skip my free reader". Not: "No Thanks", "Skip", "Not Now".
- targeting: one line with CVR data if available.
- bottom_line: one sentence on why this offer is worth running now.

2. Output ONLY this JSON — no other text:

<<<BRIEF_JSON
{
  "title": "~50 char benefit-driven headline",
  "title_backup": "A/B variant — only if substantially different",
  "description": "150-170 char offer description with hook + risk removal if applicable",
  "short_desc": "~50 char punchy condensed version for tiles/cards",
  "cta": {"yes": "Claim Free Reader", "no": "I'll miss out"},
  "targeting": "one-line with CVR data if available",
  "bottom_line": "one sentence on why this offer is worth running right now"
}
BRIEF_JSON>>>

3. After the JSON, if fallback_same_brand non-empty: "Backup plan: [advertiser] also on [network] — plug-and-play if this source hits cap."
   If only fallback_category_subs: "If this goes dark, next best in [category]: [name] ($X payout)."
   Skip if both empty.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CLICKHOUSE DATA DICTIONARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

── EVENT TABLES (partitioned by toYYYYMM(created_at)) ──────────────────────────────────────────

adpx_sdk_sessions [460M] — one row per SDK session (user visit to confirmation page)
  SORT: (user_id, created_at, id) | JOIN KEY: session_id (String UUID) | id (UInt64) = row key only
  MATERIALIZED: placement, country, state, city, zipcode, version, nexos, os, device
  COLS: user_id (UInt64, publisher), pub_user_id (loyalty member), is_offerwall, is_embedded, is_mou,
    subid, tags (Array), source, browser, fingerprint, parent_session_id, conversions (pre-computed)

adpx_impressions_details [582M] — one row per offer impression
  SORT: (pid, campaign_id, created_at, id) | pid (String) = publisher ID (NOT user_id)
  Join to users: i.pid = toString(u.id)
  COLS: session_id (join key), campaign_id, offer_id, position (carousel slot 1/2/3)

adpx_tracked_clicks [17M] — one row per carousel click
  SORT: (user_id, campaign_id, created_at, id)
  COLS: session_id (join key), campaign_id, offer_id, position (Int32, slot clicked),
    is_converted (Bool), pub_cost_cents (UInt64), os, device, browser, user_agent,
    fingerprint, is_offerwall, is_mou, is_embedded

adpx_conversionsdetails [1.7M] — one row per conversion
  SORT: (user_id, campaign_id, created_at, id)
  CRITICAL: revenue, payout are STRINGS — always cast: toFloat64OrNull(revenue)
  COLS: session_id (join key), campaign_id, offer_id, revenue (String), payout (String)
  DOWNSTREAM LAG: extend conversion window +14 days beyond session end date.

adpx_system_activity_logs [115K] — audit trail for dashboard changes
  COLS: entity, type, admin_id, old_data (JSON String), new_data (JSON String),
    user_type, user_role, created_at
  USE FOR: paused/resumed state, who changed what, when.

── CONFIGURATION TABLES (Airbyte sync) ──────────────────────────────────────────────────

from_airbyte_campaigns [4.75K] — master campaign table
  JOIN: toInt64(campaign_id) = c.id
  COLS: id, adv_name, title, status, categories, start_date, end_date,
    capping_config (JSON: {"month":{"budget":N}} — monthly revenue cap),
    pacing_config, schedule_days, geo_whitelist, geo_blacklist, platforms, os, browsers,
    is_offerwall_only, offerwall_enabled, perkswallet_enabled, network_id,
    internal_network_name (Impact offer ID), max_impressions, max_positive_cta,
    conversion_events, force_priority_till, open_to_marketplace, is_incent, is_rewarded,
    is_direct_sold, is_citrusad, is_rich_media, landing_url, useraction_url, useraction_cta,
    adv_description, offer_description, mini_description, terms_and_conditions,
    internal_notes, owner_id, advertiser_id, partner_id, deleted_at (NULL=active)

from_airbyte_publisher_campaigns [96K] — publisher×campaign pairings (operational)
  COLS: id, campaign_id, user_id (publisher), is_active (Bool — currently serving),
    payout (Int64 cents — publisher override; NULL=campaign default), priority (higher=more impressions),
    multiplier (Decimal), force_priority, max_impressions, max_positive_cta,
    capping_config, pacing_config, schedule_days, geo_whitelist, geo_blacklist,
    platforms, os, browsers, categories, goals, conversion_events, is_offerwall_only,
    stats_by_position (JSON — pre-computed stats by slot), useraction_cta, useraction_url,
    deleted_at, updated_at

from_airbyte_users [5.45K] — publisher registry
  COLS: id (UInt64 publisher_id), organization (name), is_test

from_airbyte_networks [177] — affiliate network registry (Impact, CJ, MaxBounty, FlexOffers, etc.)
  COLS: id, name, slug, postback_url, parameters, user_id

from_airbyte_placements [160] — named offer locations per publisher
  COLS: id, user_id (publisher_id), slug (e.g. "fuel_hub", "transaction_receipt"),
    display_name, is_default, is_auto_generated

from_airbyte_campaign_serving_groups [255] — groups sharing caps/schedules
  COLS: id, name, is_active, is_test, capping_config, pacing_config, schedule_days, exclude_group

from_airbyte_grouped_campaign_specs [1.33K] — maps campaigns to serving groups
  COLS: id, group_id, campaign_id

from_airbyte_placement_sequence_rules [200] — offer ordering within placement
  COLS: id, placement_id, sequence_rule_id, weight, is_active, user_id

from_airbyte_partner_categories [111] — publisher classification
  COLS: id, user_id, tier, approval, traffic_type, integration_type, custom_creatives

from_airbyte_publisher_delivery_channel_settings [33] — delivery channel config per publisher
  COLS: id, user_id, channel_name, weight, enabled, enable_force_priority

from_airbyte_publisher_nexos_settings [29] — Nexos feature flags per publisher
  COLS: id, user_id, is_enabled, enabled_percentage

from_airbyte_user_selected_perks [3.14K] — Perkswall perk selections (pre-conversion intent, NOT a conversion)
  COLS: id, user_id, campaign_id, session_id, pub_user_id, metadata (JSON), created_at

from_airbyte_custom_reports [742] — saved report definitions
  COLS: id, report_name, report_type, publisher_id, admin_id, metrics, attributes,
    range, offer_units, selected_campaigns, selected_publishers

from_airbyte_custom_report_runs [2.21K] — report execution history
from_airbyte_publisher_campaign_images [1.03K] — creative images
from_airbyte_perkswall_themes [1.43K] — Perkswall theme configs
from_airbyte_placement_themes [227] — placement theme configs

mv_adpx_campaigns — lightweight: id, internal_name, is_test. Use for campaign name resolution.
mv_adpx_users — lightweight: id, organization, is_test, parent_id. Use for publisher name resolution.

── CRITICAL QUERY RULES ──────────────────────────────────────────────────────────────────────────

JOIN KEYS:
  session_id (String): sessions ↔ impressions ↔ clicks ↔ conversions
  user_id (UInt64): sessions/clicks/conversions → from_airbyte_users
  pid (String): impressions → users via i.pid = toString(u.id)
  campaign_id: event tables (UInt64) → campaigns (Int64) via toInt64(campaign_id) = c.id

PREWHERE (always for primary sort key + partition):
  adpx_sdk_sessions:        PREWHERE user_id = X AND toYYYYMM(created_at) >= YYYYMM
  adpx_tracked_clicks:      PREWHERE user_id = X AND toYYYYMM(created_at) >= YYYYMM
  adpx_conversionsdetails:  PREWHERE user_id = X AND toYYYYMM(created_at) >= YYYYMM
  adpx_impressions_details: PREWHERE pid = 'X' AND toYYYYMM(created_at) >= YYYYMM

TYPE CASTING:
  revenue/payout → toFloat64OrNull(revenue)
  campaign_id → toInt64(campaign_id) = c.id
  pid → i.pid = toString(u.id)

DOWNSTREAM LAG: +14 days on conversion window beyond session end date.
CAPPING CONFIG: JSONExtractFloat(capping_config, 'month', 'budget')
TIMEZONE: UTC stored, report in 'America/Chicago'.

── TABLE LOOKUP GUIDE ────────────────────────────────────────────────────────────────────────────
Publisher performance        → sessions + impressions + clicks + conversions
Offer paused on publisher    → from_airbyte_publisher_campaigns (is_active)
When/who paused offer        → adpx_system_activity_logs (old_data/new_data diff)
Monthly budget cap           → from_airbyte_campaigns.capping_config or publisher_campaigns.capping_config
Carousel slot clicks         → adpx_tracked_clicks.position
Loyalty perk picks           → from_airbyte_user_selected_perks
Offer's affiliate network    → from_airbyte_campaigns.network_id → from_airbyte_networks.name
Publisher-specific payout    → from_airbyte_publisher_campaigns.payout (cents)
Publisher placements         → from_airbyte_placements WHERE user_id = X
Offerwall-only flag          → from_airbyte_campaigns.is_offerwall_only
Day-of-week schedule         → from_airbyte_publisher_campaigns.schedule_days (JSON)
Campaigns in serving group   → from_airbyte_campaign_serving_groups + from_airbyte_grouped_campaign_specs
"""


def get_supply_demand_gaps(
    publisher_name: str = "",
    advertiser_name: str = ""
) -> str:
    """
    Identify supply-demand gaps:
    - Publisher-first: which advertisers are performing elsewhere but NOT in this publisher?
    - Advertiser-first: which publishers is this advertiser NOT running in?
    Also surfaces dead weight: provisioned but zero impressions in 30 days.
    Provide either publisher_name OR advertiser_name, not both.
    """
    from concurrent.futures import ThreadPoolExecutor

    ch = _get_ch_client()

    if publisher_name and not advertiser_name:
        # --- PUBLISHER-FIRST MODE ---

        # Step 1: Resolve publisher
        pub_rows = ch.query(
            "SELECT id, organization FROM from_airbyte_users "
            "WHERE organization ILIKE %(pub)s LIMIT 5",
            parameters={"pub": f"%{publisher_name}%"}
        ).result_rows
        if not pub_rows:
            return f"No publisher found matching '{publisher_name}'."
        pub_id, pub_org = pub_rows[0][0], pub_rows[0][1]
        pub_pid = str(pub_id)

        # Step 2: Advertisers already active in this publisher
        existing_rows = ch.query(
            "SELECT DISTINCT c.adv_name "
            "FROM from_airbyte_publisher_campaigns pc "
            "JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id "
            "WHERE pc.user_id = %(pub_id)s "
            "  AND pc.is_active = true AND pc.deleted_at IS NULL AND c.deleted_at IS NULL",
            parameters={"pub_id": pub_id}
        ).result_rows
        existing = {r[0] for r in existing_rows}

        def q_gaps():
            # Pre-aggregate to avoid 582M row scan
            return ch.query("""
                WITH imp_agg AS (
                    SELECT campaign_id, count() AS impressions_30d
                    FROM adpx_impressions_details
                    WHERE created_at >= today() - 30
                      AND toYYYYMM(created_at) >= toYYYYMM(today() - 30)
                    GROUP BY campaign_id
                ),
                conv_agg AS (
                    SELECT campaign_id, sum(toFloat64OrNull(revenue)) AS revenue_30d
                    FROM adpx_conversionsdetails
                    WHERE created_at >= today() - 30
                      AND toYYYYMM(created_at) >= toYYYYMM(today() - 30)
                    GROUP BY campaign_id
                )
                SELECT
                    c.adv_name,
                    count(DISTINCT pc.user_id) AS pub_count,
                    sum(ia.impressions_30d) AS impressions_30d,
                    coalesce(sum(ca.revenue_30d), 0) AS revenue_30d,
                    round(coalesce(sum(ca.revenue_30d), 0) /
                          nullIf(sum(ia.impressions_30d), 0) * 1000, 2) AS rpm
                FROM from_airbyte_publisher_campaigns pc
                JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
                LEFT JOIN imp_agg ia ON ia.campaign_id = toUInt64(pc.campaign_id)
                LEFT JOIN conv_agg ca ON ca.campaign_id = toUInt64(pc.campaign_id)
                WHERE pc.is_active = true
                  AND pc.deleted_at IS NULL AND c.deleted_at IS NULL
                  AND pc.user_id != %(pub_id)s
                GROUP BY c.adv_name
                HAVING revenue_30d > 0 AND pub_count >= 2
                ORDER BY revenue_30d DESC
                LIMIT 20
            """, parameters={"pub_id": pub_id}).result_rows

        def q_dead_weight():
            return ch.query("""
                SELECT c.adv_name, min(pc.created_at) AS provisioned_since
                FROM from_airbyte_publisher_campaigns pc
                JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
                LEFT JOIN adpx_impressions_details i
                    ON i.campaign_id = toUInt64(pc.campaign_id)
                    AND i.pid = %(pub_pid)s
                    AND i.created_at >= today() - 30
                WHERE pc.user_id = %(pub_id)s
                  AND pc.is_active = true
                  AND pc.deleted_at IS NULL AND c.deleted_at IS NULL
                  AND i.campaign_id IS NULL
                GROUP BY c.adv_name
                LIMIT 10
            """, parameters={"pub_id": pub_id, "pub_pid": pub_pid}).result_rows

        def q_pub_sessions():
            rows = ch.query(
                "SELECT count() AS sessions FROM adpx_sdk_sessions "
                "WHERE user_id = %(pub_id)s AND created_at >= today() - 30 "
                "  AND toYYYYMM(created_at) >= toYYYYMM(today() - 30)",
                parameters={"pub_id": pub_id}
            ).result_rows
            return rows[0][0] if rows else 0

        gap_rows, dead_rows, sessions_30d = _run_parallel([q_gaps, q_dead_weight, q_pub_sessions])

        # Filter gaps to advertisers not already in this publisher
        gaps = [(adv, pub_count, imp, rev, rpm)
                for (adv, pub_count, imp, rev, rpm) in gap_rows
                if adv not in existing]

        daily_sessions = sessions_30d / 30 if sessions_30d else 0

        lines = [f"*{pub_org} — Supply Gap Analysis* (30-day data)\n"]

        if gaps:
            lines.append(":large_green_circle: *GAP OPPORTUNITIES* (performing on 2+ other publishers, not here)")
            total_est = 0
            for adv, pub_count, imp, rev, rpm in gaps[:10]:
                est_daily = round(daily_sessions * (rpm / 1000), 0) if rpm else 0
                total_est += est_daily
                lines.append(
                    f"• *{adv}* — ${rev:,.0f}/mo elsewhere · RPM ${rpm:.2f} across {pub_count} publishers"
                    + (f" → *est. ${est_daily:,.0f}/day at {pub_org} volume*" if est_daily > 0 else "")
                )
            if total_est > 0:
                lines.append(f"\n:zap: Top gaps combined: est. *${total_est:,.0f}/day* incremental revenue potential.")
        else:
            lines.append(":white_check_mark: No major gap opportunities found — coverage looks solid.")

        if dead_rows:
            lines.append("\n:yellow_circle: *DEAD WEIGHT* (provisioned here, zero impressions in 30 days)")
            for adv, since in dead_rows:
                since_str = since.strftime("%b %d") if hasattr(since, "strftime") else str(since)
                lines.append(f"• *{adv}* — active since {since_str}, 0 impressions. Remove or investigate.")

        return "\n".join(lines)

    elif advertiser_name and not publisher_name:
        # --- ADVERTISER-FIRST MODE ---

        # Publishers where advertiser IS running (active)
        active_rows = ch.query(
            "SELECT DISTINCT pc.user_id, u.organization "
            "FROM from_airbyte_publisher_campaigns pc "
            "JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id "
            "JOIN from_airbyte_users u ON pc.user_id = u.id "
            "WHERE c.adv_name ILIKE %(adv)s "
            "  AND pc.is_active = true AND pc.deleted_at IS NULL AND c.deleted_at IS NULL",
            parameters={"adv": f"%{advertiser_name}%"}
        ).result_rows
        active_pub_ids = {r[0] for r in active_rows}
        active_pub_names = [r[1] for r in active_rows]

        if not active_pub_ids:
            return f"No active publisher campaigns found for '{advertiser_name}'."

        # Publishers with significant traffic NOT running this advertiser
        missing_rows = ch.query("""
            SELECT u.id, u.organization,
                   count() AS sessions_30d
            FROM adpx_sdk_sessions s
            JOIN from_airbyte_users u ON s.user_id = u.id
            WHERE s.created_at >= today() - 30
              AND toYYYYMM(s.created_at) >= toYYYYMM(today() - 30)
              AND s.user_id NOT IN %(active_ids)s
            GROUP BY u.id, u.organization
            HAVING sessions_30d > 1000
            ORDER BY sessions_30d DESC
            LIMIT 20
        """, parameters={"active_ids": list(active_pub_ids)}).result_rows

        lines = [f"*{advertiser_name} — Publisher Gap Analysis* (30-day data)\n",
                 f":white_check_mark: Currently running in: {', '.join(active_pub_names[:8])}\n"]

        if missing_rows:
            lines.append(":large_green_circle: *NOT RUNNING IN* (publishers with >1K sessions/mo)")
            for pub_id, pub_org, sessions in missing_rows[:10]:
                lines.append(f"• *{pub_org}* — {sessions:,} sessions/mo")
            lines.append(
                f"\n:zap: {len(missing_rows)} publishers with meaningful traffic not running {advertiser_name}."
            )
        else:
            lines.append(":white_check_mark: Running in all major publishers — no significant gaps found.")

        return "\n".join(lines)

    else:
        return (
            "Please provide either a publisher_name (e.g. 'TextNow') or an advertiser_name (e.g. 'Scrambly'), "
            "not both and not neither."
        )


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
    {
        "name": "get_advertiser_revenue_projection",
        "description": (
            "Project gross revenue for a specific advertiser across ALL MS publisher partners for a target month. "
            "Uses last 30 days as the baseline (avg daily revenue × days in month). "
            "Checks campaign end dates (warns if campaigns end before month-end) and monthly budget caps. "
            "Returns: projected total revenue, breakdown by publisher, cap warnings, end-date warnings. "
            "Use for: 'projected revenue for Disney+ in April', 'how much will TurboTax generate this month', "
            "'gross revenue forecast for X across all partners', 'what's the April projection for X'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser_name": {"type": "string", "description": "Advertiser/offer name (partial match OK) e.g. 'Disney+', 'TurboTax'"},
                "month": {"type": "string", "description": "Target month e.g. 'April 2026' or '2026-04'. Defaults to next calendar month."},
            },
            "required": ["advertiser_name"],
        },
    },
    {
        "name": "get_publisher_health",
        "description": (
            "Full publisher health analysis: sessions, impressions, clicks, conversions, revenue, RPM, CTR, and CVR. "
            "Breaks down by placement (e.g. FuelHub vs TransactionReceipt) and OS (iOS/Android). "
            "Includes click position data (which carousel slot gets clicked). "
            "Use for: 'how is [publisher] doing', 'performance for [publisher]', "
            "'breakdown by placement', 'full funnel for [publisher]', "
            "'[publisher] placement performance', 'what placement drives most revenue on [publisher]'. "
            "This is the default tool for any publisher performance query — always call this before offer-level analysis."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK) e.g. '7-Eleven', 'AT&T'. Omit if using publisher_id."},
                "publisher_id": {"type": "integer", "description": "Numeric publisher ID. Use when user provides a partner number."},
                "days": {"type": "integer", "description": "Lookback window in days (default 14)"},
                "geo_state": {"type": "string", "description": "Optional: filter to a US state e.g. 'California', 'TX'"},
            },
        },
    },
    {
        "name": "get_campaign_status",
        "description": (
            "Check if an advertiser's campaigns are active or paused, and see recent changes from the audit log. "
            "Use for: 'is [offer] paused?', 'confirm [offer] is paused', 'is [offer] still live?', "
            "'what happened to [offer]?', 'when was [offer] paused?', 'confirm all [offer] campaigns are killed'. "
            "Returns current is_active status for each publisher campaign + last 30 days of change history."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser_name": {"type": "string", "description": "Advertiser/offer name (partial match OK) e.g. 'TurboTax', 'Hulu'"},
            },
            "required": ["advertiser_name"],
        },
    },
    {
        "name": "get_perkswall_engagement",
        "description": (
            "Perkswall offer selection analytics — which offers do loyalty members actually pick? "
            "Queries user_selected_perks to show offer selections, unique members engaged, and selection rates. "
            "Use for: 'which perks are [publisher] users picking?', 'Perkswall engagement for [publisher]', "
            "'what do loyalty members select on [publisher]?', 'top selected perks on [publisher]'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK)"},
                "publisher_id": {"type": "integer", "description": "Numeric publisher ID"},
                "days": {"type": "integer", "description": "Lookback window in days (default 30)"},
            },
        },
    },
    {
        "name": "get_supply_demand_gaps",
        "description": (
            "Identify supply-demand gaps: which advertisers are performing on other publishers but missing from a given publisher, "
            "or which publishers an advertiser is not running in. Also surfaces dead weight (provisioned but zero impressions in 30 days). "
            "Provide publisher_name OR advertiser_name, not both."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {
                    "type": "string",
                    "description": "Publisher to analyze (e.g. 'TextNow', 'Pinger'). Leave blank if using advertiser_name."
                },
                "advertiser_name": {
                    "type": "string",
                    "description": "Advertiser to analyze (e.g. 'Scrambly', 'BLD'). Leave blank if using publisher_name."
                }
            },
            "required": []
        },
    },
    {
        "name": "run_sql_query",
        "description": (
            "Execute an arbitrary ClickHouse SELECT query for questions not covered by other tools. "
            "Use the DATA DICTIONARY in your context to write correct SQL. "
            "Use for: any novel analytical question, multi-table joins, custom date ranges, "
            "cap/schedule config inspection, per-campaign payout lookups, "
            "serving group analysis, custom report recreation, or any query not covered by existing tools. "
            "Safety: SELECT-only, 500 row max by default. Always include a description of what you're querying. "
            "After getting results, present them clearly and add a sourcing callout: "
            "'> Queried: [description] — live ClickHouse'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Valid ClickHouse SQL SELECT statement. Use the DATA DICTIONARY for table/column names.",
                },
                "description": {
                    "type": "string",
                    "description": "One-line description of what this query retrieves, e.g. 'TurboTax campaign end dates and cap configs'",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Max rows to return (default 500, max 2000)",
                },
            },
            "required": ["sql", "description"],
        },
    },
    {
        "name": "get_ghost_campaigns",
        "description": (
            "Return the full list of ghost campaigns: active campaigns with high impressions + clicks "
            "but near-zero revenue (< $5 in last 7 days), older than 7 days. Includes per-campaign "
            "pixel/postback diagnosis. "
            "Use for: 'ghost brief', 'ghost campaigns', 'what campaigns are earning nothing', "
            "'campaigns with no revenue', 'show me the ghosts', 'zero revenue campaigns'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_low_fill_publishers",
        "description": (
            "Return publishers on post-transaction placements (checkout confirmation, order receipt, "
            "thank you pages, etc.) where fill rate is below 15% — meaning more than 85% of "
            "checkout sessions are receiving no offer. Includes missed session count and revenue-at-risk estimate. "
            "Use for: 'fill rate', 'low fill rate', 'which publishers have low fill', 'sessions not getting offers', "
            "'offer fill', 'checkout fill', 'confirmation page fill', 'publishers underserving'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_top_revenue_opportunities",
        "description": (
            "Return top cross-publisher revenue gap opportunities: high-performing advertisers "
            "(active on 2+ publishers, >$10K/30d revenue) that are NOT yet active in high-volume publishers "
            "(>100K sessions/30d). Ranked by estimated monthly revenue. Shows total revenue at risk. "
            "Use for: 'revenue opportunities', 'what are we missing', 'what should we add', 'net-new revenue', "
            "'supply gaps', 'where should we add advertisers', 'uncaptured revenue', 'largest gaps'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_scout_status",
        "description": (
            "Return a system health snapshot: benchmark freshness, offer inventory count, "
            "queue depth, ClickHouse connectivity, and any data quality warnings. "
            "Use for: '@Scout status', 'how are you doing?', 'is Scout healthy?', "
            "'benchmark freshness', 'system check'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "run_offer_scraper",
        "description": (
            "Trigger an immediate offer inventory refresh from affiliate networks "
            "(Impact, FlexOffers, MaxBounty). Takes ~2 minutes. Run when offer inventory "
            "is empty or stale. Updates offers_latest.json and posts the Scout Sniper digest. "
            "Use for: 'refresh offers', 'run scraper', 'update offer inventory', "
            "'load benchmarks', 'inventory is empty', 'reload offers', 'fetch latest offers'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_pipeline_health",
        "description": (
            "Report on the Scout offer approval pipeline: total approved offers, "
            "stale offers (>7 days without Live/Done status), and oldest pending. "
            "Use for: 'pipeline health', 'how many offers went live', 'what is stuck in the queue', "
            "'pipeline status', 'are we launching offers', 'offer queue status'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_usage_report",
        "description": (
            "Return Scout usage statistics: queries per period, top users, most-used tools, avg response time. "
            "Admin-only — requires SCOUT_ADMIN_USER_ID env var match. "
            "Use for: 'scout usage', 'usage report', 'who uses scout', 'usage stats', "
            "'how often is scout used', 'who asks the most questions'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "requesting_user_id": {
                    "type": "string",
                    "description": "Slack user ID of the person asking — for admin authorization check.",
                }
            },
        },
    },
    {
        "name": "get_offers_for_publisher",
        "description": (
            "Return top affiliate offers (from Impact, FlexOffers, MaxBounty inventory) that are "
            "a good fit for a specific publisher but not yet provisioned in their campaign set. "
            "Scored by estimated RPM using real MS conversion benchmarks. "
            "DIFFERENT from get_supply_demand_gaps — this surfaces net-new affiliate inventory, "
            "not advertisers already on the MS platform. "
            "Use for: 'offers for [partner]', 'what should we add to [partner]', "
            "'recommend offers for [partner]', 'what can we run on [partner]', "
            "'what's a good fit for [partner]', 'pitch ideas for [partner]', "
            "'affiliate offers for [partner]', 'new offers for [partner]'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {
                    "type": "string",
                    "description": "The publisher/partner name (e.g., 'TextNow', 'PCH', 'Metropolis').",
                }
            },
            "required": ["publisher_name"],
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
    if n == "maxbounty":
        return ""  # URL structure changed post-mrge acquisition — use Offer ID for manual lookup
    elif n == "impact":
        return "https://app.impact.com"
    elif n == "flexoffers" and offer_id:
        return f"https://www.flexoffers.com/affiliate-programs/{offer_id}/"
    return ""


_TRACKING_DOMAINS = {
    # Known affiliate tracking domains — URLs on these are real tracking links
    "impact.com", "sjv.io", "pxf.io", "bn5x.net", "ibfwsl.net",
    "maxbounty.com", "flexoffers.com", "jdoqocy.com", "tkqlhce.com",
    "launchingdeals.com", "adspostx.com", "pubtailer.com",
    "collectsavings.com", "referral.", "go.",
}

_CLICK_ID_PATTERNS = ("{click_id}", "{subid}", "subId", "clickid", "click_id", "aff_id")


def _validated_tracking_url(network: str, platform_url: str, scraper_url: str) -> str:
    """
    Return the best tracking URL for the brief, or a fallback message.
    Platform URL (from MS) is always preferred — it has {click_id} template.
    Scraper URL is only used if it looks like a real affiliate link, not the advertiser's site.
    """
    if platform_url:
        return platform_url

    if scraper_url:
        url_lower = scraper_url.lower()
        # Accept if it looks like a tracking link: known domain or click_id pattern
        if any(d in url_lower for d in _TRACKING_DOMAINS):
            return scraper_url
        if any(p.lower() in url_lower for p in _CLICK_ID_PATTERNS):
            return scraper_url
        # Reject — it's the advertiser's website, not an affiliate link
        log.info(f"Rejected non-tracking URL for {network} offer: {scraper_url[:60]}")

    return "Not available — pull from network portal"


# ── Brand image sourcing ──────────────────────────────────────────────────────
# Auto-sources logo + icon so brief creators don't have to hunt for assets.
#
# Priority chain (never fake — empty string beats a wrong/blurry image):
#   hero_url: MS CDN  →  App Store 512px  →  Google gstatic 256px  →  ""
#   icon_url: MS CDN  →  Google gstatic 256px  →  App Store 512px  →  ""
#
# Why these two sources:
#   - App Store (itunes.apple.com/search): 512x512 standardized square icon,
#     brand colors, no API key, most major advertisers have an iOS app.
#   - Google gstatic favicon: resolves from domain, no API key, 256px option,
#     best for the 24px icon slot where quality matters less.
#   - Clearbit logo API (logo.clearbit.com): defunct — DNS returns NXDOMAIN.
#     Clearbit autocomplete still works and gives us the domain for gstatic.

def _clearbit_domain(advertiser_name: str) -> str:
    """
    Resolve brand name → domain via Clearbit's free autocomplete API.
    The logo field in their response is null — use the domain to construct
    a Google gstatic favicon URL instead. Returns "" on failure.
    """
    query = urllib.parse.quote(advertiser_name.strip())
    url = f"https://autocomplete.clearbit.com/v1/companies/suggest?query={query}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Scout/1.0"})
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
        if data:
            return data[0].get("domain", "")
    except Exception as e:
        log.debug(f"Clearbit domain lookup failed for '{advertiser_name}': {e}")
    return ""


def _google_favicon(domain: str, size: int = 256) -> str:
    """
    Construct a Google gstatic favicon URL for a given domain.
    Use the direct t3.gstatic.com URL to avoid a redirect.
    Size 256 is the max offered and looks fine at Slack's 24px icon slot.
    """
    enc = urllib.parse.quote(domain)
    return f"https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&fallback_opts=TYPE,SIZE,URL&url=https://{enc}&size={size}"


def _app_store_icon(advertiser_name: str) -> str:
    """
    Search the iTunes Search API for a matching iOS app icon (512x512).
    Free, no API key. Matches on significant words in the app name.
    Returns "" if no strong match found.
    """
    query = urllib.parse.quote(advertiser_name.strip())
    url = f"https://itunes.apple.com/search?term={query}&entity=software&limit=5&country=us"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Scout/1.0"})
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
        results = data.get("results", [])
        if not results:
            return ""
        # Prefer results where the app name contains a significant word from the advertiser
        name_words = {w.lower() for w in advertiser_name.split() if len(w) > 3}
        for result in results:
            track = (result.get("trackName") or "").lower()
            if name_words and any(w in track for w in name_words):
                return result.get("artworkUrl512", "")
        return results[0].get("artworkUrl512", "")
    except Exception as e:
        log.debug(f"App Store icon lookup failed for '{advertiser_name}': {e}")
    return ""


def _validate_image_url(url: str) -> bool:
    """HEAD check — returns True only if the URL resolves to an image."""
    if not url or not url.startswith("http"):
        return False
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Scout/1.0"}, method="HEAD"
        )
        with urllib.request.urlopen(req, timeout=3) as r:
            return "image" in r.headers.get("Content-Type", "")
    except Exception:
        return False


_IMAGE_CACHE_PATH = pathlib.Path(__file__).parent / "data" / "image_cache.json"
_IMAGE_CACHE_TTL_DAYS = 7
_image_cache_mem: dict = {}  # in-memory layer to avoid file reads on every brief
_image_cache_loaded = False


def _load_image_cache() -> dict:
    global _image_cache_mem, _image_cache_loaded
    if not _image_cache_loaded:
        try:
            if _IMAGE_CACHE_PATH.exists():
                _image_cache_mem = json.loads(_IMAGE_CACHE_PATH.read_text())
        except Exception:
            _image_cache_mem = {}
        _image_cache_loaded = True
    return _image_cache_mem


def _save_image_cache(cache: dict) -> None:
    try:
        _IMAGE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _IMAGE_CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(cache, indent=2))
        import os; os.replace(tmp, _IMAGE_CACHE_PATH)
    except Exception as e:
        log.debug(f"image cache save failed: {e}")


def _cached_external_images(advertiser: str) -> dict | None:
    """Return cached {hero_url, icon_url} if fresh (< 7 days). None if stale or missing."""
    from datetime import datetime, timezone, timedelta
    cache = _load_image_cache()
    key = advertiser.lower().strip()
    entry = cache.get(key)
    if not entry:
        return None
    cached_at_str = entry.get("cached_at", "")
    try:
        cached_at = datetime.fromisoformat(cached_at_str).replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc) - cached_at).days >= _IMAGE_CACHE_TTL_DAYS:
            return None  # stale
    except Exception:
        return None
    return entry


def _store_image_cache(advertiser: str, hero_url: str, icon_url: str) -> None:
    from datetime import datetime, timezone
    cache = _load_image_cache()
    cache[advertiser.lower().strip()] = {
        "hero_url": hero_url,
        "icon_url": icon_url,
        "cached_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_image_cache(cache)


def _ms_cdn_image(campaign_id: str) -> str:
    """Query ClickHouse for the primary CDN creative for an MS campaign. Returns URL or ''."""
    if not campaign_id:
        return ""
    try:
        ch = _get_ch_client()
        rows = ch.query(
            """
            SELECT url FROM default.from_airbyte_publisher_campaign_images
            WHERE campaign_id = {cid:Int64}
              AND is_primary = 1
              AND deleted_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            parameters={"cid": int(campaign_id)},
        ).result_rows
        url = rows[0][0] if rows else ""
        if url and _validate_image_url(url):
            return url
    except Exception as e:
        log.debug(f"_ms_cdn_image: {e}")
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
    by_offer   = benchmarks.get("by_offer_impact_id", {})
    by_adv     = benchmarks.get("by_adv_name", {})
    by_cat_pt  = benchmarks.get("by_category_payout", {})
    by_pt      = benchmarks.get("by_payout_type", {})

    adv_key    = (o.get("advertiser") or "").lower().strip()
    category   = (o.get("category") or "").strip()
    payout_type = (o.get("_payout_type_norm") or "").lower().strip()

    if offer_id in by_offer:
        perf = by_offer[offer_id]
        perf_context = f"Real MS data: {perf['cvr_pct']}% CVR, ${perf['rpm']} RPM ({perf['impressions']:,} impressions)"
    elif adv_key in by_adv:
        perf = by_adv[adv_key]
        perf_context = f"Same advertiser benchmark: {perf['cvr_pct']}% CVR, ${perf['rpm']} RPM"
    elif (category, payout_type) in by_cat_pt:
        perf = by_cat_pt[(category, payout_type)]
        perf_context = f"{category} {payout_type} benchmark: {perf['avg_cvr_pct']}% avg CVR ({perf['sample_campaigns']} offers)"
    elif payout_type in by_pt:
        perf = by_pt[payout_type]
        perf_context = f"{payout_type} avg across all categories: {perf['avg_cvr_pct']}% avg CVR ({perf['sample_campaigns']} offers)"
    else:
        perf_context = "No MS performance data at any tier"

    icon_url = o.get("icon_url", "")
    hero_url = o.get("hero_url", "")

    score = _scout_score(o, benchmarks)

    # Pull platform copy + CDN images from MS ClickHouse.
    # The MS platform has approved title/CTA/tracking URL already — use them as source of truth.
    platform_title = platform_cta_yes = platform_cta_no = ""
    platform_landing_url = restrictions = platform_image = ""
    ms_id = None
    try:
        ch = _get_ch_client()
        p_rows = ch.query(
            """
            SELECT id, title, cta_yes, cta_no, landing_url, internal_notes
            FROM default.from_airbyte_campaigns
            WHERE lower(adv_name) LIKE lower(concat('%', {adv:String}, '%'))
              AND deleted_at IS NULL
              AND status != 'inactive'
            ORDER BY id DESC LIMIT 1
            """,
            parameters={"adv": o.get("advertiser", "")},
        ).result_rows
        if p_rows:
            ms_id, platform_title, platform_cta_yes, platform_cta_no, platform_landing_url, ms_notes = p_rows[0]
            platform_title = platform_title or ""
            platform_cta_yes = platform_cta_yes or ""
            platform_cta_no = platform_cta_no or ""
            platform_landing_url = platform_landing_url or ""
            restrictions = (ms_notes or "").strip()
            # CDN image: prefer publisher-specific creative, fall back to campaign-level
            img_rows = ch.query(
                "SELECT url FROM default.from_airbyte_publisher_campaign_images"
                " WHERE campaign_id = {cid:Int64} AND deleted_at IS NULL LIMIT 1",
                parameters={"cid": int(ms_id)},
            ).result_rows
            if img_rows:
                platform_image = img_rows[0][0] or ""
    except Exception as e:
        log.warning(f"draft_campaign_brief: platform lookup failed: {e}")

    # Image sourcing — see _clearbit_domain / _app_store_icon / _google_favicon for source rationale
    if platform_image:
        hero_url = icon_url = platform_image
    else:
        advertiser_name = o.get("advertiser", "")

        # hero_url: try MS CDN first (1,032 CDN images already in ClickHouse)
        cdn_hero = _ms_cdn_image(str(ms_id) if ms_id else "")
        if cdn_hero:
            hero_url = cdn_hero

        # Check external image cache before hitting iTunes / Clearbit / gstatic
        cached = _cached_external_images(advertiser_name)
        if cached:
            if not hero_url:
                hero_url = cached.get("hero_url", "")
            icon_url = cached.get("icon_url", "")
        else:
            app_icon = _app_store_icon(advertiser_name)
            domain   = _clearbit_domain(advertiser_name)
            favicon  = _google_favicon(domain) if domain else ""

            # hero: MS CDN (already set above) > App Store 512px > gstatic 256px > empty
            if not hero_url:
                if app_icon:
                    hero_url = app_icon
                elif favicon and _validate_image_url(favicon):
                    hero_url = favicon

            # icon: App Store > gstatic > empty
            # App Store preferred over gstatic because it's matched by brand name,
            # not domain — avoids wrong favicon when Clearbit autocomplete is off
            # (e.g., "Square" resolves to squarespace.com via Clearbit, but the
            # App Store correctly returns Square's payments app)
            if app_icon:
                icon_url = app_icon
            elif favicon and _validate_image_url(favicon):
                icon_url = favicon

            # Store results so next brief for same advertiser skips API calls
            _store_image_cache(advertiser_name, hero_url, icon_url)

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
        "tracking_url": _validated_tracking_url(net, platform_landing_url, o.get("tracking_url", "")),
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
        # Platform copy — use these directly; generate only when empty
        "platform_title": platform_title,
        "platform_cta_yes": platform_cta_yes,
        "platform_cta_no": platform_cta_no,
        "restrictions": restrictions,
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
        ch = _get_ch_client()

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
            pub_rows = ch.query(
                """
                SELECT id, organization, sdk_id
                FROM default.from_airbyte_users
                WHERE (lower(organization) LIKE lower(concat('%', {name:String}, '%'))
                    OR lower(username) LIKE lower(concat('%', {name:String}, '%')))
                  AND deletedAt IS NULL
                  AND sdk_id IS NOT NULL
                  AND sdk_id != ''
                ORDER BY createdAt ASC
                LIMIT 10
                """,
                parameters={"name": publisher_name},
            ).result_rows

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

        # Step 2 + Step 3 run in parallel — both depend only on pub_pid/pub_id_int
        # Step 2: weekly impression volume on this publisher (last 4 weeks avg)
        # Step 3: provisioned offers — what's assigned to this publisher account.
        # from_airbyte_publisher_campaigns is the source of truth for "what's set up."
        # Impressions tell us which of those are actively serving right now.
        _vol_sql = f"""
                SELECT
                    toStartOfWeek(i.created_at) AS week,
                    count() AS impressions
                FROM default.adpx_impressions_details i
                PREWHERE i.pid = '{pub_pid}'
                WHERE i.created_at >= today() - 28
                GROUP BY week
                ORDER BY week DESC
            """
        _prov_sql = f"""
                SELECT
                    pc.campaign_id,
                    c.adv_name,
                    pc.payout
                FROM default.from_airbyte_publisher_campaigns pc
                JOIN default.from_airbyte_campaigns c ON toInt64(pc.campaign_id) = toInt64(c.id)
                WHERE pc.user_id = {pub_id_int}
                  AND pc.deleted_at IS NULL
                  AND pc.is_active = true
                  AND c.deleted_at IS NULL
                ORDER BY pc.created_at DESC
                LIMIT 50
            """
        vol_rows, prov_rows = [r.result_rows for r in _run_parallel([
            lambda: ch.query(_vol_sql),
            lambda: ch.query(_prov_sql),
        ])]

        weekly_impressions = int(sum(r[1] for r in vol_rows) / max(len(vol_rows), 1)) if vol_rows else 0

        # Step 4: determine which provisioned campaigns have recent impressions (serving now).
        serving_map: dict = {}  # campaign_id → impression count
        rpm_map: dict = {}      # campaign_id → RPM (only for serving campaigns)

        if prov_rows:
            prov_ids = [str(r[0]) for r in prov_rows]
            id_list = ", ".join(f"'{cid}'" for cid in prov_ids)

            serving_rows = ch.query(f"""
                SELECT campaign_id, count() AS impressions
                FROM default.adpx_impressions_details
                PREWHERE pid = '{pub_pid}'
                WHERE created_at >= today() - 14
                  AND campaign_id IN ({id_list})
                GROUP BY campaign_id
            """).result_rows
            serving_map = {str(r[0]): int(r[1]) for r in serving_rows}

            if serving_map:
                sids = list(serving_map.keys())
                sids_str = ", ".join(f"'{cid}'" for cid in sids)
                rpm_rows = ch.query(f"""
                    SELECT
                        imp.campaign_id,
                        round(coalesce(cv.total_revenue, 0) / nullIf(imp.impressions, 0) * 1000, 2) AS rpm
                    FROM (
                        SELECT campaign_id, count() AS impressions
                        FROM default.adpx_impressions_details
                        PREWHERE pid = '{pub_pid}'
                        WHERE created_at >= today() - 14
                          AND campaign_id IN ({sids_str})
                        GROUP BY campaign_id
                    ) imp
                    LEFT JOIN (
                        SELECT cv.campaign_id,
                               sum(toFloat64OrNull(cv.revenue)) AS total_revenue
                        FROM default.adpx_conversionsdetails cv
                        WHERE cv.session_id IN (
                            SELECT session_id
                            FROM default.adpx_impressions_details
                            PREWHERE pid = '{pub_pid}'
                            WHERE created_at >= today() - 14
                              AND campaign_id IN ({sids_str})
                        )
                          AND toYYYYMM(cv.created_at) >= toYYYYMM(today() - 28)
                        GROUP BY cv.campaign_id
                    ) cv ON toInt64(imp.campaign_id) = toInt64(cv.campaign_id)
                """).result_rows
                rpm_map = {str(r[0]): float(r[1] or 0) for r in rpm_rows}

        # Build unified list — serving first (by RPM), then provisioned-only (by payout desc).
        competitors = []
        for row in prov_rows:
            campaign_id, adv_name, payout = row
            cid = str(campaign_id)
            impressions_2w = serving_map.get(cid, 0)
            is_serving = impressions_2w > 0
            rpm = rpm_map.get(cid, 0.0)
            competitors.append({
                "advertiser": adv_name,
                "campaign_id": cid,
                "provisioned": True,
                "is_serving": is_serving,
                "impressions_2w": impressions_2w,
                "rpm": rpm,
                "payout": float(payout) if payout else None,
            })
        competitors.sort(key=lambda x: (0 if x["is_serving"] else 1, -x["rpm"], -(x["payout"] or 0)))

        serving_count = sum(1 for c in competitors if c["is_serving"])
        result = {
            "publisher": pub_full_name,
            "publisher_id": pub_id_int,
            "publisher_pid": pub_pid,
            "weekly_impressions_avg": weekly_impressions,
            "projected_impressions_2w": weekly_impressions * weeks,
            "provisioned_campaigns": competitors,
            "provisioned_count": len(competitors),
            "serving_count": serving_count,
            # payout_scenario logic below expects active_competitors
            "active_competitors": [c for c in competitors if c["is_serving"]],
            "competitor_count": serving_count,
        }

        # Step 4: if offer + payout provided, compute rank scenarios
        if offer_name and hypothetical_payout is not None:
            benchmarks = _get_benchmarks()

            # Find the offer from snapshot — use it to get category/network context for scoring
            matched_offer = next(
                (o for o in _load_offers() if offer_name.lower() in (o.get("advertiser") or "").lower()),
                None,
            )
            current_payout = matched_offer.get("_payout_num") if matched_offer else None

            def _est_rpm(payout: float) -> float:
                synth = {**(matched_offer or {}), "_payout_num": payout}
                return round(_scout_score(synth, benchmarks), 2)

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
        ch = _get_ch_client()
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
            "notion_url": item.get("notion_url", ""),
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
        tmp = _LAUNCHED_OFFERS_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2))
        os.replace(tmp, _LAUNCHED_OFFERS_PATH)
    except Exception as e:
        log.warning(f"mark_offer_launched write failed: {e}")

    return {
        "status":      "launched",
        "advertiser":  key,
        "approved_by": entry.get("approved_by"),
        "thread_url":  entry.get("thread_url"),
        "notion_url":  entry.get("notion_url", ""),
        "payout":      entry.get("payout"),
        "network":     entry.get("network"),
    }


def get_advertiser_revenue_projection(
    advertiser_name: str,
    month: str = None,
) -> dict:
    """
    Project gross revenue for an advertiser across all MS publishers for a target month.

    Uses last 30 days as the baseline (avg daily revenue × days in month).
    Checks:
      - Campaign end dates (excludes/warns on campaigns ending before month)
      - Monthly budget caps from capping_config JSON
    Returns projected totals, publisher breakdown, cap warnings, end-date warnings.
    """
    import calendar as _cal
    import json as _json
    from datetime import date

    ch = _get_ch_client()
    today = date.today()

    # ── Parse target month ────────────────────────────────────────────────────
    target_year, target_month_num = today.year, today.month + 1
    if target_month_num > 12:
        target_month_num, target_year = 1, today.year + 1

    if month:
        import re as _re
        m = _re.search(r'(\d{4})[/-](\d{1,2})', month)
        if m:
            target_year, target_month_num = int(m.group(1)), int(m.group(2))
        else:
            month_map = {n.lower(): i for i, n in enumerate(_cal.month_name) if n}
            for name, num in month_map.items():
                if name in month.lower():
                    target_month_num = num
                    yr = _re.search(r'\d{4}', month)
                    if yr:
                        target_year = int(yr.group())
                    break

    days_in_month = _cal.monthrange(target_year, target_month_num)[1]
    month_start   = date(target_year, target_month_num, 1)
    month_end     = date(target_year, target_month_num, days_in_month)
    month_label   = f"{_cal.month_name[target_month_num]} {target_year}"

    # ── Steps 1 + 2 run in parallel — both depend only on advertiser_name ───────
    # Step 1: 30-day baseline — impressions + revenue per publisher
    # Step 2: Campaign end dates + monthly caps
    def _fetch_baseline():
        return ch.query(
            """
            SELECT
                cast(i.pid AS String)          AS publisher_pid,
                any(u.organization)            AS publisher_name,
                count()                        AS impressions_30d,
                count(DISTINCT i.session_id)   AS sessions_30d,
                coalesce(sum(toFloat64OrNull(cd.revenue)), 0) AS revenue_30d,
                coalesce(sum(toFloat64OrNull(cd.payout)),  0) AS payout_30d,
                count(cd.id)                   AS conversions_30d
            FROM adpx_impressions_details i
            JOIN from_airbyte_campaigns c
                ON i.campaign_id = cast(c.id AS UInt64)
            LEFT JOIN from_airbyte_users u
                ON i.pid = toString(u.id)
            LEFT JOIN adpx_conversionsdetails cd
                ON i.session_id = cd.session_id
                AND cd.campaign_id = i.campaign_id
                AND toYYYYMM(cd.created_at) >= toYYYYMM(today() - 44)
            WHERE c.adv_name ILIKE %(adv)s
              AND c.deleted_at IS NULL
              AND i.created_at >= today() - 30
              AND toYYYYMM(i.created_at) >= toYYYYMM(today() - 30)
            GROUP BY publisher_pid
            ORDER BY revenue_30d DESC
            LIMIT 30
            """,
            parameters={"adv": f"%{advertiser_name}%"},
        ).result_rows

    def _fetch_cap_data():
        return ch.query(
            """
            SELECT id, adv_name, end_date, capping_config
            FROM from_airbyte_campaigns
            WHERE adv_name ILIKE %(adv)s
              AND deleted_at IS NULL
              AND (end_date IS NULL OR end_date >= %(month_start)s)
            """,
            parameters={"adv": f"%{advertiser_name}%", "month_start": str(month_start)},
        ).result_rows

    baseline_rows = []
    cap_rows = []
    try:
        baseline_rows, cap_rows = _run_parallel([_fetch_baseline, _fetch_cap_data])
    except Exception as e:
        log.warning(f"get_advertiser_revenue_projection parallel fetch failed: {e}")
        try:
            baseline_rows = _fetch_baseline()
        except Exception as e2:
            log.warning(f"get_advertiser_revenue_projection baseline failed: {e2}")
            return {"error": str(e2), "advertiser": advertiser_name, "month": month_label}
        try:
            cap_rows = _fetch_cap_data()
        except Exception as e2:
            log.warning(f"get_advertiser_revenue_projection cap query failed: {e2}")

    if not baseline_rows:
        return {
            "advertiser": advertiser_name,
            "month": month_label,
            "error": f"No impression data for '{advertiser_name}' in the last 30 days. Check spelling or try a partial name.",
        }

    # ── Process cap/end-date results ──────────────────────────────────────────
    cap_warnings       = []
    end_date_warnings  = []
    monthly_cap_total  = None

    for row in cap_rows:
        cid, adv, end_dt, cap_cfg = row
        if end_dt and end_dt < month_end:
            end_date_warnings.append(
                f"Campaign {cid} ends {end_dt} — won't run full month"
            )
        if cap_cfg:
            try:
                cfg = _json.loads(cap_cfg) if isinstance(cap_cfg, str) else cap_cfg
                mb = (cfg.get("month") or {}).get("budget")
                if mb and float(mb) > 0:
                    cap_warnings.append(
                        f"Campaign {cid}: ${float(mb):,.0f} monthly budget cap"
                    )
                    monthly_cap_total = (monthly_cap_total or 0) + float(mb)
            except Exception:
                pass

    # ── Step 3: Projection ────────────────────────────────────────────────────
    total_revenue_30d     = sum(r[4] for r in baseline_rows)
    total_payout_30d      = sum(r[5] for r in baseline_rows)
    total_impressions_30d = sum(r[2] for r in baseline_rows)
    total_sessions_30d    = sum(r[3] for r in baseline_rows)
    total_conversions_30d = sum(r[6] for r in baseline_rows)

    uncapped_projected_revenue = round((total_revenue_30d / 30) * days_in_month, 2)
    uncapped_projected_payout  = round((total_payout_30d  / 30) * days_in_month, 2)

    cap_applied = bool(monthly_cap_total and uncapped_projected_revenue > monthly_cap_total)
    projected_revenue = monthly_cap_total if cap_applied else uncapped_projected_revenue
    projected_payout  = uncapped_projected_payout  # payout cap not modeled separately

    by_publisher = []
    for row in baseline_rows[:10]:
        pub_pid, pub_name, impr, sess, rev, pay, convs = row
        by_publisher.append({
            "publisher":          pub_name or f"Partner {pub_pid}",
            "publisher_id":       pub_pid,
            "impressions_30d":    impr,
            "revenue_30d":        round(rev, 2),
            "projected_revenue":  round((rev / 30) * days_in_month, 2),
            "conversions_30d":    convs,
            "share_pct":          round(rev / total_revenue_30d * 100, 1) if total_revenue_30d else 0,
        })

    return {
        "advertiser":                advertiser_name,
        "month":                     month_label,
        "days_in_month":             days_in_month,
        "publisher_count":           len(baseline_rows),
        # Actuals (30-day)
        "revenue_30d":               round(total_revenue_30d, 2),
        "payout_30d":                round(total_payout_30d, 2),
        "impressions_30d":           total_impressions_30d,
        "conversions_30d":           total_conversions_30d,
        # Projections
        "projected_revenue":         round(projected_revenue, 2),
        "uncapped_projected_revenue": uncapped_projected_revenue,
        "projected_payout":          round(projected_payout, 2),
        "projected_impressions":     int((total_impressions_30d / 30) * days_in_month),
        "cap_applied":               cap_applied,
        "monthly_cap_total":         monthly_cap_total,
        # Performance metrics (used for payout impact math)
        "avg_daily_revenue":         round(total_revenue_30d / 30, 2),
        "avg_rpm":                   round(total_revenue_30d / max(total_impressions_30d, 1) * 1000, 4),
        "avg_cvr":                   round(total_conversions_30d / max(total_sessions_30d, 1) * 100, 4),
        # Breakdown + warnings
        "by_publisher":              by_publisher,
        "cap_warnings":              cap_warnings,
        "end_date_warnings":         end_date_warnings,
        "methodology":               "30-day avg daily revenue × days in month. Cap applied where monthly_cap_total < uncapped projection.",
        "data_quality":              _data_quality_tier(30, total_sessions_30d),
    }


def get_publisher_health(
    publisher_name: str = None,
    publisher_id: int = None,
    days: int = 14,
    geo_state: str = None,
) -> dict:
    """
    Full publisher health analysis: sessions, impressions, clicks, conversions, revenue, RPM, CTR, CVR.
    Breaks down by placement and OS. Includes click position data.
    """
    try:
        ch = _get_ch_client()

        # ── Resolve publisher name → ID ───────────────────────────────────────
        pid = publisher_id
        pub_name = None
        if pid is None and publisher_name:
            rows = ch.query(
                "SELECT id, organization FROM from_airbyte_users WHERE organization ILIKE {name: String} LIMIT 10",
                parameters={"name": f"%{publisher_name}%"},
            ).result_rows
            if not rows:
                return {"error": f"No publisher found matching '{publisher_name}'"}
            # Disambiguate: pick the candidate with the most recent sessions.
            # Without this, accounts with the same name return in arbitrary order and
            # the wrong (inactive) account gets picked — e.g. TextNow 2527 vs 1952.
            if len(rows) > 1:
                candidate_ids = [str(int(r[0])) for r in rows]
                id_csv = ", ".join(candidate_ids)
                vol_rows = ch.query(
                    f"""
                    SELECT user_id, count() AS sessions
                    FROM adpx_sdk_sessions
                    PREWHERE user_id IN ({id_csv})
                        AND toYYYYMM(created_at) >= toYYYYMM(today() - 7)
                    WHERE created_at >= today() - 7
                    GROUP BY user_id
                    ORDER BY sessions DESC
                    LIMIT 1
                    """
                ).result_rows
                best_id = int(vol_rows[0][0]) if vol_rows else int(rows[0][0])
                best_row = next((r for r in rows if int(r[0]) == best_id), rows[0])
            else:
                best_row = rows[0]
            pid = int(best_row[0])
            pub_name = best_row[1]
        elif pid is not None:
            rows = ch.query(
                "SELECT id, organization FROM from_airbyte_users WHERE id = {pid: UInt64} LIMIT 1",
                parameters={"pid": int(pid)},
            ).result_rows
            pub_name = rows[0][1] if rows else f"Partner {pid}"

        if pid is None:
            return {"error": "Must provide publisher_name or publisher_id"}

        # ── Partition filter ──────────────────────────────────────────────────
        from datetime import date
        today = date.today()
        # partition for sessions (go back days + a little buffer)
        partition = int(today.strftime("%Y%m")) - (1 if today.day <= days else 0)
        extended_partition = partition - 1  # extra month for downstream lag

        # ── Queries 1–3 + placement names run in parallel (all depend only on pid/partition) ──
        state_clause = "AND state ILIKE {geo_state: String}" if geo_state else ""
        state_clause_inner = f"AND state ILIKE {{geo_state: String}}" if geo_state else ""

        q1 = f"""
        SELECT placement, os, count() AS sessions
        FROM adpx_sdk_sessions
        PREWHERE user_id = {{pid: UInt64}}
            AND toYYYYMM(created_at) >= {{partition: UInt32}}
        WHERE created_at >= today() - {{days: UInt32}}
          {state_clause}
        GROUP BY placement, os
        ORDER BY sessions DESC
        """
        params1 = {"pid": int(pid), "partition": partition, "days": days}
        if geo_state:
            params1["geo_state"] = f"%{geo_state}%"

        # ── Query 2: ad metrics by placement ─────────────────────────────────
        # Scan impressions LEFT (filtered by pid+date), sessions RIGHT (hash table).
        # Putting impressions on the right OOMs on large publishers (FillingRightJoinSide).
        q2 = f"""
        SELECT
            s.placement,
            count(DISTINCT i.id)                                    AS impressions,
            count(DISTINCT cd.id)                                   AS conversions,
            coalesce(sum(toFloat64OrNull(cd.revenue)), 0)           AS revenue,
            coalesce(sum(toFloat64OrNull(cd.payout)), 0)            AS payout
        FROM (
            SELECT session_id, id, campaign_id
            FROM adpx_impressions_details
            PREWHERE pid = {{pid_str: String}}
                AND toYYYYMM(created_at) >= {{partition: UInt32}}
            WHERE created_at >= today() - {{days: UInt32}}
        ) i
        JOIN (
            SELECT session_id, placement
            FROM adpx_sdk_sessions
            PREWHERE user_id = {{pid: UInt64}}
                AND toYYYYMM(created_at) >= {{partition: UInt32}}
            WHERE created_at >= today() - {{days: UInt32}}
              {state_clause_inner}
        ) s ON s.session_id = i.session_id
        LEFT JOIN (
            SELECT session_id, id, campaign_id, revenue, payout
            FROM adpx_conversionsdetails
            PREWHERE user_id = {{pid: UInt64}}
                AND toYYYYMM(created_at) >= {{extended_partition: UInt32}}
        ) cd ON cd.session_id = i.session_id AND cd.campaign_id = i.campaign_id
        GROUP BY s.placement
        """
        params2 = {"pid": int(pid), "pid_str": str(pid), "partition": partition, "extended_partition": extended_partition, "days": days}
        if geo_state:
            params2["geo_state"] = f"%{geo_state}%"

        # ── Query 3: click metrics by placement ───────────────────────────────
        # Scan clicks LEFT (filtered by user_id+date), sessions RIGHT (hash table).
        q3 = f"""
        SELECT
            s.placement,
            count(tc.id)                            AS clicks,
            countIf(tc.is_converted)               AS converted_clicks,
            round(avg(tc.position), 1)             AS avg_position
        FROM (
            SELECT session_id, id, is_converted, position
            FROM adpx_tracked_clicks
            PREWHERE user_id = {{pid: UInt64}}
                AND toYYYYMM(created_at) >= {{partition: UInt32}}
            WHERE created_at >= today() - {{days: UInt32}}
        ) tc
        JOIN (
            SELECT session_id, placement
            FROM adpx_sdk_sessions
            PREWHERE user_id = {{pid: UInt64}}
                AND toYYYYMM(created_at) >= {{partition: UInt32}}
            WHERE created_at >= today() - {{days: UInt32}}
              {state_clause_inner}
        ) s ON s.session_id = tc.session_id
        GROUP BY s.placement
        """
        params3 = {"pid": int(pid), "partition": partition, "days": days}
        if geo_state:
            params3["geo_state"] = f"%{geo_state}%"

        def _fetch_placement_names():
            try:
                rows = ch.query(
                    "SELECT slug, display_name FROM from_airbyte_placements WHERE user_id = {pid: Int64}",
                    parameters={"pid": int(pid)},
                ).result_rows
                return {slug: dn for slug, dn in rows if dn}
            except Exception:
                return {}  # non-fatal — use slugs as-is if table unavailable

        # ── Fetch placement display names ─────────────────────────────────────
        _p1, _p2, _p3 = params1, params2, params3
        _q1, _q2, _q3 = q1, q2, q3
        placement_names, q1_rows, q2_rows, q3_rows = _run_parallel([
            _fetch_placement_names,
            lambda: ch.query(_q1, parameters=_p1).result_rows,
            lambda: ch.query(_q2, parameters=_p2).result_rows,
            lambda: ch.query(_q3, parameters=_p3).result_rows,
        ])

        # ── Combine results ───────────────────────────────────────────────────
        # Build placement-keyed dicts
        sess_by_placement: dict = {}
        os_by_placement: dict = {}
        for placement, os_val, sess in q1_rows:
            p = placement or "unknown"
            sess_by_placement[p] = sess_by_placement.get(p, 0) + sess
            os_by_placement.setdefault(p, {})
            os_by_placement[p][os_val or "unknown"] = os_by_placement[p].get(os_val or "unknown", 0) + sess

        ad_metrics: dict = {}
        for placement, impressions, conversions, revenue, payout in q2_rows:
            p = placement or "unknown"
            ad_metrics[p] = {
                "impressions": int(impressions),
                "conversions": int(conversions),
                "revenue": float(revenue),
                "payout": float(payout),
            }

        click_metrics: dict = {}
        for placement, clicks, converted_clicks, avg_position in q3_rows:
            p = placement or "unknown"
            click_metrics[p] = {
                "clicks": int(clicks),
                "converted_clicks": int(converted_clicks),
                "avg_position": float(avg_position) if avg_position else 0.0,
            }

        # Aggregate OS split across all placements
        os_totals: dict = {}
        for p_os in os_by_placement.values():
            for os_val, cnt in p_os.items():
                os_totals[os_val] = os_totals.get(os_val, 0) + cnt
        total_sess_all = sum(os_totals.values()) or 1
        os_split = sorted(
            [{"os": k, "sessions": v, "share_pct": round(v / total_sess_all * 100, 1)} for k, v in os_totals.items()],
            key=lambda x: -x["sessions"],
        )

        # Build per-placement breakdown
        all_placements = set(sess_by_placement.keys()) | set(ad_metrics.keys()) | set(click_metrics.keys())
        total_revenue = sum(ad_metrics.get(p, {}).get("revenue", 0) for p in all_placements)
        total_impressions = sum(ad_metrics.get(p, {}).get("impressions", 0) for p in all_placements)
        publisher_avg_rpm = (total_revenue / total_impressions * 1000) if total_impressions else 0

        by_placement = []
        for p in all_placements:
            sess = sess_by_placement.get(p, 0)
            ad = ad_metrics.get(p, {"impressions": 0, "conversions": 0, "revenue": 0.0, "payout": 0.0})
            cl = click_metrics.get(p, {"clicks": 0, "converted_clicks": 0, "avg_position": 0.0})
            impr = ad["impressions"]
            rev = ad["revenue"]
            rpm = round(rev / impr * 1000, 2) if impr else 0.0
            ctr = round(cl["clicks"] / impr * 100, 2) if impr else 0.0
            cvr = round(ad["conversions"] / sess * 100, 2) if sess else 0.0

            anomaly = None
            if publisher_avg_rpm > 0 and rpm > 0 and rpm > publisher_avg_rpm * 5:
                ratio = round(rpm / publisher_avg_rpm, 1)
                anomaly = f"{ratio}x higher RPM than publisher avg"

            display_name = placement_names.get(p, p)
            by_placement.append({
                "placement":       display_name,
                "placement_slug":  p,
                "sessions":        sess,
                "impressions":     impr,
                "clicks":          cl["clicks"],
                "conversions":     ad["conversions"],
                "revenue":         round(rev, 2),
                "rpm":             rpm,
                "ctr_pct":         ctr,
                "cvr_pct":         cvr,
                "avg_position":    cl["avg_position"],
                "anomaly":         anomaly,
            })

        by_placement.sort(key=lambda x: -x["revenue"])

        # Overall rollup
        total_sessions = sum(sess_by_placement.values())
        total_clicks = sum(cl["clicks"] for cl in click_metrics.values())
        total_conversions = sum(ad_metrics.get(p, {}).get("conversions", 0) for p in all_placements)
        total_payout = sum(ad_metrics.get(p, {}).get("payout", 0) for p in all_placements)
        all_positions = [cl["avg_position"] for cl in click_metrics.values() if cl["avg_position"] > 0]
        avg_click_position = round(sum(all_positions) / len(all_positions), 1) if all_positions else 0.0

        overall_rpm = round(total_revenue / total_impressions * 1000, 2) if total_impressions else 0.0
        overall_ctr = round(total_clicks / total_impressions * 100, 2) if total_impressions else 0.0
        overall_cvr = round(total_conversions / total_sessions * 100, 2) if total_sessions else 0.0

        # Top placement note
        top_placement_note = ""
        if len(by_placement) >= 2:
            top = by_placement[0]
            bottom = by_placement[-1]
            if bottom["rpm"] > 0 and top["rpm"] > 0:
                ratio = round(top["rpm"] / bottom["rpm"], 1)
                top_placement_note = (
                    f"{top['placement']} generates {ratio}x RPM of {bottom['placement']}"
                )

        return {
            "publisher":     pub_name or f"Partner {pid}",
            "publisher_id":  int(pid),
            "days":          days,
            "geo_state":     geo_state or None,
            "overall": {
                "sessions":           total_sessions,
                "impressions":        total_impressions,
                "clicks":             total_clicks,
                "conversions":        total_conversions,
                "revenue":            round(total_revenue, 2),
                "payout":             round(total_payout, 2),
                "rpm":                overall_rpm,
                "ctr_pct":            overall_ctr,
                "cvr_pct":            overall_cvr,
                "avg_click_position": avg_click_position,
            },
            "by_placement":          by_placement,
            "os_split":              os_split,
            "top_placements_note":   top_placement_note,
            "data_quality":          _data_quality_tier(days, total_sessions),
        }
    except Exception as e:
        log.exception("get_publisher_health failed")
        return {"error": str(e), "publisher_name": publisher_name, "publisher_id": publisher_id}


def get_campaign_status(advertiser_name: str) -> dict:
    """
    Check if an advertiser's campaigns are active/paused and show recent changes from audit log.
    """
    try:
        import json as _json
        ch = _get_ch_client()

        # ── Queries 1 + 2 run in parallel — both depend only on advertiser_name ─
        # Query 1: current status from publisher_campaigns
        # Query 2: recent audit log entries
        def _fetch_q1():
            return ch.query(
                """
                SELECT
                    pc.id, pc.campaign_id, pc.is_active,
                    c.adv_name,
                    pc.updated_at, pc.deleted_at
                FROM from_airbyte_publisher_campaigns pc
                JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
                WHERE c.adv_name ILIKE {adv: String}
                  AND c.deleted_at IS NULL
                ORDER BY pc.updated_at DESC
                LIMIT 50
                """,
                parameters={"adv": f"%{advertiser_name}%"},
            ).result_rows

        def _fetch_q2():
            return ch.query(
                """
                SELECT
                    entity, type, old_data, new_data, created_at, user_type, user_role
                FROM adpx_system_activity_logs
                WHERE (lower(new_data) LIKE lower({adv_pat: String}) OR lower(old_data) LIKE lower({adv_pat: String}))
                  AND created_at >= today() - 30
                ORDER BY created_at DESC
                LIMIT 20
                """,
                parameters={"adv_pat": f"%{advertiser_name}%"},
            ).result_rows

        q1_rows = []
        q2_rows = []
        try:
            q1_rows, q2_rows = _run_parallel([_fetch_q1, _fetch_q2])
        except Exception as e:
            log.warning(f"get_campaign_status parallel fetch failed: {e}")
            try:
                q1_rows = _fetch_q1()
            except Exception as e2:
                log.warning(f"get_campaign_status q1 failed: {e2}")
            try:
                q2_rows = _fetch_q2()
            except Exception as e2:
                log.warning(f"get_campaign_status q2 failed: {e2}")

        # ── Build campaigns list ──────────────────────────────────────────────
        campaigns = []
        for row in q1_rows:
            pc_id, camp_id, is_active, adv_name, updated_at, deleted_at = row
            campaigns.append({
                "publisher_campaign_id": int(pc_id) if pc_id else None,
                "campaign_id":           int(camp_id) if camp_id else None,
                "adv_name":              adv_name or advertiser_name,
                "is_active":             bool(is_active),
                "last_updated":          str(updated_at) if updated_at else None,
                "is_deleted":            deleted_at is not None,
            })

        active_count = sum(1 for c in campaigns if c["is_active"] and not c["is_deleted"])
        paused_count = sum(1 for c in campaigns if not c["is_active"] and not c["is_deleted"])

        # ── Parse audit log ───────────────────────────────────────────────────
        recent_changes = []
        for row in q2_rows:
            entity, change_type_raw, old_data_str, new_data_str, created_at, user_type, user_role = row
            try:
                old_data = _json.loads(old_data_str) if old_data_str else {}
            except Exception:
                old_data = {}
            try:
                new_data = _json.loads(new_data_str) if new_data_str else {}
            except Exception:
                new_data = {}

            # Determine change type
            old_active = old_data.get("is_active")
            new_active = new_data.get("is_active")
            if old_active is True and new_active is False:
                c_type = "paused"
                summary = f"Campaign paused by {user_role or user_type or 'system'}"
            elif old_active is False and new_active is True:
                c_type = "resumed"
                summary = f"Campaign resumed by {user_role or user_type or 'system'}"
            elif "capping_config" in new_data or "capping_config" in old_data:
                c_type = "budget_changed"
                summary = "Budget/cap configuration changed"
            else:
                c_type = "other"
                summary = f"{change_type_raw or 'Change'} by {user_role or user_type or 'system'}"

            recent_changes.append({
                "entity":      entity or "",
                "change_type": c_type,
                "timestamp":   str(created_at) if created_at else None,
                "summary":     summary,
            })

        # ── Status summary ────────────────────────────────────────────────────
        last_change_str = ""
        if recent_changes:
            last = recent_changes[0]
            last_change_str = f" Last change: {last['change_type']} ({last['timestamp'][:10] if last['timestamp'] else 'unknown'})."
        status_summary = f"{active_count} active, {paused_count} paused.{last_change_str}"

        return {
            "advertiser":     advertiser_name,
            "campaign_count": len(campaigns),
            "active_count":   active_count,
            "paused_count":   paused_count,
            "campaigns":      campaigns,
            "recent_changes": recent_changes,
            "status_summary": status_summary,
        }
    except Exception as e:
        log.exception("get_campaign_status failed")
        return {"error": str(e), "advertiser": advertiser_name}


def get_perkswall_engagement(
    publisher_name: str = None,
    publisher_id: int = None,
    days: int = 30,
) -> dict:
    """
    Perkswall offer selection analytics — which offers do loyalty members actually pick?
    """
    try:
        ch = _get_ch_client()

        # ── Resolve publisher name → ID ───────────────────────────────────────
        pid = publisher_id
        pub_name = None
        if pid is None and publisher_name:
            rows = ch.query(
                "SELECT id, organization FROM from_airbyte_users WHERE organization ILIKE {name: String} LIMIT 10",
                parameters={"name": f"%{publisher_name}%"},
            ).result_rows
            if not rows:
                return {"error": f"No publisher found matching '{publisher_name}'"}
            # Disambiguate: pick the candidate with the most recent sessions.
            if len(rows) > 1:
                candidate_ids = [str(int(r[0])) for r in rows]
                id_csv = ", ".join(candidate_ids)
                vol_rows = ch.query(
                    f"""
                    SELECT user_id, count() AS sessions
                    FROM adpx_sdk_sessions
                    PREWHERE user_id IN ({id_csv})
                        AND toYYYYMM(created_at) >= toYYYYMM(today() - 7)
                    WHERE created_at >= today() - 7
                    GROUP BY user_id
                    ORDER BY sessions DESC
                    LIMIT 1
                    """
                ).result_rows
                best_id = int(vol_rows[0][0]) if vol_rows else int(rows[0][0])
                best_row = next((r for r in rows if int(r[0]) == best_id), rows[0])
            else:
                best_row = rows[0]
            pid = int(best_row[0])
            pub_name = best_row[1]
        elif pid is not None:
            rows = ch.query(
                "SELECT id, organization FROM from_airbyte_users WHERE id = {pid: UInt64} LIMIT 1",
                parameters={"pid": int(pid)},
            ).result_rows
            pub_name = rows[0][1] if rows else f"Partner {pid}"

        if pid is None:
            return {"error": "Must provide publisher_name or publisher_id"}

        # ── Partition filter ──────────────────────────────────────────────────
        from datetime import date
        today = date.today()
        partition = int(today.strftime("%Y%m")) - (1 if today.day <= days else 0)

        # ── Query: perk selections by offer ───────────────────────────────────
        sel_rows = ch.query(
            """
            SELECT
                sp.campaign_id,
                any(c.adv_name)                        AS offer_name,
                count()                                AS selections,
                count(DISTINCT sp.pub_user_id)         AS unique_members,
                count(DISTINCT sp.session_id)          AS sessions_with_selection
            FROM from_airbyte_user_selected_perks sp
            JOIN from_airbyte_campaigns c ON toInt64(sp.campaign_id) = c.id
            WHERE sp.user_id = {pid: UInt64}
              AND sp.created_at >= today() - {days: UInt32}
            GROUP BY sp.campaign_id
            ORDER BY selections DESC
            LIMIT 20
            """,
            parameters={"pid": int(pid), "days": days},
        ).result_rows

        # ── Total sessions for selection rate ─────────────────────────────────
        sess_row = ch.query(
            """
            SELECT count() AS total_sessions, count(DISTINCT session_id) AS unique_sessions
            FROM adpx_sdk_sessions
            PREWHERE user_id = {pid: UInt64}
                AND toYYYYMM(created_at) >= {partition: UInt32}
            WHERE created_at >= today() - {days: UInt32}
            """,
            parameters={"pid": int(pid), "partition": partition, "days": days},
        ).result_rows
        total_sessions = int(sess_row[0][0]) if sess_row else 0
        total_sessions_safe = total_sessions or 1

        # ── Build offer breakdown ─────────────────────────────────────────────
        by_offer = []
        total_selections = 0
        all_unique_members = set()
        for campaign_id, offer_name, selections, unique_members, sessions_with_selection in sel_rows:
            total_selections += int(selections)
            by_offer.append({
                "offer":                    offer_name or f"Campaign {campaign_id}",
                "campaign_id":              int(campaign_id) if campaign_id else None,
                "selections":               int(selections),
                "unique_members":           int(unique_members),
                "sessions_with_selection":  int(sessions_with_selection),
                "selection_rate_pct":       round(int(selections) / total_sessions_safe * 100, 2),
            })

        total_unique_members = sum(o["unique_members"] for o in by_offer)
        selection_rate_pct = round(total_selections / total_sessions_safe * 100, 2)

        # ── Insight ───────────────────────────────────────────────────────────
        insight = ""
        if by_offer:
            top = by_offer[0]
            insight = (
                f"{top['offer']} selected by {top['unique_members']:,} unique members "
                f"({top['selection_rate_pct']}% of sessions)"
            )

        return {
            "publisher":               pub_name or f"Partner {pid}",
            "publisher_id":            int(pid),
            "days":                    days,
            "total_sessions":          total_sessions,
            "total_selections":        total_selections,
            "selection_rate_pct":      selection_rate_pct,
            "unique_members_engaged":  total_unique_members,
            "by_offer":                by_offer,
            "insight":                 insight,
        }
    except Exception as e:
        log.exception("get_perkswall_engagement failed")
        return {"error": str(e), "publisher_name": publisher_name, "publisher_id": publisher_id}


def run_sql_query(sql: str, description: str = "", max_rows: int = 500) -> dict:
    """
    Execute an arbitrary SELECT query against ClickHouse.
    Safety: SELECT-only, 500 row default max, 30s timeout.
    Returns structured results for Claude to format.
    """
    import re as _re

    # Safety guard — SELECT only
    sql_stripped = sql.strip()
    first_word = sql_stripped.split()[0].upper() if sql_stripped else ""
    if first_word not in ("SELECT", "WITH"):
        return {
            "error": "Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, etc.",
            "sql": sql_stripped,
        }

    # Inject LIMIT if not present
    sql_upper = sql_stripped.upper()
    has_limit = bool(_re.search(r'\bLIMIT\b', sql_upper))
    if not has_limit:
        sql_stripped = sql_stripped.rstrip(";") + f"\nLIMIT {max_rows}"

    try:
        ch = _get_ch_client()
        result = ch.query(sql_stripped, settings={"max_execution_time": 30})
        rows = result.result_rows
        try:
            col_names = list(result.column_names)
        except (AttributeError, TypeError):
            col_names = []

        truncated = len(rows) >= max_rows

        # Sanitize values — ClickHouse returns date/datetime as Python objects
        def _sanitize(v):
            if isinstance(v, (_dt_mod.date, _dt_mod.datetime)):
                return str(v)
            if isinstance(v, (list, tuple)):
                return [_sanitize(x) for x in v]
            return v

        # Convert rows to list of dicts for readability
        if col_names:
            rows_as_dicts = [
                {k: _sanitize(v) for k, v in zip(col_names, row)}
                for row in rows[:max_rows]
            ]
        else:
            rows_as_dicts = [[_sanitize(v) for v in row] for row in rows[:max_rows]]

        return {
            "description": description,
            "sql_run": sql_stripped,
            "row_count": len(rows_as_dicts),
            "truncated": truncated,
            "truncation_note": f"Results limited to {max_rows} rows. Add LIMIT to your query to control this." if truncated else None,
            "columns": col_names,
            "rows": rows_as_dicts,
            "data_quality": {
                "tier": "free_form",
                "note": f"Free-form query — {len(rows_as_dicts)} rows. Verify column semantics before acting.",
            },
        }
    except Exception as e:
        err = str(e)
        return {
            "error": err,
            "sql_run": sql_stripped,
            "description": description,
            "hint": "Check table/column names against the DATA DICTIONARY. Common issues: wrong join type (pid vs user_id), missing PREWHERE, type mismatch (toFloat64OrNull for revenue).",
        }


def get_ghost_campaigns() -> str:
    """
    Return full ghost campaign list with per-campaign diagnosis.
    Ghost = actively serving impressions + clicks but generating near-zero revenue
    (< $5 in last 7 days), older than 7 days (not a new launch).
    """
    ch = _get_ch_client()
    sql = """
WITH imp_agg AS (
    SELECT campaign_id, count() AS impressions_7d, min(created_at)::Date AS first_impression_date
    FROM adpx_impressions_details
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
    WHERE created_at >= today() - 7
    GROUP BY campaign_id
    HAVING impressions_7d > 5000
),
click_agg AS (
    SELECT campaign_id, count() AS clicks_7d
    FROM adpx_tracked_clicks
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
    WHERE created_at >= today() - 7
    GROUP BY campaign_id
    HAVING clicks_7d > 100
),
rev_agg AS (
    SELECT campaign_id,
           coalesce(sum(toFloat64OrNull(revenue)), 0) AS revenue_7d,
           count()                                    AS conversion_count_7d
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 7)
    WHERE created_at >= today() - 7
    GROUP BY campaign_id
)
SELECT
    c.id                                          AS campaign_id,
    c.adv_name,
    c.title                                       AS campaign_title,
    ia.impressions_7d,
    ca.clicks_7d,
    coalesce(ra.revenue_7d, 0)                    AS revenue_7d,
    toString(ia.first_impression_date)            AS first_impression_date,
    groupArray(toInt64(pc.user_id))               AS publisher_ids,
    groupArray(u.organization)                    AS publisher_names
FROM imp_agg ia
JOIN click_agg ca ON toString(ca.campaign_id) = toString(ia.campaign_id)
JOIN from_airbyte_campaigns c ON toInt64(ia.campaign_id) = c.id
    AND JSONLength(c.conversion_events) > 0
    AND (c.is_test = false OR c.is_test IS NULL)
LEFT JOIN rev_agg ra ON toString(ra.campaign_id) = toString(ia.campaign_id)
LEFT JOIN from_airbyte_publisher_campaigns pc
    ON toString(pc.campaign_id) = toString(ia.campaign_id) AND pc.is_active = 1
LEFT JOIN from_airbyte_users u ON pc.user_id = u.id
WHERE coalesce(ra.conversion_count_7d, 0) = 0
  AND ia.first_impression_date <= today() - 7
  AND c.deleted_at IS NULL
GROUP BY c.id, c.adv_name, c.title, ia.impressions_7d, ca.clicks_7d, revenue_7d, ia.first_impression_date
HAVING impressions_7d > 5000 AND clicks_7d > 200
ORDER BY impressions_7d DESC
LIMIT 25
"""
    try:
        rows = ch.query(sql).result_rows
    except Exception as e:
        return f"Ghost campaign query failed: {e}"

    if not rows:
        return (
            "*Ghost Campaign Report*\n\n"
            ":white_check_mark: No ghost campaigns detected — all active campaigns with "
            "high engagement are generating revenue."
        )

    lines = [f"*Ghost Campaign Report — Full List* ({len(rows)} campaigns)\n"]
    for campaign_id, adv_name, campaign_title, imps, clicks, rev, first_date_str, pub_ids, pub_names in rows:
        imp_str = f"{imps / 1000:.0f}K" if imps >= 1000 else str(imps)
        rev_str = f"${rev:.2f}" if rev > 0 else "$0"
        first_date_str = str(first_date_str)[:10] if first_date_str else "unknown"

        # Publisher context — deduplicate and format as "Name (#ID)"
        seen, pub_parts = set(), []
        for pid, pname in zip(pub_ids, pub_names):
            if pid not in seen and pname:
                seen.add(pid)
                pub_parts.append(f"{pname} (#{pid})")
        pub_str = ", ".join(pub_parts[:3]) if pub_parts else "unknown publisher"

        if clicks > 0 and rev == 0:
            hypothesis = "Zero conversions in 7 days — postback not firing post-click. Check postback URL config."
        else:
            hypothesis = "Clicks converting but revenue not flowing — check campaign payout config."

        lines.append(
            f"• *{adv_name}* · Campaign #{campaign_id} · {pub_str}\n"
            f"  {imp_str} impressions · {clicks:,} clicks · {rev_str} · since {first_date_str}\n"
            f"  ↳ _{hypothesis}_"
        )

    lines.append(
        "\n:zap: Start with the highest-impression campaigns — they're burning the most inventory. "
        "Pull the postback URL for each campaign from the network dashboard and confirm pixel fires."
    )
    return "\n".join(lines)


_POST_TX_PLACEMENTS = (
    "'checkout_confirmation_page'", "'order_confirmation'", "'order-confirmation'",
    "'buy_flow_thank_you'", "'buyflowthankyou'", "'acctmgmt_payment_confirmation'",
    "'acctmgmtpaymentconfirmation'", "'receipt'", "'visit-receipt'", "'visit_receipt'",
    "'parking_pass_receipt'", "'order-receipt'", "'receipt-parkingdotcom'",
    "'post_checkout_receipt'", "'post_transaction'", "'post_transaction_page'",
    "'metropolis_transaction_details'", "'7eleven-fuel-transactionreceipt-bottom'",
    "'7Eleven_Fuel_TransactionReceipt_Bottom'", "'conv-orderconfirmation'",
    "'thank_you'", "'message_confirmation'", "'registration_complete'",
    "'order_status_offers'",
)


def get_low_fill_publishers() -> str:
    """
    Return publishers on post-transaction placements with fill rate < 15% over last 30 days.
    Fill rate = % of sessions that received at least one offer impression.
    Low fill on a checkout/receipt page = burned traffic with no monetization attempt.
    """
    ch = _get_ch_client()
    placements_sql = ", ".join(_POST_TX_PLACEMENTS)
    sql = f"""
WITH sessions_agg AS (
    SELECT
        toInt64(user_id) AS publisher_id,
        placement,
        count() AS sessions_30d
    FROM adpx_sdk_sessions
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
    WHERE created_at >= today() - 30
      AND placement IN ({placements_sql})
    GROUP BY user_id, placement
    HAVING sessions_30d > 10000
),
imps_agg AS (
    SELECT
        toInt64(pid) AS publisher_id,
        count(DISTINCT session_id) AS sessions_with_imps
    FROM adpx_impressions_details
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
    WHERE created_at >= today() - 30
    GROUP BY pid
),
rev_agg AS (
    SELECT
        toInt64(user_id) AS publisher_id,
        coalesce(sum(toFloat64OrNull(revenue)), 0) AS revenue_30d,
        count(DISTINCT session_id) AS converting_sessions
    FROM adpx_conversionsdetails
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 30)
    WHERE created_at >= today() - 30
    GROUP BY user_id
)
SELECT
    s.publisher_id,
    u.organization AS publisher_name,
    s.placement,
    s.sessions_30d,
    coalesce(i.sessions_with_imps, 0) AS sessions_with_imps,
    round(100.0 * coalesce(i.sessions_with_imps, 0) / s.sessions_30d, 2) AS fill_rate_pct,
    s.sessions_30d - coalesce(i.sessions_with_imps, 0)                    AS missed_sessions,
    coalesce(r.revenue_30d, 0)                                             AS revenue_30d
FROM sessions_agg s
LEFT JOIN imps_agg i ON i.publisher_id = s.publisher_id
LEFT JOIN rev_agg r  ON r.publisher_id = s.publisher_id
LEFT JOIN from_airbyte_users u ON s.publisher_id = u.id
WHERE coalesce(i.sessions_with_imps, 0) * 100.0 / s.sessions_30d < 15
ORDER BY missed_sessions DESC
LIMIT 15
"""
    try:
        rows = ch.query(sql).result_rows
    except Exception as e:
        return f"Fill rate query failed: {e}"

    if not rows:
        return (
            "*Publisher Fill Rate Report*\n\n"
            ":white_check_mark: All post-transaction publishers are filling at ≥15% — "
            "no low-fill anomalies detected."
        )

    total_missed = sum(int(r[6]) for r in rows)
    # Rough RPM from revenue / sessions_with_imps across all low-fill pubs
    total_imps = sum(int(r[4]) for r in rows)
    total_rev  = sum(float(r[7]) for r in rows)
    avg_rpm    = (total_rev / total_imps * 1000) if total_imps > 0 else 0
    est_at_risk = total_missed / 30 * avg_rpm / 1000  # daily revenue at risk

    lines = [
        f"*Publisher Fill Rate Report — Low Fill on Post-Transaction Pages*\n",
        f"{len(rows)} publisher{'s' if len(rows) != 1 else ''} below 15% fill · "
        f"{total_missed / 1_000_000:.1f}M missed sessions/30d",
    ]
    if avg_rpm > 0:
        lines.append(
            f"Est. revenue at risk: *${est_at_risk:,.0f}/day* "
            f"(based on ${avg_rpm:.2f} RPM on served sessions)\n"
        )
    else:
        lines.append("")

    for pub_id, pub_name, placement, sessions, with_imps, fill_pct, missed, rev in rows:
        sessions_str = f"{int(sessions) / 1_000_000:.1f}M" if sessions >= 1_000_000 else f"{int(sessions) / 1000:.0f}K"
        missed_str   = f"{int(missed) / 1_000_000:.1f}M" if missed >= 1_000_000 else f"{int(missed) / 1000:.0f}K"
        rev_str      = f"${float(rev) / 1000:.1f}K" if float(rev) >= 1000 else f"${float(rev):.0f}"

        if fill_pct < 2:
            hypothesis = "Near-zero fill — SDK likely not showing offers at all. Check SDK integration, geo/OS targeting config, or advertiser supply for this placement."
        elif fill_pct < 10:
            hypothesis = "Very low fill — most sessions get no offer. Check advertiser targeting restrictions (geo/OS/device), cap exhaustion, or SDK render failure."
        else:
            hypothesis = "Below-normal fill — some sessions served, but majority missed. May be geo/device targeting mismatch or insufficient advertiser supply."

        lines.append(
            f"• *{pub_name or f'Pub #{pub_id}'}* · `{placement}` · Pub #{pub_id}\n"
            f"  {sessions_str} sessions · {fill_pct:.1f}% fill · {missed_str} sessions missed · {rev_str} revenue/30d\n"
            f"  ↳ _{hypothesis}_"
        )

    lines.append(
        "\n:zap: Start with the highest missed-session publishers — they represent the largest "
        "uncaptured monetization surface. Check SDK integration logs and advertiser targeting config."
    )
    return "\n".join(lines)


def get_top_revenue_opportunities() -> str:
    """
    Return the top cross-publisher revenue gap opportunities.
    Finds high-performing advertisers (2+ publishers, >$10K/30d) not active
    in high-volume publishers (>100K sessions/30d). Ranked by estimated monthly revenue.
    """
    ch = _get_ch_client()
    sql = """
WITH adv_perf AS (
    SELECT
        c.adv_name,
        count(DISTINCT cv.user_id)                               AS publisher_count,
        round(sum(toFloat64OrNull(cv.revenue)), 2)               AS rev_30d,
        round(sum(toFloat64OrNull(cv.revenue))
              / nullIf(count(DISTINCT cv.user_id), 0), 2)        AS avg_rev_per_pub
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
        adv.adv_name,
        adv.rev_30d       AS adv_total_rev_30d,
        adv.avg_rev_per_pub AS est_monthly_rev,
        adv.publisher_count AS adv_pub_count,
        pv.sessions_30d
    FROM pub_volume pv
    CROSS JOIN adv_perf adv
)
SELECT
    c.publisher_name,
    c.publisher_id,
    c.adv_name,
    c.adv_total_rev_30d,
    c.est_monthly_rev,
    c.adv_pub_count,
    c.sessions_30d
FROM candidates c
LEFT JOIN active_pairs ap
    ON ap.publisher_id = c.publisher_id
   AND ap.adv_name = c.adv_name
WHERE ap.publisher_id IS NULL
ORDER BY c.est_monthly_rev DESC, c.sessions_30d DESC
LIMIT 20
"""
    try:
        rows = ch.query(sql).result_rows
    except Exception as e:
        return f"Revenue opportunities query failed: {e}"

    if not rows:
        return (
            "*Revenue Opportunity Report*\n\n"
            ":white_check_mark: No obvious cross-publisher gaps detected — "
            "all high-performing advertisers appear active in major publishers."
        )

    total_est = sum(float(r[4]) for r in rows)
    lines = [
        f"*Revenue Opportunity Report — Cross-Publisher Gaps* ({len(rows)} opportunities)\n",
        f"Total estimated monthly revenue at risk: *${total_est / 1000:.0f}K/mo*\n",
        "_Advertisers already earning on 2+ publishers — the revenue pattern is proven, "
        "the distribution gap is the opportunity._\n",
    ]

    for pub_name, pub_id, adv_name, adv_rev, est_rev, pub_count, sessions in rows:
        sessions_str = f"{int(sessions) / 1_000_000:.1f}M" if sessions >= 1_000_000 else f"{int(sessions) / 1000:.0f}K"
        adv_rev_str  = f"${float(adv_rev) / 1000:.0f}K" if float(adv_rev) >= 1000 else f"${float(adv_rev):.0f}"
        est_rev_str  = f"${float(est_rev) / 1000:.0f}K" if float(est_rev) >= 1000 else f"${float(est_rev):.0f}"
        lines.append(
            f"• Add *{adv_name}* → *{pub_name or f'Pub #{pub_id}'}*\n"
            f"  {adv_name} earns {adv_rev_str}/30d across {pub_count} publishers · est. *{est_rev_str}/mo* if added\n"
            f"  {pub_name}: {sessions_str} sessions/30d · not currently running this advertiser"
        )

    lines.append(
        "\n:zap: Prioritize by session volume × avg revenue. "
        "Confirm no geo/OS exclusions exist before requesting provisioning."
    )
    return "\n".join(lines)


def get_scout_status() -> dict:
    """
    System health snapshot: benchmark freshness, offer inventory, queue depth,
    ClickHouse connectivity, and data quality warnings.
    """
    import time as _time
    from datetime import datetime, timezone

    status: dict = {}

    # Benchmark freshness
    age_secs = _time.time() - _BENCHMARKS_LOADED_AT if _BENCHMARKS_LOADED_AT else None
    if age_secs is None:
        status["benchmarks"] = "not loaded"
    elif age_secs < 120:
        status["benchmarks"] = f"{int(age_secs)}s ago"
    elif age_secs < 3600:
        status["benchmarks"] = f"{int(age_secs / 60)}m ago"
    else:
        status["benchmarks"] = f"{age_secs / 3600:.1f}h ago (stale — will refresh on next query)"

    # Benchmark coverage
    bench = _BENCHMARKS or {}
    status["benchmark_coverage"] = {
        "by_offer":    len(bench.get("by_offer_impact_id", {})),
        "by_advertiser": len(bench.get("by_adv_name", {})),
        "by_category_payout": len(bench.get("by_category_payout", {})),
        "by_payout_type": len(bench.get("by_payout_type", {})),
    }

    # Offer inventory
    offers = _load_offers()
    status["offer_inventory"] = len(offers)
    if offers:
        advertisers = {o.get("advertiser") for o in offers}
        status["unique_advertisers"] = len(advertisers)
        networks = {}
        for o in offers:
            n = (o.get("network") or "unknown").lower()
            networks[n] = networks.get(n, 0) + 1
        status["by_network"] = networks

    # Demand queue
    state = _load_launched_offers_state()
    queued    = [k for k, v in state.items() if v.get("status") == "queued"]
    launched  = [k for k, v in state.items() if v.get("status") == "launched"]
    status["queue_depth"] = len(queued)
    status["launched_count"] = len(launched)
    if queued:
        status["queue_items"] = queued

    # ClickHouse connectivity
    try:
        ch = _get_ch_client()
        rows = ch.query("SELECT 1").result_rows
        status["clickhouse"] = "ok" if rows else "degraded"
    except Exception as e:
        status["clickhouse"] = f"unavailable: {str(e)[:80]}"

    # Data quality warnings
    warnings = []
    if bench and not bench.get("by_offer_impact_id"):
        warnings.append("No Tier 1 (exact offer) benchmarks — all scoring from Tier 2+")
    cats_null = sum(1 for o in offers if not o.get("category"))
    if cats_null > 0:
        pct = cats_null / max(len(offers), 1) * 100
        warnings.append(f"{cats_null} offers ({pct:.0f}%) have no category — Tier 3 scoring disabled for these")
    if warnings:
        status["warnings"] = warnings

    status["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return status


def run_offer_scraper() -> str:
    """
    Trigger an immediate offer inventory refresh from affiliate networks
    (Impact, FlexOffers, MaxBounty). Run when offer inventory is empty or stale.
    Takes ~2 minutes. Writes data/offers_latest.json and posts digest.
    """
    import scout_bot as _sb
    if _sb._SCRAPER_RUNNING.is_set():
        return (
            ":hourglass_flowing_sand: Scraper is already running (daily 6am CT run in progress). "
            "Check back in a few minutes — offer inventory will be fresh when it completes."
        )
    try:
        from offer_scraper import run_headless
        log.info("[scraper] on-demand refresh triggered via @Scout")
        run_headless()
        # Report results
        if SNAPSHOT_PATH.exists():
            import json as _json
            offers = _json.loads(SNAPSHOT_PATH.read_text())
            active = sum(1 for o in offers if o.get("status") == "Active")
            return (
                f":white_check_mark: Offer inventory refreshed — {len(offers)} total offers, "
                f"{active} active. Scout Score rankings and prospecting are now fully operational."
            )
        return (
            ":white_check_mark: Scraper ran. No offers_latest.json found — "
            "check Render logs for network errors."
        )
    except Exception as e:
        log.error(f"[scraper] on-demand run failed: {e}", exc_info=True)
        return f":x: Scraper failed: {e}. Check Render logs for details."


def get_pipeline_health() -> str:
    """
    Report on the Scout offer approval pipeline: how many offers are approved,
    how many are stale (>7 days without a Live/Done status), and the oldest pending.
    Reads from the Notion Scout Demand Queue database.
    """
    import os, requests as _req, json as _json
    from datetime import datetime, timezone, timedelta

    notion_token = os.getenv("NOTION_TOKEN")
    db_id = os.getenv("NOTION_QUEUE_DB_ID")
    if not notion_token or not db_id:
        return (":warning: Pipeline health unavailable — `NOTION_QUEUE_DB_ID` not configured. "
                "Add it to Render env vars.")

    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    resp = _req.post(
        f"https://api.notion.com/v1/databases/{db_id}/query",
        headers=headers, json={"page_size": 100}
    )
    if not resp.ok:
        return f":x: Notion query failed: {resp.status_code}"

    pages = resp.json().get("results", [])
    now = datetime.now(timezone.utc)
    stale = []

    for page in pages:
        props = page.get("properties", {})
        status_prop = props.get("Status", {})
        status_val = ""
        if status_prop.get("type") == "select" and status_prop.get("select"):
            status_val = status_prop["select"].get("name", "")
        if status_val.lower() in ("live", "done", "launched"):
            continue
        created = page.get("created_time", "")
        if not created:
            continue
        age = now - datetime.fromisoformat(created.replace("Z", "+00:00"))
        if age > timedelta(days=7):
            adv = ""
            for key in ("Offer", "Name", "title"):
                tp = props.get(key, {})
                if tp.get("type") == "title":
                    items = tp.get("title", [])
                    adv = items[0]["plain_text"] if items else ""
                    break
            stale.append((adv or "Unknown", age.days))

    stale.sort(key=lambda x: x[1], reverse=True)
    total = len(pages)
    lines = [f"*Scout Offer Pipeline* — *{total}* offers approved total"]
    if not stale:
        lines.append(":white_check_mark: Pipeline clear — no offers stale beyond 7 days.")
        lines.append("\n:zap: *Action:* Pipeline looks healthy. Consider adding Commission Junction to expand offer supply.")
    else:
        lines.append(f":warning: *{len(stale)} offers pending >7 days* without a Live status.")
        for adv, age_days in stale[:5]:
            lines.append(f"• {adv} — *{age_days} days* without Live status")
        if len(stale) > 5:
            lines.append(f"• ...and {len(stale)-5} more")
        lines.append(f"\n:zap: *Action:* Mark offers Live in Notion once entered in MS platform, or ping Gordon for a status update.")
    return "\n".join(lines)


def get_usage_report(requesting_user_id: str = "") -> str:
    """
    Return Scout usage statistics. Admin-only (SCOUT_ADMIN_USER_ID env var).
    Shows: queries per period, top users, most-used tools, avg response time.
    """
    import os, pathlib, json as _json
    from collections import Counter
    from datetime import datetime, timezone, timedelta

    admin_uid = os.getenv("SCOUT_ADMIN_USER_ID", "")
    if not admin_uid or requesting_user_id != admin_uid:
        return ":lock: Usage reports are admin-only."

    log_path = pathlib.Path(__file__).parent / "data" / "usage_log.jsonl"
    if not log_path.exists():
        return "No usage data yet — logging started after this deploy. Check back after a few queries."

    records = [_json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]
    now = datetime.now(timezone.utc)
    recent_7d  = [r for r in records if datetime.fromisoformat(r["ts"]) >= now - timedelta(days=7)]
    recent_30d = [r for r in records if datetime.fromisoformat(r["ts"]) >= now - timedelta(days=30)]

    user_counts = Counter(r.get("user_name", r.get("user_id", "unknown")) for r in recent_30d)
    tool_counts = Counter(t for r in recent_30d for t in (r.get("tools") or []))
    avg_ms = int(sum(r.get("ms", 0) for r in recent_7d) / max(len(recent_7d), 1))

    lines = [f"*Scout Usage Report*\n"]
    lines.append(f"• *{len(recent_7d)}* queries last 7 days, *{len(recent_30d)}* last 30 days")
    lines.append(f"• Avg response time (7d): *{avg_ms // 1000}s*\n")
    lines.append("*Top users (30d):*")
    for name, count in user_counts.most_common(8):
        lines.append(f"• {name} — *{count}* queries")
    if tool_counts:
        lines.append("\n*Top tools called (30d):*")
        for tool, count in tool_counts.most_common(10):
            lines.append(f"• {tool} — *{count}x*")
    return "\n".join(lines)


def get_offers_for_publisher(publisher_name: str) -> str:
    """
    Return top affiliate offers (Impact/FlexOffers/MaxBounty inventory) that are
    a good fit for this publisher but not yet provisioned in their campaign set.
    Scored by estimated RPM using real MS conversion benchmarks (_scout_score).
    Different from get_supply_demand_gaps — surfaces NET-NEW affiliate inventory,
    not advertisers already on the MS platform.
    """
    import json as _json

    if not SNAPSHOT_PATH.exists():
        return (
            f"Offer inventory is empty — the scraper hasn't run yet on Render. "
            f"Run `@Scout refresh offers` to fetch now (~2 min), "
            f"or wait for the 6am CT daily auto-refresh."
        )

    try:
        all_offers = _json.loads(SNAPSHOT_PATH.read_text())
    except Exception as e:
        return f"Offer inventory file is corrupt: {e}. Try `@Scout refresh offers`."

    active = [o for o in all_offers if o.get("status") == "Active"]
    if not active:
        return "Offer inventory has 0 active offers. Try `@Scout refresh offers` to fetch latest."

    ch = _get_ch_client()

    # Resolve publisher
    pub_rows = ch.query(
        "SELECT id, organization FROM from_airbyte_users "
        "WHERE organization ILIKE %(pub)s LIMIT 5",
        parameters={"pub": f"%{publisher_name}%"}
    ).result_rows
    if not pub_rows:
        return f"Publisher '{publisher_name}' not found in MomentScience."
    pub_id, pub_org = pub_rows[0]

    # Existing advertiser set for this publisher (active campaigns only)
    existing_rows = ch.query(
        "SELECT DISTINCT c.adv_name "
        "FROM from_airbyte_publisher_campaigns pc "
        "JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id "
        "WHERE toInt64(pc.user_id) = %(uid)s AND pc.is_active = 1 AND pc.deleted_at IS NULL",
        parameters={"uid": int(pub_id)}
    ).result_rows
    existing_adv = {row[0].lower() for row in existing_rows}

    # Filter to net-new advertisers (not already provisioned for this publisher)
    def _is_new(offer: dict) -> bool:
        adv = (offer.get("advertiser") or "").lower()
        return not any(adv in ex or ex in adv for ex in existing_adv)

    candidates = [o for o in active if _is_new(o)]

    if not candidates:
        return (
            f"All active affiliate offers in the inventory are already provisioned in {pub_org}. "
            f"Use `@Scout revenue opportunities` for cross-publisher advertiser gaps, "
            f"or `@Scout what advertisers aren't in {pub_org}` for the provisioning view."
        )

    # Score using existing benchmark infrastructure
    benchmarks = _load_performance_benchmarks()
    scored_all = [(o, _scout_score(o, benchmarks)) for o in candidates]

    # Split: confirmed rate (score > 0, has real payout) vs uncontracted (Rate TBD)
    confirmed = [(o, s) for o, s in scored_all if s > 0]
    confirmed.sort(key=lambda x: x[1], reverse=True)
    confirmed_top = confirmed[:8]

    # Uncontracted = active, net-new, no payout confirmed yet — show top 5 by category fit
    uncontracted = [
        o for o, s in scored_all if s == 0
        and (o.get("_raw_payout") or "").lower() in ("rate tbd", "tbd", "", "?")
    ]
    uncontracted = uncontracted[:5]

    if not confirmed_top and not uncontracted:
        return (
            f"{len(candidates)} net-new affiliate offers found for {pub_org}, "
            f"but none have MS benchmark data yet (no prior run history). "
            f"Try `@Scout revenue opportunities` for cross-publisher revenue gaps with proven estimates."
        )

    _NETWORK_EMOJI = {"impact": "⚡", "maxbounty": "💰", "flexoffers": "🔗"}

    lines = [
        f"*🎯  {pub_org} — Offer Recommendations*",
        f"_{len(candidates)} net-new candidates screened · {len(confirmed_top)} with confirmed rates · "
        f"{len(uncontracted)} uncontracted_",
        "",
    ]

    # ── Section 1: Confirmed rates — ranked by Scout Score ────────────────────
    if confirmed_top:
        lines.append("*✅  Confirmed Rate — Ready to Pitch*")
        lines.append("─" * 32)
        for i, (o, score) in enumerate(confirmed_top, 1):
            advertiser  = o.get("advertiser") or "Unknown"
            raw_payout  = o.get("_raw_payout") or o.get("payout") or "?"
            category    = o.get("category") or "Uncategorized"
            geo         = (o.get("geo") or "").strip()
            network     = o.get("network") or ""
            net_emoji   = _NETWORK_EMOJI.get(network.lower(), "•")
            net_label   = network.title()
            geo_str     = f" · {geo}" if geo and geo.lower() not in ("us", "usa", "united states") else " · US"
            lines.append(
                f"*{i}. {advertiser}*   {net_emoji} {net_label}\n"
                f"   `{raw_payout}` · {category}{geo_str} · est. *${score:.2f} RPM*"
            )
        lines.append("")

    # ── Section 2: Uncontracted — apply to unlock ─────────────────────────────
    if uncontracted:
        lines.append("*🔍  Uncontracted — Apply to Unlock Rate*")
        lines.append(
            "_These are in the affiliate network's marketplace but need a contract "
            "before they can be pitched. Once approved, the daily scrape picks up the rate._"
        )
        lines.append("─" * 32)
        for o in uncontracted:
            advertiser = o.get("advertiser") or "Unknown"
            category   = o.get("category") or "Uncategorized"
            geo        = (o.get("geo") or "").strip()
            network    = o.get("network") or ""
            net_emoji  = _NETWORK_EMOJI.get(network.lower(), "•")
            geo_str    = f" · {geo}" if geo and geo.lower() not in ("us", "usa", "united states") else " · US"
            lines.append(f"• *{advertiser}*   {net_emoji} {network.title()} · {category}{geo_str}")
        lines.append("")

    # ── Footer CTA ────────────────────────────────────────────────────────────
    if confirmed_top:
        top_name = confirmed_top[0][0].get("advertiser") or "top offer"
        lines.append(f":zap:  `@Scout brief {top_name}` to build a campaign brief · "
                     f"`@Scout brief [name]` for any other offer above")
    else:
        lines.append(":zap:  Apply for contracts in Impact's publisher portal to unlock rates — "
                     "Scout will pick them up automatically on the next daily scrape.")

    return "\n".join(lines)


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
    "get_publisher_health": get_publisher_health,
    "get_campaign_status": get_campaign_status,
    "get_perkswall_engagement": get_perkswall_engagement,
    "get_supply_demand_gaps": get_supply_demand_gaps,
    "run_sql_query": run_sql_query,
    "get_scout_status": get_scout_status,
    "get_advertiser_revenue_projection": get_advertiser_revenue_projection,
    "get_ghost_campaigns": get_ghost_campaigns,
    "get_low_fill_publishers": get_low_fill_publishers,
    "get_top_revenue_opportunities": get_top_revenue_opportunities,
    "run_offer_scraper": run_offer_scraper,
    "get_pipeline_health": get_pipeline_health,
    "get_usage_report": get_usage_report,
    "get_offers_for_publisher": get_offers_for_publisher,
}


def _run_tool(name: str, inputs: dict, _caller_user_id: str = ""):
    fn = TOOL_MAP.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    # Inject caller identity for admin-gated tools so the model doesn't need to
    # manually extract and pass the user_id from the injected context prefix.
    if name == "get_usage_report" and not inputs.get("requesting_user_id"):
        inputs = {**inputs, "requesting_user_id": _caller_user_id}
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


def _select_model(user_message: str) -> str:
    """Route simple queries to Haiku, complex analytical queries to Sonnet."""
    msg = user_message.lower()
    simple = ["status", "queue", "is scout", "help", "paused", "active",
              "how many", "count", "list all", "what is the cap"]
    complex_ = ["health", "competitive", "projection", "revenue", "rpm",
                "trend", "brief", "compare", "analyze", "performance",
                "velocity", "benchmark", "opportunity", "why"]
    if sum(1 for p in simple if p in msg) > sum(1 for p in complex_ if p in msg):
        return "claude-haiku-3-5-20241022"
    return "claude-sonnet-4-6"


def ask(user_message: str, history: list = None, user_id: str = "") -> str:
    """
    Send a message to Scout and get a response.
    history: optional list of prior {"role": "user"/"assistant", "content": str} messages
             from the Slack thread, providing conversation context.
    user_id: Slack user ID of the caller — injected into context so tools like
             get_usage_report can enforce admin-only access.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return "ANTHROPIC_API_KEY not set — Scout can't respond."

    client = anthropic.Anthropic(
        api_key=api_key,
        default_headers={"anthropic-beta": "prompt-caching-2024-07-31"}
    )
    # Prepend team context (from Slack channels) + corrections as grounding context
    channel_ctx     = _get_channel_context(user_message)
    corrections_ctx = _get_corrections_context()
    caller_ctx      = f"[Caller Slack user_id: {user_id}]\n" if user_id else ""
    prefix = caller_ctx + channel_ctx + corrections_ctx
    effective_message = (prefix + user_message) if prefix else user_message
    messages = list(history or []) + [{"role": "user", "content": effective_message}]
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
                    model=_select_model(user_message),
                    max_tokens=2048,
                    system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
                    tools=TOOLS,
                    messages=messages,
                )
                break
            except anthropic.APIConnectionError:
                if attempt < 3:
                    wait = 2 ** attempt
                    log.warning(f"Anthropic connection error, retry {attempt + 1}/3 in {wait}s")
                    time.sleep(wait)
                else:
                    raise
            except anthropic.APIStatusError as e:
                if e.status_code in (429, 500, 502, 503, 529) and attempt < 3:
                    wait = 2 ** attempt
                    log.warning(f"Anthropic {e.status_code}, retry {attempt + 1}/3 in {wait}s (attempt {attempt + 1}/3)")
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

                # Fallback: extract copy from Claude's plain-text response.
                # Check both new schema (title) and old schema (titles) — either means we have copy.
                has_copy = copy_data and (copy_data.get("title") or copy_data.get("titles"))
                if not has_copy:
                    copy_data = _extract_copy_from_text(text)
                    log.info(f"Extracted copy from plain text for {brief_data.get('advertiser')}: "
                             f"title={bool(copy_data.get('title'))}, titles={len(copy_data.get('titles', []))}")

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
        tool_blocks = [(i, block) for i, block in enumerate(response.content)
                       if block.type == "tool_use"]
        tool_results = []

        if len(tool_blocks) > 1:
            # Multiple tool calls — run in parallel, then reassemble in original order
            # Falls back to sequential if executor is unavailable (e.g. during shutdown)
            try:
                with ThreadPoolExecutor(max_workers=len(tool_blocks)) as executor:
                    futures = {
                        executor.submit(_run_tool, block.name, block.input, user_id): (i, block)
                        for i, block in tool_blocks
                    }
                    results_map = {}
                    for future in as_completed(futures):
                        i, block = futures[future]
                        result = future.result()
                        results_map[i] = (block, result)

                for i, _ in sorted(tool_blocks, key=lambda x: x[0]):
                    block, result = results_map[i]
                    if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                        _brief_results.append(result)
                    _all_tool_results.append(result)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result),
                    })
            except RuntimeError:
                # Interpreter shutting down — fall back to sequential
                for i, block in tool_blocks:
                    result = _run_tool(block.name, block.input, user_id)
                    if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                        _brief_results.append(result)
                    _all_tool_results.append(result)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result),
                    })
        else:
            # Single tool call — keep sequential path unchanged
            for i, block in tool_blocks:
                result = _run_tool(block.name, block.input, user_id)
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
