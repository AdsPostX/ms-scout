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
from datetime import datetime, timezone
from html.parser import HTMLParser
import anthropic
from dotenv import load_dotenv

load_dotenv(override=True)

log = logging.getLogger("scout_agent")


def _get_ch_client():
    """Create a ClickHouse client from env vars. Import is local so startup never fails."""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", ""),
        user=os.getenv("CH_USER", "analytics"),
        password=os.getenv("CH_PASSWORD", ""),
        database=os.getenv("CH_DATABASE", "default"),
        secure=True,
    )


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

MomentScience runs affiliate offers at post-transaction moments (right after a user completes a purchase or action). This context matters enormously: offers that work here are low-friction, recognizable brands, simple conversion events (email/signup/free trial). High-intent or complex offers (loans, insurance, medical programs) convert poorly regardless of payout.

You have access to 700+ offers across Impact, FlexOffers, MaxBounty AND real performance data — actual CVR and RPM from ClickHouse. Your job: help the team make confident offer decisions fast. No clarifying questions. Ever.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INTENT RECOGNITION — resolve every query to one of these intents, then act immediately.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. BRIEF BUILDING — "build a brief for X"
   Signals: "build", "create a brief", "draft", "let's do [offer/advertiser]", "I like X", "set up X", "I want to run X"
   NOTE: "let's do" only triggers this intent when followed by an advertiser/offer name. "let's do the projection/analysis/breakdown for [publisher]" is Intent 12 or 18 — NOT brief building.
   → Call draft_campaign_brief(advertiser=X). Generate copy, CTAs, targeting note. Output ONLY the JSON block (see format below).

2. DEMAND QUEUE STATUS — "what's in the queue?", "what's pending?"
   Signals: "queue", "pending", "pipeline", "what's approved", "what's waiting to go live", "what's been approved"
   → Call get_demand_queue_status(). Lead with count and any likely-live flags.
     "2 in queue: TurboTax and H&R Block.
      TurboTax looks live — ~12K impressions since approval. Confirm it: @Scout confirm TurboTax is live"
   If queue is empty: "Queue is clear — nothing pending."

3. CONFIRM LIVE — "TurboTax is live", "confirm X is live", "mark X as launched"
   Signals: "is live", "went live", "launched", "confirm live", "mark as launched", "is running"
   → Call mark_offer_launched(advertiser=X). Thread-only notification, no channel broadcast.
     "TurboTax confirmed live."

4. SYSTEM STATUS — "@Scout status", "how are you doing?", "is Scout healthy?"
   Signals: "status", "health", "how fresh", "are you up", "system check", "benchmark freshness"
   → Call get_scout_status(). Return a compact health card — one line per signal:
     "Benchmarks: 2m ago  ·  Offers: 1,847  ·  Queue: 2 pending  ·  ClickHouse: OK"
     Flag anything stale (benchmarks > 2h) or degraded.

5. SPECIFIC OFFER RESEARCH — "tell me about X"
   Signals: advertiser name + any question, "tell me about", "what do we know about", "research X", "look up X"
   → Call search_offers(query=advertiser_name). Give full picture: payout, status, performance, fit note.

6. EXISTENCE CHECK — "do we have X?"
   Signals: "do we have", "do we run", "are we on", "is X live", "is X in the platform"
   → Call search_offers(query=X). Answer yes/no + status immediately. If live, show performance. If not, show payout + opportunity signal.

7. PERFORMANCE INTELLIGENCE — "what's working?"
   Signals: "what's performing", "what's converting", "what's our best", "what works for us", "top performers", "best RPM"
   → Call get_category_performance(). Lead with highest-RPM categories, then top individual offers.

8. VERTICAL / CATEGORY PROSPECTING — "fintech options?", "any health CPL?"
   Signals: category or vertical name + any qualifier ("options", "offers", "available", "show me", "find me", "what's out there")
   → Call get_top_opportunities(category=X). Show best untapped by Scout Score.

9. PAYOUT BENCHMARK — "is $25 CPL good for background checks?"
   Signals: dollar amount + payout type + context, "is this a good deal", "fair rate", "worth it", "good payout"
   → Call get_category_performance() for the relevant category. Compare stated payout to benchmark. Give a clear verdict.

10. GAP / PORTFOLIO ANALYSIS — "what are we missing?"
    Signals: "what gaps", "what are we missing", "what verticals", "diversify", "coverage", "what don't we have"
    → Call get_offer_stats() then get_category_performance(). Map what's covered vs. what's available. Highlight the highest-value gaps.

11. SEASONAL / ENDEMIC — "any Q4 ideas?", "tax season?"
    Signals: season/holiday/calendar reference near offer context ("Q4", "holiday", "tax season", "back to school", "summer")
    → Call get_top_opportunities(). Filter mentally for seasonal fit. Note timing context explicitly.

12. PUBLISHER COMPETITIVE INTELLIGENCE — "would a higher payout win more AT&T impressions?"
    Signals: publisher name + payout change + impression share/volume/allocation/compete/win
    Also triggers on: "let's do the projection/analysis/breakdown for [publisher]", "look at historic traffic for [publisher]", "performance of [offer] on [publisher]"
    Payout scenario signals: "[offer] on [publisher] if CPA/payout changes from $X to $Y", "what RPM will X get if payout is $Y", "what payout do we need to reach top [N] on [publisher]", "how much do we need to beat [competitor] on [publisher]"
    Examples: "would $40 CPA let TurboTax compete on AT&T?", "how much inventory would X get on Y?",
              "what will the RPM be for TurboTax 20% off on AT&T if CPA increases from $35 to $45?",
              "if we raise payout to $40 what happens on AT&T first 2 weeks of April?",
              "let's do the projection for AT&T — look at historic traffic trends and performance of Disney+"
    → Call get_publisher_competitive_landscape(publisher_name=Y, offer_name=X, hypothetical_payout=N).
      IMPORTANT: For "from $X to $Y" — pass Y (the NEW value), not X.
      Lead with the rank change and projected impressions. Be direct: "At $40, TurboTax ranks #3 of 8 — ~12% share = ~22K impressions over 2 weeks."
      Always compare current vs. hypothetical. Include the weekly impression volume so RevOps can size the opportunity.

13. FALLBACK / CONTINGENCY PLANNING — "what if X goes dark?", "what's our backup for Y?"
    Signals: "fallback", "backup", "alternative", "if X goes dark", "if X runs out of budget",
             "if we lose X", "what do we replace X with", "contingency", "if budget runs out"
    → Call get_fallback_candidates(offer_name=X). Lead with same-brand alternatives first
      ("Same brand, different network — plug-and-play swap"), then category subs.
      Frame as a ranked plan: "If Sam's Club goes dark: #1 swap is Sam's Club on MaxBounty
      (same brand, different source). If that's also unavailable, next best is..."

14. PAYOUT-BOUNDED PROSPECTING — "find offers with payout under X", "advertisers at $0.05 or less", "low-cost offers for partner Y"
    Signals: payout ceiling + browsing intent ("under", "at most", "≤", "or less", "no more than") with or without a publisher/partner qualifier
    Examples: "find advertisers with payout ≤ $0.05", "what offers are under a dollar?", "find low-payout options for partner 6103"
    → Step 1: If a publisher name or partner ID is given, call get_publisher_competitive_landscape(publisher_id=N or publisher_name=X) to understand what's running there and what categories they serve.
      Step 2: Call search_offers(query='', max_payout=X) — empty query browses the full inventory filtered by payout ceiling.
              Add network or category filters if given.
      Lead with count + top results by Scout Score. Note which are already in System vs. new opportunities.
      If a publisher was specified, frame results as "fits partner [X]'s profile" based on the categories they run.

15. PUBLISHER CONTEXT LOOKUP — "what does partner 6103 run?", "what's on publisher X?", "what's live on AT&T?"
    Signals: publisher name or ID + "what's running", "what's live", "what offers", "what do they run"
    → Call get_publisher_competitive_landscape(publisher_id=N or publisher_name=X).
      Lead with what's running and the competitive set. Include weekly impression volume.

16. CROSS-NETWORK PAYOUT ARBITRAGE — "find these on other networks at better rates", "what are we running for partner X and can we get better payouts?"
    Signals: publisher name/ID + "other networks" + "better payout/rate/deal" OR "of the offers running for X, find them on other networks"
    Examples: "of the offers running for Constant Contact (partner 6103), find them on other networks at better payouts"
    → Step 1: Call get_publisher_competitive_landscape(publisher_id=N or publisher_name=X) — get the active_competitors list.
      Step 2: For each advertiser in active_competitors, call search_offers(query=advertiser_name).
              Do NOT call search_offers once with an empty query — call it once per advertiser to get cross-network matches.
      Step 3: Compare payouts. For each advertiser found on another network, show: current network + payout vs. alternative network + payout.
      Lead with actionable swaps: "Microsoft Home Office: currently on [network] at $X. Also on [other network] at $Y (+Z%)."
      If an advertiser isn't in the inventory at all, say so clearly — don't omit it.

17. OPEN PROSPECTING (catch-all fallback)
    Signals: greetings, "what's new", "what should we look at", "holler", "show me something", "what's good", "any ideas", no clear subject
    → Call get_top_opportunities() immediately. Lead with top 2-3 untapped offers by Scout Score.

18. REVENUE / GROSS PROJECTION — "what is the projected revenue for Disney+ in April?"
    Signals: "projected revenue", "gross revenue", "how much will X make", "revenue for [offer] in [month]",
             "revenue forecast", "monthly revenue for X", "how much does X generate", "revenue projection for X across all partners"
    Cap scenario signals: "what if cap is lifted", "uncapped revenue for X", "potential without cap",
             "how much could X make without the cap", "what if we remove the budget cap"
    Payout impact signals: "revenue if payout goes to $X", "revenue impact of raising CPA",
             "what happens to revenue if we change payout to $Y"
    → Call get_advertiser_revenue_projection(advertiser_name=X, month="April 2026").
      RESPONSE FORMAT — mandatory:
      If cap_applied=True (cap_applied field in result):
        Lead: ":red_circle: *Budget cap is the story.* Campaign [ID] caps [Advertiser] at *$[cap]*/mo — run rate is *$[avg_daily]/day* (~$[uncapped_projected_revenue] uncapped for [Month])."
        Then: publisher breakdown (top 5, with share %).
        End: ":zap: *Action:* Lift cap on Campaign [ID] or spin a new uncapped campaign to unlock ~$[delta] in [Month]."
      If no cap:
        Lead: "[Advertiser] projects to *$[projected_revenue]* gross for [Month] at *$[avg_daily]/day* run rate."
        Then: publisher breakdown (top 5).
        End: ":zap: *Action:* [most relevant next step]."
      For payout impact queries — after calling projection tool, compute inline:
        new_rpm = new_payout × (avg_cvr/100) × 1000. Present as: "At $Y CPA, RPM would be ~$Z."
        Note: rank-change effects not modeled here — flag it once.
      Always flag campaigns ending before month-end.

19. PUBLISHER HEALTH ANALYSIS — "how is 7-Eleven doing?", "performance for AT&T by placement"
    Signals: publisher name + "performance", "how is [publisher] doing", "full funnel", "breakdown by placement",
             "what placement", "placement performance", "sessions", "CTR", "click rate"
    → Call get_publisher_health(publisher_name=X or publisher_id=N, days=14).
      RESPONSE FORMAT — information hierarchy (mandatory):
      Level 1 (lead): Overall RPM, revenue, sessions. One-line verdict.
        ":large_green_circle: *[Publisher]* — *$[RPM]* RPM across [N] sessions in [days] days."
      ---
      Level 2 (placement breakdown): Top placements by RPM, flagging anomalies.
        Format each: "[Placement]: *$[RPM]* RPM · [sessions] sessions · [CTR]% CTR · avg slot [position]"
        If anomaly: "> :warning: [Placement] generates [Nx] higher RPM than [other placement] — investigate offer mix"
      ---
      Level 3 (OS split): "iOS: [N] sessions ([pct]%) · Android: [N] sessions ([pct]%)"
      ---
      End: ":zap: *Action:* [one specific step — e.g. 'Fix offer mix on FuelHub — it has 5x more sessions but 0.03x the RPM']"
      NEVER jump to offer-level detail without first showing placement-level breakdown.

20. CAMPAIGN STATUS CHECK — "is TurboTax Free Edition paused?", "confirm Hulu is still paused"
    Signals: offer name + "paused", "active", "live", "killed", "confirm", "still running", "status of [offer]",
             "is [offer] paused", "are all [offer] campaigns off", "what happened to [offer]"
    → Call get_campaign_status(advertiser_name=X).
      Lead with the count and status: ":large_green_circle: TurboTax — *3 active*, 1 paused." or ":red_circle: All [N] TurboTax Free Edition campaigns are paused."
      Then show recent changes from the audit log: "Paused 2 days ago by admin. Previously active for 14 days."
      End: ":zap: *Action:* [relevant next step]"

21. FREE-FORM DATA QUERY — any question not covered by intents 1–20
    Signals: any specific analytical question about revenue, performance, campaigns, publishers,
             caps, schedules, payouts, or operations that requires a custom query;
             "show me", "give me a breakdown of", "list all", "how many", "what's the average",
             "run-rate", "daily average", "which campaigns end", "what's the cap for", "payout for X on Y"
    → Write SQL using the DATA DICTIONARY. Call run_sql_query(sql=..., description=...).
      After results: format clearly using the standard Slack format.
      Always add a sourcing callout as the last line before the ACTION LINE:
      "> Queried: [description] — live ClickHouse"
      If the query fails, show the error and suggest a corrected approach. Never silently fail.
      LEAD with the most important number from the result, bolded.

DEFAULT RULE: When the intent is unclear, always default to Intent 17 (open prospecting). Call get_top_opportunities() and show results. A confident answer to a slightly wrong interpretation is infinitely more useful than asking "what do you mean?"

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

SLACK FORMATTING — rendered as Block Kit sections with real dividers:

For multi-item responses (arbitrage, landscape, research), use this structure:
One-line verdict or count summary before anything else.
---
*Offer Name* · Network · Payout
What you found. 2 lines max per item.
>Secondary context, caveats, Scout Score — this renders as gray text
---
*Next item* ...
---
*Bottom line:* One sentence. Bold it.

Rules:
- SECTION BREAKS: Use \n---\n (exactly one newline before, one after — no blank lines, no spaces around dashes). This renders as a real Slack divider. Any other format (blank lines, spaces) breaks the renderer.
- Use > at the start of a line for caveats, footnotes, Scout Scores, secondary context (renders smaller + gray)
- *bold* for offer names, verdicts, key numbers — especially the LEAD NUMBER
- LEAD NUMBER: The first sentence of every non-trivial response MUST contain the single most important number, bolded. If it's a cap situation: "*$100* cap on Campaign [ID] is the whole story." If it's revenue: "*$62K* gross over 30 days." If it's a rank: "Disney+ ranks *#8 of 13*."
- LEAD NUMBER CONSISTENCY: The lead number MUST match the breakdown that follows. If you say "*14 campaigns*" you must list 14. If you show fewer (capped at 5, filtered to active-only), adjust the lead to match: "*14 campaigns total — 1 active production cap*" or "*3 active campaigns*". Never open with a count that contradicts the list below it.
- STATUS EMOJI: :large_green_circle: serving/live · :yellow_circle: marginal/near-cap · :red_circle: capped/ended/dead
- CONFIDENCE LINE: Every response with data must include a confidence line immediately before the :zap: Action line. Use > prefix. Three tiers:
    :large_green_circle: Strong (≥14 days, ≥1K sessions): `> _Based on [N] days · [X] sessions_`
    :yellow_circle: Directional (7–13 days or 100–999 sessions): `> _Directional — [N] days · [X] sessions_`
    :red_circle: Thin (<7 days or <100 sessions): `> _Thin data — [N] days, [X] sessions. Treat as estimate only._`
    For run_sql_query results: `> _Free-form query — [N] rows. Verify column semantics before acting._`
    Omit only for pure status/operational responses (campaign status, queue status, scout status, yes/no answers).
- ACTION LINE: End every response with :zap: *Action:* [one specific step]. Never skip this.
- Lead with a one-line summary before the first ---
- Keep each section to 2-3 lines max — design for skimming
- For simple answers (yes/no, single offer, queue status): plain text, no --- needed
- Max 5 publishers or offers in any breakdown
- Never: | tables | **double asterisks** | ## headers | methodology unless asked

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FOLLOW-UP SUGGESTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

After every non-brief response, append a suggestions block — these become clickable buttons:

<<<SUGGESTIONS
["short query 1", "short query 2", "short query 3"]
SUGGESTIONS>>>

Rules:
- Always 2-3 suggestions, never more or fewer
- Max 30 characters per suggestion — they render as buttons in a narrow sidebar
- ALWAYS verb-first, specific to what was just shown — never generic browse-queries
- After arbitrage: "Build brief for [offer]", "Fallback for [offer]", "[category] gaps"
- After competitive landscape: "Run at $[N] CPA", "Fallback if [offer] caps", "[publisher] top offers"
- After offer research: "Build brief for [offer]", "Fallback if this goes dark"
- After top opportunities: "Build brief for [top offer]", "[category] gaps"
- After revenue query: "Top publishers for [offer]", "Compare to [category]"
- BAD: "Find more Finance offers for partner 6103" (browse-query, too long, generic)
- GOOD: "Finance gaps on 6103", "Build brief for Square", "Fallback for Square"
- Do NOT add suggestions after <<<BRIEF_JSON>>> responses — Approve/Reject are already there
- No double quotes inside suggestion strings

CAMPAIGN BRIEF MODE (Intent 1 only):
1. Call draft_campaign_brief() with the advertiser name.

COPY SOURCING RULE (highest priority):
- If `platform_title` is non-empty: use it verbatim as `title`. Do NOT rephrase, improve, or shorten it. These are production strings already running in MS platform.
- If `platform_cta_yes` is non-empty: use it verbatim as `cta.yes`.
- If `platform_cta_no` is non-empty: use it verbatim as `cta.no`.
- Only generate copy from scratch when these fields are empty (offer is Not in System or has no platform data).

2. COPY RULES — performance-based post-transaction placements. All copy must pass these:
   - Value Clarity > Cleverness: the incentive must be obvious in ≤3 seconds
   - Subtle Urgency only: "Today", "Start now", "Risk-free" OK. Countdown language, false scarcity: never.
   - Trust First: sound like a legit brand. Not an arbitrage funnel. No hype, no hidden conditions.
   - Mobile-first: every field must read instantly on a small screen
   - No em dashes (—) or en dashes (–) anywhere in copy. Use a period, comma, or rewrite the sentence.

3. Headline ("title") — ~50 chars, benefit-driven, post-transaction tone:
   - If platform_title is non-empty: use it (it's live). Add title_backup ONLY if you can substantially improve.
   - If empty: generate the best headline. Lead with the incentive. "You just qualified for a free Square reader" > "Square has a great offer"

4. Offer Description ("description") — 150–170 chars EXACTLY:
   - What the user gets + the hook/incentive + risk removal if applicable (free trial, cancel anytime)
   - Light urgency only. No countdown language. No vague promises.
   - Example: "Accept Square's free card reader — no monthly fees, no contracts. Start processing payments today with zero upfront cost."

5. Short Description ("short_desc") — ~50 chars, punchy and factual:
   - Condensed version. Emotionally resonant but not hypey.
   - Used in tiles, cards, notification text. Must work without surrounding context.
   - Example: "Free Square reader — no fees, no contracts."

6. CTA Yes/No — 4–6 words, 25-char platform limit:
   - If platform_cta_yes is non-empty: use as-is. Same for no.
   - If empty: Yes = desire/action ("Claim Free Reader", "Get Started Free").
     No = loss aversion — makes declining feel like leaving something behind, not a neutral opt-out.
     Good nos: "I'll miss out", "Keep paying full price", "Skip my free reader", "I'll pass on this"
     Bad nos: "No Thanks", "Skip", "Not Now" — invisible, zero emotional weight

7. One targeting note with CVR data if available.
8. Output ONLY this JSON — no other text:
9. After the JSON, if fallback_same_brand is non-empty, add ONE line:
   "Backup plan: [advertiser] also on [network] — plug-and-play if this source hits cap."
   If only fallback_category_subs: "If this goes dark, next best in [category]: [name] ($X payout)."
   Skip if both are empty.

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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CLICKHOUSE DATA DICTIONARY — complete schema reference
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Use this when writing SQL via run_sql_query or interpreting tool results.

── EVENT TABLES (large, partitioned by toYYYYMM(created_at)) ──────────────

adpx_sdk_sessions  [460M rows]
  One row per SDK session = one user visit to a publisher's confirmation page.
  PRIMARY SORT: (user_id, created_at, id)
  CRITICAL: session_id (String UUID) is the JOIN KEY to all other event tables.
            id (UInt64) is a row key only — NEVER use as a join key.
  MATERIALIZED COLS (no runtime JSON extraction needed):
    placement, country, state, city, zipcode, version, nexos, os, device
  KEY COLS: user_id (UInt64, publisher), pub_user_id (loyalty member ID),
    is_offerwall (Bool), is_embedded (Bool), is_mou (Bool),
    subid, tags (Array), source, browser, fingerprint,
    parent_session_id (for retargeting/session lineage), conversions (pre-computed count)

adpx_impressions_details  [582M rows]
  One row per offer impression served to a user.
  PRIMARY SORT: (pid, campaign_id, created_at, id)
  PUBLISHER ID: pid (String) — NOT user_id. Join to users with: i.pid = toString(u.id)
  KEY COLS: session_id (join key), campaign_id, offer_id, position (carousel slot 1/2/3)

adpx_tracked_clicks  [17M rows]
  One row per click on an offer in the carousel.
  PRIMARY SORT: (user_id, campaign_id, created_at, id)
  KEY COLS: session_id (join key), campaign_id, offer_id,
    position (Int32 — which carousel slot was clicked, 1/2/3),
    is_converted (Bool — did this click result in a conversion?),
    pub_cost_cents (UInt64 — cost to publisher),
    os, device, browser, user_agent, fingerprint,
    is_offerwall (Bool), is_mou (Bool), is_embedded (Bool)

adpx_conversionsdetails  [1.7M rows]
  One row per conversion event.
  PRIMARY SORT: (user_id, campaign_id, created_at, id)
  CRITICAL: revenue and payout are stored as STRINGS — always cast: toFloat64OrNull(revenue)
  KEY COLS: session_id (join key), campaign_id, offer_id,
    revenue (String → cast), payout (String → cast)
  DOWNSTREAM LAG: Add 14-day window beyond session date range when querying conversions.

adpx_system_activity_logs  [115K rows]
  Audit trail for every dashboard change (campaigns paused/resumed, budgets changed, etc.)
  KEY COLS: entity (what was changed), type (change type), admin_id,
    old_data (JSON String — state before), new_data (JSON String — state after),
    user_type, user_role, created_at
  USE FOR: "is X paused?", "when was X changed?", "who made this change?"

── CONFIGURATION TABLES (synced from main app DB via Airbyte) ─────────────

from_airbyte_campaigns  [4.75K rows]
  Master offer/campaign table. One row per campaign.
  JOIN: toInt64(campaign_id) = c.id  (campaign_id in event tables is UInt64; c.id is Int64)
  KEY COLS: id, adv_name, title, status, categories, end_date, start_date,
    capping_config (JSON: {"month": {"budget": N}} — monthly revenue cap),
    pacing_config (JSON — daily pacing rules),
    schedule_days (JSON — day-of-week serving schedule),
    geo_whitelist, geo_blacklist, platforms (iOS/Android/Web), os, browsers,
    is_offerwall_only, offerwall_enabled, perkswallet_enabled,
    network_id (FK to from_airbyte_networks), internal_network_name (Impact offer ID),
    max_impressions, max_positive_cta, conversion_events,
    force_priority_till (Date — force priority until this date),
    open_to_marketplace (Bool), is_incent (Bool), is_rewarded (Bool),
    is_direct_sold (Bool), is_citrusad (Bool), is_rich_media (Bool),
    landing_url, useraction_url, useraction_cta,
    adv_description, offer_description, mini_description, terms_and_conditions,
    loyaltyboost_requirements, internal_notes, owner_id, advertiser_id, partner_id,
    deleted_at (NULL = active, non-NULL = deleted/archived)

from_airbyte_publisher_campaigns  [96K rows]
  Publisher-campaign associations. One row per publisher×campaign pairing.
  This is the operational table — controls how each offer runs on each publisher.
  KEY COLS: id, campaign_id (FK to campaigns), user_id (FK to users/publishers),
    is_active (Bool — whether the offer is currently serving on this publisher),
    payout (Int64 cents — publisher-specific payout override; NULL = use campaign default),
    priority (Int64 — higher = more impressions),
    multiplier (Decimal — RPM multiplier),
    force_priority (Bool), max_impressions, max_positive_cta,
    capping_config (JSON — publisher-level monthly budget cap),
    pacing_config, schedule_days, geo_whitelist, geo_blacklist,
    platforms, os, browsers, categories, goals,
    conversion_events, is_offerwall_only,
    stats_by_position (JSON — pre-computed performance stats by carousel slot),
    useraction_cta, useraction_url, deleted_at, updated_at

from_airbyte_users  [5.45K rows]
  Publisher/user registry. id = publisher_id (matches user_id in event tables).
  KEY COLS: id (UInt64 publisher ID), organization (publisher name), is_test

from_airbyte_networks  [177 rows]
  Affiliate network registry (Impact, CJ, MaxBounty, FlexOffers, etc.)
  KEY COLS: id, name, slug, postback_url, parameters, user_id (who owns it)

from_airbyte_placements  [160 rows]
  Publisher placement registry. A placement = named location where offers appear.
  KEY COLS: id, user_id (publisher_id), slug (e.g. "fuel_hub", "transaction_receipt"),
    display_name (human-readable), is_default, is_auto_generated

from_airbyte_campaign_serving_groups  [255 rows]
  Named groups of campaigns that share caps/schedules.
  KEY COLS: id, name, is_active, is_test, capping_config, pacing_config, schedule_days, exclude_group

from_airbyte_grouped_campaign_specs  [1.33K rows]
  Maps campaigns to serving groups. group_id → campaign_id.
  KEY COLS: id, group_id (FK to serving_groups), campaign_id (FK to campaigns)

from_airbyte_placement_sequence_rules  [200 rows]
  Controls offer ordering within a placement.
  KEY COLS: id, placement_id, sequence_rule_id, weight, is_active, user_id

from_airbyte_partner_categories  [111 rows]
  Publisher classification/taxonomy.
  KEY COLS: id, user_id (publisher_id), tier, approval, traffic_type,
    integration_type, custom_creatives

from_airbyte_publisher_delivery_channel_settings  [33 rows]
  Delivery channel config per publisher (e.g. web vs app vs offerwall channel).
  KEY COLS: id, user_id (publisher_id), channel_name, weight, enabled, enable_force_priority

from_airbyte_publisher_nexos_settings  [29 rows]
  Nexos feature flag per publisher.
  KEY COLS: id, user_id (publisher_id), is_enabled, enabled_percentage

from_airbyte_user_selected_perks  [3.14K rows]
  Perkswall offer selections — when a loyalty member actively picks a perk.
  This is NOT a conversion — it's pre-conversion intent/engagement.
  KEY COLS: id, user_id (publisher_id), campaign_id, session_id,
    pub_user_id (loyalty member ID), metadata (JSON), created_at

from_airbyte_custom_reports  [742 rows]
  Saved report definitions.
  KEY COLS: id, report_name, report_type, publisher_id, admin_id,
    metrics, attributes, range, offer_units, selected_campaigns, selected_publishers

from_airbyte_custom_report_runs  [2.21K rows]
  Report execution history (when reports were run, results).

from_airbyte_publisher_campaign_images  [1.03K rows]
  Images attached to publisher campaign creatives.

from_airbyte_perkswall_themes  [1.43K rows]
  Perkswall visual theme configurations per publisher.

from_airbyte_placement_themes  [227 rows]
  Visual theme configs per placement.

mv_adpx_campaigns  (materialized view)
  Lightweight: id, internal_name, is_test. Use for campaign name resolution.

mv_adpx_users  (materialized view)
  Lightweight: id, organization, is_test, parent_id. Use for publisher name resolution.

── CRITICAL QUERY RULES ───────────────────────────────────────────────────

JOIN KEYS:
  session_id (String) links: adpx_sdk_sessions ↔ adpx_impressions_details ↔ adpx_tracked_clicks ↔ adpx_conversionsdetails
  user_id (UInt64) links: adpx_sdk_sessions / adpx_tracked_clicks / adpx_conversionsdetails → from_airbyte_users
  pid (String) links: adpx_impressions_details → from_airbyte_users via: pid = toString(user_id)
  campaign_id: event tables (UInt64) → from_airbyte_campaigns (Int64) via: toInt64(campaign_id) = c.id

PREWHERE (always use for primary sort key + partition):
  adpx_sdk_sessions:        PREWHERE user_id = X AND toYYYYMM(created_at) >= YYYYMM
  adpx_tracked_clicks:      PREWHERE user_id = X AND toYYYYMM(created_at) >= YYYYMM
  adpx_conversionsdetails:  PREWHERE user_id = X AND toYYYYMM(created_at) >= YYYYMM
  adpx_impressions_details: PREWHERE pid = 'X' AND toYYYYMM(created_at) >= YYYYMM

TYPE CASTING:
  revenue, payout in conversionsdetails → toFloat64OrNull(revenue)
  campaign_id joins to airbyte tables → toInt64(campaign_id) = c.id
  pid (String) → publisher user_id → i.pid = toString(u.id)

DOWNSTREAM LAG: Conversions can arrive 14 days after a session. When joining sessions to conversions, extend the conversion date window by +14 days beyond the session end date.

CAPPING CONFIG: JSON column on from_airbyte_campaigns and from_airbyte_publisher_campaigns.
  Extract monthly cap: JSONExtractFloat(capping_config, 'month', 'budget')
  Or: json_parsed['month']['budget'] after parsing in Python.

TIMEZONE: All timestamps stored in UTC. For reporting, use timezone 'America/Chicago'.

── WHAT EACH TABLE ANSWERS ────────────────────────────────────────────────

"How is publisher X performing?"     → adpx_sdk_sessions + adpx_impressions_details + adpx_tracked_clicks + adpx_conversionsdetails
"Is offer X paused on publisher Y?"  → from_airbyte_publisher_campaigns (is_active)
"When was offer X paused?"           → adpx_system_activity_logs (old_data/new_data JSON diff)
"What's the monthly budget cap?"     → from_airbyte_campaigns.capping_config or from_airbyte_publisher_campaigns.capping_config
"Which carousel slot gets clicked?"  → adpx_tracked_clicks.position
"Which perks do loyalty members pick?" → from_airbyte_user_selected_perks
"What network is this offer on?"     → from_airbyte_campaigns.network_id → from_airbyte_networks.name
"What's the publisher-specific payout?" → from_airbyte_publisher_campaigns.payout (in cents)
"What placements does publisher X have?" → from_airbyte_placements WHERE user_id = X
"Is this offer offerwall-only?"      → from_airbyte_campaigns.is_offerwall_only
"What's the day-of-week schedule?"   → from_airbyte_publisher_campaigns.schedule_days (JSON)
"Which campaigns are in a serving group?" → from_airbyte_campaign_serving_groups + from_airbyte_grouped_campaign_specs
"""


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

        # Step 3: provisioned offers — what's assigned to this publisher account.
        # from_airbyte_publisher_campaigns is the source of truth for "what's set up."
        # Impressions tell us which of those are actively serving right now.
        prov_rows = ch.query(f"""
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
        """).result_rows

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

    # ── Step 1: 30-day baseline — impressions + revenue per publisher ─────────
    baseline_rows = []
    try:
        result = ch.query(
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
        )
        baseline_rows = result.result_rows
    except Exception as e:
        log.warning(f"get_advertiser_revenue_projection baseline failed: {e}")
        return {"error": str(e), "advertiser": advertiser_name, "month": month_label}

    if not baseline_rows:
        return {
            "advertiser": advertiser_name,
            "month": month_label,
            "error": f"No impression data for '{advertiser_name}' in the last 30 days. Check spelling or try a partial name.",
        }

    # ── Step 2: Campaign end dates + monthly caps ─────────────────────────────
    cap_warnings       = []
    end_date_warnings  = []
    monthly_cap_total  = None

    try:
        cap_result = ch.query(
            """
            SELECT id, adv_name, end_date, capping_config
            FROM from_airbyte_campaigns
            WHERE adv_name ILIKE %(adv)s
              AND deleted_at IS NULL
              AND (end_date IS NULL OR end_date >= %(month_start)s)
            """,
            parameters={"adv": f"%{advertiser_name}%", "month_start": str(month_start)},
        )
        for row in cap_result.result_rows:
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
    except Exception as e:
        log.warning(f"get_advertiser_revenue_projection cap query failed: {e}")

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
                "SELECT id, organization FROM from_airbyte_users WHERE organization ILIKE {name: String} LIMIT 5",
                parameters={"name": f"%{publisher_name}%"},
            ).result_rows
            if not rows:
                return {"error": f"No publisher found matching '{publisher_name}'"}
            pid = int(rows[0][0])
            pub_name = rows[0][1]
        elif pid is not None:
            rows = ch.query(
                "SELECT id, organization FROM from_airbyte_users WHERE id = {pid: UInt64} LIMIT 1",
                parameters={"pid": int(pid)},
            ).result_rows
            pub_name = rows[0][1] if rows else f"Partner {pid}"

        if pid is None:
            return {"error": "Must provide publisher_name or publisher_id"}

        # ── Fetch placement display names ─────────────────────────────────────
        placement_names = {}
        try:
            pn_rows = ch.query(
                "SELECT slug, display_name FROM from_airbyte_placements WHERE user_id = {pid: Int64}",
                parameters={"pid": int(pid)},
            ).result_rows
            for slug, display_name in pn_rows:
                if display_name:
                    placement_names[slug] = display_name
        except Exception:
            pass  # non-fatal — use slugs as-is if table unavailable

        # ── Partition filter ──────────────────────────────────────────────────
        from datetime import date
        today = date.today()
        # partition for sessions (go back days + a little buffer)
        partition = int(today.strftime("%Y%m")) - (1 if today.day <= days else 0)
        extended_partition = partition - 1  # extra month for downstream lag

        # ── Query 1: session volume by placement + OS ─────────────────────────
        state_clause = "AND state ILIKE {geo_state: String}" if geo_state else ""
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
        q1_rows = ch.query(q1, parameters=params1).result_rows

        # ── Query 2: ad metrics by placement ─────────────────────────────────
        # Scan impressions LEFT (filtered by pid+date), sessions RIGHT (hash table).
        # Putting impressions on the right OOMs on large publishers (FillingRightJoinSide).
        state_clause_inner = f"AND state ILIKE {{geo_state: String}}" if geo_state else ""
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
        q2_rows = ch.query(q2, parameters=params2).result_rows

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
        q3_rows = ch.query(q3, parameters=params3).result_rows

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

        # ── Query 1: current status from publisher_campaigns ──────────────────
        q1_rows = []
        try:
            q1_rows = ch.query(
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
        except Exception as e:
            log.warning(f"get_campaign_status q1 failed: {e}")

        # ── Query 2: recent audit log entries ─────────────────────────────────
        q2_rows = []
        try:
            q2_rows = ch.query(
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
        except Exception as e:
            log.warning(f"get_campaign_status q2 failed: {e}")

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
                "SELECT id, organization FROM from_airbyte_users WHERE organization ILIKE {name: String} LIMIT 5",
                parameters={"name": f"%{publisher_name}%"},
            ).result_rows
            if not rows:
                return {"error": f"No publisher found matching '{publisher_name}'"}
            pid = int(rows[0][0])
            pub_name = rows[0][1]
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
        # Convert rows to list of dicts for readability
        if col_names:
            rows_as_dicts = [dict(zip(col_names, row)) for row in rows[:max_rows]]
        else:
            rows_as_dicts = [list(row) for row in rows[:max_rows]]

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
    "run_sql_query": run_sql_query,
    "get_scout_status": get_scout_status,
    "get_advertiser_revenue_projection": get_advertiser_revenue_projection,
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
    # Prepend team corrections as grounding context for this query
    corrections_ctx = _get_corrections_context()
    effective_message = (corrections_ctx + user_message) if corrections_ctx else user_message
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
                    model="claude-sonnet-4-6",
                    max_tokens=2048,
                    system=SYSTEM_PROMPT,
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
