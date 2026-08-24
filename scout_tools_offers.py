from __future__ import annotations

# standard library
import logging
import re  # noqa: F401 — currently unused; pre-existing, left as-is (out of scope for this cleanup)

# local — NO scout_agent (circular import)
from scout_ch import _get_ch_client, CHBusyError  # noqa: F401
import queries as _q
from scout_thresholds import _manager as _tm
from scout_types import FormattedOffer, Brief  # type: ignore[import]  # noqa: F401
from scout_images import (
    _clearbit_domain, _google_favicon, _app_store_icon,
    _validate_image_url, _cached_external_images, _store_image_cache, _ms_cdn_image,
)

log = logging.getLogger("scout_agent")

# ── Risk patterns ─────────────────────────────────────────────────────────────
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


def _fuzzy_adv_match(name: str, existing: set) -> bool:
    """True if `name` fuzzy-matches any entry in `existing` (substring in either direction)."""
    return any(name == ex or name in ex or ex in name for ex in existing)


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
    payout_type  = _norm(offer.get("_payout_type_norm", ""))
    category     = (offer.get("category") or "").strip()
    adv_name     = _norm(offer.get("advertiser", ""))

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

    # ── Tier 4: overall MS CVR baseline — covers all non-MS-run networks ─────
    # MaxBounty / FlexOffers / ShareASale / Rakuten / Awin advertisers have no
    # offer/advertiser/category match. Use the aggregate CVR across 670+ MS campaigns
    # as a low-confidence prior so they rank against each other (and below real-data offers).
    elif benchmarks.get("by_payout_type", {}).get("_all"):
        base = benchmarks["by_payout_type"]["_all"]
        cvr  = base["cvr_pct"] / 100
        confidence = 0.35
        source = f"MS overall baseline ({base['campaigns']} campaigns, low confidence)"

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


def _norm(s) -> str:
    return str(s or "").strip().lower()


# ── Tool implementations ─────────────────────────────────────────────────────

def _dedupe_by_advertiser(offers: list) -> list:
    """One offer per advertiser, preserving input order (callers pass a
    score-sorted list, so the first occurrence is that advertiser's best).
    Networks list the same program under many offer IDs — 2026-07-09:
    'top opportunities' returned 8 identical AT&T Business cards, one per
    CJ offer ID. Offers with no advertiser name are kept as-is."""
    seen: set = set()
    deduped = []
    for o in offers:
        key = _norm(o.get("advertiser") or "")
        if key:
            if key in seen:
                continue
            seen.add(key)
        deduped.append(o)
    return deduped


def _format_offers(offers: list, benchmarks: dict) -> list[FormattedOffer]:
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

        advertiser = o.get("advertiser", "")
        out.append({
            "advertiser": advertiser,
            "network": o.get("network", ""),
            "offer_id": offer_id,
            "payout": o.get("_raw_payout") or o.get("payout") or "Rate TBD",
            "payout_num": o.get("_payout_num"),
            "payout_type": o.get("_payout_type_norm") or o.get("payout_type", ""),
            "category": category,
            "geo": o.get("geo", ""),
            "ms_status": o.get("_ms_status", ""),
            "ms_internal_name": o.get("_ms_internal_name", ""),
            "fit_tier": o.get("fit_tier", "STANDARD"),
            "last_verified": o.get("last_verified"),
            "scout_score_rpm": score,
            "performance_context": perf_note,
            "risk_flag": _get_risk_flag(advertiser, category, o.get("description", "")),
            "icon_url":  o.get("icon_url", ""),
            "hero_url":  o.get("hero_url", ""),
        })
    return out


def _get_risk_flag(advertiser: str, category: str, description: str) -> str:
    """Return a one-line risk warning if the offer is a poor post-transaction fit."""
    combined = f"{advertiser} {category} {description}".lower()
    for keywords, flag in _RISK_PATTERNS:
        if any(kw in combined for kw in keywords):
            return flag
    return ""


