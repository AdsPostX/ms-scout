from __future__ import annotations

# standard library
import json
import logging
import os
import pathlib
import re
import urllib.parse
import urllib.request
from collections import Counter

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

# ── Environment config — read once at import, warn if required vars absent ────
_DEMAND_FEED_URL = os.getenv("DEMAND_FEED_URL", "")
_NOTION_TOKEN    = os.getenv("NOTION_TOKEN", "")
_NOTION_DB_ID    = os.getenv("NOTION_QUEUE_DB_ID", "")

if not _NOTION_TOKEN or not _NOTION_DB_ID:
    log.warning("[scout_tools_offers] NOTION_TOKEN/NOTION_QUEUE_DB_ID not set — pipeline health unavailable")

SNAPSHOT_PATH = pathlib.Path(__file__).parent / "data" / "offers_latest.json"

SUPPORTED_NETWORKS: tuple[str, ...] = (
    "Impact", "FlexOffers", "MaxBounty", "CJ",
)

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


_TRACKING_DOMAINS = {
    # Known affiliate tracking domains — URLs on these are real tracking links
    "impact.com", "sjv.io", "pxf.io", "bn5x.net", "ibfwsl.net",
    "maxbounty.com", "flexoffers.com", "jdoqocy.com", "tkqlhce.com",
    "launchingdeals.com", "adspostx.com", "pubtailer.com",
    "collectsavings.com", "referral.", "go.",
}

_CLICK_ID_PATTERNS = ("{click_id}", "{subid}", "subId", "clickid", "click_id", "aff_id")


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


def _load_offers() -> list:
    """Load offers from DEMAND_FEED_URL when set; fall back to disk snapshot.

    Logs source + offer count so the P1.2 cutover is verifiable from Render
    logs without grepping for stale-file timestamps.
    """
    url = _DEMAND_FEED_URL
    if url:
        endpoint = f"{url.rstrip('/')}/offers"
        # Strip userinfo/query/fragment before logging — endpoint may contain
        # credentials or tokens that must not leak to Render logs.
        _p = urllib.parse.urlparse(endpoint)
        _netloc = _p.hostname or ""
        if _p.port:
            _netloc = f"{_netloc}:{_p.port}"
        safe_endpoint = urllib.parse.urlunparse((_p.scheme, _netloc, _p.path, "", "", ""))
        # Reject non-http(s) schemes — urlopen() otherwise accepts file://,
        # ftp://, etc., which would let a misconfigured env var read local
        # files or hit arbitrary hosts.
        if _p.scheme not in ("http", "https"):
            log.warning(f"[scout_agent] DEMAND_FEED_URL has unsupported scheme {_p.scheme!r}; falling back to disk snapshot at {SNAPSHOT_PATH}")
        else:
            try:
                with urllib.request.urlopen(endpoint, timeout=10) as resp:
                    offers = json.loads(resp.read())
                log.info(f"[scout_agent] loaded {len(offers)} offers from {safe_endpoint}")
                return offers
            except Exception as exc:
                log.warning(f"[scout_agent] DEMAND_FEED_URL fetch failed ({type(exc).__name__}: {exc}); falling back to disk snapshot at {SNAPSHOT_PATH}")
    if not SNAPSHOT_PATH.exists():
        log.warning(f"[scout_agent] no offers source available — DEMAND_FEED_URL unset and {SNAPSHOT_PATH} missing")
        return []
    with open(SNAPSHOT_PATH) as f:
        offers = json.load(f)
    log.info(f"[scout_agent] loaded {len(offers)} offers from disk snapshot {SNAPSHOT_PATH}")
    return offers


def _norm(s) -> str:
    return str(s or "").strip().lower()


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
    benchmarks = _tm.benchmarks()
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


def get_top_opportunities(category: str = None, geo: str = None, limit: int = 5) -> list:
    offers = _load_offers()
    benchmarks = _tm.benchmarks()
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
    return _format_offers(_dedupe_by_advertiser(results)[:limit], benchmarks)


def get_running_offers(category: str = None) -> list:
    offers = _load_offers()
    benchmarks = _tm.benchmarks()
    results = [
        o for o in offers
        if o.get("_ms_status") == "Live"
        and (not category or _norm(category) in _norm(o.get("category", "")))
    ]
    results.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    return _format_offers(results, benchmarks)


def get_category_performance(category: str = None) -> dict:
    benchmarks = _tm.benchmarks()
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
        "finding": f"{len(cat_data)} {'category' if len(cat_data) == 1 else 'categories'} benchmarked",
        "category_benchmarks": cat_data,
        "note": "CVR and RPM are real MS performance data from ClickHouse (Jan 2025+). Use RPM to estimate expected value of new offers: RPM = payout × (CVR/100) × 1000.",
        "top_performing_offers_by_rpm": [
            {"impact_id": k, **v} for k, v in top_offers
        ],
    }


def get_offer_stats() -> dict:
    offers = _load_offers()
    benchmarks = _tm.benchmarks()
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
        "finding": f"{len(offers)} offers, {len(by_network)} networks",
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
    benchmarks = _tm.benchmarks()
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
        "finding": f"{offer_name}: {len(same_brand[:limit])} brand alt(s), {len(cat_subs[:limit])} category alt(s)",
        "primary_offer": offer_name,
        "primary_network": primary_network,
        "primary_category": inferred_category,
        "same_brand_alts": _format_offers(same_brand[:limit], benchmarks),
        "category_alts": _format_offers(cat_subs[:limit], benchmarks),
        "note": "same_brand_alts = same advertiser on a different network (plug-and-play swap). category_alts = next best in vertical if brand unavailable on any network.",
    }


def run_offer_scraper() -> str:
    """
    Trigger an immediate offer inventory refresh from affiliate networks
    (see SUPPORTED_NETWORKS at top of scout_agent.py for the canonical list).
    Run when offer inventory is empty or stale.
    Takes ~2 minutes. Writes data/offers_latest.json and posts digest.
    """
    import scout_bot as _sb
    _running = getattr(_sb, "_SCRAPER_RUNNING", None)
    if _running is not None and _running.is_set():
        return (
            ":hourglass_flowing_sand: Scraper is already running. "
            "Check back in a few minutes — offer inventory will be fresh when it completes."
        )
    try:
        from offer_scraper import run_headless
        log.info("[scraper] on-demand refresh triggered via @Scout")
        run_headless()
        # Report results
        if SNAPSHOT_PATH.exists():
            import json as _json
            from collections import Counter
            offers = _json.loads(SNAPSHOT_PATH.read_text())
            active = sum(1 for o in offers if o.get("status") == "Active")
            networks = Counter((o.get("network") or "unknown").lower() for o in offers)
            net_str = "  ·  ".join(f"{k}: {v}" for k, v in sorted(networks.items(), key=lambda x: -x[1]))

            try:
                import scout_digest as _sd
                if _sd._SCOUT_ENV != "production":
                    digest_note = f"\n:warning: Digest routing to #scout-qa (SCOUT_ENV={_sd._SCOUT_ENV!r} — set SCOUT_ENV=production on Render)"
                else:
                    digest_note = f"\n:newspaper: Digest posts to #scout-offers if new offers detected."
            except Exception as _e:
                log.warning(f"[scraper] scout_digest unavailable for digest routing note: {_e}")
                digest_note = ""

            return (
                f":white_check_mark: Offer inventory refreshed — {len(offers)} total offers, "
                f"{active} active.\n{net_str}{digest_note}"
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
    import requests as _req, json as _json
    from datetime import datetime, timezone, timedelta

    notion_token = _NOTION_TOKEN
    db_id = _NOTION_DB_ID
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
