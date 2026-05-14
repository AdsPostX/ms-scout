"""
scout_digest.py
Scout Signal — Weekly Offer Digest

Selects top candidate offers from the scraper output, filters duplicates,
scores by payout + category fit, and posts a curated Slack digest with
approve/reject buttons that feed directly into the Demand Queue workflow.

Usage:
  python scout_digest.py              # post this week's digest
  python scout_digest.py --dry-run    # print blocks without posting
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import pathlib
import re
import urllib.request
from datetime import date, datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()  # plist env vars (SCOUT_ENV, SCOUT_DIGEST_CHANNEL, etc.) take precedence over .env

log = logging.getLogger("scout_digest")

# ── Paths ──────────────────────────────────────────────────────────────────────
_DIR         = pathlib.Path(__file__).parent
DATA_DIR     = _DIR / "data"
OFFERS_FILE          = DATA_DIR / "offers_latest.json"
OFFERS_PREVIOUS_FILE = DATA_DIR / "offers_previous.json"
PAYOUT_CACHE = DATA_DIR / "payout_cache.json"
STATE_FILE   = DATA_DIR / "digest_state.json"

# ── Slack ──────────────────────────────────────────────────────────────────────
_SCOUT_QA_CHANNEL    = "C0AQEECF800"  # #bot-qa (renamed from #scout-qa) — always used in dev/force
_SCOUT_ENV           = os.getenv("SCOUT_ENV", "development")
_PROD_OFFERS_CHANNEL = os.getenv("SCOUT_DIGEST_CHANNEL", _SCOUT_QA_CHANNEL)  # #scout-offers

if _SCOUT_ENV != "production":
    log.warning(
        "[digest] SCOUT_ENV=%r — all digests routing to #scout-qa. "
        "Set SCOUT_ENV=production and SCOUT_DIGEST_CHANNEL=<channel_id> on Render "
        "to send digests to #scout-offers.", _SCOUT_ENV
    )

def _digest_channel(force: bool = False) -> str:
    """Return the correct Slack channel for the offer digest.
    Force=True OR non-production environment → always #scout-qa.
    """
    if force or _SCOUT_ENV != "production":
        return _SCOUT_QA_CHANNEL
    return _PROD_OFFERS_CHANNEL

_QUEUE_DB_ID   = os.getenv("NOTION_QUEUE_DB_ID", "")
QUEUE_LIST_URL = f"https://www.notion.so/{_QUEUE_DB_ID}" if _QUEUE_DB_ID else "https://www.notion.so/"

# Stop-words for fuzzy name matching
_STOP_WORDS = {"the", "and", "for", "inc", "llc", "corp", "ltd", "co", "via"}

# ── Post-transaction context fit ───────────────────────────────────────────────
# MS shows offers at the moment a user completes a transaction — high-intent,
# low-friction offers work. B2B tools, legal services, and complex purchases don't.
# These multipliers encode that business model knowledge directly into ranking.

# Conversion complexity: how hard is it to convert from a post-transaction moment?
# CPL (email/signup) is structurally easiest. CPS requires another purchase — hardest.
_CONVERSION_COMPLEXITY: dict[str, float] = {
    "CPL":         1.40,   # email/signup — lowest friction, best post-transaction fit
    "MOBILE_APP":  1.30,   # one-tap install
    "APP_INSTALL": 1.30,
    "CPA":         0.85,   # action varies, treat conservatively
    "CPS":         0.40,   # requires another purchase — hardest post-transaction conversion
                           # user just bought something; asking them to buy again is high friction
}

# MaxBounty/FlexOffers use raw strings ("$ per Lead", "% of Sale") instead of codes.
# Normalize to canonical types before applying conversion complexity multipliers.
_PAYOUT_TYPE_NORM: dict[str, str] = {
    "$ per lead":   "CPL",
    "per lead":     "CPL",
    "cpl":          "CPL",
    "% of sale":    "CPS",
    "$ per sale":   "CPS",
    "cps":          "CPS",
    "fixed":        "CPA",
    "cpa":          "CPA",
    "mobile_app":   "MOBILE_APP",
    "app_install":  "APP_INSTALL",
}

# Network priority (Big 4 first; established team mental model — Impact at top).
# Anything not in this tuple is appended alphabetically at module load time
# from the offers_latest.json keyset, so a 10th network requires no code change.
_PRIORITY_NETWORKS: tuple[str, ...] = ("impact", "maxbounty", "flexoffers", "cj")

# Cold-start fallback (offers_latest.json missing or empty on fresh deploy).
# PR 18: trimmed from 9 → 4 active networks (ShareASale, Rakuten, AWIN, Tune,
# Everflow currently no-op without API credentials on Render). When creds land,
# append here AND to SUPPORTED_NETWORKS at the top of scout_agent.py.
# The _NETWORK_LABEL and _NETWORK_EMOJI maps below intentionally KEEP all 9
# entries so re-enabling a network is a one-line change.
_DIGEST_NETWORKS_FALLBACK: tuple[str, ...] = (
    "impact", "maxbounty", "flexoffers", "cj",
)


def _get_active_networks() -> tuple[str, ...]:
    """
    Derive the network iteration order from offers_latest.json at module load.

    Returns Big 4 in priority order (only those actually present in inventory),
    followed by any other live networks alphabetically. New networks added to
    the scraper appear here automatically on next Render restart — no PR required.

    Returns _DIGEST_NETWORKS_FALLBACK on cold start (file missing) or any error.
    """
    try:
        if not OFFERS_FILE.exists():
            return _DIGEST_NETWORKS_FALLBACK
        offers = json.loads(OFFERS_FILE.read_text())
        if not offers:
            return _DIGEST_NETWORKS_FALLBACK
        live: set[str] = set()
        for o in offers:
            net = (o.get("network") or "").lower().strip()
            if net:
                live.add(net)
        if not live:
            return _DIGEST_NETWORKS_FALLBACK
        priority = tuple(n for n in _PRIORITY_NETWORKS if n in live)
        rest = tuple(sorted(live - set(_PRIORITY_NETWORKS)))
        return priority + rest
    except Exception as e:
        log.warning(f"[digest] _get_active_networks() failed, using fallback: {e}")
        return _DIGEST_NETWORKS_FALLBACK


# Module-load-once: scraper refreshes won't change this until next Render restart.
# Acceptable because new networks ship rarely and Render restarts daily on deploy.
_DIGEST_NETWORKS: tuple[str, ...] = _get_active_networks()

_NETWORK_LABEL: dict[str, str] = {
    "impact":     "Impact",
    "maxbounty":  "MaxBounty",
    "flexoffers": "FlexOffers",
    "cj":         "Commission Junction",
    "awin":       "Awin",
    "everflow":   "Everflow",
    "rakuten":    "Rakuten",
    "shareasale": "ShareASale",
    "tune":       "Tune",
}

# Module-level emoji map — never shadow inside _build_digest_blocks().
_NETWORK_EMOJI: dict[str, str] = {
    "impact":     "⚡",
    "maxbounty":  "💰",
    "flexoffers": "🔗",
    "cj":         "🌐",
    "awin":       "🟦",
    "everflow":   "🌊",
    "rakuten":    "🔴",
    "shareasale": "🤝",
    "tune":       "🎯",
}

# Human-readable display labels for payout types shown in the card right column
_PAYOUT_TYPE_DISPLAY: dict[str, str] = {
    "CPL":         "CPL",
    "CPS":         "CPS",
    "CPA":         "CPA",
    "MOBILE_APP":  "Mobile App",
    "APP_INSTALL": "App Install",
}

# Geo values that are actually category/fallback strings — suppress these
_NON_GEO_VALUES = {"other", "uncategorized", "n/a", "na", "none", "unknown", ""}

# MaxBounty appends internal campaign metadata to offer names:
# "ReadyRx - GLP-1 - CPS (US)" → "ReadyRx"
# "PickALender - Loans up to $40k - RevShare (US)" → "PickALender"
# Strip everything after " - " where what follows looks like a campaign suffix.
_MB_NAME_SUFFIX = re.compile(r"\s*-\s+.+$")


def _normalize_payout_type(raw: str) -> str:
    """Map raw network payout type strings to canonical scoring keys."""
    return _PAYOUT_TYPE_NORM.get(raw.lower().strip(), raw.upper())


def _display_payout_type(ptype: str) -> str:
    """Human-readable label for payout type shown in the card."""
    return _PAYOUT_TYPE_DISPLAY.get(ptype, ptype)


def _clean_advertiser_name(name: str, network: str) -> str:
    """
    Return a clean advertiser name suitable for the card header.
    MaxBounty uses internal naming like "Brand - Campaign Desc - TYPE (GEO)".
    Strip everything after the first " - " for MaxBounty offers.
    """
    if network == "maxbounty" and " - " in name:
        return _MB_NAME_SUFFIX.sub("", name).strip()
    return name


def _format_payout(payout_num: float, ptype: str) -> str:
    """
    Format payout for the card right column.
    Use whole-dollar amounts where payout is a round number.
    """
    display_type = _display_payout_type(ptype)
    if payout_num == int(payout_num):
        amount = f"${int(payout_num):,}"
    else:
        amount = f"${payout_num:,.2f}"
    return f"{amount} {display_type}".strip() if display_type else amount

def _context_fit(offer: dict) -> float:
    """
    Post-transaction context fit multiplier (0.1 – 1.8).

    Reads advertiser name + description to determine whether this offer makes
    sense to show someone who just completed a purchase. Encodes MS's core
    business model as a scoring signal — not a blocklist, but a spectrum.

    Hard disqualifiers (0.1–0.2):
      B2B tools, legal services, crypto — structurally wrong audience/context.

    Strong positive (1.4–1.8):
      Endemic to post-purchase — rewards for shopping, scan-to-earn, cashback.

    Consumer fit (1.1–1.3):
      Delivery, wellness, pet, fintech apps, streaming — relevant and low-friction.

    Default (1.0): no strong signal either way.
    """
    text = " ".join([
        offer.get("advertiser") or "",
        offer.get("description") or "",
        offer.get("category") or "",
    ]).lower()

    # ── Hard disqualifiers (genuinely incompatible — not just wrong context) ──
    # These aren't "wrong for current publisher mix" — they're categorically
    # incompatible with MomentScience's product and publisher agreements.
    # Multiplier is 0.02: accurate modeling that conversion approaches zero,
    # not an arbitrary penalty. A $25 CPL at 0.02 scores ~2 and naturally
    # falls off the bottom without needing a categorical rule.
    #
    # B2B tools — wrong audience, near-zero consumer conversion rate
    if any(s in text for s in [
        "b2b", "employers", "staffing", "enterprise teams", "helps organizations",
        "visual collaboration", "sync licensing", "small business insurance",
        "background screening", "background check",
    ]):
        return 0.02

    # Legal services — wrong context, publisher brand risk
    if any(s in text for s in ["prenup", "prenuptial", "legal agreement", "attorney"]):
        return 0.02

    # Crypto self-custody — niche audience, regulatory exposure for publishers
    if any(s in text for s in ["bitcoin", "self-custody", "crypto wallet", "blockchain wallet"]):
        return 0.05

    # Adult/dating — brand safety risk for all publisher integrations
    if any(s in text for s in [
        "ashley madison", "meet bored", "lonely housewives", "adult dating",
        "extramarital", "affair", "hookup site", "hookup app",
    ]):
        return 0.01

    # ── Endemic to post-purchase (best possible fit) ──────────────────────────
    # Rewards for scanning purchases, cashback, loyalty — the user just bought
    # something and the offer is literally about that purchase moment.
    if any(s in text for s in [
        "scan", "rewarded for scan", "rewards for scan",   # NielsenIQ, scan-to-earn
        "cashback", "cash back",
        "earn reward", "get reward", "loyalty reward",
        "rewards when they shop", "rewards members",
    ]):
        return 1.80

    # ── Strong consumer fit ───────────────────────────────────────────────────
    consumer_signals = [
        "delivered to your door", "meal kit", "meal delivery", "food delivery",
        "dog ", "pet food", "pup ", "fresh meals",           # space avoids "dogmatic" etc.
        "weight loss", "glp-1", "glp1", "wellness", "health app",
        "cash advance", "fintech", "financial health",
        "streaming service", "sport streaming",
        "workout", "fitness",
        "haircare", "skincare", "beauty",                   # Prose etc.
        "meal plan", "chef", "recipe",
    ]
    hits = sum(1 for s in consumer_signals if s in text)
    if hits >= 2:
        return 1.30
    if hits == 1:
        return 1.15

    return 1.0


# ── State management ───────────────────────────────────────────────────────────

def load_state() -> dict:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
    except Exception as e:
        log.warning(f"Could not load digest state: {e}")
    return {"approved": {}, "rejected": {}}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def record_approval(offer_id: str, advertiser: str, payout: str, actioned_by: str):
    state = load_state()
    state.setdefault("approved", {})[str(offer_id)] = {
        "advertiser": advertiser,
        "payout_at_action": payout,
        "actioned_by": actioned_by,
        "actioned_at": datetime.utcnow().isoformat(),
    }
    save_state(state)


def record_rejection(offer_id: str, advertiser: str, payout: str, actioned_by: str):
    state = load_state()
    state.setdefault("rejected", {})[str(offer_id)] = {
        "advertiser": advertiser,
        "payout_at_action": payout,
        "actioned_by": actioned_by,
        "actioned_at": datetime.utcnow().isoformat(),
    }
    save_state(state)


# ── ClickHouse: active MS campaigns for deduplication ─────────────────────────

def get_active_ms_campaigns() -> list[dict]:
    """
    Returns all non-deleted MS platform campaigns as {id, adv_name, impact_id}.
    Used to skip offers already built in the platform.
    Gracefully returns [] if ClickHouse is unavailable.
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
        rows = ch.query("""
            SELECT id, adv_name, trim(internal_network_name) AS impact_id
            FROM default.from_airbyte_campaigns
            WHERE deleted_at IS NULL
              AND adv_name != ''
              AND adv_name NOT IN (
                'Test', 'supatest', 'MomentScience', 'Agilesh K Inc',
                'Shakha Test', 'New test offer agilesh 2602'
              )
        """).result_rows
        result = [
            {
                "id": str(r[0]),
                "adv_name": (r[1] or "").strip(),
                "impact_id": (str(r[2] or "")).split(" - ")[0].strip(),
            }
            for r in rows
            if r[1]
        ]
        log.info(f"Loaded {len(result)} active MS campaigns for deduplication")
        return result
    except Exception as e:
        log.warning(f"ClickHouse unavailable — deduplication skipped: {e}")
        return []


def _name_words(name: str) -> set[str]:
    return {
        w for w in re.findall(r"\b[a-z]{3,}\b", name.lower())
        if w not in _STOP_WORDS
    }


def is_already_in_ms(offer: dict, ms_campaigns: list[dict]) -> bool:
    """True if this offer already exists in the MS platform."""
    offer_id  = str(offer.get("offer_id", ""))
    offer_words = _name_words(offer.get("advertiser", ""))

    for camp in ms_campaigns:
        # Exact Impact ID match
        if offer_id and camp["impact_id"] and offer_id == camp["impact_id"]:
            return True
        # Fuzzy name match: at least one meaningful word overlap
        if offer_words & _name_words(camp["adv_name"]):
            return True
    return False


# ── Scoring ────────────────────────────────────────────────────────────────────
# Uses the same RPM-based Scout Score as the @Scout agent.
# One model, one truth: estimated_RPM = payout × predicted_CVR × 1000 × reliability.
# CVR is sourced from (in order): real MS ClickHouse data → category benchmark → payout-type baseline.

def score_offer(offer: dict, payout_cache: dict, state: dict, benchmarks: dict, force: bool = False) -> float | None:
    """
    Returns estimated RPM (Scout Score), or None to exclude.

    Exclusion:
    - Zero/missing payout
    - Already approved/queued  (skipped when force=True)
    - Rejected and payout hasn't improved ≥15%  (skipped when force=True)

    Ranking:
    - Primary: Scout Score RPM (payout × predicted_CVR × 1000) — same model as @Scout agent
    - Tiebreaker: has tracking URL (integration-ready)

    Confidence tiers for CVR (handled inside _scout_score):
    1. Real MS data for this exact offer (ClickHouse)
    2. Category benchmark (ClickHouse aggregate)
    3. Payout-type baseline estimate

    force=True bypasses approved/rejected state so test runs always surface real cards.
    """
    from scout_agent import _scout_score, SCOUT_THRESHOLDS

    offer_id = str(offer.get("offer_id", ""))

    if not force:
        # Already approved — don't resurface
        if offer_id in state.get("approved", {}):
            return None

    # Rejected — only resurface if payout improved ≥15%
    rejected = state.get("rejected", {})
    if not force and offer_id in rejected:
        payout_data = payout_cache.get(offer_id, {})
        try:
            current = _parse_payout(payout_data.get("payout"))
            old     = _parse_payout(rejected[offer_id].get("payout_at_action"))
        except (ValueError, TypeError):
            return None
        if old <= 0 or (current - old) / old < 0.15:
            return None

    # Build enriched offer: use payout_cache amount (Impact API, more accurate)
    # over scraper-normalised _payout_num where available.
    # MaxBounty/FlexOffers won't be in payout_cache — fall back to offer fields directly.
    payout_data = payout_cache.get(offer_id, {})
    cache_payout = _parse_payout(payout_data.get("payout"))

    enriched = dict(offer)
    if cache_payout > 0:
        enriched["_payout_num"] = cache_payout

    if not enriched.get("_payout_num"):
        return None

    rpm = _scout_score(enriched, benchmarks)
    if rpm <= 0:
        return None

    # Post-transaction context fit — encodes MS's business model.
    # Disqualifying multipliers (0.02–0.05) accurately model near-zero conversion
    # probability for B2B/legal/crypto offers in consumer post-transaction moments.
    # They naturally fall off the bottom of the ranking without categorical exclusion.
    # This keeps them queryable via @Scout if publisher context changes.
    rpm *= _context_fit(enriched)

    # Conversion complexity — CPL structurally easier than CPS post-transaction.
    # Normalize raw network type strings ("$ per Lead") to canonical keys ("CPL")
    # so MaxBounty/FlexOffers get the same multipliers as Impact.
    raw_payout_type = payout_data.get("payout_type", "") or offer.get("_payout_type_norm", "")
    payout_type = _normalize_payout_type(raw_payout_type)
    rpm *= _CONVERSION_COMPLEXITY.get(payout_type, 1.0)

    if rpm <= 0:
        return None

    # Minimum quality floor: offers scoring below this estimated RPM aren't worth
    # surfacing regardless of network inventory size. Default $20 effective score
    # filters weak long-tail offers (e.g. $12 CPA meal kits, $15 trading platforms).
    # PR 18: read from config/scout_thresholds.json so the team can tune without
    # a code change. Falls back to 20.0 if config or key is missing.
    _MIN_RPM = float(SCOUT_THRESHOLDS.get("digest", {}).get("min_rpm_floor", 20.0))
    if rpm < _MIN_RPM:
        return None

    # Tiny tiebreaker: integration-ready offers preferred
    if offer.get("tracking_url"):
        rpm += 0.001

    return round(rpm, 6)


# ── "Why this offer" — RPM-first, colleague voice ─────────────────────────────

def build_why_text(offer: dict, payout_cache: dict, ms_campaigns: list[dict], benchmarks: dict, adjusted_rpm: float | None = None) -> str:
    """
    Lead with the revenue signal (RPM), qualify the confidence level, add the inventory gap.
    Three tiers of confidence, each with honest framing:
      1. Real MS data  → "We've run this — $X RPM, Y% CVR"
      2. Category bench → "~$X est. RPM — [Cat] converts at Y% on MS"
      3. Type baseline  → "$X payout at baseline Z% CVR → ~$Y est. RPM"
    """
    from scout_agent import _scout_score

    offer_id    = str(offer.get("offer_id", ""))
    payout_data = payout_cache.get(offer_id, {})
    category    = (offer.get("category") or "").strip()
    geo         = offer.get("geo", "")

    cache_payout = _parse_payout(payout_data.get("payout"))

    # Fall back to offer's own _payout_num for networks not in payout_cache (MaxBounty, FlexOffers)
    if cache_payout == 0:
        cache_payout = _parse_payout(offer.get("_payout_num"))

    # Normalize payout type from cache or offer fields
    raw_ptype   = payout_data.get("payout_type", "") or offer.get("_payout_type_norm", "")
    payout_type = _normalize_payout_type(raw_ptype)

    enriched = dict(offer)
    if cache_payout > 0:
        enriched["_payout_num"] = cache_payout

    by_offer = benchmarks.get("by_offer_impact_id", {})
    by_cat   = benchmarks.get("by_category", {})

    # Use the score already computed (includes context fit + conversion complexity)
    # so the displayed number matches what ranked it
    display_rpm = adjusted_rpm if adjusted_rpm is not None else _scout_score(enriched, benchmarks)

    parts = []

    # ── Tier 1: real MS data — show actual numbers ────────────────────────────
    if offer_id in by_offer:
        perf    = by_offer[offer_id]
        cvr_pct = perf.get("cvr_pct", 0)
        rpm_val = perf.get("rpm", 0)
        parts.append(f"*We've run this* — ${rpm_val:.2f} RPM at {cvr_pct:.2f}% CVR on MS")

    # ── Tier 2: category benchmark — show category signal, not invented RPM ──
    elif category in by_cat:
        cat_perf = by_cat[category]
        cvr_pct  = cat_perf.get("avg_cvr_pct", 0)
        payout_display = _format_payout(cache_payout, payout_type) if cache_payout else payout_type
        parts.append(
            f"{category} converts at *{cvr_pct:.2f}% CVR* on MS — {payout_display} payout"
        )

    # ── Tier 3: no data — lead with what we do know, skip the system status ──
    else:
        fit_reason = ""
        if _context_fit(enriched) >= 1.40:
            fit_reason = " · high post-transaction fit"
        elif payout_type in ("CPL", "MOBILE_APP", "APP_INSTALL"):
            fit_reason = " · low-friction conversion type"
        elif payout_type == "CPS":
            fit_reason = " · requires purchase — higher post-transaction friction"
        payout_display = _format_payout(cache_payout, payout_type) if cache_payout else _display_payout_type(payout_type)
        parts.append(f"{payout_display}{fit_reason}" if payout_display else "New offer — no MS history yet")

    # ── Inventory gap — only surface when category is genuinely empty ────────
    # "complements X in the Y slot" is too noisy — word-overlap matching is loose
    # and generates false matches (e.g. "Food & Dining" → "Food for the Poor").
    # The actionable signal is the gap, not the partial match.
    _SKIP_CAT = {"other", "uncategorized", ""}
    if category and category.lower() not in _SKIP_CAT:
        cat_words = _name_words(category)
        has_similar = any(cat_words & _name_words(c["adv_name"]) for c in ms_campaigns)
        if not has_similar:
            parts.append(f"nothing in {category} currently")

    # ── Geo note (only if genuinely multi-market) ─────────────────────────────
    regions = [r.strip() for r in geo.split(",") if r.strip()]
    if len(regions) > 4:
        parts.append(f"{len(regions)} markets")

    if not parts:
        return "New offer not yet in inventory."
    text = " · ".join(parts)
    return text[0].upper() + text[1:] if text else text


# ── OG image prefetch ──────────────────────────────────────────────────────────

def _prefetch_offer_images(scored_offers: list[tuple[float, dict]]) -> dict[str, str]:
    """
    Scrape og:image from each offer's tracking URL in parallel (4s timeout per offer).
    Returns {offer_id: image_url}. Empty string for offers where scrape fails.
    Only called for offers without an existing icon_url/hero_url.
    """
    from scout_agent import _scrape_og_image

    def _fetch(args):
        offer_id, tracking_url, existing_url = args
        if existing_url:
            return offer_id, existing_url
        if not tracking_url:
            return offer_id, ""
        url = _scrape_og_image(tracking_url)
        return offer_id, url

    tasks = [
        (str(o.get("offer_id", "")), o.get("tracking_url", ""), o.get("icon_url") or o.get("hero_url") or "")
        for _, o in scored_offers
    ]

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futs = {pool.submit(_fetch, t): t[0] for t in tasks}
        for fut in concurrent.futures.as_completed(futs, timeout=10):
            try:
                offer_id, img_url = fut.result()
                results[offer_id] = img_url
            except Exception:
                results[futs[fut]] = ""
    return results


# ── Slack Block Kit digest ─────────────────────────────────────────────────────

def build_digest_blocks(
    offers_by_network: dict[str, list[tuple[float, dict]]],
    payout_cache:      dict,
    ms_campaigns:      list[dict],
    benchmarks:        dict,
    run_date:          str,
    offer_images:      dict | None = None,
    sel_meta:          dict | None = None,
) -> list:
    all_offers = _load_offers()
    total_screened = len(all_offers)
    total_selected = sum(len(v) for v in offers_by_network.values())
    networks_active = len(offers_by_network)

    # PR 16c: surface dedup count when select_offers suppressed cross-network duplicates
    deduped = (sel_meta or {}).get("advertisers_deduped", 0)
    dedup_note = f" {deduped} duplicate advertiser{'s' if deduped != 1 else ''} filtered." if deduped else ""

    blocks: list = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🎯  Scout Signal  ·  {run_date}"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{total_selected} offer{'s' if total_selected != 1 else ''} "
                    f"worth a look this week* — evaluated {len(_DIGEST_NETWORKS)} networks, "
                    f"{total_screened} offers scored, "
                    f"{networks_active} network{'s' if networks_active != 1 else ''} with qualifying results. "
                    f"Filtered existing inventory and zero-payout campaigns.{dedup_note}"
                ),
            },
        },
        {"type": "divider"},
    ]

    # Count total screened per network for the subheader
    network_totals = {}
    for o in all_offers:
        net = o.get("network", "")
        network_totals[net] = network_totals.get(net, 0) + 1

    # Render each network as its own section with a proper header block.
    # Uses module-level _DIGEST_NETWORKS — single source for the 9-network list.
    for network in _DIGEST_NETWORKS:
        scored_offers = offers_by_network.get(network, [])
        if not scored_offers:
            continue

        network_label = _NETWORK_LABEL.get(network, network.title())
        emoji         = _NETWORK_EMOJI.get(network, "•")
        screened      = network_totals.get(network, 0)

        # header block = large bold text — proper visual break between networks
        blocks.append({
            "type": "header",
            "text": {"type": "plain_text", "text": f"{emoji}  {network_label}", "emoji": True},
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_{screened} offers screened · top {len(scored_offers)} surfaced_"}],
        })

        for _score, offer in scored_offers:
            offer_id     = str(offer.get("offer_id", ""))
            advertiser   = _clean_advertiser_name(offer.get("advertiser", "Unknown"), network)
            payout_data  = payout_cache.get(offer_id, {})
            # Normalize payout type — MaxBounty/FlexOffers use raw strings
            raw_ptype   = payout_data.get("payout_type", "") or offer.get("_payout_type_norm", "")
            payout_type = _normalize_payout_type(raw_ptype)
            category     = (offer.get("category") or "Uncategorized").strip()
            geo_raw      = (offer.get("geo") or "").strip()
            geo          = geo_raw if geo_raw.lower() not in _NON_GEO_VALUES else ""
            tracking_url = offer.get("tracking_url", "")

            payout_num = (
                _parse_payout(payout_data.get("payout"))
                or _parse_payout(offer.get("_payout_num"))
            )
            payout_str = _format_payout(payout_num, payout_type) if payout_num else "Rate TBD"

            # One-line offer summary — first sentence of description, max 80 chars.
            # Strip newlines: a \n inside the italic field breaks the closing underscore
            # onto a new line, causing description text to bleed outside the italic span.
            raw_desc   = " ".join((offer.get("description") or "").strip().split())
            first_sent = raw_desc.split(".")[0].strip()
            # Guard: if summary repeats the advertiser name, fall back to category
            if first_sent.lower().strip(" .") == advertiser.lower().strip(" ."):
                first_sent = ""
            if not first_sent and category and category.lower() not in {"other", "uncategorized", ""}:
                first_sent = category
            offer_summary = (first_sent[:78] + "…") if len(first_sent) > 78 else first_sent

            why = build_why_text(offer, payout_cache, ms_campaigns, benchmarks, adjusted_rpm=_score)

            # Action value — full offer payload for approve/reject handlers
            action_value = json.dumps({
                "offer_id":     offer_id,
                "advertiser":   advertiser,
                "payout":       str(payout_data.get("payout", "") or offer.get("_payout_num", "")),
                "payout_type":  payout_type,
                "category":     category,
                "geo":          geo,
                "tracking_url": tracking_url,
                "description":  offer.get("description", ""),
            })

            # Offer card: 2-col fields + optional thumbnail on the right
            # Left col: advertiser name + one-line italic summary (omitted if blank)
            # Right col: payout + geo (geo omitted if non-geographic placeholder value)
            left_text  = f"*{advertiser}*\n_{offer_summary}_" if offer_summary else f"*{advertiser}*"
            right_text = f"*{payout_str}*\n{geo}" if geo else f"*{payout_str}*"
            img_url = (offer_images or {}).get(offer_id, "")
            offer_block: dict = {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": left_text},
                    {"type": "mrkdwn", "text": right_text},
                ],
            }
            if img_url and img_url.startswith("http"):
                offer_block["accessory"] = {
                    "type": "image",
                    "image_url": img_url,
                    "alt_text": advertiser,
                }

            blocks += [
                offer_block,
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": why}],
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "✓  Add to Queue", "emoji": True},
                            "style": "primary",
                            "action_id": "scout_approve",
                            "value": action_value,
                        },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "✕  Skip"},
                        "style": "danger",
                        "action_id": "scout_reject",
                        "value": action_value,
                    },
                ],
            },
            {"type": "divider"},
        ]

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"_<{QUEUE_LIST_URL}|Demand Queue> · @Scout to research any offer or find gaps · "
                f"Scoring sharpens as campaign offer IDs are linked in the platform_"
            ),
        }],
    })

    return blocks


# ── Sourcing intelligence signals ──────────────────────────────────────────────
# These signals run inside post_digest() and append ONE proactive sourcing
# section to the #scout-offers digest.  They are NOT operational pulse signals.
# Pulse = what's wrong with what's running (#revenue-operations).
# Sourcing = what you should be running next (#scout-offers).

_SOURCING_SIGNAL_PRIORITY = ["new_offers", "seasonal", "payout_upgrades"]

_GROSS_TO_NET_FACTOR: float = 0.70   # ~30% MS margin; adjust if rev-share changes
_MIN_UPGRADE_DELTA:   float = 1.00   # at least $1 net improvement to surface

# Tokens that signal a meaningfully different advertiser entity ("Gap" ≠ "Gap Insurance")
_QUALIFIER_WORDS = frozenset({
    "insurance", "capital", "financial", "mortgage", "credit", "loans",
    "legal", "medical", "health", "realty", "properties", "banking", "fund",
})
_MATCH_STOP_WORDS = frozenset({"the", "and", "inc", "llc", "ltd", "corp", "co", "via"})


def _sourcing_signal_enabled(key: str) -> bool:
    """Return False when key is in SCOUT_DISABLED_SOURCING_SIGNALS env var."""
    disabled = {s.strip() for s in os.getenv("SCOUT_DISABLED_SOURCING_SIGNALS", "").split(",") if s.strip()}
    return key not in disabled


def _parse_payout(val) -> float:
    """Parse a payout value that may carry a '$' prefix (e.g. '$7.20' → 7.20)."""
    if not val:
        return 0.0
    try:
        return float(str(val).strip().lstrip("$").strip())
    except (ValueError, TypeError):
        return 0.0


def _nth_weekday(year: int, month: int, n: int, weekday: int) -> date:
    """
    Return the nth occurrence of weekday (0=Mon … 6=Sun) in month/year.
    n=1 → first occurrence, n=2 → second, etc.

    Examples:
        _nth_weekday(2026, 5, 2, 6)  → 2nd Sunday in May 2026  → Mother's Day (May 10)
        _nth_weekday(2026, 6, 3, 6)  → 3rd Sunday in June 2026 → Father's Day (June 21)
        _nth_weekday(2026, 11, 4, 3) → 4th Thursday in Nov 2026 → Thanksgiving (Nov 26)
    """
    first = date(year, month, 1)
    # days until first occurrence of weekday
    delta = (weekday - first.weekday()) % 7
    first_occurrence = date(year, month, first.day + delta)
    # advance by (n-1) weeks
    return first_occurrence + timedelta(weeks=n - 1)


# date_fn(year) → date.  Fixed holidays use a lambda; floating ones use _nth_weekday.
_SEASONAL_CALENDAR = [
    # Fixed-date holidays
    ("Valentine's Day",   ["jewelry", "flowers", "gifts", "dining"],             21, lambda y: date(y, 2, 14)),
    ("St. Patrick's Day", ["dining", "entertainment"],                            7, lambda y: date(y, 3, 17)),
    ("Tax Day",           ["finance", "tax", "software"],                        21, lambda y: date(y, 4, 15)),
    ("4th of July",       ["travel", "retail", "sports"],                        10, lambda y: date(y, 7,  4)),
    ("Halloween",         ["entertainment", "retail"],                           14, lambda y: date(y, 10, 31)),
    ("Black Friday",      ["retail", "electronics", "shopping"],                 21, lambda y: _nth_weekday(y, 11, 4, 3) + timedelta(days=1)),   # day after 4th Thursday
    ("Cyber Monday",      ["retail", "electronics", "software"],                 21, lambda y: _nth_weekday(y, 11, 4, 3) + timedelta(days=4)),   # Monday after Black Friday
    ("Christmas/Holiday", ["gifts", "travel", "retail", "experiences"],          30, lambda y: date(y, 12, 25)),
    ("New Year's",        ["travel", "fitness", "health"],                       14, lambda y: date(y, 12, 31)),
    # Floating holidays — computed correctly per year via _nth_weekday
    ("MLK Day Weekend",   ["travel", "leisure"],                                 14, lambda y: _nth_weekday(y, 1, 3, 0)),   # 3rd Monday in Jan
    ("Mother's Day",      ["flowers", "gifts", "jewelry", "experiences"],        21, lambda y: _nth_weekday(y, 5, 2, 6)),   # 2nd Sunday in May
    ("Father's Day",      ["golf", "sports", "tools", "experiences", "gifts"],   21, lambda y: _nth_weekday(y, 6, 3, 6)),   # 3rd Sunday in June
    ("Labor Day Weekend", ["travel", "leisure", "retail"],                       14, lambda y: _nth_weekday(y, 9, 1, 0)),   # 1st Monday in Sep
    ("Thanksgiving Week", ["travel", "retail", "food"],                          14, lambda y: _nth_weekday(y, 11, 4, 3)),  # 4th Thursday in Nov
    ("Back to School",    ["education", "software", "retail", "electronics"],    30, lambda y: date(y, 8, 25)),
]


def _sourcing_signal_seasonal(offers: list) -> list:
    """Check for upcoming seasonal events and surface PRIME/STRONG offers in matching verticals."""
    if not _sourcing_signal_enabled("seasonal"):
        return []

    today = date.today()
    results = []

    for event_name, verticals, window_days, date_fn in _SEASONAL_CALENDAR:
        try:
            event_date = date_fn(today.year)
        except Exception:
            continue

        days_until = (event_date - today).days
        if days_until < 0:
            # try next year
            try:
                event_date = date_fn(today.year + 1)
                days_until = (event_date - today).days
            except Exception:
                continue

        if not (0 <= days_until <= window_days):
            continue

        matching = []
        for o in offers:
            tier = o.get("fit_tier", "STANDARD")
            if tier not in ("PRIME", "STRONG"):
                continue
            cat  = (o.get("category") or "").lower()
            name = (o.get("offer_name") or "").lower()
            if any(v in cat or v in name for v in verticals):
                matching.append(o)

        if matching:
            matching.sort(key=lambda x: _parse_payout(x.get("payout")), reverse=True)
            results.append({
                "event_name":  event_name,
                "days_until":  days_until,
                "offer_count": len(matching),
                "top_offers":  matching[:3],
                "verticals":   verticals,
            })

    return results


def _fuzzy_name_match(a: str, b: str) -> bool:
    """
    Word-boundary advertiser name match with qualifier-word guard.
    'Gap' does NOT match 'Gap Insurance'. 'AT&T' DOES match 'AT&T Wireless'.
    """
    import re as _re

    def _clean(s: str) -> frozenset:
        return frozenset(
            t for t in _re.split(r"[^a-z0-9]+", s.lower())
            if len(t) >= 2 and t not in _MATCH_STOP_WORDS
        )

    ca, cb = _clean(a), _clean(b)
    if not ca or not cb:
        return False
    if ca == cb:
        return True
    shorter, longer = (ca, cb) if len(ca) <= len(cb) else (cb, ca)
    if not shorter.issubset(longer):
        return False
    extra = longer - shorter
    return not (extra & _QUALIFIER_WORDS)


def _sourcing_signal_payout_upgrades(offers: list) -> list:
    """
    Detect advertisers where offer inventory has a better payout vs. what's running.
    Inventory gross payout × GROSS_TO_NET_FACTOR vs ClickHouse net payout.
    Only surfaces upgrades >= _MIN_UPGRADE_DELTA after normalization.
    """
    if not offers:
        return []

    if not _sourcing_signal_enabled("payout_upgrades"):
        return []

    try:
        from scout_agent import _get_ch_client
        ch = _get_ch_client()
    except Exception as e:
        log.warning(f"[sourcing] payout_upgrades: could not get CH client: {e}")
        return []

    try:
        # adv_name lives in from_airbyte_campaigns; adpx_conversionsdetails has no advertiser column.
        # Join on campaign_id = id (cast to UInt64 to match types).
        # payout_type is also not in conversions — group by adv_name only.
        rows = ch.query("""
            SELECT
                c.adv_name,
                avg(toFloat64OrNull(cv.payout)) AS avg_net_payout
            FROM default.adpx_conversionsdetails cv
            INNER JOIN (
                SELECT toUInt64(id) AS id, adv_name
                FROM default.from_airbyte_campaigns
                WHERE adv_name IS NOT NULL AND id IS NOT NULL
            ) c ON cv.campaign_id = c.id
            PREWHERE toYYYYMM(cv.created_at) >= toYYYYMM(now() - INTERVAL 30 DAY)
            WHERE cv.created_at >= now() - INTERVAL 30 DAY
            GROUP BY c.adv_name
            HAVING avg_net_payout > 0 AND count() >= 5
            ORDER BY count() DESC
            LIMIT 100
        """).result_rows
    except Exception as e:
        log.warning(f"[sourcing] payout_upgrades query failed: {e}")
        return []

    if not rows:
        return []

    upgrades = []
    for adv_name, avg_net_payout in rows:
        if not adv_name:
            continue
        matches = [
            o for o in offers
            if _fuzzy_name_match(adv_name, o.get("advertiser") or o.get("offer_name") or "")
            and o.get("fit_tier") in ("PRIME", "STRONG")
        ]
        for m in matches:
            inv_gross   = _parse_payout(m.get("payout"))
            inv_net_est = inv_gross * _GROSS_TO_NET_FACTOR
            delta       = inv_net_est - (avg_net_payout or 0)
            payout_type = (m.get("payout_type") or "").upper() or "CPA"
            if delta >= _MIN_UPGRADE_DELTA:
                upgrades.append({
                    "advertiser":            adv_name,
                    "payout_type":           payout_type,
                    "current_net_payout":    avg_net_payout,
                    "inventory_gross_payout": inv_gross,
                    "inventory_net_est":     inv_net_est,
                    "network":               m.get("network", ""),
                    "offer_name":            m.get("offer_name", ""),
                    "delta_net_est":         delta,
                })

    # Deduplicate: same advertiser, keep best delta
    seen: dict = {}
    for u in upgrades:
        key = u["advertiser"].lower()
        if key not in seen or u["delta_net_est"] > seen[key]["delta_net_est"]:
            seen[key] = u

    return sorted(seen.values(), key=lambda x: x["delta_net_est"], reverse=True)[:5]


def _sourcing_signal_new_offers(offers: list) -> list:
    """Surface PRIME/STRONG offers that first appeared in inventory in the last 48 hours."""
    if not _sourcing_signal_enabled("new_offers"):
        return []

    cutoff = datetime.now(timezone.utc).timestamp() - (48 * 3600)

    new_offers = []
    for o in offers:
        if o.get("fit_tier") not in ("PRIME", "STRONG"):
            continue
        fs = o.get("first_seen", "")
        if not fs:
            continue  # predates first_seen field — skip
        try:
            ts = datetime.fromisoformat(fs.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        if ts >= cutoff:
            new_offers.append(o)

    new_offers.sort(key=lambda x: _parse_payout(x.get("payout")), reverse=True)
    return new_offers[:5]


def _run_sourcing_signals(offers: list) -> dict:
    """
    Run sourcing signals lazily in priority order against the offer inventory.
    Applies fatigue budget: emit at most ONE section per digest post.
    Priority: new_offers → seasonal → payout_upgrades.
    Stops evaluating as soon as a non-empty result is found.
    """
    _signal_fns: dict = {
        "new_offers":      _sourcing_signal_new_offers,
        "seasonal":        _sourcing_signal_seasonal,
        "payout_upgrades": _sourcing_signal_payout_upgrades,
    }
    signals: dict = {}
    found = False
    for key in _SOURCING_SIGNAL_PRIORITY:
        if found:
            signals[key] = []
        else:
            result = _signal_fns[key](offers)
            signals[key] = result
            if result:
                found = True
    return signals


def _build_sourcing_intel_blocks(signals: dict) -> list:
    """Build Block Kit offer cards for the winning sourcing signal. Returns [] if nothing fired.

    new_offers / seasonal → rich per-offer cards (image + payout + geo + context + buttons).
    payout_upgrades → plain mrkdwn text (different data shape; references running campaigns).
    Max 3 cards per section to keep visual density appropriate.
    """
    blocks: list = []

    # ── payout_upgrades: plain mrkdwn (different data shape — not offer-inventory cards) ──
    if signals.get("payout_upgrades") and not signals.get("new_offers") and not signals.get("seasonal"):
        lines = [":moneybag: *Payout upgrades worth checking* _(est. net after ~30% margin)_"]
        for u in signals["payout_upgrades"]:
            lines.append(
                f">  {u['advertiser']} — running ${u['current_net_payout']:.2f} {u['payout_type']} net"
                f" · {u['network']} has ${u['inventory_gross_payout']:.2f} gross"
                f" (est. ~${u['inventory_net_est']:.2f} net, +${u['delta_net_est']:.2f})"
            )
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}})
        return blocks

    # ── Determine active signal + header ──────────────────────────────────────────
    active_offers: list = []
    header_text: str = ""
    context_fn = None  # callable(offer) -> str

    if signals.get("new_offers"):
        active_offers = signals["new_offers"][:3]
        header_text   = ":new: *New PRIME/STRONG offers in the last 48h*"

        def context_fn(o):  # noqa: E301
            return (
                f"_{_parse_payout(o.get('payout')):.2f} {(o.get('payout_type') or '').upper()}"
                f" · {o.get('fit_tier', 'PRIME')} tier · first seen in last 48h_"
            )

    elif signals.get("seasonal"):
        evt           = signals["seasonal"][0]
        day_str       = f"{evt['days_until']} day{'s' if evt['days_until'] != 1 else ''}"
        active_offers = evt["top_offers"][:3]
        header_text   = (
            f":calendar: *{evt['event_name']} in {day_str}*"
            f" — {evt['offer_count']} PRIME/STRONG offer{'s' if evt['offer_count'] != 1 else ''}"
        )

        def context_fn(o):  # noqa: E301
            return (
                f"_{(o.get('category') or '').title() or (o.get('payout_type') or '').upper()}"
                f" · {(o.get('payout_type') or '').upper()} ${_parse_payout(o.get('payout')):.2f}_"
            )

    if not active_offers:
        return blocks

    # ── Header + divider ──────────────────────────────────────────────────────────
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": header_text}})
    blocks.append({"type": "divider"})

    # ── Per-offer cards ───────────────────────────────────────────────────────────
    for o in active_offers:
        offer_id    = o.get("offer_id") or o.get("offer_name", "")
        advertiser  = o.get("advertiser") or o.get("offer_name") or "Unknown"
        # Collapse newlines/whitespace before wrapping in Slack italic (_..._)
        summary     = " ".join((o.get("description") or "").split())[:80]
        payout_num  = _parse_payout(o.get("payout"))
        payout_type = (o.get("payout_type") or "").upper()
        payout_str  = _format_payout(payout_num, payout_type) if payout_num else "Rate TBD"
        geo         = o.get("geo") or o.get("country") or ""
        network     = o.get("network", "")
        img_url     = o.get("image_url") or o.get("creative_url") or ""
        why         = context_fn(o) if context_fn else ""

        action_value = json.dumps({
            "offer_id":    offer_id,
            "offer_name":  advertiser,
            "network":     network,
            "payout":      payout_num,
            "payout_type": payout_type,
            "source":      "sourcing_signal",
        }, separators=(",", ":"))

        left_text  = f"*{advertiser}*\n_{summary}_" if summary else f"*{advertiser}*"
        right_text = f"*{payout_str}*\n{geo}" if geo else f"*{payout_str}*"

        offer_block: dict = {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": left_text},
                {"type": "mrkdwn", "text": right_text},
            ],
        }
        if img_url and img_url.startswith("http"):
            offer_block["accessory"] = {
                "type":      "image",
                "image_url": img_url,
                "alt_text":  advertiser,
            }

        blocks += [
            offer_block,
            {"type": "context", "elements": [{"type": "mrkdwn", "text": why}]},
            {
                "type": "actions",
                "elements": [
                    {
                        "type":      "button",
                        "text":      {"type": "plain_text", "text": "✓  Add to Draft", "emoji": True},
                        "style":     "primary",
                        "action_id": "scout_draft_add",
                        "value":     action_value,
                    },
                    {
                        "type":      "button",
                        "text":      {"type": "plain_text", "text": "✕  Skip"},
                        "style":     "danger",
                        "action_id": "scout_draft_skip",
                        "value":     action_value,
                    },
                ],
            },
            {"type": "divider"},
        ]

    return blocks


# ── Offer loader ───────────────────────────────────────────────────────────────

def _load_offers() -> list:
    """Load offers from DEMAND_FEED_URL when set; fall back to disk snapshot."""
    url = os.getenv("DEMAND_FEED_URL")
    if url:
        try:
            with urllib.request.urlopen(f"{url.rstrip('/')}/offers", timeout=10) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            log.warning(f"[scout_digest] DEMAND_FEED_URL fetch failed ({exc}); falling back to disk")
    try:
        return json.loads(OFFERS_FILE.read_text())
    except Exception as e:
        log.error(f"Could not load offers: {e}")
        return []


# ── Main selection ─────────────────────────────────────────────────────────────

def select_offers(
    n_per_network: int = 5,
    ms_campaigns:  list[dict] | None = None,
    benchmarks:    dict | None = None,
    force:         bool = False,
) -> tuple[dict[str, list[tuple[float, dict]]], dict]:
    """
    Returns (offers_by_network, meta) where meta has skip counts.
    Result: {"impact": [(score, offer), ...], "maxbounty": [...], "flexoffers": [...]}
    Networks with no qualifying offers are omitted.
    ms_campaigns and benchmarks can be passed in to avoid redundant external calls.
    force=True bypasses the is_already_in_ms filter so testing always surfaces real offers.
    """
    from scout_agent import _get_benchmarks, SCOUT_THRESHOLDS

    state         = load_state()
    offers        = _load_offers()
    payout_cache  = json.loads(PAYOUT_CACHE.read_text()) if PAYOUT_CACHE.exists() else {}
    if ms_campaigns is None:
        ms_campaigns = get_active_ms_campaigns()
    if benchmarks is None:
        benchmarks = _get_benchmarks()

    # PR 18: diversity caps now come from config/scout_thresholds.json so the team
    # can tune without a code change. Defaults match prior hardcoded behavior.
    _digest_cfg = SCOUT_THRESHOLDS.get("digest", {})
    _max_per_category = int(_digest_cfg.get("max_per_category", 2))
    _max_per_payout_type = int(_digest_cfg.get("max_per_payout_type", 2))

    # Score all offers across all networks
    by_network: dict[str, list] = {}
    skipped_in_ms, skipped_no_score = 0, 0

    for offer in offers:
        network = offer.get("network", "")
        if not network:
            continue

        if not force and is_already_in_ms(offer, ms_campaigns):
            skipped_in_ms += 1
            continue

        s = score_offer(offer, payout_cache, state, benchmarks, force=force)
        if s is None:
            skipped_no_score += 1
            continue

        by_network.setdefault(network, []).append((s, offer))

    # Sort each network and apply diversity cap (max 2 per named category)
    _UNCAPPED = {"Other", "Uncategorized", ""}
    result: dict[str, list[tuple[float, dict]]] = {}
    total_selected = 0

    # Cross-network dedup: if the same advertiser appears on multiple networks,
    # keep only the first (highest priority network wins). Iteration order is
    # _DIGEST_NETWORKS — Big 4 first, alphabetical remainder.
    seen_advertisers: set[str] = set()
    advertisers_deduped = 0

    for network in _DIGEST_NETWORKS:
        candidates = sorted(by_network.get(network, []), key=lambda x: x[0], reverse=True)
        selected: list[tuple[float, dict]] = []
        category_counts: dict[str, int] = {}
        ptype_counts: dict[str, int] = {}  # max 2 per payout type — forces CPL/CPS variety

        for score, offer in candidates:
            adv_key = (offer.get("advertiser") or "").strip().lower()
            if adv_key and adv_key in seen_advertisers:
                advertisers_deduped += 1
                log.debug(
                    "[digest] dedup: skipping advertiser '%s' on %s (already surfaced on earlier network)",
                    offer.get("advertiser"), network,
                )
                continue

            cat = (offer.get("category") or "").strip()
            if cat not in _UNCAPPED and category_counts.get(cat, 0) >= _max_per_category:
                continue
            # Payout-type cap: avoid surfacing N× CPS with no CPL in the same section.
            # Default cap of 2 — if a CPL exists it will break through even if ranked lower.
            raw_ptype = offer.get("_payout_type_norm", "")
            ptype = _normalize_payout_type(raw_ptype)
            if ptype and ptype_counts.get(ptype, 0) >= _max_per_payout_type:
                continue
            selected.append((score, offer))
            if adv_key:
                seen_advertisers.add(adv_key)
            if cat not in _UNCAPPED:
                category_counts[cat] = category_counts.get(cat, 0) + 1
            if ptype:
                ptype_counts[ptype] = ptype_counts.get(ptype, 0) + 1
            if len(selected) >= n_per_network:
                break

        if selected:
            result[network] = selected
            total_selected += len(selected)

    meta = {
        "candidates": sum(len(v) for v in by_network.values()),
        "skipped_in_ms": skipped_in_ms,
        "skipped_no_score": skipped_no_score,
        "total_selected": total_selected,
        "total_offers": len(offers),
        # PR 16c: surface dedup count to the digest footer so the team can see it
        "advertisers_deduped": advertisers_deduped,
    }
    log.info(
        f"Selection: {meta['candidates']} candidates across "
        f"{len(by_network)} networks "
        f"(skipped: {skipped_in_ms} in-platform, {skipped_no_score} below-threshold/state) "
        f"→ {total_selected} selected"
    )
    return result, meta


# ── Entry point ────────────────────────────────────────────────────────────────

def post_digest(dry_run: bool = False, is_force: bool = False):
    """Select top offers and post the offer digest to Slack.

    Event-driven: posts when new offers are detected OR on Mondays (weekly review).
    Force=True bypasses all gates and always posts to #scout-qa.
    Channel: #scout-offers in production, #scout-qa in dev/force.
    """
    from slack_sdk.web import WebClient

    now      = datetime.now()
    is_monday = now.weekday() == 0  # Monday = 0

    # ── Load all external data once — no redundant calls ─────────────────────
    from scout_agent import _get_benchmarks, SCOUT_THRESHOLDS

    payout_cache     = json.loads(PAYOUT_CACHE.read_text()) if PAYOUT_CACHE.exists() else {}
    ms_campaigns     = get_active_ms_campaigns()
    benchmarks       = _get_benchmarks()
    # PR 18: offers_per_network now from config so the team can tune without code change.
    _offers_per_network = int(SCOUT_THRESHOLDS.get("digest", {}).get("offers_per_network", 3))
    offers_by_network, sel_meta = select_offers(
        n_per_network=_offers_per_network, ms_campaigns=ms_campaigns, benchmarks=benchmarks, force=is_force
    )

    total_selected = sel_meta["total_selected"]
    if total_selected == 0:
        log.info("No offers to surface — skipping digest.")
        if is_force:
            raise RuntimeError(
                f"0 offers selected from {sel_meta['total_offers']} total "
                f"({sel_meta['skipped_in_ms']} in-platform filtered, "
                f"{sel_meta['skipped_no_score']} below-threshold/state)"
            )
        return

    # ── Diff detection: new offers since last scraper run ────────────────────
    new_offer_keys: set = set()
    try:
        prev_data = json.loads(OFFERS_PREVIOUS_FILE.read_text())
        prev_keys = {o["_unique_key"] for o in prev_data if o.get("_unique_key")}
        curr_keys = {o["_unique_key"] for item in offers_by_network.values() for _, o in item if o.get("_unique_key")}
        new_offer_keys = curr_keys - prev_keys
    except FileNotFoundError:
        log.warning("[digest] offers_previous.json missing — treating all offers as new")
        new_offer_keys = {o["_unique_key"] for item in offers_by_network.values() for _, o in item if o.get("_unique_key")}
    except json.JSONDecodeError:
        log.warning("[digest] offers_previous.json corrupt — treating all offers as new")
        new_offer_keys = {o["_unique_key"] for item in offers_by_network.values() for _, o in item if o.get("_unique_key")}

    # ── Event-driven gate: post only when meaningful ──────────────────────────
    # Skip if: no new offers AND not Monday (weekly review) AND not forced
    if not dry_run and not is_force:
        if not new_offer_keys and not is_monday:
            log.info("[digest] no new offers and not Monday — skipping post")
            return

    # Flatten for image prefetch
    all_scored = [item for v in offers_by_network.values() for item in v]
    log.info("Prefetching offer images…")
    offer_images = _prefetch_offer_images(all_scored)
    found = sum(1 for v in offer_images.values() if v)
    log.info(f"Images found: {found}/{len(all_scored)}")

    run_date = now.strftime("%b %-d")
    blocks   = build_digest_blocks(offers_by_network, payout_cache, ms_campaigns, benchmarks, run_date, offer_images=offer_images, sel_meta=sel_meta)

    # Prepend NEW THIS WEEK section
    new_block: list = []
    if new_offer_keys:
        new_offers_flat = [
            o for item in offers_by_network.values() for o in item
            if o.get("_unique_key") in new_offer_keys
        ]
        new_names = ", ".join(o.get("offer_name") or o.get("adv_name", "Unknown") for o in new_offers_flat[:8])
        if len(new_offers_flat) > 8:
            new_names += f" +{len(new_offers_flat) - 8} more"
        new_block = [
            {"type": "section", "text": {"type": "mrkdwn", "text": f":new:  *NEW THIS WEEK*  ({len(new_offer_keys)} offers)"}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": new_names}]},
            {"type": "divider"},
        ]
    else:
        new_block = [
            {"type": "context", "elements": [{"type": "mrkdwn", "text": "No new offers this week — all offers already in your pipeline."}]},
            {"type": "divider"},
        ]
    blocks = new_block + blocks

    # ── Sourcing intelligence section (appended after offer list) ─────────────
    # Runs 3 signals against the full offer inventory (not just this week's
    # selected offers).  Fatigue budget: at most ONE proactive section per post.
    # Priority: new_offers → seasonal → payout_upgrades.
    try:
        all_offers_flat = _load_offers()
        sourcing_signals = _run_sourcing_signals(all_offers_flat)
        sourcing_blocks  = _build_sourcing_intel_blocks(sourcing_signals)
        if sourcing_blocks:
            blocks = blocks + [{"type": "divider"}] + sourcing_blocks
    except Exception as _e:
        log.warning(f"[digest] sourcing intelligence failed (non-fatal): {_e}")

    fallback = f"🎯 Scout Signal — {run_date}: {total_selected} new offers across {len(offers_by_network)} networks"

    if dry_run:
        print(json.dumps(blocks, indent=2))
        return

    channel = _digest_channel(force=is_force)
    web  = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))
    resp = web.chat_postMessage(
        channel=channel,
        text=fallback,
        blocks=blocks,
        unfurl_links=False,
        unfurl_media=False,
    )
    log.info(f"Digest posted → {channel} ts={resp['ts']}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Scout Signal — offer digest")
    parser.add_argument("--dry-run", action="store_true", help="Print blocks without posting")
    parser.add_argument("--force", action="store_true", help="Bypass new-offer gate, always post to #scout-qa")
    args = parser.parse_args()
    post_digest(dry_run=args.dry_run, is_force=args.force)
