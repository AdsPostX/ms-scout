"""
scout_digest.py
SCOUT Sniper — Weekly Offer Digest

Selects top candidate offers from the scraper output, filters duplicates,
scores by payout + category fit, and posts a curated Slack digest with
approve/reject buttons that feed directly into the Demand Queue workflow.

Usage:
  python scout_digest.py              # post this week's digest
  python scout_digest.py --dry-run    # print blocks without posting
"""

import argparse
import concurrent.futures
import json
import logging
import os
import pathlib
import re
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(override=True)

log = logging.getLogger("scout_digest")

# ── Paths ──────────────────────────────────────────────────────────────────────
_DIR         = pathlib.Path(__file__).parent
DATA_DIR     = _DIR / "data"
OFFERS_FILE  = DATA_DIR / "offers_latest.json"
PAYOUT_CACHE = _DIR / "payout_cache.json"
STATE_FILE   = DATA_DIR / "digest_state.json"

# ── Slack ──────────────────────────────────────────────────────────────────────
SCOUT_DIGEST_CHANNEL = "C0AQEECF800"  # #scout-hq — digest always posts here, not configurable
QUEUE_LIST_URL = "https://momentscience.slack.com/lists/T03Q93Q96UD/F07QCKFP0RM"

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

_NETWORK_LABEL: dict[str, str] = {
    "impact":     "Impact",
    "maxbounty":  "MaxBounty",
    "flexoffers": "FlexOffers",
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

def score_offer(offer: dict, payout_cache: dict, state: dict, benchmarks: dict) -> float | None:
    """
    Returns estimated RPM (Scout Score), or None to exclude.

    Exclusion:
    - Zero/missing payout
    - Already approved/queued
    - Rejected and payout hasn't improved ≥15%

    Ranking:
    - Primary: Scout Score RPM (payout × predicted_CVR × 1000) — same model as @Scout agent
    - Tiebreaker: has tracking URL (integration-ready)

    Confidence tiers for CVR (handled inside _scout_score):
    1. Real MS data for this exact offer (ClickHouse)
    2. Category benchmark (ClickHouse aggregate)
    3. Payout-type baseline estimate
    """
    from scout_agent import _scout_score

    offer_id = str(offer.get("offer_id", ""))

    # Already approved — don't resurface
    if offer_id in state.get("approved", {}):
        return None

    # Rejected — only resurface if payout improved ≥15%
    rejected = state.get("rejected", {})
    if offer_id in rejected:
        payout_data = payout_cache.get(offer_id, {})
        try:
            current = float(payout_data.get("payout") or 0)
            old     = float(rejected[offer_id].get("payout_at_action") or 0)
        except (ValueError, TypeError):
            return None
        if old <= 0 or (current - old) / old < 0.15:
            return None

    # Build enriched offer: use payout_cache amount (Impact API, more accurate)
    # over scraper-normalised _payout_num where available.
    # MaxBounty/FlexOffers won't be in payout_cache — fall back to offer fields directly.
    payout_data = payout_cache.get(offer_id, {})
    try:
        cache_payout = float(payout_data.get("payout") or 0)
    except (ValueError, TypeError):
        cache_payout = 0.0

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
    # surfacing regardless of network inventory size. $20 effective score filters
    # weak long-tail offers (e.g. $12 CPA meal kits, $15 trading platforms) that
    # only appear because a network has thin inventory.
    _MIN_RPM = 20.0
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

    try:
        cache_payout = float(payout_data.get("payout") or 0)
    except (ValueError, TypeError):
        cache_payout = 0.0

    # Fall back to offer's own _payout_num for networks not in payout_cache (MaxBounty, FlexOffers)
    if cache_payout == 0:
        try:
            cache_payout = float(offer.get("_payout_num") or 0)
        except (ValueError, TypeError):
            cache_payout = 0.0

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
) -> list:
    all_offers = _load_offers()
    total_screened = len(all_offers)
    total_selected = sum(len(v) for v in offers_by_network.values())
    networks_active = len(offers_by_network)

    blocks: list = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🎯  SCOUT Sniper  ·  {run_date}"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{total_selected} offer{'s' if total_selected != 1 else ''} "
                    f"worth a look this week* — screened {total_screened} offers across "
                    f"{networks_active} network{'s' if networks_active != 1 else ''}, "
                    f"filtered existing inventory and zero-payout campaigns."
                ),
            },
        },
        {"type": "divider"},
    ]

    # Emoji per network for fast visual scanning
    _NETWORK_EMOJI = {"impact": "⚡", "maxbounty": "💰", "flexoffers": "🔗"}

    # Count total screened per network for the subheader
    network_totals = {}
    for o in all_offers:
        net = o.get("network", "")
        network_totals[net] = network_totals.get(net, 0) + 1

    # Render each network as its own section with a proper header block
    for network in ("impact", "maxbounty", "flexoffers"):
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

            try:
                payout_num = float(payout_data.get("payout") or offer.get("_payout_num") or 0)
                payout_str = _format_payout(payout_num, payout_type)
            except (ValueError, TypeError):
                payout_num, payout_str = 0.0, "Rate TBD"

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
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": why},
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


# ── Offer loader ───────────────────────────────────────────────────────────────

def _load_offers() -> list:
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
) -> dict[str, list[tuple[float, dict]]]:
    """
    Returns top-N scored offers per network, diversity-capped by category (max 2 per category).
    Result: {"impact": [(score, offer), ...], "maxbounty": [...], "flexoffers": [...]}
    Networks with no qualifying offers are omitted.
    ms_campaigns and benchmarks can be passed in to avoid redundant external calls.
    """
    from scout_agent import _get_benchmarks

    state         = load_state()
    offers        = _load_offers()
    payout_cache  = json.loads(PAYOUT_CACHE.read_text()) if PAYOUT_CACHE.exists() else {}
    if ms_campaigns is None:
        ms_campaigns = get_active_ms_campaigns()
    if benchmarks is None:
        benchmarks = _get_benchmarks()

    # Score all offers across all networks
    by_network: dict[str, list] = {}
    skipped_in_ms, skipped_no_score = 0, 0

    for offer in offers:
        network = offer.get("network", "")
        if not network:
            continue

        if is_already_in_ms(offer, ms_campaigns):
            skipped_in_ms += 1
            continue

        s = score_offer(offer, payout_cache, state, benchmarks)
        if s is None:
            skipped_no_score += 1
            continue

        by_network.setdefault(network, []).append((s, offer))

    # Sort each network and apply diversity cap (max 2 per named category)
    _UNCAPPED = {"Other", "Uncategorized", ""}
    result: dict[str, list[tuple[float, dict]]] = {}
    total_selected = 0

    for network in ("impact", "maxbounty", "flexoffers"):
        candidates = sorted(by_network.get(network, []), key=lambda x: x[0], reverse=True)
        selected: list[tuple[float, dict]] = []
        category_counts: dict[str, int] = {}
        ptype_counts: dict[str, int] = {}  # max 2 per payout type — forces CPL/CPS variety

        for score, offer in candidates:
            cat = (offer.get("category") or "").strip()
            if cat not in _UNCAPPED and category_counts.get(cat, 0) >= 2:
                continue
            # Payout-type cap: avoid surfacing 3× CPS with no CPL in the same section.
            # Cap at 2 per type — if a CPL exists it will break through, even if ranked lower.
            raw_ptype = offer.get("_payout_type_norm", "")
            ptype = _normalize_payout_type(raw_ptype)
            if ptype and ptype_counts.get(ptype, 0) >= 2:
                continue
            selected.append((score, offer))
            if cat not in _UNCAPPED:
                category_counts[cat] = category_counts.get(cat, 0) + 1
            if ptype:
                ptype_counts[ptype] = ptype_counts.get(ptype, 0) + 1
            if len(selected) >= n_per_network:
                break

        if selected:
            result[network] = selected
            total_selected += len(selected)

    log.info(
        f"Selection: {sum(len(v) for v in by_network.values())} candidates across "
        f"{len(by_network)} networks "
        f"(skipped: {skipped_in_ms} in-platform, {skipped_no_score} below-threshold/state) "
        f"→ {total_selected} selected"
    )
    return result


# ── Entry point ────────────────────────────────────────────────────────────────

def post_digest(dry_run: bool = False):
    """Select top offers and post the weekly Slack digest.

    Idempotent within the same ISO week — won't post twice even if called
    multiple times (e.g. cron retry or manual trigger on the same day).
    """
    from slack_sdk.web import WebClient

    # ── Idempotency: one post per ISO week ────────────────────────────────────
    now       = datetime.now()
    this_week = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"

    if not dry_run:
        state = load_state()
        last_posted = state.get("last_posted_week", "")
        if last_posted == this_week:
            log.info(f"Digest already posted this week ({this_week}) — skipping.")
            return

    # ── Load all external data once — no redundant calls ─────────────────────
    from scout_agent import _get_benchmarks

    payout_cache     = json.loads(PAYOUT_CACHE.read_text()) if PAYOUT_CACHE.exists() else {}
    ms_campaigns     = get_active_ms_campaigns()
    benchmarks       = _get_benchmarks()
    offers_by_network = select_offers(n_per_network=3, ms_campaigns=ms_campaigns, benchmarks=benchmarks)

    total_selected = sum(len(v) for v in offers_by_network.values())
    if total_selected == 0:
        log.info("No new offers to surface this week — skipping digest.")
        return

    # Flatten for image prefetch
    all_scored = [item for v in offers_by_network.values() for item in v]
    log.info("Prefetching offer images…")
    offer_images = _prefetch_offer_images(all_scored)
    found = sum(1 for v in offer_images.values() if v)
    log.info(f"Images found: {found}/{len(all_scored)}")

    run_date = now.strftime("%b %-d")
    blocks   = build_digest_blocks(offers_by_network, payout_cache, ms_campaigns, benchmarks, run_date, offer_images=offer_images)
    fallback = f"🎯 SCOUT Sniper — {run_date}: {total_selected} new offers across {len(offers_by_network)} networks"

    if dry_run:
        print(json.dumps(blocks, indent=2))
        return

    web  = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))
    resp = web.chat_postMessage(
        channel=SCOUT_DIGEST_CHANNEL,
        text=fallback,
        blocks=blocks,
        unfurl_links=False,
        unfurl_media=False,
    )
    log.info(f"Digest posted → {SCOUT_DIGEST_CHANNEL} ts={resp['ts']}")

    # Record the week so we don't post again until next Monday
    state = load_state()
    state["last_posted_week"] = this_week
    save_state(state)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="SCOUT Sniper — post weekly offer digest")
    parser.add_argument("--dry-run", action="store_true", help="Print blocks without posting")
    args = parser.parse_args()
    post_digest(dry_run=args.dry_run)
