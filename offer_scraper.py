"""
offer_scraper.py
Affiliate Network Offer Scraper → Notion + JSON snapshot

Networks supported:
  - Impact       [HTTP Basic Auth]
  - FlexOffers   [API Key, domain-based]
  - MaxBounty    [REST API: email + password → JWT token (2hr TTL)]

Output:
  - Notion database (primary)
  - data/offers_latest.json (for Scout intelligence bot)

Usage:
  python offer_scraper.py               # run all networks
  python offer_scraper.py --network impact  # run single network
"""

import os
import pathlib
import re
import json
import logging
import argparse
import requests
import time
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# CONFIG — loaded from .env file (see .env.example) or environment variables
# ---------------------------------------------------------------------------
IMPACT_SID          = os.environ.get("IMPACT_SID", "")
IMPACT_TOKEN        = os.environ.get("IMPACT_TOKEN", "")

FLEXOFFERS_API_KEY  = os.environ.get("FLEXOFFERS_API_KEY", "")
FLEXOFFERS_DOMAIN_ID = os.environ.get("FLEXOFFERS_DOMAIN_ID", "")

MAXBOUNTY_EMAIL    = os.environ.get("MAXBOUNTY_EMAIL", "")
MAXBOUNTY_PASSWORD = os.environ.get("MAXBOUNTY_PASSWORD", "")

CJ_API_KEY         = os.environ.get("CJ_API_KEY", "")      # Personal Access Token from developers.cj.com
CJ_WEBSITE_ID      = os.environ.get("CJ_WEBSITE_ID", "")   # CJ Company ID (CID) — used for advertiser lookup
CJ_PUBLISHER_ID    = os.environ.get("CJ_PUBLISHER_ID", "") # CJ Publisher Website ID (PID) — used for link-search

# ShareASale
SHAREASALE_AFFILIATE_ID = os.environ.get("SHAREASALE_AFFILIATE_ID", "3279349")
SHAREASALE_API_TOKEN    = os.environ.get("SHAREASALE_API_TOKEN", "")   # API token from dashboard
SHAREASALE_API_SECRET   = os.environ.get("SHAREASALE_API_SECRET", "")  # API secret from dashboard

# Rakuten Advertising (formerly LinkShare)
RAKUTEN_PUBLISHER_ID    = os.environ.get("RAKUTEN_PUBLISHER_ID", "3948979")
RAKUTEN_API_TOKEN       = os.environ.get("RAKUTEN_API_TOKEN", "")  # OAuth token from Rakuten dashboard

# Awin
AWIN_PUBLISHER_ID       = os.environ.get("AWIN_PUBLISHER_ID", "")
AWIN_API_KEY            = os.environ.get("AWIN_API_KEY", "")  # API key from awin.com/us/en/interface/api/

# TUNE / HasOffers — multiple white-label instances (KashKick, BrownBoots, AdAction, Revoffers, etc.)
# Each instance is at {subdomain}.hasoffers.com (or {subdomain}.tune.com for newer Revoffers)
# Credentials stored per-instance: TUNE_{NAME}_NETWORK_ID + TUNE_{NAME}_API_KEY
TUNE_INSTANCES = [
    # (label, network_id, api_key, base_url)
    # Populated at startup from env vars; add more as needed
]
# Dynamically populate from env vars: TUNE_{NAME}_NETWORK_ID (subdomain) + TUNE_{NAME}_API_KEY
# base_url should be https://{subdomain}.api.hasoffers.com for the V3 JSON API
for _inst_name in ("KASHKICK", "BROWNBOOTS", "ADACTION", "REVOFFERS", "ADBLOOM", "SUCCESSFUL_MEDIA"):
    _nid = os.environ.get(f"TUNE_{_inst_name}_NETWORK_ID", "")
    _key = os.environ.get(f"TUNE_{_inst_name}_API_KEY", "")
    # If BASE_URL is explicitly set, use it; otherwise derive from network_id (subdomain)
    _default_url = f"https://{_nid}.api.hasoffers.com" if _nid else ""
    _url = os.environ.get(f"TUNE_{_inst_name}_BASE_URL", _default_url)
    if _nid and _key:
        TUNE_INSTANCES.append((_inst_name.lower(), _nid, _key, _url))

# Everflow — multiple white-label instances (GiddyUp, Accio Ads, Klay, Credit.com, etc.)
EVERFLOW_INSTANCES = [
    # (label, api_key, base_url)
    # Populated at startup from env vars
]
for _ef_name in ("GIDDYUP", "ACCIOADS", "KLAYMEDIA", "CREDITCOM", "MWKCONSULTING", "PAWZITIVITY", "ARAGONPREMIUM"):
    _ef_key = os.environ.get(f"EVERFLOW_{_ef_name}_API_KEY", "")
    _ef_url = os.environ.get(f"EVERFLOW_{_ef_name}_BASE_URL", "")
    if _ef_key and _ef_url:
        EVERFLOW_INSTANCES.append((_ef_name.lower(), _ef_key, _ef_url))

# Notion — primary output destination
# Setup: notion.so/my-integrations → create integration → copy secret_... token
#        Then share the "Offer Inventory — MS Network" database with that integration
NOTION_TOKEN        = os.environ.get("NOTION_TOKEN", "")
NOTION_DB_ID        = os.environ.get("NOTION_DB_ID", "")

# ClickHouse Cloud — for MS campaign matching
CH_HOST             = os.environ.get("CH_HOST", "")
CH_USER             = os.environ.get("CH_USER", "")
CH_PASSWORD         = os.environ.get("CH_PASSWORD", "")
CH_DATABASE         = os.environ.get("CH_DATABASE", "default")

DEBUG = False  # set to True via --debug flag at runtime

# Payout cache — lives in data/ for persistence across Render deploys
PAYOUT_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "payout_cache.json")

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NORMALIZED OFFER SCHEMA
# Every network adapter must return a list of dicts matching these keys.
# ---------------------------------------------------------------------------
OFFER_FIELDS = [
    # Identity
    "network",
    "offer_id",
    "advertiser",      # brand/company name (maps to adv_name in MS platform)

    # Creative & copy — what the MS platform team needs to enter a campaign
    "title",           # short offer headline (e.g. "Save 15% on your first order")
    "description",     # longer ad copy / offer details (maps to MS description field)
    "cta",             # call-to-action button text (e.g. "Get Offer", "Shop Now")
    "terms",           # terms & conditions / fine print
    "mini_description",# very short teaser (fits in small placements)

    # Payout — multiple signals, highest fidelity wins
    "payout",          # numeric amount string (e.g. "12.50")
    "payout_type",     # CPA | CPL | CPS | CPC | RevShare
    "currency",        # ISO code (USD, GBP, etc.)

    # Targeting & geo
    "geo",             # normalized Notion select (e.g. "US Only", "Global")
    "geo_raw",         # raw string from network before normalization (for MS geo_targeting)
    "os_targeting",    # OS/device targeting if provided (e.g. "iOS,Android", "All")
    "platform_targeting", # platform type (Mobile, Desktop, All)

    # Categorization
    "category",        # normalized string for Notion (may be comma-joined)

    # Creatives — URLs for MS platform upload
    "icon_url",        # square logo/icon (150×150 target)
    "hero_url",        # wide banner creative (1000×280 target)
    "banner_url",      # medium rectangle or alternate creative

    # Tracking
    "tracking_url",    # affiliate tracking URL (goes in MS campaign tracking field)
    "preview_url",     # landing page preview URL (for QA)

    # Status & metadata
    "status",
    "date_scraped",
]

def empty_offer() -> dict:
    return {k: "" for k in OFFER_FIELDS}


# ---------------------------------------------------------------------------
# PAYOUT CACHE HELPERS
# ---------------------------------------------------------------------------

def _load_payout_cache() -> dict:
    """
    Load payout cache from payout_cache.json if it exists.
    Returns dict: {campaign_id: {"payout": "5.00", "payout_type": "CPA"}}
    """
    if not os.path.exists(PAYOUT_CACHE_FILE):
        return {}
    try:
        with open(PAYOUT_CACHE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"Failed to load payout cache: {e}")
        return {}


def _save_payout_cache(cache: dict):
    """Save payout cache to data/payout_cache.json"""
    try:
        pathlib.Path(PAYOUT_CACHE_FILE).parent.mkdir(parents=True, exist_ok=True)
        with open(PAYOUT_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
        log.info(f"Payout cache saved: {len(cache)} campaigns")
    except Exception as e:
        log.error(f"Failed to save payout cache: {e}")


def _run_impact_payout_enrichment(campaign_ids: list, existing_cache: dict = None):
    """
    Fetch payout data from Impact /Campaigns/{id}/Actions endpoint.
    Merges results into existing_cache (incremental — skips IDs already cached).

    Args:
        campaign_ids: list of campaign ID strings to potentially enrich
        existing_cache: current cache dict to merge into. If None, loads from disk.
                        Pass {} to force a full re-fetch (--enrich-payouts flag).

    Returns the merged cache dict (also persisted to payout_cache.json).
    """
    if existing_cache is None:
        existing_cache = _load_payout_cache()

    missing = [cid for cid in campaign_ids if str(cid) not in existing_cache]
    if not missing:
        log.info("Impact payout enrichment: all campaigns already cached — nothing to fetch")
        return existing_cache

    log.info(f"Impact payout enrichment: fetching Actions for {len(missing)} new campaigns "
             f"({len(existing_cache)} already cached)...")

    merged = dict(existing_cache)
    _CATEGORY_TO_PTYPE = {
        "LEAD":  "CPL",
        "SALE":  "CPS",
        "CLICK": "CPC",
    }

    for idx, cid in enumerate(missing, 1):
        # Payout lives in the active contract, not a separate /Actions endpoint
        url = f"https://api.impact.com/Mediapartners/{IMPACT_SID}/Campaigns/{cid}/Contracts/Active"
        try:
            resp = requests.get(
                url,
                auth=(IMPACT_SID, IMPACT_TOKEN),
                headers={"Accept": "application/json"},
                params={"summary": "false"},
                timeout=30,
            )
            if resp.ok:
                data = resp.json()
                payouts = data.get("PayoutTermsList", [])
                if payouts:
                    best = payouts[0]  # first action type is the primary conversion
                    payout = str(best.get("PayoutAmount", ""))
                    category = best.get("TrackerType", "")
                    payout_type = _CATEGORY_TO_PTYPE.get(category, category)
                    merged[str(cid)] = {"payout": payout, "payout_type": payout_type}
                    if DEBUG and idx % 25 == 0:
                        log.info(f"  [{idx}/{len(missing)}] {cid}: ${payout} {payout_type}")
            elif resp.status_code == 404:
                merged[str(cid)] = {"payout": "", "payout_type": "", "payout_note": "Rate TBD"}
            else:
                log.warning(f"  Campaign {cid}: status {resp.status_code}")
        except Exception as e:
            log.warning(f"  Campaign {cid}: {e}")

        time.sleep(0.1)  # stay within Impact rate limits

    _save_payout_cache(merged)
    return merged


# ---------------------------------------------------------------------------
# NETWORK ADAPTERS
# ---------------------------------------------------------------------------

# --- Impact -----------------------------------------------------------------

def _fetch_impact_ads_data() -> dict:
    """
    Fetch Impact /Ads endpoint in a single pass.
    Returns cat_map: {campaign_id: label_string}  e.g. "Payments,Capital,US"
    """
    url = f"https://api.impact.com/Mediapartners/{IMPACT_SID}/Ads"
    cat_map = {}
    page = 1

    while True:
        resp = requests.get(
            url,
            auth=(IMPACT_SID, IMPACT_TOKEN),
            headers={"Accept": "application/json"},
            params={"PageSize": 1000, "Page": page},
            timeout=30,
        )
        if not resp.ok:
            log.warning(f"Impact Ads endpoint returned {resp.status_code} — ads data unavailable")
            break

        data = resp.json()
        ads = data.get("Ads", [])
        if not ads:
            break

        if DEBUG and page == 1:
            log.info(f"[DEBUG] Impact Ads raw first record:\n{json.dumps(ads[0], indent=2)}")

        for ad in ads:
            cid = str(ad.get("CampaignId", ""))
            if not cid:
                continue

            # Category labels
            labels = ad.get("Labels", "")
            if labels and cid not in cat_map:
                cat_map[cid] = labels

        total = int(data.get("@totalrecords", 0))
        if page * 1000 >= total:
            break
        page += 1

    log.info(f"Impact: ads data loaded — {len(cat_map)} categories")
    return cat_map


def _fetch_impact_campaigns_raw() -> list:
    """Fetch raw campaign dicts from Impact API (pagination only, no enrichment)."""
    url = f"https://api.impact.com/Mediapartners/{IMPACT_SID}/Campaigns"
    params = {"PageSize": 1000}
    campaigns_all = []
    page = 1
    while True:
        params["Page"] = page
        resp = requests.get(
            url,
            auth=(IMPACT_SID, IMPACT_TOKEN),
            headers={"Accept": "application/json"},
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        campaigns = data.get("Campaigns", [])
        if DEBUG and campaigns and page == 1:
            log.info(f"[DEBUG] Impact raw first record:\n{json.dumps(campaigns[0], indent=2)}")
        if not campaigns:
            break
        campaigns_all.extend(campaigns)
        total = int(data.get("@totalrecords", 0))
        if page * params["PageSize"] >= total:
            break
        page += 1
    return campaigns_all


def fetch_impact() -> list:
    """
    Impact REST API v13 — publisher campaigns list + payout enrichment from /Ads.
    Docs: https://developer.impact.com/default#operations-Ads-GetCampaigns
    """
    log.info("Impact: fetching campaigns...")
    campaigns_all = _fetch_impact_campaigns_raw()

    log.info("Impact: fetching ads data (categories)...")
    cat_map = _fetch_impact_ads_data()

    offers = []
    for c in campaigns_all:
        cid = str(c.get("CampaignId", ""))

        # geo: ShippingRegions is a list ["AUSTRALIA", "CANADA", "UK", "US"]
        regions = c.get("ShippingRegions", [])
        geo = ", ".join(r.title() for r in regions) if regions else "US"

        # Creative URLs — Impact provides logo and banner via LogoUri / ImageUri
        logo_url   = c.get("LogoUri", "") or c.get("LogoUrl", "") or ""
        image_url  = c.get("ImageUri", "") or c.get("BannerUri", "") or logo_url

        o = empty_offer()
        o["network"]           = "impact"
        o["offer_id"]          = cid
        o["advertiser"]        = c.get("CampaignName", "")
        o["title"]             = c.get("CampaignName", "")  # Impact uses campaign name as the offer headline
        o["description"]       = str(c.get("CampaignDescription", ""))[:500]
        o["mini_description"]  = str(c.get("CampaignDescription", ""))[:120]
        o["cta"]               = ""  # not provided by Impact API
        o["terms"]             = str(c.get("Terms", c.get("Restrictions", "")))[:500]
        o["payout"]            = ""    # requires /Campaigns/{id}/Actions — auto-enriched below
        o["payout_type"]       = ""
        o["currency"]          = "USD"
        o["category"]          = cat_map.get(cid, "")
        o["geo"]               = geo
        o["geo_raw"]           = ", ".join(regions) if regions else ""
        o["os_targeting"]      = c.get("MobileOptimized", "")  # True/False string
        o["platform_targeting"] = "Mobile" if c.get("MobileOptimized") else "All"
        o["tracking_url"]      = c.get("TrackingLink", "")
        o["preview_url"]       = c.get("CampaignUrl", "")
        o["icon_url"]          = logo_url
        o["hero_url"]          = image_url
        o["banner_url"]        = image_url
        o["status"]            = c.get("ContractStatus", "")
        o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
        offers.append(o)

    # Auto-enrich payouts: fetch Actions for any campaign not already in cache
    campaign_ids = [o["offer_id"] for o in offers]
    payout_cache = _run_impact_payout_enrichment(campaign_ids)

    # Merge payout data into offers
    merged_count = 0
    for o in offers:
        cid = o["offer_id"]
        if cid in payout_cache:
            raw = payout_cache[cid].get("payout", "")
            ptype = payout_cache[cid].get("payout_type", "")
            note = payout_cache[cid].get("payout_note", "")
            o["payout"] = raw
            o["payout_type"] = ptype
            # Build human-readable string e.g. "$2.00 CPL"
            if raw and raw != "0.00" and raw != "0":
                o["_raw_payout"] = f"${raw} {ptype}".strip()
            elif note:
                o["_raw_payout"] = note  # "Rate TBD" for uncontracted campaigns
            elif ptype:
                o["_raw_payout"] = ptype  # at least show the type (CPS, CPL, etc.)
            else:
                o["_raw_payout"] = ""
            merged_count += 1
        else:
            o["_raw_payout"] = ""
    log.info(f"Impact: {merged_count}/{len(offers)} offers enriched with payout data")

    log.info(f"Impact: {len(offers)} offers fetched")
    return offers


# --- FlexOffers -------------------------------------------------------------

def fetch_flexoffers() -> list:
    """
    FlexOffers REST API — approved advertisers list.
    Auth: ApiKey header (NOT query param — query param returns 401)
    Endpoint: GET https://api.flexoffers.com/advertisers
    Response root: {"results": [...]}
    """
    log.info("FlexOffers: fetching advertisers...")
    url = "https://api.flexoffers.com/advertisers"
    headers = {"ApiKey": FLEXOFFERS_API_KEY}
    params = {
        "domainId":          FLEXOFFERS_DOMAIN_ID,
        "ProgramStatus":     "Approved",       # required per API spec
        "ApplicationStatus": "Approved",
        "SortColumn":        "LastCommissionUpdated",
        "SortOrder":         "DESC",
        "Page":              1,
        "pageSize":          25,               # spec enum max: 5, 10, 25
    }
    offers = []

    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code in (401, 403):
            log.error(f"FlexOffers: auth failed ({resp.status_code}) — check FLEXOFFERS_API_KEY")
            break
        resp.raise_for_status()

        data = resp.json()
        items = data.get("results", data) if isinstance(data, dict) else data
        if not items:
            break

        for a in items:
            # Extract best payout from actions[] if available
            actions = a.get("actions") or []
            payout_str = ""
            payout_type = ""
            for act in actions:
                atype = act.get("actionType", "")
                if atype and atype.lower() != "non-commissionable":
                    payout_str  = act.get("commission", "")
                    payout_type = act.get("commissionType", "")
                    break
            # Fall back to top-level payout field (descriptive string)
            if not payout_str:
                payout_str = str(a.get("payout", ""))[:100]

            # Build human-readable raw payout (preserves context parse_payout() loses)
            raw_payout = f"{payout_str} {payout_type}".strip() if payout_type else payout_str

            logo = (a.get("logoUrl") or a.get("logo_url") or
                    a.get("thumbnailUrl") or a.get("imageUrl") or "")
            banner = a.get("bannerUrl") or a.get("banner_url") or logo
            geo_raw = a.get("country", "") or a.get("countries", "")
            if isinstance(geo_raw, list):
                geo_raw = ", ".join(geo_raw)

            o = empty_offer()
            o["network"]           = "flexoffers"
            o["offer_id"]          = str(a.get("id", ""))
            o["advertiser"]        = a.get("name", "")
            o["title"]             = a.get("headline") or a.get("name", "")
            o["description"]       = str(a.get("description", ""))[:500]
            o["mini_description"]  = str(a.get("shortDescription") or a.get("description", ""))[:120]
            o["cta"]               = a.get("ctaText") or a.get("cta", "")
            o["terms"]             = str(a.get("termsAndConditions") or a.get("restrictions", ""))[:500]
            o["payout"]            = payout_str
            o["payout_type"]       = payout_type
            o["_raw_payout"]       = raw_payout[:120]
            o["currency"]          = "USD"
            o["category"]          = a.get("categoryNames", "")
            o["geo"]               = a.get("country", "US")
            o["geo_raw"]           = geo_raw
            o["os_targeting"]      = a.get("mobileSupportedTypes") or a.get("deviceType", "All")
            o["platform_targeting"] = a.get("networkType", "All")
            o["tracking_url"]      = a.get("deeplinkURL", a.get("defaultUrl", ""))
            o["preview_url"]       = a.get("landingPageUrl") or a.get("defaultUrl", "")
            o["icon_url"]          = logo
            o["hero_url"]          = banner
            o["banner_url"]        = banner
            o["status"]            = a.get("applicationStatus", a.get("programStatus", ""))
            o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
            offers.append(o)

        if len(items) < 25:   # fewer than a full page = last page
            break
        params["Page"] += 1

    log.info(f"FlexOffers: {len(offers)} offers fetched")
    return offers


# --- MaxBounty --------------------------------------------------------------

def fetch_maxbounty() -> list:
    """
    MaxBounty REST API v1 — affiliate campaign list.
    Docs: https://affiliates.maxbounty.com (Redocly)
    Auth: POST /authentication → mb-api-token (expires every 2 hours)
    Campaigns: GET /campaigns/{list} with x-access-token header
    """
    BASE = "https://api.maxbounty.com/affiliates/api"

    # Step 1: Authenticate
    log.info("MaxBounty: authenticating via REST API...")
    try:
        auth_resp = requests.post(
            f"{BASE}/authentication",
            json={"email": MAXBOUNTY_EMAIL, "password": MAXBOUNTY_PASSWORD},
            headers={"Content-Type": "application/json"},
            timeout=20,
        )
        auth_resp.raise_for_status()
        auth_data = auth_resp.json()
    except Exception as e:
        log.error(f"MaxBounty: auth failed — {e}")
        return []

    token = auth_data.get("mb-api-token", "")
    if not token:
        log.error(f"MaxBounty: no token in auth response — {auth_data}")
        return []

    headers = {"x-access-token": token}

    # Step 2: Sweep multiple list types and deduplicate by campaign_id
    # "recentlyApproved" only shows new approvals — need broader lists for full picture
    LISTS_TO_FETCH = ["recentlyApproved", "top", "popular", "suggested", "bookmarked"]
    log.info(f"MaxBounty: fetching campaigns across {len(LISTS_TO_FETCH)} lists...")

    seen_ids = set()
    raw_campaigns = []
    limit = 100

    for list_name in LISTS_TO_FETCH:
        page = 1
        while True:
            try:
                resp = requests.get(
                    f"{BASE}/campaigns/{list_name}",
                    headers=headers,
                    params={"page": page, "limit": limit},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.warning(f"MaxBounty: {list_name} list failed (page {page}) — {e}")
                break

            if DEBUG and page == 1 and list_name == "top":
                log.info(f"[DEBUG] MaxBounty '{list_name}' raw first record:\n{json.dumps((data.get('campaigns') or [{}])[0], indent=2)[:1200]}")

            campaigns = data.get("campaigns", [])
            if not campaigns:
                break

            for c in campaigns:
                cid = str(c.get("campaign_id", ""))
                if cid and cid not in seen_ids:
                    seen_ids.add(cid)
                    raw_campaigns.append(c)

            if len(campaigns) < limit:
                break
            page += 1

        log.info(f"MaxBounty: {list_name} → {len(seen_ids)} unique campaigns so far")

    # Normalize to offer schema
    # List endpoint returns flat structure — fields are at top level
    offers = []
    for c in raw_campaigns:
        categories = c.get("categories") or []
        category   = ", ".join(str(x) for x in categories if x is not None)

        payout     = c.get("rate", "")
        rate_type  = c.get("rate_type", "")   # e.g. "$ per lead", "% of sale"

        thumbnail = c.get("thumbnail", "")
        banner = c.get("banner_image") or c.get("banner_url") or thumbnail
        geo_countries = c.get("countries") or c.get("geo") or []
        if isinstance(geo_countries, list):
            geo_raw = ", ".join(str(x) for x in geo_countries) if geo_countries else "US"
        else:
            geo_raw = str(geo_countries) or "US"
        os_raw = ", ".join(c.get("platforms") or []) if isinstance(c.get("platforms"), list) else (c.get("platforms") or "All")

        o = empty_offer()
        o["network"]           = "maxbounty"
        o["offer_id"]          = str(c.get("campaign_id", ""))
        o["advertiser"]        = c.get("name", "")
        o["title"]             = c.get("headline") or c.get("name", "")
        o["description"]       = str(c.get("description", ""))[:500]
        o["mini_description"]  = str(c.get("short_description") or c.get("description", ""))[:120]
        o["cta"]               = c.get("cta_text") or c.get("cta") or ""
        o["terms"]             = str(c.get("restrictions") or c.get("terms", ""))[:500]
        o["payout"]            = str(payout) if payout != "" else ""
        o["payout_type"]       = rate_type
        o["currency"]          = "USD"
        o["category"]          = category
        o["geo"]               = "US"
        o["geo_raw"]           = geo_raw
        o["os_targeting"]      = os_raw
        o["platform_targeting"] = "All"
        o["_raw_payout"]       = f"{payout} {rate_type}".strip() if payout != "" else ""
        o["tracking_url"]      = c.get("landing_page_sample", "")
        o["preview_url"]       = c.get("preview_url") or c.get("landing_page_sample", "")
        o["icon_url"]          = thumbnail
        o["hero_url"]          = banner
        o["banner_url"]        = banner
        o["status"]            = c.get("affiliate_campaign_status", c.get("status", ""))
        o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
        offers.append(o)

    log.info(f"MaxBounty: {len(offers)} offers fetched")
    return offers



# ---------------------------------------------------------------------------
# DATA NORMALIZATION — applied before writing to any destination
# ---------------------------------------------------------------------------

_US_VARIANTS    = {"us", "usa", "unitedstates"}
_CA_VARIANTS    = {"ca", "canada"}
_UK_VARIANTS    = {"uk", "gb", "unitedkingdom", "greatbritain"}
_EU_COUNTRIES   = {"france", "germany", "spain", "italy", "portugal", "netherlands",
                   "belgium", "austria", "sweden", "denmark", "finland", "norway",
                   "poland", "czechrepublic", "hungary", "romania", "greece", "ireland"}

def normalize_geo(raw: str) -> str:
    """Collapse country-list blobs into a clean Notion select value."""
    if not raw or not raw.strip():
        return "Unknown"
    parts = {p.strip().lower().replace(" ", "") for p in raw.split(",")}
    parts = {p for p in parts if p}
    n = len(parts)
    if not n:
        return "Unknown"
    has_us = bool(parts & _US_VARIANTS)
    has_ca = bool(parts & _CA_VARIANTS)
    has_uk = bool(parts & _UK_VARIANTS)
    if n == 1:
        if has_us: return "US Only"
        if has_uk: return "UK"
        return "Other"
    if n == 2 and has_us and has_ca:
        return "US + CA"
    if n <= 4 and parts <= (_US_VARIANTS | _CA_VARIANTS | {"mexico", "puertorico"}):
        return "North America"
    if n > 5:
        return "Global"   # 6+ countries = treat as global
    if has_us:
        return "US + CA" if has_ca else "US Only"
    if len(parts & _EU_COUNTRIES) >= n - 1:
        return "EU"
    return "Other"


_CATEGORY_KEYWORDS = {
    "Finance":           ["financ", "banking", "credit", "loan", "mortgage", "lending", "invest", "capital", "payment", "money", "borrow"],
    "Insurance":         ["insur"],
    "Tax Services":      ["tax"],
    "Travel":            ["travel", "hotel", "flight", "vacation", "cruise", "airline", "lodg", "resort"],
    "Health & Wellness": ["health", "wellness", "medical", "dental", "fitness", "diet", "supplement", "pharma", "rx", "vitamin"],
    "Beauty":            ["beauty", "cosmet", "skincare", "makeup", "hair", "grooming"],
    "Retail":            ["retail", "shop", "gift", "fashion", "apparel", "cloth", "store", "flower"],
    "Food & Dining":     ["food", "dining", "restaurant", "meal", "grocery", "delivery", "pizza"],
    "Technology":        ["tech", "software", "app ", "digital", "cyber", "saas", "cloud", "internet", "vpn"],
    "Education":         ["educat", "learn", "course", "tutor", "school", "college", "university", "degree"],
    "Gaming":            ["gaming", "game", "casino", "gambl"],
    "Surveys":           ["survey", " doi", "opinion", "poll"],
    "Loyalty & Rewards": ["loyalty", "reward", "cashback", "point", "perks", "miles", "swagbuck", "mypoints", "inboxdollar"],
    "Home & Garden":     ["home", "garden", "furniture", "decor", "kitchen", "interior", "mattress"],
    "Automotive":        ["auto", " car ", "vehicle", "truck", "motor", "tire"],
    "Legal":             ["legal", "attorney", "law", "lawyer", "lawsuit"],
    "Business Services": ["business", "b2b", "marketing", "seo", "crm", "erp", "accounting", "hr ", "payroll"],
}

def normalize_categories(raw: str, advertiser: str = "") -> list:
    """Map raw category string to canonical Notion multi-select values."""
    if not raw and not advertiser:
        return ["Other"]
    text = (raw + " " + advertiser).lower()
    matched = []
    for cat, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            matched.append(cat)
    return matched[:3] if matched else ["Other"]  # cap at 3 tags


def parse_payout(raw_payout: str, raw_type: str) -> tuple:
    """Return (numeric_float_or_None, normalized_payout_type_str)."""
    ptype_map = {
        "percentage": "% of Sale",
        "% of sale": "% of Sale",
        "% per sale": "% of Sale",
        "per sale": "% of Sale",
        "cps": "% of Sale",
        "revshare": "% of Sale",
        "per lead": "$ per Lead",
        "$ per lead": "$ per Lead",
        "cpl": "$ per Lead",
        "per click": "$ per Click",
        "$ per click": "$ per Click",
        "cpc": "$ per Click",
        "fixed": "Fixed",
        "flat": "Fixed",
    }
    norm_type = "Unknown"
    for key, val in ptype_map.items():
        if key in raw_type.lower():
            norm_type = val
            break

    num = None
    if raw_payout:
        # Strip % and $ signs, find the first number
        nums = re.findall(r"[\d]+(?:\.\d+)?", raw_payout.replace(",", ""))
        if nums:
            parsed = float(nums[0])
            num = parsed if parsed > 0 else None  # 0 payout has no signal value
        if "%" in raw_payout and norm_type == "Unknown":
            norm_type = "% of Sale"
        elif "$" in raw_payout and norm_type == "Unknown":
            norm_type = "$ per Lead"

    return num, norm_type


def normalize_status(raw: str) -> str:
    s = raw.lower().strip()
    if any(x in s for x in ["active", "approved", "live"]):
        return "Active"
    if any(x in s for x in ["expir", "inactive", "deactivat", "ended"]):
        return "Expired"
    if any(x in s for x in ["pending", "approv", "review", "required"]):
        return "Pending Approval"
    return "Unknown"


# ---------------------------------------------------------------------------
# CLICKHOUSE — MS campaign index for offer matching
# ---------------------------------------------------------------------------

def fetch_ms_campaign_index() -> dict:
    """
    Query ClickHouse for all MS internal campaigns and return two lookup dicts:
      - by_impact_id: {impact_campaign_id_str: {adv_name, status, is_live}}
      - by_name:      {normalized_adv_name:     {adv_name, status, is_live}}

    Impact campaign IDs are stored in from_airbyte_campaigns.internal_network_name.
    is_live = True when a publisher_campaign row exists with is_active=True.

    Returns empty dicts (graceful degradation) if CH credentials are missing or query fails.
    """
    if not CH_HOST or not CH_USER or not CH_PASSWORD:
        log.warning("ClickHouse credentials not configured — skipping MS campaign matching")
        return {"by_impact_id": {}, "by_name": {}}

    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=CH_HOST, user=CH_USER, password=CH_PASSWORD,
            database=CH_DATABASE, secure=True,
        )

        query = """
            SELECT
                trim(c.internal_network_name) AS impact_id,
                c.adv_name,
                trim(c.status)                AS status,
                (pc.campaign_id IS NOT NULL)  AS is_live
            FROM default.from_airbyte_campaigns AS c
            LEFT JOIN (
                SELECT DISTINCT campaign_id
                FROM default.from_airbyte_publisher_campaigns
                WHERE is_active = true
            ) AS pc ON c.id = pc.campaign_id
            WHERE c.deleted_at IS NULL
        """
        result = client.query(query)

        by_impact_id = {}
        by_name = {}
        _strip_re = re.compile(r"[^a-z0-9 ]")

        for row in result.result_rows:
            impact_id, adv_name, status, is_live = row
            entry = {
                "adv_name": adv_name or "",
                "status":   status or "",
                "is_live":  bool(is_live),
            }
            # ID-based index (for Impact exact match)
            if impact_id:
                by_impact_id[impact_id] = entry
            # Name-based index (for FlexOffers/MaxBounty fuzzy match)
            if adv_name:
                norm = _strip_re.sub("", adv_name.lower()).strip()
                # Strip common suffixes that vary across networks
                for suffix in [" inc", " llc", " ltd", " corp", " com", " us"]:
                    if norm.endswith(suffix):
                        norm = norm[:-len(suffix)].strip()
                if norm:
                    by_name[norm] = entry

        log.info(f"MS campaign index: {len(by_impact_id)} Impact matches, {len(by_name)} name entries")
        return {"by_impact_id": by_impact_id, "by_name": by_name}

    except Exception as e:
        log.warning(f"ClickHouse query failed — skipping MS campaign matching: {e}")
        return {"by_impact_id": {}, "by_name": {}}


def match_ms_status(offer: dict, ms_index: dict) -> tuple:
    """
    Match an offer against the MS campaign index.
    Returns (ms_status, ms_internal_name) where ms_status is one of:
      "Live"                — in MS system, active, deployed to publishers
      "In System"           — in MS system, active, but not currently deployed
      "In System (Inactive)"— in MS system but campaign is inactive
      "Not in System"       — no match found
    """
    by_impact_id = ms_index.get("by_impact_id", {})
    by_name      = ms_index.get("by_name", {})

    match = None

    if offer.get("network") == "impact":
        match = by_impact_id.get(str(offer.get("offer_id", "")))

    if match is None and by_name:
        # Normalize advertiser name the same way we built the index
        raw_name = offer.get("advertiser", "")
        norm = re.sub(r"[^a-z0-9 ]", "", raw_name.lower()).strip()
        for suffix in [" inc", " llc", " ltd", " corp", " com", " us"]:
            if norm.endswith(suffix):
                norm = norm[:-len(suffix)].strip()
        match = by_name.get(norm)

    if match is None:
        return "Not in System", None

    status   = match["status"].lower()
    is_live  = match["is_live"]
    adv_name = match["adv_name"]

    if "active" in status and is_live:
        return "Live", adv_name
    elif "active" in status:
        return "In System", adv_name
    else:
        return "In System (Inactive)", adv_name


def clean_offers(offers: list, ms_index: dict = None) -> list:
    """Apply all normalizations. Returns cleaned list, active-only by default."""
    if ms_index is None:
        ms_index = {"by_impact_id": {}, "by_name": {}}
    cleaned = []
    for o in offers:
        status = normalize_status(o.get("status", ""))
        # Drop expired — they pollute actionable views
        if status == "Expired":
            continue
        geo = normalize_geo(o.get("geo", ""))
        categories = normalize_categories(o.get("category", ""), o.get("advertiser", ""))
        payout_num, payout_type_norm = parse_payout(
            str(o.get("payout", "")), str(o.get("payout_type", ""))
        )
        ms_status, ms_internal_name = match_ms_status(o, ms_index)
        cleaned.append({
            **o,
            "status":            status,
            "geo":               geo,
            "category":          ", ".join(categories),
            "_categories":       categories,          # list form for Notion multi-select
            "_payout_num":       payout_num,
            "_payout_type_norm": payout_type_norm,
            "_raw_payout":       o.get("_raw_payout", ""),  # preserve through cleaning
            "_unique_key":       f"{o['network']}:{o['offer_id']}",
            "_ms_status":        ms_status,
            "_ms_internal_name": ms_internal_name or "",
        })
    return cleaned


# ---------------------------------------------------------------------------
# NOTION WRITER — upsert by unique_key (network:offer_id)
# ---------------------------------------------------------------------------

def _notion_headers() -> dict:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }

def _notion_properties(o: dict, is_new: bool = False) -> dict:
    props = {
        "Advertiser":    {"title": [{"text": {"content": o.get("advertiser", "")[:100]}}]},
        "Network":       {"select": {"name": o.get("network", "")}},
        "Status":        {"select": {"name": o.get("status", "Unknown")}},
        "Geo":           {"select": {"name": o.get("geo", "Unknown")}},
        "Description":   {"rich_text": [{"text": {"content": o.get("description", "")[:2000]}}]},
        "Unique Key":    {"rich_text": [{"text": {"content": o.get("_unique_key", "")}}]},
        "Offer ID":      {"rich_text": [{"text": {"content": str(o.get("offer_id", ""))}}]},
        "Payout Type":   {"select": {"name": o.get("_payout_type_norm", "Unknown")}},
        "Date Added":    {"date": {"start": o.get("date_scraped", datetime.today().strftime("%Y-%m-%d"))}},
    }
    # Categories (multi-select)
    cats = o.get("_categories", [])
    if cats:
        props["Category"] = {"multi_select": [{"name": c} for c in cats]}
    # Payout (number)
    pn = o.get("_payout_num")
    if pn is not None:
        props["Payout"] = {"number": pn}
    # Tracking URL
    url = o.get("tracking_url", "")
    if url and url.startswith("http"):
        props["Tracking URL"] = {"url": url}
    # Raw Payout — human-readable payout string from the API (e.g. "$12.00 CPA", "5-15% Percentage")
    raw_payout = o.get("_raw_payout", "")
    # For Impact offers with no payout info, label explicitly so team knows it's missing data, not a bug
    if not raw_payout and o.get("network") == "impact":
        raw_payout = "Rate TBD"
    if raw_payout:
        props["Raw Payout"] = {"rich_text": [{"text": {"content": raw_payout[:200]}}]}
    # MS Status — system-derived, always written (not a manual field)
    ms_status = o.get("_ms_status", "")
    if ms_status:
        props["MS Status"] = {"select": {"name": ms_status}}
    # MS Internal Name — the adv_name from ClickHouse so team can cross-reference internally
    ms_name = o.get("_ms_internal_name", "")
    if ms_name:
        props["MS Internal Name"] = {"rich_text": [{"text": {"content": ms_name[:200]}}]}
    # Outreach Status — only set on new pages to preserve manual team edits
    if is_new:
        props["Outreach Status"] = {"select": {"name": "Not Reviewed"}}
    return props

def write_notion(offers: list):
    """Upsert all offers into Notion. Creates new pages, updates existing ones."""
    if not NOTION_TOKEN:
        log.warning("NOTION_TOKEN not set — skipping Notion write. Set env var to enable.")
        return

    headers = _notion_headers()
    base = "https://api.notion.com/v1"

    # 1. Fetch all existing pages keyed by unique_key
    log.info("Notion: loading existing offer index...")
    existing = {}  # unique_key → page_id
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        resp = requests.post(
            f"{base}/databases/{NOTION_DB_ID}/query",
            headers=headers, json=body, timeout=30
        )
        if not resp.ok:
            log.error(f"Notion query failed: {resp.status_code} {resp.text[:200]}")
            return
        data = resp.json()
        for page in data.get("results", []):
            key_prop = page.get("properties", {}).get("Unique Key", {})
            rt = key_prop.get("rich_text", [])
            if rt:
                key = rt[0].get("plain_text", "")
                if key:
                    existing[key] = page["id"]
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.35)

    log.info(f"Notion: {len(existing)} existing pages found")

    # 2. Upsert each offer
    created = updated = errors = 0
    for o in offers:
        ukey = o.get("_unique_key", "")
        try:
            if ukey in existing:
                # Update existing page — do NOT set Outreach Status (preserve team edits)
                props = _notion_properties(o, is_new=False)
                r = requests.patch(
                    f"{base}/pages/{existing[ukey]}",
                    headers=headers,
                    json={"properties": props},
                    timeout=30
                )
                if r.ok:
                    updated += 1
                else:
                    log.warning(f"Notion update failed [{ukey}]: {r.status_code}")
                    errors += 1
            else:
                # Create new page — set Outreach Status to "Not Reviewed"
                props = _notion_properties(o, is_new=True)
                r = requests.post(
                    f"{base}/pages",
                    headers=headers,
                    json={"parent": {"database_id": NOTION_DB_ID}, "properties": props},
                    timeout=30
                )
                if r.ok:
                    created += 1
                else:
                    log.warning(f"Notion create failed [{ukey}]: {r.status_code}")
                    errors += 1
        except Exception as e:
            log.warning(f"Notion error [{ukey}]: {e}")
            errors += 1
        time.sleep(0.4)  # stay safely under 3 req/sec rate limit

        # Progress log every 50 offers so the terminal doesn't look stuck
        n = created + updated + errors
        if n % 50 == 0:
            log.info(f"Notion: {n}/{len(offers)} processed ({created} created, {updated} updated, {errors} errors)...")

    log.info(f"Notion: {created} created, {updated} updated, {errors} errors")




# ---------------------------------------------------------------------------
# CJ Affiliate (Commission Junction)
# ---------------------------------------------------------------------------

def fetch_cj() -> list:
    """
    CJ Affiliate — two-source scraper:
    1. publisherCommissions GraphQL → actual commission rates per advertiser (best payout signal)
    2. REST link-search → real tracking URLs (3,755 text links across joined programs)

    Auth: Authorization: Bearer {CJ_API_KEY}
    CID (CJ_WEBSITE_ID): company account ID — for commission query
    PID (CJ_PUBLISHER_ID): website property ID — for link-search tracking URLs
    """
    import xml.etree.ElementTree as ET

    if not CJ_API_KEY:
        log.warning("CJ: CJ_API_KEY not set — skipping")
        return []

    HEADERS     = {"Authorization": f"Bearer {CJ_API_KEY}"}
    pid         = CJ_PUBLISHER_ID or ""
    cid         = CJ_WEBSITE_ID or ""

    # ── Step 1: Commission rates per advertiser (last 12 months) ──────────────
    # Groups by advertiser to get max payout seen — better signal than EPC
    commission_by_adv: dict[str, dict] = {}   # advertiser_id → {amount, type, name}
    if cid:
        log.info("CJ: fetching commission rates from publisherCommissions API...")
        try:
            cr = requests.post(
                "https://commissions.api.cj.com/query",
                headers={**HEADERS, "Content-Type": "application/json"},
                json={"query": f"""{{
                  publisherCommissions(
                    forPublishers: ["{cid}"]
                    sincePostingDate: "2024-01-01T00:00:00Z"
                  ) {{
                    count
                    records {{
                      advertiserId advertiserName actionType
                      pubCommissionAmountUsd
                    }}
                  }}
                }}"""},
                timeout=30,
            )
            if cr.ok:
                data = cr.json()
                records = (data.get("data") or {}).get("publisherCommissions", {}).get("records") or []
                for rec in records:
                    adv_id  = str(rec.get("advertiserId", ""))
                    amt     = float(rec.get("pubCommissionAmountUsd") or 0)
                    atype   = rec.get("actionType", "")
                    adv_name = rec.get("advertiserName", "")
                    prev = commission_by_adv.get(adv_id, {})
                    if amt > prev.get("amount", 0):
                        commission_by_adv[adv_id] = {"amount": amt, "type": atype, "name": adv_name}
                log.info(f"CJ: commission data loaded for {len(commission_by_adv)} advertisers")
        except Exception as e:
            log.warning(f"CJ: commission query failed (non-fatal) — {e}")

    # ── Step 2: Text links with real tracking URLs ────────────────────────────
    if not pid:
        log.warning("CJ: CJ_PUBLISHER_ID not set — using program URLs only (no tracked links)")

    offers: list[dict] = []
    seen_offer_ids: set[str] = set()
    page_num = 1

    while True:
        params: dict = {
            "website-id":       pid or cid,
            "advertiser-ids":   "joined",
            "link-type":        "Text Link",
            "records-per-page": 100,
            "page-number":      page_num,
        }
        try:
            resp = requests.get(
                "https://linksearch.api.cj.com/v2/link-search",
                headers=HEADERS, params=params, timeout=30,
            )
            if resp.status_code in (400, 401, 403):
                log.warning(f"CJ link-search: {resp.status_code} — {resp.text[:200]}")
                break
            resp.raise_for_status()
        except Exception as e:
            log.warning(f"CJ link-search failed (page {page_num}): {e}")
            break

        try:
            lroot = ET.fromstring(resp.text)
        except ET.ParseError:
            break

        links = list(lroot.iter("link"))
        if not links:
            break

        for link in links:
            link_id     = link.findtext("link-id", "").strip()
            if link_id in seen_offer_ids:
                continue
            seen_offer_ids.add(link_id)

            adv_id      = link.findtext("advertiser-id", "").strip()
            adv_name    = link.findtext("advertiser-name", "").strip()
            category    = link.findtext("category", "").strip()
            link_name   = link.findtext("link-name", "").strip()
            description = link.findtext("description", "").strip()[:200]
            destination = link.findtext("destination", "").strip()
            sale_comm   = link.findtext("sale-commission", "").strip()
            lead_comm   = link.findtext("lead-commission", "").strip()
            click_comm  = link.findtext("click-commission", "").strip()

            # Build tracking URL from link-code-html (extract href)
            link_html   = link.findtext("link-code-html", "").strip()
            tracking_url = destination  # fallback
            if link_html and 'href="' in link_html:
                start = link_html.index('href="') + 6
                end   = link_html.index('"', start)
                tracking_url = link_html[start:end]

            # Payout: prefer commission data, then sale/lead commission from link
            comm_info = commission_by_adv.get(adv_id, {})
            if comm_info.get("amount", 0) > 0:
                payout_amt  = str(comm_info["amount"])
                payout_type = "CPA"
                raw_payout  = f"${comm_info['amount']} {comm_info.get('type', 'CPA')}"
            elif lead_comm:
                payout_amt  = lead_comm
                payout_type = "CPL"
                raw_payout  = f"{lead_comm} CPL" if "%" in lead_comm else f"${lead_comm} CPL"
            elif sale_comm:
                payout_amt  = sale_comm
                payout_type = "CPS"
                raw_payout  = f"{sale_comm} CPS" if "%" in sale_comm else f"${sale_comm} CPS"
            else:
                payout_amt  = click_comm or "0"
                payout_type = "CPC"
                raw_payout  = f"${click_comm} CPC" if click_comm and click_comm not in ("0", "0.0") else "Commission varies"

            # CJ links include image URLs
            img_url = link.findtext("img-url", "").strip()
            img_src = link.findtext("img", "").strip()
            banner  = img_url or img_src

            o = empty_offer()
            o["network"]           = "cj"
            o["offer_id"]          = link_id
            o["advertiser"]        = adv_name
            o["title"]             = link_name or adv_name
            o["description"]       = description
            o["mini_description"]  = description[:120]
            o["cta"]               = ""  # CJ text links have name but no explicit CTA field
            o["terms"]             = ""
            o["payout"]            = payout_amt
            o["payout_type"]       = payout_type
            o["_raw_payout"]       = raw_payout[:120]
            o["currency"]          = "USD"
            o["category"]          = category
            o["geo"]               = "US"
            o["geo_raw"]           = "US"
            o["os_targeting"]      = "All"
            o["platform_targeting"] = "All"
            o["tracking_url"]      = tracking_url
            o["preview_url"]       = destination
            o["icon_url"]          = banner
            o["hero_url"]          = banner
            o["banner_url"]        = banner
            o["status"]            = "Active"
            o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
            offers.append(o)

        if len(links) < 100:
            break
        page_num += 1

    log.info(f"CJ: {len(offers)} text links fetched")
    return offers


# ---------------------------------------------------------------------------
# ShareASale
# ---------------------------------------------------------------------------

def fetch_shareasale() -> list:
    """
    ShareASale — publisher affiliate programs.
    Uses ShareASale REST API v1.0 with HMAC-SHA256 authentication.
    Publisher ID: 3279349
    Docs: https://api.shareasale.com/r.cfm?b=1433704&u=3279349&m=47189&urllink=&afftrack=
    """
    import hashlib
    import hmac
    from datetime import datetime, timezone

    if not SHAREASALE_API_TOKEN or not SHAREASALE_API_SECRET:
        log.warning("ShareASale: SHAREASALE_API_TOKEN or SHAREASALE_API_SECRET not set — skipping")
        return []

    aff_id   = SHAREASALE_AFFILIATE_ID
    token    = SHAREASALE_API_TOKEN
    secret   = SHAREASALE_API_SECRET

    def _auth_headers(action: str) -> dict:
        ts = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        sig_str = f"{token}:{ts}:{action}:{secret}"
        sig = hmac.new(secret.encode(), sig_str.encode(), hashlib.sha256).hexdigest()
        return {
            "x-ShareASale-APIToken": token,
            "x-ShareASale-Date": ts,
            "x-ShareASale-Authentication": sig,
        }

    offers: list[dict] = []
    page = 1

    while True:
        try:
            params = {
                "affiliateId": aff_id,
                "action":      "getMerchants",
                "version":     "2.8",
                "pageNumber":  page,
            }
            headers = _auth_headers("getMerchants")
            resp = requests.get(
                f"https://api.shareasale.com/x.cfm",
                headers=headers,
                params=params,
                timeout=30,
            )
            if not resp.ok:
                log.warning(f"ShareASale: API {resp.status_code} — {resp.text[:200]}")
                break
        except Exception as e:
            log.warning(f"ShareASale: request failed — {e}")
            break

        try:
            data = resp.json()
        except Exception:
            log.warning(f"ShareASale: non-JSON response — {resp.text[:200]}")
            break

        merchants = data if isinstance(data, list) else data.get("data", [])
        if not merchants:
            break

        for m in merchants:
            merchant_id = str(m.get("merchantId") or m.get("MerchantID", ""))
            name        = m.get("name") or m.get("Name") or m.get("MerchantName", "")
            category    = m.get("category") or m.get("Category", "")
            commission  = m.get("commissionRate") or m.get("CommissionRate", "")
            status      = m.get("activationStatus", "active")
            url         = m.get("url") or m.get("Website", "")

            if not merchant_id or str(status).lower() not in ("active", "1", "true"):
                continue

            payout_str = str(commission).strip()
            if "%" in payout_str:
                ptype, raw = "CPS", f"{payout_str} CPS"
            elif payout_str and payout_str not in ("0", "0.0"):
                ptype, raw = "CPL", f"${payout_str} CPL"
            else:
                ptype, raw = "CPA", "Commission varies"

            tracking = f"https://www.shareasale.com/r.cfm?b=1&u={aff_id}&m={merchant_id}"

            logo = m.get("logoUrl") or m.get("LogoURL") or m.get("thumbnail", "")
            desc_text = m.get("description") or m.get("Description", "")

            o = empty_offer()
            o["network"]           = "shareasale"
            o["offer_id"]          = merchant_id
            o["advertiser"]        = name
            o["title"]             = name
            o["description"]       = (desc_text or "")[:500]
            o["mini_description"]  = (desc_text or "")[:120]
            o["cta"]               = ""
            o["terms"]             = str(m.get("termsAndConditions") or m.get("Terms", ""))[:500]
            o["payout"]            = payout_str
            o["payout_type"]       = ptype
            o["_raw_payout"]       = raw[:120]
            o["currency"]          = "USD"
            o["category"]          = category
            o["geo"]               = "US"
            o["geo_raw"]           = "US"
            o["os_targeting"]      = "All"
            o["platform_targeting"] = "All"
            o["tracking_url"]      = tracking
            o["preview_url"]       = url
            o["icon_url"]          = logo
            o["hero_url"]          = logo
            o["banner_url"]        = logo
            o["status"]            = "Active"
            o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
            offers.append(o)

        if len(merchants) < 50:
            break
        page += 1

    log.info(f"ShareASale: {len(offers)} merchant programs fetched")
    return offers


# ---------------------------------------------------------------------------
# Rakuten Advertising
# ---------------------------------------------------------------------------

def fetch_rakuten() -> list:
    """
    Rakuten Advertising (formerly LinkShare) — publisher programs.
    REST API: https://api.rakutenmarketing.com/1.0/publisher/advertisers
    OAuth Bearer token required.
    Publisher ID: 3948979
    """
    if not RAKUTEN_API_TOKEN:
        log.warning("Rakuten: RAKUTEN_API_TOKEN not set — skipping")
        return []

    headers = {
        "Authorization": f"Bearer {RAKUTEN_API_TOKEN}",
        "Accept": "application/json",
    }

    offers: list[dict] = []
    page = 1

    while True:
        try:
            resp = requests.get(
                "https://api.rakutenmarketing.com/1.0/publisher/advertisers",
                headers=headers,
                params={"network": "2", "page": page, "limit": 100},
                timeout=30,
            )
            if resp.status_code == 401:
                log.warning("Rakuten: 401 Unauthorized — check RAKUTEN_API_TOKEN")
                break
            if not resp.ok:
                log.warning(f"Rakuten: API {resp.status_code} — {resp.text[:200]}")
                break
        except Exception as e:
            log.warning(f"Rakuten: request failed — {e}")
            break

        try:
            data = resp.json()
        except Exception:
            log.warning(f"Rakuten: non-JSON response — {resp.text[:200]}")
            break

        advertisers = data.get("advertisers") or data.get("data") or (data if isinstance(data, list) else [])
        if not advertisers:
            break

        for a in advertisers:
            adv_id   = str(a.get("id") or a.get("advertiserId", ""))
            name     = a.get("name") or a.get("advertiserName", "")
            category = a.get("category") or a.get("vertical", "")
            status   = a.get("status", "")
            site_url = a.get("siteUrl") or a.get("websiteUrl", "")

            if not adv_id or str(status).lower() not in ("active", "approved", "joined", "1", ""):
                continue

            # Rakuten publisher link format
            mid     = a.get("mid") or adv_id
            tracking = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_PUBLISHER_ID}&mid={mid}&murl={site_url}"

            commission = a.get("commissionRate") or a.get("baseCommission", "")
            payout_str = str(commission).strip()
            if "%" in payout_str:
                ptype, raw = "CPS", f"{payout_str} CPS"
            elif payout_str and payout_str not in ("0", "0.0"):
                ptype, raw = "CPA", f"${payout_str} CPA"
            else:
                ptype, raw = "CPA", "Commission varies"

            logo = a.get("logoUrl") or a.get("bannerUrl") or a.get("imageUrl", "")
            desc_text = a.get("description", "")

            o = empty_offer()
            o["network"]           = "rakuten"
            o["offer_id"]          = adv_id
            o["advertiser"]        = name
            o["title"]             = name
            o["description"]       = (desc_text or "")[:500]
            o["mini_description"]  = (desc_text or "")[:120]
            o["cta"]               = ""
            o["terms"]             = ""
            o["payout"]            = payout_str
            o["payout_type"]       = ptype
            o["_raw_payout"]       = raw[:120]
            o["currency"]          = "USD"
            o["category"]          = category
            o["geo"]               = "US"
            o["geo_raw"]           = "US"
            o["os_targeting"]      = "All"
            o["platform_targeting"] = "All"
            o["tracking_url"]      = tracking
            o["preview_url"]       = site_url
            o["icon_url"]          = logo
            o["hero_url"]          = logo
            o["banner_url"]        = logo
            o["status"]            = "Active"
            o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
            offers.append(o)

        if len(advertisers) < 100:
            break
        page += 1

    log.info(f"Rakuten: {len(offers)} advertiser programs fetched")
    return offers


# ---------------------------------------------------------------------------
# Awin
# ---------------------------------------------------------------------------

def fetch_awin() -> list:
    """
    Awin (formerly Affiliate Window) — publisher programmes.
    REST API: https://api.awin.com/publishers/{publisherId}/programmes
    API key from awin.com dashboard → API Credentials tab.
    """
    if not AWIN_API_KEY or not AWIN_PUBLISHER_ID:
        log.warning("Awin: AWIN_API_KEY or AWIN_PUBLISHER_ID not set — skipping")
        return []

    headers = {
        "Authorization": f"Bearer {AWIN_API_KEY}",
        "Accept": "application/json",
    }

    offers: list[dict] = []

    try:
        resp = requests.get(
            f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmes",
            headers=headers,
            params={"relationship": "joined", "countryCode": "US"},
            timeout=60,
        )
        if resp.status_code == 401:
            log.warning("Awin: 401 — check AWIN_API_KEY")
            return []
        if not resp.ok:
            log.warning(f"Awin: {resp.status_code} — {resp.text[:200]}")
            return []
    except Exception as e:
        log.warning(f"Awin: request failed — {e}")
        return []

    try:
        programmes = resp.json()
    except Exception:
        log.warning(f"Awin: non-JSON response — {resp.text[:200]}")
        return []

    if isinstance(programmes, dict):
        programmes = programmes.get("programmes") or programmes.get("data") or []

    for p in programmes:
        prog_id  = str(p.get("id") or p.get("programId", ""))
        name     = p.get("name") or p.get("programName", "")
        category = p.get("primarySector") or p.get("category", "")
        ctype    = p.get("commissionRange", {})
        comm_max = ctype.get("max") if isinstance(ctype, dict) else None
        site_url = p.get("primaryUrl") or p.get("url", "")

        if not prog_id or not name:
            continue

        payout_str = str(comm_max or "").strip() if comm_max is not None else ""
        ptype = "CPS"  # Awin is primarily revenue share
        raw   = f"{payout_str}% CPS" if payout_str else "Commission varies"

        tracking = f"https://www.awin1.com/cread.php?awinmid={prog_id}&awinaffid={AWIN_PUBLISHER_ID}&p={site_url}"

        desc_long  = p.get("longDescription") or p.get("description", "")
        desc_short = p.get("shortDescription") or desc_long
        logo_large = p.get("logoUrlLarge") or p.get("displayUrl", "")
        logo_small = p.get("logoUrl") or logo_large
        geo_raw    = p.get("primaryRegion") or p.get("countryCode", "US")

        o = empty_offer()
        o["network"]           = "awin"
        o["offer_id"]          = prog_id
        o["advertiser"]        = name
        o["title"]             = name
        o["description"]       = desc_long[:500]
        o["mini_description"]  = desc_short[:120]
        o["cta"]               = ""
        o["terms"]             = str(p.get("terms") or p.get("termsUrl", ""))[:200]
        o["payout"]            = payout_str
        o["payout_type"]       = ptype
        o["_raw_payout"]       = raw[:120]
        o["currency"]          = "USD"
        o["category"]          = category
        o["geo"]               = "US"
        o["geo_raw"]           = geo_raw
        o["os_targeting"]      = "All"
        o["platform_targeting"] = "All"
        o["tracking_url"]      = tracking
        o["preview_url"]       = site_url
        o["icon_url"]          = logo_small
        o["hero_url"]          = logo_large
        o["banner_url"]        = logo_large
        o["status"]            = "Active"
        o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
        offers.append(o)

    log.info(f"Awin: {len(offers)} programmes fetched")
    return offers


# ---------------------------------------------------------------------------
# TUNE / HasOffers (multi-instance)
# ---------------------------------------------------------------------------

def fetch_tune_instance(label: str, network_id: str, api_key: str, base_url: str) -> list:
    """
    TUNE/HasOffers publisher API — single instance.
    Uses HasOffers V3 JSON API: {NetworkId}.api.hasoffers.com/Apiv3/json
    Auth: NetworkAffiliateToken (publisher's API key from their dashboard)
    The base_url should be: https://{NetworkId}.api.hasoffers.com
    """
    offers: list[dict] = []
    page = 1

    while True:
        try:
            resp = requests.get(
                f"{base_url.rstrip('/')}/Apiv3/json",
                params={
                    "Target":                 "Affiliate_Offer",
                    "Method":                 "findAll",
                    "NetworkAffiliateToken":  api_key,
                    "filters[status]":        "active",
                    "page":                   page,
                    "limit":                  500,
                },
                timeout=30,
            )
            if resp.status_code in (401, 403):
                log.warning(f"TUNE/{label}: {resp.status_code} — check credentials")
                break
            if not resp.ok:
                log.warning(f"TUNE/{label}: {resp.status_code} — {resp.text[:200]}")
                break
        except Exception as e:
            log.warning(f"TUNE/{label}: request error — {e}")
            break

        try:
            data = resp.json()
        except Exception:
            log.warning(f"TUNE/{label}: non-JSON response — {resp.text[:200]}")
            break

        # HasOffers V3 response: {request:{}, response:{status:1, data:{count:N, data:{id: {Offer:{...}}, ...}}}}
        resp_body = data.get("response", data)
        if resp_body.get("status") == -1:
            errs = resp_body.get("errors", [])
            log.warning(f"TUNE/{label}: API error — {errs}")
            break
        inner  = resp_body.get("data", {})
        # data.data is a dict keyed by offer_id in HasOffers V3
        raw    = inner.get("data", {}) if isinstance(inner, dict) else {}
        if isinstance(raw, dict):
            records = list(raw.values())
        elif isinstance(raw, list):
            records = raw
        else:
            records = []

        if not records:
            # Alternative: top-level list
            records = data if isinstance(data, list) else []

        if not records:
            break

        for o_raw in records:
            if isinstance(o_raw, list):
                continue  # skip malformed

            # HasOffers V3: fields nested under "Offer" sub-object
            offer_obj  = o_raw.get("Offer") or o_raw
            offer_id   = str(offer_obj.get("id") or o_raw.get("id", ""))
            name       = offer_obj.get("name") or o_raw.get("name", "")
            desc       = offer_obj.get("description") or o_raw.get("description", "")
            ptype_raw  = offer_obj.get("payout_type") or o_raw.get("payout_type", "CPA")
            payout_val = offer_obj.get("default_payout") or o_raw.get("default_payout", "")
            preview    = offer_obj.get("preview_url") or o_raw.get("preview_url", "")

            adv_obj    = o_raw.get("Advertiser") or {}
            adv_name   = adv_obj.get("company") or adv_obj.get("name") or name

            cats_obj   = o_raw.get("Category") or o_raw.get("categories") or {}
            if isinstance(cats_obj, dict):
                cats_obj = list(cats_obj.values())
            category   = cats_obj[0].get("name") if cats_obj else ""

            if not offer_id or not name:
                continue

            ptype_map  = {"cpa": "CPA", "cpl": "CPL", "cps": "CPS", "cpc": "CPC", "revshare": "CPS"}
            ptype      = ptype_map.get(str(ptype_raw).lower(), "CPA")
            payout_str = str(payout_val or "")
            raw_payout = f"${payout_str} {ptype}" if payout_str else "Commission varies"

            # TUNE affiliate tracking link format
            tracking   = f"{base_url.rstrip('/')}/aff_c?offer_id={offer_id}&aff_id={network_id}"

            # TUNE HasOffers fields
            thumbnail  = offer_obj.get("thumbnail_url") or offer_obj.get("logo_url") or ""
            terms_text = offer_obj.get("terms_of_service") or offer_obj.get("restrictions") or ""
            geo_raw    = offer_obj.get("countries") or offer_obj.get("country") or "US"
            if isinstance(geo_raw, (list, dict)):
                geo_raw = ", ".join(geo_raw.keys() if isinstance(geo_raw, dict) else geo_raw)
            os_raw     = offer_obj.get("mobile_device_targeting") or offer_obj.get("os_list") or "All"
            if isinstance(os_raw, list):
                os_raw = ", ".join(os_raw)
            cta_text   = offer_obj.get("cta") or offer_obj.get("call_to_action") or ""

            o = empty_offer()
            o["network"]           = f"tune_{label}"
            o["offer_id"]          = offer_id
            o["advertiser"]        = adv_name
            o["title"]             = name
            o["description"]       = (desc or "")[:500]
            o["mini_description"]  = (desc or "")[:120]
            o["cta"]               = cta_text
            o["terms"]             = (terms_text or "")[:500]
            o["payout"]            = payout_str
            o["payout_type"]       = ptype
            o["_raw_payout"]       = raw_payout[:120]
            o["currency"]          = "USD"
            o["category"]          = category
            o["geo"]               = "US"
            o["geo_raw"]           = str(geo_raw)
            o["os_targeting"]      = str(os_raw)
            o["platform_targeting"] = "All"
            o["tracking_url"]      = tracking
            o["preview_url"]       = preview or ""
            o["icon_url"]          = thumbnail
            o["hero_url"]          = thumbnail
            o["banner_url"]        = thumbnail
            o["status"]            = "Active"
            o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
            offers.append(o)

        if len(records) < 500:
            break
        page += 1

    log.info(f"TUNE/{label}: {len(offers)} offers fetched")
    return offers


def fetch_tune_all() -> list:
    """Run all configured TUNE/HasOffers instances in sequence."""
    if not TUNE_INSTANCES:
        return []
    all_offers = []
    for label, nid, key, url in TUNE_INSTANCES:
        try:
            all_offers.extend(fetch_tune_instance(label, nid, key, url))
        except Exception as e:
            log.error(f"TUNE/{label}: unhandled error — {e}")
    return all_offers


# ---------------------------------------------------------------------------
# Everflow (multi-instance)
# ---------------------------------------------------------------------------

def fetch_everflow_instance(label: str, api_key: str, base_url: str) -> list:
    """
    Everflow publisher API — single white-label instance.
    Endpoint: {base_url}/v1/publisher/offers
    Auth: X-Eflow-API-Key header
    """
    offers: list[dict] = []
    page = 1

    while True:
        try:
            resp = requests.get(
                f"{base_url.rstrip('/')}/v1/publisher/offers",
                headers={"X-Eflow-API-Key": api_key, "Accept": "application/json"},
                params={
                    "status":      "active",
                    "page":        page,
                    "page-size":   200,
                    "relationship": "joined",
                },
                timeout=30,
            )
            if resp.status_code in (401, 403):
                log.warning(f"Everflow/{label}: {resp.status_code} — check API key")
                break
            if not resp.ok:
                log.warning(f"Everflow/{label}: {resp.status_code} — {resp.text[:200]}")
                break
        except Exception as e:
            log.warning(f"Everflow/{label}: request error — {e}")
            break

        try:
            data = resp.json()
        except Exception:
            log.warning(f"Everflow/{label}: non-JSON — {resp.text[:200]}")
            break

        records = data.get("offers") or data.get("data") or (data if isinstance(data, list) else [])
        if not records:
            break

        for rec in records:
            offer_id   = str(rec.get("id") or rec.get("offer_id", ""))
            name       = rec.get("name") or rec.get("offer_name", "")
            desc       = rec.get("description", "")
            ptype_raw  = rec.get("payout_type") or rec.get("type", "CPA")
            payout_val = rec.get("default_payout") or rec.get("payout", "")
            adv_name   = (rec.get("advertiser") or {}).get("name") or name
            category   = (rec.get("categories") or [{}])[0].get("name") if rec.get("categories") else ""
            preview    = rec.get("preview_url") or rec.get("destination_url", "")

            if not offer_id or not name:
                continue

            ptype_map = {"cpa": "CPA", "cpl": "CPL", "cps": "CPS", "cpc": "CPC", "revshare": "CPS"}
            ptype     = ptype_map.get(str(ptype_raw).lower(), "CPA")
            payout_str = str(payout_val or "")
            raw_payout = f"${payout_str} {ptype}" if payout_str and payout_str not in ("0", "0.0") else "Commission varies"

            # Everflow tracking URL format (publisher must be configured in dashboard)
            tracking  = rec.get("tracking_url") or f"{base_url.rstrip('/')}/click?offer_id={offer_id}"

            thumbnail  = rec.get("thumbnail_url") or rec.get("logo_url") or rec.get("image_url") or ""
            terms_text = rec.get("terms_and_conditions") or rec.get("restrictions") or ""
            geo_raw    = rec.get("countries") or rec.get("geo") or "US"
            if isinstance(geo_raw, list):
                geo_raw = ", ".join(str(x) for x in geo_raw)
            os_raw     = rec.get("os_targeting") or rec.get("platforms") or "All"
            if isinstance(os_raw, list):
                os_raw = ", ".join(str(x) for x in os_raw)
            cta_text   = rec.get("cta") or rec.get("call_to_action") or ""

            o = empty_offer()
            o["network"]           = f"everflow_{label}"
            o["offer_id"]          = offer_id
            o["advertiser"]        = adv_name
            o["title"]             = name
            o["description"]       = (desc or "")[:500]
            o["mini_description"]  = (desc or "")[:120]
            o["cta"]               = cta_text
            o["terms"]             = (terms_text or "")[:500]
            o["payout"]            = payout_str
            o["payout_type"]       = ptype
            o["_raw_payout"]       = raw_payout[:120]
            o["currency"]          = "USD"
            o["category"]          = category
            o["geo"]               = "US"
            o["geo_raw"]           = str(geo_raw)
            o["os_targeting"]      = str(os_raw)
            o["platform_targeting"] = "All"
            o["tracking_url"]      = tracking
            o["preview_url"]       = preview or ""
            o["icon_url"]          = thumbnail
            o["hero_url"]          = thumbnail
            o["banner_url"]        = thumbnail
            o["status"]            = "Active"
            o["date_scraped"]      = datetime.today().strftime("%Y-%m-%d")
            offers.append(o)

        if len(records) < 200:
            break
        page += 1

    log.info(f"Everflow/{label}: {len(offers)} offers fetched")
    return offers


def fetch_everflow_all() -> list:
    """Run all configured Everflow instances in sequence."""
    if not EVERFLOW_INSTANCES:
        return []
    all_offers = []
    for label, key, url in EVERFLOW_INSTANCES:
        try:
            all_offers.extend(fetch_everflow_instance(label, key, url))
        except Exception as e:
            log.error(f"Everflow/{label}: unhandled error — {e}")
    return all_offers


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

NETWORK_MAP = {
    "impact":      fetch_impact,
    "flexoffers":  fetch_flexoffers,
    "maxbounty":   fetch_maxbounty,
    "cj":          fetch_cj,
    "shareasale":  fetch_shareasale,
    "rakuten":     fetch_rakuten,
    "awin":        fetch_awin,
    "tune":        fetch_tune_all,
    "everflow":    fetch_everflow_all,
}

def run_headless() -> None:
    """
    Run the full scraper pipeline programmatically (no CLI args, no --dry-run).

    Called from scout_bot.py daemon thread — avoids argparse / sys.argv interaction.
    Fetches all networks, normalises offers, writes data/offers_latest.json,
    rotates data/offers_previous.json, and posts the Scout Sniper digest.
    """
    global DEBUG
    DEBUG = False

    all_offers = []
    for name, fn in NETWORK_MAP.items():
        try:
            all_offers.extend(fn())
        except Exception as e:
            log.error(f"[scraper] {name}: failed — {e}")

    log.info(f"[scraper] Total offers collected: {len(all_offers)}")
    ms_index = fetch_ms_campaign_index()
    cleaned  = clean_offers(all_offers, ms_index)
    log.info(f"[scraper] After cleaning (active only, normalised): {len(cleaned)} offers")

    if not cleaned:
        log.warning("[scraper] No active offers — skipping writes")
        return

    if NOTION_TOKEN:
        write_notion(cleaned)

    # Atomic rotation: latest → previous, then write new latest
    _data_dir    = pathlib.Path(__file__).parent / "data"
    _data_dir.mkdir(exist_ok=True)
    snapshot_path = _data_dir / "offers_latest.json"
    previous_path = _data_dir / "offers_previous.json"
    tmp_path      = _data_dir / "offers_latest.tmp"

    if snapshot_path.exists():
        import shutil
        shutil.copy(snapshot_path, previous_path)

    tmp_path.write_text(json.dumps(cleaned, default=str))
    os.replace(tmp_path, snapshot_path)
    log.info(f"[scraper] snapshot written: {len(cleaned)} offers → {snapshot_path}")

    try:
        import scout_digest
        log.info("[scraper] posting Scout Sniper digest...")
        scout_digest.post_digest()
    except Exception as e:
        log.warning(f"[scraper] digest post failed (non-fatal): {e}")


def main():
    parser = argparse.ArgumentParser(description="Affiliate offer scraper")
    parser.add_argument("--network", choices=list(NETWORK_MAP.keys()),
                        help="Run a single network (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and clean but don't write any output files")
    parser.add_argument("--debug", action="store_true",
                        help="Print raw first record from each network (for field mapping inspection)")
    parser.add_argument("--enrich-payouts", action="store_true",
                        help="Fetch Impact payout data from /Actions endpoints (232+ calls, ~5-8 min). Saves to payout_cache.json")
    args = parser.parse_args()

    global DEBUG
    DEBUG = args.debug

    # If --enrich-payouts is set, force re-fetch all Impact payouts (ignores cache)
    if args.enrich_payouts:
        log.info("Enriching Impact payouts (--enrich-payouts): force re-fetching all campaigns...")
        campaigns = _fetch_impact_campaigns_raw()
        campaign_ids = [str(c.get("CampaignId", "")) for c in campaigns if c.get("CampaignId")]
        if campaign_ids:
            _run_impact_payout_enrichment(campaign_ids, existing_cache={})  # empty dict = force refresh all
        return

    networks = [args.network] if args.network else list(NETWORK_MAP.keys())
    all_offers = []

    for name in networks:
        try:
            offers = NETWORK_MAP[name]()
            all_offers.extend(offers)
        except Exception as e:
            log.error(f"{name}: failed — {e}")
            continue

    log.info(f"Total offers collected: {len(all_offers)}")

    # Load MS campaign index from ClickHouse (gracefully skipped if creds missing)
    ms_index = fetch_ms_campaign_index()

    # Apply normalization + drop expired + MS matching
    cleaned = clean_offers(all_offers, ms_index)
    log.info(f"After cleaning (active only, normalized): {len(cleaned)} offers")

    if args.dry_run:
        log.info("Dry run — not writing to any destination")
        return

    if not cleaned:
        log.warning("No active offers collected — nothing written")
        return

    # Write to Notion if token is configured
    if NOTION_TOKEN:
        write_notion(cleaned)
    else:
        log.info("NOTION_TOKEN not set — skipping Notion. Set it to enable Notion output.")

    # Write JSON snapshot for Scout (offer intelligence bot)
    # P9-1: atomic rotation — copy latest → previous, then replace latest via tmp
    _data_dir = pathlib.Path(__file__).parent / "data"
    _data_dir.mkdir(exist_ok=True)
    snapshot_path  = _data_dir / "offers_latest.json"
    previous_path  = _data_dir / "offers_previous.json"
    tmp_path       = _data_dir / "offers_latest.tmp"

    # Rotate: current latest → previous (only if latest exists)
    if snapshot_path.exists():
        import shutil
        shutil.copy(snapshot_path, previous_path)

    # Write new latest atomically
    tmp_path.write_text(json.dumps(cleaned, default=str))
    os.replace(tmp_path, snapshot_path)
    log.info(f"Scout snapshot written: {len(cleaned)} offers → {snapshot_path}")

    # Post SCOUT Sniper weekly digest to Slack
    try:
        import scout_digest
        log.info("Posting SCOUT Sniper digest...")
        scout_digest.post_digest()
    except Exception as e:
        log.warning(f"SCOUT Sniper digest failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
