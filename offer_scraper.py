"""
offer_scraper.py
MVP: Affiliate Network Offer Scraper → Google Sheets (Drive)

Networks supported (v1):
  - Impact       [HTTP Basic Auth]
  - FlexOffers   [API Key, domain-based]
  - MaxBounty    [REST API: email + password → JWT token (2hr TTL)]

Output: Google Sheet with two tabs
  - "Latest"  : overwritten each run with today's full offer set
  - "History" : appended on each run (date-stamped rows)

Setup:
  1. pip install requests google-api-python-client google-auth-oauthlib
  2. Create a Google Cloud project, enable Drive + Sheets APIs
  3. Download credentials.json (OAuth2 desktop app) into this directory
  4. Set environment variables (see CONFIG section below)
  5. Run once manually to complete OAuth flow → token.pickle is saved
  6. Schedule with cron or the MomentScience schedule skill

Usage:
  python offer_scraper.py               # run all networks
  python offer_scraper.py --network impact  # run single network
"""

import os
import re
import csv
import json
import pickle
import logging
import argparse
import requests
import time
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

# Google API
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# CONFIG — loaded from .env file (see .env.example) or environment variables
# ---------------------------------------------------------------------------
IMPACT_SID          = os.environ.get("IMPACT_SID", "")
IMPACT_TOKEN        = os.environ.get("IMPACT_TOKEN", "")

FLEXOFFERS_API_KEY  = os.environ.get("FLEXOFFERS_API_KEY", "")
FLEXOFFERS_DOMAIN_ID = os.environ.get("FLEXOFFERS_DOMAIN_ID", "")

MAXBOUNTY_EMAIL    = os.environ.get("MAXBOUNTY_EMAIL", "")
MAXBOUNTY_PASSWORD = os.environ.get("MAXBOUNTY_PASSWORD", "")

# Google Sheets — create a blank Sheet, copy the ID from its URL
# e.g. https://docs.google.com/spreadsheets/d/THIS_PART_HERE/edit
SHEET_ID            = os.environ.get("GSHEET_ID", "")

# Notion — preferred output destination
# Setup: notion.so/my-integrations → create integration → copy secret_... token
#        Then share the "Offer Inventory — MS Network" database with that integration
NOTION_TOKEN        = os.environ.get("NOTION_TOKEN", "")
NOTION_DB_ID        = os.environ.get("NOTION_DB_ID", "")

# ClickHouse Cloud — for MS campaign matching
CH_HOST             = os.environ.get("CH_HOST", "")
CH_USER             = os.environ.get("CH_USER", "")
CH_PASSWORD         = os.environ.get("CH_PASSWORD", "")
CH_DATABASE         = os.environ.get("CH_DATABASE", "default")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

DEBUG = False  # set to True via --debug flag at runtime

# Payout cache file (in script directory)
PAYOUT_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "payout_cache.json")

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
    "network",
    "offer_id",
    "advertiser",
    "description",
    "payout",
    "payout_type",   # CPA | CPL | CPS | RevShare
    "currency",
    "category",
    "geo",
    "tracking_url",
    "icon_url",      # 150x150 creative (for Slack brief + MS platform upload)
    "hero_url",      # wide banner creative, target 1000x280
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
    """Save payout cache to payout_cache.json"""
    try:
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

def _fetch_impact_ads_data() -> tuple:
    """
    Fetch Impact /Ads endpoint in a single pass.
    Returns (cat_map, creative_map):
      - cat_map:      {campaign_id: label_string}   e.g. "Payments,Capital,US"
      - creative_map: {campaign_id: {"icon_url": str, "hero_url": str}}
        icon = closest to 150x150 square; hero = widest banner (target 1000x280)
    """
    url = f"https://api.impact.com/Mediapartners/{IMPACT_SID}/Ads"
    cat_map = {}
    creative_map = {}  # cid → {icon_url, hero_url, _best_icon_area_diff, _best_hero_width}
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

            # Impact /Ads returns link/text ads — CreativeUrl/ImageUrl are empty for most campaigns.
            # Creative images are sourced via OG scrape at brief time instead.
            # (Creative map intentionally left empty — fallback handled in draft_campaign_brief)

        total = int(data.get("@totalrecords", 0))
        if page * 1000 >= total:
            break
        page += 1

    log.info(f"Impact: ads data loaded — {len(cat_map)} categories (creatives via OG scrape at brief time)")
    return cat_map, creative_map


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

    # Fetch category labels + creative URLs from /Ads endpoint in one pass
    log.info("Impact: fetching ads data (categories + creatives)...")
    cat_map, creative_map = _fetch_impact_ads_data()

    offers = []
    for c in campaigns_all:
        cid = str(c.get("CampaignId", ""))

        # geo: ShippingRegions is a list ["AUSTRALIA", "CANADA", "UK", "US"]
        regions = c.get("ShippingRegions", [])
        geo = ", ".join(r.title() for r in regions) if regions else "US"

        o = empty_offer()
        creatives = creative_map.get(cid, {})
        o["network"]      = "impact"
        o["offer_id"]     = cid
        o["advertiser"]   = c.get("CampaignName", "")
        o["description"]  = str(c.get("CampaignDescription", ""))[:200]
        o["payout"]       = ""    # requires /Campaigns/{id}/Actions — future enrichment
        o["payout_type"]  = ""    # same — Impact payouts are per action type
        o["currency"]     = "USD"
        o["category"]     = cat_map.get(cid, "")
        o["geo"]          = geo
        o["tracking_url"] = c.get("TrackingLink", "")
        o["icon_url"]     = creatives.get("icon_url", "")
        o["hero_url"]     = creatives.get("hero_url", "")
        o["status"]       = c.get("ContractStatus", "")
        o["date_scraped"] = datetime.today().strftime("%Y-%m-%d")
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

            o = empty_offer()
            logo = (a.get("logoUrl") or a.get("logo_url") or
                    a.get("thumbnailUrl") or a.get("imageUrl") or "")
            o["network"]      = "flexoffers"
            o["offer_id"]     = str(a.get("id", ""))
            o["advertiser"]   = a.get("name", "")
            o["description"]  = str(a.get("description", ""))[:200]
            o["payout"]       = payout_str
            o["payout_type"]  = payout_type
            o["_raw_payout"]  = raw_payout[:120]
            o["currency"]     = "USD"
            o["category"]     = a.get("categoryNames", "")
            o["geo"]          = a.get("country", "US")
            o["tracking_url"] = a.get("deeplinkURL", a.get("defaultUrl", ""))
            o["icon_url"]     = logo
            o["hero_url"]     = logo
            o["status"]       = a.get("applicationStatus", a.get("programStatus", ""))
            o["date_scraped"] = datetime.today().strftime("%Y-%m-%d")
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

        o = empty_offer()
        o["network"]      = "maxbounty"
        o["offer_id"]     = str(c.get("campaign_id", ""))
        o["advertiser"]   = c.get("name", "")
        o["description"]  = str(c.get("description", ""))[:200]
        o["payout"]       = str(payout) if payout != "" else ""
        o["payout_type"]  = rate_type
        o["currency"]     = "USD"
        o["category"]     = category
        o["geo"]          = "US"  # MaxBounty is a US-centric CPA network; list API omits geo
        o["_raw_payout"]  = f"{payout} {rate_type}".strip() if payout != "" else ""
        o["tracking_url"] = c.get("landing_page_sample", "")
        thumbnail = c.get("thumbnail", "")
        o["icon_url"]     = thumbnail   # MaxBounty CDN thumbnail URL
        o["hero_url"]     = thumbnail   # same image used for both until we get banner
        o["status"]       = c.get("affiliate_campaign_status", c.get("status", ""))
        o["date_scraped"] = datetime.today().strftime("%Y-%m-%d")
        offers.append(o)

    log.info(f"MaxBounty: {len(offers)} offers fetched")
    return offers


# ---------------------------------------------------------------------------
# GOOGLE SHEETS OUTPUT
# ---------------------------------------------------------------------------

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


def _get_sheets_service():
    creds = None
    token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.pickle")
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.json")

    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "wb") as f:
            pickle.dump(creds, f)

    return build("sheets", "v4", credentials=creds)


def write_to_sheets(cleaned_offers: list, raw_offers: list):
    """
    Write offers to Google Sheets.
    - Latest tab: cleaned_offers (active only, normalized) — overwritten each run
    - History tab: raw_offers (full snapshot including expired) — appended each run
    """
    if not SHEET_ID:
        log.error("GSHEET_ID not set — skipping Google Sheets upload")
        _write_local_csv(cleaned_offers)
        return

    log.info(f"Writing to Google Sheets: {len(cleaned_offers)} active offers to Latest, "
             f"{len(raw_offers)} total to History...")
    service = _get_sheets_service()
    sheet = service.spreadsheets()

    header      = OFFER_FIELDS
    latest_rows = [[o.get(f, "") for f in OFFER_FIELDS] for o in cleaned_offers]
    history_rows = [[o.get(f, "") for f in OFFER_FIELDS] for o in raw_offers]
    today       = datetime.today().strftime("%Y-%m-%d")

    # --- Tab 1: "Latest" — clear and overwrite with active/cleaned offers ---
    try:
        sheet.values().clear(
            spreadsheetId=SHEET_ID,
            range="Latest!A:Z",
        ).execute()
        sheet.values().update(
            spreadsheetId=SHEET_ID,
            range="Latest!A1",
            valueInputOption="RAW",
            body={"values": [header] + latest_rows},
        ).execute()
        log.info(f"Latest tab: updated with {len(cleaned_offers)} active rows")
    except Exception as e:
        log.warning(f"Latest tab write failed: {e}")

    # --- Tab 2: "History" — append full raw snapshot ---
    try:
        sheet.values().append(
            spreadsheetId=SHEET_ID,
            range="History!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": history_rows},
        ).execute()
        log.info(f"History tab: appended {len(raw_offers)} rows for {today}")
    except Exception as e:
        log.warning(f"History tab append failed: {e}")


def _write_local_csv(offers: list, filename: str = None):
    filename = filename or f"offers_{datetime.today().strftime('%Y-%m-%d')}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OFFER_FIELDS)
        writer.writeheader()
        writer.writerows(offers)
    log.info(f"Fallback: written to {filename}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

NETWORK_MAP = {
    "impact":      fetch_impact,
    "flexoffers":  fetch_flexoffers,
    "maxbounty":   fetch_maxbounty,
}

def main():
    parser = argparse.ArgumentParser(description="Affiliate offer scraper")
    parser.add_argument("--network", choices=list(NETWORK_MAP.keys()),
                        help="Run a single network (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch but don't write to Sheets (prints count only)")
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

    # Write to Notion if token is configured (preferred)
    if NOTION_TOKEN:
        write_notion(cleaned)
    else:
        log.info("NOTION_TOKEN not set — skipping Notion. Set it to enable Notion output.")

    # Always write to Sheets as fallback/backup
    write_to_sheets(cleaned, all_offers)  # Latest=active cleaned, History=full raw snapshot

    # Write JSON snapshot for Scout (offer intelligence bot)
    import pathlib
    snapshot_path = pathlib.Path(__file__).parent / "data" / "offers_latest.json"
    snapshot_path.parent.mkdir(exist_ok=True)
    with open(snapshot_path, "w") as f:
        import json as _json
        _json.dump(cleaned, f, default=str)
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
