"""
scout_match_mapping.py — human-curated (network, advertiser) -> MS campaign override table.

Split out of offer_scraper.py to keep that file under its line-count ceiling
(module_size_within_ceiling in smoke_test.py) — this is a self-contained external I/O
concern (reading a Notion database) with no ties to network-scraping logic, so it's a
clean module boundary, not a mechanical line-shaving move.
"""

from __future__ import annotations

import logging
import os
import re

import requests

log = logging.getLogger(__name__)


def fetch_match_mapping_table(notion_token: str, by_campaign_id: dict) -> dict:
    """
    Read the human-curated (network, advertiser) -> MS campaign ID override table from
    Notion, keyed by MS_MATCH_MAPPING_DB_ID. This is the durable fix for a fuzzy match a
    human has already confirmed or corrected once — it should never need re-guessing.

    Expected Notion database schema (see KNOWN_DEBT.md for setup):
      - "Network"        (select)    — e.g. "rakuten", "awin"
      - "Advertiser Key" (rich_text) — the raw advertiser name as it appears in the offer;
                                        normalized the same way as offer_scraper.py's by_name
      - "MS Campaign ID" (number)    — resolved against by_campaign_id to get the actual
                                        status/is_live/adv_name entry

    Fails open (empty dict) on any error, missing env vars, or an unresolvable campaign ID
    — the caller falls through to exact/fuzzy matching exactly as if no mapping existed.
    This must never turn a Notion outage into a scrape failure; match_ms_status has no
    other external dependency today.
    """
    mapping_db_id = os.getenv("MS_MATCH_MAPPING_DB_ID", "").strip()
    if not notion_token or not mapping_db_id:
        return {}

    _strip_re = re.compile(r"[^a-z0-9 ]")

    def _normalize(raw_name: str) -> str:
        norm = _strip_re.sub("", raw_name.lower()).strip()
        for suffix in [" inc", " llc", " ltd", " corp", " com", " us"]:
            if norm.endswith(suffix):
                norm = norm[:-len(suffix)].strip()
        return norm

    mapping: dict = {}
    cursor = None
    try:
        while True:
            body: dict = {"page_size": 100}
            if cursor:
                body["start_cursor"] = cursor
            resp = requests.post(
                f"https://api.notion.com/v1/databases/{mapping_db_id}/query",
                headers={
                    "Authorization": f"Bearer {notion_token}",
                    "Notion-Version": "2022-06-28",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=10,
            )
            if not resp.ok:
                log.warning(f"match-mapping: Notion query failed {resp.status_code} — falling through to exact/fuzzy matching")
                return {}
            data = resp.json()
            for page in data.get("results", []):
                try:
                    props = page.get("properties", {})
                    network = ((props.get("Network") or {}).get("select") or {}).get("name", "")
                    adv_rt = (props.get("Advertiser Key") or {}).get("rich_text", [])
                    advertiser_key = adv_rt[0].get("plain_text", "") if adv_rt else ""
                    ms_campaign_id = (props.get("MS Campaign ID") or {}).get("number")
                    if not network or not advertiser_key or ms_campaign_id is None:
                        continue
                    entry = by_campaign_id.get(str(int(ms_campaign_id)))
                    if entry is None:
                        log.warning(f"match-mapping: MS Campaign ID {ms_campaign_id} not found — skipping row")
                        continue
                    mapping[(network, _normalize(advertiser_key))] = entry
                except Exception as e:
                    log.warning(f"match-mapping: row parse error: {e}")
                    continue
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
            if not cursor:
                # Notion contractually always pairs has_more=true with a next_cursor,
                # but if that ever isn't true, omitting start_cursor on the next
                # request would silently re-fetch page 1 forever. Fail open instead
                # of hanging the scraper run.
                log.warning("match-mapping: has_more=true but next_cursor missing — stopping pagination early")
                break
    except Exception as e:
        log.warning(f"match-mapping: request error — falling through to exact/fuzzy matching: {e}")
        return {}

    return mapping
