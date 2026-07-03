from __future__ import annotations

import json
import logging
import os
import pathlib
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

from scout_ch import _get_ch_client

log = logging.getLogger(__name__)

_IMAGE_CACHE_PATH = pathlib.Path(__file__).parent / "data" / "image_cache.json"
_IMAGE_CACHE_TTL_DAYS = 7
_image_cache_mem: dict = {}  # in-memory layer to avoid file reads on every brief
_image_cache_loaded = False


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
    except Exception as e:
        log.debug("_validate_image_url swallowed: %s", e)
        return False


def _load_image_cache() -> dict:
    global _image_cache_mem, _image_cache_loaded
    if not _image_cache_loaded:
        try:
            if _IMAGE_CACHE_PATH.exists():
                _image_cache_mem = json.loads(_IMAGE_CACHE_PATH.read_text())
        except Exception as e:
            log.debug("_load_image_cache swallowed: %s", e)
            _image_cache_mem = {}
        _image_cache_loaded = True
    return _image_cache_mem


def _save_image_cache(cache: dict) -> None:
    try:
        _IMAGE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _IMAGE_CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(cache, indent=2))
        os.replace(tmp, _IMAGE_CACHE_PATH)
    except Exception as e:
        log.debug(f"image cache save failed: {e}")


def _cached_external_images(advertiser: str) -> dict | None:
    """Return cached {hero_url, icon_url} if fresh (< 7 days). None if stale or missing."""
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
    except Exception as e:
        log.debug("_cached_external_images swallowed: %s", e)
        return None
    return entry


def _store_image_cache(advertiser: str, hero_url: str, icon_url: str) -> None:
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
