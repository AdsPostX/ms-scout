from __future__ import annotations

import re
import urllib.parse

# Accepted patterns (known sources that serve genuine square logos):
#   flexlinks.com …programsquarelogo…     ← FlexOffers square logos
#   ui.awin.com …merchant/profile/…       ← Awin merchant profile icons
# Rejected patterns:
#   cdn.mb1-content.com …creative/lp…    ← MaxBounty landing-page thumbnails
#   anything that looks like a banner/hero/promo creative

_ICON_ACCEPT_RE = re.compile(
    r"(programsquarelogo|merchant/profile/|/icon[s/_]|[/_]logo[s/_.-]|square.?logo)",
    re.IGNORECASE,
)
_ICON_REJECT_RE = re.compile(
    r"(creative/lp|/banner|/hero|/promo|/creative)",
    re.IGNORECASE,
)


def _is_icon_url(url: str) -> bool:
    """Return True only if the URL is likely a square logo suitable for a 75px card thumbnail."""
    if not url or not url.startswith("http"):
        return False
    if _ICON_REJECT_RE.search(url):
        return False
    if _ICON_ACCEPT_RE.search(url):
        return True
    # Unknown CDN — reject rather than guess wrong.
    # OG images from tracking URLs land here and are filtered out.
    return False


def _advertiser_favicon_url(offer: dict) -> str:
    """
    Return a Google Favicon API URL for the offer's advertiser domain.
    Extracts the root domain from preview_url (available on CJ, Impact, MaxBounty).
    Google's favicon API always returns a valid image (fallback globe icon when
    no favicon is found), so Slack never shows a broken-image placeholder.
    Returns "" when no usable domain can be found.
    """
    raw = offer.get("preview_url") or offer.get("tracking_url") or ""
    if not raw or not raw.startswith("http"):
        return ""
    try:
        host = urllib.parse.urlparse(raw).hostname or ""
        # Strip leading www. / rec. / discover. / static. etc.
        parts = host.split(".")
        # Take last two segments as root domain (e.g. lifelinescreening.com)
        domain = ".".join(parts[-2:]) if len(parts) >= 2 else host
        if not domain or "." not in domain:
            return ""
        return f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
    except Exception:
        return ""


def resolve_icon_image(offer: dict, extra_candidates: tuple[str, ...] = ()) -> str:
    """
    Resolve a square logo URL for an offer.

    Priority:
      1. icon_url / hero_url / extra_candidates (in order) if _is_icon_url()
         confirms it's a genuine square logo (FlexOffers programsquarelogo,
         Awin merchant/profile — NOT MaxBounty lp thumbnails)
      2. Google Favicon API derived from preview_url domain — always returns
         a valid image (never 404s), so Slack never shows a broken-image
         placeholder.

    No OG-image scraping: og:image is a wide social-preview banner, wrong
    shape for a 75 px square Slack accessory slot.
    """
    candidates = [offer.get("icon_url") or "", offer.get("hero_url") or "", *extra_candidates]
    return next((u for u in candidates if _is_icon_url(u)), _advertiser_favicon_url(offer))
