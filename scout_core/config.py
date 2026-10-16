"""scout_agent.py's config, extracted so scout_digest.py doesn't need a top-level
`import scout_agent` just to read two fields — that top-level import was the
last piece of a circular dependency (scout_agent.py lazily imports
scout_digest back). scout_agent.py still owns and reads this config; it just
no longer defines it.
"""

from __future__ import annotations

import logging
import os
import urllib.parse
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ScoutCfg:
    demand_feed_url: str
    anthropic_api_key: str = ""

    @classmethod
    def from_env(cls) -> "_ScoutCfg":
        raw = os.getenv("DEMAND_FEED_URL", "").rstrip("/")
        url = ""
        if raw:
            try:
                parsed = urllib.parse.urlparse(raw)
                if parsed.scheme in ("http", "https") and parsed.hostname:
                    url = raw
                else:
                    log.warning("DEMAND_FEED_URL missing valid scheme or hostname — using disk")
            except Exception:
                log.warning("DEMAND_FEED_URL could not be parsed — using disk")
        return cls(
            demand_feed_url=url,
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
        )


_CFG = _ScoutCfg.from_env()
