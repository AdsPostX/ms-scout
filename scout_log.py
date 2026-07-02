"""
scout_log.py — structured JSON logging for Better Stack.

log_event() emits one JSON object per line so Better Stack can filter and
build dashboards on `event`/`location` instead of grepping free-text
messages. Use it at high-value diagnostic points (alert fires, scraper
network failures, digest scoring decisions) — not a replacement for
ordinary log.info()/log.warning() calls.

    from scout_log import log_event
    log_event("alert_fired", "ghost campaign detected", "alert_registry.mark_firing",
               alert_name=alert_name, publisher=pub)

Emits through the caller's own logger so existing handlers/levels/routing
are unaffected — this only changes what gets put on the line.
"""

from __future__ import annotations

import json
import logging


def log_event(event: str, message: str, location: str, *, level: int = logging.INFO, **fields) -> None:
    logger = logging.getLogger(location.split(".")[0])
    logger.log(level, json.dumps({
        "event": event,
        "message": message,
        "location": location,
        **fields,
    }, default=str))
