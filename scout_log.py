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

Emits on its own logger with a bare-message formatter, so the line is raw
JSON on stdout regardless of any `logging.basicConfig()` prefix format the
calling entry point (offer_scraper, demand_feed_main, scout_digest) has set
on the root logger — a prefixed line like `2026-... [INFO] {...}` would not
parse as JSON and Better Stack could not promote `event`/`location` as
top-level fields.
"""

from __future__ import annotations

import json
import logging

_LOGGER_NAME = "scout.structured"


def _get_json_logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def log_event(event: str, message: str, location: str, *, level: int = logging.INFO, **fields) -> None:
    _get_json_logger().log(level, json.dumps({
        "event": event,
        "message": message,
        "location": location,
        **fields,
    }, default=str))
