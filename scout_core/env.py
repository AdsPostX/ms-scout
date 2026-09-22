"""Shared env-var parsing helpers for ms-scout and ms-demand-feed.

Both services need the same "read an int env var, warn and fall back to a
default on a bad value" behavior. Kept here instead of one service
importing the other's copy.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def env_int(name: str, default: int, tag: str = "") -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        log.warning("[%s] %s is not a valid integer; using default %d", tag or __name__, name, default)
        return default
