"""Pytest configuration for scout_core tests.

Adds the worktree root to sys.path so `scout_core` resolves without
installing the package, and registers a deterministic geo normalizer
stub so tests don't depend on offer_scraper being importable.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scout_core.contracts import set_geo_normalizer  # noqa: E402


def _stub_normalize_geo(raw: str) -> str:
    """Minimal stand-in for offer_scraper.normalize_geo. Covers only the
    cases the contracts tests exercise; producers register the real one
    at runtime."""
    if not raw:
        return "Unknown"
    if "United States" in raw or raw.strip().upper() in {"US", "USA"}:
        return "US Only"
    return raw


set_geo_normalizer(_stub_normalize_geo)
