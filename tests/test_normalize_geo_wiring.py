"""Producer-side wiring tests for scout_core.contracts geo normalization.

P2.1 added `NormalizedOffer.normalize_geo(raw)` which delegates to a callable
registered via `scout_core.contracts.set_geo_normalizer(fn)`. P2.2 wires the
real `offer_scraper.normalize_geo` into both producers (scout_agent,
demand_feed_main) at import time. These tests prove that registration is in
fact run at startup, and that the registered function is the canonical
offer_scraper implementation (idempotent fast-path included).

scout_agent imports anthropic / clickhouse_connect / queries / scout_types at
module level — none of which are installed in the lightweight test venv. We
stub them into sys.modules before import, matching the pattern in
tests/test_demand_feed_http.py (lines 36-69).
"""

from __future__ import annotations

import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

_WT_ROOT = Path(__file__).parent.parent
if str(_WT_ROOT) not in sys.path:
    sys.path.insert(0, str(_WT_ROOT))


def _stub_module(name: str, **attrs) -> types.ModuleType:
    """Create a minimal fake module and register it in sys.modules."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# anthropic — scout_agent does `import anthropic` and uses Anthropic + sub-types
if "anthropic" not in sys.modules:
    _ant = _stub_module("anthropic")
    _ant.Anthropic = MagicMock
    _ant.types = _stub_module("anthropic.types")

# clickhouse_connect — lazily used; stub for safety
if "clickhouse_connect" not in sys.modules:
    _stub_module("clickhouse_connect")


class NormalizeGeoWiringTest(unittest.TestCase):
    """Importing a producer module must register the canonical normalizer."""

    def _reset_registration(self) -> None:
        """Force the contracts module back to an unregistered state so we can
        prove the producer import actually re-registers."""
        import scout_core.contracts as c
        c._geo_normalizer = None

    def test_offer_scraper_normalize_geo_is_canonical(self) -> None:
        """Sanity-check the canonical function before testing wiring."""
        import offer_scraper
        # Producer-side function exists and behaves as the contracts layer
        # expects. "United States" is not in the variant set (which uses
        # country codes / 'us' / 'usa'), so the canonical answer is "Other".
        # The wiring contract is about *which* function is called, so we
        # assert on well-known canonical inputs documented in the source.
        self.assertEqual(offer_scraper.normalize_geo("US"), "US Only")
        self.assertEqual(offer_scraper.normalize_geo("US Only"), "US Only")  # idempotent
        self.assertEqual(offer_scraper.normalize_geo(""), "Unknown")

    def test_importing_scout_agent_registers_normalizer(self) -> None:
        # queries / scout_types may already be real (worktree has them) — only
        # stub if absent so we don't shadow the real implementations.
        if "queries" not in sys.modules:
            _stub_module("queries")
        if "scout_types" not in sys.modules:
            _stub_module("scout_types", FormattedOffer=dict, Brief=dict)

        self._reset_registration()
        # Force a fresh import so the top-level registration runs.
        sys.modules.pop("scout_agent", None)
        importlib.import_module("scout_agent")

        from scout_core.contracts import NormalizedOffer
        # Round-trip a value that exercises the real (not stubbed) normalizer.
        self.assertEqual(NormalizedOffer.normalize_geo("US"), "US Only")
        self.assertEqual(NormalizedOffer.normalize_geo("US Only"), "US Only")

    def test_importing_demand_feed_main_registers_normalizer(self) -> None:
        self._reset_registration()
        sys.modules.pop("demand_feed_main", None)
        importlib.import_module("demand_feed_main")

        from scout_core.contracts import NormalizedOffer
        self.assertEqual(NormalizedOffer.normalize_geo("US"), "US Only")
        self.assertEqual(NormalizedOffer.normalize_geo("US Only"), "US Only")


if __name__ == "__main__":
    unittest.main()
