"""Tests for P2.7 — nightly_harvest daemon migration to demand_feed_main.

Covers:
  T1: _nightly_harvest_daemon function exists in demand_feed_main and is callable
  T2: With HARVESTER_AUTO_WRITE_ENABLED=false (default), daemon completes one
      cycle without writing when context_harvester.is_stale() returns False
  T3: demand_feed_main imports cleanly
"""

from __future__ import annotations

import importlib
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_WT_ROOT = Path(__file__).parent.parent
if str(_WT_ROOT) not in sys.path:
    sys.path.insert(0, str(_WT_ROOT))


# ---------------------------------------------------------------------------
# Stub heavy deps that aren't available in the test venv
# Only stub packages that DON'T exist in the test venv — leave real ones alone.
# ---------------------------------------------------------------------------

def _stub(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# anthropic — not in test venv
try:
    importlib.import_module("anthropic")
except ImportError:
    _ant = _stub("anthropic")
    _ant.Anthropic = MagicMock
    _ant.types = _stub("anthropic.types")
    sys.modules["anthropic.types"] = _ant.types

# slack_sdk — not in test venv
try:
    importlib.import_module("slack_sdk")
except ImportError:
    _sdk = _stub("slack_sdk")
    _sdk_web = _stub("slack_sdk.web")
    _sdk_web.WebClient = MagicMock
    sys.modules["slack_sdk.web"] = _sdk_web

# clickhouse_connect / queries / scout_types — not needed in test venv
for _dep in ("clickhouse_connect", "queries", "scout_types"):
    try:
        importlib.import_module(_dep)
    except ImportError:
        _stub(_dep)

# context_harvester — stub so we control is_stale/harvest in tests
_ch_stub = _stub(
    "context_harvester",
    harvest=MagicMock(return_value={"context": {}, "audit": []}),
    is_stale=MagicMock(return_value=False),
)


# ---------------------------------------------------------------------------
# T1: _nightly_harvest_daemon exists and is callable
# ---------------------------------------------------------------------------

class TestDaemonExists(unittest.TestCase):

    def test_nightly_harvest_daemon_is_callable(self):
        """_nightly_harvest_daemon must be defined and callable in demand_feed_main."""
        import demand_feed_main as dm
        importlib.reload(dm)
        self.assertTrue(
            callable(getattr(dm, "_nightly_harvest_daemon", None)),
            "_nightly_harvest_daemon not found or not callable in demand_feed_main",
        )


# ---------------------------------------------------------------------------
# T2: With kill switch off and is_stale()=False, one cycle sleeps without harvesting
# ---------------------------------------------------------------------------

class TestDaemonKillSwitch(unittest.TestCase):

    def test_no_harvest_when_context_is_fresh(self):
        """With HARVESTER_AUTO_WRITE_ENABLED=false and is_stale()=False,
        the daemon's startup check skips harvest() and proceeds to sleep."""
        import demand_feed_main as dm
        importlib.reload(dm)

        # Confirm kill switch defaults off in context_harvester
        self.assertNotEqual(
            os.getenv("HARVESTER_AUTO_WRITE_ENABLED", "false").lower(),
            "true",
            "Kill switch must remain false for this test",
        )

        import context_harvester as ch
        ch.is_stale = MagicMock(return_value=False)
        ch.harvest = MagicMock(return_value={"context": {}, "audit": []})

        # Patch sleep so the daemon doesn't actually wait; raise to break the loop
        with patch("time.sleep", side_effect=StopIteration("stop after first sleep")):
            try:
                dm._nightly_harvest_daemon()
            except StopIteration:
                pass

        # harvest() should NOT have been called since is_stale() returned False
        ch.harvest.assert_not_called()


# ---------------------------------------------------------------------------
# T3: demand_feed_main imports cleanly
# ---------------------------------------------------------------------------

class TestDemandFeedImport(unittest.TestCase):

    def test_module_imports_without_error(self):
        """demand_feed_main must import (and reload) without raising."""
        try:
            import demand_feed_main as dm
            importlib.reload(dm)
        except Exception as exc:
            self.fail(f"demand_feed_main failed to import: {exc}")


if __name__ == "__main__":
    unittest.main()
