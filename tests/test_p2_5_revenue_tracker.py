"""Tests for P2.5 — revenue_tracker daemon ported to demand_feed_main.

Covers:
  T1: _revenue_tracker_daemon function exists in demand_feed_main and is callable
  T2: With REVENUE_TRACKER_ENABLED=false, kill switch skips CH query (no query called)
  T3: demand_feed_main module imports cleanly (no import errors)
"""

from __future__ import annotations

import importlib
import os
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_WT_ROOT = Path(__file__).parent.parent
if str(_WT_ROOT) not in sys.path:
    sys.path.insert(0, str(_WT_ROOT))


# ---------------------------------------------------------------------------
# Stub heavy deps that aren't available in the test venv.
# Only stub packages that DON'T exist — leave real ones alone.
# ---------------------------------------------------------------------------

def _stub(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# anthropic — not in test venv; scout_agent imports it at module level
try:
    importlib.import_module("anthropic")
except ImportError:
    _ant = _stub("anthropic")
    _ant.Anthropic = MagicMock
    _ant.types = _stub("anthropic.types")
    sys.modules["anthropic.types"] = _ant.types

# clickhouse_connect / queries / scout_types — not needed in tests
for _dep in ("clickhouse_connect", "queries", "scout_types"):
    try:
        importlib.import_module(_dep)
    except ImportError:
        _stub(_dep)

# pytz — should be in venv; stub defensively if absent
try:
    importlib.import_module("pytz")
except ImportError:
    _pytz_mod = _stub("pytz")
    _fake_tz = MagicMock()
    _pytz_mod.timezone = MagicMock(return_value=_fake_tz)

# slack_sdk — stub if not installed
try:
    importlib.import_module("slack_sdk")
except ImportError:
    _sdk = _stub("slack_sdk")
    _sdk_web = _stub("slack_sdk.web")
    _sdk_web.WebClient = MagicMock
    sys.modules["slack_sdk.web"] = _sdk_web


# ---------------------------------------------------------------------------
# T1: _revenue_tracker_daemon exists and is callable
# ---------------------------------------------------------------------------

class TestRevenueDaemonExists(unittest.TestCase):

    def test_function_exists_and_callable(self):
        """_revenue_tracker_daemon must be importable and callable from demand_feed_main."""
        import demand_feed_main as dm
        importlib.reload(dm)
        self.assertTrue(
            hasattr(dm, "_revenue_tracker_daemon"),
            "_revenue_tracker_daemon not found in demand_feed_main",
        )
        self.assertTrue(
            callable(dm._revenue_tracker_daemon),
            "_revenue_tracker_daemon is not callable",
        )


# ---------------------------------------------------------------------------
# T2: Kill switch — REVENUE_TRACKER_ENABLED=false → no CH query issued
# ---------------------------------------------------------------------------

class TestKillSwitch(unittest.TestCase):

    def test_kill_switch_off_skips_ch_query(self):
        """When REVENUE_TRACKER_ENABLED is false the daemon's inner loop must not
        call _query_intraday_revenue_total (the CH query) at all.

        Strategy: patch time.sleep so each call counts a tick; after 2 ticks raise
        a sentinel exception to stop the loop. CH query is patched to a bomb that
        sets a flag — if the flag stays False, the kill switch works correctly.
        """
        import demand_feed_main as dm
        importlib.reload(dm)

        class _StopLoop(Exception):
            pass

        tick_count = {"n": 0}
        ch_called  = {"flag": False}

        def _boom_ch(*a, **kw):
            ch_called["flag"] = True
            raise AssertionError("CH query must not be called when kill switch is off")

        def _fake_sleep(secs):
            tick_count["n"] += 1
            if tick_count["n"] >= 2:
                raise _StopLoop("done")

        # Stub scout_agent CH functions (daemon imports them locally on each call)
        try:
            import scout_agent as _sa
        except ImportError:
            _sa = _stub("scout_agent")
        _sa._query_intraday_revenue_total        = _boom_ch
        _sa._query_intraday_revenue_by_publisher = _boom_ch
        if not hasattr(_sa, "SCOUT_THRESHOLDS"):
            _sa.SCOUT_THRESHOLDS = {}

        result_errors = []

        def _run_daemon():
            # Patch time.sleep at the module level the daemon uses via its
            # local `import time as _time` — which resolves to sys.modules["time"].
            import time as _t_real
            orig_sleep = _t_real.sleep
            _t_real.sleep = _fake_sleep
            try:
                dm._revenue_tracker_daemon()
            except _StopLoop:
                pass
            except Exception as exc:
                result_errors.append(exc)
            finally:
                _t_real.sleep = orig_sleep

        t = threading.Thread(target=_run_daemon, daemon=True)
        with patch.dict(os.environ, {"REVENUE_TRACKER_ENABLED": "false"}):
            t.start()
            t.join(timeout=5)

        self.assertFalse(
            ch_called["flag"],
            "CH query was called despite kill switch being off",
        )
        self.assertFalse(
            result_errors,
            f"Unexpected errors in daemon thread: {result_errors}",
        )


# ---------------------------------------------------------------------------
# T3: Module imports cleanly
# ---------------------------------------------------------------------------

class TestModuleImports(unittest.TestCase):

    def test_demand_feed_main_imports_cleanly(self):
        """demand_feed_main must import without raising ImportError or AttributeError."""
        try:
            import demand_feed_main
            importlib.reload(demand_feed_main)
        except (ImportError, AttributeError) as exc:
            self.fail(f"demand_feed_main failed to import: {exc}")

    def test_revenue_tracker_daemon_in_main_namespace(self):
        """_revenue_tracker_daemon must be accessible at module level after import."""
        import demand_feed_main as dm
        importlib.reload(dm)
        fn = getattr(dm, "_revenue_tracker_daemon", None)
        self.assertIsNotNone(fn, "_revenue_tracker_daemon missing from demand_feed_main")
        self.assertTrue(callable(fn))

    def test_format_revenue_alert_in_main_namespace(self):
        """_format_revenue_alert helper must be accessible at module level."""
        import demand_feed_main as dm
        importlib.reload(dm)
        fn = getattr(dm, "_format_revenue_alert", None)
        self.assertIsNotNone(fn, "_format_revenue_alert missing from demand_feed_main")
        self.assertTrue(callable(fn))


if __name__ == "__main__":
    unittest.main()
