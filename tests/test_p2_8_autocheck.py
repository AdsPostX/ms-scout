"""Tests for P2.8 — projection_autocheck daemon migration to demand-feed.

Covers:
  T1: _projection_autocheck_daemon function exists in demand_feed_main
  T2: Kill switch off (default) → daemon sleeps without calling ClickHouse
  T3: demand_feed_main imports cleanly
"""

from __future__ import annotations

import importlib
import sys
import time as _real_time
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_WT_ROOT = Path(__file__).parent.parent
if str(_WT_ROOT) not in sys.path:
    sys.path.insert(0, str(_WT_ROOT))


# ---------------------------------------------------------------------------
# Stub heavy deps that aren't available in the test venv
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

# clickhouse_connect / queries / scout_types — not needed in test venv
for _dep in ("clickhouse_connect", "queries", "scout_types"):
    try:
        importlib.import_module(_dep)
    except ImportError:
        _stub(_dep)

# pytz — stub if absent
try:
    importlib.import_module("pytz")
except ImportError:
    import datetime as _dt_mod

    class _FakeTz:
        """Minimal pytz.timezone stub."""
        def __init__(self, zone: str):
            self.zone = zone

        def localize(self, dt):
            return dt.replace(tzinfo=_dt_mod.timezone.utc)

    _pytz = _stub("pytz")
    _pytz.timezone = _FakeTz
    sys.modules["pytz"] = _pytz

# slack_sdk — stub if absent
try:
    importlib.import_module("slack_sdk")
except ImportError:
    _sdk = _stub("slack_sdk")
    _sdk_web = _stub("slack_sdk.web")
    _sdk_web.WebClient = MagicMock
    sys.modules["slack_sdk"] = _sdk
    sys.modules["slack_sdk.web"] = _sdk_web


# ---------------------------------------------------------------------------
# T1: function exists in demand_feed_main
# ---------------------------------------------------------------------------

class TestDaemonExists(unittest.TestCase):

    def test_projection_autocheck_daemon_callable(self):
        """_projection_autocheck_daemon must be a callable in demand_feed_main."""
        try:
            import demand_feed_main as dm
            importlib.reload(dm)
        except ImportError as exc:
            self.skipTest(f"demand_feed_main not importable: {exc}")
        self.assertTrue(
            callable(getattr(dm, "_projection_autocheck_daemon", None)),
            "_projection_autocheck_daemon should be a callable in demand_feed_main",
        )


# ---------------------------------------------------------------------------
# T2: kill switch off → no ClickHouse query issued
# ---------------------------------------------------------------------------

class TestKillSwitchOff(unittest.TestCase):

    def test_kill_switch_skips_ch_when_disabled(self):
        """With PROJECTION_AUTOCHECK_ENABLED=false (default), daemon sleeps once
        then exits — ClickHouse client is never called."""
        try:
            import demand_feed_main as dm
            importlib.reload(dm)
        except ImportError as exc:
            self.skipTest(f"demand_feed_main not importable: {exc}")

        if not callable(getattr(dm, "_projection_autocheck_daemon", None)):
            self.skipTest("_projection_autocheck_daemon not found")

        # patch time.sleep on the real time module so `import time as _time`
        # inside the daemon function picks up the stub.
        # Use a BaseException subclass (not Exception) so it escapes all
        # try/except Exception blocks inside the daemon.
        class _Sentinel(BaseException):
            pass

        def _fake_sleep(_secs):
            raise _Sentinel("test sentinel")

        ch_get_mock = MagicMock(return_value=MagicMock())

        def _make_stub(name: str, **attrs) -> types.ModuleType:
            """Create a stub module WITHOUT installing it into sys.modules."""
            mod = types.ModuleType(name)
            for k, v in attrs.items():
                setattr(mod, k, v)
            return mod

        scout_agent_stub = _make_stub("scout_agent", _get_ch_client=ch_get_mock,
                                       SCOUT_THRESHOLDS={})
        scout_ch_stub = _make_stub(
            "scout_ch",
            _get_ch_client=ch_get_mock,
            project_today_revenue=MagicMock(return_value={"status": "ok"}),
            _query_intraday_revenue_total=MagicMock(return_value={}),
            _query_intraday_revenue_by_publisher=MagicMock(return_value=[]),
        )
        scout_state_stub = _make_stub(
            "scout_state",
            _load_projection_autocheck_slot=MagicMock(return_value=None),
            _save_projection_autocheck_slot=MagicMock(),
            _load_eod_posted_date=MagicMock(return_value=None),
            _save_eod_posted_date=MagicMock(),
            _load_projection_autocheck_fires=MagicMock(return_value=[]),
            _append_projection_autocheck_fire=MagicMock(),
            _evict_stale_projection_autocheck_fires=MagicMock(),
        )
        job_runs_stub = _make_stub("scout_core.job_runs", record_job_run=MagicMock())

        with patch.dict("os.environ", {"PROJECTION_AUTOCHECK_ENABLED": "false"}), \
             patch.dict(sys.modules, {
                 "scout_agent": scout_agent_stub,
                 "scout_ch": scout_ch_stub,
                 "scout_state": scout_state_stub,
                 "scout_core.job_runs": job_runs_stub,
             }), \
             patch.object(_real_time, "sleep", side_effect=_fake_sleep):
            try:
                dm._projection_autocheck_daemon()
            except _Sentinel:
                pass  # expected — sentinel fired after first sleep

        # ClickHouse client must never have been constructed when kill switch is off.
        ch_get_mock.assert_not_called()


# ---------------------------------------------------------------------------
# T3: demand_feed_main imports cleanly
# ---------------------------------------------------------------------------

class TestImportClean(unittest.TestCase):

    def test_module_imports_without_error(self):
        """demand_feed_main should be importable without raising."""
        try:
            import demand_feed_main as dm
            importlib.reload(dm)
        except ImportError as exc:
            self.fail(f"demand_feed_main raised ImportError: {exc}")
        except Exception as exc:
            # Startup side-effects (mkdir, etc.) are OK; only hard failures matter.
            if "No module named" in str(exc):
                self.skipTest(f"optional dep missing: {exc}")
            raise


if __name__ == "__main__":
    unittest.main()
