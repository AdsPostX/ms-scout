"""Tests for P2.6 — hourly-shadow monitor framework migration.

T1: _run_shadow_monitor is callable from demand_feed_main
T2: kill switch SCOUT_HOURLY_SHADOW_ENABLED=false → never calls signal_fn
T3: all 6 monitor daemons are callable from demand_feed_main
T4: module imports without error
"""
from __future__ import annotations

import importlib
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
# ---------------------------------------------------------------------------

def _stub(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# anthropic
try:
    importlib.import_module("anthropic")
except ImportError:
    _ant = _stub("anthropic")
    _ant.Anthropic = MagicMock
    _ant.types = _stub("anthropic.types")
    sys.modules["anthropic.types"] = _ant.types

# clickhouse_connect / queries / scout_types
for _dep in ("clickhouse_connect", "queries", "scout_types"):
    try:
        importlib.import_module(_dep)
    except ImportError:
        _stub(_dep)

# pytz — may be absent in minimal test envs; stub it if needed
try:
    importlib.import_module("pytz")
except ImportError:
    import datetime as _datetime_mod

    class _FakeTZ(_datetime_mod.tzinfo):
        def utcoffset(self, dt):
            return _datetime_mod.timedelta(hours=-6)
        def dst(self, dt):
            return _datetime_mod.timedelta(0)
        def tzname(self, dt):
            return "CST"

    _pytz = _stub("pytz")
    _pytz.timezone = MagicMock(return_value=_FakeTZ())

# slack_sdk
try:
    importlib.import_module("slack_sdk")
except ImportError:
    _slack_sdk = _stub("slack_sdk")
    _slack_sdk_web = _stub("slack_sdk.web")
    _slack_sdk_web.WebClient = MagicMock
    sys.modules["slack_sdk.web"] = _slack_sdk_web


# ---------------------------------------------------------------------------
# T1: _run_shadow_monitor is importable and callable
# ---------------------------------------------------------------------------

class TestRunShadowMonitorImportable(unittest.TestCase):

    def test_function_exists_on_module(self):
        """_run_shadow_monitor should be a callable on demand_feed_main."""
        import demand_feed_main as dm
        self.assertTrue(
            callable(getattr(dm, "_run_shadow_monitor", None)),
            "_run_shadow_monitor not found or not callable on demand_feed_main",
        )


# ---------------------------------------------------------------------------
# T2: kill switch — SCOUT_HOURLY_SHADOW_ENABLED=false never calls signal_fn
# ---------------------------------------------------------------------------

class TestKillSwitch(unittest.TestCase):

    def test_kill_switch_prevents_signal_fn_call(self):
        """With SCOUT_HOURLY_SHADOW_ENABLED unset, signal_fn must never be called."""
        import demand_feed_main as dm

        signal_mock = MagicMock(return_value=[])
        format_mock = MagicMock(return_value=("fallback", []))
        load_state_mock = MagicMock(return_value=None)
        save_state_mock = MagicMock()

        env_override = {
            "SCOUT_HOURLY_SHADOW_ENABLED": "false",
        }

        # patch time.sleep to raise StopIteration after the first inner-loop sleep
        # so the daemon exits cleanly after one cycle without actually sleeping 300s
        with patch.dict("os.environ", env_override, clear=False), \
             patch("time.sleep", side_effect=StopIteration):
            try:
                dm._run_shadow_monitor(
                    monitor_name="test-monitor",
                    config_key="test",
                    signal_fn=signal_mock,
                    format_fn=format_mock,
                    load_state_fn=load_state_mock,
                    save_state_fn=save_state_mock,
                )
            except StopIteration:
                pass  # expected — StopIteration from the patched time.sleep

        signal_mock.assert_not_called()


# ---------------------------------------------------------------------------
# T3: all 6 monitor daemon functions are callable from demand_feed_main
# ---------------------------------------------------------------------------

class TestMonitorDaemonsExist(unittest.TestCase):

    _DAEMON_NAMES = [
        "_cap_monitor_daemon",
        "_velocity_down_monitor_daemon",
        "_ghost_monitor_daemon",
        "_fill_monitor_daemon",
        "_cvr_anomaly_monitor_daemon",
        "_expiration_monitor_daemon",
    ]

    def test_all_six_daemons_callable(self):
        """All 6 monitor daemon wrappers must be callable on demand_feed_main."""
        import demand_feed_main as dm
        for name in self._DAEMON_NAMES:
            with self.subTest(daemon=name):
                fn = getattr(dm, name, None)
                self.assertIsNotNone(fn, f"{name} not found on demand_feed_main")
                self.assertTrue(callable(fn), f"{name} is not callable")


# ---------------------------------------------------------------------------
# T4: module imports without error
# ---------------------------------------------------------------------------

class TestModuleImports(unittest.TestCase):

    def test_demand_feed_main_imports_cleanly(self):
        """demand_feed_main should import without raising."""
        try:
            import demand_feed_main  # noqa: F401
        except Exception as exc:
            self.fail(f"demand_feed_main import raised: {exc}")


if __name__ == "__main__":
    unittest.main()
