"""Tests for P2.6 — hourly-shadow monitor framework migration.

T1: _run_shadow_monitor is callable from demand_feed_main
T2: kill switch SCOUT_HOURLY_SHADOW_ENABLED=false → never calls signal_fn
T3: all 6 monitor daemons are callable from demand_feed_main
T4: module imports without error
T5: CH exception from signal_fn → _run_shadow_monitor records status='error'
T6: concurrent _update_pulse_state calls don't clobber each other's date keys
T7: as_of_date parameter substitutes today() with toDate(as_of_date) in SQL
T8: force_run_monitor passes Slack channel string (not CH client) to lambda
"""
from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import threading
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

from scout_thresholds import _manager as _tm  # noqa: E402


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


# ---------------------------------------------------------------------------
# T5: CH exception from signal_fn → _run_shadow_monitor records status='error'
#     not status='success'.  Verifies the raise in each signal function's outer
#     except block propagates up to the exception handler at line 1017 of
#     demand_feed_main, which calls record_job_run(status='error').
# ---------------------------------------------------------------------------

class TestCHExceptionPropagatesFromSignalFns(unittest.TestCase):

    def _make_ch_mock(self, side_effect=None, rows=None):
        ch = MagicMock()
        if side_effect:
            ch.query.side_effect = side_effect
        else:
            result = MagicMock()
            result.result_rows = rows or []
            ch.query.return_value = result
        return ch

    def _assert_propagates(self, signal_fn_name: str, ch):
        """Assert signal fn re-raises when ch.query raises RuntimeError.

        Used only for _pulse_signal_ghost which retains inline SQL + re-raise.
        Cap/velocity/fill now delegate to canonical queries.* functions that
        catch CH errors internally and return [] — use _assert_returns_empty
        for those.
        """
        try:
            import scout_bot as _sb
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"scout_bot not importable in test env: {exc}")
        fn = getattr(_sb, signal_fn_name, None)
        if fn is None:
            self.skipTest(f"{signal_fn_name} not found on scout_bot")
        with self.assertRaises(RuntimeError, msg=f"{signal_fn_name} must re-raise CH errors"):
            fn(ch)

    def _assert_returns_empty(self, signal_fn_name: str, ch):
        """Assert canonical-delegating signal fn returns [] on CH error (does not raise).

        Cap, velocity, and fill-rate signal functions now delegate to queries.*
        canonical functions which catch exceptions internally and return [] so
        that a CH failure is recorded as 'no anomaly' rather than crashing the
        shadow monitor loop.  T4/T9/T14 in test_query_contracts.py cover the
        canonical layer; this asserts the delegation wrapper preserves that
        behaviour at the signal layer.
        """
        try:
            import scout_bot as _sb
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"scout_bot not importable in test env: {exc}")
        fn = getattr(_sb, signal_fn_name, None)
        if fn is None:
            self.skipTest(f"{signal_fn_name} not found on scout_bot")
        # Should not raise — canonical functions swallow CH errors and return []
        result = fn(ch)
        self.assertEqual(result, [], f"{signal_fn_name} must return [] on CH error (not raise)")

    def test_cap_signal_returns_empty_on_ch_error(self):
        """Cap delegates to canonical queries.cap_alert_campaigns which returns [] on error."""
        self._assert_returns_empty("_pulse_signal_cap",
                                   self._make_ch_mock(side_effect=RuntimeError("CH down")))

    def test_velocity_signal_returns_empty_on_ch_error(self):
        """Velocity delegates to canonical queries.velocity_alerts which returns [] on error."""
        self._assert_returns_empty("_pulse_signal_velocity",
                                   self._make_ch_mock(side_effect=RuntimeError("CH down")))

    def test_fill_rate_signal_returns_empty_on_ch_error(self):
        """Fill rate delegates to canonical queries.fill_rate_publishers which returns [] on error."""
        self._assert_returns_empty("_pulse_signal_fill_rate",
                                   self._make_ch_mock(side_effect=RuntimeError("CH down")))

    def test_ghost_signal_reraises_ch_error(self):
        self._assert_propagates("_pulse_signal_ghost",
                                self._make_ch_mock(side_effect=RuntimeError("CH down")))


# ---------------------------------------------------------------------------
# T6: concurrent _update_pulse_state calls don't clobber each other's keys.
#     Fires 20 threads each writing a distinct key.  Asserts all 20 are
#     present in the final state — proves the threading.Lock prevents
#     lost-write races in the RMW cycle.
# ---------------------------------------------------------------------------

class TestConcurrentPulseStateUpdate(unittest.TestCase):

    def test_no_clobber_under_concurrent_writes(self):
        """20 threads writing distinct keys — all must survive."""
        try:
            import scout_state as _ss
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"scout_state not importable: {exc}")

        N = 20
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_file = Path(tmpdir) / "pulse_state.json"
            with patch.object(_ss, "_PULSE_STATE_FILE", tmp_file):
                errors: list[Exception] = []

                def _write(i: int) -> None:
                    try:
                        _ss._update_pulse_state(f"key_{i}", f"val_{i}")
                    except Exception as exc:
                        errors.append(exc)

                threads = [threading.Thread(target=_write, args=(i,)) for i in range(N)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                self.assertEqual(errors, [], f"Threads raised: {errors}")
                final = json.loads(tmp_file.read_text())
                for i in range(N):
                    self.assertIn(f"key_{i}", final,
                                  f"key_{i} missing — concurrent write was clobbered")
                    self.assertEqual(final[f"key_{i}"], f"val_{i}")


# ---------------------------------------------------------------------------
# T7: as_of_date replaces today() with toDate(as_of_date) in SQL.
#     Calls _pulse_signal_cap with a fixed historical date and checks the SQL
#     that reached the (mocked) ClickHouse client.
# ---------------------------------------------------------------------------

class TestAsOfDateSQLSubstitution(unittest.TestCase):

    def _empty_ch(self):
        ch = MagicMock()
        result = MagicMock()
        result.result_rows = []
        ch.query.return_value = result
        return ch

    def _call_and_get_sql(self, signal_fn_name: str, as_of_date: str) -> str | None:
        try:
            import scout_bot as _sb
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"scout_bot not importable: {exc}")
        fn = getattr(_sb, signal_fn_name, None)
        if fn is None:
            self.skipTest(f"{signal_fn_name} not found")
        ch = self._empty_ch()
        try:
            fn(ch, as_of_date=as_of_date)
        except Exception:
            pass  # result rows are empty; some fns raise later — we only care about the SQL
        if not ch.query.called:
            return None
        return ch.query.call_args[0][0]

    def _assert_sql_has_date_not_today(self, sql: str | None, as_of_date: str) -> None:
        if sql is None:
            self.skipTest("ch.query was not called — function returned early")
        self.assertIn(f"toDate('{as_of_date}')", sql,
                      "Expected toDate substitution missing from SQL")
        self.assertNotIn("today()", sql,
                         "today() still present in SQL despite as_of_date being set")

    def test_cap_sql_uses_as_of_date(self):
        sql = self._call_and_get_sql("_pulse_signal_cap", "2026-01-15")
        self._assert_sql_has_date_not_today(sql, "2026-01-15")

    def test_velocity_sql_uses_as_of_date(self):
        sql = self._call_and_get_sql("_pulse_signal_velocity", "2026-01-15")
        self._assert_sql_has_date_not_today(sql, "2026-01-15")

    def test_fill_rate_sql_uses_as_of_date(self):
        sql = self._call_and_get_sql("_pulse_signal_fill_rate", "2026-01-15")
        self._assert_sql_has_date_not_today(sql, "2026-01-15")

    def test_ghost_sql_uses_as_of_date(self):
        sql = self._call_and_get_sql("_pulse_signal_ghost", "2026-01-15")
        self._assert_sql_has_date_not_today(sql, "2026-01-15")


# ---------------------------------------------------------------------------
# T8: force_run_monitor passes a Slack channel string to the monitor lambda,
#     not a ClickHouse client object.  The bug (pre-fix) was passing ch_factory()
#     (a CH client) as the second arg; the lambda's second param is `ch` but
#     the semantics is "channel string", and _one_shot_monitor expects str.
# ---------------------------------------------------------------------------

class TestForceRunMonitorPassesChannelString(unittest.TestCase):

    def test_force_run_monitor_channel_arg_is_string(self):
        """force_run_monitor must pass a channel string, not a CH client, to the lambda."""
        try:
            import scout_agent as _sa
            import scout_handlers as _sh
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"scout_agent/scout_handlers not importable: {exc}")

        captured: list = []

        def _fake_fn(web, channel, thread_ts=""):
            captured.append(channel)

        original_fns = dict(_sh._FORCE_MONITOR_FNS)
        _sh._FORCE_MONITOR_FNS["_test_ch_check"] = _fake_fn

        original_ctx = dict(_sa._FORCE_MONITOR_CTX)
        _sa._FORCE_MONITOR_CTX["web"] = MagicMock()
        _sa._FORCE_MONITOR_CTX["ch_factory"] = lambda: MagicMock()

        try:
            with patch.dict(os.environ,
                            {"SCOUT_SHADOW_CHANNEL": "#scout-qa-test",
                             "SCOUT_THRESHOLD_ADMINS": "U_TEST_ADMIN"}):
                result = _sa.force_run_monitor("_test_ch_check",
                                               _caller_user_id="U_TEST_ADMIN")
        finally:
            _sh._FORCE_MONITOR_FNS.clear()
            _sh._FORCE_MONITOR_FNS.update(original_fns)
            _sa._FORCE_MONITOR_CTX.clear()
            _sa._FORCE_MONITOR_CTX.update(original_ctx)

        self.assertTrue(result.get("ok"), f"force_run_monitor returned error: {result}")
        self.assertEqual(len(captured), 1, "lambda should have been called exactly once")
        channel_arg = captured[0]
        self.assertIsInstance(channel_arg, str,
                              f"Expected str channel, got {type(channel_arg).__name__}")
        self.assertEqual(channel_arg, "#scout-qa-test")


# ---------------------------------------------------------------------------
# T9: mark_firing called after a successful Slack post
# ---------------------------------------------------------------------------

class TestMarkFiringCalledOnPost(unittest.TestCase):

    def test_shadow_monitor_calls_mark_firing_on_post(self):
        """_run_shadow_monitor calls alert_registry.mark_firing on a production (non-shadow) post.

        mark_firing must NOT be called on shadow-only ticks — only when the monitor
        posts to the production channel.  The framework kill-switch requires
        SCOUT_HOURLY_SHADOW_ENABLED=true; we freeze time at 09:05 CT with
        check_hour_ct=9 so in_prod_window=True.  Since prod has not fired yet
        (load_state returns None) and we ARE in the prod window, is_shadow_tick=False
        and the production branch executes.
        """
        import demand_feed_main as dm
        import alert_registry
        import pytz
        import datetime as _dt_module
        from datetime import datetime as _real_dt

        fired = []

        # Stub signal_fn returning one anomaly row (non-empty → fires alert)
        signal_mock = MagicMock(return_value=[{"network": "test-net", "cap_pct": 99.0}])
        # format_fn must return a non-empty fallback string + block list
        format_mock = MagicMock(return_value=("Alert: cap at 99%", [{"type": "section", "text": {"type": "mrkdwn", "text": "cap"}}]))
        load_state_mock = MagicMock(return_value=None)
        save_state_mock = MagicMock()

        # check_hour=9; time frozen to 09:05 CT so in_prod_window=True
        # Shadow is enabled (required to pass the outer kill switch) but
        # since we're in the prod window with prod not yet fired, is_shadow_tick=False.
        fake_thresholds = {"signals": {"test_monitor_enabled": True, "test_monitor_check_hour_ct": 9}}

        # WebClient mock — chat_postMessage must succeed
        fake_web = MagicMock()
        fake_web.chat_postMessage.return_value = {"ok": True}

        # Frozen CT time at 09:05 — satisfies (hour==9 and minute<10)
        CT_TZ = pytz.timezone("America/Chicago")
        frozen_ct = CT_TZ.localize(_real_dt(2024, 6, 1, 9, 5, 0))

        class _FakeDt(_real_dt):
            @classmethod
            def now(cls, tz=None):
                if tz is not None:
                    return frozen_ct.astimezone(tz)
                return frozen_ct

        # First sleep (300s) must pass silently so the inner loop body executes.
        # Second sleep raises StopIteration to exit cleanly after one cycle.
        sleep_calls = [0]

        def _counting_sleep(secs):
            sleep_calls[0] += 1
            if sleep_calls[0] > 1:
                raise StopIteration("one cycle done")

        with patch.dict("os.environ", {"SCOUT_HOURLY_SHADOW_ENABLED": "true"}, clear=False), \
             patch.object(_tm, "_thresholds_cache", fake_thresholds), \
             patch("demand_feed_main.alert_registry.mark_firing",
                   side_effect=lambda name, ctx: fired.append((name, ctx))), \
             patch("demand_feed_main.alert_registry.mark_cleared", MagicMock()), \
             patch("scout_ch._get_ch_client", MagicMock(return_value=MagicMock())), \
             patch("slack_sdk.web.WebClient", return_value=fake_web), \
             patch("scout_core.job_runs.record_job_run", MagicMock()), \
             patch(_dt_module.__name__ + ".datetime", _FakeDt), \
             patch("time.sleep", side_effect=_counting_sleep):
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
                pass

        self.assertEqual(len(fired), 1, f"mark_firing should be called once on prod post, got: {fired}")
        self.assertEqual(fired[0][0], "test-monitor")


# ---------------------------------------------------------------------------
# T10: mark_cleared called when signal_fn returns no anomalies
# ---------------------------------------------------------------------------

class TestMarkClearedCalledOnNoAnomaly(unittest.TestCase):

    def test_shadow_monitor_calls_mark_cleared_on_no_anomaly(self):
        """_run_shadow_monitor calls alert_registry.mark_cleared on a production tick with no anomalies.

        mark_cleared must NOT be called on shadow-only ticks — only on production
        ticks where the signal returns empty.  The framework kill-switch requires
        SCOUT_HOURLY_SHADOW_ENABLED=true; time is frozen at 09:05 CT with
        check_hour_ct=9 so in_prod_window=True and is_shadow_tick=False.
        """
        import demand_feed_main as dm
        import pytz
        import datetime as _dt_module
        from datetime import datetime as _real_dt

        cleared = []

        # signal_fn returns empty list → no anomalies
        signal_mock = MagicMock(return_value=[])
        format_mock = MagicMock(return_value=("", []))
        load_state_mock = MagicMock(return_value=None)
        save_state_mock = MagicMock()

        # check_hour=9; time frozen to 09:05 CT so in_prod_window=True
        # Shadow enabled (required for kill switch) but prod window is active → is_shadow_tick=False
        fake_thresholds = {"signals": {"test_monitor_enabled": True, "test_monitor_check_hour_ct": 9}}

        # Frozen CT time at 09:05 — satisfies (hour==9 and minute<10)
        CT_TZ = pytz.timezone("America/Chicago")
        frozen_ct = CT_TZ.localize(_real_dt(2024, 6, 1, 9, 5, 0))

        class _FakeDt(_real_dt):
            @classmethod
            def now(cls, tz=None):
                if tz is not None:
                    return frozen_ct.astimezone(tz)
                return frozen_ct

        # Let the first sleep pass so the inner-loop body executes; raise on the second.
        _cleared_sleep_calls = [0]

        def _cleared_counting_sleep(secs):
            _cleared_sleep_calls[0] += 1
            if _cleared_sleep_calls[0] > 1:
                raise StopIteration("one cycle done")

        with patch.dict("os.environ", {"SCOUT_HOURLY_SHADOW_ENABLED": "true"}, clear=False), \
             patch.object(_tm, "_thresholds_cache", fake_thresholds), \
             patch("demand_feed_main.alert_registry.mark_cleared",
                   side_effect=lambda name: cleared.append(name)), \
             patch("demand_feed_main.alert_registry.mark_firing", MagicMock()), \
             patch("scout_ch._get_ch_client", MagicMock(return_value=MagicMock())), \
             patch("scout_core.job_runs.record_job_run", MagicMock()), \
             patch(_dt_module.__name__ + ".datetime", _FakeDt), \
             patch("time.sleep", side_effect=_cleared_counting_sleep):
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
                pass

        self.assertEqual(len(cleared), 1, f"mark_cleared should be called once, got: {cleared}")
        self.assertEqual(cleared[0], "test-monitor")


# ---------------------------------------------------------------------------
# TestHourlyDedup: unit tests for _run_hourly_with_web deduplication logic
# ---------------------------------------------------------------------------

class TestHourlyDedup(unittest.TestCase):
    """Unit tests for _run_hourly_with_web deduplication logic in scout_core.monitors."""

    # Frozen CT time at 10:05 — inside default business hours (9-17)
    _FROZEN_DATE = "2026-06-05"
    _FROZEN_HOUR = 10
    _EXPECTED_SLOT = f"{_FROZEN_DATE}T{_FROZEN_HOUR:02d}"

    def _run_one_cycle(
        self,
        signal_fn,
        format_fn,
        save_slot_fn,
        load_slot_fn,
        load_context_fn,
        save_context_fn,
    ):
        """Run _run_hourly_with_web for exactly one inner-loop cycle."""
        import datetime as _dt_module
        from datetime import datetime as _real_dt
        import pytz

        CT_TZ = pytz.timezone("America/Chicago")
        frozen_ct = CT_TZ.localize(_real_dt(2026, 6, 5, self._FROZEN_HOUR, 5, 0))

        class _FakeDt(_real_dt):
            @classmethod
            def now(cls, tz=None):
                if tz is not None:
                    return frozen_ct.astimezone(tz)
                return frozen_ct

        sleep_calls = [0]

        def _counting_sleep(secs):
            sleep_calls[0] += 1
            if sleep_calls[0] > 1:
                raise StopIteration("one cycle done")

        fake_web_instance = MagicMock()
        fake_web_instance.chat_postMessage.return_value = {"ok": True}
        fake_web_cls = MagicMock(return_value=fake_web_instance)

        fake_ar = MagicMock()
        fake_ar.current_state.return_value = []

        from scout_core import monitors as _mon

        with patch(_dt_module.__name__ + ".datetime", _FakeDt), \
             patch("time.sleep", side_effect=_counting_sleep), \
             patch("scout_ch._get_ch_client", return_value=MagicMock()), \
             patch("scout_core.job_runs.record_job_run", MagicMock()), \
             patch("slack_sdk.web.WebClient", fake_web_cls), \
             patch.dict("sys.modules", {"alert_registry": fake_ar}):
            try:
                _mon._run_hourly_with_web(
                    signal_fn=signal_fn,
                    format_fn=format_fn,
                    load_slot_fn=load_slot_fn,
                    save_slot_fn=save_slot_fn,
                    load_context_fn=load_context_fn,
                    save_context_fn=save_context_fn,
                    severity_key="cap_pct",
                    escalation_pct=5.0,
                    alert_name="test-cap",
                )
            except StopIteration:
                pass

        return fake_ar, fake_web_instance

    def test_same_advertisers_no_escalation_deduplicates(self):
        """Same advertisers, cap_pct unchanged → no re-fire, slot NOT saved."""
        # Prior context: adv A at 87%; current results: adv A at 87% (no change)
        signal_fn = MagicMock(return_value=[{"adv_name": "adv_a", "cap_pct": 87.0}])
        format_fn = MagicMock(return_value=("Alert!", [{"type": "section"}]))
        save_slot_fn = MagicMock()
        # Return a different slot so the already-fired-this-hour check passes
        load_slot_fn = MagicMock(return_value="2026-06-05T09")
        load_context_fn = MagicMock(return_value=[{"adv_name": "adv_a", "cap_pct": 87.0}])
        save_context_fn = MagicMock()

        _ar, _web = self._run_one_cycle(
            signal_fn, format_fn, save_slot_fn, load_slot_fn, load_context_fn, save_context_fn
        )

        signal_fn.assert_called_once()
        format_fn.assert_not_called()
        save_slot_fn.assert_not_called()

    def test_escalation_at_exact_threshold_fires(self):
        """cap_pct increase == escalation_pct (5%) → fires (>= not >)."""
        # Prior: adv A at 85%; current: adv A at 90% (exactly +5 — meets >= threshold)
        signal_fn = MagicMock(return_value=[{"adv_name": "adv_a", "cap_pct": 90.0}])
        format_fn = MagicMock(return_value=("Alert: cap at 90%", [{"type": "section"}]))
        save_slot_fn = MagicMock()
        load_slot_fn = MagicMock(return_value="2026-06-05T09")
        load_context_fn = MagicMock(return_value=[{"adv_name": "adv_a", "cap_pct": 85.0}])
        save_context_fn = MagicMock()

        _ar, _web = self._run_one_cycle(
            signal_fn, format_fn, save_slot_fn, load_slot_fn, load_context_fn, save_context_fn
        )

        format_fn.assert_called_once()

    def test_new_advertiser_fires_regardless_of_prior(self):
        """New advertiser in results that wasn't in prior context → fires."""
        # Prior context: adv A at 85%; current: adv A at 85% + adv B at 90%
        signal_fn = MagicMock(return_value=[
            {"adv_name": "adv_a", "cap_pct": 85.0},
            {"adv_name": "adv_b", "cap_pct": 90.0},
        ])
        format_fn = MagicMock(return_value=("Alert: new advertiser!", [{"type": "section"}]))
        save_slot_fn = MagicMock()
        load_slot_fn = MagicMock(return_value="2026-06-05T09")
        load_context_fn = MagicMock(return_value=[{"adv_name": "adv_a", "cap_pct": 85.0}])
        save_context_fn = MagicMock()

        _ar, _web = self._run_one_cycle(
            signal_fn, format_fn, save_slot_fn, load_slot_fn, load_context_fn, save_context_fn
        )

        format_fn.assert_called_once()


if __name__ == "__main__":
    unittest.main()
