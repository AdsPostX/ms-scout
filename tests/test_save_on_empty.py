"""
Save-on-empty regression tests — Phase 1.5-lite.

Pins the trust-killer fix: when a silent monitor's signal query returns no
rows, the per-day state file must NOT be written. The old behavior silenced
the monitor for the next 23h59m (AT&T Payment Confirmation, May 9).

No ClickHouse, no Slack, no Anthropic — exercises the generic monitor body
`_run_with_web` with mocked dependencies. The 5-min poll loop is broken
after one iteration by having the patched `time.sleep` raise StopIteration.
"""
import os
import sys
import unittest
import unittest.mock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import scout_bot  # noqa: E402


class _LoopBreak(Exception):
    """Raised by patched sleep to escape the inner poll loop after one tick."""


def _make_signal_fn(rows):
    return lambda _ch: rows


def _run_one_iteration(
    *,
    signal_rows,
    in_fire_window: bool,
    shadow_on: bool = False,
    saved_today=None,
):
    """Drive _run_with_web through exactly one inner-loop iteration.

    Returns (save_state_calls, post_calls, dq_advisory_calls).
    """
    import datetime as _dtmod
    from datetime import datetime as _real_dt

    save_state_calls: list = []
    post_calls: list = []

    web = unittest.mock.MagicMock()
    def _record_post(**kwargs):
        post_calls.append(kwargs)
        return {"ok": True}
    web.chat_postMessage.side_effect = _record_post

    def load_state_fn():
        return saved_today

    def save_state_fn(d):
        save_state_calls.append(d)

    # First sleep returns normally (entering the loop body); the second sleep
    # (which would start iter #2) raises to exit.
    sleep_counter = {"n": 0}
    def _fake_sleep(_secs):
        sleep_counter["n"] += 1
        if sleep_counter["n"] >= 2:
            raise _LoopBreak()

    # Pick a "now" that hits the fire window iff requested. The monitor reads
    # check_hour from SCOUT_THRESHOLDS; we patch SCOUT_THRESHOLDS to lock it.
    fire_hour = 9
    now_hour = fire_hour if in_fire_window else (fire_hour + 3) % 24
    fixed_now = _real_dt(2026, 5, 18, now_hour, 1, 0)

    class _FakeDt:
        @staticmethod
        def now(tz=None):
            # Return tz-aware datetime in the same tz to satisfy CT_TZ access.
            if tz is None:
                return fixed_now
            return tz.localize(fixed_now)

    with unittest.mock.patch.object(scout_bot, "_monitor_enabled", return_value=True), \
         unittest.mock.patch.object(
             scout_bot, "_hourly_shadow_enabled", return_value=shadow_on
         ), \
         unittest.mock.patch(
             "scout_agent.SCOUT_THRESHOLDS",
             {"signals": {"cap_monitor_check_hour_ct": fire_hour}},
             create=True,
         ), \
         unittest.mock.patch("scout_agent._get_ch_client", create=True,
                             return_value=unittest.mock.MagicMock()), \
         unittest.mock.patch("time.sleep", side_effect=_fake_sleep), \
         unittest.mock.patch("datetime.datetime", _FakeDt):
        try:
            scout_bot._run_with_web(
                web,
                monitor_name="cap-monitor",
                config_key="cap",
                signal_fn=_make_signal_fn(signal_rows),
                format_fn=lambda rows: (
                    f"alert: {len(rows)} rows",
                    [{"type": "section", "text": {"type": "mrkdwn", "text": "x"}}],
                ),
                load_state_fn=load_state_fn,
                save_state_fn=save_state_fn,
            )
        except _LoopBreak:
            pass

    return save_state_calls, post_calls


class TestSaveOnEmpty(unittest.TestCase):
    def test_empty_results_does_not_persist_state(self):
        saves, posts = _run_one_iteration(
            signal_rows=[], in_fire_window=True, saved_today=None,
        )
        # The fix: empty result must NOT write today_str.
        self.assertEqual(saves, [])
        # No post when there's nothing to alert on.
        self.assertEqual(posts, [])

    def test_real_fire_does_persist_state(self):
        rows = [{"publisher_name": "P", "cvr_today": 0.04, "sessions_today": 100}]
        saves, posts = _run_one_iteration(
            signal_rows=rows, in_fire_window=True, saved_today=None,
        )
        # One save (today_str) on real fire.
        self.assertEqual(len(saves), 1)
        self.assertEqual(len(posts), 1)

    def test_dq_dropped_results_treated_as_empty_no_persist(self):
        # All rows hit the DQ filter (cvr_today == 100% misfire). After
        # filtering, results are effectively empty → must not persist.
        rows = [
            {"publisher_name": "Bad1", "cvr_today": 1.0, "sessions_today": 100},
            {"publisher_name": "Bad2", "cvr_today": 100.0, "sessions_today": 50},
        ]
        saves, posts = _run_one_iteration(
            signal_rows=rows, in_fire_window=True, saved_today=None,
        )
        self.assertEqual(saves, [])
        # Production channel must not see an alert; only the DQ advisory hits
        # the shadow channel. Both are chat_postMessage calls though, so we
        # check that no post text resembles the formatter output.
        production_alerts = [p for p in posts if p.get("text", "").startswith("alert:")]
        self.assertEqual(production_alerts, [])


class TestHourlyShadow(unittest.TestCase):
    def test_shadow_off_outside_window_skips(self):
        saves, posts = _run_one_iteration(
            signal_rows=[{"publisher_name": "P"}],
            in_fire_window=False,
            shadow_on=False,
        )
        self.assertEqual(saves, [])
        self.assertEqual(posts, [])

    def test_shadow_on_outside_window_routes_to_shadow_channel(self):
        rows = [{"publisher_name": "P", "cvr_today": 0.04, "sessions_today": 100}]
        saves, posts = _run_one_iteration(
            signal_rows=rows, in_fire_window=False, shadow_on=True,
        )
        # Shadow fire must NOT persist daily state.
        self.assertEqual(saves, [])
        # Should have posted to the HQ/shadow channel.
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]["channel"], scout_bot._SCOUT_HQ_CHANNEL)


if __name__ == "__main__":
    unittest.main()
