"""Tests for the maintenance gate across Scout entry points.

Covers:
- _is_admin() helper: SCOUT_THRESHOLD_ADMINS, SCOUT_ADMIN_USER_ID legacy fallback
- /scout-maintenance toggle: non-admin blocked via old SCOUT_ADMIN_USER_ID guard
- _on_startup: maintenance active → warning posted, NOT auto-cleared; no channel → silent
"""

from __future__ import annotations

import os
import pathlib
import sys
import unittest
import unittest.mock

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Helper: _is_admin (scout_agent)
# ---------------------------------------------------------------------------

class TestIsAdmin(unittest.TestCase):
    """scout_agent._is_admin() — env-based allowlist."""

    def _call(self, user_id: str, *, admins: str = "", legacy: str = "") -> bool:
        import scout_agent
        with unittest.mock.patch.dict(os.environ, {
            "SCOUT_THRESHOLD_ADMINS": admins,
            "SCOUT_ADMIN_USER_ID": legacy,
        }, clear=False):
            return scout_agent._is_admin(user_id)

    def test_returns_false_for_empty_user_id(self):
        self.assertFalse(self._call(""))

    def test_threshold_admin_allowed(self):
        self.assertTrue(self._call("U_ADMIN", admins="U_ADMIN,U_OTHER"))

    def test_non_admin_blocked(self):
        self.assertFalse(self._call("U_RANDOM", admins="U_ADMIN"))

    def test_legacy_env_fallback(self):
        """SCOUT_ADMIN_USER_ID still grants access when THRESHOLD_ADMINS is empty."""
        self.assertTrue(self._call("U_LEGACY", admins="", legacy="U_LEGACY"))

    def test_legacy_does_not_grant_other_users(self):
        self.assertFalse(self._call("U_RANDOM", admins="", legacy="U_LEGACY"))


# ---------------------------------------------------------------------------
# Inline maintenance gate logic in scout_handlers (mention path)
# ---------------------------------------------------------------------------

class TestInlineMaintenanceGate(unittest.TestCase):
    """scout_handlers._is_under_maintenance() — the centralized gate helper.

    Exercises the production function directly with patched dependencies.
    """

    def _call(self, maint_state, is_admin_result: bool, user_id: str = "U_USER") -> bool:
        import scout_handlers
        with unittest.mock.patch("scout_state.get_maintenance", return_value=maint_state), \
             unittest.mock.patch("scout_agent._is_admin", return_value=is_admin_result):
            return scout_handlers._is_under_maintenance(user_id)

    def test_maintenance_active_non_admin_is_blocked(self):
        maint = {"active": True, "set_by": "U_ADMIN", "attempts": []}
        self.assertTrue(self._call(maint, is_admin_result=False))

    def test_maintenance_active_admin_passes(self):
        maint = {"active": True, "set_by": "U_ADMIN", "attempts": []}
        self.assertFalse(self._call(maint, is_admin_result=True))

    def test_maintenance_not_active_passes(self):
        self.assertFalse(self._call(None, is_admin_result=False))

    def test_maintenance_empty_dict_falsy_passes(self):
        """get_maintenance() returns {} (falsy) → gate open."""
        self.assertFalse(self._call({}, is_admin_result=False))

    def test_empty_user_id_short_circuits(self):
        """Empty user_id (bot/system events) → always False, no get_maintenance call."""
        import scout_handlers
        maint = {"active": True, "set_by": "U_ADMIN", "attempts": []}
        with unittest.mock.patch("scout_state.get_maintenance", return_value=maint) as mock_gm:
            result = scout_handlers._is_under_maintenance("")
        self.assertFalse(result)
        mock_gm.assert_not_called()


# ---------------------------------------------------------------------------
# /scout-maintenance toggle admin guard (old SCOUT_ADMIN_USER_ID check)
# ---------------------------------------------------------------------------

class TestMaintenanceToggleAdminGuard(unittest.TestCase):
    """The `/scout-maintenance` slash handler uses `if _admin_id and user_id != _admin_id`.

    Tests 8-9: admin vs non-admin behaviour.
    """

    def _run_toggle(self, user_id: str, admin_id: str) -> list:
        """Returns list of chat_postEphemeral calls."""
        calls = []

        class _FakeWeb:
            def chat_postEphemeral(self, **kwargs):
                calls.append(("ephemeral", kwargs))
            def chat_postMessage(self, **kwargs):
                calls.append(("message", kwargs))

        web = _FakeWeb()
        channel = "C_TEST"

        # Replicate the guard from scout_handlers.py line 2132-2136
        _admin_id = admin_id
        if _admin_id and user_id != _admin_id:
            web.chat_postEphemeral(
                channel=channel, user=user_id,
                text="Only admins can toggle maintenance mode."
            )
            return calls
        # If we get here, admin check passed — simulate a no-op "on" action
        with unittest.mock.patch("scout_state.set_maintenance", return_value={"active": True}):
            web.chat_postMessage(channel=channel, text=":wrench: Maintenance on.")
        return calls

    def test_admin_user_passes_guard(self):
        calls = self._run_toggle("U_ADMIN", admin_id="U_ADMIN")
        ephemeral_calls = [c for c in calls if c[0] == "ephemeral"]
        self.assertEqual(ephemeral_calls, [])
        message_calls = [c for c in calls if c[0] == "message"]
        self.assertEqual(len(message_calls), 1)

    def test_non_admin_blocked_by_guard(self):
        calls = self._run_toggle("U_RANDOM", admin_id="U_ADMIN")
        ephemeral_calls = [c for c in calls if c[0] == "ephemeral"]
        self.assertEqual(len(ephemeral_calls), 1)
        self.assertIn("Only admins", ephemeral_calls[0][1]["text"])

    def test_empty_admin_id_allows_everyone(self):
        """Old guard: if _admin_id is '', condition is False → anyone gets through."""
        calls = self._run_toggle("U_RANDOM", admin_id="")
        ephemeral_calls = [c for c in calls if c[0] == "ephemeral"]
        self.assertEqual(ephemeral_calls, [])


# ---------------------------------------------------------------------------
# _on_startup (scout_bot)
# ---------------------------------------------------------------------------

class TestOnStartup(unittest.TestCase):
    """scout_bot._on_startup(): maintenance-aware startup message."""

    def _make_web(self):
        calls = []

        class _FakeWeb:
            def chat_postMessage(self, **kwargs):
                calls.append(kwargs)

        return _FakeWeb(), calls

    def test_maintenance_active_posts_warning_does_not_clear(self):
        maint = {"active": True, "set_by": "U_ADMIN", "set_at": "2026-06-09T00:00:00",
                 "attempts": [{"user_id": "U1", "query": "test"}]}
        web, calls = self._make_web()
        clear_calls = []

        with unittest.mock.patch.dict(os.environ, {"SIDD_QA_CHANNEL_ID": "C_QA"}), \
             unittest.mock.patch("scout_state.get_maintenance", return_value=maint), \
             unittest.mock.patch("scout_state.clear_maintenance",
                                 side_effect=lambda: clear_calls.append(1) or []):
            import scout_bot
            scout_bot._on_startup(web)

        self.assertEqual(len(calls), 1)
        self.assertIn("Maintenance is still active", calls[0]["text"])
        self.assertIn("1 blocked", calls[0]["text"])
        self.assertEqual(clear_calls, [], "clear_maintenance must NOT be called on startup")

    def test_no_maintenance_posts_back_online(self):
        web, calls = self._make_web()
        with unittest.mock.patch.dict(os.environ, {"SIDD_QA_CHANNEL_ID": "C_QA"}), \
             unittest.mock.patch("scout_state.get_maintenance", return_value=None):
            import scout_bot
            scout_bot._on_startup(web)

        self.assertEqual(len(calls), 1)
        self.assertIn("back online", calls[0]["text"])
        self.assertNotIn("Maintenance is still active", calls[0]["text"])

    def test_no_sidd_qa_channel_silent(self):
        web, calls = self._make_web()
        env = {k: v for k, v in os.environ.items()}
        env.pop("SIDD_QA_CHANNEL_ID", None)
        with unittest.mock.patch.dict(os.environ, {"SIDD_QA_CHANNEL_ID": ""}), \
             unittest.mock.patch("scout_state.get_maintenance", return_value=None):
            import scout_bot
            scout_bot._on_startup(web)

        self.assertEqual(calls, [], "should not post when SIDD_QA_CHANNEL_ID is unset")


if __name__ == "__main__":
    unittest.main()
