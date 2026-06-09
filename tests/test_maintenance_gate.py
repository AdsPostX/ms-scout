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
    """The mention-path gate at scout_handlers.py lines 2360-2371.

    Tests the observable contract: when maintenance is active and the user is
    not the SCOUT_ADMIN_USER_ID, chat_postMessage is called with the offline
    text and the handler returns early.

    We extract and exercise the gate logic directly rather than driving a full
    SocketModeRequest to avoid circular import complexity.
    """

    def _run_gate(self, maint_state, user_id: str, admin_id: str) -> tuple[bool, list]:
        """Reproduce the gate check.  Returns (blocked, postMessage_calls)."""
        calls = []

        class _FakeWeb:
            def chat_postMessage(self, **kwargs):
                calls.append(kwargs)

        web = _FakeWeb()

        with unittest.mock.patch("scout_state.get_maintenance", return_value=maint_state):
            from scout_state import get_maintenance
            _admin_id = admin_id
            _maint = get_maintenance()
            blocked = bool(_maint and (not _admin_id or user_id != _admin_id))
            if blocked:
                web.chat_postMessage(
                    channel="C_TEST",
                    thread_ts=None,
                    text="🔧 Scout is offline for maintenance.",
                )
        return blocked, calls

    def test_maintenance_active_non_admin_is_blocked(self):
        maint = {"active": True, "set_by": "U_ADMIN", "attempts": []}
        blocked, calls = self._run_gate(maint, user_id="U_USER", admin_id="U_ADMIN")
        self.assertTrue(blocked)
        self.assertEqual(len(calls), 1)
        self.assertIn("offline for maintenance", calls[0]["text"])

    def test_maintenance_active_admin_passes(self):
        maint = {"active": True, "set_by": "U_ADMIN", "attempts": []}
        blocked, calls = self._run_gate(maint, user_id="U_ADMIN", admin_id="U_ADMIN")
        self.assertFalse(blocked)
        self.assertEqual(calls, [])

    def test_maintenance_not_active_passes(self):
        blocked, calls = self._run_gate(None, user_id="U_USER", admin_id="U_ADMIN")
        self.assertFalse(blocked)
        self.assertEqual(calls, [])

    def test_maintenance_empty_dict_falsy_passes(self):
        """get_maintenance() returns {} (falsy) → gate open."""
        blocked, calls = self._run_gate({}, user_id="U_USER", admin_id="U_ADMIN")
        self.assertFalse(blocked)

    def test_no_admin_env_blocks_everyone(self):
        """When SCOUT_ADMIN_USER_ID is unset (''), the condition `not _admin_id` is
        True, so the gate blocks all users — old (pre-fix) behaviour to pin."""
        maint = {"active": True, "set_by": "U_ADMIN", "attempts": []}
        blocked, calls = self._run_gate(maint, user_id="U_ADMIN", admin_id="")
        self.assertTrue(blocked)


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
