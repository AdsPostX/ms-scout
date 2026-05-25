"""Tests for Phase 3 — format function action footers and goal+pace framing.

Covers:
  T1: _format_cap_alert includes action footer "→ Contact advertiser"
  T2: _format_velocity_down_alert includes action footer "→ Check publisher fill rate"
  T3: _format_ghost_alert includes action footer "→ Check tracking pixel"
  T4: _format_fill_alert includes action footer "→ Review floor price"
  T5: goal+pace block shows 🟢 when revenue_mtd_cents is on or above pace
  T6: goal+pace block is omitted when monthly_revenue_target is 0
"""
from __future__ import annotations

import importlib
import sys
import types
import unittest
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

_WT_ROOT = Path(__file__).parent.parent
if str(_WT_ROOT) not in sys.path:
    sys.path.insert(0, str(_WT_ROOT))


# ---------------------------------------------------------------------------
# Stub heavy deps not present in the test venv
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

# clickhouse_connect
try:
    importlib.import_module("clickhouse_connect")
except ImportError:
    _stub("clickhouse_connect")

# dotenv
try:
    importlib.import_module("dotenv")
except ImportError:
    _denv = _stub("dotenv")
    _denv.load_dotenv = lambda *a, **kw: None

# requests
try:
    importlib.import_module("requests")
except ImportError:
    _stub("requests")

# slack_sdk
for _sl in (
    "slack_sdk",
    "slack_sdk.socket_mode",
    "slack_sdk.socket_mode.request",
    "slack_sdk.socket_mode.response",
    "slack_sdk.http_retry",
    "slack_sdk.http_retry.builtin_handlers",
    "slack_sdk.web",
):
    try:
        importlib.import_module(_sl)
    except ImportError:
        _stub(_sl, SocketModeClient=MagicMock, SocketModeRequest=MagicMock,
              SocketModeResponse=MagicMock, RateLimitErrorRetryHandler=MagicMock,
              WebClient=MagicMock)

# queries — stub minimal so scout_bot can import
try:
    importlib.import_module("queries")
except ImportError:
    _stub("queries")

# scout_types
try:
    importlib.import_module("scout_types")
except ImportError:
    _stub("scout_types")

# scout_agent
try:
    importlib.import_module("scout_agent")
except ImportError:
    _sa = _stub("scout_agent")
    _sa.ask = MagicMock(return_value="")

# scout_notion
try:
    importlib.import_module("scout_notion")
except ImportError:
    _sn = _stub("scout_notion")
    _sn.get_beverly_brief = MagicMock(return_value="")
    _sn.save_deal_memory = MagicMock()
    _sn.load_deal_memory = MagicMock(return_value={})

# scout_state
try:
    importlib.import_module("scout_state")
except ImportError:
    _ss = _stub("scout_state")
    _ss.get_pulse_state = MagicMock(return_value={})
    _ss.update_pulse_state = MagicMock()
    _ss.read_daily_firing_log = MagicMock(return_value={})
    _ss.write_daily_firing_log = MagicMock()
    _ss.set_force_monitor_fn = MagicMock()
    _ss._set_force_monitor_fn = MagicMock()

# scout_ch
try:
    importlib.import_module("scout_ch")
except ImportError:
    _sch = _stub("scout_ch")
    _sch._query_cvr_anomaly = MagicMock(return_value=[])
    _sch._query_expiring_campaigns = MagicMock(return_value=[])

# scout_handlers
try:
    importlib.import_module("scout_handlers")
except ImportError:
    _sh = _stub("scout_handlers")
    _sh.handle_scout_ask = MagicMock()
    _sh.handle_force_run = MagicMock()
    _sh.handle_config = MagicMock()
    _sh.handle_thumbs_down = MagicMock()

# pytz
try:
    importlib.import_module("pytz")
except ImportError:
    _stub("pytz")

# zoneinfo / backport
try:
    importlib.import_module("zoneinfo")
except ImportError:
    _zi = _stub("zoneinfo")
    import datetime as _dt
    _zi.ZoneInfo = lambda tz: _dt.timezone.utc

# ---------------------------------------------------------------------------
# Now import the modules under test — with SCOUT_KIT_ENABLED=true
# ---------------------------------------------------------------------------
import os
os.environ.setdefault("SCOUT_KIT_ENABLED", "true")

# Force clean import of scout_ui_kit so env var is picked up
for _mod_name in ("scout_ui_kit",):
    sys.modules.pop(_mod_name, None)

import scout_ui_kit  # noqa: E402


# ---------------------------------------------------------------------------
# Helper: extract all mrkdwn text from a block list
# ---------------------------------------------------------------------------

def _blocks_text(blocks: list) -> str:
    """Concatenate all mrkdwn / plain_text values from a block list."""
    parts: list[str] = []

    def _walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ("text",) and isinstance(v, str):
                    parts.append(v)
                else:
                    _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(blocks)
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Import format functions from scout_bot via targeted patching
# ---------------------------------------------------------------------------

# We need to import scout_bot, but it has heavy runtime side-effects at
# module level (socket connections, env-var checks). We patch the heaviest
# entry points before importing so the module loads cleanly in the test venv.

def _get_format_fns():
    """Import scout_bot, extract format functions, then evict from sys.modules.

    Eviction is important: scout_bot's module-level init registers entries into
    scout_agent.TOOL_MAP (via _set_force_monitor_fn). If the module stays in
    sys.modules, later tests that patch TOOL_MAP with a reduced dict will fail
    because the keyword router resolves to a key the patch doesn't include.
    We capture the function references before eviction — they remain callable.
    """
    for _mod in ("scout_bot",):
        sys.modules.pop(_mod, None)

    with patch.dict(os.environ, {
        "SLACK_BOT_TOKEN": "xoxb-test",
        "SLACK_APP_TOKEN": "xapp-test",
        "SCOUT_KIT_ENABLED": "true",
    }):
        with patch("slack_sdk.socket_mode.SocketModeClient", MagicMock()), \
             patch("slack_sdk.web.WebClient", MagicMock()):
            import scout_bot as _sb  # noqa: PLC0415

    # Capture references before cleanup
    fns = (
        _sb._format_cap_alert,
        _sb._format_velocity_down_alert,
        _sb._format_ghost_alert,
        _sb._format_fill_alert,
    )

    # Evict so scout_bot's TOOL_MAP registrations don't pollute later tests
    sys.modules.pop("scout_bot", None)

    return fns


try:
    (
        _format_cap_alert,
        _format_velocity_down_alert,
        _format_ghost_alert,
        _format_fill_alert,
    ) = _get_format_fns()
    _SCOUT_BOT_AVAILABLE = True
except ImportError as _exc:
    _SCOUT_BOT_AVAILABLE = False
    _SCOUT_BOT_IMPORT_ERR = str(_exc)
except Exception:
    raise


# ---------------------------------------------------------------------------
# T1–T4: Format function action footers
# ---------------------------------------------------------------------------

@unittest.skipUnless(_SCOUT_BOT_AVAILABLE, "scout_bot import failed")
class TestFormatFnFooters(unittest.TestCase):

    def test_cap_alert_action_footer(self):
        """T1: _format_cap_alert includes the advertiser-contact action line."""
        rows = [{
            "adv_name": "TestCo",
            "cap_pct": 91.0,
            "revenue_mtd": 9100,
            "monthly_cap": 10000,
            "days_to_cap": 3,
            "days_remaining": 8,
        }]
        fallback, blocks = _format_cap_alert(rows)
        text = _blocks_text(blocks)
        self.assertIn("→ Contact advertiser", text,
                      f"Action footer missing from cap alert blocks.\nGot:\n{text}")

    def test_velocity_down_alert_action_footer(self):
        """T2: _format_velocity_down_alert includes the fill-rate check action line."""
        rows = [{
            "direction": "down",
            "publisher_name": "PubA",
            "revenue_30d": 40000,
            "revenue_7d_ann": 25000,
            "pct_delta": -33.0,
            "hypothesis": "test hypothesis",
        }]
        fallback, blocks = _format_velocity_down_alert(rows)
        text = _blocks_text(blocks)
        self.assertIn("→ Check publisher fill rate", text,
                      f"Action footer missing from velocity alert blocks.\nGot:\n{text}")

    def test_ghost_alert_action_footer(self):
        """T3: _format_ghost_alert includes the tracking-pixel check action line."""
        rows = [{
            "adv_name": "TestCo",
            "impressions_7d": 10000,
            "impressions_2d": 3200,
        }]
        fallback, blocks = _format_ghost_alert(rows)
        text = _blocks_text(blocks)
        self.assertIn("→ Check tracking pixel", text,
                      f"Action footer missing from ghost alert blocks.\nGot:\n{text}")

    def test_fill_alert_action_footer(self):
        """T4: _format_fill_alert includes the floor-price review action line."""
        rows = [{
            "publisher_name": "PubY",
            "fill_rate_pct": 9.0,
            "missed_sessions": 1800,
            "sessions_7d": 3200,
        }]
        fallback, blocks = _format_fill_alert(rows)
        text = _blocks_text(blocks)
        self.assertIn("→ Review floor price", text,
                      f"Action footer missing from fill alert blocks.\nGot:\n{text}")


# ---------------------------------------------------------------------------
# T5–T6: Goal + pace framing in App Home scoreboard
# ---------------------------------------------------------------------------

@dataclass
class _MockRollup:
    """Minimal ScoreboardRollup-compatible object for unit testing."""
    revenue_today_cents: int = 50_000_00     # $50K today
    revenue_yesterday_same_time_cents: int = 45_000_00
    revenue_7d_avg_cents: int = 48_000_00
    conversions_today: int = 100
    conversions_yesterday_same_time: int = 90
    conversions_7d_avg: int = 95
    revenue_7d_series: list = field(default_factory=list)
    revenue_eod_projection_cents: int = 0
    revenue_mtd_cents: int = 0
    generated_at: datetime = field(default_factory=datetime.utcnow)


class TestGoalPaceFraming(unittest.TestCase):

    def _run_scoreboard(self, rollup, thresholds_override: dict) -> str:
        """Call _build_home_scoreboard_blocks with a patched SCOUT_THRESHOLDS."""
        with patch.object(scout_ui_kit, "SCOUT_THRESHOLDS", thresholds_override):
            blocks = scout_ui_kit._build_home_scoreboard_blocks(rollup, alerts=[])
        return _blocks_text(blocks)

    def test_on_pace_shows_green_icon(self):
        """T5: When MTD revenue is at or above daily run-rate needed, 🟢 appears."""
        # Target: $1M/month. Use $1.1M MTD — above target regardless of day-of-month.
        # ($600K was flaky: behind pace after day ~18 of a 31-day month.)
        rollup = _MockRollup(revenue_mtd_cents=1_100_000_00)  # $1.1M MTD — always on pace
        thresholds = {"monthly_revenue_target": 1_000_000}  # $1M target
        text = self._run_scoreboard(rollup, thresholds)
        self.assertIn("🟢", text,
                      f"Expected 🟢 pace icon for on-pace revenue.\nGot blocks text:\n{text}")

    def test_missing_target_omits_goal_line(self):
        """T6: When monthly_revenue_target is 0, the goal/pace line is not rendered."""
        rollup = _MockRollup(revenue_mtd_cents=500_000_00)
        thresholds = {"monthly_revenue_target": 0}
        text = self._run_scoreboard(rollup, thresholds)
        # The pace line format is "$XXX / $YYY MTD" — should not appear
        self.assertNotIn("MTD ·", text,
                         f"Goal line should be absent when target is 0.\nGot:\n{text}")

    def test_missing_target_key_omits_goal_line(self):
        """T6b: When monthly_revenue_target key is absent, goal line is not rendered."""
        rollup = _MockRollup(revenue_mtd_cents=500_000_00)
        thresholds = {}  # key absent entirely
        text = self._run_scoreboard(rollup, thresholds)
        self.assertNotIn("MTD ·", text,
                         f"Goal line should be absent when target key is missing.\nGot:\n{text}")


if __name__ == "__main__":
    unittest.main()
