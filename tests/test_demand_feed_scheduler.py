"""Behavioral tests for demand_feed_main.py scheduler logic.

Tests the should_run decision tree only — _run() is always mocked out.
No real scraper deps required.
"""

import json
import sys
import time
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import tempfile
import os


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dt(hour: int, month: int = 6) -> datetime:
    """Return a Chicago-tz-like datetime at the given hour (CDT = UTC-5, month 6)."""
    return datetime(2026, month, 12, hour, 0, 0,
                    tzinfo=timezone(timedelta(hours=-5)))


def _run_one_cycle(tmp_dir: Path, now_dt: datetime) -> MagicMock:
    """
    Import demand_feed_main, patch paths + clock + _run, execute ONE scheduler
    loop iteration (by letting time.sleep raise StopIteration to break out), and
    return the mock for _run so callers can assert on it.
    """
    import importlib
    # We reload demand_feed_main on every test so module-level _DATA_DIR
    # doesn't carry over between runs.
    import demand_feed_main as dm
    importlib.reload(dm)

    dm._DATA_DIR = tmp_dir
    dm._SCRAPER_STATE = tmp_dir / "scraper_state.json"
    dm._OFFERS_FILE   = tmp_dir / "offers_latest.json"

    mock_run = MagicMock()
    sleep_calls: list[float] = []

    def _fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        raise StopIteration("end of first cycle")

    with patch.object(dm, "_run", mock_run), \
         patch.object(dm, "_now_chicago", return_value=now_dt), \
         patch.object(dm, "_alert_slack"), \
         patch.object(dm, "_start_http_server"), \
         patch("time.sleep", side_effect=_fake_sleep):
        try:
            dm.main()
        except StopIteration:
            pass

    return mock_run


class TestDemandFeedScheduler(unittest.TestCase):

    def setUp(self):
        # Each test gets its own temp directory so state files don't bleed over.
        self._tmp = tempfile.mkdtemp()
        self.tmp = Path(self._tmp)
        # Ensure demand_feed_main is importable from the worktree root.
        wt_root = Path(__file__).parent.parent
        if str(wt_root) not in sys.path:
            sys.path.insert(0, str(wt_root))

    # ------------------------------------------------------------------
    # C5-1: First boot triggers a run
    # ------------------------------------------------------------------
    def test_first_boot_triggers_run(self):
        """No state file exists → _run() must be called exactly once."""
        now = _make_dt(hour=7)  # 07:00 CDT — past the 06:00 window
        mock_run = _run_one_cycle(self.tmp, now)
        mock_run.assert_called_once()

    # ------------------------------------------------------------------
    # C5-2: Same-day run is skipped
    # ------------------------------------------------------------------
    def test_same_day_run_skipped(self):
        """State records today's date → _run() must NOT be called."""
        today_str = "2026-06-12"
        state_file = self.tmp / "scraper_state.json"
        state_file.write_text(json.dumps({"last_run_date": today_str}))

        # Write offers file >= 100 bytes so offers_missing is False.
        offers_file = self.tmp / "offers_latest.json"
        offers_file.write_text(json.dumps([{"id": f"offer-{i}", "name": "Test Offer", "payout": 5.0} for i in range(5)]))

        now = _make_dt(hour=8)  # 08:00 CDT — well past 06:00
        mock_run = _run_one_cycle(self.tmp, now)
        mock_run.assert_not_called()

    # ------------------------------------------------------------------
    # C5-3: Next day at or after 06:00 triggers a run
    # ------------------------------------------------------------------
    def test_next_day_after_0600_triggers_run(self):
        """State records yesterday → today at 06:00 or later → _run() called."""
        yesterday_str = "2026-06-11"
        state_file = self.tmp / "scraper_state.json"
        state_file.write_text(json.dumps({"last_run_date": yesterday_str}))

        offers_file = self.tmp / "offers_latest.json"
        offers_file.write_text(json.dumps([{"id": f"offer-{i}", "name": "Test Offer", "payout": 5.0} for i in range(5)]))

        now = _make_dt(hour=6)  # exactly 06:00 CDT — boundary case
        mock_run = _run_one_cycle(self.tmp, now)
        mock_run.assert_called_once()


    # ------------------------------------------------------------------
    # C5-4: SCRAPER_TIMEOUT_SECS env var is respected; bad value falls back
    # ------------------------------------------------------------------
    def test_scraper_timeout_configurable(self):
        """_env_int reads SCRAPER_TIMEOUT_SECS; invalid value falls back to 1800."""
        import importlib
        import demand_feed_main as dm

        with patch.dict(os.environ, {"SCRAPER_TIMEOUT_SECS": "999"}):
            importlib.reload(dm)
            self.assertEqual(dm._SCRAPER_TIMEOUT_SECS, 999)

        with patch.dict(os.environ, {"SCRAPER_TIMEOUT_SECS": "not_a_number"}):
            importlib.reload(dm)
            self.assertEqual(dm._SCRAPER_TIMEOUT_SECS, 1800)


if __name__ == "__main__":
    unittest.main()
