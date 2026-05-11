"""
demand_feed_main.py — ms-demand-feed Render service entrypoint

Runs offer_scraper.run_headless() once daily at 06:00 CT, with an immediate
first-boot run when no prior state exists or offers_latest.json is missing.

This is the entry point for the standalone ms-demand-feed Render worker.
Scout (ms-scout) continues running its own _run_scraper_daemon in parallel
during this PR — both write to their respective Render Disk independently.
That parallel redundancy is intentional: PR 27 removes the scraper from
Scout once ms-demand-feed is confirmed stable.

State: data/scraper_state.json  (same key as Scout's daemon — no conflict,
       different disk volumes on Render)
"""

import json
import logging
import os
import pathlib
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("demand_feed")

_DATA_DIR = pathlib.Path(__file__).parent / "data"
_DATA_DIR.mkdir(exist_ok=True)

_SCRAPER_STATE = _DATA_DIR / "scraper_state.json"
_OFFERS_FILE   = _DATA_DIR / "offers_latest.json"

# 06:00 CT in UTC offset hours (CST = UTC-6, CDT = UTC-5).
# zoneinfo handles DST automatically; fall back to a fixed UTC-6 offset
# if the system tzdata is absent (e.g. minimal Docker images).
try:
    from zoneinfo import ZoneInfo
    _CHICAGO_TZ: ZoneInfo | None = ZoneInfo("America/Chicago")
except Exception:
    _CHICAGO_TZ = None  # type: ignore[assignment]

_RUN_HOUR_CT = 6  # 06:00 CT


def _now_chicago() -> datetime:
    if _CHICAGO_TZ is not None:
        return datetime.now(_CHICAGO_TZ)
    # Fallback: treat UTC-6 as Chicago (ignores DST, acceptable for a 1h window)
    return datetime.now(timezone(timedelta(hours=-6)))


def _load_state() -> dict:
    try:
        return json.loads(_SCRAPER_STATE.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    tmp = _SCRAPER_STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(_SCRAPER_STATE)


def _run() -> None:
    from offer_scraper import run_headless
    run_headless()


def main() -> None:
    log.info("[demand-feed] starting")

    while True:
        try:
            now = _now_chicago()
            today_str = now.strftime("%Y-%m-%d")
            state = _load_state()

            offers_missing = not _OFFERS_FILE.exists() or _OFFERS_FILE.stat().st_size < 100
            is_first_boot  = not _SCRAPER_STATE.exists() or not state

            should_run = (
                is_first_boot
                or offers_missing
                or (state.get("last_run_date") != today_str and now.hour >= _RUN_HOUR_CT)
            )

            if should_run:
                if is_first_boot:
                    reason = "first boot"
                elif offers_missing:
                    reason = "offers file missing"
                else:
                    reason = "daily run"
                log.info(f"[demand-feed] running offer fetch ({reason})")
                try:
                    _run()
                    state["last_run_date"] = today_str
                    _save_state(state)
                    log.info("[demand-feed] done — offers_latest.json updated")
                except Exception as e:
                    log.error(f"[demand-feed] scraper failed: {e}", exc_info=True)
                    # Don't update last_run_date — retry next cycle
                    time.sleep(3600)
                    continue

            # Sleep until next 06:00 CT
            target = now.replace(hour=_RUN_HOUR_CT, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            sleep_secs = (target - now).total_seconds()
            log.info(f"[demand-feed] sleeping {sleep_secs / 3600:.1f}h until {target.strftime('%Y-%m-%d %H:%M %Z')}")
            time.sleep(sleep_secs)

        except Exception as e:
            log.error(f"[demand-feed] cycle error: {e}", exc_info=True)
            time.sleep(3600)


if __name__ == "__main__":
    main()
