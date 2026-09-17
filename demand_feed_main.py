from __future__ import annotations

"""
demand_feed_main.py — ms-demand-feed Render service entrypoint

Runs scout.offers.scraper.run_headless() once daily at 06:00 CT, with an
immediate first-boot run when no prior state exists or offers_latest.json
is missing. (Scraper moved from offer_scraper.py — see that file for the
backward-compat shim kept for rollback safety.)

This is the entry point for the standalone ms-demand-feed Render worker.
Scout (ms-scout) continues running its own _run_scraper_daemon in parallel
during this PR — both write to their respective Render Disk independently.
That parallel redundancy is intentional: PR 27 removes the scraper from
Scout once ms-demand-feed is confirmed stable.

State: data/scraper_state.json  (same key as Scout's daemon — no conflict,
       different disk volumes on Render)
"""

import http.server
import json
import logging
import multiprocessing
import os
import pathlib
import socketserver
import threading
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional

_PROCESS_START_TS = time.time()

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

# Register the canonical geo normalizer with scout_core.contracts so any
# NormalizedOffer.normalize_geo(...) call resolves to the same implementation
# offer_scraper uses. Producers (this module + scout_agent) own this wiring;
# scout_core stays unaware of offer_scraper to keep the contracts layer
# import-cheap.
from scout_core.contracts import set_geo_normalizer as _set_geo_normalizer
from scout.offers.scraper import normalize_geo as _normalize_geo
_set_geo_normalizer(_normalize_geo)

import alert_registry
from scout_bot import _env_int as _shared_env_int

# Monitor daemons (revenue-tracker, projection-autocheck, the 5-signal shadow
# monitor factory, cap-monitor) live in scout/monitoring/daemons.py — extracted
# out of this file, which had accreted them alongside the scraper it's named
# for. Each daemon function does its own local `from demand_feed_main import
# _FEED_CFG` at call time (not import time), so this import is NOT circular:
# by the time any of these functions actually runs (as a background thread,
# started from main() below), this module has already finished loading.
from scout.monitoring.daemons import (
    _format_projection_autocheck_fire,
    _format_projection_autocheck_eod,
    _projection_autocheck_daemon,
    _revenue_worsened_enough,
    _revenue_tracker_daemon,
    _is_shadow_tick,
    _run_shadow_monitor,
    _SHADOW_MONITOR_CONFIG,
    _make_shadow_daemon,
    _velocity_down_monitor_daemon,
    _ghost_monitor_daemon,
    _fill_monitor_daemon,
    _cvr_anomaly_monitor_daemon,
    _expiration_monitor_daemon,
    _cap_monitor_daemon,
)

_SCRAPER_STATE = _DATA_DIR / "scraper_state.json"
_OFFERS_FILE   = _DATA_DIR / "offers_latest.json"

# 06:00 CT in UTC offset hours (CST = UTC-6, CDT = UTC-5).
# zoneinfo handles DST automatically; fall back to a month-based approximation
# if tzdata is absent (e.g. minimal Docker images on Render).
try:
    from zoneinfo import ZoneInfo
    _CHICAGO_TZ: ZoneInfo | None = ZoneInfo("America/Chicago")
except Exception:
    _CHICAGO_TZ = None  # type: ignore[assignment]

_RUN_HOUR_CT = 6  # 06:00 CT

def _env_int(name: str, default: int) -> int:
    return _shared_env_int(name, default, tag="demand-feed")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        log.warning("[demand-feed] %s is not a valid float; using default %g", name, default)
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes")


def _env_positive_int(name: str, default: int) -> int:
    value = _env_int(name, default)
    if value <= 0:
        log.warning("[demand-feed] %s must be positive; using default %d", name, default)
        return default
    return value


_DEMAND_FEED_HQ_CHANNEL = "C0AQEECF800"  # #bot-qa — shared fallback for non-production routing

# All env vars read once at module init — no scattered os.getenv() calls beyond this block.
@dataclass(frozen=True)
class _FeedConfig:
    slack_bot_token: str = ""
    slack_alert_channel: str = "#scout-offers"
    scout_qa_channel: str = "#sidd-qa"
    demand_feed_port: int = 8080
    scout_env: str = "development"
    revenue_ops_channel: str = "C0AQEECF800"
    revenue_tracker_enabled: bool = False
    revenue_tracker_check_hour_ct: int = 10
    scout_monitor_channel: str = "#scout-offers"
    scout_shadow_channel: str = "#scout-qa"
    scout_hourly_shadow_enabled: bool = False
    projection_autocheck_enabled: bool = False
    projection_autocheck_window_start_ct: int = 10
    projection_autocheck_window_end_ct: int = 17
    projection_autocheck_eod_hour_ct: int = 17
    projection_autocheck_eod_minute_ct: int = 30
    projection_autocheck_apples_hour_ct: int = 15
    projection_autocheck_apples_tol_usd: float = 500.0
    projection_autocheck_max_errors: int = 2
    scraper_timeout_secs: int = 1800

    @classmethod
    def from_env(cls) -> "_FeedConfig":
        _hq = _DEMAND_FEED_HQ_CHANNEL
        return cls(
            slack_bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
            slack_alert_channel=os.getenv("SLACK_ALERT_CHANNEL", "#scout-offers"),
            scout_qa_channel=os.getenv("SCOUT_QA_CHANNEL", "#sidd-qa"),
            demand_feed_port=_env_int("DEMAND_FEED_PORT", 8080),
            scout_env=os.getenv("SCOUT_ENV", "development"),
            revenue_ops_channel=os.getenv("REVENUE_OPS_CHANNEL", _hq),
            revenue_tracker_enabled=os.getenv("REVENUE_TRACKER_ENABLED", "false").strip().lower() in ("1", "true", "yes"),
            revenue_tracker_check_hour_ct=_env_int("REVENUE_TRACKER_CHECK_HOUR_CT", 10),
            scout_monitor_channel=os.getenv("SCOUT_MONITOR_CHANNEL", "#scout-offers"),
            scout_shadow_channel=os.getenv("SCOUT_SHADOW_CHANNEL", "#scout-qa"),
            scout_hourly_shadow_enabled=os.getenv("SCOUT_HOURLY_SHADOW_ENABLED", "false").strip().lower() in ("1", "true", "yes"),
            projection_autocheck_enabled=os.getenv("PROJECTION_AUTOCHECK_ENABLED", "false").strip().lower() in ("1", "true", "yes"),
            projection_autocheck_window_start_ct=_env_int("PROJECTION_AUTOCHECK_WINDOW_START_CT", 10),
            projection_autocheck_window_end_ct=_env_int("PROJECTION_AUTOCHECK_WINDOW_END_CT", 17),
            projection_autocheck_eod_hour_ct=_env_int("PROJECTION_AUTOCHECK_EOD_HOUR_CT", 17),
            projection_autocheck_eod_minute_ct=_env_int("PROJECTION_AUTOCHECK_EOD_MINUTE_CT", 30),
            projection_autocheck_apples_hour_ct=_env_int("PROJECTION_AUTOCHECK_APPLES_HOUR_CT", 15),
            projection_autocheck_apples_tol_usd=_env_float("PROJECTION_AUTOCHECK_APPLES_TOL_USD", 500.0),
            projection_autocheck_max_errors=_env_int("PROJECTION_AUTOCHECK_MAX_ERRORS", 2),
            scraper_timeout_secs=_env_positive_int("SCRAPER_TIMEOUT_SECS", 1800),
        )


_FEED_CFG = _FeedConfig.from_env()


def _now_chicago() -> datetime:
    if _CHICAGO_TZ is not None:
        return datetime.now(_CHICAGO_TZ)
    # Fallback: approximate DST — CDT (UTC-5) runs roughly Mar–Oct
    _utc_now = datetime.now(timezone.utc)
    _offset = -5 if 3 <= _utc_now.month <= 10 else -6
    return _utc_now.astimezone(timezone(timedelta(hours=_offset)))


def _load_state() -> dict:
    try:
        return json.loads(_SCRAPER_STATE.read_text())
    except Exception as e:
        log.warning(f"Could not load scraper state from {_SCRAPER_STATE}: {e}")
        return {}


def _save_state(state: dict) -> None:
    tmp = _SCRAPER_STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(_SCRAPER_STATE)


def _alert_slack(msg: str) -> None:
    token = _FEED_CFG.slack_bot_token
    channel = _FEED_CFG.slack_alert_channel
    if not token:
        return
    try:
        import requests as _req
        r = _req.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": f"*ms-demand-feed* {msg}"},
            timeout=10,
        )
        if not r.ok or not r.json().get("ok", False):
            log.warning("[demand-feed] Slack alert failed: %s", r.text[:200])
    except Exception:
        pass  # alerting must never crash the scheduler loop


def _scraper_worker(q: multiprocessing.Queue) -> None:
    from scout.offers.scraper import run_headless
    try:
        run_headless(post_digest=False)
    except Exception as e:
        q.put(e)


def _run() -> None:
    q: multiprocessing.Queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=_scraper_worker, args=(q,), daemon=True)
    p.start()
    p.join(timeout=_FEED_CFG.scraper_timeout_secs)
    if p.is_alive():
        p.terminate()
        p.join(timeout=5)
        if p.is_alive():
            p.kill()
        raise TimeoutError(f"run_headless() hung after {_FEED_CFG.scraper_timeout_secs}s")
    if not q.empty():
        raise q.get()


def _write_json(handler: http.server.BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class _OffersHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/offers":
            try:
                data = _OFFERS_FILE.read_bytes()
            except FileNotFoundError:
                self.send_error(503, "offers not yet available")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(data)
            return

        if self.path == "/health":
            _write_json(self, 200, {
                "status": "ok",
                "uptime_secs": int(time.time() - _PROCESS_START_TS),
            })
            return

        if self.path == "/last-run":
            state = _load_state()
            offers_mtime = None
            offers_size = None
            try:
                st = _OFFERS_FILE.stat()
                offers_mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
                offers_size = st.st_size
            except OSError:
                # FileNotFoundError, PermissionError, etc. — keep endpoint resilient
                pass
            _write_json(self, 200, {
                "last_run_date":      state.get("last_run_date"),
                "last_success_ts":    state.get("last_success_ts"),
                "last_failure_ts":    state.get("last_failure_ts"),
                "last_failure_reason": state.get("last_failure_reason"),
                "offers_mtime":       offers_mtime,
                "offers_size":        offers_size,
            })
            return

        if self.path.startswith("/digest/blocks"):
            # Query params: ?force=1 to bypass event gate
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            is_force = qs.get("force", ["0"])[0] == "1"
            try:
                import scout_digest
                payload = scout_digest.build_digest_payload(is_force=is_force)
            except Exception as exc:
                log.error("[demand-feed] /digest/blocks error: %s", exc, exc_info=True)
                _write_json(self, 500, {"error": "InternalServerError"})
                return
            if payload is None:
                self.send_response(204)
                self.end_headers()
                return
            _write_json(self, 200, payload)
            return

        self.send_error(404)

    def do_POST(self):
        self.send_error(404)

    def log_message(self, *args):  # suppress request logs
        pass


def _start_http_server() -> None:
    port = _FEED_CFG.demand_feed_port
    server = socketserver.TCPServer(("", port), _OffersHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info(f"[demand-feed] HTTP server started on :{port}")


def main() -> None:
    _start_http_server()
    threading.Thread(
        target=_revenue_tracker_daemon,
        daemon=True,
        name="revenue-tracker",
    ).start()
    log.info("[demand-feed] revenue-tracker daemon started (kill switch: REVENUE_TRACKER_ENABLED — requires redeploy to change)")
    threading.Thread(
        target=_projection_autocheck_daemon,
        daemon=True,
        name="projection-autocheck",
    ).start()
    log.info(
        "[demand-feed] projection-autocheck daemon started "
        "(kill switch: PROJECTION_AUTOCHECK_ENABLED — requires redeploy to change)"
    )

    for _monitor_fn, _monitor_name in [
        (_cap_monitor_daemon, "cap-monitor"),
        (_velocity_down_monitor_daemon, "velocity-down-monitor"),
        (_ghost_monitor_daemon, "ghost-monitor"),
        (_fill_monitor_daemon, "fill-monitor"),
        (_cvr_anomaly_monitor_daemon, "cvr-anomaly-monitor"),
        (_expiration_monitor_daemon, "expiration-monitor"),
    ]:
        threading.Thread(target=_monitor_fn, daemon=True, name=_monitor_name).start()
    log.info("[demand-feed] hourly-shadow monitors started (SCOUT_HOURLY_SHADOW_ENABLED gates shadow ticks only — prod-window firing always active)")
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
                _t0 = time.monotonic()
                try:
                    from scout_core.job_runs import record_job_run
                    _run()
                    _dur = int((time.monotonic() - _t0) * 1000)
                    state["last_run_date"]    = today_str
                    state["last_success_ts"]  = datetime.now(timezone.utc).isoformat()
                    state["last_failure_ts"]    = None
                    state["last_failure_reason"] = None
                    _save_state(state)
                    record_job_run("offer_scraper", status="success", duration_ms=_dur)
                    log.info("[demand-feed] done — offers_latest.json updated")
                    _alert_slack(":white_check_mark: daily scrape complete — offers_latest.json updated")
                except Exception as e:
                    _dur = int((time.monotonic() - _t0) * 1000)
                    try:
                        from scout_core.job_runs import record_job_run
                        record_job_run("offer_scraper", status="error",
                                       error=type(e).__name__, duration_ms=_dur)
                    except Exception:
                        pass
                    log.error(f"[demand-feed] scraper failed: {e}", exc_info=True)
                    _alert_slack(f":rotating_light: scrape failed — retrying in 1h: {e}")
                    # Don't update last_run_date — retry next cycle
                    state["last_failure_ts"]     = datetime.now(timezone.utc).isoformat()
                    # Only expose exception type — repr(e) can leak tokens, URLs, paths
                    # through the unauthenticated /last-run endpoint.
                    state["last_failure_reason"] = type(e).__name__
                    _save_state(state)
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
