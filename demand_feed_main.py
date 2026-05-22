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
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        log.warning("[demand-feed] %s is not a valid integer; using default %d", name, default)
        return default


# Scraper is killed and retried after this many seconds (default 30 min).
_SCRAPER_TIMEOUT_SECS = _env_int("SCRAPER_TIMEOUT_SECS", 1800)


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
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    tmp = _SCRAPER_STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(_SCRAPER_STATE)


def _alert_slack(msg: str) -> None:
    token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_ALERT_CHANNEL", "#scout-offers")
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
    from offer_scraper import run_headless
    try:
        run_headless(post_digest=False)
    except Exception as e:
        q.put(e)


def _run() -> None:
    q: multiprocessing.Queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=_scraper_worker, args=(q,), daemon=True)
    p.start()
    p.join(timeout=_SCRAPER_TIMEOUT_SECS)
    if p.is_alive():
        p.terminate()
        p.join(timeout=5)
        if p.is_alive():
            p.kill()
        raise TimeoutError(f"run_headless() hung after {_SCRAPER_TIMEOUT_SECS}s")
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

        self.send_error(404)

    def log_message(self, *args):  # suppress request logs
        pass


def _start_http_server() -> None:
    port = int(os.getenv("DEMAND_FEED_PORT", "8080"))
    server = socketserver.TCPServer(("", port), _OffersHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info(f"[demand-feed] HTTP server started on :{port}")


def main() -> None:
    _start_http_server()
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
                    state["last_run_date"]    = today_str
                    state["last_success_ts"]  = datetime.now(timezone.utc).isoformat()
                    state["last_failure_ts"]    = None
                    state["last_failure_reason"] = None
                    _save_state(state)
                    log.info("[demand-feed] done — offers_latest.json updated")
                    _alert_slack(":white_check_mark: daily scrape complete — offers_latest.json updated")
                except Exception as e:
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
