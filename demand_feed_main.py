from __future__ import annotations

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
import uuid
from datetime import datetime, timedelta, timezone
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
from offer_scraper import normalize_geo as _normalize_geo
_set_geo_normalizer(_normalize_geo)

import alert_registry

_SCRAPER_STATE = _DATA_DIR / "scraper_state.json"
_OFFERS_FILE   = _DATA_DIR / "offers_latest.json"
_QUEUE_FILE    = _DATA_DIR / "queue.json"

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

        if self.path == "/queue/config":
            _handle_queue_config(self)
            return

        if self.path == "/queue/pending":
            drafts = _load_queue()
            pending = [d for d in drafts.values()
                       if d.get("approval", {}).get("state") == "pending"]
            _write_json(self, 200, {"drafts": pending, "count": len(pending)})
            return

        if self.path.startswith("/queue/"):
            # GET /queue/<draft_id>
            draft_id = self.path[len("/queue/"):]
            if draft_id:
                drafts = _load_queue()
                draft = drafts.get(draft_id)
                if draft is None:
                    _write_json(self, 404, {"error": f"draft not found: {draft_id}"})
                else:
                    _write_json(self, 200, draft)
                return

        self.send_error(404)

    def do_POST(self):
        if self.path == "/queue/draft":
            _handle_queue_create_draft(self)
            return
        if self.path == "/queue/approve":
            _handle_queue_approve(self)
            return
        if self.path == "/queue/reject":
            _handle_queue_reject(self)
            return
        if self.path == "/campaigns/create":
            _handle_campaigns_create(self)
            return
        self.send_error(404)

    def log_message(self, *args):  # suppress request logs
        pass


def _format_projection_autocheck_fire(
    slot: str,
    result: dict,
    daemon_raw: Optional[float],
    delta_abs: Optional[float],
    cmp_tol: float,
) -> tuple[str, list[dict]]:
    """Compact Slack fire for one autocheck slot. Routed to SCOUT_QA_CHANNEL."""
    status = result.get("status", "error")
    if status == "ok":
        today_rev = float(result.get("today_revenue") or 0)
        proj      = float(result.get("projected_full_day") or 0)
        med       = result.get("dow_median")
        pct       = result.get("pct_of_expected")
        share     = result.get("curve_share")
        source    = result.get("curve_source") or "?"
        wd        = result.get("weekday") or "?"

        cmp_line = ""
        if daemon_raw is not None and delta_abs is not None:
            in_tol = delta_abs <= cmp_tol
            cmp_line = (
                f"\n• Apples vs daemon raw: helper=${today_rev:,.0f} "
                f"daemon=${daemon_raw:,.0f} Δ=${delta_abs:,.0f} "
                f"{'within' if in_tol else 'OUT OF'} ±${cmp_tol:,.0f} tolerance"
            )
            if not in_tol:
                cmp_line += " ⚠️"

        med_line = f" vs ${float(med):,.0f} {wd} median ({pct}%)" if med else ""
        lines = [
            f"[projection-autocheck] :bar_chart: *Projection autocheck* `{slot}`",
            f"• Today so far: ${today_rev:,.0f}",
            f"• Projected EOD: ${proj:,.0f}{med_line}",
            f"• Curve share: {share} ({source})",
        ]
        if cmp_line:
            lines.append(cmp_line.lstrip("\n"))

        if result.get("projected_low") and result.get("projected_high"):
            lines.append(f"• Range:  ${result['projected_low']:,.0f} – ${result['projected_high']:,.0f}")

        diag = result.get("diagnostic")
        if diag and diag != "on_track":
            diag_labels = {
                "efficiency":     "⚠ Efficiency signal — traffic ok, revenue soft (experimental)",
                "traffic":        "⚠ Traffic signal — volume below baseline (experimental)",
                "traffic_upside": "↑ Upside signal — traffic + revenue ahead (experimental)",
            }
            lines.append(f"• Signal: {diag_labels.get(diag, diag)}")

        text = "\n".join(lines)
    elif status in ("too_early", "insufficient_history", "unstable"):
        formatted = result.get("formatted") or f"status={status}"
        text = f"[projection-autocheck] `{slot}` {formatted}"
    else:
        err = result.get("error") or "unknown"
        text = f"[projection-autocheck] :x: `{slot}` error: {err}"

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    return text, blocks


def _format_projection_autocheck_eod(
    date_str: str, entries: list[dict],
) -> tuple[str, list[dict]]:
    """17:30 CT EOD rollup — one summary message so muting the channel is not
    the path of least resistance. Trust visibility = one read."""
    if not entries:
        text = (
            f"[projection-autocheck] :bar_chart: *Projection autocheck EOD* {date_str}\n"
            "• No fires recorded today (monitor disabled, paused, or restarted late)."
        )
        return text, [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]

    ok      = [e for e in entries if e["status"] == "ok"]
    errors  = [e for e in entries if e["status"] == "error"]
    last_ok = ok[-1] if ok else None

    lines = [f"[projection-autocheck] :bar_chart: *Projection autocheck EOD* {date_str}"]
    lines.append(f"• Fires: {len(entries)} (ok={len(ok)}, errors={len(errors)})")
    if last_ok:
        proj = last_ok.get("projected_full_day")
        med  = last_ok.get("dow_median")
        pct  = last_ok.get("pct_of_expected")
        proj_s = f"${float(proj):,.0f}" if proj else "—"
        med_s  = f"${float(med):,.0f}" if med else "—"
        pct_s  = f"{pct}%" if pct is not None else "—"
        lines.append(f"• Last projection: {proj_s} vs {med_s} median ({pct_s})")

    apples = [e for e in entries if e.get("delta_abs") is not None]
    if apples:
        deltas = [float(e["delta_abs"]) for e in apples]
        lines.append(
            f"• Apples-vs-daemon Δ: min=${min(deltas):,.0f} max=${max(deltas):,.0f} "
            f"(n={len(apples)})"
        )

    text = "\n".join(lines)
    return text, [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]


def _projection_autocheck_daemon() -> None:
    """Demand-feed port of scout_bot._projection_autocheck_monitor.

    Hourly projection anomaly check within a configurable CT window.
    Kill switch: PROJECTION_AUTOCHECK_ENABLED env var (default false — off).
    Wired to job_runs telemetry.
    """
    while True:  # outer restart wrapper
        try:
            import time as _time
            import pytz
            from datetime import datetime as _dt
            from scout_ch import _get_ch_client
            from scout_ch import project_today_revenue, _query_intraday_revenue_total
            from scout_state import (
                _load_projection_autocheck_slot,
                _save_projection_autocheck_slot,
                _load_eod_posted_date,
                _save_eod_posted_date,
                _load_projection_autocheck_fires,
                _append_projection_autocheck_fire,
                _evict_stale_projection_autocheck_fires,
            )
            from scout_core.job_runs import record_job_run
            from slack_sdk.web import WebClient

            CT_TZ   = pytz.timezone("America/Chicago")
            channel = os.getenv("SCOUT_QA_CHANNEL", "#sidd-qa")
            tag     = "[projection-autocheck]"
            _bot_token = os.getenv("SLACK_BOT_TOKEN")
            if not _bot_token:
                log.error("[projection-autocheck] SLACK_BOT_TOKEN not set — retrying in 60s.")
                _time.sleep(60)
                continue
            web = WebClient(token=_bot_token)

            # Seed in-memory slot from persisted state so a mid-hour restart
            # does not re-fire the current slot.
            last_slot: Optional[str] = _load_projection_autocheck_slot()
            consecutive_errors    = 0
            paused_for_date: Optional[str] = None
            # Seed EOD-posted marker from disk so a restart after 17:30 CT
            # does not re-post the same day's EOD summary.
            eod_posted_for_date: Optional[str] = _load_eod_posted_date()

            while True:  # inner poll loop
                try:
                    # Kill switch: default false — dormant until staging validated.
                    if os.getenv("PROJECTION_AUTOCHECK_ENABLED", "false").strip().lower() != "true":
                        _time.sleep(300)
                        continue

                    win_start  = int(os.getenv("PROJECTION_AUTOCHECK_WINDOW_START_CT", "10"))
                    win_end    = int(os.getenv("PROJECTION_AUTOCHECK_WINDOW_END_CT", "17"))
                    eod_hour   = int(os.getenv("PROJECTION_AUTOCHECK_EOD_HOUR_CT", "17"))
                    eod_minute = int(os.getenv("PROJECTION_AUTOCHECK_EOD_MINUTE_CT", "30"))
                    cmp_hour   = int(os.getenv("PROJECTION_AUTOCHECK_APPLES_HOUR_CT", "15"))
                    cmp_tol    = float(os.getenv("PROJECTION_AUTOCHECK_APPLES_TOL_USD", "500"))
                    max_errs   = int(os.getenv("PROJECTION_AUTOCHECK_MAX_ERRORS", "2"))

                    now_ct    = _dt.now(CT_TZ)
                    today_str = now_ct.date().isoformat()
                    slot      = f"{today_str}T{now_ct.hour:02d}"

                    # Reset kill-switch + per-day log on date rollover.
                    if paused_for_date and paused_for_date != today_str:
                        paused_for_date = None
                        consecutive_errors = 0
                    _evict_stale_projection_autocheck_fires(today_str)

                    # EOD rollup — once per day, after eod_hour:eod_minute CT.
                    if (
                        eod_posted_for_date != today_str
                        and (
                            now_ct.hour > eod_hour
                            or (now_ct.hour == eod_hour and now_ct.minute >= eod_minute)
                        )
                    ):
                        try:
                            entries = _load_projection_autocheck_fires(today_str)
                            text, blocks = _format_projection_autocheck_eod(
                                today_str, entries
                            )
                            web.chat_postMessage(channel=channel, text=text, blocks=blocks)
                            eod_posted_for_date = today_str
                            try:
                                _save_eod_posted_date(today_str)
                            except Exception as _e:
                                log.warning(f"{tag} persist eod_posted_date failed: {_e}")
                            log.info(f"{tag} posted EOD rollup for {today_str} ({len(entries)} fires).")
                        except Exception as e:
                            log.warning(f"{tag} EOD rollup post failed: {e}")
                            eod_posted_for_date = today_str  # don't retry-spam
                            try:
                                _save_eod_posted_date(today_str)
                            except Exception as _e:
                                log.warning(f"{tag} persist eod_posted_date failed: {_e}")

                    # Hourly fire gate.
                    in_window = win_start <= now_ct.hour <= win_end
                    if not in_window:
                        _time.sleep(300)
                        continue
                    if paused_for_date == today_str:
                        _time.sleep(300)
                        continue
                    if last_slot == slot:
                        _time.sleep(300)
                        continue
                    # Top-of-hour only (first 10 min of the hour).
                    if now_ct.minute >= 10:
                        _time.sleep(300)
                        continue

                    # Fire.
                    _t0 = _time.monotonic()
                    try:
                        ch = _get_ch_client()
                        result = project_today_revenue(ch)
                        status = result.get("status", "error")
                    except Exception as e:
                        log.warning(f"{tag} projection query failed: {e}")
                        result = {"status": "error", "error": str(e)}
                        status = "error"

                    # Apples-to-apples comparison at the configured hour.
                    daemon_raw = None
                    delta_abs = None
                    if status == "ok" and now_ct.hour == cmp_hour:
                        try:
                            daemon_dict = _query_intraday_revenue_total(_get_ch_client())
                            if daemon_dict and daemon_dict.get("today_revenue") is not None:
                                daemon_raw = float(daemon_dict["today_revenue"])
                                helper_raw = float(result.get("today_revenue") or 0)
                                delta_abs  = abs(daemon_raw - helper_raw)
                        except Exception as e:
                            log.warning(f"{tag} daemon-compare query failed: {e}")

                    fallback, blocks = _format_projection_autocheck_fire(
                        slot, result, daemon_raw, delta_abs, cmp_tol,
                    )

                    try:
                        web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)
                        last_slot = slot
                        _save_projection_autocheck_slot(slot)
                        try:
                            _append_projection_autocheck_fire(today_str, {
                                "slot": slot,
                                "status": status,
                                "today_revenue":      result.get("today_revenue"),
                                "projected_full_day": result.get("projected_full_day"),
                                "dow_median":         result.get("dow_median"),
                                "pct_of_expected":    result.get("pct_of_expected"),
                                "daemon_raw":         daemon_raw,
                                "delta_abs":          delta_abs,
                                "projected_low":      result.get("projected_low"),
                                "projected_high":     result.get("projected_high"),
                                "projection_n":       result.get("projection_n"),
                                "diagnostic":         result.get("diagnostic"),
                            })
                        except Exception as _e:
                            log.warning(f"{tag} persist fires_log failed: {_e}")
                        log.info(f"{tag} posted slot={slot} status={status} → {channel}.")
                        duration_ms = int((_time.monotonic() - _t0) * 1000)
                        record_job_run("projection_autocheck", status="success", duration_ms=duration_ms)
                    except Exception as e:
                        log.warning(f"{tag} slack post failed: {e}")
                        status = "error"

                    # Kill-switch accounting.
                    if status == "error":
                        duration_ms = int((_time.monotonic() - _t0) * 1000)
                        record_job_run(
                            "projection_autocheck",
                            status="error",
                            duration_ms=duration_ms,
                            error=str(result.get("error", ""))[:400],
                        )
                        consecutive_errors += 1
                        if (
                            consecutive_errors >= max_errs
                            and paused_for_date != today_str
                        ):
                            paused_for_date = today_str
                            try:
                                web.chat_postMessage(
                                    channel=channel,
                                    text=(
                                        f"{tag} ≥{max_errs} consecutive errors — "
                                        f"pausing for the rest of {today_str}. "
                                        "Resumes at midnight CT."
                                    ),
                                )
                            except Exception as _e:
                                log.error(
                                    f"{tag} kill-switch notification failed — "
                                    f"daemon paused but Slack was not told: {_e}",
                                    exc_info=True,
                                )
                            log.warning(
                                f"{tag} kill-switch tripped ({consecutive_errors} errors) — "
                                f"paused for {today_str}."
                            )
                    else:
                        consecutive_errors = 0

                except Exception as e:
                    log.warning(f"{tag} unexpected error: {e}")
                finally:
                    _time.sleep(300)

        except Exception as e:
            log.error(f"[projection-autocheck] fatal crash — restarting in 30s: {e}", exc_info=True)
            import time
            time.sleep(30)


def _start_http_server() -> None:
    port = int(os.getenv("DEMAND_FEED_PORT", "8080"))
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
    log.info("[demand-feed] revenue-tracker daemon started (kill switch: REVENUE_TRACKER_ENABLED)")
    threading.Thread(
        target=_projection_autocheck_daemon,
        daemon=True,
        name="projection-autocheck",
    ).start()
    log.info(
        "[demand-feed] projection-autocheck daemon started "
        "(kill switch: PROJECTION_AUTOCHECK_ENABLED)"
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
    log.info("[demand-feed] hourly-shadow monitors started (kill switch: SCOUT_HOURLY_SHADOW_ENABLED)")
    _wh  = os.getenv("CAMPAIGN_CREATE_WEBHOOK_URL", "").strip()
    _dry = os.getenv("CAMPAIGN_CREATE_DRY_RUN", "true").strip().lower() in ("1", "true", "yes")
    _cc_mode = "live" if (_wh and not _dry) else "dry_run"
    log.info(
        "[demand-feed] campaign-creation mode=%s webhook_url_set=%s "
        "(MS_PLATFORM_TODO: set CAMPAIGN_CREATE_WEBHOOK_URL + CAMPAIGN_CREATE_DRY_RUN=false to go live)",
        _cc_mode, bool(_wh),
    )
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


def _revenue_tracker_daemon() -> None:
    """Demand-feed port of scout_bot._revenue_tracker.

    Hourly mode (revenue_tracker_hourly_enabled=true, default):
      Posts during business hours (9am–5pm CT) whenever revenue drops below
      threshold. Smart deduplication: re-fires only when pct_of_expected drops
      by >= revenue_tracker_refire_drop_pct since the last posted alert.
      Uses YYYY-MM-DDTHH slot for per-hour idempotency.

    Daily fallback (revenue_tracker_hourly_enabled=false):
      Legacy once-daily behaviour at revenue_tracker_check_hour_ct (now 10am CT).

    Kill switch: REVENUE_TRACKER_ENABLED env var (default false — off).
    Wired to job_runs telemetry via scout_core.job_runs.record_job_run.

    Posts to REVENUE_OPS_CHANNEL (production) or #bot-qa (non-production).

    Outer restart wrapper: any unhandled crash logs the traceback and restarts
    after 30s so the thread stays alive indefinitely without a Render redeploy.
    """
    _HQ_CHANNEL = "C0AQEECF800"  # #bot-qa fallback (matches scout_bot._SCOUT_HQ_CHANNEL)

    def _get_channel() -> str:
        scout_env = os.getenv("SCOUT_ENV", "development")
        if scout_env != "production":
            return _HQ_CHANNEL
        return os.getenv("REVENUE_OPS_CHANNEL", _HQ_CHANNEL)

    while True:  # outer restart wrapper — self-heals any unhandled crash
        try:
            from scout_bot import _format_revenue_alert
            import time as _time
            import pytz
            from datetime import datetime as _dt
            from slack_sdk.web import WebClient
            from scout_ch import _query_intraday_revenue_total, _query_intraday_revenue_by_publisher, _get_ch_client
            from scout_state import (
                _load_revenue_alert_state, _save_revenue_alert_date,
                _load_revenue_alert_slot, _save_revenue_alert_slot,
                _load_revenue_alert_context, _save_revenue_alert_context,
                _clear_revenue_alert_context,
            )
            from scout_core.job_runs import record_job_run
            from scout_agent import SCOUT_THRESHOLDS as _ST

            CT_TZ      = pytz.timezone("America/Chicago")
            sig        = _ST.get("signals", {})
            check_hour = int(sig.get("revenue_tracker_check_hour_ct",
                             os.getenv("REVENUE_TRACKER_CHECK_HOUR_CT", "10")))
            hourly_enabled     = sig.get("revenue_tracker_hourly_enabled", True)
            hourly_start       = int(sig.get("revenue_tracker_hourly_start_ct", 9))
            hourly_end         = int(sig.get("revenue_tracker_hourly_end_ct", 17))
            refire_drop_pct    = float(sig.get("revenue_tracker_refire_drop_pct", 10))

            while True:  # inner poll loop
                _time.sleep(300)  # 5-min poll
                try:
                    # Kill switch — default false; set REVENUE_TRACKER_ENABLED=true to activate
                    if os.getenv("REVENUE_TRACKER_ENABLED", "false").strip().lower() != "true":
                        continue

                    now_ct = _dt.now(CT_TZ)

                    # Weekdays only (Mon=0 … Fri=4)
                    if now_ct.weekday() >= 5:
                        continue

                    today_str = now_ct.date().isoformat()

                    if hourly_enabled:
                        # Business-hours gate
                        if not (hourly_start <= now_ct.hour < hourly_end):
                            continue

                        slot = f"{today_str}T{now_ct.hour:02d}"
                        if _load_revenue_alert_slot() == slot:
                            continue  # already handled this hour

                    else:
                        # Legacy: fire window — target hour ± 10 minutes
                        if not (now_ct.hour == check_hour and now_ct.minute < 10):
                            continue

                        if _load_revenue_alert_state() == today_str:
                            continue  # already posted today

                        slot = today_str  # not used below in daily mode

                    channel = _get_channel()
                    _bot_token = os.getenv("SLACK_BOT_TOKEN")
                    if not _bot_token:
                        log.error("[revenue-tracker] SLACK_BOT_TOKEN not set — skipping slot.")
                        if not hourly_enabled:
                            _save_revenue_alert_date(today_str)
                        else:
                            _save_revenue_alert_slot(slot)
                        record_job_run("revenue_tracker", status="error", error="SLACK_BOT_TOKEN not set", duration_ms=0)
                        continue
                    web = WebClient(token=_bot_token)
                    ch  = _get_ch_client()

                    _t0 = _time.monotonic()

                    # Phase 1: fast platform total
                    try:
                        total = _query_intraday_revenue_total(ch)
                    except Exception as e:
                        log.warning("[revenue-tracker] Phase 1 query failed: %s", e, exc_info=True)
                        if not hourly_enabled:
                            _save_revenue_alert_date(today_str)  # avoid hammering on CH error
                        else:
                            _save_revenue_alert_slot(slot)  # skip this hour on error
                        record_job_run(
                            "revenue_tracker",
                            status="error",
                            error=str(e)[:400],
                            duration_ms=int((_time.monotonic() - _t0) * 1000),
                        )
                        continue

                    if total is None:
                        # Revenue within normal range — mark checked, stay silent
                        if not hourly_enabled:
                            _save_revenue_alert_date(today_str)
                        else:
                            _save_revenue_alert_slot(slot)
                            # If we had an active revenue alert, clear it
                            if _load_revenue_alert_context() is not None:
                                firing_names = {s.alert_name for s in alert_registry.current_state()}
                                if "revenue_tracker" in firing_names:
                                    alert_registry.mark_cleared("revenue_tracker")
                                _clear_revenue_alert_context()
                                log.info("[revenue-tracker] Revenue on pace — alert cleared.")
                        log.info("[revenue-tracker] Revenue on pace — no alert needed.")
                        record_job_run(
                            "revenue_tracker",
                            status="success",
                            duration_ms=int((_time.monotonic() - _t0) * 1000),
                        )
                        continue

                    # Revenue below threshold
                    curr_pct = float(total.get("pct_of_expected", 0))

                    _last_alerted_pct = _load_revenue_alert_context()
                    if hourly_enabled and _last_alerted_pct is not None:
                        # Re-fire only if revenue has worsened by refire_drop_pct or more since last alert
                        worsened_enough = curr_pct <= (_last_alerted_pct or 0.0) - refire_drop_pct
                        if not worsened_enough:
                            # deduplicate
                            log.info(
                                "[revenue-tracker] dedup — pct=%.1f%% last_alerted=%.1f%% "
                                "no significant drop (slot=%s).",
                                curr_pct, _last_alerted_pct, slot,
                            )
                            _save_revenue_alert_slot(slot)
                            record_job_run(
                                "revenue_tracker", status="success",
                                duration_ms=int((_time.monotonic() - _t0) * 1000),
                            )
                            continue
                        # else: worsened_enough — fall through to fire

                    # Phase 2: per-publisher decomposition
                    _phase2_error: str | None = None
                    try:
                        publishers = _query_intraday_revenue_by_publisher(ch, total)
                    except Exception as e:
                        log.warning("[revenue-tracker] Phase 2 query failed: %s", e, exc_info=True)
                        publishers = []
                        _phase2_error = str(e)[:400]

                    fallback, blocks = _format_revenue_alert(total, publishers)
                    web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)

                    if not hourly_enabled:
                        _save_revenue_alert_date(today_str)
                    else:
                        _save_revenue_alert_slot(slot)
                        _save_revenue_alert_context(curr_pct)
                        alert_registry.mark_firing("revenue_tracker", {"slot": slot, "pct_of_expected": curr_pct})

                    duration_ms = int((_time.monotonic() - _t0) * 1000)
                    log.info(
                        "[revenue-tracker] Alert posted for %s (%.0f%% of expected).",
                        today_str, curr_pct,
                    )
                    record_job_run(
                        "revenue_tracker",
                        status="partial_error" if _phase2_error else "success",
                        error=_phase2_error,
                        duration_ms=duration_ms,
                    )

                except Exception as e:
                    log.warning("[revenue-tracker] Unexpected error: %s", e, exc_info=True)

        except Exception as e:
            log.error("[revenue-tracker] Fatal crash — restarting in 30s: %s", e, exc_info=True)
            import time
            time.sleep(30)


# ── Per-monitor prod-fire dedup ────────────────────────────────────────────────
# Maps monitor_name → last prod-fire date (YYYY-MM-DD CT).  Belt-and-suspenders
# against Render ephemeral disk wipes that reset pulse_state.json and would
# cause a monitor to re-fire the same day after a deploy.  Lives in memory so
# it resets on each process restart, but that's acceptable: deploy wipes are rare
# and this complements (not replaces) the persistent state file.
_PROD_FIRED: dict[str, str] = {}


def _run_shadow_monitor(
    *,
    monitor_name: str,
    config_key: str,
    signal_fn,
    format_fn,
    load_state_fn,
    save_state_fn,
) -> None:
    """Generic demand-feed daemon for hourly shadow monitors."""
    import time as _time
    import pytz
    from datetime import datetime as _dt
    from scout_ch import _get_ch_client
    from scout_agent import SCOUT_THRESHOLDS as _ST

    while True:  # outer restart wrapper
        try:
            CT_TZ          = pytz.timezone("America/Chicago")
            channel        = os.getenv("SCOUT_MONITOR_CHANNEL", "#scout-offers")
            shadow_channel = os.getenv("SCOUT_SHADOW_CHANNEL", "#scout-qa")
            tag            = f"[{monitor_name}]"
            last_shadow_slot = None

            while True:  # inner poll loop
                _time.sleep(300)
                try:
                    # Outer kill switch — entire framework disabled by default
                    if os.getenv("SCOUT_HOURLY_SHADOW_ENABLED", "false").strip().lower() not in ("1", "true", "yes"):
                        continue

                    # Per-monitor kill switch
                    if not _ST.get("signals", {}).get(f"{config_key}_monitor_enabled", False):
                        continue

                    check_hour = int(_ST.get("signals", {}).get(f"{config_key}_monitor_check_hour_ct", 9))
                    now_ct      = _dt.now(CT_TZ)
                    today_str   = now_ct.date().isoformat()
                    shadow_on   = os.getenv("SCOUT_HOURLY_SHADOW_ENABLED", "false").strip().lower() in ("1", "true", "yes")
                    in_prod_window = (now_ct.hour == check_hour and now_ct.minute < 10)
                    in_shadow_window = shadow_on

                    if not in_prod_window and not in_shadow_window:
                        continue

                    prod_already_fired = in_prod_window and load_state_fn() == today_str
                    shadow_slot = f"{today_str}T{now_ct.hour:02d}"
                    shadow_already_fired = in_shadow_window and last_shadow_slot == shadow_slot

                    if (
                        (not in_prod_window or prod_already_fired)
                        and (not in_shadow_window or shadow_already_fired)
                    ):
                        continue

                    t0 = _time.monotonic()
                    try:
                        raw_results = signal_fn(_get_ch_client())
                    except Exception as e:
                        log.warning(f"{tag} signal query failed: {e}")
                        from scout_core.job_runs import record_job_run
                        record_job_run(monitor_name, status="error",
                                       duration_ms=int((_time.monotonic() - t0) * 1000),
                                       error=str(e)[:400])
                        if in_shadow_window and not shadow_already_fired:
                            last_shadow_slot = shadow_slot
                        continue

                    results = raw_results or []
                    is_shadow_tick = (
                        in_shadow_window
                        and not shadow_already_fired
                        and (not in_prod_window or prod_already_fired)
                    )
                    target_channel = shadow_channel if is_shadow_tick else channel

                    duration_ms = int((_time.monotonic() - t0) * 1000)

                    if not results:
                        from scout_core.job_runs import record_job_run
                        record_job_run(monitor_name, status="success", duration_ms=duration_ms)
                        if is_shadow_tick:
                            last_shadow_slot = shadow_slot
                        else:
                            alert_registry.mark_cleared(monitor_name)
                        log.info(f"{tag} no anomalies — staying silent.")
                        continue

                    fallback, blocks = format_fn(results)
                    if not fallback:
                        from scout_core.job_runs import record_job_run
                        record_job_run(monitor_name, status="success", duration_ms=duration_ms)
                        if is_shadow_tick:
                            last_shadow_slot = shadow_slot
                        continue

                    # Belt-and-suspenders dedup: skip if this monitor already fired
                    # today in this process run (catches post-deploy re-fires where
                    # pulse_state.json was wiped but _PROD_FIRED still remembers).
                    if not is_shadow_tick and _PROD_FIRED.get(monitor_name) == today_str:
                        log.info(f"{tag} dedup: already fired for {today_str} — suppressing.")
                        from scout_core.job_runs import record_job_run
                        record_job_run(monitor_name, status="success", duration_ms=duration_ms)
                        continue

                    from slack_sdk.web import WebClient as _WC
                    web = _WC(token=os.getenv("SLACK_BOT_TOKEN"))
                    web.chat_postMessage(channel=target_channel, text=fallback, blocks=blocks)

                    from scout_core.job_runs import record_job_run
                    record_job_run(monitor_name, status="success", duration_ms=duration_ms)

                    if is_shadow_tick:
                        last_shadow_slot = shadow_slot
                        log.info(f"{tag} shadow-posted {shadow_slot} ({len(results)} items) → {target_channel}.")
                    else:
                        alert_registry.mark_firing(monitor_name, {"results_count": len(results), "channel": target_channel})
                        _PROD_FIRED[monitor_name] = today_str
                        save_state_fn(today_str)
                        log.info(f"{tag} posted alert for {today_str} ({len(results)} items).")

                except Exception as e:
                    log.warning(f"{tag} unexpected error: {e}")

        except Exception as e:
            log.error(f"{tag} fatal crash — restarting in 30s: {e}", exc_info=True)
            import time as _t2; _t2.sleep(30)


from scout_core.monitors import _run_hourly_with_web  # noqa: E402 — defined after top-level imports


# ── Shadow monitor config table + factory ─────────────────────────────────
# Each entry drives one _run_shadow_monitor daemon. Adding a new monitor =
# add one dict here. signal_filter is optional (velocity_down uses it).
_SHADOW_MONITOR_CONFIG: list[dict] = [
    {
        "monitor_name":  "velocity-down-monitor",
        "config_key":    "velocity_down",
        "signal_module": "scout_bot",
        "signal_fn":     "_pulse_signal_velocity",
        "signal_filter": lambda rows: [v for v in rows if v.get("direction") == "down"],
        "format_module": "scout_bot",
        "format_fn":     "_format_velocity_down_alert",
        "state_module":  "scout_state",
        "load_fn":       "_load_velocity_down_alert_state",
        "save_fn":       "_save_velocity_down_alert_date",
    },
    {
        "monitor_name":  "ghost-monitor",
        "config_key":    "ghost",
        "signal_module": "scout_bot",
        "signal_fn":     "_pulse_signal_ghost",
        "format_module": "scout_bot",
        "format_fn":     "_format_ghost_alert",
        "state_module":  "scout_state",
        "load_fn":       "_load_ghost_alert_state",
        "save_fn":       "_save_ghost_alert_date",
    },
    {
        "monitor_name":  "fill-monitor",
        "config_key":    "fill",
        "signal_module": "scout_bot",
        "signal_fn":     "_pulse_signal_fill_rate",
        "format_module": "scout_bot",
        "format_fn":     "_format_fill_alert",
        "state_module":  "scout_state",
        "load_fn":       "_load_fill_alert_state",
        "save_fn":       "_save_fill_alert_date",
    },
    {
        "monitor_name":  "cvr-anomaly-monitor",
        "config_key":    "cvr_anomaly",
        "signal_module": "scout_ch",
        "signal_fn":     "_query_cvr_anomaly",
        "format_module": "scout_bot",
        "format_fn":     "_format_cvr_alert",
        "state_module":  "scout_state",
        "load_fn":       "_load_cvr_anomaly_alert_state",
        "save_fn":       "_save_cvr_anomaly_alert_date",
    },
    {
        "monitor_name":  "expiration-monitor",
        "config_key":    "expiration",
        "signal_module": "scout_ch",
        "signal_fn":     "_query_expiring_campaigns",
        "format_module": "scout_bot",
        "format_fn":     "_format_expiration_alert",
        "state_module":  "scout_state",
        "load_fn":       "_load_expiration_alert_state",
        "save_fn":       "_save_expiration_alert_date",
    },
]


def _make_shadow_daemon(cfg: dict):
    """Factory: returns a zero-arg daemon fn from a _SHADOW_MONITOR_CONFIG entry.

    Uses importlib to preserve lazy-import semantics and avoid circular imports
    that arise from top-level scout_bot / scout_ch / scout_state imports here.
    """
    import importlib as _il

    def _daemon() -> None:
        sig_mod   = _il.import_module(cfg["signal_module"])
        signal_fn = getattr(sig_mod, cfg["signal_fn"])
        if cfg.get("signal_filter"):
            _raw, _filt = signal_fn, cfg["signal_filter"]
            def _filtered_signal_fn(ch, _r=_raw, _f=_filt):
                return _f(_r(ch))
            signal_fn = _filtered_signal_fn
        fmt_mod   = _il.import_module(cfg["format_module"])
        format_fn = getattr(fmt_mod, cfg["format_fn"])
        state_mod = _il.import_module(cfg["state_module"])
        load_fn   = getattr(state_mod, cfg["load_fn"])
        save_fn   = getattr(state_mod, cfg["save_fn"])
        _run_shadow_monitor(
            monitor_name=cfg["monitor_name"],
            config_key=cfg["config_key"],
            signal_fn=signal_fn,
            format_fn=format_fn,
            load_state_fn=load_fn,
            save_state_fn=save_fn,
        )

    return _daemon


_velocity_down_monitor_daemon = _make_shadow_daemon(_SHADOW_MONITOR_CONFIG[0])
_ghost_monitor_daemon         = _make_shadow_daemon(_SHADOW_MONITOR_CONFIG[1])
_fill_monitor_daemon          = _make_shadow_daemon(_SHADOW_MONITOR_CONFIG[2])
_cvr_anomaly_monitor_daemon   = _make_shadow_daemon(_SHADOW_MONITOR_CONFIG[3])
_expiration_monitor_daemon    = _make_shadow_daemon(_SHADOW_MONITOR_CONFIG[4])


def _cap_monitor_daemon() -> None:
    from scout_state import (
        _load_cap_alert_slot, _save_cap_alert_slot,
        _load_cap_alert_context, _save_cap_alert_context,
    )
    from scout_bot import _pulse_signal_cap, _format_cap_alert
    from scout_agent import SCOUT_THRESHOLDS as _ST

    sig = _ST.get("signals", {})
    hourly_enabled = sig.get("cap_monitor_hourly_enabled", True)

    if hourly_enabled:
        _run_hourly_with_web(
            signal_fn=_pulse_signal_cap,
            format_fn=_format_cap_alert,
            load_slot_fn=_load_cap_alert_slot,
            save_slot_fn=_save_cap_alert_slot,
            load_context_fn=_load_cap_alert_context,
            save_context_fn=_save_cap_alert_context,
            severity_key="cap_pct",
            escalation_pct=float(sig.get("cap_monitor_severity_escalation_pct", 5)),
            alert_name="cap_alert",
            hourly_start=int(sig.get("cap_monitor_hourly_start_ct", 9)),
            hourly_end=int(sig.get("cap_monitor_hourly_end_ct", 17)),
        )
    else:
        from scout_state import _load_cap_alert_state, _save_cap_alert_date
        _run_shadow_monitor(
            monitor_name="cap-monitor", config_key="cap",
            signal_fn=_pulse_signal_cap, format_fn=_format_cap_alert,
            load_state_fn=_load_cap_alert_state, save_state_fn=_save_cap_alert_date,
        )


# ── Queue storage ─────────────────────────────────────────────────────────────
# Persists QueueDraft rows to data/queue.json on Render disk.
# Atomic writes via a .tmp rename so a crash mid-write never corrupts state.
# Concurrency: the HTTP server runs in a single-threaded socketserver loop
# (socketserver.TCPServer defaults to non-threaded), so no lock is needed here.
# If the server is ever switched to ThreadingTCPServer, add threading.Lock().

_QUEUE_LOCK = threading.Lock()


def _load_queue() -> dict:
    """Return {draft_id: draft_dict} from queue.json, or {} on any error."""
    try:
        return json.loads(_QUEUE_FILE.read_text()).get("drafts", {})
    except Exception:
        return {}


def _save_queue(drafts: dict) -> None:
    """Atomically persist the drafts dict to queue.json."""
    tmp = _QUEUE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"drafts": drafts}, indent=2))
    tmp.replace(_QUEUE_FILE)


def _read_body(handler: http.server.BaseHTTPRequestHandler) -> Optional[dict]:
    """Read and parse JSON body from a request. Returns None on bad input."""
    try:
        length = int(handler.headers.get("Content-Length", 0))
        if length <= 0:
            return {}
        raw = handler.rfile.read(length)
        return json.loads(raw)
    except Exception:
        return None



# ── MS Platform integration — env vars needed before going live ────────────────
#
# MS_PLATFORM_TODO: Ask Vamsee to provide the following before flipping live:
#
#   CAMPAIGN_CREATE_WEBHOOK_URL
#       POST endpoint on MS Platform that accepts a CampaignRequest JSON body.
#       Shape posted (see _fire_campaign_creation):
#         {
#           "draft_id":    "<uuid>",
#           "offer":       { network, offer_id, advertiser, title, payout_num, ... },
#           "ai_copy":     { headline, description, cta_yes, cta_no, ... },
#           "approver":    "sidd",
#           "approved_at": "2026-05-24T14:00:00+00:00",
#           "dry_run":     false
#         }
#       Expected success response: any 2xx, body is relayed back to caller as-is.
#       Leave unset → dry_run mode (default, safe for staging).
#
#   CAMPAIGN_CREATE_API_KEY
#       Bearer token sent as Authorization: Bearer <token>.
#       Leave unset → no auth header (dev / local only).
#
#   CAMPAIGN_CREATE_DRY_RUN
#       "true" (default) → log + return preview, no HTTP call.
#       Set "false" AND set WEBHOOK_URL to go live.
#       GET /queue/config shows current state without exposing secrets.
#
# Flip order once Vamsee provides the endpoint:
#   1. Set CAMPAIGN_CREATE_WEBHOOK_URL in Render
#   2. Set CAMPAIGN_CREATE_API_KEY in Render  (if platform requires auth)
#   3. Keep CAMPAIGN_CREATE_DRY_RUN=true — test one approve, check /queue/config
#   4. Verify the "would_send" payload in the dry_run response looks right
#   5. Set CAMPAIGN_CREATE_DRY_RUN=false → live
# ──────────────────────────────────────────────────────────────────────────────


def _fire_campaign_creation(draft: dict) -> dict:
    """Hand a QueueDraft to the MS Platform campaign creation webhook.

    Safe-by-default: returns {"status": "dry_run"} when
    CAMPAIGN_CREATE_DRY_RUN=true (the default) or when
    CAMPAIGN_CREATE_WEBHOOK_URL is not set. In dry-run mode the full payload
    that *would* be sent is included as "would_send" so Vamsee can review
    it before flipping live.

    Auth: if CAMPAIGN_CREATE_API_KEY is set, it is sent as
    "Authorization: Bearer <key>".  Uses stdlib urllib — no extra dep.

    See the MS_PLATFORM_TODO block above for flip-live instructions.
    """
    webhook_url = os.getenv("CAMPAIGN_CREATE_WEBHOOK_URL", "").strip()
    api_key     = os.getenv("CAMPAIGN_CREATE_API_KEY", "").strip()
    dry_run     = os.getenv("CAMPAIGN_CREATE_DRY_RUN", "true").strip().lower() in ("1", "true", "yes")

    # Build the CampaignRequest payload regardless — used for both dry_run
    # preview and the real POST so there's one source of truth.
    payload: dict = {
        "draft_id":    draft.get("draft_id"),
        "offer":       draft.get("offer", {}),
        "ai_copy":     draft.get("ai_copy", {}),
        "approver":    draft.get("approval", {}).get("approver", ""),
        "approved_at": draft.get("approval", {}).get("approved_at", ""),
        "dry_run":     False,
    }

    if dry_run or not webhook_url:
        mode = (
            "CAMPAIGN_CREATE_DRY_RUN=true"
            if dry_run
            else "CAMPAIGN_CREATE_WEBHOOK_URL not set"
        )
        log.info(
            "[queue] dry_run campaign creation (%s) draft_id=%s offer=%s/%s",
            mode,
            draft.get("draft_id"),
            draft.get("offer", {}).get("network"),
            draft.get("offer", {}).get("offer_id"),
        )
        # MS_PLATFORM_TODO: review "would_send" in the response, then flip live.
        return {
            "status":    "dry_run",
            "draft_id":  draft.get("draft_id"),
            "mode":      mode,
            "would_send": payload,
        }

    import urllib.request as _ur
    import urllib.error as _ue

    try:
        body_bytes = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        req = _ur.Request(webhook_url, data=body_bytes, headers=headers, method="POST")
        with _ur.urlopen(req, timeout=10) as resp:
            try:
                resp_body = json.loads(resp.read() or b"{}")
            except Exception:
                resp_body = {}
            log.info(
                "[queue] campaign creation fired draft_id=%s → HTTP %s",
                draft.get("draft_id"), resp.status,
            )
            return {
                "status":            "fired",
                "draft_id":          draft.get("draft_id"),
                "platform_status":   resp.status,
                "platform_response": resp_body,
            }
    except _ue.HTTPError as exc:
        log.error("[queue] campaign creation webhook HTTP %s: %s", exc.code, exc)
        return {"status": "error", "error": f"HTTP {exc.code}", "draft_id": draft.get("draft_id")}
    except Exception as exc:
        log.error("[queue] campaign creation webhook failed: %s", exc)
        return {"status": "error", "error": str(exc), "draft_id": draft.get("draft_id")}


# ── Queue HTTP endpoints ───────────────────────────────────────────────────────
# Wired into _OffersHandler.do_POST / do_GET below.
# GET  /queue/config          → platform integration status (no secrets)
# GET  /queue/pending         → drafts awaiting approval
# GET  /queue/<id>            → single draft by id
# POST /queue/draft           → create new draft
# POST /queue/approve         → approve + fire campaign creation
# POST /queue/reject          → reject draft
# POST /campaigns/create      → fire campaign from approved draft directly

def _handle_queue_config(handler: http.server.BaseHTTPRequestHandler) -> None:
    """GET /queue/config — returns current MS Platform integration status.

    No secrets are exposed — only booleans for set/unset and the current mode.
    The platform team (or Vamsee) can hit this to verify the connection is
    configured before flipping CAMPAIGN_CREATE_DRY_RUN=false.

    Example response:
        {
          "campaign_creation": {
            "mode": "dry_run",
            "webhook_url_set": false,
            "api_key_set": false,
            "dry_run_flag": true
          },
          "queue_depth": { "pending": 3, "approved": 1, "rejected": 0 }
        }
    """
    webhook_url = os.getenv("CAMPAIGN_CREATE_WEBHOOK_URL", "").strip()
    api_key     = os.getenv("CAMPAIGN_CREATE_API_KEY", "").strip()
    dry_run     = os.getenv("CAMPAIGN_CREATE_DRY_RUN", "true").strip().lower() in ("1", "true", "yes")
    live        = bool(webhook_url) and not dry_run

    drafts = _load_queue()
    depth: dict = {"pending": 0, "approved": 0, "rejected": 0}
    for d in drafts.values():
        state = d.get("approval", {}).get("state", "pending")
        if state in depth:
            depth[state] += 1

    _write_json(handler, 200, {
        "campaign_creation": {
            # MS_PLATFORM_TODO: mode should read "live" before launch.
            "mode":            "live" if live else "dry_run",
            "webhook_url_set": bool(webhook_url),
            "api_key_set":     bool(api_key),
            "dry_run_flag":    dry_run,
        },
        "queue_depth": depth,
    })


def _handle_queue_create_draft(handler: http.server.BaseHTTPRequestHandler) -> None:
    """POST /queue/draft — create a new QueueDraft from offer JSON + optional copy."""
    body = _read_body(handler)
    if body is None:
        _write_json(handler, 400, {"error": "invalid JSON body"})
        return

    offer_dict = body.get("offer")
    if not offer_dict or not isinstance(offer_dict, dict):
        _write_json(handler, 400, {"error": "missing required field: offer"})
        return

    # Require at minimum network + offer_id so the draft is addressable.
    if not offer_dict.get("network") or not offer_dict.get("offer_id"):
        _write_json(handler, 400, {"error": "offer must include network and offer_id"})
        return

    draft_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    draft = {
        "draft_id": draft_id,
        "offer": offer_dict,
        "ai_copy": body.get("ai_copy") or {},
        "estimated_rpm": body.get("estimated_rpm"),
        "perf_ctx": body.get("perf_ctx") or "",
        "risk_flag": body.get("risk_flag") or "",
        "approval": {
            "state": "pending",
            "approver": "",
            "approved_at": None,
            "note": "",
        },
        "created_at": now,
    }

    with _QUEUE_LOCK:
        drafts = _load_queue()
        drafts[draft_id] = draft
        _save_queue(drafts)

    log.info("[queue] created draft_id=%s offer=%s/%s",
             draft_id, offer_dict.get("network"), offer_dict.get("offer_id"))
    _write_json(handler, 201, {"draft_id": draft_id, "status": "pending", "created_at": now})


def _handle_queue_approve(handler: http.server.BaseHTTPRequestHandler) -> None:
    """POST /queue/approve — approve a pending draft; fires campaign creation."""
    body = _read_body(handler)
    if body is None:
        _write_json(handler, 400, {"error": "invalid JSON body"})
        return

    draft_id = (body.get("draft_id") or "").strip()
    approver = (body.get("approver") or "").strip()
    if not draft_id:
        _write_json(handler, 400, {"error": "missing required field: draft_id"})
        return
    if not approver:
        _write_json(handler, 400, {"error": "missing required field: approver"})
        return

    now = datetime.now(timezone.utc).isoformat()

    with _QUEUE_LOCK:
        drafts = _load_queue()
        draft = drafts.get(draft_id)
        if draft is None:
            _write_json(handler, 404, {"error": f"draft not found: {draft_id}"})
            return

        current_state = draft.get("approval", {}).get("state", "pending")
        if current_state == "approved":
            _write_json(handler, 409, {"error": "draft already approved"})
            return
        if current_state == "rejected":
            _write_json(handler, 409, {"error": "draft is rejected; create a new draft"})
            return

        draft["approval"]["state"] = "approved"
        draft["approval"]["approver"] = approver
        draft["approval"]["approved_at"] = now
        draft["approval"]["note"] = body.get("note") or ""
        drafts[draft_id] = draft
        _save_queue(drafts)

    log.info("[queue] approved draft_id=%s by %s", draft_id, approver)

    campaign_result = _fire_campaign_creation(draft)
    _write_json(handler, 200, {
        "draft_id": draft_id,
        "status": "approved",
        "approved_at": now,
        "campaign": campaign_result,
    })


def _handle_queue_reject(handler: http.server.BaseHTTPRequestHandler) -> None:
    """POST /queue/reject — reject a pending draft."""
    body = _read_body(handler)
    if body is None:
        _write_json(handler, 400, {"error": "invalid JSON body"})
        return

    draft_id = (body.get("draft_id") or "").strip()
    approver = (body.get("approver") or "").strip()
    if not draft_id:
        _write_json(handler, 400, {"error": "missing required field: draft_id"})
        return

    now = datetime.now(timezone.utc).isoformat()

    with _QUEUE_LOCK:
        drafts = _load_queue()
        draft = drafts.get(draft_id)
        if draft is None:
            _write_json(handler, 404, {"error": f"draft not found: {draft_id}"})
            return

        current_state = draft.get("approval", {}).get("state", "pending")
        if current_state == "rejected":
            _write_json(handler, 409, {"error": "draft already rejected"})
            return

        draft["approval"]["state"] = "rejected"
        draft["approval"]["approver"] = approver
        draft["approval"]["approved_at"] = now
        draft["approval"]["note"] = body.get("note") or ""
        drafts[draft_id] = draft
        _save_queue(drafts)

    log.info("[queue] rejected draft_id=%s by %s", draft_id, approver or "(anonymous)")
    _write_json(handler, 200, {"draft_id": draft_id, "status": "rejected", "rejected_at": now})


def _handle_campaigns_create(handler: http.server.BaseHTTPRequestHandler) -> None:
    """POST /campaigns/create — fire a campaign from an approved draft.

    Callers may use this directly (platform page, Slack /scout queue launch)
    instead of going through POST /queue/approve. If draft_id is provided and
    the draft is in the queue, it is marked approved before firing.
    """
    body = _read_body(handler)
    if body is None:
        _write_json(handler, 400, {"error": "invalid JSON body"})
        return

    draft_id = (body.get("draft_id") or "").strip()

    with _QUEUE_LOCK:
        drafts = _load_queue()
        draft = drafts.get(draft_id) if draft_id else None

    if draft_id and draft is None:
        _write_json(handler, 404, {"error": f"draft not found: {draft_id}"})
        return

    target = draft if draft is not None else body
    result = _fire_campaign_creation(target)
    _write_json(handler, 200 if result.get("status") != "error" else 502, result)


if __name__ == "__main__":
    main()
