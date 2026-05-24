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
        text = (
            f"[projection-autocheck] :bar_chart: *Projection autocheck* `{slot}`\n"
            f"• Today so far: ${today_rev:,.0f}\n"
            f"• Projected EOD: ${proj:,.0f}{med_line}\n"
            f"• Curve share: {share} ({source})"
            f"{cmp_line}"
        )
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
    import time as _time
    import pytz
    from datetime import datetime as _dt
    from scout_agent import _get_ch_client
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

    while True:  # outer restart wrapper
        try:
            CT_TZ   = pytz.timezone("America/Chicago")
            channel = os.getenv("SCOUT_QA_CHANNEL", "#sidd-qa")
            tag     = "[projection-autocheck]"
            web     = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

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
            _time.sleep(30)


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
        target=_nightly_harvest_daemon, daemon=True, name="context-harvest"
    ).start()
    log.info("[demand-feed] nightly-harvest daemon started (kill switch: HARVESTER_AUTO_WRITE_ENABLED)")
    threading.Thread(
        target=_projection_autocheck_daemon,
        daemon=True,
        name="projection-autocheck",
    ).start()
    log.info(
        "[demand-feed] projection-autocheck daemon started "
        "(kill switch: PROJECTION_AUTOCHECK_ENABLED)"
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


def _format_revenue_alert(total: dict, publishers: list, as_of: Optional[str] = None) -> tuple:
    """Format the proactive revenue alert Slack message.

    Inlined from scout_bot._format_revenue_alert to avoid cross-service coupling.
    Produces Block Kit blocks when possible; falls back to plain-text list.

    total: dict with today_revenue, projected_full_day, dow_median,
           pct_of_expected, weekday, sample_days
    publishers: list of dicts with publisher_name, publisher_id, delta, root_cause
    as_of: human-readable time string; defaults to current CT time
    """
    import pytz
    from datetime import datetime as _dt

    if as_of is None:
        _ct = _dt.now(pytz.timezone("America/Chicago"))
        as_of = _ct.strftime("%-I:%M%p CT").lower()

    pct       = round(total["pct_of_expected"])
    today_rev = total["today_revenue"]
    projected = total["projected_full_day"]
    expected  = total["dow_median"]
    weekday   = total["weekday"]
    samples   = total["sample_days"]

    _ROOT_LABELS = {
        "ghost_campaign": "impressions ✓, $0 revenue → ghost campaign",
        "fill_rate":      "zero impressions → fill rate or cap hit",
        "traffic":        "zero sessions → no upstream traffic",
        "revenue_down":   "revenue below expected, specific cause unclear",
    }

    lines = [
        f"Platform so far ({as_of}): *${today_rev:,.0f}* | projected: *${projected:,.0f}* | expected [{weekday}]: ~*${expected:,.0f}*",
        f"Tracking at *{pct}%* of expected ({samples} same-weekday samples)",
    ]

    if publishers:
        lines.append("*Where the gap is:*")
        for p in publishers:
            name   = p.get("publisher_name") or f"pub {p.get('publisher_id', '?')}"
            pub_id = p.get("publisher_id", "")
            delta  = p.get("delta", 0.0)
            cause  = p.get("root_cause", "normal")
            label  = _ROOT_LABELS.get(cause, cause)
            id_str = f" *(pub {pub_id})*" if pub_id else ""
            lines.append(f"{name}{id_str}: *−${abs(delta):,.0f}* below expected · {label}")
        lines.append("All other publishers within normal range.")
        top_cause = publishers[0].get("root_cause", "normal")
        top_pub   = publishers[0].get("publisher_name", "")
        if top_cause == "ghost_campaign":
            lines.append(f"Immediate: `@Scout ghost campaigns` — {top_pub} matches ghost detection criteria.")
        elif top_cause == "fill_rate":
            lines.append(f"Immediate: `@Scout fill rate` — {top_pub} has zero impressions despite active sessions.")
        elif top_cause == "revenue_down":
            lines.append(
                f"Immediate: `@Scout {top_pub}` — revenue is below expected with no single dominant signal; "
                f"check traffic, fill rate, and ghost-campaign indicators."
            )
        elif top_cause == "traffic":
            lines.append(f"Immediate: `@Scout {top_pub}` — no sessions; confirm SDK is sending traffic.")
    else:
        lines.append(
            "No single publisher accounts for the gap — revenue is spread-down across the platform.\n"
            "Likely causes: session volume drop, fill rate platform-wide, or a slow day.\n"
            "Run `@Scout fill rate` to check publisher-level session health."
        )

    fallback = "🔴 Revenue alert — today is tracking soft"
    body = "\n".join(lines)
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":red_circle: *Revenue alert — today is tracking soft*"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": body},
        },
    ]
    return fallback, blocks


def _revenue_tracker_daemon() -> None:
    """Demand-feed port of scout_bot._revenue_tracker.

    Proactive intraday revenue alert at 3pm CT on weekdays.
    Kill switch: REVENUE_TRACKER_ENABLED env var (default false — off).
    Wired to job_runs telemetry via scout_core.job_runs.record_job_run.

    Runs every 5 minutes. Fires at most once per calendar day (state in
    pulse_state.json via scout_state). Posts to REVENUE_ALERT_CHANNEL
    (default: #revenue-operations channel id from REVENUE_OPS_CHANNEL env var).

    Outer restart wrapper: any unhandled crash logs the traceback and restarts
    after 30s so the thread stays alive indefinitely without a Render redeploy.
    """
    import time as _time
    import pytz
    from datetime import datetime as _dt
    from slack_sdk.web import WebClient
    from scout_agent import _query_intraday_revenue_total, _query_intraday_revenue_by_publisher
    from scout_state import _load_revenue_alert_state, _save_revenue_alert_date
    from scout_ch import _get_ch_client
    from scout_core.job_runs import record_job_run

    _HQ_CHANNEL = "C0AQEECF800"  # #bot-qa fallback (matches scout_bot._SCOUT_HQ_CHANNEL)

    def _get_channel() -> str:
        scout_env = os.getenv("SCOUT_ENV", "development")
        if scout_env != "production":
            return _HQ_CHANNEL
        return os.getenv("REVENUE_OPS_CHANNEL", _HQ_CHANNEL)

    while True:  # outer restart wrapper — self-heals any unhandled crash
        try:
            CT_TZ      = pytz.timezone("America/Chicago")
            check_hour = int(os.getenv("REVENUE_TRACKER_CHECK_HOUR_CT", "15"))

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

                    # Fire window: target hour ± 10 minutes
                    if not (now_ct.hour == check_hour and now_ct.minute < 10):
                        continue

                    today_str = now_ct.date().isoformat()
                    if _load_revenue_alert_state() == today_str:
                        continue  # already posted today

                    channel = _get_channel()
                    web = WebClient(token=os.getenv("SLACK_BOT_TOKEN", ""))
                    ch  = _get_ch_client()

                    _t0 = _time.monotonic()

                    # Phase 1: fast platform total
                    try:
                        total = _query_intraday_revenue_total(ch)
                    except Exception as e:
                        log.warning("[revenue-tracker] Phase 1 query failed: %s", e)
                        _save_revenue_alert_date(today_str)  # avoid hammering on CH error
                        record_job_run(
                            "revenue_tracker",
                            status="error",
                            error=str(e)[:400],
                            duration_ms=int((_time.monotonic() - _t0) * 1000),
                        )
                        continue

                    if total is None:
                        # Revenue within normal range — mark checked, stay silent
                        _save_revenue_alert_date(today_str)
                        log.info("[revenue-tracker] Revenue on pace — no alert needed.")
                        record_job_run(
                            "revenue_tracker",
                            status="success",
                            duration_ms=int((_time.monotonic() - _t0) * 1000),
                        )
                        continue

                    # Phase 2: per-publisher decomposition
                    try:
                        publishers = _query_intraday_revenue_by_publisher(ch, total)
                    except Exception as e:
                        log.warning("[revenue-tracker] Phase 2 query failed: %s", e)
                        publishers = []

                    fallback, blocks = _format_revenue_alert(total, publishers)
                    web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)
                    _save_revenue_alert_date(today_str)
                    duration_ms = int((_time.monotonic() - _t0) * 1000)
                    log.info(
                        "[revenue-tracker] Alert posted for %s (%.0f%% of expected).",
                        today_str, total["pct_of_expected"],
                    )
                    record_job_run(
                        "revenue_tracker",
                        status="success",
                        duration_ms=duration_ms,
                    )

                except Exception as e:
                    log.warning("[revenue-tracker] Unexpected error: %s", e)

        except Exception as e:
            log.error("[revenue-tracker] Fatal crash — restarting in 30s: %s", e, exc_info=True)
            _time.sleep(30)


def _nightly_harvest_daemon() -> None:
    """Demand-feed port of scout_bot._nightly_harvest.

    Harvests Slack channel context once per day at midnight CT.
    Kill switch: HARVESTER_AUTO_WRITE_ENABLED env var (default false — off).
    Wired to job_runs telemetry.
    """
    from datetime import datetime as _dt, timedelta as _td
    import time as _time

    while True:
        try:
            # Kill switch — default false; set HARVESTER_AUTO_WRITE_ENABLED=true to activate
            if os.getenv("HARVESTER_AUTO_WRITE_ENABLED", "false").strip().lower() != "true":
                _time.sleep(300)
                continue

            now = _now_chicago()
            tomorrow_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + _td(days=1)
            sleep_secs = (tomorrow_midnight - now).total_seconds()

            from context_harvester import harvest, is_stale
            from scout_core.job_runs import record_job_run

            if is_stale():
                log.info("[harvest] context stale or missing — running immediate harvest")
                t0 = _time.monotonic()
                try:
                    result = harvest()
                    duration_ms = int((_time.monotonic() - t0) * 1000)
                    record_job_run("nightly_harvest", status="success", duration_ms=duration_ms)
                    _post_harvest_audit(result)
                except Exception as exc:
                    duration_ms = int((_time.monotonic() - t0) * 1000)
                    record_job_run("nightly_harvest", status="error",
                                   duration_ms=duration_ms, error=str(exc)[:400])
                    raise
            else:
                log.info(f"[harvest] context is fresh — sleeping {sleep_secs / 3600:.1f}h until midnight CT")

            _time.sleep(sleep_secs)

            log.info("[harvest] midnight CT — running nightly harvest")
            t0 = _time.monotonic()
            try:
                result = harvest()
                duration_ms = int((_time.monotonic() - t0) * 1000)
                record_job_run("nightly_harvest", status="success", duration_ms=duration_ms)
                _post_harvest_audit(result)
            except Exception as exc:
                duration_ms = int((_time.monotonic() - t0) * 1000)
                record_job_run("nightly_harvest", status="error",
                               duration_ms=duration_ms, error=str(exc)[:400])
                raise
        except Exception as e:
            log.error(f"[harvest] cycle failed: {e}", exc_info=True)
            import time as _time2
            _time2.sleep(3600)  # retry in 1 hour on failure


def _post_harvest_audit(harvest_result: dict) -> None:
    """Post a brief audit summary to #scout-qa if the harvester learned any entity facts."""
    try:
        audit = harvest_result.get("audit", []) if isinstance(harvest_result, dict) else []
        if not audit:
            return  # nothing to report

        written = [e for e in audit if e.get("action") == "written"]
        skipped = [e for e in audit if e.get("action") == "skipped"]

        if not written and not skipped:
            return

        lines = [f":newspaper: *Scout learned overnight* ({len(written)} fact{'s' if len(written) != 1 else ''} added to entity knowledge)"]
        for e in written:
            icon = ":office:" if e.get("type") == "publisher" else ":chart_with_upwards_trend:"
            lines.append(f"{icon} *{e['name']}* ({e['type']}) — {e.get('note', '')[:80]}")
        for e in skipped:
            lines.append(f":grey_exclamation: *{e['name']}* — skipped: {e.get('reason', 'manual entry exists')}")
        lines.append("_To correct anything: `@Scout, actually [entity] does X` — I'll overwrite it._")

        _alert_slack("\n".join(lines))
        log.info(f"[harvest] audit posted — {len(written)} written, {len(skipped)} skipped")
    except Exception as e:
        log.warning(f"[harvest] audit post failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
