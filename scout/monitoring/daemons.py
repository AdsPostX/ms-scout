"""
scout/monitoring/daemons.py — demand-feed background alert daemons.

Extracted from demand_feed_main.py, which had accreted these four alerting
systems (projection-autocheck, revenue-tracker, the 5-signal shadow-monitor
factory, cap-monitor) alongside the offer scraper it's actually named for.
Per DESIGN.md's Service Topology decision: this is a module-boundary fix
within the same ms-demand-feed process, NOT a new deployed service.

Each daemon function does a LOCAL `from demand_feed_main import _FEED_CFG`
(and, for the revenue tracker, `_DEMAND_FEED_HQ_CHANNEL`) at its own top,
matching this codebase's existing lazy-import convention for exactly this
reason: demand_feed_main.py imports these daemon functions from this module
at ITS top level, so importing back from demand_feed_main.py at THIS
module's top level would be circular. The local import only executes when a
daemon actually runs (as a background thread, after both modules have fully
loaded), so there is no real circularity at runtime — same pattern
_projection_autocheck_daemon and _make_shadow_daemon already used for
scout_ch/scout_state/importlib before this move.

Baseline test coverage for the pure/testable surface of these 4 systems
lives in smoke_test.py, written against demand_feed_main.py's location
before this move (see that commit) — behavior here must match exactly.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import alert_registry
from scout_core.monitors import _run_hourly_with_web

log = logging.getLogger("demand_feed")  # same logger name as demand_feed_main.py — preserves Render log grouping


# ═══════════════════════════════════════════════════════════════════════════
# System 1 — Projection Autocheck
# ═══════════════════════════════════════════════════════════════════════════

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
            lines.append(f"• Range:  ${result['projected_low']:,.0f} - ${result['projected_high']:,.0f}")

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
    Kill switch: PROJECTION_AUTOCHECK_ENABLED env var (default false — off; requires redeploy to take effect).
    Wired to job_runs telemetry.
    """
    from demand_feed_main import _FEED_CFG

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
            channel = _FEED_CFG.scout_qa_channel
            tag     = "[projection-autocheck]"
            _bot_token = _FEED_CFG.slack_bot_token
            if not _bot_token:
                log.error("[projection-autocheck] SLACK_BOT_TOKEN not set — retrying in 60s.")
                _time.sleep(60)
                continue
            web = WebClient(token=_bot_token)
            from scout_slack_safe import guard_web_client
            guard_web_client(web)

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
                    if not _FEED_CFG.projection_autocheck_enabled:
                        _time.sleep(300)
                        continue

                    win_start  = _FEED_CFG.projection_autocheck_window_start_ct
                    win_end    = _FEED_CFG.projection_autocheck_window_end_ct
                    eod_hour   = _FEED_CFG.projection_autocheck_eod_hour_ct
                    eod_minute = _FEED_CFG.projection_autocheck_eod_minute_ct
                    cmp_hour   = _FEED_CFG.projection_autocheck_apples_hour_ct
                    cmp_tol    = _FEED_CFG.projection_autocheck_apples_tol_usd
                    max_errs   = _FEED_CFG.projection_autocheck_max_errors

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
            import time as _t3
            _t3.sleep(30)


# ═══════════════════════════════════════════════════════════════════════════
# System 2 — Revenue Tracker
# ═══════════════════════════════════════════════════════════════════════════

def _revenue_worsened_enough(
    curr_pct: float, last_alerted_pct: Optional[float], refire_drop_pct: float,
) -> bool:
    """True when revenue has dropped at least `refire_drop_pct` further below
    expected since the last alert, i.e. it's worth re-firing rather than
    deduplicating.
    """
    return curr_pct <= (last_alerted_pct or 0.0) - refire_drop_pct


def _revenue_tracker_daemon() -> None:
    """Demand-feed port of scout_bot._revenue_tracker.

    Hourly mode (revenue_tracker_hourly_enabled=true, default):
      Posts during business hours (9am–5pm CT) whenever revenue drops below
      threshold. Smart deduplication: re-fires only when pct_of_expected drops
      by >= revenue_tracker_refire_drop_pct since the last posted alert.
      Uses YYYY-MM-DDTHH slot for per-hour idempotency.

    Daily fallback (revenue_tracker_hourly_enabled=false):
      Legacy once-daily behaviour at revenue_tracker_check_hour_ct (now 10am CT).

    Kill switch: REVENUE_TRACKER_ENABLED env var (default false — off; requires redeploy to take effect).
    Wired to job_runs telemetry via scout_core.job_runs.record_job_run.

    Posts to REVENUE_OPS_CHANNEL (production) or #bot-qa (non-production).

    Outer restart wrapper: any unhandled crash logs the traceback and restarts
    after 30s so the thread stays alive indefinitely without a Render redeploy.
    """
    from demand_feed_main import _FEED_CFG, _DEMAND_FEED_HQ_CHANNEL

    def _get_channel() -> str:
        if _FEED_CFG.scout_env != "production":
            return _DEMAND_FEED_HQ_CHANNEL
        return _FEED_CFG.revenue_ops_channel

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
            from scout_thresholds import _manager as _tm

            CT_TZ      = pytz.timezone("America/Chicago")
            sig        = _tm.load().get("signals", {})
            check_hour = int(sig.get("revenue_tracker_check_hour_ct",
                             _FEED_CFG.revenue_tracker_check_hour_ct))
            hourly_enabled     = sig.get("revenue_tracker_hourly_enabled", True)
            hourly_start       = int(sig.get("revenue_tracker_hourly_start_ct", 9))
            hourly_end         = int(sig.get("revenue_tracker_hourly_end_ct", 17))
            refire_drop_pct    = float(sig.get("revenue_tracker_refire_drop_pct", 10))

            while True:  # inner poll loop
                _time.sleep(300)  # 5-min poll
                try:
                    # Kill switch — default false; set REVENUE_TRACKER_ENABLED=true and redeploy to activate
                    if not _FEED_CFG.revenue_tracker_enabled:
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
                    _bot_token = _FEED_CFG.slack_bot_token
                    if not _bot_token:
                        log.error("[revenue-tracker] SLACK_BOT_TOKEN not set — skipping slot.")
                        if not hourly_enabled:
                            _save_revenue_alert_date(today_str)
                        else:
                            _save_revenue_alert_slot(slot)
                        record_job_run("revenue_tracker", status="error", error="SLACK_BOT_TOKEN not set", duration_ms=0)
                        continue
                    web = WebClient(token=_bot_token)
                    from scout_slack_safe import guard_web_client
                    guard_web_client(web)
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
                        worsened_enough = _revenue_worsened_enough(curr_pct, _last_alerted_pct, refire_drop_pct)
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

                    fallback, blocks = _format_revenue_alert(total, publishers, alert_name="revenue_tracker")

                    # UC-1: check snooze state before posting
                    _should_post = True
                    try:
                        _ps = alert_registry.get_post_state("revenue_tracker")
                        if _ps and _ps.snooze_until:
                            _snooze_dt = datetime.fromisoformat(_ps.snooze_until)
                            if _snooze_dt > datetime.now(timezone.utc):
                                _should_post = False
                                log.info("demand_feed: [revenue_tracker] suppressed — active snooze until %s", _ps.snooze_until)
                            elif _ps.snoozed_by:
                                from scout_ui_kit import _refire_context_block
                                blocks = [_refire_context_block(_ps.snoozed_by, _ps.snooze_until)] + list(blocks)
                                alert_registry.clear_snooze("revenue_tracker")
                    except Exception:
                        pass  # snooze check is best-effort — never block the alert post

                    if _should_post:
                        _post_resp = web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)

                        if not hourly_enabled:
                            _save_revenue_alert_date(today_str)
                        else:
                            _save_revenue_alert_slot(slot)
                            _save_revenue_alert_context(curr_pct)
                            alert_registry.mark_firing("revenue_tracker", {"slot": slot, "pct_of_expected": curr_pct})
                            try:
                                if _post_resp and _post_resp.get("ok"):
                                    alert_registry.set_post_state(
                                        "revenue_tracker",
                                        _post_resp["ts"],
                                        channel,
                                        datetime.now(timezone.utc).isoformat(),
                                    )
                            except Exception:
                                log.warning("demand_feed: set_post_state failed for revenue_tracker")

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
            import time as _t4
            _t4.sleep(30)


# ═══════════════════════════════════════════════════════════════════════════
# System 3 — Shadow Monitors (5 signals: velocity-down, ghost, fill,
# cvr-anomaly, expiration) + the generic engine cap-monitor's shadow fallback
# also uses.
# ═══════════════════════════════════════════════════════════════════════════

# ── Per-monitor prod-fire dedup ────────────────────────────────────────────────
# Maps monitor_name → last prod-fire date (YYYY-MM-DD CT).  Belt-and-suspenders
# against Render ephemeral disk wipes that reset pulse_state.json and would
# cause a monitor to re-fire the same day after a deploy.  Lives in memory so
# it resets on each process restart, but that's acceptable: deploy wipes are rare
# and this complements (not replaces) the persistent state file.
#
# Shared across all 5 shadow-monitor daemon threads (velocity-down, ghost, fill,
# cvr-anomaly, expiration) plus cap-monitor's shadow-fallback path — each thread
# only ever reads/writes its own monitor_name key, so no lock is needed today,
# but this invariant must be preserved (or replaced with real synchronization)
# if this dict is ever restructured.
_PROD_FIRED: dict[str, str] = {}


def _is_shadow_tick(
    in_shadow_window: bool,
    shadow_already_fired: bool,
    in_prod_window: bool,
    prod_already_fired: bool,
) -> bool:
    """True when this tick's post should go to the shadow channel rather than
    the real monitor channel: fire to shadow when shadow mode is on, hasn't
    already fired this hour's shadow slot, AND either we're outside the real
    prod window or the prod window already fired (so shadow never steals a
    genuine prod fire).
    """
    return (
        in_shadow_window
        and not shadow_already_fired
        and (not in_prod_window or prod_already_fired)
    )


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
    from demand_feed_main import _FEED_CFG

    import time as _time
    import pytz
    from datetime import datetime as _dt
    from scout_ch import _get_ch_client
    from scout_thresholds import _manager as _tm

    while True:  # outer restart wrapper
        try:
            CT_TZ          = pytz.timezone("America/Chicago")
            channel        = _FEED_CFG.scout_monitor_channel
            shadow_channel = _FEED_CFG.scout_shadow_channel
            tag            = f"[{monitor_name}]"
            last_shadow_slot = None

            while True:  # inner poll loop
                _time.sleep(300)
                try:
                    # Per-monitor kill switch
                    if not _tm.load().get("signals", {}).get(f"{config_key}_monitor_enabled", False):
                        continue

                    check_hour = int(_tm.load().get("signals", {}).get(f"{config_key}_monitor_check_hour_ct", 9))
                    now_ct      = _dt.now(CT_TZ)
                    today_str   = now_ct.date().isoformat()

                    # Prod-window path: fires to SCOUT_MONITOR_CHANNEL at check_hour regardless of shadow mode.
                    in_prod_window = (now_ct.hour == check_hour and now_ct.minute < 10)

                    # Shadow-tick path: hourly snapshots to #scout-qa, gated by SCOUT_HOURLY_SHADOW_ENABLED.
                    shadow_on        = _FEED_CFG.scout_hourly_shadow_enabled
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
                    is_shadow_tick = _is_shadow_tick(
                        in_shadow_window, shadow_already_fired, in_prod_window, prod_already_fired,
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

                    fallback, blocks = format_fn(results, alert_name=monitor_name)
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
                    web = _WC(token=_FEED_CFG.slack_bot_token)
                    # UC-1: check snooze state before posting (prod only — shadow ticks always post)
                    _should_post = True
                    if not is_shadow_tick:
                        try:
                            _ps = alert_registry.get_post_state(monitor_name)
                            if _ps and _ps.snooze_until:
                                _snooze_dt = datetime.fromisoformat(_ps.snooze_until)
                                if _snooze_dt > datetime.now(timezone.utc):
                                    _should_post = False
                                    log.info("demand_feed: [%s] suppressed — active snooze until %s", monitor_name, _ps.snooze_until)
                                elif _ps.snoozed_by:
                                    from scout_ui_kit import _refire_context_block
                                    blocks = [_refire_context_block(_ps.snoozed_by, _ps.snooze_until)] + list(blocks)
                                    alert_registry.clear_snooze(monitor_name)
                        except Exception:
                            pass  # snooze check is best-effort — never block the alert post

                    if _should_post:
                        _post_resp = web.chat_postMessage(channel=target_channel, text=fallback, blocks=blocks)
                    else:
                        _post_resp = None

                    from scout_core.job_runs import record_job_run
                    record_job_run(monitor_name, status="success", duration_ms=duration_ms)

                    if is_shadow_tick:
                        last_shadow_slot = shadow_slot
                        log.info(f"{tag} shadow-posted {shadow_slot} ({len(results)} items) → {target_channel}.")
                    elif _should_post:
                        alert_registry.mark_firing(monitor_name, {"results_count": len(results), "channel": target_channel})
                        try:
                            if _post_resp and _post_resp.get("ok"):
                                alert_registry.set_post_state(
                                    monitor_name,
                                    _post_resp["ts"],
                                    target_channel,
                                    datetime.now(timezone.utc).isoformat(),
                                )
                        except Exception:
                            log.warning("demand_feed: set_post_state failed for %s", monitor_name)
                        _PROD_FIRED[monitor_name] = today_str
                        save_state_fn(today_str)
                        log.info(f"{tag} posted alert for {today_str} ({len(results)} items).")

                except Exception as e:
                    log.warning(f"{tag} unexpected error: {e}")

        except Exception as e:
            log.error(f"{tag} fatal crash — restarting in 30s: {e}", exc_info=True)
            import time as _t2; _t2.sleep(30)


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


# ═══════════════════════════════════════════════════════════════════════════
# System 4 — Cap Monitor (dispatcher only — delegates to _run_hourly_with_web
# by default, or this module's own _run_shadow_monitor as a fallback)
# ═══════════════════════════════════════════════════════════════════════════

def _cap_monitor_daemon() -> None:
    from scout_state import (
        _load_cap_alert_slot, _save_cap_alert_slot,
        _load_cap_alert_context, _save_cap_alert_context,
    )
    from scout_bot import _pulse_signal_cap, _format_cap_alert
    from scout_thresholds import _manager as _tm

    sig = _tm.load().get("signals", {})
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
