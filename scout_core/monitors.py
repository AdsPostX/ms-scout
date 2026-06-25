"""scout_core.monitors — shared hourly monitor daemon helper.

Shared by ms-scout (scout_bot.py) and ms-demand-feed (demand_feed_main.py).
Single source of truth for the generic hourly cap/revenue monitor loop.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def _run_hourly_with_web(
    *,
    signal_fn,
    format_fn,
    load_slot_fn,
    save_slot_fn,
    load_context_fn,
    save_context_fn,
    severity_key: str,
    escalation_pct: float,
    alert_name: str,
    hourly_start: int = 9,
    hourly_end: int = 17,
) -> None:
    """Generic hourly daemon for cap/revenue monitors with smart deduplication.

    Polls every 5 minutes. During business hours (hourly_start <= hour < hourly_end CT):
      - Uses YYYY-MM-DDTHH slot for per-hour idempotency.
      - Fires only when new advertisers appear or existing ones escalate by >= escalation_pct.
      - Posts a resolved message when the condition clears.
      - Slot is NOT saved on dedup so next hour can re-check in case severity changes.

    Outer restart wrapper: any unhandled crash logs the traceback and restarts
    after 30s so the thread stays alive indefinitely without a Render redeploy.
    """
    import time as _time
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo
    import alert_registry
    from slack_sdk.web import WebClient as _WC
    from scout_ch import _get_ch_client
    from scout_core.job_runs import record_job_run

    CT_TZ = ZoneInfo("America/Chicago")
    tag = f"[{alert_name}]"

    while True:  # outer restart wrapper — self-heals any unhandled crash
        try:
            while True:  # inner poll loop
                _time.sleep(300)  # 5-minute poll interval
                try:
                    now_ct = _dt.now(CT_TZ)

                    # Business-hours gate
                    if not (hourly_start <= now_ct.hour < hourly_end):
                        continue

                    slot = f"{now_ct.date().isoformat()}T{now_ct.hour:02d}"
                    if load_slot_fn() == slot:
                        continue  # already fired this hour

                    t0 = _time.monotonic()
                    try:
                        results = signal_fn(_get_ch_client())
                    except Exception as e:
                        # Intentionally do NOT save slot on CH error — allows retry next hour.
                        # Risk: up to 12 retries/hr during outage. Revenue tracker takes the
                        # opposite approach and saves slot on error to prevent CH hammering;
                        # the cap monitor prioritizes not missing a cap alert over CH load.
                        log.warning(f"{tag} signal query failed: {e}")
                        record_job_run(alert_name, status="error",
                                       duration_ms=int((_time.monotonic() - t0) * 1000),
                                       error=str(e)[:400])
                        continue

                    results = results or []
                    duration_ms = int((_time.monotonic() - t0) * 1000)

                    if not results:
                        # Condition cleared — post resolved message if alert was active
                        firing_names = {s.alert_name for s in alert_registry.current_state()}
                        if alert_name in firing_names:
                            web = _WC(token=os.getenv("SLACK_BOT_TOKEN", ""))
                            channel = os.getenv("SCOUT_MONITOR_CHANNEL", "#scout-offers")
                            if os.getenv("SCOUT_ENV", "development") != "production":
                                channel = os.getenv("SCOUT_SHADOW_CHANNEL", "#scout-qa")
                            web.chat_postMessage(
                                channel=channel,
                                text=f"✅ {alert_name} resolved — no advertisers above threshold.",
                            )
                            # Save slot BEFORE mark_cleared: if mark_cleared throws,
                            # the slot is already persisted so the resolved message
                            # won't re-post on the next poll.
                            save_slot_fn(slot)
                            alert_registry.mark_cleared(alert_name)
                            log.info(f"{tag} condition cleared — resolved message posted.")
                        record_job_run(alert_name, status="success", duration_ms=duration_ms)
                        continue

                    # Load prior context for severity comparison
                    prior = load_context_fn()
                    prior_by_name = {
                        str(r.get("adv_name", "")).strip().lower(): r
                        for r in prior
                    }
                    current_names = {str(r.get("adv_name", "")).strip().lower() for r in results}
                    prior_names = set(prior_by_name.keys())

                    new_advertisers = current_names - prior_names
                    escalated = set()
                    for r in results:
                        name = str(r.get("adv_name", "")).strip().lower()
                        if name in prior_by_name:
                            prior_val = float(prior_by_name[name].get(severity_key, 0))
                            curr_val = float(r.get(severity_key, 0))
                            if curr_val >= prior_val + escalation_pct:  # spec: >= not >
                                escalated.add(name)

                    if not new_advertisers and not escalated:
                        # Same advertisers, severity unchanged or improved — deduplicate
                        # Do NOT save slot so next hour can re-check
                        log.info(f"{tag} dedup — same advertisers, no escalation (slot {slot}).")
                        record_job_run(alert_name, status="success", duration_ms=duration_ms)
                        continue

                    # New or escalated — fire the alert
                    fallback, blocks = format_fn(results, alert_name=alert_name)
                    if not fallback:
                        record_job_run(alert_name, status="success", duration_ms=duration_ms)
                        continue

                    web = _WC(token=os.getenv("SLACK_BOT_TOKEN", ""))
                    channel = os.getenv("SCOUT_MONITOR_CHANNEL", "#scout-offers")
                    if os.getenv("SCOUT_ENV", "development") != "production":
                        channel = os.getenv("SCOUT_SHADOW_CHANNEL", "#scout-qa")
                    web.chat_postMessage(channel=channel, text=fallback, blocks=blocks)

                    alert_registry.mark_firing(alert_name, {"slot": slot, "count": len(results)})
                    save_context_fn(results)
                    save_slot_fn(slot)
                    record_job_run(alert_name, status="success", duration_ms=duration_ms)
                    log.info(
                        f"{tag} posted alert slot={slot} new={len(new_advertisers)} "
                        f"escalated={len(escalated)} total={len(results)}."
                    )

                except Exception as e:
                    log.warning(f"{tag} unexpected error: {e}")

        except Exception as e:
            log.error(f"{tag} fatal crash — restarting in 30s: {e}", exc_info=True)
            import time as _t2; _t2.sleep(30)
