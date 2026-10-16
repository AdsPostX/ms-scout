"""Alert-formatting helpers shared between ms-scout and ms-demand-feed.

`_pulse_signal_cap`/`_format_cap_alert` and `_format_revenue_alert` are used
both by scout_bot.py's own on-demand force-run monitor system (`@Scout force
cap`/`revenue`, `/scout-cap`) and by scout/monitoring/daemons.py's scheduled
cap-monitor and revenue-tracker daemons. Extracted here so daemons.py doesn't
have to reach back into scout_bot.py to get them (see DESIGN.md's Phase 3
circular-import cleanup for the same pattern applied elsewhere).

scout_bot.py's other five monitor formatters (velocity, ghost, fill, cvr,
expiration) are NOT shared with daemons.py and stay defined in scout_bot.py.
"""

from __future__ import annotations

from scout_thresholds import _manager as _tm
from scout_ui_kit import Card, ResponsePattern, Severity, Surface, wrap_response

_SIGNAL_CFG = _tm.load().get("signals", {})
_CAP_ALERT_PCT = float(_SIGNAL_CFG.get("cap_alert_pct", 85))


def _pulse_signal_cap(ch, as_of_date: str | None = None) -> list:
    import queries as _q
    return _q.cap_alert_campaigns(
        ch,
        as_of_date=as_of_date,
        cap_alert_pct=_CAP_ALERT_PCT,
    )


def _build_alert_response(severity: Severity, headline: str, body: str, alert_name: str = "") -> tuple[str, list]:
    """Shared boilerplate for all monitor alert formatters.

    Builds a Card, calls wrap_response on MONITOR_ALARM with the ALERT pattern,
    and returns (fallback_text, blocks). Per-formatter row-parsing and
    headline/body construction stays in each formatter.
    """
    card = Card(severity=severity, headline=headline, body=body)
    if alert_name:
        card.actions = [
            ("✓ Acknowledge", "scout_acknowledge", alert_name, "primary"),
            ("Snooze ▾", "scout_snooze_open", alert_name, ""),
        ]
    _, blocks = wrap_response(
        card=card,
        surface=Surface.MONITOR_ALARM,
        pattern=ResponsePattern.ALERT,
    )
    return f"🟠 {headline}", blocks


def _build_alert_body(items: list[str], action_footer: str = "") -> str:
    body = "\n".join(f"• {item}" for item in items)
    if body and action_footer:
        return f"{body}\n\n{action_footer}"
    return body or action_footer


def _format_cap_alert(rows: list, alert_name: str = "") -> tuple[str, list[dict]]:
    """Return (fallback, blocks) Block Kit alert for advertisers nearing monthly cap.

    Each item shows: pct of cap, MTD revenue / cap, dollar headroom at risk,
    and days-to-cap vs days-remaining — so the reader knows urgency at a glance.
    Action footer prompts the only two levers: contact advertiser or lower bid floor.
    """
    items = []
    for r in rows[:8]:
        adv     = r.get("adv_name", "Unknown")
        cap_pct = r.get("cap_pct", 0)
        rev_mtd = r.get("revenue_mtd", 0)
        cap     = r.get("monthly_cap", 0)
        dtc     = r.get("days_to_cap", 0)
        dr      = r.get("days_remaining", 0)
        at_risk = max(cap - rev_mtd, 0)
        pace_note = (
            f"caps in ~{dtc:.0f}d, {dr}d left — *~${at_risk:,.0f} at risk*"
            if dtc < dr
            else f"~{dtc:.0f}d to cap, {dr}d remaining"
        )
        items.append(
            f"*{adv}*: *{cap_pct:.0f}%* of cap · "
            f"${rev_mtd:,.0f} / ${cap:,.0f} · "
            f"{pace_note}"
        )

    headline = "Cap alert — advertisers approaching monthly budget"
    return _build_alert_response(Severity.WARN, headline, _build_alert_body(items, "→ Contact advertiser or lower bid floor before cap hits"), alert_name=alert_name)


def _format_revenue_alert(total: dict, publishers: list, as_of: str | None = None, alert_name: str = "") -> tuple[str, list[dict]]:
    """
    Format the proactive revenue alert message.

    total: dict from _query_intraday_revenue_total (today_revenue, projected_full_day,
           dow_median, pct_of_expected, weekday, sample_days)
    publishers: list of dicts from _query_intraday_revenue_by_publisher
                (publisher_name, publisher_id, delta, root_cause, ...)
    as_of: human-readable time string, e.g. "3pm CT" — defaults to current CT time
    """
    import pytz
    from datetime import datetime as _dt

    if as_of is None:
        _ct = _dt.now(pytz.timezone("America/Chicago"))
        as_of = _ct.strftime("%-I:%M%p CT").lower()  # e.g. "3:04pm CT"

    pct        = round(total["pct_of_expected"])
    today_rev  = total["today_revenue"]
    projected  = total["projected_full_day"]
    expected   = total["dow_median"]
    weekday    = total["weekday"]
    samples    = total["sample_days"]

    # Body items only — header is supplied by the alert renderer at the call site
    items = [
        f"Platform so far ({as_of}): *${today_rev:,.0f}* | projected: *${projected:,.0f}* | expected [{weekday}]: ~*${expected:,.0f}*",
        f"Tracking at *{pct}%* of expected ({samples} same-weekday samples)",
    ]

    _ROOT_LABELS = {
        "ghost_campaign": "impressions ✓, $0 revenue → ghost campaign",
        "fill_rate":      "zero impressions → fill rate or cap hit",
        "traffic":        "zero sessions → no upstream traffic",
        "revenue_down":   "revenue below expected, specific cause unclear",
    }

    if publishers:
        items.append("*Where the gap is:*")
        for p in publishers:
            name     = p.get("publisher_name") or f"pub {p.get('publisher_id', '?')}"
            pub_id   = p.get("publisher_id", "")
            delta    = p.get("delta", 0.0)
            cause    = p.get("root_cause", "normal")
            label    = _ROOT_LABELS.get(cause, cause)
            id_str   = f" *(pub {pub_id})*" if pub_id else ""
            items.append(f"{name}{id_str}: *−${abs(delta):,.0f}* below expected · {label}")

        items.append("All other publishers within normal range.")

        # Suggest a next step based on top root cause
        top_cause = publishers[0].get("root_cause", "normal")
        top_pub   = publishers[0].get("publisher_name", "")
        if top_cause == "ghost_campaign":
            items.append(f"Immediate: `@Scout ghost campaigns` — {top_pub} matches ghost detection criteria.")
        elif top_cause == "fill_rate":
            items.append(f"Immediate: `@Scout fill rate` — {top_pub} has zero impressions despite active sessions.")
        elif top_cause == "revenue_down":
            items.append(
                f"Immediate: `@Scout {top_pub}` — revenue is below expected with no single dominant signal; "
                f"check traffic, fill rate, and ghost-campaign indicators."
            )
        elif top_cause == "traffic":
            items.append(f"Immediate: `@Scout {top_pub}` — no sessions; confirm SDK is sending traffic.")
    else:
        items.append(
            "No single publisher accounts for the gap — revenue is spread-down across the platform.\n"
            "Likely causes: session volume drop, fill rate platform-wide, or a slow day.\n"
            "Run `@Scout fill rate` to check publisher-level session health."
        )

    headline = "Revenue alert — today is tracking soft"
    body = "\n".join(items)
    card = Card(severity=Severity.CRITICAL, headline=headline, body=body)
    if alert_name:
        card.actions = [
            ("✓ Acknowledge", "scout_acknowledge", alert_name, "primary"),
            ("Snooze ▾", "scout_snooze_open", alert_name, ""),
        ]
    _, blocks = wrap_response(card=card, surface=Surface.MONITOR_ALARM, pattern=ResponsePattern.ALERT)
    return f"🔴 {headline}", blocks
