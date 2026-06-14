from __future__ import annotations

# standard library
import json
import logging
import os
import pathlib
import urllib.parse
import urllib.request
from datetime import date

# third-party
# (none beyond what's in local modules)

# local — NO scout_agent
import queries as _q
from scout_ch import (
    _get_ch_client,
    _query_cvr_anomaly,
)
from scout_thresholds import _manager
from scout_types import FormattedOffer  # noqa: F401
from scout_tools_offers import _get_risk_flag, _scout_score, _load_offers, _format_offers

log = logging.getLogger("scout_agent")

# ── Snapshot path (mirrors scout_agent.SNAPSHOT_PATH) ────────────────────────
SNAPSHOT_PATH = pathlib.Path(__file__).parent / "data" / "offers_latest.json"

# ── Post-transaction placement types (used in ClickHouse queries) ─────────────
_POST_TX_PLACEMENTS = (
    "'checkout_confirmation_page'", "'order_confirmation'", "'order-confirmation'",
    "'buy_flow_thank_you'", "'buyflowthankyou'", "'acctmgmt_payment_confirmation'",
    "'acctmgmtpaymentconfirmation'", "'receipt'", "'visit-receipt'", "'visit_receipt'",
    "'parking_pass_receipt'", "'order-receipt'", "'receipt-parkingdotcom'",
    "'post_checkout_receipt'", "'post_transaction'", "'post_transaction_page'",
    "'metropolis_transaction_details'", "'7eleven-fuel-transactionreceipt-bottom'",
    "'7Eleven_Fuel_TransactionReceipt_Bottom'", "'conv-orderconfirmation'",
    "'thank_you'", "'message_confirmation'", "'registration_complete'",
    "'order_status_offers'",
)


# ── Private helpers ───────────────────────────────────────────────────────────

def _data_quality_tier(days_of_data: int, sessions: int = 0) -> dict:
    return _manager.data_quality_tier(days_of_data, sessions)


def _load_performance_benchmarks() -> dict:
    return _manager._load_benchmarks_file()


# ── Extracted publisher tool functions ────────────────────────────────────────

def get_publisher_competitive_landscape(
    publisher_name: str = None,
    publisher_id: int = None,
    offer_name: str = None,
    hypothetical_payout: float = None,
    weeks: int = 2,
) -> dict:
    """
    Query ClickHouse for what's competing on a given publisher right now.
    Returns: ranked offer list by RPM, publisher weekly impression volume,
    and (if offer_name + hypothetical_payout supplied) where that offer would rank
    at current vs. hypothetical payout.

    BLUF output: "At $40 CPA, TurboTax would rank #3 of 8 on AT&T (~12% share).
                  At $35 it ranks #5 (~7% share). AT&T runs ~180K impressions/week."
    """
    try:
        ch = _get_ch_client()

        # Step 1: find publisher — by numeric ID or name.
        # IMPORTANT: adpx_impressions_details.pid stores the numeric user id as a string,
        # NOT the hex sdk_id. Always use str(id) as the pid for impression queries.
        if not publisher_name and not publisher_id:
            return {"error": "Provide either publisher_name or publisher_id."}

        if publisher_id:
            pub_result = _q.publisher_lookup_by_id(ch, int(publisher_id))
            if not pub_result:
                return {"error": f"No publisher found with ID {publisher_id}."}
            pub_rows = [(pub_result["id"], pub_result["organization"], pub_result["sdk_id"])]
        else:
            raw = _q.publisher_lookup_by_name(ch, publisher_name)
            if not raw:
                return {"error": f"No publisher found matching '{publisher_name}'. Try a shorter name e.g. 'AT&T' or 'TXB'."}
            pub_rows = [(r["id"], r["organization"], r["sdk_id"]) for r in raw]

        if not pub_rows:
            return {"error": f"No publisher found matching '{publisher_name}'. Try a shorter name e.g. 'AT&T' or 'TXB'."}

        # Pick the best row in a single pass — no extra ClickHouse roundtrips.
        # Priority: non-test/demo row whose id has the most recent impressions.
        candidate_ids = [str(row[0]) for row in pub_rows]
        vol_map = _q.publisher_impression_volume(ch, candidate_ids, days=7)

        # Score each candidate: deprioritize test/demo, then rank by volume
        def _candidate_score(row) -> tuple:
            org = (row[1] or "").lower()
            is_noise = int("test" in org or "demo" in org)
            volume = vol_map.get(str(row[0]), 0)
            return (is_noise, -volume)  # lower is better

        chosen = sorted(pub_rows, key=_candidate_score)[0]

        pub_id_int, pub_full_name, pub_sdk_id = chosen[0], chosen[1], chosen[2]
        # pid in impressions table = numeric user id as string
        pub_pid = str(pub_id_int)

        # Step 2 + Step 3: weekly impression volume + provisioned campaigns (sequential)
        # SQL lives in queries.py — parameterized, no f-strings
        vol_data = _q.publisher_weekly_impressions(ch, pub_pid, days=28)
        prov_data = _q.publisher_provisioned_campaigns(ch, pub_id_int)

        vol_rows = [(d["week"], d["impressions"]) for d in vol_data]
        prov_rows = [(d["campaign_id"], d["adv_name"], d["payout"]) for d in prov_data]

        weekly_impressions = int(sum(r[1] for r in vol_rows) / max(len(vol_rows), 1)) if vol_rows else 0

        # Step 4: determine which provisioned campaigns have recent impressions (serving now).
        serving_map: dict = {}  # campaign_id → impression count
        rpm_map: dict = {}      # campaign_id → RPM (only for serving campaigns)

        if prov_data:
            prov_ids = [d["campaign_id"] for d in prov_data]
            serving_map = _q.publisher_serving_campaign_impressions(ch, pub_pid, prov_ids, days=14)

            if serving_map:
                serving_ids = list(serving_map.keys())
                rpm_map = _q.publisher_campaign_rpms(ch, pub_pid, serving_ids, days=14)

        # Build unified list — serving first (by RPM), then provisioned-only (by payout desc).
        competitors = []
        for row in prov_rows:
            campaign_id, adv_name, payout = row
            cid = str(campaign_id)
            impressions_2w = serving_map.get(cid, 0)
            is_serving = impressions_2w > 0
            rpm = rpm_map.get(cid, 0.0)
            competitors.append({
                "advertiser": adv_name,
                "campaign_id": cid,
                "provisioned": True,
                "is_serving": is_serving,
                "impressions_2w": impressions_2w,
                "rpm": rpm,
                "payout": float(payout) if payout else None,
            })
        competitors.sort(key=lambda x: (0 if x["is_serving"] else 1, -x["rpm"], -(x["payout"] or 0)))

        serving_count = sum(1 for c in competitors if c["is_serving"])
        result = {
            "publisher": pub_full_name,
            "publisher_pid": pub_pid,
            "weekly_impressions_avg": weekly_impressions,
            "projected_impressions_2w": weekly_impressions * weeks,
            "provisioned_campaigns": competitors,
            "provisioned_count": len(competitors),
            "serving_count": serving_count,
            # payout_scenario logic below expects active_competitors
            "active_competitors": [c for c in competitors if c["is_serving"]],
            "competitor_count": serving_count,
        }

        # Step 4: if offer + payout provided, compute rank scenarios
        if offer_name and hypothetical_payout is not None:
            benchmarks = _manager.benchmarks()

            # Find the offer from snapshot — use it to get category/network context for scoring
            matched_offer = next(
                (o for o in _load_offers() if offer_name.lower() in (o.get("advertiser") or "").lower()),
                None,
            )
            current_payout = matched_offer.get("_payout_num") if matched_offer else None

            def _est_rpm(payout: float) -> float:
                synth = {**(matched_offer or {}), "_payout_num": payout}
                return round(_scout_score(synth, benchmarks), 2)

            hyp_rpm = _est_rpm(hypothetical_payout)
            cur_rpm = _est_rpm(current_payout) if current_payout else None

            rpms = sorted([c["rpm"] for c in competitors if c["rpm"] > 0], reverse=True)

            def _rank(rpm: float) -> int:
                return sum(1 for r in rpms if r > rpm) + 1

            def _share_pct(rank: int, total: int) -> float:
                if total == 0:
                    return 0.0
                # Simple linear decay: rank 1 gets 2x share of last, proportional
                weight = max(total - rank + 1, 1)
                total_weight = sum(range(1, total + 2))
                return round(weight / total_weight * 100, 1)

            n = len(rpms)
            hyp_rank = _rank(hyp_rpm)
            hyp_share = _share_pct(hyp_rank, n)

            result["payout_scenario"] = {
                "offer": offer_name,
                "hypothetical_payout": hypothetical_payout,
                "hypothetical_rpm": hyp_rpm,
                "hypothetical_rank": hyp_rank,
                "hypothetical_impression_share_pct": hyp_share,
                "projected_impressions_2w": round(weekly_impressions * weeks * hyp_share / 100),
            }

            if cur_rpm is not None:
                cur_rank = _rank(cur_rpm)
                cur_share = _share_pct(cur_rank, n)
                result["payout_scenario"]["current_payout"] = current_payout
                result["payout_scenario"]["current_rpm"] = cur_rpm
                result["payout_scenario"]["current_rank"] = cur_rank
                result["payout_scenario"]["current_impression_share_pct"] = cur_share
                result["payout_scenario"]["current_impressions_2w"] = round(
                    weekly_impressions * weeks * cur_share / 100
                )

        return result

    except Exception as e:
        from scout_ch import CHBusyError
        if isinstance(e, CHBusyError):
            raise
        log.warning(f"get_publisher_competitive_landscape failed: {e}")
        return {"error": str(e)}


def get_publisher_health(
    publisher_name: str = None,
    publisher_id: int = None,
    days: int = 14,
    geo_state: str = None,
) -> dict:
    """
    Full publisher health analysis: sessions, impressions, clicks, conversions, revenue, RPM, CTR, CVR.
    Breaks down by placement and OS. Includes click position data.
    """
    try:
        ch = _get_ch_client()

        # ── Resolve publisher name → ID ───────────────────────────────────────
        pid = publisher_id
        pub_name = None
        if pid is None and publisher_name:
            pub_results = _q.publisher_lookup_by_name(ch, publisher_name)
            if not pub_results:
                return {"error": f"No publisher found matching '{publisher_name}'"}
            # Disambiguate: pick the candidate with the most recent sessions.
            # Without this, accounts with the same name return in arbitrary order and
            # the wrong (inactive) account gets picked — e.g. TextNow 2527 vs 1952.
            if len(pub_results) > 1:
                candidate_pids = [str(r["id"]) for r in pub_results]
                vol_map = _q.publisher_impression_volume(ch, candidate_pids, days=7)
                best_pid_str = max(candidate_pids, key=lambda p: vol_map.get(p, 0))
                best = next((r for r in pub_results if str(r["id"]) == best_pid_str), pub_results[0])
            else:
                best = pub_results[0]
            pid = int(best["id"])
            pub_name = best["organization"]
        elif pid is not None:
            pub_result = _q.publisher_lookup_by_id(ch, int(pid))
            pub_name = pub_result["organization"] if pub_result else f"Partner {pid}"

        if pid is None:
            return {"error": "Must provide publisher_name or publisher_id"}

        # ── Partition filter ──────────────────────────────────────────────────
        today = date.today()
        # partition for sessions (go back days + a little buffer)
        partition = int(today.strftime("%Y%m")) - (1 if today.day <= days else 0)
        extended_partition = partition - 1  # extra month for downstream lag

        # ── Fetch all data via queries.py (sequential — CH client not thread-safe) ──
        placement_names = _q.publisher_placement_names(ch, int(pid))
        q1_data = _q.publisher_health_sessions(ch, int(pid), partition, days, geo_state)
        q2_data = _q.publisher_health_ad_metrics(ch, int(pid), str(pid), partition, extended_partition, days, geo_state)
        q3_data = _q.publisher_health_click_metrics(ch, int(pid), partition, days, geo_state)

        # ── Combine results ───────────────────────────────────────────────────
        # Build placement-keyed dicts
        sess_by_placement: dict = {}
        os_by_placement: dict = {}
        for row in q1_data:
            p = row["placement"] or "unknown"
            sess_by_placement[p] = sess_by_placement.get(p, 0) + row["sessions"]
            os_by_placement.setdefault(p, {})
            os_val = row["os"] or "unknown"
            os_by_placement[p][os_val] = os_by_placement[p].get(os_val, 0) + row["sessions"]

        ad_metrics: dict = {}
        for row in q2_data:
            p = row["placement"] or "unknown"
            ad_metrics[p] = {
                "impressions": row["impressions"],
                "conversions": row["conversions"],
                "revenue":     row["revenue"],
                "payout":      row["payout"],
            }

        click_metrics: dict = {}
        for row in q3_data:
            p = row["placement"] or "unknown"
            click_metrics[p] = {
                "clicks":           row["clicks"],
                "converted_clicks": row["converted_clicks"],
                "avg_position":     row["avg_position"],
            }

        # Aggregate OS split across all placements
        os_totals: dict = {}
        for p_os in os_by_placement.values():
            for os_val, cnt in p_os.items():
                os_totals[os_val] = os_totals.get(os_val, 0) + cnt
        total_sess_all = sum(os_totals.values()) or 1
        os_split = sorted(
            [{"os": k, "sessions": v, "share_pct": round(v / total_sess_all * 100, 1)} for k, v in os_totals.items()],
            key=lambda x: -x["sessions"],
        )

        # Build per-placement breakdown
        all_placements = set(sess_by_placement.keys()) | set(ad_metrics.keys()) | set(click_metrics.keys())
        total_revenue = sum(ad_metrics.get(p, {}).get("revenue", 0) for p in all_placements)
        total_impressions = sum(ad_metrics.get(p, {}).get("impressions", 0) for p in all_placements)
        publisher_avg_rpm = (total_revenue / total_impressions * 1000) if total_impressions else 0

        by_placement = []
        for p in all_placements:
            sess = sess_by_placement.get(p, 0)
            ad = ad_metrics.get(p, {"impressions": 0, "conversions": 0, "revenue": 0.0, "payout": 0.0})
            cl = click_metrics.get(p, {"clicks": 0, "converted_clicks": 0, "avg_position": 0.0})
            impr = ad["impressions"]
            rev = ad["revenue"]
            rpm = round(rev / impr * 1000, 2) if impr else 0.0
            ctr = round(cl["clicks"] / impr * 100, 2) if impr else 0.0
            cvr = round(ad["conversions"] / cl["clicks"] * 100, 2) if cl["clicks"] else 0.0

            anomaly = None
            if publisher_avg_rpm > 0 and rpm > 0 and rpm > publisher_avg_rpm * 5:
                ratio = round(rpm / publisher_avg_rpm, 1)
                anomaly = f"{ratio}x higher RPM than publisher avg"

            display_name = placement_names.get(p, p)
            by_placement.append({
                "placement":       display_name,
                "placement_slug":  p,
                "sessions":        sess,
                "impressions":     impr,
                "clicks":          cl["clicks"],
                "conversions":     ad["conversions"],
                "revenue":         round(rev, 2),
                "rpm":             rpm,
                "ctr_pct":         ctr,
                "cvr_pct":         cvr,
                "avg_position":    cl["avg_position"],
                "anomaly":         anomaly,
            })

        by_placement.sort(key=lambda x: -x["revenue"])

        # Overall rollup
        total_sessions = sum(sess_by_placement.values())
        total_clicks = sum(cl["clicks"] for cl in click_metrics.values())
        total_conversions = sum(ad_metrics.get(p, {}).get("conversions", 0) for p in all_placements)
        total_payout = sum(ad_metrics.get(p, {}).get("payout", 0) for p in all_placements)
        all_positions = [cl["avg_position"] for cl in click_metrics.values() if cl["avg_position"] > 0]
        avg_click_position = round(sum(all_positions) / len(all_positions), 1) if all_positions else 0.0

        overall_rpm = round(total_revenue / total_impressions * 1000, 2) if total_impressions else 0.0
        overall_ctr = round(total_clicks / total_impressions * 100, 2) if total_impressions else 0.0
        overall_cvr = round(total_conversions / total_clicks * 100, 2) if total_clicks else 0.0

        # Top placement note
        top_placement_note = ""
        if len(by_placement) >= 2:
            top = by_placement[0]
            bottom = by_placement[-1]
            if bottom["rpm"] > 0 and top["rpm"] > 0:
                ratio = round(top["rpm"] / bottom["rpm"], 1)
                top_placement_note = (
                    f"{top['placement']} generates {ratio}x RPM of {bottom['placement']}"
                )

        return {
            "publisher":     pub_name or f"Partner {pid}",
            "days":          days,
            "geo_state":     geo_state or None,
            "overall": {
                "sessions":           total_sessions,
                "impressions":        total_impressions,
                "clicks":             total_clicks,
                "conversions":        total_conversions,
                "revenue":            round(total_revenue, 2),
                "payout":             round(total_payout, 2),
                "rpm":                overall_rpm,
                "ctr_pct":            overall_ctr,
                "cvr_pct":            overall_cvr,
                "avg_click_position": avg_click_position,
            },
            "by_placement":          by_placement,
            "os_split":              os_split,
            "top_placements_note":   top_placement_note,
            "data_quality":          _data_quality_tier(days, total_sessions),
        }
    except Exception as e:
        log.exception("get_publisher_health failed")
        return {"error": str(e), "publisher_name": publisher_name, "publisher_id": publisher_id}


def get_perkswall_engagement(
    publisher_name: str = None,
    publisher_id: int = None,
    days: int = 30,
) -> dict:
    """
    Perkswall offer selection analytics — which offers do loyalty members actually pick?
    """
    try:
        ch = _get_ch_client()

        # ── Resolve publisher name → ID ───────────────────────────────────────
        pid = publisher_id
        pub_name = None
        if pid is None and publisher_name:
            pub_results = _q.publisher_lookup_by_name(ch, publisher_name)
            if not pub_results:
                return {"error": f"No publisher found matching '{publisher_name}'"}
            # Disambiguate: pick the candidate with the most recent sessions.
            if len(pub_results) > 1:
                candidate_pids = [str(r["id"]) for r in pub_results]
                vol_map = _q.publisher_impression_volume(ch, candidate_pids, days=7)
                best_pid_str = max(candidate_pids, key=lambda p: vol_map.get(p, 0))
                best = next((r for r in pub_results if str(r["id"]) == best_pid_str), pub_results[0])
            else:
                best = pub_results[0]
            pid = int(best["id"])
            pub_name = best["organization"]
        elif pid is not None:
            pub_result = _q.publisher_lookup_by_id(ch, int(pid))
            pub_name = pub_result["organization"] if pub_result else f"Partner {pid}"

        if pid is None:
            return {"error": "Must provide publisher_name or publisher_id"}

        # ── Partition filter ──────────────────────────────────────────────────
        today = date.today()
        partition = int(today.strftime("%Y%m")) - (1 if today.day <= days else 0)

        # ── Query: perk selections by offer ───────────────────────────────────
        sel_rows = ch.query(
            """
            SELECT
                sp.campaign_id,
                any(c.adv_name)                        AS offer_name,
                count()                                AS selections,
                count(DISTINCT sp.pub_user_id)         AS unique_members,
                count(DISTINCT sp.session_id)          AS sessions_with_selection
            FROM from_airbyte_user_selected_perks sp
            JOIN from_airbyte_campaigns c ON toInt64(sp.campaign_id) = c.id
            WHERE sp.user_id = {pid: UInt64}
              AND sp.created_at >= today() - {days: UInt32}
            GROUP BY sp.campaign_id
            ORDER BY selections DESC
            LIMIT 20
            """,
            parameters={"pid": int(pid), "days": days},
        ).result_rows

        # ── Total sessions for selection rate ─────────────────────────────────
        sess_row = ch.query(
            """
            SELECT count() AS total_sessions, count(DISTINCT session_id) AS unique_sessions
            FROM adpx_sdk_sessions
            PREWHERE user_id = {pid: UInt64}
                AND toYYYYMM(created_at) >= {partition: UInt32}
            WHERE created_at >= today() - {days: UInt32}
            """,
            parameters={"pid": int(pid), "partition": partition, "days": days},
        ).result_rows
        total_sessions = int(sess_row[0][0]) if sess_row else 0
        total_sessions_safe = total_sessions or 1

        # ── Build offer breakdown ─────────────────────────────────────────────
        by_offer = []
        total_selections = 0
        all_unique_members = set()
        for campaign_id, offer_name, selections, unique_members, sessions_with_selection in sel_rows:
            total_selections += int(selections)
            by_offer.append({
                "offer":                    offer_name or f"Campaign {campaign_id}",
                "campaign_id":              int(campaign_id) if campaign_id else None,
                "selections":               int(selections),
                "unique_members":           int(unique_members),
                "sessions_with_selection":  int(sessions_with_selection),
                "selection_rate_pct":       round(int(selections) / total_sessions_safe * 100, 2),
            })

        total_unique_members = sum(o["unique_members"] for o in by_offer)
        selection_rate_pct = round(total_selections / total_sessions_safe * 100, 2)

        # ── Insight ───────────────────────────────────────────────────────────
        insight = ""
        if by_offer:
            top = by_offer[0]
            insight = (
                f"{top['offer']} selected by {top['unique_members']:,} unique members "
                f"({top['selection_rate_pct']}% of sessions)"
            )

        return {
            "publisher":               pub_name or f"Partner {pid}",
            "publisher_id":            int(pid),
            "days":                    days,
            "total_sessions":          total_sessions,
            "total_selections":        total_selections,
            "selection_rate_pct":      selection_rate_pct,
            "unique_members_engaged":  total_unique_members,
            "by_offer":                by_offer,
            "insight":                 insight,
        }
    except Exception as e:
        log.exception("get_perkswall_engagement failed")
        return {"error": str(e), "publisher_name": publisher_name, "publisher_id": publisher_id}


def get_low_fill_publishers() -> str:
    """
    Return publishers with fill rate < 15% over last 7 days (canonical: 7d window, 2,500 min sessions).
    Fill rate = % of sessions that received at least one offer impression.
    Low fill on a checkout/receipt page = burned traffic with no monetization attempt.

    Uses fill_rate_publishers() canonical query (7-day window, minimum 2,500 sessions/7d).
    Previously used low_fill_publishers() (30-day window, 10,000 minimum sessions) — deprecated.
    """
    ch = _get_ch_client()
    try:
        # Canonical fill rate query: 7d window, 2500 min sessions, <15% threshold, entity overrides.
        # Any threshold change belongs in config/scout_thresholds.json (fill_rate_min_sessions_7d).
        data = _q.fill_rate_publishers(ch, limit=15)
    except Exception as e:
        return f"Fill rate query failed: {e}"

    if not data:
        return (
            "*Publisher Fill Rate Report*\n\n"
            ":white_check_mark: All publishers are filling at ≥15% over the last 7 days — "
            "no low-fill anomalies detected."
        )

    total_missed = sum(int(d["missed_sessions"]) for d in data)

    lines = [
        "*Publisher Fill Rate Report — Low Fill on Post-Transaction Pages*\n",
        f"{len(data)} publisher{'s' if len(data) != 1 else ''} below 15% fill · "
        f"{total_missed / 1_000_000:.1f}M missed sessions/7d\n",
    ]

    for d in data:
        pub_id   = d["publisher_id"]
        pub_name = d["publisher_name"]
        sessions = d["sessions_7d"]
        fill_pct = d["fill_rate_pct"]
        missed   = d["missed_sessions"]

        sessions_str = f"{int(sessions) / 1_000_000:.1f}M" if sessions >= 1_000_000 else f"{int(sessions) / 1000:.0f}K"
        missed_str   = f"{int(missed) / 1_000_000:.1f}M" if missed >= 1_000_000 else f"{int(missed) / 1000:.0f}K"

        if fill_pct < 2:
            hypothesis = "Near-zero fill — SDK likely not showing offers at all. Check SDK integration, geo/OS targeting config, or advertiser supply."
        elif fill_pct < 10:
            hypothesis = "Very low fill — most sessions get no offer. Check advertiser targeting restrictions (geo/OS/device), cap exhaustion, or SDK render failure."
        else:
            hypothesis = "Below-normal fill — some sessions served, but majority missed. May be geo/device targeting mismatch or insufficient advertiser supply."

        lines.append(
            f"• *{pub_name or f'Pub #{pub_id}'}* · Pub #{pub_id}\n"
            f"  {sessions_str} sessions/7d · {fill_pct:.1f}% fill · {missed_str} sessions missed\n"
            f"  ↳ _{hypothesis}_"
        )

    lines.append(
        "\n:zap: Start with the highest missed-session publishers — they represent the largest "
        "uncaptured monetization surface. Check SDK integration logs and advertiser targeting config."
    )
    return "\n".join(lines)


def get_offers_for_publisher(publisher_name: str) -> dict:  # returns dict since PR #18; old str annotation was stale
    """
    Return top affiliate offers (from SUPPORTED_NETWORKS inventory) that are
    a good fit for this publisher but not yet provisioned in their campaign set.
    Scored by estimated RPM using real MS conversion benchmarks (_scout_score).
    Different from get_supply_demand_gaps — surfaces NET-NEW affiliate inventory,
    not advertisers already on the MS platform.
    """
    if not SNAPSHOT_PATH.exists():
        return (
            f"Offer inventory is empty — the scraper hasn't run yet on Render. "
            f"Run `@Scout refresh offers` to fetch now (~2 min), "
            f"or wait for the 6am CT daily auto-refresh."
        )

    try:
        all_offers = json.loads(SNAPSHOT_PATH.read_text())
    except Exception as e:
        return f"Offer inventory file is corrupt: {e}. Try `@Scout refresh offers`."

    active = [o for o in all_offers if o.get("status") == "Active"]
    if not active:
        return "Offer inventory has 0 active offers. Try `@Scout refresh offers` to fetch latest."

    ch = _get_ch_client()

    # Resolve publisher
    pub_results = _q.publisher_lookup_by_name(ch, publisher_name)
    if not pub_results:
        return f"Publisher '{publisher_name}' not found in MomentScience."
    pub_id  = pub_results[0]["id"]
    pub_org = pub_results[0]["organization"]

    # Pull top-converting categories for this publisher from ClickHouse.
    # Used downstream to re-rank offers by audience fit, not just RPM.
    # Non-fatal — falls back to RPM-only ranking if query fails.
    top_categories: list[str] = _q.publisher_top_categories(ch, pub_id)

    # Existing advertiser set for this publisher (active campaigns only, lowercase)
    existing_adv: set[str] = _q.publisher_existing_advertisers(ch, pub_id)

    # Filter to net-new advertisers (not already provisioned for this publisher)
    def _is_new(offer: dict) -> bool:
        adv = (offer.get("advertiser") or "").lower()
        return not any(adv in ex or ex in adv for ex in existing_adv)

    candidates = [o for o in active if _is_new(o)]

    if not candidates:
        return (
            f"All active affiliate offers in the inventory are already provisioned in {pub_org}. "
            f"Use `@Scout revenue opportunities` for cross-publisher advertiser gaps, "
            f"or `@Scout what advertisers aren't in {pub_org}` for the provisioning view."
        )

    # Score using existing benchmark infrastructure
    benchmarks = _load_performance_benchmarks()
    scored_all = [(o, _scout_score(o, benchmarks)) for o in candidates]

    # Split: confirmed rate (score > 0, has real payout) vs uncontracted (Rate TBD)
    confirmed = [(o, s) for o, s in scored_all if s > 0]
    confirmed.sort(key=lambda x: x[1], reverse=True)
    confirmed_top = confirmed[:8]

    # Uncontracted = active, net-new, no payout confirmed yet — show top 5 by category fit
    uncontracted = [
        o for o, s in scored_all if s == 0
        and (o.get("_raw_payout") or "").lower() in ("rate tbd", "tbd", "", "?")
    ]
    uncontracted = uncontracted[:5]

    if not confirmed_top and not uncontracted:
        return (
            f"{len(candidates)} net-new affiliate offers found for {pub_org}, "
            f"but none have MS benchmark data yet (no prior run history). "
            f"Try `@Scout revenue opportunities` for cross-publisher revenue gaps with proven estimates."
        )

    _NETWORK_EMOJI = {"impact": "⚡", "maxbounty": "💰", "flexoffers": "🔗", "cj": "🌐"}

    # Surface category signals so the model can reason about audience fit
    category_signal = ""
    if top_categories:
        category_signal = f"\n*📊  Top-Converting Categories on {pub_org} (last 6mo):* {', '.join(top_categories)}"

    lines = [
        f"*🎯  {pub_org} — Offer Recommendations*",
        f"_{len(candidates)} net-new candidates screened · {len(confirmed_top)} with confirmed rates · "
        f"{len(uncontracted)} uncontracted_",
        category_signal,
        "",
    ]

    # ── Section 1: Confirmed rates — ranked by Scout Score ────────────────────
    if confirmed_top:
        lines.append("*✅  Confirmed Rate — Ready to Pitch*")
        lines.append("─" * 32)
        for i, (o, score) in enumerate(confirmed_top, 1):
            advertiser  = o.get("advertiser") or "Unknown"
            raw_payout  = o.get("_raw_payout") or o.get("payout") or "?"
            category    = o.get("category") or "Uncategorized"
            geo         = (o.get("geo") or "").strip()
            network     = o.get("network") or ""
            net_emoji   = _NETWORK_EMOJI.get(network.lower(), "•")
            net_label   = network.title()
            geo_str     = f" · {geo}" if geo and geo.lower() not in ("us", "usa", "united states") else " · US"
            lines.append(
                f"*{i}. {advertiser}*   {net_emoji} {net_label}\n"
                f"   `{raw_payout}` · {category}{geo_str} · est. *${score:.2f} RPM*"
            )
        lines.append("")

    # ── Section 2: Uncontracted — apply to unlock ─────────────────────────────
    if uncontracted:
        lines.append("*🔍  Uncontracted — Apply to Unlock Rate*")
        lines.append(
            "_These are in the affiliate network's marketplace but need a contract "
            "before they can be pitched. Once approved, the daily scrape picks up the rate._"
        )
        lines.append("─" * 32)
        for o in uncontracted:
            advertiser = o.get("advertiser") or "Unknown"
            category   = o.get("category") or "Uncategorized"
            geo        = (o.get("geo") or "").strip()
            network    = o.get("network") or ""
            net_emoji  = _NETWORK_EMOJI.get(network.lower(), "•")
            geo_str    = f" · {geo}" if geo and geo.lower() not in ("us", "usa", "united states") else " · US"
            lines.append(f"• *{advertiser}*   {net_emoji} {network.title()} · {category}{geo_str}")
        lines.append("")

    # ── Footer CTA ────────────────────────────────────────────────────────────
    if confirmed_top:
        top_name = confirmed_top[0][0].get("advertiser") or "top offer"
        lines.append(f":zap:  `@Scout brief {top_name}` to build a campaign brief · "
                     f"`@Scout brief [name]` for any other offer above")
    else:
        lines.append(":zap:  Apply for contracts in Impact's publisher portal to unlock rates — "
                     "Scout will pick them up automatically on the next daily scrape.")

    return {
        "pub_name": pub_org,
        "total_candidates": len(candidates),
        "confirmed_count": len(confirmed_top),
        "uncontracted_count": len(uncontracted),
        "category_signals": top_categories,
        "offers": _format_offers([o for o, _ in confirmed_top], benchmarks),
        "uncontracted": [
            {
                "advertiser": o.get("advertiser") or "Unknown",
                "network":    o.get("network") or "",
                "category":   o.get("category") or "Uncategorized",
                "geo":        (o.get("geo") or "").strip(),
            }
            for o in uncontracted
        ],
        "summary": "\n".join(l for l in lines if l),
    }


def get_exposure_rate_anomalies(
    min_impressions_7d: int = None,
    min_payout: float = None,
    drop_pct: float = None,
) -> dict:
    """
    Find publisher-campaign pairs where yesterday's exposure CVR dropped vs. 7d baseline.

    Exposure CVR = conversions / impressions (exposure rate, NOT canonical CVR).
    Canonical CVR = conversions / clicks. These are different metrics:
    - exposure_cvr measures what fraction of ad impressions led to a conversion.
    - canonical CVR measures what fraction of clicks led to a conversion.
    This tool uses exposure_cvr intentionally for anomaly detection signal quality.

    Threshold defaults come from scout_thresholds.json; caller can override per-call.
    """
    try:
        ch = _get_ch_client()
        rows = _query_cvr_anomaly(
            ch,
            drop_pct=drop_pct,
            min_payout=min_payout,
            min_impressions_7d=min_impressions_7d,
        )
        if not rows:
            return {"anomalies": [], "count": 0, "summary": "No exposure CVR anomalies detected."}
        return {
            "anomalies": rows,
            "count": len(rows),
            "summary": f"{len(rows)} publisher-campaign pair(s) with significant exposure CVR drops.",
        }
    except Exception as e:
        log.exception("get_exposure_rate_anomalies failed")
        return {"error": str(e), "anomalies": []}


def _format_fleet_health(result: dict, days: int) -> str:
    """Render fleet health result as Slack mrkdwn. Called by get_publisher_fleet_health."""
    lines = [f"*Publisher Fleet Health — last {days} day{'s' if days != 1 else ''}*"]

    if result.get("insufficient_history"):
        lines.append(
            "_⚠️ Not enough history yet — need at least 3 weeks of data at $500+/week._"
        )
        return "\n".join(lines)

    total_gap = result.get("total_gap", 0.0)
    total = result.get("total_publishers", 0)
    act_now = result.get("act_now", [])
    watch = result.get("watch", [])
    healthy = result.get("healthy_top5", [])
    as_of = (result.get("as_of", "") or "")[:10]
    affected = len(act_now) + len(watch)

    # ── Headline ─────────────────────────────────────────────────────────
    if result.get("platform_alarm"):
        lines.append("")
        lines.append(
            f":rotating_light: *{len(act_now)} publishers in freefall simultaneously* — "
            "likely a platform-wide tracking issue, not individual publisher problems. "
            "Check ClickHouse connectivity and pixel/postback health first."
        )
    elif total_gap > 0:
        pub_word = "publisher" if affected == 1 else "publishers"
        lines.append(
            f":red_circle: *${total_gap:,.0f} behind baseline* across {affected} {pub_word}"
        )
    else:
        lines.append(":large_green_circle: All publishers tracking on or above baseline")

    def _pub_line(pub: dict) -> str:
        tier = f" [T{pub['tier']}]" if pub.get("tier") else ""
        sigma = pub["sigma_score"]
        sigma_str = f"{abs(sigma):.1f}σ" if sigma != -99.0 else "∞σ"
        return (
            f"• {pub['publisher_name']}{tier} · "
            f"*${pub['dollar_gap']:,.0f} gap* · "
            f"{pub['delta_pct']:+.1f}% · "
            f"_{sigma_str} below normal_"
        )

    # ── Act Now ──────────────────────────────────────────────────────────
    if act_now:
        lines.append("")
        label = "publisher" if len(act_now) == 1 else "publishers"
        lines.append(f":rotating_light: *Act Now* ({len(act_now)} {label})")
        for pub in act_now:
            lines.append(_pub_line(pub))

    # ── Watch ────────────────────────────────────────────────────────────
    if watch:
        lines.append("")
        label = "publisher" if len(watch) == 1 else "publishers"
        lines.append(f":eyes: *Watch* ({len(watch)} {label})")
        for pub in watch:
            lines.append(_pub_line(pub))

    if not act_now and not watch:
        lines.append("")
        lines.append(":white_check_mark: No publishers need attention this week")

    # ── Healthy compact line ──────────────────────────────────────────────
    if healthy:
        lines.append("")
        gains = " · ".join(
            f"{p['publisher_name']} +${p['revenue_actual'] - p['revenue_expected']:,.0f}"
            for p in healthy[:3]
        )
        extra = f" · +{len(healthy) - 3} more" if len(healthy) > 3 else ""
        lines.append(f":white_check_mark: *Healthy* — {gains}{extra}")

    # ── Action line ───────────────────────────────────────────────────────
    if act_now and total_gap > 0:
        top = act_now[0]
        tier_str = f" [T{top['tier']}]" if top.get("tier") else ""
        pct_of_gap = top["dollar_gap"] / total_gap * 100 if total_gap > 0 else 0
        lines.append(
            f":zap: *{top['publisher_name']}{tier_str}* is the top priority — "
            f"${top['dollar_gap']:,.0f} gap ({pct_of_gap:.0f}% of total shortfall)."
        )
    elif watch:
        lines.append(
            f":zap: *Action:* Investigate {watch[0]['publisher_name']} — "
            f"${watch[0]['dollar_gap']:,.0f} gap."
        )

    # ── Footer ────────────────────────────────────────────────────────────
    lines.append(f"_{total} publishers tracked · as of {as_of}_")
    return "\n".join(lines)


def get_publisher_fleet_health(
    days: int = 7,
) -> dict:
    """
    Fleet-level publisher health using σ-based statistical baseline.

    Rolling 4-week same-period average as baseline. Only surfaces publishers
    with >= $500/week baseline AND >= 1.5σ below normal — separates real
    signal from normal variance.

    Classifies:
    - Act Now: >= 2σ below normal AND >= $500 dollar gap
    - Watch:   >= 1.5σ below normal AND >= $200 dollar gap
    - Healthy: on or above baseline (top 3 gains shown)
    - Platform alarm: > 5 Act Now publishers simultaneously

    Returns formatted Slack text in the 'formatted' key.
    """
    try:
        days = max(1, min(90, int(days)))

        from queries import get_publisher_fleet_health_data
        ch = _get_ch_client()
        result = get_publisher_fleet_health_data(ch, days=days)

        return {"formatted": _format_fleet_health(result, days)}

    except Exception:
        log.exception("get_publisher_fleet_health failed")
        return {
            "formatted": "⚠️ Fleet health unavailable — query failed. Try again or check ClickHouse.",
        }
