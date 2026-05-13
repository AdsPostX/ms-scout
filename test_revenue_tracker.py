"""
Manual test for the PR 25 revenue tracker.

Run from the worktree root:
  python3 test_revenue_tracker.py

Posts to #bot-qa (never to #revenue-operations — SCOUT_ENV isn't set to production).

Two modes:
  1. Real Phase 1 check against live ClickHouse — shows what would fire today
  2. Force-fire with a mock soft-day total — always posts the full alert format
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from scout_agent import _get_ch_client, _query_intraday_revenue_total, _query_intraday_revenue_by_publisher
from scout_bot import _format_revenue_alert, _route_channel
from slack_sdk import WebClient

web = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
ch  = _get_ch_client()
channel = _route_channel("revenue")  # → #bot-qa in dev

print(f"\nPosting to channel: {channel}")

# ── Mode 1: Real check ────────────────────────────────────────────────────────
print("\n[Phase 1] Checking today's revenue against 8-week DOW baseline...")
total = _query_intraday_revenue_total(ch)

if total is None:
    print("✅ Revenue is on pace — Phase 1 returned None (no anomaly). No alert would fire.")
    print(f"   (To force a test post anyway, the script continues with a mock total below.)\n")
else:
    print(f"🔴 Phase 1 tripped: {total['pct_of_expected']:.1f}% of expected ({total['weekday']})")
    print(f"   Today so far: ${total['today_revenue']:,.0f} | Projected: ${total['projected_full_day']:,.0f} | Expected: ${total['dow_median']:,.0f}")
    publishers = _query_intraday_revenue_by_publisher(ch, total)
    msg = _format_revenue_alert(total, publishers)
    print("\n── Alert message preview ──────────────────────────────")
    print(msg)
    print("───────────────────────────────────────────────────────\n")
    web.chat_postMessage(channel=channel, text=msg)
    print(f"✅ Posted real alert to {channel}")
    sys.exit(0)

# ── Mode 2: Force-fire with mock soft-day total ───────────────────────────────
print("[Force] Running Phase 2 with a mock soft-day total to test the full alert path...")

import datetime, pytz
today = datetime.datetime.now(pytz.timezone("America/Chicago"))
weekday = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"][today.weekday()]

mock_total = {
    "today_revenue":      14200.0,
    "projected_full_day": 20300.0,
    "dow_median":         28500.0,
    "pct_of_expected":    71.2,
    "weekday":            weekday,
    "sample_days":        8,
}

print(f"Mock: ${mock_total['today_revenue']:,.0f} today → projects to ${mock_total['projected_full_day']:,.0f} vs ${mock_total['dow_median']:,.0f} expected ({weekday})")
print("Running Phase 2 against live ClickHouse for real publisher breakdown...")

publishers = _query_intraday_revenue_by_publisher(ch, mock_total)
print(f"Phase 2 returned {len(publishers)} publishers above delta threshold.\n")

msg = _format_revenue_alert(mock_total, publishers)
print("── Alert message preview ──────────────────────────────")
print(msg)
print("───────────────────────────────────────────────────────\n")

web.chat_postMessage(channel=channel, text=msg)
print(f"✅ Posted mock alert to {channel}")
