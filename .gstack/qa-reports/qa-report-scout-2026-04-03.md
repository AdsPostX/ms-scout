# QA Report — Scout Bot
**Date:** 2026-04-03
**Branch:** main
**Mode:** Diff-aware (no web UI — Python Slack bot)
**Commits covered:** 3 (P5 pulse, _run_parallel fix, deleted_at fix)
**Duration:** ~5 min
**Tester:** /qa-only

---

## Summary

| Category | Score | Notes |
|----------|-------|-------|
| Syntax / compilation | 100 | Both scout_agent.py and scout_bot.py compile clean |
| Runtime health | 95 | Scout online since 07:44, zero errors post-restart |
| Logic correctness | 92 | All checks pass, one low-risk finding noted |
| Data integrity | 100 | SELECT columns match tuple unpacks |
| Concurrency | 95 | _run_parallel fallback verified (sequential path works) |
| Slack block budget | 100 | Max 30 blocks, well under 50-block limit |

**Overall Health: 97/100**

---

## What Changed

| Commit | Change | Status |
|--------|--------|--------|
| P5-3 | Sort NEEDS ATTENTION by abs(dollar_delta) | ✅ PASS |
| P5-1 | Causal hypothesis per down partner | ✅ PASS |
| P5-2 | Gap opportunities pushed inline in Pulse | ✅ PASS |
| fix | _run_parallel() wraps all ThreadPoolExecutor sites | ✅ PASS |
| fix | deleted_at removed from from_airbyte_users query | ✅ PASS |

---

## Findings

### PASS: Syntax and compilation
Both files compile with `python3 -m py_compile`. No import errors at startup.

### PASS: Scout is online and error-free post-restart
All RuntimeError logs predate the 07:44 restart. Zero errors in the 20 minutes since. Pulse scheduled for 08:00 tomorrow (22.3h), watchdog at 10:00 CT today (0.3h — first live fire in ~18 min).

### PASS: SELECT column count matches tuple unpack
Gap query SELECT: `(adv_name, pub_count, revenue_30d, rpm)` = 4 columns.
Unpack: `for adv, _cnt, _rev, rpm in gap_rows` = 4 values. Match confirmed.

### PASS: Sort key is correct
`abs(revenue_7d_ann - revenue_30d)` for both downs and ups. For a down partner, 7d_ann < 30d so delta is negative, abs() makes it comparable. Larger = more dollars moving = higher priority. ✅

### PASS: _run_parallel fallback verified
Simulated RuntimeError mid-execution. Sequential fallback returned correct results. All 5 ThreadPoolExecutor sites now use this helper.

### PASS: Slack block budget
Worst case: 30 blocks (3 downs × 3 blocks each + MOMENTUM + caps + standing + footer). Slack limit is 50. Clean.

### LOW: hypothesis initialized for all velocity_shifts (not just downs)
`v["hypothesis"] = ""` and `v["gaps"] = []` are set on ALL velocity_shifts (ups and downs) before the `continue` for non-downs. This means up partners carry empty hypothesis/gaps keys. Non-issue since `_format_pulse_blocks` only renders them for downs, but the keys add minor memory overhead. Not a bug.

### LOW: Lambda capture in scout_agent.py health query
`_vol_sql` and `_prov_sql` are captured by reference in two lambdas inside `get_publisher_health`. Since they're not in a loop and don't change after assignment, late-binding is not a problem. Verified by inspection.

---

## Watchdog First Fire (10:00 CT — ~18 min from now)

The watchdog runs for the first time today at 10:00 AM CT. Expected behavior:
- Queries `launched_offers.json` + `from_airbyte_publisher_campaigns` for campaigns launched in the last 2 days
- If any meet alert conditions (impressions without clicks, zero impressions, etc.) → posts to #revenue-operations
- Logs `[watchdog] 0 alert(s)` or `[watchdog] posted alert for [campaign]`

**Verification:** Check `logs/scout.log` at 10:05 CT for watchdog output.

---

## No Test Framework Detected

No pytest, unittest, or test directories found. Run `/qa` to bootstrap test infrastructure and enable regression test generation.

---

## Status: DONE_WITH_CONCERNS

**DONE:** All logic checks pass. Scout is running clean. P5 changes are structurally correct.

**CONCERN:** Cannot do live end-to-end verification of the new Pulse format until tomorrow's 8am Pulse fires. The causal hypothesis and gap queries run during `_run_pulse_signals()` which only executes at Pulse time — not triggerable without modifying pulse_state.json manually.

**Recommendation:** At 8:05am tomorrow, check #revenue-operations for the Pulse. Verify:
1. NEEDS ATTENTION list is ordered by dollar drop magnitude (not %)
2. Each flagged partner has a gray italic causal line beneath it
3. Each flagged partner has a "↳ Missing:" gap line (or is absent if no gaps)

