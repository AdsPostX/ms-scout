---
phase: 09-audit-debt
plan: 01
status: DONE
commit: a1fb2cb
---

## What shipped

Added `_build_alert_body(items, action_footer="")` helper in `scout_bot.py` after
`_alert_blocks()` and updated all 6 `_format_*_alert()` functions to call it.

## Verification

- `python3 -c "import ast; ast.parse(...)"` → OK
- `grep -c "_build_alert_body" scout_bot.py` → 7 (1 def + 6 calls)
- `grep "full_body\|action_footer"` → 0 lines outside `_build_alert_body` itself
- `python3 smoke_test.py` → ALL PASS

## Stats

- 1 file modified: `scout_bot.py`
- Net: -7 lines (13 added, 20 deleted)

## Scope respected

- `_format_revenue_alert()` — untouched (Card() directly, CRITICAL severity)
- `_post_watchdog_alert()` — untouched (raw blocks, different pattern)
- All other files — untouched
