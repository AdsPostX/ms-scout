---
phase: 12-alert-interactivity
plan: 04
status: COMPLETE
---

## Delivered

- `queries_revenue.py` — `get_publisher_drill_summary(pub_id: str)` with output shape comment; queries `adpx_conversionsdetails` joined to `mv_adpx_users` (pub name) and `from_airbyte_campaigns` (adv name); groups by day + adv_name; Python aggregation for 7d/yesterday totals and top_offer; zero-row case returns valid shape; creates its own CH client via `_get_ch_client()`
- `scout_ui_kit.py` — three pure modal builders: `_drill_loading_modal()`, `_drill_data_modal(summary)`, `_drill_error_modal()`; `_drill_data_modal` uses local `_fmt` helper for `$X.XK` formatting
- `scout_handlers.py` — `_handle_drill_publisher` (loading modal first via `views_open`, then daemon thread calls `views_update` with data or error modal); `"scout_drill_publisher"` entry in `_BLOCK_ACTION_DISPATCH`
- `smoke_test.py` — 2 new guards (+2 over 12-03 baseline of 161): drill publisher in dispatch, `_drill_loading_modal` purity

## Acceptance Criteria

| AC | Result |
|----|--------|
| AC-1: Drill button triggers loading modal immediately | PASS — `views_open` called before any ClickHouse query; daemon thread handles query |
| AC-2: Data modal shows correct fields when query succeeds | PASS — `_drill_data_modal` renders pub_name, 7d rev/conv, yesterday rev/conv, top offer, as_of |
| AC-3: Error state shown when query fails | PASS — daemon thread catches all exceptions, calls `views_update` with `_drill_error_modal()` |
| AC-4: get_publisher_drill_summary defines output shape before SQL | PASS — shape comment at function top before SQL string |
| AC-5: Drill handler in _BLOCK_ACTION_DISPATCH | PASS — `"scout_drill_publisher": _handle_drill_publisher` wired |
| AC-6: Smoke tests pass | PASS — 163/163 ALL PASS |

## Deferred

None. All scope completed as planned.
p95 gate: resolved — loading modal pattern makes latency irrelevant.
