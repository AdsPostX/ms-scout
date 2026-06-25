---
phase: 12-alert-interactivity
plan: 03
status: COMPLETE
---

## Delivered

- `scout_handlers.py` — `_SNOOZE_DURATIONS` module-level config dict; `_handle_acknowledge` (in-place card update via `chat_update`); `_handle_snooze_open` (modal with 4 duration options); `_handle_snooze_submit` (view submission handler); `_extract_view_submission_context`; `_handle_view_submission`; `_VIEW_SUBMISSION_DISPATCH`; `view_submission` intercept branch in `_handle_event_impl` before `_handle_block_action`; `"scout_acknowledge"` + `"scout_snooze_open"` entries in `_BLOCK_ACTION_DISPATCH`
- `scout_ui_kit.py` — `_refire_context_block(snoozed_by, snoozed_at_iso)` pure function; `_alert_status_chip_blocks(status, actor_id, display_time)` pure function
- `demand_feed_main.py` — UC-1 `_should_post` snooze-suppress pattern applied at both mark_firing sites (revenue_tracker + monitor loop); re-fire context block prepended on expired snooze; `clear_snooze()` called on re-fire; shadow-tick guard preserved (snooze check only on `not is_shadow_tick`)
- `smoke_test.py` — 3 new guards (+3 over 12-02 baseline of 158): acknowledge in dispatch, snooze in dispatch, `_refire_context_block` purity

## Acceptance Criteria

| AC | Result |
|----|--------|
| AC-1: Acknowledge replaces buttons with status chip in-place | PASS — `chat_update` with `_alert_status_chip_blocks(acknowledged)` |
| AC-2: Snooze opens duration-picker modal | PASS — `views_open` with 4 options, callback_id=scout_snooze_submit |
| AC-3: Snooze submission writes state and updates card | PASS — `snooze_alert` + `chat_update` with snooze chip |
| AC-4: Re-fire context block shows snooze history | PASS — `_refire_context_block` prepended at both demand_feed sites |
| AC-5: `_refire_context_block` is a pure function | PASS — no API calls, identical output on repeat call |
| AC-6: Handlers degrade gracefully when post state missing | PASS — `get_post_state` None → `chat_postEphemeral` fallback, no exception |
| AC-7: Smoke tests pass | PASS — 161/161 ALL PASS |

## Deferred

None. All scope completed as planned.
