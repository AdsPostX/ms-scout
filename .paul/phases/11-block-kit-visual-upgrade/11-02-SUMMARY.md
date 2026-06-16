---
phase: 11-block-kit-visual-upgrade
plan: 02
subsystem: ui
tags: [slack, block-kit, scout_ui_kit, scout_handlers, smoke_test]

requires:
  - phase: 11-01
    provides: markdown block + rich_text spec gaps closed; _MARKDOWN_BLOCKS_ENABLED flag

provides:
  - Native Slack plan block for agent reasoning chain
  - Surface-aware body rendering (_MESSAGE_SURFACES)
  - Dead markdown parser deleted (-322 lines)
  - Block Kit helper library (_build_modal_view, _slack_card_block, _carousel_block, _render_subheader, _build_maintenance_home_view)
  - Entire feedback/suggestion routing system removed (-354 lines net)
  - All stale tests repaired (test_text_to_blocks deleted, test_agent_blocks + test_kit_lint + test_trust_reset updated)

affects: Phase 12 (alert-interactivity) — feedback plumbing is now fully gone; Phase 12 must not reference _FEEDBACK_LOG, _build_feedback_buttons, _handle_feedback

tech-stack:
  added: []
  patterns:
    - Surface-aware rendering via _MESSAGE_SURFACES frozenset
    - Module-top constant dict for status→Block Kit status mapping
    - _build_modal_view as single constructor for all modal views
    - _render_subheader for all header block construction

key-files:
  modified:
    - scout_ui_kit.py
    - scout_handlers.py
    - scout_bot.py
    - smoke_test.py
    - tests/test_kit_lint.py
    - tests/test_agent_blocks.py
    - tests/test_trust_reset.py
  deleted:
    - tests/test_text_to_blocks.py

key-decisions:
  - "Delete parser now: _text_to_blocks deleted; native markdown block is the production path"
  - "Feedback removed entirely: no migration, no gradual deprecation — clean cut"
  - "_STATUS_TO_PLAN hoisted to module top (Vamsee: no per-call dict construction)"
  - "_build_modal_view requires callback_id — validates at construction (Vamsee: validate at construction)"

patterns-established:
  - "All modal view construction goes through _build_modal_view() — enforced by test"
  - "All header blocks go through _render_subheader() — consistent level field"
  - "Surface-aware rendering: _MESSAGE_SURFACES determines native markdown vs section"

duration: ~3h
started: 2026-06-16T00:00:00Z
completed: 2026-06-16T09:45:00Z
---

# Phase 11 Plan 02: Block Kit Refactor — Parser Removal + Helpers + Feedback Removal Summary

**Deleted 322-line dead markdown parser, added 5 Block Kit helpers, removed entire feedback routing system (-354 lines net), repaired 4 stale test files. 290 pytest + 155 smoke, all pass.**

## Performance

| Metric | Value |
|--------|-------|
| Duration | ~3h |
| Commits | 7 |
| Files modified | 10 |
| Net lines removed | ~460 |
| Tests after | 290 pytest + 155 smoke (all pass) |

## Acceptance Criteria Results

| Criterion | Status | Notes |
|-----------|--------|-------|
| Dead parser deleted | Pass | `_text_to_blocks`, `_parse_inline_elements`, `_escape_md_code`, `_MARKDOWN_SIGNALS`, 9 related constants — 322 lines |
| Surface-aware body | Pass | `_render_body(card, surface)` uses `_MESSAGE_SURFACES` frozenset |
| Native plan block | Pass | `_agent_plan_block` returns `{"type": "plan", ...}` — confirmed in smoke tests |
| Helpers added | Pass | `_build_modal_view`, `_slack_card_block`, `_carousel_block`, `_render_subheader`, `_build_maintenance_home_view` |
| Modal migration | Pass | 6 inline modal dicts in scout_handlers.py → `_build_modal_view()` |
| Feedback system removed | Pass | Button dispatch, reaction handler, correction capture, `_retry_with_hint` — all deleted |
| Double-enforce bug fixed | Pass | `_build_home_view` single `enforce()` call confirmed by smoke test |
| Stale tests repaired | Pass | 290 pytest pass; test_text_to_blocks.py deleted; test_agent_blocks + test_kit_lint + test_trust_reset updated |

## Vamsee Lens

| Check | Result |
|-------|--------|
| No invisible accumulators | ✅ All block lists returned as values |
| No no-op side-channel capture | ✅ All helpers return dicts/lists directly |
| No repeated inline comprehensions | ✅ 6 modal dicts → `_build_modal_view()`; 2 maintenance views → `_build_maintenance_home_view()` |
| Config objects, not scattered env reads | ✅ `_MARKDOWN_BLOCKS_ENABLED`, `_MESSAGE_SURFACES`, `_STATUS_TO_PLAN` at module top |
| Validate at construction | ✅ `_build_modal_view` raises `ValueError` for empty `callback_id` |

## Task Commits

| Task | Commit | Description |
|------|--------|-------------|
| Extract private renderers | `0c1b9f1` | Extract private renderers from wrap_response, remove dead flag |
| Phase 2 — parser deletion | `2ef7270` | Delete dead parser, surface-aware body, native plan block |
| Phase 3A — helpers + feedback removal | `0cbac70` | Block Kit helpers, remove feedback system |
| Phase 3B+3C — modal migration + tests | `27c637a` | _build_modal_view migration, smoke tests |
| Phase 4 — maintenance view + headers | `58bb561` | _build_maintenance_home_view, _render_subheader migration |
| test_kit_lint repair | `56877b1` | Repair test_kit_lint after feedback removal and Phase 2 |
| Dead test removal + repair | `ee1346c` | Delete test_text_to_blocks, repair agent_blocks and trust_reset |

## Files Created/Modified

| File | Change | Net |
|------|--------|-----|
| `scout_ui_kit.py` | Modified | -322 lines (parser) + helpers added |
| `scout_handlers.py` | Modified | -189 lines (feedback) + modal migration |
| `scout_bot.py` | Modified | -2 lines (feedback call sites) |
| `smoke_test.py` | Modified | +13 tests (Phase 3A helpers, double-enforce, maintenance view) |
| `tests/test_kit_lint.py` | Modified | Removed deprecated feedback= params, updated fenced code test |
| `tests/test_agent_blocks.py` | Modified | Updated plan block assertions (section→plan type) |
| `tests/test_trust_reset.py` | Modified | Deleted TestFeedbackHelpers class |
| `tests/test_text_to_blocks.py` | Deleted | Tested functions no longer exist |

## Deviations from Plan

| Deviation | Why | Impact |
|-----------|-----|--------|
| Feedback system removed entirely (not in original 11-02 plan) | User directed; no migration value — clean cut | Phase 12 must not reference deleted feedback symbols |
| `_data_table` block builder not built | Audit found no mrkdwn text tables in codebase; no use case | None — skip was correct |
| 4 stale test files needed repair | Prior phase deletions weren't propagated to tests | 290 pytest now pass (were failing at collection) |

## Next Phase Readiness

**Ready:**
- `_build_modal_view` available for Phase 12 acknowledge/snooze modals
- `_slack_card_block` + `_carousel_block` available for demand queue card views
- Feedback routing fully removed — Phase 12 starts clean
- 290 pytest + 155 smoke all green

**Concerns:**
- Phase 12 note in STATE.md references `block_actions dispatch architecture` using `_ACTION_HANDLERS` dict replacing current feedback if-chain — that if-chain no longer exists. Phase 12 plan should account for this: the starting point is cleaner than the note suggests.

**Blockers:** None

---
*Phase: 11-block-kit-visual-upgrade, Plan: 02*
*Completed: 2026-06-16*
