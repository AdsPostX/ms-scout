---
phase: 11-block-kit-visual-upgrade
plan: 01
subsystem: ui
tags: [slack, block-kit, rich_text, markdown, scout_ui_kit]

requires:
  - phase: 01-wrap-response-hierarchy
    provides: wrap_response() + Card/Severity/Surface primitives this plan extends

provides:
  - Native markdown block body rendering behind SCOUT_MARKDOWN_BLOCKS flag
  - rich_text_quote for blockquote lines
  - strike style for ~~text~~
  - ordered rich_text_list for numbered lists
  - Multi-line blockquote buffering (single rich_text_quote per consecutive > block)
  - _MARKDOWN_SIGNALS routing for blockquote-only and ordered-list-only bodies

affects: [11-02, 12-alert-interactivity]

tech-stack:
  added: []
  patterns:
    - Feature-flag pattern for Slack API adoption (SCOUT_MARKDOWN_BLOCKS)
    - Signal-tuple routing (_MARKDOWN_SIGNALS) for fallback parser gating

key-files:
  modified: [scout_ui_kit.py, tests/test_text_to_blocks.py]

key-decisions:
  - "language field omitted from rich_text_preformatted — not in Slack's documented schema (Codex review finding)"
  - "fence_lang variable removed entirely — dead code after language emission dropped"
  - "_MARKDOWN_SIGNALS extended with '> ' and '1. ' to route blockquote/ordered-list bodies through _text_to_blocks() when flag is off"

patterns-established:
  - "_MARKDOWN_SIGNALS tuple gates which Card bodies reach _text_to_blocks() when SCOUT_MARKDOWN_BLOCKS=false"

duration: ~3 sessions
started: 2026-06-13T00:00:00Z
completed: 2026-06-14T00:00:00Z
---

# Phase 11 Plan 01: Markdown Block + Rich Text Spec Gaps Summary

**Native `markdown` block shipped to production behind `SCOUT_MARKDOWN_BLOCKS=true`; four rich_text fallback spec gaps closed; Slack visual confirmation done.**

## Performance

| Metric | Value |
|--------|-------|
| Duration | ~3 sessions |
| Completed | 2026-06-14 |
| Tasks | 3 completed |
| Files modified | 2 |
| Commits | 3 |

## Acceptance Criteria Results

| Criterion | Status | Notes |
|-----------|--------|-------|
| AC-1: Markdown block in production | Pass | `SCOUT_MARKDOWN_BLOCKS=true` in Render; visual confirmation from screenshot |
| AC-2: Blockquotes → rich_text_quote | Pass | Multi-line blockquotes buffered into single element |
| AC-3: Strikethrough → strike style | Pass | `~~text~~` emits `{"style": {"strike": true}}` |
| AC-4: Language field on preformatted | Deviated | Plan specified emitting `language` field; Codex review found it absent from Slack's schema — field is parsed but NOT emitted. Second gherkin (no key on plain fences) still passes. |
| AC-5: Ordered lists → ordered rich_text_list | Pass | `1. 2. 3.` → `rich_text_list` with `style: ordered`; number prefix stripped |
| AC-6: All tests pass | Pass | smoke_test.py green; test_text_to_blocks.py covers all new paths |

## Task Commits

| Task | Commit | Description |
|------|--------|-------------|
| Task 1+2: markdown block + 4 spec gaps | `fade756` | feat(ui_kit): adopt markdown block + close 4 rich_text spec gaps |
| Fix: multi-line blockquote buffer | `e14d2e2` | fix(ui_kit): buffer multi-line blockquotes, drop dead ctx_lines |
| Fix: Codex P2 findings + dead code | `684e504` | fix(ui_kit): drop dead fence_lang, add > and 1. to _MARKDOWN_SIGNALS, omit language |

## Files Modified

| File | Change |
|------|--------|
| `scout_ui_kit.py` | `_markdown_block()` added; `_MARKDOWN_BLOCKS_ENABLED` flag wired into `wrap_response()`; `_MARKDOWN_SIGNALS` extended; `rich_text_quote`, strike, ordered list, multi-line blockquote added to `_text_to_blocks()` |
| `tests/test_text_to_blocks.py` | `TestRichTextSpecGaps` class added; `TestMarkdownSignalRouting` class added; `test_fenced_code_block_with_language` updated to assert absence of `language` field |

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Drop `language` from `rich_text_preformatted` | Codex independent review found the field absent from Slack's documented Block Kit schema. Emitting undocumented fields is brittle. Deviated from AC-4 first gherkin intentionally. |
| Remove `fence_lang` variable entirely | After dropping emission, the variable was set-and-reset with no reader — pure dead code. Removed in UNIFY cleanup. |
| Extend `_MARKDOWN_SIGNALS` with `"> "` and `"1. "` | Blockquote-only and ordered-list-only bodies were silently falling through to the plain `mrkdwn` section path when the flag is off. Added to ensure routing correctness regardless of flag state. |

## Deviations from Plan

### Summary

| Type | Count |
|------|-------|
| Auto-fixed | 2 (multi-line blockquote buffer, `_MARKDOWN_SIGNALS` gap) |
| Codex-driven correction | 1 (`language` field — plan was wrong about Slack's schema) |
| UNIFY cleanup | 1 (`fence_lang` dead variable removed) |

### Detail

**1. Multi-line blockquote buffer** — discovered during code review: consecutive `> ` lines each flushed a separate `rich_text_quote` instead of merging. Fixed in `e14d2e2` with a `quote_buf` accumulator, same pattern as bullet list handling.

**2. `_MARKDOWN_SIGNALS` gap** — Codex P2 finding: `"> "` and `"1. "` missing from the signal tuple. Blockquote-only and ordered-list-only Card bodies bypassed `_text_to_blocks()` when `SCOUT_MARKDOWN_BLOCKS=false`. Fixed in `684e504`.

**3. `language` field omission** — Plan's AC-4 specified emitting `{"language": "python"}` on `rich_text_preformatted`. Codex review found this field is not in Slack's schema. Corrected: field is parsed, not emitted. Test updated to assert absence.

## Next Phase Readiness

**Ready:**
- `wrap_response()` body path is clean and flag-gated — Phase 11-02 agent blocks can add their own block types alongside it
- `_text_to_blocks()` fallback is now spec-correct for all common markdown patterns — safe rollback path confirmed

**Concerns:**
- Phase 11-02 (agent blocks) adds `SCOUT_AGENT_BLOCKS` flag — two feature flags in production; verify Render env var management doesn't become a maintenance burden

**Blockers:** None
