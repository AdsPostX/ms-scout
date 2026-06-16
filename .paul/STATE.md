## Current Position

Milestone: block-kit-visual-upgrade + alert-interactivity
Phase: 12 of 12 (alert-interactivity) — not started
Plan: Not started
Status: Ready to plan Phase 12
Last activity: 2026-06-16 — Phase 11 complete (2/2 plans), UNIFY ✓, transitioned to Phase 12

Progress:
- Phase 11: [██████████] 100% ✅ DONE
- Phase 12: [░░░░░░░░░░] 0% (alert-interactivity — not started)

## Loop Position

Current loop state:
```
PLAN ──▶ APPLY ──▶ UNIFY
  ✓        ✓        ✓     [Phase 11 complete — ready for Phase 12 PLAN]
```

## Session Continuity

Last session: 2026-06-16
Stopped at: Phase 11 complete. Both plans done, all tests green (290 pytest + 155 smoke), UNIFY closed.
Next action: /paul:plan for Phase 12 (alert-interactivity — acknowledge + snooze on MONITOR_ALARM, publisher drill modal)
Resume file: .paul/phases/11-block-kit-visual-upgrade/11-02-SUMMARY.md

## Prior Context

Phase 09 (audit-debt) 09-01 PR open — UNIFY for that phase = PR merge. Independent of this phase.
Phase 10 (command-registry) ✅ DONE.

## Phase 12 Note

Phase 12 (alert-interactivity): acknowledge + snooze on MONITOR_ALARM, publisher drill modal (gated on ClickHouse p95 latency).

**Updated starting point (Phase 11-02 changed this):** The feedback if-chain in scout_handlers.py
is fully deleted — no _FEEDBACK_LOG, no _build_feedback_buttons, no _handle_feedback. Phase 12
starts with a cleaner handler dispatch than the original note assumed. The `_ACTION_HANDLERS` dict
pattern is still the right architecture, but there is NO pre-existing if-chain to refactor first —
write it fresh. `_build_modal_view` is available for acknowledge/snooze modals.

## Completed Phases

| Phase | Name | Status | PRs |
|---|---|---|---|
| 01 | wrap-response-hierarchy | ✅ DONE | #223, #235 |
| 06 | maint-hardening | ✅ DONE | #249 |
| 07 | loading-ux | ✅ DONE | #248, #250 |
| 08 | routing-refactor | ✅ DONE | #251, #252 |
| 10 | command-registry | ✅ DONE | — |
| 11-01 | block-kit-visual-upgrade (markdown block + spec gaps) | ✅ DONE | #285 |
| 11-02 | block-kit-visual-upgrade (parser removal + helpers + feedback removal) | ✅ DONE | #290 |

## Decisions

| Decision | Chosen | Reason |
|---|---|---|
| State storage | Keep file-based | Redis deferred; persistent disk works |
| Scope (phase 06) | Gate + startup log only | Don't over-engineer; close the two known gaps |
| Routing approach (phase 08) | Remove `_ROUTE_KEYWORDS` + `pre_formatted`, keep `_classify_intent` | LLM handles intent better than keyword table; narrowing tool set is the right abstraction |
| set_threshold regex | Keep | Structured command parsing, not intent routing |
| markdown block rollout | Feature-flagged SCOUT_MARKDOWN_BLOCKS | Build now, ship to any bot; partner blocks gated separately |
| Agent blocks | Feature-flagged SCOUT_AGENT_BLOCKS | Standard public Block Kit blocks (confirmed Block Kit Builder); feature-flag for rollout control only |
| Interactivity scope (Phase 12) | Acknowledge + snooze + drill modal only | Closes read→act loop; highest JTBD value. Feedback buttons already exist; plumbing proven. Drill modal gated on ClickHouse p95 latency. |
| Drill modal approach | Async pre-fetch OR loading modal pattern | trigger_id expires in 3s; ClickHouse query can't block the acknowledgment. Decide in 12-01-PLAN.md. |
| block_actions dispatch architecture (Phase 12) | `_ACTION_HANDLERS` dict in `scout_handlers.py` | Write fresh — feedback routing already deleted in Phase 11-02; no pre-existing if-chain to refactor |
| `_text_to_blocks()` fate | Deleted in Phase 11-02 | 322-line parser removed; native markdown block is the production path |
| Feedback system fate | Removed entirely in Phase 11-02 | _FEEDBACK_LOG, _build_feedback_buttons, _handle_feedback all deleted; no migration |
