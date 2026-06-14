## Current Position

Milestone: block-kit-visual-upgrade + alert-interactivity
Phase: 11 of 12 (block-kit-visual-upgrade) — 11-02 PLAN created
Plan: 11-02-PLAN.md ready for APPLY (agent blocks + annotate_reasoning_steps tool)
Status: PLAN created. Awaiting approval then APPLY.
Last activity: 2026-06-14 — 11-02-PLAN.md written; 3 tasks + 1 checkpoint; SCOUT_AGENT_BLOCKS flag + AgentStep + _agent_plan_block() + annotate_reasoning_steps tool

Progress:
- Phase 11: [████░░░░░░] 33% (11-01 done; 11-02 PLAN ready; 11-02 APPLY next)

## Loop Position

Current loop state:
```
PLAN ──▶ APPLY ──▶ UNIFY
  ✓        ○        ○     [11-02 PLAN created, awaiting APPLY]
```

## Session Continuity

Last session: 2026-06-14
Stopped at: 11-02 PLAN written. PAUL plan-phase workflow complete.
Next action: Approve plan → run /paul:apply .paul/phases/11-block-kit-visual-upgrade/11-02-PLAN.md
Resume file: .paul/phases/11-block-kit-visual-upgrade/11-02-PLAN.md

## Prior Context

Phase 09 (audit-debt) 09-01 PR open — UNIFY for that phase = PR merge. Independent of this phase.
Phase 10 (command-registry) ✅ DONE.

## Phase 12 Note

Phase 12 (alert-interactivity) added to ROADMAP on 2026-06-13 after JTBD lens analysis revealed
the entire Phase 11 roadmap is visual upgrades but zero loop-closure. Key insight: the feedback
button handler in scout_handlers.py already demonstrates the block_actions plumbing — acknowledge
and snooze are the same pattern. Scope: acknowledge button + snooze 4h on MONITOR_ALARM, publisher
drill modal (gated on ClickHouse p95 latency). Phase 12 is independent of Phase 11; can start after
11-01 ships or run concurrently once the handler architecture is stable.

## Completed Phases

| Phase | Name | Status | PRs |
|---|---|---|---|
| 01 | wrap-response-hierarchy | ✅ DONE | #223, #235 |
| 06 | maint-hardening | ✅ DONE | #249 |
| 07 | loading-ux | ✅ DONE | #248, #250 |
| 08 | routing-refactor | ✅ DONE | #251, #252 |
| 10 | command-registry | ✅ DONE | — |
| 11-01 | block-kit-visual-upgrade (markdown block + spec gaps) | ✅ DONE | #285 |

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
| block_actions dispatch architecture (Phase 12) | `_ACTION_HANDLERS` dict in `scout_handlers.py` | Replaces current if-chain; borrowed from Vamsee's redis_listener.ts entity router. Pre-work: refactor existing feedback routing before adding Phase 12 handlers. |
| `_text_to_blocks()` long-term fate | Keep as fallback; evaluate deletion after 30 days in prod | When SCOUT_MARKDOWN_BLOCKS=true has been stable in production for 30 days, evaluate removing the custom parser. Not a Phase 11 decision. |
