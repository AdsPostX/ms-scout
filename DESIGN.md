# Scout — First-Principles Redesign

Six independent investigations (nomenclature, routing/dispatch, module dependency graph,
documentation, test architecture, output/config discipline) ran in parallel against the
current codebase. This document is the synthesis: what a staff/principal engineer would
actually change, in what order, and why.

**Status: Phases 1 and 2 are shipped** (this PR) — the docs truth pass and the 12
dead-function deletion, both independently verified by an `/codex review` pass on this
diff.

**The remaining phases below (v3) were substantially revised after a `/codex consult`
pass against this plan** — not a diff review this time, a review of the plan itself, with
Codex reading the actual source files to check the plan's claims rather than trusting its
prose. Unlike the Phase 1-2 diff review (4 minor corrections), this pass found the
v2 Phase 3-7 design had real structural problems: a "unified `Route` registry"
that conflated daemon threads with one-shot Slack actions, a package move that called
itself "mechanical" while quietly requiring real code extraction, wrong hoist targets for
shared infra, and no rollback story for a repo-wide import refactor. Two of Codex's most
consequential claims were independently spot-checked against source before accepting them:
`_INTERNAL_TOOLS` genuinely exists and filters `TOOLS` (`scout_agent.py:1703,1721`) — a
registry can't just replace `TOOL_MAP` and ignore it — and the `scout_suggestion*`/
`home_try_query*` prefix dispatch is real (`scout_handlers.py:2067,2073`). Both held up.
Every finding got a verdict (agree/fix, and how) rather than blanket acceptance — 20 points
in total, all landed in the sections below. The sections below are the corrected v3 design.

## Self-audit against `lenses.json` (v2 correction)

The first pass of this document skipped two lenses `~/.claude/lenses.json` registers for
exactly this kind of task, and both skips produced real defects, corrected below:

- **`coding.md` rule 12** ("verify a capability doesn't already exist before planning new
  work") — not run. `scout_core/` (`contracts.py`, `monitors.py`, `job_runs.py`) already
  exists as a **cross-repo shared package between `ms-scout` and a separate `ms-demand-feed`
  service** — its own docstrings state "Shared by ms-scout (scout_bot.py) and ms-demand-feed
  (demand_feed_main.py)." V1 of this design proposed a parallel `scout/domain/`, `scout/data/`
  tree that would have competed with an already-established shared boundary instead of
  extending it. Fixed in the package structure below.
- **`coding.md`'s Subagent Discipline rule 3** ("gate subagent output before treating it as
  ground truth") — closed. A second wave of 4 independent verification agents re-derived
  every major claim directly from source (ast-diffs, exact grep counts, load-order tracing)
  rather than re-reading the first reports. Result: 1 finding refuted (the "second query
  layer" was actually a documented delegation pattern), 2 corrected (circular-import framing
  was overstated; two dispatch-mechanism file attributions were wrong), 1 came back worse
  than reported (raw-block bypass rate is 56%, not "a third"), and the duplicate-function
  finding was confirmed with an important addition the first pass missed (all 12 copies are
  dead code, not "drifted" live behavior — see The Honest Scorecard for the full breakdown).
- **`anti-AI.md`'s Generalizing Principle** ("the flattened, most-expected answer on any
  axis with real variation is the tell") — the v1 package layout (`data/domain/slack/agent/
  routing/entrypoint`) was generic tech-tier layering, the textbook "clean architecture"
  answer, not derived from Scout's actual bounded contexts. Scout already encodes real
  domain boundaries — the `queries_*.py` split, `scout_core/contracts.py`'s named pipeline
  stages (scraped → normalized → digest candidate → queue draft → campaign request) — and
  the redesign below organizes around those instead of around generic layers.
- `design.md` (UI/UX lens) correctly doesn't apply — no UI decision here. Commercial Framing
  correctly doesn't apply either — CLAUDE.md's own exception covers "pure infra... no
  meaningful revenue path," which this refactor is.

## The honest scorecard

`VAMSEE_AUDIT.md` claims "43/43 tools audited, 24/24 violations fixed, 100% COMPLETE" under
a self-graded checklist, filed under a real engineer's name, never actually reviewed by him
(previously flagged). The fleet's job here was to check whether that confidence was earned
elsewhere in the codebase. It largely wasn't — but every finding below has now been through
an independent second-pass verification (direct diff/grep against source, not re-trusting
the first report), and **one finding didn't survive it**:

1. ~~A second, drifted query layer~~ — **REFUTED on verification.** `scout_ch.py`'s
   `_query_*` functions are thin delegation wrappers (they resolve threshold config, then
   call the bare-noun `queries_*.py` function) — a documented single-source-of-truth
   pattern, not a competing implementation. Confirmed by reading the actual bodies:
   `_query_ghost_campaigns` (scout_ch.py:176) is 3 lines that end in
   `return _q.ghost_campaigns(ch, recency_hours=recency_hours, ...)`. This is fine as-is
   and drops out of the plan.
2. **12 duplicated functions** between `scout_agent.py` and `scout_tools_offers.py`,
   beyond the 4 KNOWN_DEBT.md already tracks — **confirmed via `ast`-diff of every pair**:
   4 byte-identical (`run_offer_scraper`, `_format_payout`, `_network_portal_url`,
   `_validated_tracking_url`), 8 behaviorally drifted. The verification found something the
   first pass missed and understates by calling it "drift": **none of the 12
   `scout_tools_offers.py` copies are ever called in production** — `TOOL_MAP` binds every
   one to the `scout_agent.py` definition, so the `scout_tools_offers.py` side is dead code
   (only referenced by one test, `tests/test_demand_feed_http.py`, exercising it directly).
   6 of the 8 "drifted" functions route through **two separate benchmark-cache
   implementations** (`scout_agent.py`'s `_get_benchmarks()` vs. `scout_tools_offers.py`'s
   `ThresholdManager.benchmarks()`) that can independently go stale — inert today because
   the dead copies are never called, but a real landmine if anyone ever wires them live
   without reconciling. Because it's dead code, the fix is simpler than first framed: delete
   the 12 `scout_tools_offers.py` copies outright and repoint the one test, not a careful
   behavior-reconciliation exercise.
3. **Two structural, currently-dormant circular-import couplings** (`scout_agent.py` ↔
   `scout_bot.py`, `scout_agent.py` ↔ `scout_digest.py`) — **citations confirmed exactly**
   (13 lazy imports in `scout_bot.py` at the claimed lines, the "avoids circular dep"
   comments verbatim in `scout_digest.py`), but the "live cycle" framing was overstated on
   verification: neither cycle actually breaks anything today — the lazy/function-local
   imports are a correct, working defense, not a bug in production. What's real: moving any
   one of these lazy imports to the top of its file would produce an immediate, reproducible
   `ImportError` on `import scout_agent` (verified by tracing exact load order and confirming
   which symbol wouldn't exist yet). So these are dormant landmines a future contributor
   could trigger by "cleaning up" an import, not an active bug — worth fixing before the
   package move, not urgently before anything else.
4. **~8 different dispatch mechanisms**, count confirmed on verification, but **two location
   errors got corrected**: the scheduled monitor-polling mechanism lives in
   `demand_feed_main.py` (an ordered list of `(daemon_fn, name)` tuples, one thread each —
   `demand_feed_main.py:690-698`), not `scout_bot.py` as first reported, and it's closer to a
   lightweight registry than "hardcoded, no registry." The `_FORCE_MONITOR_FNS` dict itself
   is defined in `scout_handlers.py:168`, not `scout_bot.py` (only its population calls are
   in `scout_bot.py:2044-2050`, which was correct). The consolidation case still holds — 8
   real, differently-shaped mechanisms is still true — but the `Route` registry design below
   now targets the corrected file locations.
5. **Response-pattern enforcement is opt-in, not structural** — confirmed exactly as
   reported (`wrap_response()`'s own docstring: "Callers that omit pattern= are unaffected").
   One correction: the raw-block problem is **worse than first reported**. Independent count
   across `scout_handlers.py`: 13 raw `blocks=[...]` sites vs. 10 `wrap_response()` calls —
   **56% of Slack-output call sites bypass the pattern check**, not "roughly a third."
6. **"Never surface unverified data silently" has zero enforcement mechanism** — confirmed
   exactly. `Card`'s full field list is `severity, headline, body, facts, actions,
   chart_url` — no verification/provenance field of any kind.

Plus the already-known: `VAMSEE_AUDIT.md` self-contradicts (claims complete, its own table
shows half the domains queued); `smoke_test.py` is 220 hand-rolled tests in one 6,154-line
file with zero test references to `queries_monitor.py`, `queries_publisher.py`,
`demand_feed_main.py`, `scout_notion.py`, `scout_images.py`, `scout_telemetry.py`, and a
live-API test sitting uncommented next to 200+ pure unit tests on every deploy gate.

None of this is "vibe-coded crap" in the pejorative sense — the individual fixes in
KNOWN_DEBT.md are careful, well-reasoned, and correctly scoped. The problem is structural:
each fix solved its instance of a pattern without building the mechanism that would catch
the *next* instance. That's the gap this design closes.

## Target package structure (v3 — corrected after `/codex` plan review)

A `/codex` consult pass reviewed Phases 3-7 against the actual source files (not just the
plan's prose) and found the v2 package/registry design had real feasibility problems, not
just wording issues. Full findings and per-point verdicts are the record of what changed;
summary of what's different in v3:

- **No `scout/slack/` catch-all.** Direct read confirmed `scout_state.py` is cross-domain
  persistence infra ("All JSON state I/O for Scout" — its own docstring) and `scout_ch.py`
  is explicitly "ClickHouse client infrastructure + backward-compat wrappers" (its own file
  header) — neither is presentation code. Both were wrongly bucketed under `slack/` in v2.
- **New `scout/shared/` bucket** for genuine cross-cutting infrastructure: the Anthropic
  client singleton (`_get_anthropic_client` — belonged in neither `offers/` nor
  `monitoring/`, the v2 hoist targets were wrong), `scout_ch.py`'s client construction
  (`_get_ch_client`, `CHBusyError`), and `scout_state.py`. Domain-driven layouts always need
  a shared-kernel bucket for things no single domain owns — v2 omitted it.
- **`scout_tools_offers.py` is NOT uniformly canonical.** Phase 2 already proved canonicality
  runs in different directions per function (5 functions: `scout_tools_offers.py` canonical,
  `scout_agent.py` imports; the 12 just deleted: the reverse was true). Phase 5 now requires
  a verification sub-step — map which file holds the live implementation, per function —
  before writing `scout/offers/`, not an assumption baked into the plan.
- **`notion.py`** moves near `scout/monitoring/` (pipeline health / Scout Demand Queue),
  not `slack/` — it's workflow logic that happens to emit Slack-adjacent formatting.

```
scout_core/        [UNCHANGED — cross-repo contract, ms-scout + ms-demand-feed]
  contracts.py, monitors.py, job_runs.py

scout/
  shared/         _get_anthropic_client (from scout_agent.py), scout_ch.py's client
                  construction (_get_ch_client, CHBusyError — SQL stays in domain
                  packages), scout_state.py — genuine cross-cutting infra, owned by no
                  single domain
  offers/         offer_scraper.py; canonical offer-tool implementations — WHICH file
                  (scout_agent.py vs scout_tools_offers.py) is canonical per function
                  gets verified at move time, not assumed; offer-shaped slice of
                  scout_types.py
  publishers/     queries_publisher.py → fetch_*(ch, ...), its _query_* config wrapper
                  moved in alongside it
  campaigns/      queries_campaign.py → fetch_*(ch, ...); demand_feed_main.py's
                  campaign-creation logic EXTRACTED here (a real refactor — see
                  Sequencing 5b, not a mechanical move)
  revenue/        queries_revenue.py → fetch_*(ch, ...)
  monitoring/     queries_monitor.py → fetch_*(ch, ...), alert_registry.py,
                  scout_thresholds.py (ThresholdManager — kept as-is), notion.py
                  (pipeline/queue workflow, not presentation)
  slack/          ui_kit.py, slack_safe.py, image_resolve.py, images.py — presentation
                  only, nothing persistence-shaped
  agent/          agent.py (tool dispatch — imports downward into shared/offers/
                  publishers/campaigns/revenue/monitoring/slack; never lazily reaches
                  into bot/digest)
  routing/        events.py (was scout_handlers.py), attachments.py
  entrypoint/     bot.py, demand_feed_main.py, digest.py
tests/
  test_shared/, test_offers/, test_publishers/, test_campaigns/, test_revenue/,
  test_monitoring/, test_slack/, test_agent/, test_routing/, test_entrypoint/
  integration/    test_ask_tool_call.py, test_nl_routing.py (was nl_query_test.py)
docs/
  ARCHITECTURE.md, ENGINEERING_STANDARDS.md, DEPLOY.md
  archive/        VAMSEE_AUDIT.md (superseded — see below)
```

The two circular-import cycles are resolved by construction: `_get_anthropic_client` moves
to `scout/shared/` (not `offers/` or `monitoring/` — v2's hoist targets were wrong, per the
review), `_scout_score`/`_network_portal_url` move to `scout/offers/`. `agent/` becomes a
true leaf-consumer, never a hub — and cross-repo-shared logic (the hourly monitor loop,
job-run telemetry) stays in `scout_core/` rather than getting pulled into `scout/monitoring/`
and orphaned from `ms-demand-feed`.

**Cross-repo risk, named honestly:** `ms-demand-feed` is a separate repo this worktree
cannot inspect. Before Phase 5 touches `demand_feed_main.py` or anything `scout_core/`
exports, someone needs to check that repo for what it actually imports beyond
`scout_core/` — an open external dependency this plan cannot verify from inside `ms-scout`
alone.

**Rollback strategy (new, was missing in v2):** Phase 5 ships backward-compat shim modules
at every old top-level import path (e.g. `scout_ch.py` re-exporting from
`scout.shared.clickhouse`) for at least one deploy cycle, so a Render rollback to the prior
commit doesn't hit an `ImportError`, and any external script referencing an old path has a
migration window instead of an atomic break.

## Naming conventions (applied during the move, not a separate pass)

- Drop the `scout_` file prefix — with 20+ of 28 files sharing it, it distinguishes
  nothing; the package (`scout.offers`, `scout.publishers`, `scout.slack`, ...) now does
  that job.
- One bare-query naming convention, not two: `fetch_<noun>(ch, ...)` for the SQL-executing
  function in each domain package. `scout_ch.py`'s `_query_*` wrappers keep their prefix and
  move alongside — they're the config-resolution boundary, verified as a real pattern, not
  a duplicate to delete (see The Honest Scorecard).
- Disambiguate the three "what should we chase" tools: `get_top_opportunities` →
  `get_unrun_offer_opportunities`, `get_top_revenue_opportunities` →
  `get_publisher_revenue_gaps`, `get_supply_demand_gaps` → `get_advertiser_supply_gaps`.
- `get_queue_status` / `get_demand_queue_status` — rename the second to
  `get_ms_platform_queue_status` so the distinction is in the name, not a docstring caveat.
- `why_entity_note` → `explain_entity_note` (verb-consistency with `record_`/`forget_`).
- `nl_query_test.py` → `tests/integration/test_nl_routing.py` — it was never a smoke test
  by its own docstring's admission ("Do NOT add to startup smoke test"); the name was lying.
- `queries.py` (5-line re-export shim) — delete if nothing depends on the wildcard import
  (grep first), otherwise rename to `queries_compat.py` so its purpose is legible.

## Routing architecture (v3 — unified registry killed, per `/codex` review)

v2 proposed a single `Route` object spanning all 8 dispatch mechanisms behind one decorator
API. Review verdict: **wrong.** LLM tools, one-shot Slack block actions, slash commands, and
scheduled daemon threads are different contracts — different lifetimes, different failure
modes, different definitions of "complete." A tool call returns a value; a monitor is an
infinite daemon factory that never returns. Forcing both behind `@route.monitor(...)` hides
that difference instead of fixing it. This was the same category error `anti-AI.md`'s
Generalizing Principle already caught once this session (the v1 generic-tech-tier package
layout) — made again one layer up, at the routing level. Two further problems compounded it:
`TOOL_MAP` is only half the LLM tool surface (`_INTERNAL_TOOLS` filters `TOOLS` at
`scout_agent.py:1703,1721` — spot-checked directly, holds up — a registry that ignores it can
silently desync visibility), and import-time decorator auto-registration would force eager
cross-service imports, reintroducing the exact circular-import risk Phase 3 exists to remove
(`demand_feed_main.py` must not eagerly load Slack-heavy bot modules).

**Corrected design: separate, type-specific registries, unified later only if their
contracts prove genuinely identical — not as a stated goal.**

- **`ToolRegistry`** (lives in `scout/agent/`) — one entry per tool capturing schema
  (`TOOLS`), handler (`TOOL_MAP`), visibility (`_INTERNAL_TOOLS`), and intent hints as a
  single object per tool, not a bare handler dict. `route.assert_all_tools_have_intent_hints()`
  replaces the manual "SYSTEM_PROMPT needs a numbered line per tool" audit; the numbered list
  itself can be *generated* from intent hints instead of hand-maintained prose.
- **`BlockActionRegistry`** (lives in `scout/routing/`) — exact-key dict PLUS the existing
  ordered prefix-predicate fallback (`scout_suggestion*`, `home_try_query*` — confirmed real
  at `scout_handlers.py:2067,2073`, spot-checked directly). Its own
  `assert_all_actions_handled()`.
- **`SlashCommandRegistry`** (lives in `scout/routing/`) — internal consistency only (every
  registered command has a local handler). Cannot and does not claim to verify the Slack app
  manifest at api.slack.com/apps stays in sync — that's a manual step, already documented in
  CLAUDE.md's Slash Commands table, not a new gap this creates.
- **Two separate `MonitorRegistry` instances**, one per service (`scout_bot.py`'s force-run
  path, `demand_feed_main.py`'s scheduled daemon list) — populated by explicit `.register()`
  calls at each service's own natural import point, never by a cross-service auto-discovery
  scan. They stay separate because a daemon factory and a one-shot force-run callable are not
  the same primitive; collapsing them was v2's mistake.
- **`ExtractorRegistry`** (lives in `scout/routing/attachments.py`) — the existing ordered
  predicate-cascade shape, unchanged; it already works and isn't part of the problem.

Each registry gets its own simple, type-specific completeness check instead of one
polymorphic `assert_complete()` trying to mean five different things.

## Output/config: make policy structural (v3 — rescoped per `/codex` review)

v2 proposed a required `verified: bool` field on `Card` and a single global `ScoutConfig`.
Review verdict: both too blunt.

- **`Card.verified` — rejected as a `Card` field.** Loading states, modals, help cards, and
  self-QA status aren't "verified/unverified data" responses; forcing a value onto every
  `Card` construction creates either fake defaults or a bypass API nobody uses correctly.
  **Corrected:** validation moves into `wrap_response()`, conditional on `pattern` — only
  `ResponsePattern.ANSWER`/`STATUS` carrying a `facts` tuple derived from a live query
  require a `verified`/`source` marker, passed as a `wrap_response()` kwarg, not a `Card`
  field. Same conditional-validation shape `wrap_response()` already uses for surface/pattern
  mismatch — extending an existing mechanism, not adding a new blanket requirement.
- **Mandatory `pattern=` needs a migration model first, not a flag flip.** Sequence: (a)
  enumerate the missing pattern/surface pairs for `Surface.MODAL`/`HOME`/ephemeral that the
  current `ResponsePattern` enum doesn't cover, (b) migrate call sites to use them, (c) only
  then make `pattern=` required. Doing (c) before (a)/(b) either breaks every raw call site at
  once or forces meaningless patterns onto UI chrome that isn't a data response.
- **The raw-`blocks=` count isn't proof of bypass on its own.** 13 raw sites vs. 10
  `wrap_response()` calls in `scout_handlers.py` (56%) is a real number, but some of those 13
  are legitimate modal scaffolding and loading placeholders, not policy-relevant data
  responses. **Before migrating any of them**, classify each of the 13 individually: queue-card
  post (line 639), App Home modal states (1558, 1607, 1636, 1665, 1685, 1695 — 1648 already
  goes through `wrap_response`), self-QA runner (2919, 2960), plus 1388/2209/2294/3276 — is
  this a real data response, or UI chrome that doesn't carry "verified/unverified" meaning?
  Only the former gets migrated.
- **`ScoutConfig` — rejected as one global dataclass.** Config dataclasses already exist,
  scattered across `scout_bot.py`, `scout_handlers.py`, and `demand_feed_main.py` — the real
  problem is fragmented *ownership*, not absence of config objects. A single cross-service
  dataclass would recreate exactly the coupling `scout_core/`'s deliberate minimalism already
  avoids between `ms-scout` and `ms-demand-feed`. **Corrected:** extend each existing
  per-service config object with its own scattered `os.getenv()` reads, using
  `ThresholdManager`'s pattern (constructor-validated, one load) — one config object per
  service boundary, not one object spanning both.

## Test architecture (v3 — sequencing and metric both corrected per `/codex` review)

v2 sequenced test migration *after* the package move ("mirrors the fresh package layout from
step 5"). Review verdict: backwards — that means the single biggest diff in the whole plan
happens while only the 220-test monolith (with its own known blind spots) protects it, not a
real per-module suite. **Corrected: write pytest coverage against the CURRENT flat-file
locations first**, verify it passes, then the package move (step 5) becomes a mechanical
import-path rename in the same PR as tests that already pass — the biggest diff is never
left unprotected.

- Migrate `smoke_test.py`'s 220 tests to pytest, organized by *current* module — one file per
  module, so the current silent zero-coverage gap (`queries_monitor`, `queries_publisher`,
  `demand_feed_main`, `scout_notion`, `scout_images`, `scout_telemetry`) becomes visible
  instead of invisible, **before** any file moves.
- Tag every test that hits live ClickHouse/Anthropic/Slack with `@pytest.mark.integration`
  (starting with `test_ask_tool_call`, which currently makes a live Anthropic + ClickHouse
  call inside the deploy gate). Render's redeploy check runs `pytest -m "not integration"`
  only — fast, deterministic, no API cost or flakiness in the gate.
- **Coverage metric corrected:** "zero test references" (a grep-based existence check) was
  flagged as weak — it rewards a meaningless import, not real coverage. Use actual
  `coverage.py` line/branch thresholds per package, or a small number of named contract tests
  per module (e.g. "every `fetch_*`/`_query_*` function has at least one test that mocks `ch`
  and asserts the SQL shape or return shape") — an honest floor, not a grep pretending to be
  a real bar.

## Documentation set

| File | Fate |
|---|---|
| `README.md` | Keep — already good. Add a pointer to `docs/DEPLOY.md` once that file exists (see below — not yet created). |
| `CLAUDE.md` | Keep as agent-operating instructions — that's genuinely what it's for. |
| `docs/ARCHITECTURE.md` | **Shipped.** The missing "why" doc: service topology, concurrency model (semaphore, retry pools), why response patterns are enforced structurally, data flow. Pulls rationale currently scattered across `.claude/rules/*.md` scar tissue and `KNOWN_DEBT.md` narration into one durable reference. |
| `docs/DEPLOY.md` | **Still pending** — not created in Phase 1 (caught by `/codex review`: this table originally claimed it as shipped). Extract README's Render section + env var table; add rollback/monitoring notes. Do this as part of Phase 3 or its own small doc PR. |
| `docs/ENGINEERING_STANDARDS.md` | **Shipped.** The 5 engineering-quality checks from `VAMSEE_AUDIT.md` (invisible accumulators, no-op side-channels, repeated inline patterns, config objects, validate-at-construction) — the durable part of that file, kept; the session-log part, retired. |
| `FEATURES.md` | Keep, but CI-enforced as fully generated — fails if stale relative to the `Route` registry. |
| `CHANGELOG.md` | **Shipped.** RESOLVED entries moved here from `KNOWN_DEBT.md`, dated by PR where recorded. |
| `KNOWN_DEBT.md` | **Shipped** — pruned to open items only. |
| `VAMSEE_AUDIT.md` | **Shipped** — archived to `docs/archive/`; durable content moved to `ENGINEERING_STANDARDS.md`. All root-level references to the old path (`CLAUDE.md`, `FEATURES.md`, `scripts/generate_feature_map.py`) updated too — caught by `/codex review`, not the original pass. |
| `PLAN.md` | Left untouched — it's live, git-tracked work-in-progress (attachment ingestion), not scratch. |
| `.claude/rules/*.md` | Keep as-is — correctly scoped to incident-anchored invariants, not architecture. |

Also close the docstring gap on `scout_bot.py` (0/7 public functions documented) — it's the
file its own rules file flags as having the trickiest concurrency constraints in the repo.

## Sequencing (one PR = one concern, per existing repo discipline)

This is explicitly NOT a big-bang rewrite — each phase ships independently and the
codebase stays green throughout:

1. ✅ **Shipped.** Docs + naming truth pass (no code moves): archived `VAMSEE_AUDIT.md`,
   wrote `docs/ARCHITECTURE.md` and `docs/ENGINEERING_STANDARDS.md`, pruned `KNOWN_DEBT.md`,
   created `CHANGELOG.md`. Reviewed independently via `/codex review`, which caught 2 gaps
   the original pass missed (stale `VAMSEE_AUDIT.md` references in `CLAUDE.md`/`FEATURES.md`/
   `scripts/generate_feature_map.py`; a `KNOWN_DEBT.md` entry that was substantively resolved
   but never moved to `CHANGELOG.md`) — both fixed in the same PR before merge.
2. ✅ **Shipped.** Deleted the 12 dead `scout_tools_offers.py` function copies and repointed
   `tests/test_demand_feed_http.py` (the only caller of that side) at `scout_agent.py`'s
   copies. `/codex review` independently confirmed the 12 copies are unreferenced anywhere
   else in the repo and that the retargeted tests cover the same 4 behaviors as before.
**Steps 3-7 below are v3 — re-sequenced after the `/codex` plan review found the v2 order
left the biggest diff (the package move) untested and bundled a real refactor into what it
called "mechanical."**

3. **Circular-import hoist.** Move `_get_anthropic_client` to `scout/shared/` (not
   `offers/`/`monitoring/` — v2's hoist targets were wrong per review) and
   `_scout_score`/`_network_portal_url` to `scout/offers/`, so neither
   `scout_agent.py`↔`scout_bot.py` nor `scout_agent.py`↔`scout_digest.py` needs a
   function-local import. Not urgent (confirmed dormant, not live), but a prerequisite for
   the package move.
4. **Structural output/config fixes, rescoped.** `wrap_response()`-conditional
   `verified`/`source` validation (not a `Card` field), the pattern-taxonomy migration for
   MODAL/HOME/ephemeral before making `pattern=` required, per-service config objects (not
   one global `ScoutConfig`) — see Output/config above for the corrected design. Independent
   of the package move; lands against familiar file paths first.
5. **Write pytest coverage against current file locations FIRST** (re-sequenced — was step
   6, run after the package move; review found that backwards). One file per current module,
   closing the zero-coverage gap named in Test Architecture, verified green before touching
   any file paths.
6. **Package restructure**, split in two because review found "mechanical" was hiding a real
   refactor:
   - **6a — pure moves.** Files that need zero logic changes (`queries_*.py` → their domain
     packages, `scout_ui_kit.py`/`scout_slack_safe.py`/etc. → `scout/slack/`) — a mechanical
     rename, done with the passing tests from step 5 updated to the new import paths in the
     same PR, so coverage is never lost mid-move.
   - **6b — real extraction.** `demand_feed_main.py`'s embedded campaign-creation and
     revenue-tracking logic gets pulled into `scout/campaigns/` and `scout/revenue/` as an
     actual refactor, its own PR, reviewed as a behavior change — not bundled with 6a.
     `scout_tools_offers.py` vs. `scout_agent.py` canonicality gets verified per-function
     before either file is moved (see Target package structure above).
   - **Ships with backward-compat shim modules** at every old top-level import path for at
     least one deploy cycle (new — v2 had no rollback story for a repo-wide import refactor).
   - **Blocked on one external check:** whether `ms-demand-feed` imports anything from
     `ms-scout` beyond `scout_core/` — cannot be verified from inside this repo, needs a
     look at that repo before 6b executes.
7. **Per-type dispatch registries**, one at a time, each its own PR — `ToolRegistry` first
   (lowest cross-service risk), `BlockActionRegistry` and `SlashCommandRegistry` next, the
   two `MonitorRegistry` instances last (spans `scout_handlers.py` and `demand_feed_main.py`
   — corrected file attribution — the highest-risk, most cross-service-entangled one).
   **No unified `Route` object** — v2's single registry conflated daemon threads with
   one-shot Slack actions with LLM tools; killed per review, replaced with five
   purpose-built registries that unify later only if their contracts prove genuinely
   identical, not as a stated goal. Backed by real test coverage from step 5/6, not the
   220-test monolith's blind spots.

## What this buys

Today: 46% of all commits are fix/revert/hotfix/incident. The target isn't zero — it's a
codebase where the next new tool, button, or monitor can't silently skip a wiring step,
where "unverified data" is a type error, not a hope, and where a self-audit can't claim
100% while its own table says otherwise. That's the actual definition of "world-class" here
— not fewer lines of code, but fewer ways to get it wrong without the system telling you.
