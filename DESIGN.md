# Scout — First-Principles Redesign

Six independent investigations (nomenclature, routing/dispatch, module dependency graph,
documentation, test architecture, output/config discipline) ran in parallel against the
current codebase. This document is the synthesis: what a staff/principal engineer would
actually change, in what order, and why. No code has been touched yet — this is the design,
pending approval to execute against `KNOWN_DEBT.md` and the active `PLAN.md` work.

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

## Target package structure

Flat 28-file layout → packaged by **domain**, not by generic tech-tier. `scout_core/`
already exists as the correct cross-repo shared boundary (`ms-scout` ↔ `ms-demand-feed`) —
it is extended, not duplicated. Domain boundaries come from what's already real in the
codebase: the `queries_*.py` split and `scout_core/contracts.py`'s named pipeline stages
(scraped → normalized → digest candidate → queue draft → campaign request):

```
scout_core/        [UNCHANGED — cross-repo contract, ms-scout + ms-demand-feed]
  contracts.py, monitors.py, job_runs.py

scout/
  offers/         offer_scraper.py, scout_tools_offers.py (canonical — scout_agent.py
                  imports from here, never redefines), the offer-shaped slice of
                  scout_types.py
  publishers/     queries_publisher.py → fetch_*(ch, ...) — one convention, not two
                  (scout_ch.py's duplicate _query_* implementations deleted)
  campaigns/      queries_campaign.py → fetch_*(ch, ...), demand_feed_main.py's
                  campaign-creation logic
  revenue/        queries_revenue.py → fetch_*(ch, ...)
  monitoring/     queries_monitor.py → fetch_*(ch, ...), alert_registry.py,
                  scout_thresholds.py (ThresholdManager — already the right pattern,
                  kept as-is)
  slack/          ui_kit.py, slack_safe.py, image_resolve.py, images.py, state.py,
                  notion.py — cross-cutting presentation layer, used by every domain
                  above but depends on none of them
  agent/          agent.py (tool dispatch — imports downward into offers/publishers/
                  campaigns/revenue/monitoring/slack; never lazily reaches into bot/digest)
  routing/        events.py (was scout_handlers.py), attachments.py
  entrypoint/     bot.py, demand_feed_main.py, digest.py
tests/
  test_offers/, test_publishers/, test_campaigns/, test_revenue/, test_monitoring/,
  test_slack/, test_agent/, test_routing/, test_entrypoint/
  integration/    test_ask_tool_call.py, test_nl_routing.py (was nl_query_test.py)
docs/
  ARCHITECTURE.md, ENGINEERING_STANDARDS.md, DEPLOY.md
  archive/        VAMSEE_AUDIT.md (superseded — see below)
```

`scout_ch.py`'s `_query_*` wrappers are **kept, not deleted** — verification refuted the
original "duplicate query layer" finding; they're the config-resolution boundary in front
of each domain package's bare query function, a real and useful seam, not debt. They move
into each domain package alongside the function they wrap (e.g. `_query_ghost_campaigns`
moves into `scout/campaigns/` next to `fetch_ghost_campaigns`). `scout_types.py`'s actual
contents still need one direct read before it's split across `offers/` vs. staying as a
shared cross-domain module — flagging this as an open assumption rather than asserting it.

The two circular-import cycles are resolved by construction: anything `scout_bot.py` or
`scout_digest.py` currently reaches back into `scout_agent.py` for (e.g. `_scout_score`,
`_network_portal_url`, `_get_anthropic_client`) gets hoisted into `scout/offers/` or
`scout/monitoring/` depending on which domain it actually belongs to. `agent/` becomes a
true leaf-consumer, never a hub — and cross-repo-shared logic (the hourly monitor loop,
job-run telemetry) stays in `scout_core/` rather than getting pulled into `scout/monitoring/`
and orphaned from `ms-demand-feed`.

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

## Unified routing architecture

Replace the seven independent mechanisms with one `Route` registry, decorator-based:

```python
@route.tool(name="get_ghost_campaigns", intent_hints=[...])
@route.block_action("scout_acknowledge")
@route.slash_command("/scout-ghost", monitor="ghost")
@route.monitor(name="ghost", schedule_minutes=5, post_channel="offers")
@route.extractor(predicate=_is_pdf)
def get_ghost_campaigns(...): ...
```

One object holds five parallel dicts (tools, block_actions, slash_commands, monitors,
extractors), built from decorator registration at import time. Concretely this:

- Replaces the `TOOL_MAP` literal + the 5 post-hoc `TOOL_MAP["x"] = x` patches.
- Replaces `_FORCE_MONITOR_FNS` (dict defined in `scout_handlers.py:168`, populated from
  `scout_bot.py:2044-2050` — corrected location, verification caught the original report
  attributing the dict itself to the wrong file) and `demand_feed_main.py`'s separate
  `(daemon_fn, name)` tuple list (`demand_feed_main.py:690-698` — the actual scheduled-monitor
  mechanism, not `scout_bot.py` as first reported) with one iteration over `route.monitors`
  — collapsing two mechanisms that live in two different files/services into one.
- Lets the SYSTEM_PROMPT's numbered routing list be *generated* from `intent_hints` instead
  of hand-maintained prose — closes the "line missing for a tool" gap structurally.
- Adds `route.assert_complete()` as a single smoke-test call replacing the scattered
  per-table completeness tests — fails loudly if anything is registered in one place but
  not wired to its Slack-facing counterpart. This is the direct fix for finding #4 above.

## Output/config: make policy structural

- `wrap_response()`: make `pattern=` a required argument (or require an explicit
  `wrap_response_unchecked()` call for the rare cases that don't have one) so omission is a
  loud exception, not silent pass-through.
- Add `verified: bool` (or `source: Literal["clickhouse","cache","partial"]`) as a required
  field on `Card`. `wrap_response()` refuses to emit ANSWER/STATUS patterns when
  `verified is False` unless the body text carries an explicit caveat marker. This is what
  turns "Scout never surfaces unverified data silently" from a CLAUDE.md sentence into a
  guarantee the code enforces the same way surface/pattern mismatch already is.
- Route the remaining raw `blocks=[...]` sites in `scout_handlers.py` through `wrap_response`
  — verified count: **13 raw sites vs. 10 `wrap_response()` calls in that file, 56% bypass**,
  worse than the "roughly a third" first reported. Confirmed raw at queue-card post (line
  639), most of the App Home modal states (1558, 1607, 1636, 1665, 1685, 1695 — one call in
  that range, line 1648, already does go through `wrap_response`), self-QA runner (2919,
  2960), plus 5 more sites (1388, 2209, 2294, 3276) the first pass didn't enumerate. Define
  new MODAL/ephemeral ResponsePattern variants where needed rather than leaving these as
  hand-built exceptions.
- Consolidate `os.getenv()`/`os.environ` (223 occurrences across the repo) into one
  `ScoutConfig` dataclass, constructor-validated like the existing `ThresholdManager` (which
  is already the right pattern — copy it, don't reinvent it). Covers Redis, Notion,
  ClickHouse, and campaign-webhook env vars that currently read ad hoc at 8+ call sites.

## Test architecture

- Migrate `smoke_test.py`'s 220 tests to pytest, split into `tests/` mirroring the new
  package layout — one file per module, so the current silent zero-coverage gap
  (`queries_monitor`, `queries_publisher`, `demand_feed_main`, `scout_notion`,
  `scout_images`, `scout_telemetry`) becomes visible instead of invisible.
- Tag every test that hits live ClickHouse/Anthropic/Slack with `@pytest.mark.integration`
  (starting with `test_ask_tool_call`, which currently makes a live Anthropic + ClickHouse
  call inside the deploy gate). Render's redeploy check runs `pytest -m "not integration"`
  only — fast, deterministic, no API cost or flakiness in the gate.
- Add a coverage-by-module CI check that fails if any `scout/**/*.py` file has zero test
  references — same enforcement pattern as `route.assert_complete()`, applied to tests.

## Documentation set

| File | Fate |
|---|---|
| `README.md` | Keep — already good. Add a pointer to `docs/DEPLOY.md`. |
| `CLAUDE.md` | Keep as agent-operating instructions — that's genuinely what it's for. |
| `docs/ARCHITECTURE.md` | **New.** The missing "why" doc: service topology, concurrency model (semaphore, retry pools), why response patterns are enforced structurally, data flow. Pulls rationale currently scattered across `.claude/rules/*.md` scar tissue and `KNOWN_DEBT.md` narration into one durable reference. |
| `docs/DEPLOY.md` | **New.** Extract README's Render section + env var table; add rollback/monitoring notes. |
| `docs/ENGINEERING_STANDARDS.md` | **New.** The 5 engineering-quality checks from `VAMSEE_AUDIT.md` (invisible accumulators, no-op side-channels, repeated inline patterns, config objects, validate-at-construction) — the durable part of that file, kept; the session-log part, retired. |
| `FEATURES.md` | Keep, but CI-enforced as fully generated — fails if stale relative to the `Route` registry. |
| `CHANGELOG.md` | **New.** RESOLVED entries move here from `KNOWN_DEBT.md`, dated by PR. |
| `KNOWN_DEBT.md` | Keep, but prune to open items only — no more accumulated historical narrative. |
| `VAMSEE_AUDIT.md` | Archive to `docs/archive/` — its durable content moves to `ENGINEERING_STANDARDS.md`; the self-contradictory status claim doesn't survive as root-level "documentation" under someone else's name. |
| `PLAN.md` | Leave untouched — it's live, git-tracked work-in-progress (attachment ingestion), not scratch. |
| `.claude/rules/*.md` | Keep as-is — correctly scoped to incident-anchored invariants, not architecture. |

Also close the docstring gap on `scout_bot.py` (0/7 public functions documented) — it's the
file its own rules file flags as having the trickiest concurrency constraints in the repo.

## Sequencing (one PR = one concern, per existing repo discipline)

This is explicitly NOT a big-bang rewrite — each phase ships independently and the
codebase stays green throughout:

1. **Docs + naming truth pass** (no code moves): archive `VAMSEE_AUDIT.md`, write
   `docs/ARCHITECTURE.md` and `docs/ENGINEERING_STANDARDS.md`, prune `KNOWN_DEBT.md`,
   create `CHANGELOG.md`. Zero risk, immediate integrity payoff.
2. **Delete the 12 dead `scout_tools_offers.py` function copies** and repoint
   `tests/test_demand_feed_http.py` (currently the only caller of that side) at
   `scout_agent.py`'s copies. Verification confirmed these are unreferenced by any live code
   path — simpler and lower-risk than a behavior-reconciliation exercise, since there's no
   live behavior on the `scout_tools_offers.py` side to reconcile.
3. **Hoist the shared symbols out of the two dormant circular-import couplings** — move
   `_scout_score`/`_network_portal_url`/`_get_anthropic_client`-equivalent shared logic into
   `scout/offers/` or `scout/monitoring/` so neither direction needs a function-local import.
   Not urgent (verification confirmed nothing is broken today), but a prerequisite for the
   package move — a real top-level package can't carry a dormant landmine that only a lazy
   import defuses.
4. **Structural output/config fixes** — `Card.verified`, mandatory `pattern=`, `ScoutConfig`
   dataclass. Independent of the package move, can land in parallel with 2-3.
5. **Package restructure** (`scout/offers`, `scout/publishers`, `scout/campaigns`,
   `scout/revenue`, `scout/monitoring`, `scout/slack`, `scout/agent`, `scout/routing`,
   `scout/entrypoint`) — mechanical once 3 is done; biggest diff, do it alone, no logic
   changes bundled in.
6. **Unified `Route` registry** — replaces TOOL_MAP/_BLOCK_ACTION_DISPATCH/_FORCE_MONITOR_FNS/
   polling loop one mechanism at a time, each a separate PR, oldest/highest-risk last.
7. **Test migration to pytest + module split** — can start anytime after step 5 gives the
   package layout tests should mirror; independent of steps 4/6.

## What this buys

Today: 46% of all commits are fix/revert/hotfix/incident. The target isn't zero — it's a
codebase where the next new tool, button, or monitor can't silently skip a wiring step,
where "unverified data" is a type error, not a hope, and where a self-audit can't claim
100% while its own table says otherwise. That's the actual definition of "world-class" here
— not fewer lines of code, but fewer ways to get it wrong without the system telling you.
