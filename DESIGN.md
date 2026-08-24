# Scout — First-Principles Redesign

Six independent investigations (nomenclature, routing/dispatch, module dependency graph,
documentation, test architecture, output/config discipline) ran in parallel against the
current codebase. This document is the synthesis: what a staff/principal engineer would
actually change, in what order, and why.

**Status: Phases 1 and 2 are shipped** (this PR) — the docs truth pass and the 12
dead-function deletion, both independently verified by an `/codex review` pass on this
diff.

**Steps 3-7 (v4, this revision) are a plan, not code.** Nothing below Phase 2 has been
built. This document has now been through five independent passes, each catching real
things the last one missed: an internal self-audit, an external `/codex` plan review that
killed a "unified `Route` registry" design as infeasible, an automated CodeRabbit PR review
that caught leftover contradictions the fixes for the `/codex` pass introduced, and a final
three-agent parallel audit (file classification against real content, a naming-collision
stress test, and a fresh lenses.json pass) that closed the remaining gaps — including one
genuinely embarrassing one: `scout_thresholds.py` was placed in `monitoring/` despite being,
by its own import graph, the single most cross-domain-consumed file in the repo. Every one
of those five passes found something real. That pattern is itself the honest answer to "is
this done": no, a sixth pass would probably find something too. What's true is that nothing
outstanding from any of the five is left open below — see What Changed In This Revision.

## What changed in this revision (v4)

The final three-agent audit (file classification, naming-collision stress test, fresh
lenses.json pass) found:

- **`scout_thresholds.py` misplaced.** Grep-verified live importers: `queries_monitor.py`,
  `queries_campaign.py`, `queries_publisher.py`, `scout_agent.py`, `scout_bot.py`,
  `scout_handlers.py`, `scout_digest.py`, `scout_ui_kit.py`, `scout_state.py`, `scout_ch.py`,
  `demand_feed_main.py`, `scout_tools_offers.py` — every domain plus the slack and agent
  layers. v3 kept it "in `monitoring/`, as-is" purely because `ThresholdManager` was cited as
  a good *pattern* to copy elsewhere; that's not the same claim as the file belonging to the
  monitoring domain. Moved to `scout/shared/`.
- **`scout_types.py` should not be split.** v3 guessed it splits between `offers/` and a
  shared module. Grep-verified live consumers are 100% offer/campaign-domain
  (`scout_agent.py`, `offer_scraper.py`, `scout_tools_offers.py`). Two of its four exported
  symbols are dead code — `PulseSignal` has zero consumers anywhere despite its docstring
  claiming otherwise, and `tool_result()` is called only by its own test, never by any real
  `TOOL_MAP` handler. Moves whole into `scout/offers/` as `types.py`; the dead symbols get
  flagged for a removal decision during the move, not silently carried forward.
- **`scout_images.py` was wrongly guessed as `slack/`.** Direct read: Clearbit domain
  lookup, Google favicon construction, iTunes App Store icon search, a JSON file-cache, and
  one ClickHouse query — offer-creative *enrichment*, zero Slack API calls, zero Block Kit.
  Same "looks like slack/ but isn't" pattern `scout_state.py` already caught once. Moves to
  `scout/offers/`.
- **`scout_telemetry.py` and `scout_log.py`** were absent from the v3 package tree entirely
  — an omission, not a deliberate exclusion. Both are genuine cross-cutting infra (OTel
  tracing; structured JSON logging for Better Stack, consumed across `alert_registry.py`,
  `offer_scraper.py`, `scout_digest.py`). Both go to `scout/shared/`.
- **`scout_response.py`** is also absent from v3's tree — and it's dead code
  (`ScoutResponse`/`Metric`/`Item`, grep-confirmed zero non-test imports) that already
  encodes almost exactly the `verified`/`data_freshness`/`confidence` idea Phase 4 proposes
  building from scratch. Before Phase 4 executes: decide whether to revive/merge
  `ScoutResponse`'s validation logic into `wrap_response()` instead of building a parallel
  mechanism — coding.md's "verify a capability doesn't already exist" rule, which the v3
  self-audit ran once and still missed this file.
- **`alert_registry.py`** renamed to `alerts.py`, not the generic `registry.py` the
  drop-prefix rule would otherwise suggest — v3's routing section introduces a family of
  `*Registry` types (`ToolRegistry`, `BlockActionRegistry`, `MonitorRegistry`, etc.); naming
  this file `monitoring/registry.py` would misread as a member of that family when it's
  actually a Redis-backed alert-state store, unrelated to dispatch.
- **`get_supply_demand_gaps` → `get_advertiser_supply_gaps` was a real bug in v3's own
  naming plan.** The live tool description (`scout_agent.py:1127`) is explicit: "Provide
  publisher_name OR advertiser_name, not both" — it's bidirectional. Renaming it to an
  advertiser-scoped name would misrepresent its own contract. Not renamed.
- **`scout/agent/agent.py` was a stutter** (`scout.agent.agent`) inconsistent with every
  other package's distinct-filename convention. Renamed to `scout/agent/dispatch.py`.
- **The "cross-repo risk, cannot verify" line in v3 was wrong** — corrected below.
  `ms-demand-feed` is not a separate repository; it's a second Render service deployed from
  this same repo (confirmed directly in `render.yaml`). The dependency is fully verifiable
  from inside this worktree, and now verified: `demand_feed_main.py`'s only non-stdlib,
  non-`scout_core` imports are `offer_scraper`, `alert_registry`, and `scout_bot` — all
  three need import-path updates during the package move; no external check is needed.
- **Two "should the query layer, demand-feed, and revenue-alarming be three separate
  services" questions got answered honestly, not deferred:** see New: Service Topology
  below. Short version: two services already exist (not one, not three), and going to three
  fully independent deployments would trade a real coupling problem for a bigger
  distributed-systems problem the traffic here doesn't justify. The fix is a clean module
  boundary within the existing second service, not a third deployment.
- Five stale "Phase 5" cross-references (left over from an earlier resequencing that moved
  the package move from step 5 to step 6) and two remaining "the `Route` registry" mentions
  CodeRabbit's pass didn't catch are corrected throughout below — not called out individually
  since they're folded directly into the corrected text.

## Self-audit against `lenses.json`

- **`coding.md` rule 12** ("verify a capability doesn't already exist before planning new
  work") — run three times now, found something new each time: `scout_core/`'s existence
  (v2), then `scout_response.py`'s existing verified/confidence pattern (v4, this revision).
  The rule doesn't get "satisfied" by running it once.
- **`coding.md`'s Subagent Discipline rule 3** ("gate subagent output before treating it as
  ground truth") — applied to every wave of agents in this document, including the `/codex`
  and CodeRabbit passes themselves: their claims got spot-checked against real source
  (`_INTERNAL_TOOLS` at `scout_agent.py:1703,1721`, prefix dispatch at
  `scout_handlers.py:2067,2073`, both confirmed) rather than accepted because they arrived
  with citations.
- **`anti-AI.md`'s Generalizing Principle** ("the flattened, most-expected answer on any
  axis with real variation is the tell") — caught twice in this document's own history: once
  in the v1 generic tech-tier package layout (`data/domain/slack/agent/...`), once in v2's
  unified `Route` registry (forcing daemon threads, one-shot Slack actions, and LLM tools
  behind one decorator API because they're all "dispatch"). Both killed on the same
  principle, one layer apart.
- `design.md` (UI/UX lens) correctly doesn't apply. Commercial Framing correctly doesn't
  apply — CLAUDE.md's own exception covers "pure infra... no meaningful revenue path."

## The honest scorecard

`VAMSEE_AUDIT.md` claimed "43/43 tools audited, 24/24 violations fixed, 100% COMPLETE"
under a self-graded checklist, filed under a real engineer's name, never actually reviewed
by him. Every finding below has been through independent verification — direct diff/grep
against source, not re-trusting the report that raised it — and one didn't survive it:

1. ~~A second, drifted query layer~~ — **REFUTED.** `scout_ch.py`'s `_query_*` functions
   are thin delegation wrappers (resolve threshold config, then call the bare-noun
   `queries_*.py` function) — a documented single-source-of-truth pattern, not a competing
   implementation. `_query_ghost_campaigns` (scout_ch.py:176) is 3 lines ending in
   `return _q.ghost_campaigns(ch, recency_hours=recency_hours, ...)`. Fine as-is.
2. **12 duplicated functions** between `scout_agent.py` and `scout_tools_offers.py`, beyond
   the 4 KNOWN_DEBT.md already tracked — **confirmed via `ast`-diff of every pair**: 4
   byte-identical, 8 behaviorally drifted, and **none of the 12 `scout_tools_offers.py`
   copies were ever called in production** — `TOOL_MAP` bound every one to `scout_agent.py`.
   6 of the 8 drifted ones routed through two independent benchmark caches that could have
   silently diverged if ever wired live. Shipped: deleted, test repointed (Phase 2).
3. **Two structural, dormant circular-import couplings** (`scout_agent.py` ↔ `scout_bot.py`,
   `scout_agent.py` ↔ `scout_digest.py`) — citations confirmed exactly, but "live cycle" was
   overstated: neither breaks anything today, the lazy imports are a correct working
   defense. What's real: moving either lazy import to the top of its file produces an
   immediate, reproducible `ImportError` (verified by tracing exact load order). Dormant
   landmines, not active bugs — fix before the package move, not urgently before anything.
4. **~8 different dispatch mechanisms**, count confirmed, two location errors corrected:
   the scheduled monitor-polling mechanism lives in `demand_feed_main.py` (6-entry
   `(daemon_fn, name)` tuple list, `demand_feed_main.py:690-698`), not `scout_bot.py`;
   `_FORCE_MONITOR_FNS` is defined in `scout_handlers.py:168`, not `scout_bot.py` (only its
   population calls are there, which was correct).
5. **Response-pattern enforcement is opt-in, not structural** — confirmed exactly
   (`wrap_response()`'s own docstring: "Callers that omit pattern= are unaffected"). The
   raw-block problem is worse than first reported: 13 raw `blocks=[...]` sites vs. 10
   `wrap_response()` calls in `scout_handlers.py` — 56% bypass, not "roughly a third."
6. **"Never surface unverified data silently" has zero enforcement mechanism** — confirmed.
   `Card`'s full field list is `severity, headline, body, facts, actions, chart_url` — no
   provenance field of any kind (and, per this revision, `scout_response.py` already built
   most of one, unused).

Plus the already-known: `smoke_test.py` is 220 hand-rolled tests in one 6,154-line file with
zero test references to `queries_monitor.py`, `queries_publisher.py`, `demand_feed_main.py`,
`scout_notion.py`, `scout_images.py`, `scout_telemetry.py`, and a live-API test sitting next
to 200+ pure unit tests on every deploy gate.

None of this is "vibe-coded crap" — the individual fixes in KNOWN_DEBT.md are careful and
correctly scoped. The problem is structural: each fix solved its instance of a pattern
without building the mechanism that catches the next one. That's the gap this design closes.

## New: service topology (resolved, not deferred)

Raised directly: should Scout be three separate services — a query layer, a demand-feed
layer, and a revenue-alarming layer — instead of one module? Investigated, not guessed:

**What actually exists today (`render.yaml`, confirmed):** two Render services, both
deployed from this one repo. `ms-scout` (`type: worker`, runs `scout_bot.py`) is the Slack
query/digest surface. `ms-demand-feed` (`type: web`, runs `demand_feed_main.py`) runs the
offer scraper, all 6 monitor daemons (cap, velocity-down, ghost, fill, cvr-anomaly,
expiration), the revenue tracker, and the projection-autocheck daemon — with its own
`WebClient`, posting Slack alerts directly, independent of `scout_bot.py`. They communicate
by importing each other's Python files directly (`scout_core/` plus, verified,
`demand_feed_main.py` importing `offer_scraper`, `alert_registry`, and `scout_bot` — not an
API boundary).

**So "revenue alarming" isn't missing a home — it's bundled into the demand-feed service**,
alongside a conceptually unrelated job (scraping affiliate networks). That bundling is real
coupling: a scraper hang or crash shares failure blast radius with the alerting engine
that's supposed to warn when revenue drops. Concrete evidence this class of bug already
bites: `72c239c fix(queries_monitor): pct_delta raw filter, campaign_id rename, midnight
drift` and `32d0f85 fix(scout): persist autocheck EOD marker, sleep-at-end, surface
kill-switch failures` — both are the overnight/day-boundary bug shape that shows up when
timing-sensitive logic shares a process with unrelated work.

**Verdict: don't split into three deployed services.** Two services already exist; a third
independently-deployed service for alerting would need a real API/queue contract between it
and demand-feed (since alerts need the same offer/campaign data the scraper produces) —
that's a materially bigger lift than the coupling problem justifies for a system serving one
Slack workspace's internal ops. That's the same category of mistake this document already
killed twice at the module level (generic package tiers, the unified `Route` registry) —
forcing more boundaries than the actual traffic/failure-domain needs justify, one layer up
at the deployment level this time.

**What actually fixes the coupling:** extract the 6 monitor daemons + revenue tracker +
projection-autocheck out of `demand_feed_main.py` into a clean `scout/monitoring/` package
with its own interface (Phase 6b, below) — module-boundary isolation, not a third
deployment. If a scraper crash silencing alerts turns out to be a documented incident (not
verified in this pass — would need Render's incident history, which this worktree can't
read), the cheap fix is running the monitor daemons in their own thread group with the same
crash-isolation wrapper `.claude/rules/scout_handlers.md` already documents for
`_handle_event_impl` — still not a new service.

**Feature-cutting**, raised alongside the split question: legitimate, separate axis, not
answered here — it's a usage-data question, not an architecture one. Scout already has a
`get_usage_report` tool; pulling that data (which of the 43 tools actually get called, how
often) would tell you what's dead weight, rather than guessing.

## Target package structure

Packaged by **domain**, not generic tech-tier. `scout_core/` already exists as the
cross-repo... no — **cross-*service*** shared boundary (corrected from v3: same repo, two
Render deployments, not two repos) and is extended, not duplicated:

```text
scout_core/        [UNCHANGED — shared between the ms-scout and ms-demand-feed services,
                    same repo, two Render deployments]
  contracts.py, monitors.py, job_runs.py

scout/
  shared/           thresholds.py (was scout_thresholds.py — moved here, not monitoring/;
                    grep-verified as the most cross-domain-consumed file in the repo, used
                    by every domain package plus slack/ and agent/), clickhouse.py (from
                    scout_ch.py — _get_ch_client, CHBusyError; SQL stays in domain packages),
                    state.py (was scout_state.py — "All JSON state I/O for Scout," its own
                    docstring), telemetry.py (was scout_telemetry.py — OTel/Latitude tracing,
                    omitted from v3's tree by oversight), log.py (was scout_log.py —
                    structured logging for Better Stack, same omission), anthropic_client.py
                    (the _get_anthropic_client singleton, extracted from scout_agent.py)
  offers/           scraper.py (was offer_scraper.py), tools.py (was scout_tools_offers.py —
                    canonical status verified per-function at move time, not assumed; see
                    Phase 6b), types.py (was scout_types.py — moves WHOLE, not split; v3
                    guessed a split, grep-verified 100% offer-domain live usage; dead
                    PulseSignal/tool_result() symbols flagged for a removal decision, not
                    silently carried forward), images.py (was scout_images.py — offer-creative
                    enrichment: Clearbit/favicon/App-Store icon lookup + a ClickHouse query;
                    v3 wrongly guessed this was slack/ presentation code)
  publishers/       queries.py (was queries_publisher.py) — fetch_*(ch, ...)
  campaigns/        queries.py (was queries_campaign.py) — fetch_*(ch, ...);
                    demand_feed_main.py's campaign-creation logic EXTRACTED here (Phase 6b,
                    a real refactor, not a mechanical move)
  revenue/          queries.py (was queries_revenue.py) — fetch_*(ch, ...)
  monitoring/       queries.py (was queries_monitor.py) — fetch_*(ch, ...); alerts.py (was
                    alert_registry.py — renamed, not "registry.py," to avoid misreading as
                    one of the *Registry dispatch types below); notion.py (was
                    scout_notion.py — pipeline/Scout-Demand-Queue workflow, zero Slack API
                    calls per its own docstring, not presentation); daemons.py (Phase 6b —
                    the 6 monitor daemons + revenue-tracker + projection-autocheck, extracted
                    from demand_feed_main.py as its own module with its own interface — see
                    Service Topology above)
  slack/            ui_kit.py, slack_safe.py, image_resolve.py — genuinely Slack-Block-Kit
                    presentation, confirmed by direct read (not a generic ui/ or
                    presentation/ layer — every file here is Slack-specific, and naming it
                    that way is more honest, not less). response.py (was scout_response.py —
                    dead code today; carried forward pending the Phase 4 revive-or-delete
                    decision, not silently dropped or silently kept live)
  agent/            dispatch.py (was scout_agent.py's TOOL_MAP/TOOLS/dispatch — renamed from
                    v3's `agent.py` to avoid the scout.agent.agent stutter; imports downward
                    into shared/offers/publishers/campaigns/revenue/monitoring/slack, never
                    lazily reaches into bot/digest)
  routing/          events.py (was scout_handlers.py), attachments.py
  entrypoint/       bot.py, demand_feed_main.py (now thin — daemon logic lives in
                    scout/monitoring/daemons.py after 6b), digest.py
tests/
  test_shared/, test_offers/, test_publishers/, test_campaigns/, test_revenue/,
  test_monitoring/, test_slack/, test_agent/, test_routing/, test_entrypoint/
  integration/      test_ask_tool_call.py, test_nl_routing.py (was nl_query_test.py)
docs/
  ARCHITECTURE.md, ENGINEERING_STANDARDS.md, DEPLOY.md
  archive/          VAMSEE_AUDIT.md
```

The two circular-import cycles resolve by construction: `_get_anthropic_client` moves to
`scout/shared/anthropic_client.py`, `_scout_score`/`_network_portal_url` move to
`scout/offers/tools.py`. `scout/agent/dispatch.py` becomes a true leaf-consumer, never a
hub — and cross-service-shared logic (the hourly monitor loop, job-run telemetry) stays in
`scout_core/` rather than getting pulled into `scout/monitoring/` and orphaned from the
`ms-demand-feed` deployment.

**Rollback strategy:** Phase 6 ships backward-compat shim modules at every old top-level
import path (e.g. `scout_ch.py` re-exporting from `scout.shared.clickhouse`) for at least
one deploy cycle, so a Render rollback to the prior commit doesn't hit an `ImportError`.

## Naming conventions

- Drop the `scout_` file prefix — with 20+ of 28 files sharing it, it distinguishes
  nothing; the package (`scout.offers`, `scout.shared`, ...) now does that job. One
  exception, and it's a real one: `scout_thresholds.py`'s class `ThresholdManager` is
  already an established name in commit history and other docs — the file becomes
  `shared/thresholds.py` (prefix dropped, same as everything else), but the class name
  itself doesn't need to change.
- One bare-query naming convention: `fetch_<noun>(ch, ...)` for the SQL-executing function
  in each domain's `queries.py`. `scout_ch.py`'s `_query_*` wrappers keep their prefix and
  move to `scout/shared/clickhouse.py` alongside the client they configure — verified as a
  real config-resolution pattern, not a duplicate (see The Honest Scorecard, #1).
- `get_top_opportunities` → `get_unrun_offer_opportunities`, `get_top_revenue_opportunities`
  → `get_publisher_revenue_gaps`. **`get_supply_demand_gaps` is NOT renamed** — v3 proposed
  `get_advertiser_supply_gaps`, which was wrong: the live tool is explicitly bidirectional
  ("Provide publisher_name OR advertiser_name, not both," `scout_agent.py:1127`), and an
  advertiser-scoped name would misrepresent it.
- `get_queue_status` / `get_demand_queue_status` — rename the second to
  `get_ms_platform_queue_status`; verified against the live description, this one genuinely
  clarifies real behavior (cross-references ClickHouse impressions against the MS Demand
  Queue specifically).
- `why_entity_note` → `explain_entity_note` (verb-consistency with `record_`/`forget_`).
- `nl_query_test.py` → `tests/integration/test_nl_routing.py` — never a smoke test by its
  own docstring's admission ("Do NOT add to startup smoke test").
- `queries.py` (5-line re-export shim at the repo root) — delete if nothing depends on the
  wildcard import (grep first), otherwise rename to `queries_compat.py`.

## Routing architecture — unified registry killed

v2 proposed a single `Route` object spanning all ~8 dispatch mechanisms behind one
decorator API. Review verdict: wrong. LLM tools, one-shot Slack block actions, slash
commands, and scheduled daemon threads are different contracts — different lifetimes,
different failure modes, different definitions of "complete." A tool call returns a value;
a monitor is an infinite daemon factory that never returns. This was the same category
error `anti-AI.md`'s Generalizing Principle already caught once in this document (the v1
generic package-tier layout) — made again one layer up. Two further problems compounded
it: `TOOL_MAP` is only half the LLM tool surface (`_INTERNAL_TOOLS` filters `TOOLS` at
`scout_agent.py:1703,1721`, spot-checked, holds up), and import-time decorator
auto-registration would force eager cross-service imports, reintroducing the exact
circular-import risk Phase 3 exists to remove.

**Corrected: separate, type-specific registries, unified later only if their contracts
prove genuinely identical — not a stated goal.**

- **`ToolRegistry`** (`scout/agent/dispatch.py`) — one entry per tool capturing schema
  (`TOOLS`), handler (`TOOL_MAP`), visibility (`_INTERNAL_TOOLS`), and intent hints as a
  single object, not a bare handler dict. `ToolRegistry.assert_all_tools_have_intent_hints()`
  replaces the manual "SYSTEM_PROMPT needs a numbered line per tool" audit; the numbered
  list itself can be *generated* from intent hints instead of hand-maintained prose.
- **`BlockActionRegistry`** (`scout/routing/`) — exact-key dict PLUS the existing ordered
  prefix-predicate fallback (`scout_suggestion*`, `home_try_query*` — confirmed real at
  `scout_handlers.py:2067,2073`). Its own `assert_all_actions_handled()`.
- **`SlashCommandRegistry`** (`scout/routing/`) — internal consistency only. Cannot verify
  the Slack app manifest at api.slack.com/apps stays in sync — that's a manual step,
  already documented in CLAUDE.md, not a new gap this creates.
- **Two separate `MonitorRegistry` instances**, one per service — `scout_bot.py`'s
  force-run path, and `scout/monitoring/daemons.py`'s scheduled daemon list (post-6b) —
  populated by explicit `.register()` calls at each service's own natural import point,
  never by a cross-service auto-discovery scan. A daemon factory and a one-shot force-run
  callable are not the same primitive; collapsing them was v2's mistake.
- **`ExtractorRegistry`** (`scout/routing/attachments.py`) — the existing ordered
  predicate-cascade shape, unchanged; it already works.

Each registry gets its own simple, type-specific completeness check instead of one
polymorphic method trying to mean five different things. There is no `route.*` object
anywhere in this design — every registry above is its own class, referenced by its own
name.

## Output/config: make policy structural

v2 proposed a required `verified: bool` field on `Card` and a single global `ScoutConfig`.
Review verdict: both too blunt.

- **`Card.verified` — rejected as a `Card` field.** Loading states, modals, help cards, and
  self-QA status aren't "verified/unverified data" responses. **Corrected:** validation
  moves into `wrap_response()`, conditional on `pattern` — only `ANSWER`/`STATUS` carrying a
  `facts` tuple from a live query require a `verified`/`source` marker, passed as a
  `wrap_response()` kwarg. **New in this revision:** before building this, check whether
  `scout/slack/response.py` (the revived-or-not `ScoutResponse`) already has the right
  shape — it independently invented `data_freshness` and a derived `confidence` level; don't
  build a second version of the same idea next to the first, unused one.
- **Mandatory `pattern=` needs a migration model first.** Sequence: (a) enumerate the
  missing pattern/surface pairs for `MODAL`/`HOME`/ephemeral, (b) migrate call sites, (c)
  only then make `pattern=` required.
- **The raw-`blocks=` count isn't proof of bypass on its own.** 13 raw sites vs. 10
  `wrap_response()` calls in `scout_handlers.py` (56%) is real, but some are legitimate
  modal scaffolding. Classify each of the 13 individually before migrating any: queue-card
  post (639), App Home modal states (1558, 1607, 1636, 1665, 1685, 1695 — 1648 already
  compliant), self-QA runner (2919, 2960), plus 1388/2209/2294/3276.
- **`ScoutConfig` — rejected as one global dataclass.** Config objects already exist,
  scattered across `scout_bot.py`, `scout_handlers.py`, and `demand_feed_main.py` — the
  problem is fragmented ownership, not absence. One cross-service dataclass would recreate
  exactly the coupling `scout_core/`'s minimalism already avoids. **Corrected:** extend each
  existing per-service config object with its own scattered `os.getenv()` reads, using
  `ThresholdManager`'s pattern — one config object per service boundary.

## Test architecture

v2 sequenced test migration *after* the package move. Review verdict: backwards — the
single biggest diff in the plan would happen while only the 220-test monolith protects it.
**Corrected: write pytest coverage against current file locations first**, verify green,
then the package move becomes a mechanical import-path rename in the same PR as tests that
already pass.

- Migrate `smoke_test.py`'s 220 tests to pytest by *current* module — one file per module,
  closing the silent zero-coverage gap (`queries_monitor`, `queries_publisher`,
  `demand_feed_main`, `scout_notion`, `scout_images`, `scout_telemetry`) before any moves.
- Tag every test hitting live ClickHouse/Anthropic/Slack with `@pytest.mark.integration`
  (starting with `test_ask_tool_call`). Render's redeploy check runs `pytest -m "not
  integration"` only.
- **Coverage metric corrected:** "zero test references" (grep-based) rewards a meaningless
  import, not real coverage. Use `coverage.py` thresholds per package, or named contract
  tests ("every `fetch_*` function has a test that mocks `ch` and asserts the SQL/return
  shape") — an honest floor, not a grep pretending to be one.
- **Clarifying note, not a naming defect:** `scout/monitoring/queries.py`'s functions
  (`fill_rate_publishers`, `velocity_alerts`, `cap_alert_campaigns`) and the `MonitorRegistry`
  in `scout/monitoring/daemons.py` both use "monitor" for related but distinct things — the
  former is the SQL powering the signals, the latter is what dispatches them. Legitimate
  data-layer/dispatch-layer split, not confusion; worth one docstring line at the move, not
  a rename.

## Documentation set

| File | Fate |
|---|---|
| `README.md` | Keep — already good. Add a pointer to `docs/DEPLOY.md` once it exists. |
| `CLAUDE.md` | Keep as agent-operating instructions. One correction owed: its Response Patterns table still says ALERT has 0 buttons; `scout_bot.py`'s `_build_alert_response` (lines 723-726) adds Acknowledge/Snooze whenever `alert_name` is set, and `wrap_response()` doesn't reject it. `docs/ARCHITECTURE.md` already carries the corrected version with a footnote; CLAUDE.md's copy is still stale. |
| `docs/ARCHITECTURE.md` | **Shipped.** Service topology, concurrency model, response patterns, data flow. |
| `docs/DEPLOY.md` | **Still pending.** Extract README's Render section + env var table; add rollback/monitoring notes. Do as part of Phase 3 or its own small doc PR. |
| `docs/ENGINEERING_STANDARDS.md` | **Shipped.** The 5 durable engineering-quality checks from `VAMSEE_AUDIT.md`. |
| `FEATURES.md` | Keep, CI-enforced as fully generated — fails if stale relative to `ToolRegistry` (not "the Route registry" — no such object exists in this design). |
| `CHANGELOG.md` | **Shipped.** |
| `KNOWN_DEBT.md` | **Shipped** — pruned to open items only. |
| `VAMSEE_AUDIT.md` | **Shipped** — archived to `docs/archive/`. |
| `PLAN.md` | Left untouched — live, git-tracked work-in-progress (attachment ingestion), not scratch. |
| `.claude/rules/*.md` | Keep as-is — incident-anchored invariants, not architecture. |

Also close the docstring gap on `scout_bot.py` (0/7 public functions documented) — the file
its own rules file flags as having the trickiest concurrency constraints in the repo.

## Sequencing (one PR = one concern)

Not a big-bang rewrite — each phase ships independently, codebase stays green throughout.

1. ✅ **Shipped.** Docs + naming truth pass. `/codex review` caught 2 gaps the original pass
   missed, both fixed before merge.
2. ✅ **Shipped.** Deleted the 12 dead `scout_tools_offers.py` function copies, repointed
   the one test that exercised them. `/codex review` independently confirmed no other
   references exist.
3. **Next up. Circular-import hoist.** Move `_get_anthropic_client` to
   `scout/shared/anthropic_client.py` (not `offers/`/`monitoring/` — v2's hoist targets were
   wrong) and `_scout_score`/`_network_portal_url` to `scout/offers/tools.py`. Not urgent
   (confirmed dormant), but a prerequisite for the package move.
4. **Structural output/config fixes.** `wrap_response()`-conditional `verified`/`source`
   validation — check `scout_response.py` for a revive-or-merge decision first (see
   Output/config above) — the MODAL/HOME pattern-taxonomy migration before `pattern=`
   becomes required, per-service config objects instead of one global `ScoutConfig`.
5. **Write pytest coverage against current file locations FIRST.** One file per current
   module, closing the zero-coverage gap named above, verified green before any file moves.
6. **Package restructure**, split because "mechanical" was hiding a real refactor:
   - **6a — pure moves.** Files needing zero logic changes (`queries_*.py` → domain
     packages, `scout_ui_kit.py`/`scout_slack_safe.py`/`scout_image_resolve.py` → `slack/`,
     `scout_types.py` whole → `offers/`, `scout_images.py` → `offers/`,
     `scout_thresholds.py`/`scout_ch.py`/`scout_state.py`/`scout_telemetry.py`/`scout_log.py`
     → `shared/`, `alert_registry.py` → `monitoring/alerts.py`, `scout_notion.py` →
     `monitoring/notion.py`) — mechanical rename, done with step 5's passing tests updated
     to new import paths in the same PR.
   - **6b — real extraction.** `demand_feed_main.py`'s embedded campaign-creation logic →
     `scout/campaigns/`; its 6 monitor daemons + revenue-tracker + projection-autocheck →
     new `scout/monitoring/daemons.py` (see Service Topology — this is the module-boundary
     fix that addresses the coupling without standing up a third service). `scout_tools_offers.py`
     vs. `scout_agent.py` canonicality verified per-function before either moves — for each
     function, `ast`-diff both copies, keep whichever is proven live by `TOOL_MAP`, delete
     the other's copy in the same commit as the move, never move both and decide later.
   - **No external blocker** (corrected from v3 — `ms-demand-feed` is a same-repo second
     Render service, not a separate repository; its only non-`scout_core` imports from this
     codebase are `offer_scraper`, `alert_registry`, and `scout_bot`, all three verified and
     accounted for in the moves above).
   - Ships with backward-compat shim modules at every old top-level import path for at
     least one deploy cycle.
7. **Per-type dispatch registries**, one at a time, each its own PR — `ToolRegistry` first
   (lowest cross-service risk), `BlockActionRegistry` and `SlashCommandRegistry` next, the
   two `MonitorRegistry` instances last (the most cross-service-entangled). No unified
   `Route` object — five purpose-built registries, unified later only if proven identical.
   Backed by real test coverage from steps 5/6, not the 220-test monolith's blind spots.

## What this buys

Today: 46% of all commits are fix/revert/hotfix/incident. The target isn't zero — it's a
codebase where the next new tool, button, or monitor can't silently skip a wiring step,
where "unverified data" is a type error, not a hope, where a self-audit can't claim 100%
while its own table says otherwise, and where the plan for fixing all of that has itself
been checked by five independent passes rather than trusted because it sounds thorough.
That's the definition of "world-class" this document can actually earn on its own: not zero
remaining defects — a sixth pass would likely find one — but zero defects left open once
found. What it cannot earn on its own is proof that the design is *right*: that comes from
building Phase 3 onward and watching the fix/revert ratio actually move, and from a human
reviewing this before more of it ships. Neither has happened yet.
