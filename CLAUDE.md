# Scout — Architecture Invariants

## Session Start Protocol

At the start of any Scout session where code changes are expected:
1. **Verify you are working in `tools/offer-scraper/`** — if the current directory is `tools/ms-scout-pr14/` or any other path, stop. That directory is a stale worktree. This CLAUDE.md is only current on the `main` branch of `tools/offer-scraper/`.
2. Read `## Known Debt` (bottom of this file) and surface items relevant to the current task
3. If the current task resolves a Known Debt item, remove it from the list as part of the PR
4. New deferred items go into `## Known Debt` — not into gstack plan files (those are not auto-loaded into sessions; this file is)

Why: ms-scout has no project management system. Linear, Notion, and gstack TODO files
are not auto-loaded by Claude. CLAUDE.md is. If a deferred item isn't here, nobody sees it
until something breaks.

---

## The Core Rule: One Function Per Signal

Scout computes signals in two places:
1. **Agent tools** (`scout_agent.py`) — called when a user asks @Scout a question
2. **Pulse signals** (`scout_bot.py` `_run_pulse_signals()`) — computed at 8am

When the same business logic exists in BOTH, they MUST call a shared `_query_*()` function.
Duplicate SQL guarantees drift. This happened with ghost detection in Apr 2026 — the Pulse
missed a 48h recency filter that was added to the agent tool, causing false alarms.

---

## Engineering Principles

Five principles derived from Scout's actual incident history. Read before planning any change.

**P1 — Validate at the boundary.**
- Module boundaries: all stdlib imports at file top, not inside functions — a missing `import os` inside a handler crashes silently at the first @mention
- API boundaries: every Slack/Anthropic/ClickHouse/Notion call wrapped in `try/except` with a safe fallback — uncaught exceptions drop the entire handler, not just the signal
- Config boundaries: every configurable threshold read from `config/scout_thresholds.json` or env vars at module load, not hardcoded mid-function — hardcoded values are silently ignored when config changes
- Data boundaries: never assume upstream columns are populated; use NULL-safe SQL (`coalesce`, `nullIf`, `arrayFilter`); validate schema at boot via `_SCHEMA_DEPS` before querying

**P2 — One source of truth per concept.**
- One function per signal: shared `_query_*()` functions called by both Agent tools and Pulse — duplicate SQL guarantees filter drift (ghost campaigns, Apr 2026)
- One config store: `config/team_corrections.json` for static platform facts, `data/entity_overrides.json` for entity facts — not inline constants in SQL strings or `pulse_state.json`
- One network list: `SUPPORTED_NETWORKS` in `scout_agent.py` is the single source — `_DIGEST_NETWORKS_FALLBACK` in `scout_digest.py` derives from the live offers file, not a parallel hardcoded list

**P3 — Read before building.**
- Before planning any new Scout capability: read `scout_agent.py` SYSTEM_PROMPT, TOOLS, TOOL_MAP — the feature may already exist
- Before adding a new shared function: check if a `_query_*()` equivalent already exists in `scout_agent.py`
- Before adding a new daemon: check what daemons already exist in `scout_bot.py` and how they register via `_start_daemon()`
- Proposing code that duplicates existing functionality is a defect, not a feature

**P4 — Tests describe behaviors, not incidents.**
See `### Tests-as-behaviors rule` under File-Editing Rules for `scout_slack_ui.py`.

**P5 — Self-heal, don't report chores.**
See `## User-Facing Action Rule` above.

---

## PR Definition of Done

Before marking any Scout PR complete, verify ALL of the following:

- [ ] `python3 smoke_test.py` passes — paste the output line ("PASSED N/N" or "FAILED M/N") into the PR description
- [ ] No new test names contain PR numbers, fix labels, or dates — test names describe behavior only
- [ ] No new live API calls added to `smoke_test.py` — health probes belong in `_compute_health_status()`, not the smoke suite
- [ ] Every new `_query_*()` shared function has a corresponding smoke test
- [ ] Every new config-driven threshold has a test proving behavior changes when the config value changes (monkey-patch pattern) — note: signal config keys in `scout_thresholds.json` are not yet wired to their queries; adding a test for those before wiring will pass for the wrong reason
- [ ] Import DAG unchanged: `grep -rn "from scout_bot import" scout_handlers.py` must return empty
- [ ] Block Kit canonical primitives used in any new Pulse blocks (no naked `section.fields`, no NBSP padding `\xa0`, no `·` separators between items)
- [ ] No "Action: run X" or "Action required" messages added to user-facing Slack output unless the action requires genuine human judgment
- [ ] Signal Map updated if a new signal was added or an existing one changed
- [ ] Known Debt updated: resolved items removed, new deferred items added

**Enforcement:** `smoke_test.py` is not wired to CI. Until it is, running it and pasting the output is the gate. "It's a small change" is how the last 3 production breaks happened.

---

## Signal Map — Verified Apr 2026

| Signal | Shared function | Consumers | Notes |
|---|---|---|---|
| Ghost campaigns | `_query_ghost_campaigns(ch)` in `scout_agent.py` | `get_ghost_campaigns()` + Pulse `_run_pulse_signals()` | Pulse groups by adv_name; agent keeps per-campaign detail |
| Revenue opportunities | `revenue_opportunities(ch)` in `queries.py` | `get_top_revenue_opportunities()` + Pulse `_pulse_signal_opportunities()` | Both callers use the same fuzzy anti-join SQL. Agent adds Python-level grouping; Pulse takes top 5. No drift. |
| Fill rate | **Intentionally separate** | `get_low_fill_publishers()` (30d/10K threshold) + Pulse signal (7d/5K threshold) | Different thresholds on purpose: Pulse = early warning, Agent = stable analysis. Do not merge. |
| Advertiser RPM context | `_query_advertiser_rpm_context(ch, adv_name)` in `scout_agent.py` | `_handle_approve()` in `scout_handlers.py` | At approval time only — not a Pulse signal. Fuzzy ILIKE match on adv_name; uses `trim(status) = 'active'`. Fails safe (returns has_history=False on any error). |
| Velocity shifts | Pulse-only | `_build_velocity_signal()` | No agent tool — no divergence risk |
| Cap alerts | Pulse-only | `_build_cap_signal()` | No agent tool — no divergence risk |
| Overnight events | Pulse-only | `_build_overnight_signal()` | No agent tool — no divergence risk |
| Pulse recall | `get_pulse_summary()` in `scout_agent.py` reads `last_signals_summary` from `pulse_state.json` | Agent tool only — Pulse writes summary, agent reads it | Written by `_run_once_pulse()` (non-force runs only). Force runs intentionally excluded to preserve canonical 8am state. |
| Health heartbeat (PR 15c) | `_run_health_heartbeat()` in `scout_bot.py` | Background daemon every 30 min | Calls `_compute_health_status()` + standalone CH ping. CH ping affects HEARTBEAT only — never the HTTP `/health` probe (Render must not restart on CH outage). Posts one Slack alert on transition to degraded after `_HEALTH_CONSECUTIVE_THRESHOLD` consecutive bad checks; one recovery alert on return to ok. PR 16c: `_run_startup_smoke_test()` also fires a one-shot CH ping right after smoke posts so the 35-min warmup window is no longer a blind spot. |
| Benchmarks warmer (PR 19a) | `_benchmarks_warmer()` in `scout_bot.py` | Background daemon every 30 min | Keeps `_BENCHMARKS` populated in memory by calling `_get_benchmarks()` on a schedule. Boot-time warm happens in `_run_startup_smoke_test()`. `get_scout_status()` self-heals stale/missing benchmarks before reporting. Result: status check never reports "not loaded" except in real CH outage scenarios. |

---

## Rules When Editing Signal Logic

1. **Threshold, window, or filter change** → edit the shared `_query_*()` function only, never in the caller
2. **Adding a field** → add to the shared function SELECT, then consume in both callers
3. **New Pulse signal** → check if an agent tool already computes the same thing; if yes, extract shared function first
4. **New agent tool** → check if the Pulse already computes the same thing; if yes, use or create a shared function

---

## User-Facing Action Rule (PR 19a)

**"Action: run X" messages in Scout's user-facing output are red flags.** Every one
of those should be eliminated unless the user genuinely must do something.

The test: when Scout tells the team "Run @Scout X", ask "could Scout do X itself?"
- If yes (state Scout owns): fix it. Add a daemon, add a self-heal in the read path,
  load on boot. Whatever it takes. Never make Sidd press a button to refresh a cache.
- If no (real human judgment required, like "approve this offer" or "decide between
  two strategies"): keep the action. But verify the action is actually about judgment,
  not chore work.

PR 19a fixed the first instance: "Benchmarks not loaded → Run @Scout refresh offers"
was a chore message. Solution: warm benchmarks at boot, refresh every 30 min via the
benchmarks-warmer daemon, self-heal in `get_scout_status()`. The team only sees
benchmark state when ClickHouse itself is down — at which point it's a real escalation,
not a chore.

When you find yourself adding an "Action: run X" line to user-facing output (Pulse,
digest footer, status response, error response), STOP and ask:
1. Is this state Scout owns? (cache, derived data, refresh of something Scout reads)
   → fix it in code. Don't ask the user to do it.
2. Is this state Scout doesn't own but could attempt? (CH reachable but query failed
   for transient reason) → retry with backoff, alert only after N failures.
3. Is this state truly external? (env var missing, credentials revoked, queue empty)
   → action message is appropriate. Make it specific and explain WHY.

---

## Revenue Opportunities — Resolved Apr 2026

Both callers now use `revenue_opportunities(ch)` in `queries.py`. The shared function uses a
fuzzy position-match anti-join that suppresses name variants (e.g., "Disney+" is suppressed
when a publisher already runs "Disney+ and Hulu"). No drift between Pulse and agent tool.

---

## Architecture Map

```
offer_scraper.py        — Scraper: 9 networks → offers_latest.json (every 6h)
scout_agent.py          — Claude intelligence: TOOLS list, SYSTEM_PROMPT, shared _query_*() functions
scout_bot.py            — Orchestrator: startup, daemon launch, SocketMode handler, Pulse signal runner
scout_handlers.py       — Slack event handlers: _handle_approve, _handle_block_action, handle_event, etc.
scout_slack_ui.py       — Block Kit builders: Pulse blocks, brief blocks, opportunity cards, home view
scout_notion.py         — Notion API: write queue page, AI copy pipeline, notion watcher
scout_state.py          — State I/O: all JSON read/write for 8 state files in data/
scout_digest.py         — Daily digest: offer scoring + dedup + Slack post
context_harvester.py    — Nightly Slack context extraction
campaign_builder.py     — PARKED: Playwright automation (pending Vamsee sign-off)
```

**Knowledge stores:**
- `data/offers_latest.json` — current offer snapshot (refreshed every 6h)
- `data/entity_overrides.json` — publisher/advertiser facts Scout has learned from the team
- `data/pulse_state.json` — runtime Pulse state; never manually edit publisher names without verifying in ClickHouse
- `config/team_corrections.json` — static platform-wide facts (git-tracked), not for entity facts
- `config/scout_thresholds.json` — Scout's tunable thresholds (PR 17a; loaded by `scout_agent._load_thresholds()` at startup). The `@Scout config` tool surfaces current values so the team can audit without reading source.

---

## New Capability Checklist

When adding a new Scout capability, touch these files in this order:

| Capability type | Files to touch |
|---|---|
| New agent tool (LLM-callable) | `scout_agent.py` (TOOLS list + function + SYSTEM_PROMPT) |
| New Pulse signal | `scout_agent.py` (shared `_query_*` function) + `scout_bot.py` (wire to `_run_pulse_signals`) |
| New Slack button handler | `scout_handlers.py` (handler) + `scout_slack_ui.py` (Block Kit card) |
| New Notion page type | `scout_notion.py` (page builder) |
| New state value | `scout_state.py` (load/save functions) |
| Documentation change | `CLAUDE.md` always — update Signal Map and File-Editing Rules |

---

## File-Editing Rules

### scout_agent.py
- **TOOLS list**: every new tool needs 4 things — `name`, `description`, `input_schema`, and a TOOL_MAP entry + function. Missing any one silently breaks routing.
- **SYSTEM_PROMPT**: add a numbered intent routing line for every new tool
- **Shared functions**: prefix with `_query_` and accept `ch` (ClickHouse client) as first arg; return plain dicts, not formatted text
- **DAG terminal**: `scout_agent.py` is the terminal node of the import DAG — no other module imports FROM it. `scout_handlers.py` calls its functions at runtime via `TOOL_MAP`, not via import. Any `import scout_agent` in another module creates a circular dependency.

### scout_bot.py
- **Orchestrator only** — startup, daemon thread launch, SocketMode event routing, Pulse signal runner
- **`_run_pulse_signals()`**: signal computation only — SQL belongs in shared `_query_*()` functions in scout_agent.py, not inline here
- **`_format_pulse_blocks()`**: imported from scout_slack_ui — rendering only, no SQL or business logic
- **`pulse_state.json`** is written by the bot at runtime — don't put static facts here
- **Client instances** (WebClient, ClickHouse) are created here in `main()` and passed as parameters to modules — never imported from scout_bot (circular import)
- **`_BOT_USER_ID`, `_LAST_THREAD_PER_CHANNEL`, and `_PULSE_RUNNER`** are injected into scout_handlers via `_set_bot_user_id()`, `_set_thread_state()`, and `_set_pulse_runner()` after auth — this is the circular-import workaround. Add new functions that scout_handlers needs from scout_bot here; never import scout_bot from scout_handlers.
- **Daemon registration (PR 16b)**: long-running daemons that must be alive for Scout to be healthy go through `_start_daemon(target, name=, args=())` instead of raw `threading.Thread(...).start()`. This auto-registers them in `_REQUIRED_DAEMONS`, which both `_compute_health_status()` and `_thread_watchdog` read from. Use raw `threading.Thread()` only for one-shot threads (smoke-test) or self-monitoring threads (`thread-watchdog`, `launch-watchdog`, `health-server`).

### scout_handlers.py
- **All `_handle_*` functions** live here: approve, reject, DM, block_action routing
- **Import DAG**: `scout_handlers → scout_slack_ui, scout_notion, scout_state, scout_agent` — never imports from `scout_bot`
- **Add `elif action_id == "..."` for each new button** in `_handle_block_action`; always thread-dispatch heavy operations
- **`_update_brief_card_queued`** is defined here (NOT in scout_notion) — updates the Slack digest card after an offer is added to queue
- **Routing rule**: Block Kit *builders* (functions that return `list[dict]` blocks) → `scout_slack_ui.py`. Functions that call `web.chat_postMessage` / `web.chat_update` → `scout_handlers.py`. If it builds blocks, it belongs in scout_slack_ui. If it sends them to Slack, it belongs here.
- **Import prohibition**: `scout_slack_ui` and `scout_notion` must NOT import from `scout_handlers` — this would create a circular import. If you need shared state, pass it as a parameter.
- **No bare variable references from `scout_bot`**: `scout_handlers.py` cannot reference `BOT_TOKEN`, `APP_TOKEN`, or any other module-level constant from `scout_bot.py`. Use `os.getenv("SLACK_BOT_TOKEN")` etc. directly. Bare references crash silently at runtime — the smoke test won't catch it because it bypasses `handle_event`.
- **All stdlib imports required**: `os`, `re`, and any other stdlib module used inside `handle_event` or any handler function MUST be imported at the top of the file. The smoke test does not exercise handlers — missing imports crash silently at the first @mention.
- **Functions from `scout_bot` needed in handlers**: use the `_set_*` injection pattern (see `_set_pulse_runner`). Never import from `scout_bot` directly — circular import.

### Cross-module button value contract
`scout_slack_ui.py` builds button values; `scout_handlers.py` parses them.
These share an implicit JSON contract:

```
_build_opportunity_cards() sets:  {"offer_id": ..., "advertiser": ..., ...}
_handle_approve() reads:           v.get("offer_id") and v.get("advertiser")
```

Changing key names in one file REQUIRES updating the other. There is no type enforcement.
If the contract drifts, approved offers keep showing active buttons — no error, no alert.

**Rule: Never rename these keys without a grep across both files first.**

### scout_slack_ui.py
- **Zero ClickHouse calls, zero Notion calls** — pure data-in → blocks-out
- **`_SOLO_HEADER_RE`** is defined at MODULE LEVEL (not inside `_text_to_blocks`) — compiles once at import
- **Conditional rendering based on caller-provided data is OK** — showing a warning when `risk_flag` is non-empty, hiding a button when a field is absent. What's NOT OK: making threshold comparisons, writing SQL, or deciding what action to take. The caller decides what's true; `scout_slack_ui.py` decides how to display it.
- **Constants**: `_HELP_TRIGGERS`, `_EMOJI_ALIASES`, `_INLINE_RE`, `_HOME_EXAMPLES` live here

### Block Kit Rendering Contract (PR 14 — Apr 2026)

Every Pulse signal group and per-item card MUST use the canonical primitives. Inline `{"type": "section", ...}` construction in `_format_pulse_blocks()` is prohibited — it drifts on the next edit.

| Use case | Canonical primitive | Notes |
|---|---|---|
| Signal group header (ghost, fill, opps, NA, momentum) | `_build_signal_header(emoji, title, context="")` | Returns 1 block (no context) or 2 blocks (section + context). No "WARNING:"/"CRITICAL:" label. |
| Per-item card (publisher, campaign, opportunity) | `_build_item_card(name, left_body, right_body="", context="", action_button=None)` | Uses `section.fields` when `right_body` is set; plain `section.text` when empty. ONE call per item — never join multiple items on one line. |
| Publisher velocity card (NEEDS ATTENTION, MOMENTUM) | `_build_publisher_card(name, delta_pct, ...)` | Thin wrapper over `_build_item_card`. Includes `float(delta_pct)` type guard. Use `*Top Advertiser*` label (not "Driven by"). |
| Actions row (buttons) | `_build_action_row(buttons)` | Pass pre-built button element dicts. |

**Prohibited patterns** — do NOT use in `_format_pulse_blocks()`:
- NBSP padding (` `, `\xa0`) in any mrkdwn text — renders as garbage on mobile
- Joining multiple items on one line with `·` separators (e.g. `pub1 · pub2 · pub3`)
- `section.fields` with an empty right column (`"*Label*\n—"`) — use plain `text` section instead
- `_build_alert_block()` for Pulse signal headers — that function is for `_build_brief_blocks()` risk flags only

**Block count hard limit**: Slack silently drops messages over 50 blocks. `_format_pulse_blocks()` logs the count via `log.debug("[pulse] block count: %d", len(blocks))` and gates the standing section using `_ALWAYS_TAIL = 4` to stay under the limit.

**`_today` test-injection seam**: `_format_pulse_blocks()` accepts `_today=None`. When `None`, it calls `_date.today()` at runtime. Tests pass `_today=date(2026, 4, 27)` (a known Monday) to exercise the opportunities code path without mocking. **Do NOT remove this parameter** — Test 16 in `smoke_test.py` depends on it. No production caller passes it.

### scout_notion.py
- **All Notion API calls** live here: `_write_to_notion_queue`, `_patch_notion_copy`, `_notion_watcher_loop`
- **Zero Slack calls** — fire-and-forget; callers don't wait on it
- **`_patch_notion_copy`** is a LIVE async fallback for the coalescer — do NOT delete it
- **Coalescer**: `_copy_coalescer_loop` batches AI copy enrichment with a 10s window + 24h cache; this is the fallback when sync copy generation fails in `_handle_approve`
- **AI copy pipeline**: `_generate_offer_copy` → `_queue_copy_enrichment` → `_copy_coalescer_loop` → `_patch_notion_copy`

### scout_state.py
- **The ONLY module that reads/writes the `data/` directory** (besides offer_scraper.py)
- **All 8 state file paths** are defined here as constants
- **All reads/writes are atomic** (write to `.tmp`, then `os.replace`) — prevents partial writes on crash
- **Pattern**: `_load_*()` returns dict/list; `_save_*()` writes atomically

### Tests-as-behaviors rule (PR 19b)

Test names describe behavior, never history. Never `PR XYZ —`, `Test N`, or `fix #123`. The git log is the changelog. The test name is the contract.

When a test's behavior evolves, rename it; the old name was a snapshot, not a permanent identity. When you're tempted to write `test_pr_X_invariant_Y`, stop and ask: "what behavior is this asserting?" That IS the test name.

Why: 14 of 40 current smoke tests are PR-numbered. Each release adds 1-3 more. The accumulation makes the morning #scout-qa post a museum of past work instead of a status signal. The git log already records when each test shipped — putting that in the test name is duplicate, lossy, and noise.

This rule is honored on NEW tests. Existing PR-numbered tests stay until someone has a concrete reason to touch `smoke_test.py`; they don't merit a dedicated cleanup PR. The cleanup is captured in `## Known Debt` below.

### Smoke-vs-runtime-vs-config separation rule (PR 19b)

Three different things should NOT share a Slack post:

1. **Code behavior tests** (`smoke_test.py`) — deterministic; run at deploy time; failures point at code regressions. Belong in #scout-qa as a SIGNAL.
2. **Runtime state probes** — non-deterministic; depend on external systems (ClickHouse, Anthropic API, Slack auth). Belong in heartbeat / `_compute_health_status`; alert on TRANSITIONS only (OK→degraded, degraded→OK). Never a recurring boot post.
3. **Boot config checks** — required env vars, persistent disk present, etc. Belong in startup logs OR fail-fast at boot. Don't add to the smoke Slack post.

When you find yourself adding a "smoke test" that probes external state (CH up, API valid, file fresh, env var set), STOP — that test belongs in #2 or #3, not #1.

Why: the current `smoke_test.py` mixes all three. The post grows linearly with PRs because every concern lands in the same bucket. This rule stops the next conflated test from being added.

This rule is honored on NEW tests. The 5 existing runtime probes in `smoke_test.py` (Anthropic API auth, ClickHouse query, Offer inventory present, Slack auth.test, Notion queue DB ID) stay until there's a concrete reason to migrate them. The migration is captured in `## Known Debt` below.

### Shared function contract
```python
def _query_ghost_campaigns(ch) -> list[dict]:
    # Returns: list of dicts with keys:
    #   campaign_id, adv_name, campaign_title,
    #   impressions_7d, impressions_2d, clicks_7d, revenue_7d,
    #   first_impression_date, publisher_ids, publisher_names
    ...
```

Always return plain Python dicts — let the caller decide how to format for Slack, Pulse display, etc.

---

## Known Data Quality Issues

### "Major Rocket Real Real" publisher name
- **Publisher ID**: 927 in `mv_adpx_users`
- **Status**: Genuine organization name in ClickHouse — not a Scout artifact
- **Verified**: Apr 23 2026 — 1,388 impressions in last 30 days, active publisher
- **Fix needed**: Platform ops — update organization name in the MS platform for publisher 927 to "Major Rocket" (flag for Vamsee)
- **Do not**: Edit pulse_state.json to rename this — the name comes directly from ClickHouse and will revert next Pulse run

---

## Known Debt

Items deferred from review pipelines (PR 15 reviews + ps-lens hardcoding audit, Apr 2026).
Surfaced automatically at session start via the Session Start Protocol above.

When you start a Scout task, scan this list for items the task touches. If you ship a fix,
remove the item in the same PR. New deferred items go here, not into gstack files.

**[Action — Vamsee] 5 affiliate networks need API credentials on Render to actually fetch offers** — Scout has scraper code for ShareASale, Rakuten, AWIN, Tune (HasOffers), and Everflow but they all silently `return []` when their env vars aren't set. PR 18 trimmed `SUPPORTED_NETWORKS` and `_DIGEST_NETWORKS_FALLBACK` from 9 → 4 to be honest about coverage. To re-enable each network: set the env vars on Render, then add the network back to `SUPPORTED_NETWORKS` (`scout_agent.py`) and `_DIGEST_NETWORKS_FALLBACK` (`scout_digest.py`). `_NETWORK_LABEL` and `_NETWORK_EMOJI` already have all 9 entries — no edit needed there.

Env var checklist:
- ShareASale: `SHAREASALE_API_TOKEN`, `SHAREASALE_API_SECRET` (affiliate ID 3279349 defaulted)
- Rakuten: `RAKUTEN_API_TOKEN` (publisher ID 3948979 defaulted)
- AWIN: `AWIN_API_KEY`, `AWIN_PUBLISHER_ID`
- Tune (per-instance for KASHKICK/BROWNBOOTS/ADACTION/REVOFFERS/ADBLOOM/SUCCESSFUL_MEDIA): `TUNE_<NAME>_NETWORK_ID` + `TUNE_<NAME>_API_KEY`
- Everflow (per-instance for GIDDYUP/ACCIOADS/KLAYMEDIA/CREDITCOM/MWKCONSULTING/PAWZITIVITY/ARAGONPREMIUM): `EVERFLOW_<NAME>_API_KEY` + `EVERFLOW_<NAME>_BASE_URL`

**[Resolved by PR 19] `from_airbyte_campaigns.categories` is NULL — but data is in `c.tags`.** Original framing was wrong (claimed needed upstream fix). Verified Apr 2026: the column is genuinely NULL across all 4,816 rows, BUT real category data lives in `c.tags` as a JSON array. PR 19 rewrites `queries.performance_benchmarks_raw()` to parse tags via `arrayFilter(t -> NOT startsWith(lower(t), 'internal-'), JSONExtract(coalesce(c.tags, '[]'), 'Array(String)'))` — drops `internal-*` system tags (network/channel metadata), keeps real categories. Result: 25+ categories with usable sample sizes light up Tier 2/3 benchmarks. Same pattern applies to `publisher_top_categories()`. SYSTEM_PROMPT DATA DICTIONARY updated with the same SQL pattern so the LLM ad-hoc `run_sql_query` path uses tags too.

**[New schema-deps pattern — PR 19] Boot-time validation against `system.columns`.** `scout_agent._SCHEMA_DEPS` is a list of `(table, column, must_have_data)` tuples for the columns Scout reads. `_validate_schema_deps(ch)` runs on startup (wired into `_run_startup_smoke_test()` in scout_bot.py), confirms each column exists, and (where `must_have_data=True`) confirms it has at least 100 non-null rows. Violations post to #scout-qa. Catches the "Scout reads a column with no data" class of silent failure that bit us with `categories`. When you add a new ClickHouse query to Scout, add the columns it reads to `_SCHEMA_DEPS`. The threshold lives in `_SCHEMA_DEPS_MIN_ROWS` (=100).

**[Resolved by PR 22] Cleanup — smoke test compliance + boot card redesign** — 14 PR-numbered tests renamed to behavior names, 4 runtime probes deleted (CH/offers/Slack/Notion all covered by heartbeat), `format_slack_blocks()` redesigned to 2-block summary card on all-pass and capped failure list on fail, 5 renderer unit tests added, name validator added at `@test()` decorator (fails at import time on PR-numbered names).

**[Cleanup — paired migration with smoke cleanup] Anthropic API auth check needs a replacement home before it can be deleted from `smoke_test.py`.** Currently the only place Scout would notice an Anthropic API key revocation is the smoke test at boot. If we delete the smoke check without adding a replacement, Scout silently 401s on every @mention until a human notices. Required: add an Anthropic ping (a 1-token completion call) to `_compute_health_status()` (or as a daemon similar to `_run_health_heartbeat` if rate-limit concerns) BEFORE removing it from `smoke_test.py`. Same PR.

**[Future] Signal thresholds in `scout_bot.py` SQL queries are still decorative** — PR 18 wired the `digest` and `health` sections of `config/scout_thresholds.json` to actually drive behavior. The `signals` section (fill_rate_min_sessions_7d, ghost_recency_hours, velocity ±%, cap_alert_pct) is surfaced by `@Scout config` but the SQL queries that use them in `_run_pulse_signals()` and `_query_*` functions still hardcode the literal numbers (e.g. `HAVING sessions_7d > 5000`, `> 48 HOUR`). Editing the JSON for those keys is a no-op until each query is parameterised. Wire them via ClickHouse parameter binding when next touching those queries.

**[Future] SYSTEM_PROMPT DATA DICTIONARY may drift from ClickHouse schema** — `from_airbyte_campaigns` was already missing `start_date`, `categories`, `end_date` (caught in PR 8 eng review). No test validates SYSTEM_PROMPT schema against live tables. Fix: schema smoke test that queries ClickHouse for column existence. `scout_agent.py` SYSTEM_PROMPT lines ~820-900.

**[Future] `@test()` category system deferred** — PR 22 added a name validator that rejects PR-numbered test names at import time. The remaining structural gap is test *body* contents: a developer could write `@test("legit-name")` and still call `_get_ch_client()` inside, adding a runtime probe invisibly. The fix is a `category` parameter on `@test` (`"code" | "runtime" | "config"`) with the renderer filtering by category. Deferred because: (a) all 4 current runtime probes are deleted in PR 22, so the risk is absent; (b) adding the parameter requires tagging all ~39 existing tests; (c) it doesn't structurally prevent body-level violations anyway — code review remains the gate. Revisit if runtime probes re-accumulate.

**[Future] SYSTEM_PROMPT body still references network names verbatim** — PR 17c scoped `SUPPORTED_NETWORKS` to tool description strings + docstrings only. SYSTEM_PROMPT line ~430 still requires a manual edit when a network is added or removed. This was intentional — converting the 4300-line SYSTEM_PROMPT to an f-string risks silent format breakage in SQL/JSON examples. Revisit only if the prompt structure is refactored for other reasons.
