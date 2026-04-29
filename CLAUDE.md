# Scout — Architecture Invariants

## The Core Rule: One Function Per Signal

Scout computes signals in two places:
1. **Agent tools** (`scout_agent.py`) — called when a user asks @Scout a question
2. **Pulse signals** (`scout_bot.py` `_run_pulse_signals()`) — computed at 8am

When the same business logic exists in BOTH, they MUST call a shared `_query_*()` function.
Duplicate SQL guarantees drift. This happened with ghost detection in Apr 2026 — the Pulse
missed a 48h recency filter that was added to the agent tool, causing false alarms.

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

---

## Rules When Editing Signal Logic

1. **Threshold, window, or filter change** → edit the shared `_query_*()` function only, never in the caller
2. **Adding a field** → add to the shared function SELECT, then consume in both callers
3. **New Pulse signal** → check if an agent tool already computes the same thing; if yes, extract shared function first
4. **New agent tool** → check if the Pulse already computes the same thing; if yes, use or create a shared function

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

## Engineering Principles (read this first)

Five principles prevent the recurring failure modes in Scout's history:

**P1 — Validate at the boundary.**
- Module boundaries: all stdlib imports at file top (not inside functions) — `import json` inside a function crashes silently at runtime
- API boundaries: every Slack/Anthropic/ClickHouse/Notion call wrapped in `try/except` with a safe fallback — uncaught exceptions drop the entire handler
- Config boundaries: every configurable threshold read from `config/team_corrections.json` or env vars at module load, not hardcoded mid-function — hardcoded values are ignored when config changes
- Data boundaries: never assume upstream columns are populated; use `NULL`-safe SQL (`coalesce`, `nullIf`, `arrayFilter`); validate schema at boot before querying

**P2 — One source of truth per concept.**
- One function per signal: shared `_query_*()` functions called by both Agent tools and Pulse — duplicate SQL guarantees filter drift (this happened with ghost campaigns, Apr 2026)
- One daemon name set: currently defined in two places in `scout_bot.py` — unify before adding a third; divergence causes missed health alerts
- One network list: `_scraper_networks()` in `offer_scraper.py` — not a hardcoded list in `scout_digest.py`
- One config store: `config/team_corrections.json` for static platform facts, `data/entity_overrides.json` for entity facts — not inline constants in SQL strings

**P3 — Read before building.**
- Before planning any new Scout capability: read `scout_agent.py` SYSTEM_PROMPT, TOOLS, TOOL_MAP — the feature may already exist (this happened with the custom query tool, Apr 2026)
- Before adding a new query: check if a `_query_*()` equivalent already exists in `scout_agent.py`
- Before adding a new daemon: check what daemons already exist in `scout_bot.py`
- Corollary: proposing code that duplicates existing functionality is a defect, not a feature

**P4 — Tests describe behaviors, not incidents.**
- Test names describe what the code does, never which PR added it or when it broke
- No test calls a live external API (ClickHouse live, Slack live, Anthropic live) — live calls are health probes, not smoke tests; CI can't control external availability
- Every new `_query_*()` function: one test. Every new daemon: one test. Every new config key: one test proving behavior changes when config changes.
- "It's a small change" is the most common rationalization for skipping tests. It is never correct.

**P5 — Self-heal, don't report chores.**
- **Any Slack message to the user that says "run X" or "Action required" is a defect.** Scout should detect staleness and refresh automatically.
- Failures must degrade gracefully: return empty state with `has_error=True`, not crash the entire handler
- Health transitions (healthy → degraded) alert once, not every Pulse cycle — repeated identical alerts train users to ignore them
- Boot-time warmup + background refresh daemon + read-path self-heal: this is the pattern for any cached data (benchmarks, offers, entity overrides)

---

## Scout PR Definition of Done

Before marking any Scout PR complete, verify ALL of the following:

- [ ] `python smoke_test.py` passes with 0 failures, 0 errors — paste the output count into the PR description
- [ ] No new test names contain PR numbers, fix labels, or dates — test names describe behavior only
- [ ] No new live API calls in `smoke_test.py` — health probes belong in `_compute_health_status()`, not the test suite
- [ ] Every new `_query_*()` function has a corresponding smoke test
- [ ] Every new config-driven threshold has a test proving the behavior changes when the config value changes (monkey-patch pattern)
- [ ] Import DAG unchanged: run `grep -rn "from scout_bot import" scout_handlers.py` — must return empty
- [ ] Block Kit canonical primitives used (no naked `section.fields`, no NBSP padding `\xa0`, no `·` separators)
- [ ] No "Action: run X" or "Action required" messages added to user-facing Slack output
- [ ] Signal Map updated if a new signal was added or an existing one was changed
- [ ] Known Debt table updated if any item was resolved or a new one discovered

**Enforcement note**: `smoke_test.py` is not wired to CI yet. Until it is, running it manually and pasting the output is the gate. Skipping it because "it's a small change" is how the last 3 production breaks happened.

---

## Known Debt (prioritized)

| Priority | Item | Effort | Risk if deferred |
|---|---|---|---|
| P0 | Rename PR-numbered test names to behavior descriptions | 1h | CI output misleads on what failed |
| P0 | Move live API calls (Anthropic credential check, ClickHouse ping) out of `smoke_test.py` into `_compute_health_status()` | 2h | CI fails on network unavailability, not on code regressions |
| P1 | Block Kit 50-block hard limit: add an assertion in `smoke_test.py` that `_format_pulse_blocks()` never exceeds 50 blocks | 2h | Slack silently drops messages with no error |
| P1 | Unify the daemon name set: create one canonical set in `scout_bot.py` used by both `_compute_health_status()` and `_thread_watchdog()` | 3h | New daemons added to one place go unmonitored by the other |
| P2 | SYSTEM_PROMPT DATA DICTIONARY schema drift: add a boot-time check that verifies ClickHouse columns referenced in the DATA DICTIONARY still exist | 4h | Schema changes make Intent 21 queries silently wrong |
| P2 | Button value contract validation: add a test that key names in `scout_slack_ui.py` match the parser in `scout_handlers.py` | 3h | Key renaming breaks approved offers with no error |

**Owner**: Sidd reviews priority; Chris (SE) executes P0/P1 items. P2 items are architectural — flag to Sidd before starting.

---

## Known Data Quality Issues

### "Major Rocket Real Real" publisher name
- **Publisher ID**: 927 in `mv_adpx_users`
- **Status**: Genuine organization name in ClickHouse — not a Scout artifact
- **Verified**: Apr 23 2026 — 1,388 impressions in last 30 days, active publisher
- **Fix needed**: Platform ops — update organization name in the MS platform for publisher 927 to "Major Rocket" (flag for Vamsee)
- **Do not**: Edit pulse_state.json to rename this — the name comes directly from ClickHouse and will revert next Pulse run
