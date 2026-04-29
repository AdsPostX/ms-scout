# Scout — Architecture Invariants

## Session Start Protocol

At the start of any Scout session where code changes are expected:
1. Read `## Known Debt` (bottom of this file) and surface items relevant to the current task
2. If the current task resolves a Known Debt item, remove it from the list as part of the PR
3. New deferred items go into `## Known Debt` — not into gstack plan files (those are not auto-loaded into sessions; this file is)

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
| Health heartbeat (PR 15c) | `_run_health_heartbeat()` in `scout_bot.py` | Background daemon every 30 min | Calls `_compute_health_status()` + standalone CH ping. CH ping affects HEARTBEAT only — never the HTTP `/health` probe (Render must not restart on CH outage). Posts one Slack alert on transition to degraded after `_HEALTH_CONSECUTIVE_THRESHOLD` consecutive bad checks; one recovery alert on return to ok. |

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

**[PR16] 35-minute CH startup blind spot** — if ClickHouse goes down during `_HEALTH_HEARTBEAT_WARMUP_SECS` (300s), first detection fires at minute 35. Fix: add startup-time CH check in `_run_startup_smoke_test()` that posts immediately to #scout-qa on failure. `scout_bot.py → _run_startup_smoke_test()`

**[PR16] Dedup suppression count invisible** — `select_offers()` (PR 15a) logs suppression via `log.debug()` but the digest post has no team-visible count. Fix: expose `meta['advertisers_deduped'] = N`; digest footer notes "X duplicate advertisers filtered." `scout_digest.py → select_offers()`

**[PR16] `_DIGEST_NETWORKS` still hardcoded** — when a 10th network is added to `offer_scraper.py`, another PR is required to update the tuple. Fix: derive from `offers_latest.json` keyset at module load with `_DIGEST_NETWORKS_FALLBACK` for the cold-start case. `scout_digest.py` module level.

**[PR16] `required` thread set manually maintained in TWO places** — `_compute_health_status()` (line ~1727) AND `_thread_watchdog` REQUIRED (line ~1795) both have hardcoded thread name sets. Every new daemon requires edits in both. Fix: module-level `_REQUIRED_DAEMONS: set[str] = set()` populated via `.add()` at each daemon start site in `main()`; both checks read from it. `scout_bot.py → main()` + both check sites.

**[PR17] Scoring thresholds scattered + invisible** — `$20 RPM floor`, `n_per_network=3`, `2-per-category`, `2-per-payout-type` in `scout_digest.py`; fill rate 5K/7d, ghost 48h, velocity ±40/20%, cap 90% in `scout_bot.py`. Team cannot audit thresholds without reading source. Fix: `config/scout_thresholds.json` (loaded at startup) + `@Scout config` agent tool that returns current values.

**[PR17] SYSTEM_PROMPT DATA DICTIONARY may drift from ClickHouse schema** — `from_airbyte_campaigns` was already missing `start_date`, `categories`, `end_date` (caught in PR 8 eng review). No test validates SYSTEM_PROMPT schema against live tables. Fix: schema smoke test that queries ClickHouse for column existence. `scout_agent.py` SYSTEM_PROMPT lines ~820-900.

**[PR17] Network lists in 4+ locations in `scout_agent.py`** — `browse_offers` description (line ~1041), `find_offer_replacements` (line ~1161), `run_offer_scraper` description (line ~1381), SYSTEM_PROMPT line 542. Drift risk is permanent. Fix: single `SUPPORTED_NETWORKS` constant at top of `scout_agent.py` referenced from tool description strings and docstrings. SYSTEM_PROMPT body left untouched (f-string conversion is too risky on a 4300-line prompt).
