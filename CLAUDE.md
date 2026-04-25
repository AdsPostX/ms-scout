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
| Revenue opportunities | **Not yet extracted** — see note below | `get_top_revenue_opportunities()` + `_build_opportunity_signal()` | Agent has fuzzy name-matching anti-join; Pulse does not. No confirmed user-visible bug yet. |
| Fill rate | **Intentionally separate** | `get_low_fill_publishers()` (30d/10K threshold) + Pulse signal (7d/5K threshold) | Different thresholds on purpose: Pulse = early warning, Agent = stable analysis. Do not merge. |
| Velocity shifts | Pulse-only | `_build_velocity_signal()` | No agent tool — no divergence risk |
| Cap alerts | Pulse-only | `_build_cap_signal()` | No agent tool — no divergence risk |
| Overnight events | Pulse-only | `_build_overnight_signal()` | No agent tool — no divergence risk |

---

## Rules When Editing Signal Logic

1. **Threshold, window, or filter change** → edit the shared `_query_*()` function only, never in the caller
2. **Adding a field** → add to the shared function SELECT, then consume in both callers
3. **New Pulse signal** → check if an agent tool already computes the same thing; if yes, extract shared function first
4. **New agent tool** → check if the Pulse already computes the same thing; if yes, use or create a shared function

---

## Revenue Opportunities — Outstanding Work

`get_top_revenue_opportunities()` in `scout_agent.py` has fuzzy name-matching in its `active_pairs`
anti-join (prevents recommending "Disney+ and Hulu" when a partner already runs "Disney+").
`_build_opportunity_signal()` in `scout_bot.py` does not. No confirmed user-visible bug yet.

When this causes a bad recommendation, extract `_query_revenue_opportunities(ch)` and wire both callers to it.

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

### scout_bot.py
- **Orchestrator only** — startup, daemon thread launch, SocketMode event routing, Pulse signal runner
- **`_run_pulse_signals()`**: signal computation only — SQL belongs in shared `_query_*()` functions in scout_agent.py, not inline here
- **`_format_pulse_blocks()`**: imported from scout_slack_ui — rendering only, no SQL or business logic
- **`pulse_state.json`** is written by the bot at runtime — don't put static facts here
- **Client instances** (WebClient, ClickHouse) are created here in `main()` and passed as parameters to modules — never imported from scout_bot (circular import)
- **`_BOT_USER_ID` and `_LAST_THREAD_PER_CHANNEL`** are injected into scout_handlers via `_set_bot_user_id()` and `_set_thread_state()` after auth — this is the circular-import workaround

### scout_handlers.py
- **All `_handle_*` functions** live here: approve, reject, DM, block_action routing
- **Import DAG**: `scout_handlers → scout_slack_ui, scout_notion, scout_state, scout_agent` — never imports from `scout_bot`
- **Add `elif action_id == "..."` for each new button** in `_handle_block_action`; always thread-dispatch heavy operations
- **`_update_brief_card_queued`** is defined here (NOT in scout_notion) — updates the Slack digest card after an offer is added to queue

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
