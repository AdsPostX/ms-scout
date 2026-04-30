# Plan: PR 23 — Slack as Front Door + Notion as Source of Truth

## Problem Statement

The offer approval flow has a visibility gap: once an offer is approved, its pipeline progress (Awaiting Entry → In Platform → Test Offer ON → Live) lives entirely in Notion. Ops has to leave Slack to know where anything stands.

**What already exists:**
- `app_home_opened` handler in `scout_handlers.py` already calls `web.views_publish(user_id, _build_home_view())`
- `_build_home_view()` + `_build_home_queue_section()` in `scout_slack_ui.py` already render the Home tab
- `get_demand_queue_status()` tool in `scout_agent.py` already exists (reads local `launched_offers.json` + ClickHouse impressions)
- `_check_notion_queue_changes()` in `scout_notion.py` already reads Notion DB for status changes and posts to #scout-offers

**What's missing:**
- `_build_home_queue_section()` reads from local state (`launched_offers.json`) — static after approval, no pipeline status
- No Notion READ path that returns structured queue items grouped by stage
- `get_demand_queue_status()` doesn't reflect actual pipeline stage (Awaiting Entry vs In Platform vs Live) — only ClickHouse impression detection

**User intent:** Slack is the front door (ops lives there), Notion is the source of truth (all copy + pipeline status). When an op opens Scout in the left nav, they see the live queue. When they type @Scout queue, they get the same view.

---

## Proposed Architecture

```
app_home_opened event (any user opens Scout in left nav)
    └─ _fetch_notion_queue_items() [scout_notion.py]
        ↓ returns list of queue items with pipeline status
    └─ _build_queue_card(items) [scout_slack_ui.py]
        ↓ returns Block Kit blocks grouped by status
    └─ views.publish(user_id, home_view) [scout_handlers.py]

@Scout "queue" / "what's in the queue" / "pipeline"
    └─ get_queue_status() tool [scout_agent.py]
        └─ _fetch_notion_queue_items() [shared with Home tab path]
        └─ _build_queue_card(items) [shared render function]
        └─ post message to channel
```

Notion = source of truth. Slack Home = live pull-on-demand view. No sync, no cache, no Slack Lists.

---

## What Already Exists (No Change)

- `app_home_opened` handler (scout_handlers.py ~line 1372) — already calls `views.publish`; only needs to inject Notion data
- `_build_home_view()` (scout_slack_ui.py ~line 1546) — already renders home; needs to accept injected queue blocks
- `_check_notion_queue_changes()` (scout_notion.py ~line 708) — already reads Notion DB for change detection; `_fetch_notion_queue_items()` will share the same query pattern

---

## Proposed Changes

### Change 1: Add `_fetch_notion_queue_items()` in scout_notion.py

**Env var correction:** Use `NOTION_QUEUE_DB_ID` (not `NOTION_DEMAND_QUEUE_DB_ID`) — matches all existing call sites in `scout_notion.py` (lines 481, 799) and `scout_agent.py` (line 3928). Using the wrong name silently returns `[]` on every call with no error.

**Timeout:** Set `timeout=5` on the Notion API call. This function is called in the `app_home_opened` hot path — a hung Notion connection blocks the home tab render indefinitely. 5s is sufficient under normal conditions and fails fast under degraded conditions.

**Rate limit protection:** Add a module-level TTL cache keyed to the function, not per-user: `_QUEUE_CACHE_TTL_SECONDS = 30` (named constant, not magic number) + `_queue_items_cache = {"ts": 0, "items": None}`. When `app_home_opened` fires for 5+ simultaneous users, they all get the cached result from the first Notion call. Cache miss only after 30s. This caps Notion API calls to 2/min regardless of how many ops open Scout simultaneously.

**Error state:** Return `None` on any Notion API error (timeout, 429, network failure), return `[]` for genuinely empty queue. Callers distinguish these: `None` → render "_(Could not reach Notion — queue data unavailable)_"; `[]` → render "Queue is clear — nothing awaiting entry."

**Returns:** `list[dict] | None` — one dict per offer:
```python
{
    "page_id": str,
    "advertiser": str,
    "payout": str,      # e.g. "$45 CPA"
    "network": str,
    "status": str,      # "Awaiting Entry" | "In Platform" | "Test Offer ON" | "Live" | "Rejected"
    "notion_url": str,
    "approved_by": str,
    "approved_at": str, # ISO date
    "category": str,    # empty string if not set
}
```

**Implementation notes:**
- Uses existing Notion API pattern from `_write_to_notion_queue()` — same DB ID via `NOTION_QUEUE_DB_ID`
- Filters: exclude Rejected items by default (show active pipeline only)
- Zero Slack calls (consistent with Zero-Slack-calls rule in `scout_notion.py`)

### Change 2: Add `_build_queue_card()` in scout_slack_ui.py + delete `_build_home_queue_section()`

New Block Kit render function. Accepts `items: list[dict]` from `_fetch_notion_queue_items()`.

**Output format:**
```
📋 *Demand Queue — 3 offers*
━━━━━━━━━━━━━━━━━━━━━━━━
🟡 Awaiting Entry
   Nike MX — $45 CPA · Impact  →  [Notion ↗]
🔵 In Platform
   Disney+ — $8.50 CPL · MaxBounty  →  [Notion ↗]
✅ Live
   Capital One — $65 CPA · CJ  →  [Notion ↗]
```

**Status emoji map:**
- `Awaiting Entry` → 🟡
- `In Platform` → 🔵  
- `Test Offer ON` → 🟠
- `Live` → ✅
- `Rejected` → ❌ (only shown if include_rejected=True)

**Empty state:** "Queue is clear — nothing awaiting entry or in platform."

**Block Kit constraints:** Uses canonical primitives from scout_slack_ui.py (no naked `section.fields`, no NBSP, no `·` separators between items). One `_build_item_card()` per offer.

**Block count cap:** `_build_home_view()` has a fixed structure of ~14 blocks. Budget for queue card: 36 blocks. With 4 status groups (2 header blocks each) + 2 blocks per offer item, cap at `_MAX_QUEUE_ITEMS_RENDERED = 12` offers (named constant, not magic number): 12×2 + 4×2 = 32 blocks. Beyond 12: append a single "…and N more — [View full queue in Notion ↗]" context block. This mirrors `_ALWAYS_TAIL` in `_format_pulse_blocks()`.

**Error state rendering:** If caller passes `items=None` (Notion error), render: "_Could not reach Notion — queue data unavailable_" (italic context block, not the "Queue is clear" text).

**Dead code removal:** Delete `_build_home_queue_section()` in this same PR. It reads from `launched_offers.json` (no pipeline status), is replaced entirely by `_build_queue_card()`, and has no other callers. Leaving it risks future reuse by accident.

### Change 3: Update `app_home_opened` handler in scout_handlers.py

Update the handler (~line 1372) to:
1. Call `_fetch_notion_queue_items()` from `scout_notion`
2. Pass items to `_build_home_view(queue_items=items)`

`_build_home_view()` passes them to `_build_queue_card()` instead of the current `_build_home_queue_section()`.

**Error boundary:** `_build_home_view()` accepts `queue_items: list | None = None`. If `None` (Notion error), still publish the Home view — render the error state in the queue section, not a blank page. If `[]` (empty), render "Queue is clear."

**`/scout-queue` slash command:** Update the `/scout-queue` handler in `scout_handlers.py` to call `_fetch_notion_queue_items()` + `_build_queue_card()` — same as the Home tab path. Currently it calls `get_demand_queue_status()` (ClickHouse-based). After this PR, `/scout-queue`, `@Scout queue`, and the Home tab all show the same Notion-based pipeline view. Three surfaces, one truth.

### Change 4: Add `get_queue_status()` tool in scout_agent.py

New agent tool alongside existing `get_demand_queue_status()`.

**Distinction:**
- `get_demand_queue_status()` (existing) — ClickHouse-based live detection via impressions since approval. Answers "has this offer started running?"
- `get_queue_status()` (new) — Notion-based pipeline view. Answers "where is each offer in the approval-to-live pipeline?"

**Registration (all 4 required pieces):**
1. `TOOLS` list entry with name, description, `input_schema: {"type": "object", "properties": {}}` (no parameters — reads full queue)
2. `TOOL_MAP` entry: `"get_queue_status": get_queue_status`
3. `SYSTEM_PROMPT` routing: Update **Intent 2** (currently routes "queue", "what's approved", "what's queued" → `get_demand_queue_status`) to route those triggers to `get_queue_status` instead. Narrow `get_demand_queue_status` to "is X live?", "has X started running?", "impressions since approval" — ClickHouse-specific queries only.
4. Function `get_queue_status()` (no `input` arg — matches zero-arg tool pattern like `get_demand_queue_status`)

**Import in function body (not top-level):** `get_queue_status()` must import `_build_queue_card` from `scout_slack_ui` inside the function body, not at the top of `scout_agent.py`. Reason: `scout_slack_ui.py` already imports from `scout_agent` (line 1556) inside a function body to avoid circular imports at module load. A top-level `from scout_slack_ui import` in `scout_agent.py` creates a circular import that crashes at boot.

**Output:** Returns structured dict when LLM needs to reason, or directly posts the Block Kit card to channel.

### Change 5: Tests — structural split (smoke_test.py + stdlib unittest)

**The architectural decision:** `smoke_test.py` and `tests/` serve different purposes. Conflating them caused the bloat PR 22 fixed. Adding 6 more behavioral tests back into `smoke_test.py` would undo that fix.

**Why `unittest` not `pytest`:** `requirements.txt` is Scout's single install file — `render.yaml` runs `pip install -r requirements.txt` on every deploy. `pytest` would ship to Render production and add `iniconfig`, `pluggy`, `packaging` as transitive deps. `unittest` is stdlib — zero new dependency, already used in `smoke_test.py` (it imports `unittest.mock`). The 5 behavioral tests have no pytest-specific features (no `@pytest.mark.parametrize`, no fixtures, no plugins). `unittest.TestCase` handles all 5 cleanly. If the test suite grows to the point where pytest pays off, migrate then — but that decision belongs with the growth, not the first 5 tests.

**Rule:** `smoke_test.py` = boot invariants only (deploy-time signal). Never grows with features, only with new boot-time invariants. `tests/` = behavioral/unit tests for specific functions. Grows with each feature.

**`smoke_test.py` — 1 new test (boot invariant):**
- `get_queue_status_tool_registered_with_all_contract_pieces` — TOOLS list + TOOL_MAP + SYSTEM_PROMPT routing all present. Pattern mirrors existing `get_scout_config_registered_with_all_4_contract_pieces` at line 729.

**`tests/test_queue_card.py` — 5 behavioral tests (`unittest.TestCase`):**
1. `test_empty_state_shows_queue_clear_message` — `_build_queue_card([])` → contains "Queue is clear"
2. `test_error_state_shows_notion_unavailable_not_clear` — `_build_queue_card(None)` → contains "Could not reach Notion", NOT "Queue is clear"
3. `test_items_grouped_by_status_with_correct_emoji` — items with known statuses → correct emoji per group
4. `test_unknown_status_falls_back_gracefully` — `status="Surprise Status"` → renders without crash, uses fallback emoji
5. `test_queue_card_block_count_under_budget_with_12_offers` — `_build_queue_card(12_items)` → `len(blocks) <= 36` (the queue card's block budget, not `_build_home_view()` — avoids triggering `scout_agent` import chain via `_build_home_view` line 1556)

**Test 5 isolation note:** The plan intentionally tests `_build_queue_card(items)` block count, NOT `_build_home_view(queue_items=items)`. Calling `_build_home_view()` triggers a function-body import of `scout_agent` at line 1556, which pulls in Anthropic, ClickHouse, and `.env` state — not a pure unit test. The queue card budget is 36 blocks (50 limit − 14 fixed blocks); testing the card directly validates the cap without the agent import side-effect.

**New infrastructure (minimal):**
- `tests/test_queue_card.py` — 5 tests above (no `tests/__init__.py` — not needed; Python discovers unittest tests by module path)
- **No new dependency** — `python3 -m unittest discover -s tests -p "test_*.py"` works with stdlib

**Run command (always from project root):**
`python3 -m unittest discover -s tests -p "test_*.py" -v`

**Verification command added to CLAUDE.md PR Definition of Done:**
`python3 -m unittest discover -s tests -p "test_*.py"` — must pass alongside `python3 smoke_test.py`

**Renderer test migration (in this PR):** The 5 renderer tests from PR 22 (`format_slack_blocks`) are behavioral tests currently in `smoke_test.py`. They belong in `tests/test_boot_card.py`. Since PR 23 creates `tests/` for the first time, this is the right moment — shipping `test_queue_card.py` without migrating `test_boot_card.py` would leave the directory half-done from day one.

Migration: create `tests/test_boot_card.py`, `from smoke_test import format_slack_blocks`, rewrite each `@test()` as a `unittest.TestCase.test_*()` method (mechanical — identical assertions), delete the 5 from `smoke_test.py`. Verify `smoke_test.py` imports cleanly without triggering test execution (it should — runner is called via `run_all_tests()`, not at import time).

### Change 6: CLAUDE.md + Known Debt update

- **Keep** "campaign_builder.py PARKED" Known Debt entry until Vamsee Idaho meeting (~2026-05-06) resolves the API fork decision
- Add to Signal Map: `get_queue_status()` — Notion-based pipeline view (no shared `_query_*` function — Notion not ClickHouse)
- Note: `_fetch_notion_queue_items()` is the Notion read counterpart to `_write_to_notion_queue()`
- Add Known Debt: `get_pipeline_health()` in `scout_agent.py` also reads the Notion Demand Queue DB independently — consolidate to call `_fetch_notion_queue_items()` in PR 24
- Add `python3 -m unittest discover -s tests -p "test_*.py"` to PR Definition of Done verification checklist

---

## Files Touched

| File | Changes |
|---|---|
| `scout_notion.py` | Add `_fetch_notion_queue_items()` (with TTL cache + timeout=5 + `None`/`[]` error distinction) |
| `scout_slack_ui.py` | Add `_build_queue_card()`, delete `_build_home_queue_section()`, update `_build_home_view(queue_items=None)` |
| `scout_handlers.py` | Update `app_home_opened` handler; update `/scout-queue` handler to use Notion-based path |
| `scout_agent.py` | Add `get_queue_status()` + TOOLS + TOOL_MAP + SYSTEM_PROMPT routing |
| `smoke_test.py` | 1 new test (tool registration boot invariant only) |
| `tests/test_queue_card.py` | New — 5 behavioral unittest tests for `_build_queue_card()` (no new dependency) |
| `tests/test_boot_card.py` | New — 5 renderer tests migrated from `smoke_test.py` (rewritten as `unittest.TestCase`; `smoke_test.py` importable, runner not triggered at import) |
| `CLAUDE.md` | Signal Map update + Known Debt cleanup + add `python3 -m unittest discover` to PR Definition of Done |

---

## Out of Scope

- `campaign_builder.py` — PARKED pending Vamsee API conversation (Idaho meeting ~2026-05-06)
- Slack Lists — Two independent API spikes confirmed not viable. First spike used `lists.*` (internal). Second spike used `slackLists.*` (documented Web API, 2026-04-30): list creation works, items create but `fields: []` always empty in response, `slackLists.items.list` + `slackLists.items.info` blocked by missing `lists:read` scope, `slackLists.items.update` cell value format undocumented (all tried field names rejected as `invalid additional property`). To revisit: add `lists:read` + `lists:write` scopes to app manifest and reinstall, then re-spike.
- Write-back from Slack to Notion (Slack edits updating Notion) — deferred to PR 24
- `get_demand_queue_status()` — keep as-is (ClickHouse-based live detection is a different question; both tools coexist)
- `_post_offer_queue_card` mrkdwn → Block Kit refactor — intentionally dense per prior decision

---

## PR 23 Micro-Add: `chat.postEphemeral` for Approval Confirmations

Currently the approval confirmation ("✅ Approved Nike MX") posts visibly to the channel. Swap to `chat.postEphemeral` — confirmation is only visible to the approving user. Zero new infrastructure, one change in `_handle_approve()` in `scout_handlers.py`. Reduces noise in #revenue-operations on days with multiple approvals.

**Change:** In `_handle_approve()`, replace the confirmation `web.chat_postMessage(channel=..., text="✅ Approved...")` with `web.chat_postEphemeral(channel=..., user=user_id, text="✅ Approved...")`. Notion write, digest card update, and queue flush all stay as-is.

---

## Slack API Opportunities (Future PRs)

Reviewed against the full Slack Web API method list (2026-04-30). Check this list before planning any Scout PR — something here may slot in cleanly.

### High Value — PR 24/25 candidates

**`conversations.canvases.create` / `canvases.edit`** — Slack Canvas as ambient pipeline board. A channel canvas in #revenue-operations is always visible as a pinned tab — no user action required, shared across the whole team. Simpler write API than Slack Lists (edit sections, not structured rows). Spike this before committing to any Slack Lists re-attempt. This may be the "always visible" ambient view that Slack Lists promised but couldn't deliver.

**`views.push`** — Drill-down from Home tab queue card. Clicking an offer pushes a detail view (full specs + approve/reject buttons) without leaving the Scout Home tab. Post-PR 23 when queue card exists. Makes approvals possible from the queue view instead of having to find the digest card in channel.

**`reminders.add`** — Scout sets a Slack reminder for ops: "Nike MX has been Awaiting Entry for 48h — was it entered in the platform?" Closes the loop on stuck pipeline items proactively, without a new daemon. Natural PR 25 addition alongside Notion write-back.

**`assistant.*` (setStatus, setSuggestedPrompts, setTitle)** — Scout as a Slack AI Assistant. Puts Scout in the AI sidebar with suggested prompts ("What's in the queue?", "Show ghost campaigns"). Requires enabling "Agent" capability in app manifest. Major UX upgrade when team is ready for it.

### Medium Value — Future Consideration

**`slackLists.*` with `lists:read` scope** — Already documented in Out of Scope. Path forward: add `lists:read` + `lists:write` to app manifest → reinstall app → re-spike `slackLists.items.create` + `slackLists.items.list` to verify field persistence. If the canvas spike fails, revisit this.

**`search.messages`** — `@Scout search "Nike"` → searches Scout's message history in #revenue-operations. New agent tool: `search_scout_history(query)`. Low effort, useful for ops who want to find prior Scout analysis on a specific advertiser.

**`users.profile.get`** — Enrich "approved by" in the queue card. Notion stores user IDs or emails; this would convert to display names for cleaner queue rendering.

**`files.upload`** — Weekly queue CSV or performance report uploaded directly to #revenue-operations. Better than long messages for data-heavy content. Alternative delivery for the weekly digest.

**`apps.datastore.*`** — Slack's hosted key-value store. Could replace `launched_offers.json`, `pulse_state.json`, `entity_overrides.json` with Slack-managed persistent storage. Requires migrating off SocketMode to Bolt. Long-term architectural option if Scout outgrows file-based state.

### Low Value — Skip Unless Obvious Need

**`reactions.add`** — Scout adds ✅ when offer goes live, 🔴 on Pulse warnings. Cute but low signal per engineering effort.

**`pins.add`** — Pin Pulse messages to #revenue-operations. Bookmarks are better.

**`chat.scheduleMessage`** — Not a win for Scout: Pulse needs ClickHouse at post time, so content can't be pre-baked. Daemons stay.

---

## Verification

1. `python3 smoke_test.py` — PASSED N/N, 0 failures
1b. `python3 -m unittest discover -s tests -p "test_*.py" -v` — PASSED N/N, 0 failures
2. Open Scout in Slack left nav — Home tab shows queue grouped by pipeline stage
3. Type `@Scout what's in the queue` — same queue card posts in channel
4. Approve a test offer → Notion page created → open Scout Home → offer appears as "Awaiting Entry"
5. Manually change Notion status to "In Platform" → re-open Scout Home → status reflects change
6. Note for ops: Notion changes take up to 30 seconds to appear in Scout Home (TTL cache). This is expected behavior — not a bug.

---

## Architecture Corrections (from CEO + Eng reviews, 2026-04-30)

**A — Env var was wrong in original plan.** `NOTION_DEMAND_QUEUE_DB_ID` does not exist — the live env var is `NOTION_QUEUE_DB_ID`. Using the wrong name silently returns `[]` on every call. Fixed in Change 1.

**B — `/scout-queue` slash command was missing from scope.** After this PR, three surfaces would have answered the same question with different data sources. Fixed: Change 3 now explicitly updates the `/scout-queue` handler in addition to the Home tab.

**C — Notion API rate limits on concurrent `app_home_opened` events.** At 5+ simultaneous users (e.g., standup), concurrent Notion calls can hit the 3 req/s limit — silently returning empty queue data with no log differentiation from "actually empty." Fixed: 30-second in-process TTL cache added to `_fetch_notion_queue_items()`.

**D — `None` vs `[]` error distinction required.** Returning `[]` on Notion error is indistinguishable from a genuinely empty queue. Fixed: `_fetch_notion_queue_items()` returns `None` on error, `[]` on empty. `_build_queue_card()` renders different text for each case.

**E — Circular import risk if `scout_agent.py` imports `scout_slack_ui` at top level.** `scout_slack_ui.py` already imports from `scout_agent` (line 1556) inside a function body. A matching top-level import in `scout_agent.py` creates a circular import at boot. Fixed: `get_queue_status()` imports `_build_queue_card` inside the function body (deferred import).

**F — Block Kit 50-block limit with full queue.** With 4 status groups + 15 offers, `_build_queue_card()` generates ~40 blocks — plus the 14-block fixed Home view structure = 54 total, silently truncated by Slack. Fixed: cap at 12 rendered offers; overflow becomes a "…and N more" context block.

**G — `_build_home_queue_section()` dead code risk.** Leaving the old local-state-based function alongside the new Notion-based one invites accidental reuse. Fixed: explicitly deleted in Change 2.

**H — `get_queue_status()` input_schema clarified.** Tool takes no parameters: `{"type": "object", "properties": {}}`. Function signature is `get_queue_status()` with no args (matches existing zero-arg tool pattern). Fixed in Change 4.

**I — Intent 2 routing ambiguity.** Two tools with nearly identical trigger phrases ("queue", "pending", "pipeline") would leave routing to LLM judgment. Fixed: Change 4 explicitly updates Intent 2 to route queue/pipeline → `get_queue_status()` and narrows `get_demand_queue_status()` to ClickHouse-based impression queries only.

**J — `timeout=5` on Notion call in hot path.** No timeout means a hung Notion connection blocks the home tab render indefinitely on Render. Fixed in Change 1.

---

## Decision Audit Trail

| # | Phase | Decision | Classification | Principle | Rationale | Rejected |
|---|-------|----------|-----------|-----------|-----------|---------|
| 1 | Arch | Read from Notion on demand, no local sync | Mechanical | P1+P2 | Notion is source of truth; pull-on-demand via app_home_opened eliminates stale state risk | Cache Notion status in launched_offers.json |
| 2 | Arch | Shared `_fetch_notion_queue_items()` + `_build_queue_card()` for both paths | Mechanical | P2 | One function per signal — same data, same render, no drift | Separate implementations per path |
| 3 | Scope | Keep `get_demand_queue_status()` unchanged | Mechanical | P3 | It answers a different question (has offer started running?) vs pipeline stage; both coexist | Replace with Notion-based tool |
| 4 | Spike | Slack Lists architecture abandoned after 2 independent spikes | Mechanical | P1 | Spike 1 (lists.* internal): empty fields, unknown_method on update. Spike 2 (slackLists.* Web API, 2026-04-30): list creation works, items create but fields always empty, read blocked by missing lists:read scope, update cell value format undocumented. Both spikes hit same core gap. Path forward if needed: add lists:read scope + reinstall app, then re-spike. | Build on Slack Lists |
| 5 | Scope | campaign_builder.py untouched | Mechanical | P3 | PARKED — Playwright janky; correct path is MS platform API (Vamsee meeting) | Wire Playwright in this PR |
| 6 | Arch | app_home_opened event (pull) not proactive push | Mechanical | P1+P5 | Slack fires event for every user click — no user ID list needed, always fresh | Push to fixed SCOUT_OPS_USER_IDS list |
| 7 | Testing | unittest over pytest for behavioral tests | Mechanical | P5 | `requirements.txt` is single file shipped to Render production — pytest becomes a runtime dep; unittest is stdlib, already used via `unittest.mock`, no new dependency, sufficient for 5 tests with no pytest-specific features | Add pytest to requirements.txt |
| 8 | Testing | Test 5 tests `_build_queue_card()` block count, not `_build_home_view()` | Mechanical | P5 | Calling `_build_home_view()` triggers deferred `scout_agent` import (line 1556) — not a pure unit test; testing the card directly validates the cap without import side-effect | Test full home view to "be thorough" |

---

## GSTACK REVIEW REPORT

| Review | Run | Status | Findings |
|--------|-----|--------|---------|
| CEO | 2026-04-30 (round 1) | issues_resolved | 2 critical, 2 high, 3 medium — all incorporated |
| Eng | 2026-04-30 (round 1) | issues_resolved | 4 high, 3 medium, 1 low — all incorporated |
| CEO | 2026-04-30 (round 2) | issues_resolved | 0 critical, 2 high (pytest revert risk + TTL stale note), 3 medium — addressed via named constants, stale note, unittest decision |
| Eng | 2026-04-30 (round 2) | issues_resolved | 2 critical (pytest ships to Render + test 5 triggers agent import), 2 high, 3 medium — addressed: switched to unittest, test 5 scoped to `_build_queue_card()` only, `tests/__init__.py` removed |
| Design | skipped | — | No UI scope beyond Block Kit card |
