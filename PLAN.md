# Scout Codebase Cleanup — Reduce Without Breaking

**Date:** 2026-06-05
**Branch:** `claude/wonderful-visvesvaraya-05aa86`
**Status:** APPROVED-PENDING-USER — autoplan 2026-06-05 (CEO + Eng review complete, corrections applied)
**Worktree:** `/Users/siddharthshah/code/ms-scout/.claude/worktrees/wonderful-visvesvaraya-05aa86/`

---

## Problem in One Sentence

Scout has grown to 28,088 lines across 18 files over 222 PRs; dead code and copy-paste structural patterns have accumulated that can be removed with zero behavior change (~450–550 lines, Phases 0–3).

---

## Context

Comprehensive codebase audit (2026-06-05) across all files using parallel subagents. Scout earns a **B+** overall — no broken logic beyond one shadowed function, clean separation of concerns, good naming. The accumulation is structural duplication from iterative shipping, not design failure.

**Post-review corrections applied (do not re-draft):**
- Phase 0 line range corrected: L1666–L1769 (not L1666–L1896 — L1771 is live `publisher_fleet_health_stats` called by v2)
- `import difflib` REMOVED from Phase 1 — used at L2937/L2943 for fuzzy-match hints
- `_seed_feedback_reactions`: 9 call sites exist and must also be deleted
- Payout normalization map merge REMOVED — two different maps for different pipeline stages; merging breaks scraper normalization
- Daemon factory scoped to 4 identical daemons only (not 7)
- Alert formatter savings corrected: ~18 lines (not ~120)
- SQL helper: column is `created_at` not `event_time`; ~11 parameterized occurrences (not ~30); `_ch_date_filter` extraction deferred
- Pulse handlers description corrected: they call `ask()` with query strings, not dismiss handlers
- Dispatch table pulled from Phase 4 into Phase 3 (co-located with other scout_handlers.py changes)
- Added Phase 0 smoke test prerequisite (fleet health has zero coverage in current smoke_test.py)

---

## Scope

**In scope:**
- Dead code deletion (no logic change)
- Intra-file helper deduplication (pure refactor)
- Structural factory patterns for confirmed copy-paste patterns
- Revenue trend SQL deduplication (two parallel query blocks)

**Out of scope:**
- `import difflib` (used for fuzzy matching — keep)
- Payout type normalization map merge (two different maps, different pipeline stages — keep separate)
- `_ch_date_filter` SQL helper extraction (deferred — inconsistent column names, ~11 occurrences, not worth the silent regression risk yet)
- `ask()` full decomposition (Phase 4 — deferred, high risk, separate PR)
- `_build_brief_blocks()` full split (Phase 4 — deferred)
- Network adapter base class (Phase 4 — deferred)
- ScoutKit public API changes (`Card`, `wrap_response`, `Severity`, `Surface`, `ResponsePattern`)
- `alert_registry.py` → Redis migration (blocked on App Home PR 2)
- PR #161 Bug 3 root cause (`score_offer()` returning None)

---

## Implementation Plan

### Phase 0 (Prerequisite + Correctness Fix): Delete shadowed fleet health v1

**Files:** `queries.py`

**Framing: This is a correctness fix, not cleanup.** The old `get_publisher_fleet_health_data()` at L1666–L1769 is the pre-v2 percentage-based implementation, completely shadowed by the σ-based v2 at L1898 (shipped in PR #221). The sole caller (`scout_agent.py:5207`) passes `days=` kwargs and uses v2's return schema (`total_gap`, `act_now`, `watch`, `platform_alarm`), which v1 never emitted. If anyone edits the wrong definition in a future PR, fleet health silently regresses to the % baseline.

**CRITICAL boundary:** L1771 is the start of `publisher_fleet_health_stats()` — a live function called by v2 at L1926. Do NOT delete past L1769.

**Steps:**
1. Delete L1666–L1769 (old `get_publisher_fleet_health_data` v1 only — ~104 lines)
2. The `_trend()` at L1645 is inside `publisher_revenue_trends()` (live) — do NOT touch
3. The `_trend()` at L2055 is inside `advertiser_revenue_trends()` (live) — do NOT touch
4. **Add smoke test coverage first:** fleet health has zero coverage in smoke_test.py. Before deleting, add a test that imports `queries` and verifies `get_publisher_fleet_health_data` is callable with the v2 signature (`ch, days, min_windows, min_revenue, act_now_sigma, watch_sigma`), and that it returns a dict with `act_now`, `watch`, `total_gap`, `platform_alarm` keys.
5. Run `python smoke_test.py` after adding the test — must pass.
6. Delete L1666–L1769.
7. Run `python smoke_test.py` again — must still pass.

**Effort:** ~45 min (test + delete). **Risk:** Low once test is in place.

---

### Phase 1 (Quick wins): Dead code deletion, ~2 hours

**Files:** `scout_handlers.py`, `scout_digest.py`, `scout_ui_kit.py`

1. **`scout_handlers.py:338`** — delete `_seed_feedback_reactions()` (~15 lines) AND its 9 call sites at L2348, L2375, L2408, L2785, L2802, L2824, L2939, L2963, L2987. The function body is `return` (literal no-op stub), docstring says "Call sites kept for diff-stability." Total: ~24 lines deleted.

2. **`scout_ui_kit.py`** — `_fit()` is defined twice: nested in `Card.__init__` (L393) and nested in `_build_suggestion_buttons()` (L1271). They are in separate scopes — cannot call one from the other directly. **Hoist to module-level** `_fit(s: str, max_len: int = 25) -> str`, then reference from both `Card.__init__` and `_build_suggestion_buttons`. Saves ~6 lines, eliminates the duplication.

3. **`scout_digest.py:333-352`** — merge `record_approval()` and `record_rejection()` into `_record_action(action: str)` with the action string as parameter. Both are ~10 lines differing only by `"approved"` vs `"rejected"`. Saves ~10 lines.

**Effort:** ~2 hours. **Risk:** Low — deletion + pure equivalents. `python smoke_test.py` must pass.

---

### Phase 2 (Intra-file helpers): ~2 hours

**Files:** `scout_agent.py`, `queries.py`

1. **`scout_agent.py`** — `_fmt_rev` in two tool handler closures (L4795, L4933): extract as module-level `_fmt_rev(amount: float | None) -> str` with the None guard (use the more defensive version from L4933 — adds `if amount is None: return "$?"`). Replace both local definitions. Saves ~15 lines.

2. **`queries.py`** — The two revenue trend query blocks share near-identical SQL (publisher trends L1551–L1663 and advertiser trends L1980–L2072): extract `_build_revenue_trend_sql(group_by_dim: str, value_col: str) -> str` returning the shared CTE structure. Publisher version uses `publisher_id`/`publisher_name` + `sessions_actual`; advertiser uses `advertiser_id`/`advertiser_name` + `conversions_actual`. Parameterize the dimension and value column. Saves ~60 lines.

   **Validation required:** after Phase 2, run `@Scout revenue publisher` AND `@Scout revenue advertiser` in Slack, compare the numbers against the pre-change baseline. A query returning wrong data passes `smoke_test.py` (it only checks that Scout responds, not that numbers are correct).

**Effort:** ~2 hours. **Risk:** Low-medium for SQL template. Numbers-match Slack check is required.

---

### Phase 3 (Structural factories + dispatch): ~1 day

**Files:** `demand_feed_main.py`, `scout_bot.py`, `scout_handlers.py`

#### 3a: Monitor daemon factory (`demand_feed_main.py`)

Of the 9 daemon functions, only 4 are structurally identical enough to factory-ize safely: `_ghost_monitor_daemon`, `_fill_rate_monitor_daemon`, `_cvr_anomaly_monitor_daemon`, `_expiration_monitor_daemon`. Each is 7–9 lines wrapping `_run_shadow_monitor()` with a single `interval` arg.

**Do NOT factory:** `_projection_autocheck_daemon` (207 lines, bespoke logic), `_cap_monitor_daemon` (32 lines, conditional hourly vs. shadow branch), `_velocity_down_monitor_daemon` (has a signal filter closure).

Collapse the 4 identical ones into a `_SHADOW_DAEMON_SPECS = [(fn, name, interval), ...]` data table + a registration loop that spawns them. Preserve thread names (checked by `test_required_daemons_single_source` in smoke_test.py). Saves ~36 lines.

#### 3b: Alert formatter inline helper (`scout_bot.py`)

The 6 alert formatters (`_format_cap_alert`, `_format_velocity_down_alert`, `_format_ghost_alert`, `_format_fill_alert`, `_format_cvr_alert`, `_format_expiration_alert`) share a 3-line terminal pattern: `Card(severity=..., headline=..., body=...) → wrap_response(card, surface=Surface.MONITOR_ALARM, feedback="none") → return blocks`.

Extract `_alert_blocks(headline: str, body: str, severity=Severity.WARN) -> tuple[str, list]` returning `(fallback_text, blocks)`. Call from all 6. **Preserve `feedback="none"` — do NOT add `pattern=ResponsePattern.ALERT`.** Saves ~18 lines.

#### 3c: Ask-query pulse handler factory (`scout_handlers.py`)

4 `@app.action("pulse_*")` closures each: unpack `user_id` + `msg_ts`, spawn a thread that calls `ask(query_str)` and posts to Slack. They are structurally identical except for the `query_str`.

Note: `pulse_scout_offers` and `pulse_dig_in` compute their query_str at runtime from `action_id` and `action.get("value")` — these cannot be collapsed into the simple factory call. Handle them separately.

Extract `_make_ask_pulse_handler(query_str: str) -> Callable` for the 2 handlers with static query strings. The 2 dynamic handlers stay as-is. Saves ~20 lines.

#### 3d: Action dispatch table (`scout_handlers.py`)

Pull from Phase 4: the 12-branch `if/elif` action dispatch at L1233–1336 → replace with `_ACTION_HANDLERS: dict[str, Callable] = {...}` keyed by action_id string. Each value is the current branch body extracted as a private function. This is mechanical (string → callable) with no logic change.

**Note on payload extraction:** handler payload unpacking varies across handlers:
- Some: `payload.get("user", {}).get("id", "")`
- Some: `(payload.get("user") or {}).get("id", "unknown")`
- Some use `container.channel_id` fallback

Do NOT extract a shared `_extract_interaction_context()` utility without doing case-by-case verification — the container fallback is load-bearing in some handlers. Leave extraction as-is within each handler body.

Saves ~30 lines from dispatch, risk is medium (mechanical but touches all action routing). Full Slack manual test for 5+ action types required before this ships.

**Effort:** ~1 day for 3a–3d. **Risk:** Medium. `python smoke_test.py` + 5 Slack action manual tests required.

---

### Phase 4 (Core refactors): DEFERRED — separate PR

These are the right refactors but touch hot paths. Each is its own PR after Phases 0–3 are live and stable.

1. **`ask()` decomposition** (`scout_agent.py:5780-6129`) — 349 lines. Split into `_build_context()`, `_execute_tools()`, `_format_response()`.
2. **`_build_brief_blocks()` split** (`scout_ui_kit.py:774-957`) — 184 lines → 5 focused helpers.
3. **Network adapter base class** (`offer_scraper.py`) — 9 adapters × 25-40 lines boilerplate.
4. **`_ch_date_filter()` SQL helper** — defer until there's ClickHouse query test coverage; silent regression risk is too high now.

---

## Files Changed (Phases 0-3)

| File | Change | Risk |
|---|---|---|
| `queries.py` | Delete dead v1 `get_publisher_fleet_health_data` L1666-L1769 (~104 lines); extract revenue trend SQL helper | Low/Medium |
| `scout_agent.py` | Extract module-level `_fmt_rev` | Low |
| `scout_handlers.py` | Delete `_seed_feedback_reactions` + 9 call sites; pulse handler factory (2 static); dispatch table | Low/Medium |
| `scout_digest.py` | Merge `record_approval`/`record_rejection` | None |
| `scout_ui_kit.py` | Hoist `_fit()` to module level | None |
| `demand_feed_main.py` | Shadow daemon data table (4 identical daemons) | Medium |
| `scout_bot.py` | Extract `_alert_blocks` inline helper | Low |
| `tests/smoke_test.py` | Add fleet health v2 schema test before Phase 0 deletion | None |

**Estimated total reduction: ~450–550 lines (Phases 0-3).**

---

## Risk

- **Phase 0:** Low with test-first gate. **Fleet health has zero smoke_test coverage — must add test before deleting.**
- **Phase 1:** Near-zero. Dead code + pure equivalents.
- **Phase 2:** Low-medium. SQL template changes require mandatory Slack numbers-match check post-deploy.
- **Phase 3:** Medium. Action dispatch table and daemon factory touch live Slack routing. `smoke_test.py` + manual Slack action testing required.
- **Phase 4:** High. Each is its own future PR.

---

## Success Criteria

1. `python smoke_test.py` passes after each phase (including new fleet health test from Phase 0)
2. `@Scout revenue publisher` and `@Scout revenue advertiser` return correct numbers post-Phase 2
3. 5+ Slack action types tested manually post-Phase 3d
4. Fleet health monitor still fires correctly (`@Scout fleet health`)
5. Total line count decreases by ≥450 lines across targeted files

---

## Decision Audit Trail

| # | Decision | Classification | Rationale |
|---|----------|----------------|-----------|
| 1 | Delete fleet health v1 L1666-L1769 — confirmed dead (shadowed by v2 at L1898) | One-way | Python module load: only L1898 executes; caller uses v2 schema only. Frame as correctness fix. |
| 2 | Add fleet health smoke test BEFORE Phase 0 deletion | One-way | Zero fleet health coverage in current smoke_test.py; eng review finding |
| 3 | `import difflib` — KEEP, do NOT delete | One-way | Used at L2937/L2943 for fuzzy-match hints; plan v1 was wrong |
| 4 | Payout normalization map merge — REMOVED from plan | One-way | Two different maps (scraper normalization vs digest display); merging silently breaks scraper output |
| 5 | Daemon factory scoped to 4 identical daemons only | Two-way | Cap/velocity/projection_autocheck have meaningful unique logic; over-factoring adds risk |
| 6 | Alert formatter: extract 3-line helper only (~18 lines), not a "base builder" | One-way | Each formatter has 15-30 lines of unique field logic; plan v1 overstated savings by 6-7x |
| 7 | Preserve `feedback="none"` in alert formatters — do NOT add `pattern=ResponsePattern.ALERT` | One-way | Current code uses `feedback=`; adding `pattern=` triggers surface validation, behavior change |
| 8 | Phase 2 SQL: defer `_ch_date_filter()`, only do revenue trend dedup | Two-way | ~11 parameterized occurrences (not ~30), column is `created_at` not `event_time`; silent regression risk exceeds value |
| 9 | Pull dispatch table from Phase 4 → Phase 3 | Two-way | Co-located in scout_handlers.py with 3c/3d; 3 overlapping edits to same block across PRs adds diff noise |
| 10 | `_extract_interaction_context()` — REMOVED from plan | One-way | Inconsistent extraction patterns with load-bearing container fallback variants; not safely mechanical |
| 11 | Pulse handler factory: only 2 static-query handlers, not 4 | One-way | pulse_scout_offers/pulse_dig_in have runtime-computed query_str; can't be collapsed with same factory |
| 12 | Phase ordering: 0→1→2→3, smoke test + manual test at each phase boundary | One-way | Each phase depends on previous being stable; parallel refactors compound risk |

---

## GSTACK REVIEW REPORT

**Generated:** 2026-06-05 by `/autoplan`
**Reviews run:** CEO (strategic) + Engineering (code verification)
**Design review:** SKIPPED — no UI changes
**DX review:** SKIPPED — no developer-facing APIs or docs changed
**Outside Voice (Codex):** SKIPPED — eng review already caught all material errors via live code reads

### Critical Corrections Applied (from Eng review)

1. **Phase 0 line range error** — deleting L1666-L1896 would destroy `publisher_fleet_health_stats` (live dependency of v2 at L1926). Correct range: L1666-L1769. Would have caused fleet health to silently return `insufficient_history: True` for all queries.
2. **`import difflib` is NOT dead** — used at L2937/L2943 for fuzzy-match hints. Plan v1 was wrong; deleting would break the threshold-config tool.
3. **`_seed_feedback_reactions` has 9 callers** — deleting without removing call sites causes `NameError` at runtime on any Slack reply path.
4. **Payout normalization maps are different** — `scout_digest.py:90` and `offer_scraper.py:807` serve different pipeline stages with different key sets; merging silently breaks scraper-sourced offer normalization.
5. **Fleet health smoke test gap** — zero fleet health coverage in smoke_test.py; Phase 0 deletion wouldn't be caught by the test suite. Test-first gate added.

### Next Step

Run `/ship` in the worktree. Execution order:
1. Add fleet health v2 schema test to `smoke_test.py`, run to confirm pass
2. Phase 0 — delete `get_publisher_fleet_health_data` v1 L1666-L1769 in `queries.py`
3. Phase 1 — `_seed_feedback_reactions` + 9 call sites; `_fit()` hoist; `_record_action()` merge
4. Phase 2 — `_fmt_rev` extract; revenue trend SQL dedup + Slack numbers-match check
5. Phase 3 — daemon data table (4 only); `_alert_blocks` helper; pulse handler factory (2 static); dispatch table
6. Phase 4 — SEPARATE PRs after Phases 0-3 stable
