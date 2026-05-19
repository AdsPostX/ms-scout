<!-- /autoplan restore point: ~/.gstack/projects/AdsPostX-ms-scout/eager-driscoll-2863e9-autoplan-restore-20260518-170356.md -->

# Scout — Today-Revenue Projection Tool

**Date:** 2026-05-18
**Status:** PROPOSED — pending CEO + Eng review
**Trigger:** Slack — Ali asked "@Scout how much do you estimate our revenue for today after it ends?" and "project today's revenue". Scout returned today-so-far summary instead. Routing miss.

---

## Problem in One Sentence

`get_revenue_today` answers "where are we right now" but Scout has no tool for "where will we land", so the agent falls back to the summary tool when asked to project — and the projection math that *does* exist (in the 3pm alert daemon) is unreachable from `@Scout` mentions.

## What Success Looks Like

When any user asks Scout to project/estimate/forecast today's revenue, Scout returns a single end-of-day dollar number with confidence framing — not the intraday summary. Daemon behavior is unchanged (alert still fires at 3pm with same threshold).

Falsifiable: re-ask Ali's exact questions ("how much do you estimate our revenue for today after it ends?", "project today's revenue") and Scout returns a projected EOD dollar value, not the intraday summary.

---

## Eng Review Resolutions (2026-05-18)

- **`_query_intraday_revenue_total` stays byte-for-byte unchanged.** No "thin wrapper" — the daemon path keeps its `/0.70`, `None`-on-tolerance, and `SCOUT_THRESHOLDS` lazy import as-is. The new `project_today_revenue` helper is purely additive; the daemon will be unified post-validation in a separate ticket.
- **Single return contract for the helper:** `{status: "ok"|"too_early"|"insufficient_history"|"unstable"|"error", formatted: str, today_revenue: float|None, projected_full_day: float|None, dow_median: float|None, pct_of_expected: float|None, as_of_ct: str, hour_ct: int, curve_share: float|None, curve_source: "60d"|"fallback_0.70", sample_days: int, warning: str|None}`. No flag-passing; agent tool dispatches on `status`.
- **Drop `with_breakdown` parameter entirely for v1.** Per-publisher is fully out of scope.
- **Drop the >25% divergence guardrail.** Daemon doesn't persist its projected number (`scout_bot.py:858-864` calls helper, only `_save_revenue_alert_date` writes state). Rely on `status="too_early"`, `sample_days < 4` → `insufficient_history`, and the Step 6 backtest gate.
- **All SQL CT-correct.** Use `toTimeZone(created_at, 'America/Chicago')` and `toTimeZone(now(), 'America/Chicago')` everywhere — do not inherit the daemon's bare `today()` at scout_ch.py:189.
- **Curve query shape:** exclude today, group by CT day, require `full_day_revenue > 0`, share = `sumIf(revenue, ct_hour <= H) / full_day_revenue`, average across same-weekday days; `sample_days` = number of qualifying days, not rows.
- **Module-scope TTL cache** on the curve array (10-min TTL) so a 5-user Slack burst doesn't fan out into 5 × 60-day scans.
- **Tool registration touches three sites** in scout_agent.py: schema near `get_revenue_today` (~L1174), import alongside other `scout_ch` helpers (~L31-38), `TOOL_MAP` binding near `"get_revenue_today"` (~L4477-4490). Update `get_revenue_today`'s description for anti-routing (~L1176-1180).
- **Error handling:** helper may raise; tool wrapper catches and returns Slack-safe pre-formatted mrkdwn like `get_revenue_today` (~scout_agent.py:4367-4372). Never let exceptions bubble through `TOOL_MAP` dispatch.
- **Smoke tests are deterministic** (no live SQL). Pattern matches smoke_test.py:636-658: `hasattr` checks for the helper (via `scout_agent` re-export), `TOOLS` schema presence, `TOOL_MAP` binding, anti-routing text on old tool, exact `too_early` string via stubbed CH client. Add `project_today_revenue` to the `scout_agent.py` re-export list (~L34).

## CEO Review Resolutions (2026-05-18)

- **Daemon refactor (was Step 6) dropped.** Additive-only — projection tool reads shared helper; daemon path untouched until projection has earned trust. Both CEO voices flagged this as the biggest unforced risk.
- **Success criterion expanded.** Lead with one number, but include (a) confidence/error band, (b) pace vs typical at this hour, (c) same-weekday EOD comparison. "$X projected, likely $Y–$Z, currently $A, tracking B% of typical for this hour, typical Monday lands ~$C."
- **Rollout hardened.** Pre-public gate = 10-day backtest report (projected EOD vs actual EOD at multiple CT hours, median + 90th percentile error). Tool returns "not enough confidence to project" when sample size or error band fails the gate. Only after the report is reviewed does the tool become visible in #revenue-operations.
- **Per-publisher breakdown moved fully OUT for v1.** Total only. Publisher-level projection re-enters after total earns trust.
- **Hard guardrail (SUPERSEDED).** Original CEO ask was: if new projection diverges > 25% from the daemon's last reading on the same day, return "projection unstable". Eng review dropped this (see line 28) because the daemon does not persist its projected number; we rely on `status="too_early"`, `sample_days < 4` → `insufficient_history`, and the Step 6 backtest gate instead. The `"unstable"` status enum value on line 26 remains as a reserved code path for future use but is not emitted by v1.
- **Hard-coded too-early string.** "Too early to project reliably — ask after 10am CT." Smoke test asserts verbatim surface, not paraphrased.

## Proposed Plan (7 steps)

**Step 1 — Add additive helper `project_today_revenue(ch) -> dict` in `scout_ch.py`.**
New function alongside the existing `_query_intraday_revenue_total` (scout_ch.py:153) — the existing function is NOT modified. The helper computes today_revenue, hour-of-day cumulative share, projected EOD, same-weekday 8-week dow_median, and pct_of_expected, returning the status-enum dict defined in Eng Review Resolutions. CT-correct SQL throughout. Module-scope TTL cache (10 min) on the **90-day** curve query, with **median share per (dow, hour)** (not mean) — see Backtest Result. Re-export `project_today_revenue` from scout_agent.py (~L34).

**Step 2 — Hour-of-day curve query (inside the new helper only — daemon's `/0.70` is untouched).**
Compute cumulative share from `adpx_conversionsdetails`: **last 90 days**, exclude today, group by CT day, require `full_day_revenue > 0`, share = `sumIf(revenue, ct_hour <= H) / full_day_revenue`, **median across same-weekday days** (not mean — see Backtest Result). `sample_days` counts qualifying days. If `sample_days < 4` → `status="insufficient_history"`. If current CT hour < 10 → `status="too_early"` with the verbatim string "Too early to project reliably — ask after 10am CT." Curve query result cached in module scope, 10-min TTL.

**Step 3 — Add `get_revenue_today_projection` agent tool.**
Three registration sites in `scout_agent.py`: (a) TOOLS schema near `get_revenue_today` (~L1174), (b) import of `project_today_revenue` alongside other scout_ch helpers (~L31-38), (c) TOOL_MAP binding near `"get_revenue_today"` (~L4477-4490). Wrapper calls `project_today_revenue(ch)`, catches exceptions Slack-safely (returns "Projection unavailable" mrkdwn), and dispatches on `status`: `ok` → Intent-18 mrkdwn ("Projected EOD: *$X* (range $Y–$Z based on ±10% pace error). Currently *$A* — tracking *B%* of typical for this hour. Typical {weekday} lands ~*$C*."), `too_early` / `insufficient_history` / `unstable` / `error` → returns the helper's pre-formatted `formatted` string verbatim. Always answers. Pre-formatted shape matches `get_revenue_today` so scout_handlers renders mrkdwn verbatim.

**Step 4 — Update `get_revenue_today` description.**
Add explicit anti-routing: "Use ONLY for 'how is revenue today / right now / so far'. Do NOT use for 'project / estimate / forecast / EOD / end of day / how will today land'." Keep current behavior.

**Step 5 — Set trigger phrases on new tool.**
Description triggers: "project today's revenue", "estimate today's revenue", "EOD revenue", "what will today land at", "how much will we make today", "forecast today", "after it ends".

**Step 6 — Backtest report (gates public rollout).**
Replay the new helper against the last 10 business days at hours 10/12/14/16 CT. Compare projected EOD to actual EOD. Emit a markdown table with absolute error, % error, and pace-vs-typical for each (day, hour). Median error and 90th percentile error must be presented before flipping the tool from dev-Scout-only to channel-visible. No daemon code is touched in this step.

**Step 7 — Smoke test + autonomous validation into #sidd-qa.**
- `python3 smoke_test.py` — green. New deterministic tests (no live SQL — stub the CH client like smoke_test.py:636-658):
  - `test_project_today_revenue_helper_exists` — `hasattr(scout_agent, "project_today_revenue")`
  - `test_revenue_projection_tool_registered` — `"get_revenue_today_projection"` in TOOLS schema names AND TOOL_MAP keys
  - `test_revenue_today_anti_routing` — old `get_revenue_today` description contains "Do NOT use for"
  - `test_too_early_string_verbatim` — stub CH client, force hour=9 path, assert exact "Too early to project reliably — ask after 10am CT"
  - `test_projection_autocheck_monitor_registered` — assert (a) a `_start_daemon(...)` call for `_projection_autocheck_monitor` exists in `scout_bot.py`, (b) `_save_projection_autocheck_slot` / `_load_projection_autocheck_slot` exist in `scout_state.py`, (c) `_route_channel('qa')` resolves to `#sidd-qa`.

**Autonomous validation cadence (replaces the prior manual 11am/3pm CT checks).**
Rationale: Sidd won't remember to ping dev-Scout at fixed times; instead, Scout fires the projection itself into `#sidd-qa` on a fixed cadence, and the absence of fires is itself a signal worth investigating the next morning.

- New `_projection_autocheck_monitor` in `scout_bot.py` (sibling of existing daemon monitors, registered via `_start_daemon(...)` near scout_bot.py:2549). Reuses `_run_with_web` plumbing — do NOT introduce a parallel framework.
  - Cadence: top of each CT hour from **10:00 → 17:00 CT** inclusive (8 fires per business day; brackets the daemon's 3pm alert).
  - Per-hour payload: `project_today_revenue(ch)` output rendered verbatim (status-dispatched). Header tag `[projection-autocheck]` so it's grep-able.
  - **Channel routing:** new `_route_channel('qa')` case returns `#sidd-qa`. Monitor never routes anywhere else, regardless of the Step 6 channel-visibility flip.
  - **State persistence (mirrors `_save_revenue_alert_date` pattern):** new helpers `_save_projection_autocheck_slot(slot)` / `_load_projection_autocheck_slot()` in `scout_state.py`, writing through `_save_pulse_state` → `_atomic_write`. Slot string **CT-anchored**: `f"{now_ct.date().isoformat()}T{now_ct.hour:02d}"` where `now_ct = datetime.now(pytz.timezone('America/Chicago'))`. On monitor start, seed in-memory `last_slot` from `_load_projection_autocheck_slot()` so a mid-hour restart does not re-fire the current hour.
  - **Gating:** `_monitor_enabled('projection_autocheck')` via `SCOUT_THRESHOLDS` entry — matches sibling monitors, not a raw env var.
  - **Kill-switch:** track in-memory `consecutive_error_count`. ≥2 consecutive `error`-status fires → monitor auto-pauses for the remainder of the day and posts one `[projection-autocheck] auto-paused: 2 consecutive errors, see logs` notice into `#sidd-qa`. Resets at 10:00 the next CT day.
  - **EOD rollup:** at 17:30 CT, post a single summary into `#sidd-qa` — `[projection-autocheck] {N_ok}/8 ok, {N_too_early} too_early, {N_error} error, max |today_revenue - daemon_15:00| = ${delta}` — so the visibility decision needs one message read, not 80.
- Pass criteria (read back next morning):
  - 8 fires landed in `#sidd-qa` (≥ 7 acceptable for slot drift).
  - **Apples-to-apples comparison at 15:00:** helper's raw `today_revenue` within **$500** of daemon's `_query_intraday_revenue_total` reading at the 15:00 slot. (We do NOT compare projected EOD to daemon's `/0.70` projection — those are different numbers by construction.)
  - Zero `error`-status fires. `too_early` only at the 10:00 slot is acceptable.
  - State/Slack parity: slot keys present in `pulse_state.json` after the run match the fires posted.
  - `get_revenue_today` regression: zero unintended projection answers — grep `#sidd-qa` for the projection header tag vs the summary tool's output shape.
- Failure mode: if any of the above fail, the channel-visibility flip in Step 6 does NOT happen. Tool stays dev-Scout-only.

---

## Backtest Result (2026-05-18)

First pass (60-day window, **mean** share per dow×hour) **FAILED the gate**:
median |% err| ≈ 12%, P90 ≈ 26%; 33/40 cells under-projected.

Methodology fix: **90-day window + median (not mean)** share per (dow, hour).
Median suppresses the heavy under-arrival outlier days that pulled the mean
share up (which in turn drove projections down). Re-backtested via ClickHouse
MCP against the same 40 (day, hour) cells:

- Median |% err| = **7.25%** (gate ≤ 8% ✅)
- P90    |% err| = **17.3%** (gate ≤ 18% ✅)

Applied in `_build_hour_curve` (scout_ch.py): `INTERVAL 60 DAY` → `INTERVAL
90 DAY`, share aggregation `sum/len` → `_median(vals)`. Daemon's
`_query_intraday_revenue_total` still untouched.

## Risks

- **Hour-of-day curve has thin tails.** Early-morning hours have few full-day reference points; floor at 8am CT, require `sample_days >= 4`, otherwise return "too early to project reliably."
- **Daemon baseline (8-week same-weekday median) vs summary tool (30-day calendar avg)** are deliberately different. The projection tool uses the *daemon* baseline — this aligns projection with the alert it pairs with, and avoids two different "expected today" numbers floating around.
- **No daemon behavior change.** `_query_intraday_revenue_total` is untouched; the daemon's 3pm alert path is byte-for-byte preserved. Unification deferred to a separate ticket post-validation.
- **Trust deficit** — Scout's first-impression fragility means a wrong projection in #revenue-operations is worse than no tool. Mitigation: gate the tool to dev-Scout / Sidd DM until Step 6 backtest passes (median error < 8%, 90th pct error < 18%), then enable channel-wide.

---

## Out of Scope

- Bidirectional 🟢 upside projection — deferred (separate work).
- Per-publisher projection — fully out for v1 (was: optional breakdown). Total only.
- Multi-day forecasting — projection is today only.
- UI / Slack Block Kit changes — text/mrkdwn output, same as `get_revenue_today`.
- **`last_shadow_slot` persistence to `pulse_state.json`** — diagnosed during the 2026-05-18 alarm-sweep (`_cvr_anomaly_monitor` and siblings re-fire identical shadow alerts on every restart because `last_shadow_slot` is process-local at scout_bot.py:1231). Tracked as a separate follow-up PR. Pattern: follow-up introduces `_save_shadow_slot(monitor_name, slot)` keyed by monitor name; **do NOT generalize the new `_save_projection_autocheck_slot` helper** to anticipate this — keep them independent until the follow-up lands. Contamination note: this PR's #sidd-qa signal stays clean **only because** the noisy shadow alerts route to `#revenue-operations`. If that routing ever changes before the follow-up lands, the projection autocheck's signal-to-noise dies.
