# Scout — Key Architectural Decisions

Decisions that aren't obvious from the code and would otherwise need to be rediscovered.

## State Storage: File-Based JSON (Not Database)

**Decision**: All runtime state (launched offers, pulse state, thread context, etc.) persists as JSON files in `data/` directory on Render's persistent disk.

**Why**: Redis deferred. Persistent disk on Render is simple, zero-cost, survives deploys. File-based state is inspectable, debuggable, and easy to reset manually.

**Trade-off**: Not multi-instance safe. Single Render instance per service is the constraint this relies on. If ms-scout ever scales to multiple instances, this breaks immediately.

---

## Two Services, Not One

**Decision**: `scout_bot.py` (Slack listener) and `demand_feed_main.py` (scraper) are separate Render services with separate disks.

**Why**: Offer scraping runs for up to 30 minutes and cannot block the Slack bot's event processing. Crash isolation: scraper failure doesn't kill the bot.

**Trade-off**: `offers_latest.json` is written by demand-feed, read by scout-bot. They communicate via disk (on separate Render volumes). In practice, scout-bot reads a stale copy until scraper updates. `DEMAND_FEED_URL` env var allows scout-bot to fetch fresh data from demand-feed's HTTP endpoint instead.

---

## Scout_ui_kit.py is Intentionally Pure

**Decision**: `scout_ui_kit.py` imports stdlib only. Zero Slack API calls, zero ClickHouse, zero file I/O.

**Why**: Enables independent unit testing of every Block Kit builder without mocking external dependencies. Also means UI tests are deterministic and fast.

**How to apply**: Any new UI helper goes in `scout_ui_kit.py` only if it has zero side effects. If it needs data, the caller fetches it and passes it in.

---

## Intent Routing: LLM Not Keyword Table

**Decision**: `_classify_intent()` uses Claude for intent classification. Replaced `_ROUTE_KEYWORDS` keyword table (removed in phase 08).

**Why**: Keyword matching was brittle and grew into an unmaintainable lookup table. LLM handles natural language variants, abbreviations, and ambiguous phrasing better. See PR #251, #252 for the refactor.

**Trade-off**: Adds latency + cost to the routing step. Regression guard: `evals/run_routing_evals.py` catches tool routing regressions.

---

## Alert Registry: Redis Optional, In-Memory Fallback

**Decision**: `alert_registry.py` uses Upstash Redis when configured, falls back to in-memory + `pulse_state.json` otherwise.

**Why**: Redis adds operational complexity and cost. In-memory fallback is sufficient for current single-instance deployment. Redis needed only when App Home scoreboard requires cross-restart persistence (deferred, gate 2026-07-18).

---

## ScoutKit Pattern: wrap_response() is the Only Entry Point

**Decision**: All Slack posts from `ask()` results go through `scout_ui_kit.wrap_response()`. No callers construct Block Kit dicts directly.

**Why**: Block Kit has subtle mobile-first constraints (budget limits, action_id uniqueness, no fenced code). Centralizing the entry point allows lint tests (`test_kit_lint.py`) to enforce these constraints once.

---

## Config in JSON, Not Python

**Decision**: `config/scout_thresholds.json` stores thresholds externally, not as Python constants.

**Why**: Enables the team to audit and discuss thresholds without reading code. `@Scout config` displays live values. Runtime overrides (via `@Scout set_threshold`) write to `data/threshold_overrides.json` and merge on top without a deploy.

---

## Offer Pipeline: Typed Contracts at the Boundary

**Decision**: `scout_core/contracts.py` defines `RawOffer → NormalizedOffer` boundary. `offer_scraper.py` produces `RawOffer`. Consumers (`scout_digest.py`, `scout_agent.py`) consume `NormalizedOffer`.

**Why**: Before contracts, dict shapes drifted silently between scraper and digest. Typed boundary caught mismatches at the import level.

**How to add a new network**: implement scraper → produce `RawOffer` → use `NormalizedOffer.from_raw()`. Never pass raw dicts to consumers.

---

## Prompt: Single File, Loaded at Startup

**Decision**: `prompts/scout_system.md` is one file loaded once at startup (either from Latitude or local fallback).

**Why**: Single-file simplifies iteration — one edit, one redeploy, the whole agent updates. Versioning via git history. Latitude adds remote-fetch so prompt can be updated without a code deploy (when `LATITUDE_API_KEY` is set).

**Trade-off**: Hard to A/B test prompt variants. No version metadata in the file itself (git history is the audit trail).
