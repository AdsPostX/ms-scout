<!-- /autoplan restore point: /Users/siddharthshah/.gstack/projects/AdsPostX-ms-scout/feat-latitude-telemetry-autoplan-restore-20260527-183614.md -->

# Scout + Latitude — Complete Observability Loop

**Date:** 2026-05-27
**Branch:** `feat/latitude-telemetry`
**Status:** APPROVED — autoplan 2026-05-27 | Phase 1 shipped (PR #210 open), Phase 2 ready for /ship
**Worktree:** `/Users/siddharthshah/code/ms-scout/.claude/worktrees/feat-latitude-telemetry/`

---

## Problem in One Sentence

Scout runs blind — when it gives a wrong answer at 2am, there is no prompt visibility, no user attribution, no cost tracking, and no mechanism to know the answer was wrong, let alone fix it.

---

## Context

### What Phase 1 shipped (PR #210)
- `scout_telemetry.py` — singleton that initializes Latitude Python SDK (`latitude-telemetry~=1.0`) and exposes `capture(path, fn, metadata)` wrapper
- 4 named spans: `scout/agent`, `scout/entity-parse`, `scout/context-compress`, `scout/entity-extract`
- Auto-instrumentation via `Instrumentors.Anthropic` — all Anthropic SDK calls traced
- Graceful no-op when `LATITUDE_API_KEY` not set

### What Phase 1 did NOT do (the real 75% of value)
Three structural gaps identified in post-ship critique:

1. **No user attribution.** `distinct_id` is unused. Traces are anonymous blobs — can't filter by Slack user.
2. **No prompt management.** `prompts/scout_system.md` (990 lines) is hardcoded Python. Latitude's core differentiator — prompt versioning, A/B testing, response comparison — is completely unreachable.
3. **No improvement loop trigger.** No alerting, no signal for "Scout gave a bad answer today." The loop that makes Scout measurably better (bad trace → identify failure → fix prompt → measure delta) doesn't exist.

### Scout Architecture (relevant)
- `scout_agent.py` — 5,582 lines. `SYSTEM_PROMPT` loaded from `prompts/scout_system.md` (990 lines). Single `ask()` → `messages.create`. Uses Anthropic prompt caching (`cache_control: ephemeral`).
- `context_harvester.py` — 2 LLM calls: Sonnet for channel compression, Haiku for entity extraction.
- `scout_handlers.py` — 1 LLM call: Haiku for "remember" command entity parse.
- `scout_bot.py` — 2 startup/heartbeat pings (intentionally NOT traced).
- `scout_telemetry.py` — new singleton (Phase 1). `capture(path, fn, metadata)` wrapper.

---

## Goal

Close the loop: Latitude traces → bad answer surfaced → prompt fixed → improvement measured.

Three deliverables that close the loop:

1. **User attribution** — `capture()` passes `user_id` as `distinct_id`. Traces filterable by Slack user.
2. **Managed prompt** — `prompts/scout_system.md` registered in Latitude. `ask()` fetches at startup with local fallback. Enables prompt versioning, A/B testing, response quality comparison.
3. **Alerting** — Latitude alert when `scout/agent` returns empty/error response. Creates the trigger.

---

## Proposed Implementation

### Step 1: Fix distinct_id attribution (all call sites)
**Files:** `scout_telemetry.py`, `scout_handlers.py`, `context_harvester.py`

Update `capture()` signature AND fix latent double-execution bug: if `fn()` raises inside the span, the current broad `except Exception` catches it and runs `fn()` a second time (side-effect double-firing). The fix separates span-init failures (fall back silently) from `fn()` failures (propagate normally):

```python
def capture(path: str, fn, metadata: dict | None = None, distinct_id: str | None = None):
    if _telemetry is None:
        return fn()
    _span_entered = False
    try:
        _span = _telemetry.span(path, distinct_id=distinct_id, metadata=metadata or {})
        _span.__enter__()
        _span_entered = True
        result = fn()
        _span.__exit__(None, None, None)
        return result
    except Exception as exc:
        if not _span_entered:
            # Span init failed — fall back silently, fn() not yet called
            log.warning("[telemetry] capture(%s) span-init error: %s — running untraced", path, exc)
            return fn()
        # fn() itself raised — close span and re-raise (never double-execute)
        try:
            _span.__exit__(type(exc), exc, exc.__traceback__)
        except Exception:
            pass
        raise
```

Update call sites:
- `scout/agent` — pass `user_id` (available in `_ask_with_timeout` kwargs)
- `scout/entity-parse` — pass `user_id` (available in `scout_handlers.py`)
- `scout/context-compress` — no user_id (channel-scoped, `channel` in metadata is sufficient)
- `scout/entity-extract` — no user_id (harvester-scoped, not user-triggered)

**Effort:** ~30 min. 3 files, 5 call sites.

### Step 2: Latitude-managed prompt for scout_system.md

**Option A (Recommended): Runtime fetch with local fallback**
- Register `prompts/scout_system.md` as a Latitude prompt via `latitude-sdk` (separate package from `latitude-telemetry`)
- In `scout_agent.py`, fetch prompt once at startup → cached in memory → pass to `messages.create`
- Fallback: if Latitude unreachable or key not set → read from `prompts/scout_system.md` as today
- Enables: edit prompt in Latitude UI → next Scout restart picks it up → compare traces before/after

**Option B: Log prompt version as span metadata (no managed prompt)**
- Add `prompt_sha` (git hash of `scout_system.md`) to `scout/agent` span metadata
- No Latitude-managed prompt — versions are traceable but not editable via Latitude
- Lower risk, much lower reward

**Rationale for A:** The closed-loop improvement cycle requires editable, versioned prompts. Option B is better logging, not a loop. The risk of A is fetch latency at startup — mitigated by caching + fallback.

**Key constraint:** `latitude-sdk` is the prompt management package (separate from `latitude-telemetry`). Must add to `requirements.txt`. Verify version compatibility before adding.

**Anthropic prompt caching compatibility:** `cache_control: ephemeral` is set on the system prompt text in `ask()`. Fetching from Latitude changes the source but not the text — Anthropic cache hit rate is unaffected. **Implementation requirement:** after fetching from Latitude, assert the returned text equals `prompts/scout_system.md` character-for-character (or use `.strip()` match). If Latitude normalizes whitespace/encoding on return, every `ask()` call will be a cache miss (300ms → 1500ms cold). Add a startup log: `[agent] prompt sha (Latitude)={hash} sha (local)={hash}` — mismatch = cache poisoned.

### Step 3: Alerting (Latitude dashboard, no code)
- Latitude project settings → Alerts: when `scout/agent` span has 0 output tokens or empty response content → fire webhook to Slack `#scout-dev`
- No code change. Documents webhook URL in `.env.example` as `LATITUDE_ALERT_WEBHOOK_URL`.
- Creates the loop trigger.

### ~~Step 4~~ → **Step 0 (Prerequisite): Resolve OTLP 404**

**Step 0.1 — Tracing API (OTLP):**
- **Hard gate:** `POST https://gateway.latitude.so/api/v2/otlp/v1/traces` returns 404
- Root cause unknown: wrong project ID, API key format mismatch, or OTLP not enabled for this Latitude project
- **All Steps 1–3 produce zero observable telemetry value until this is resolved.** Do not implement Steps 1–3 without first confirming traces land in the Latitude dashboard.
- Investigation: Latitude dashboard → Project Settings → Integrations → OTLP; test with curl + your LATITUDE_API_KEY; verify project_id matches.

**Step 0.2 — Prompt Management API (latitude-sdk, separate from OTLP):**
- OTLP resolution does NOT confirm prompt management is working — these are different API paths
- Verify independently: install `latitude-sdk~=5.0`, check import name (`from latitude_sdk import ...`), check env var names for prompt UUID, test a single prompt fetch against the Latitude API
- Check for transitive dep conflicts: `pip install latitude-sdk~=5.0 latitude-telemetry~=1.0 --dry-run`
- If prompt management API is unreachable at Step 2, the local fallback will silently activate — which is safe but undetectable without this check

---

## What is NOT in scope
- Migrating `_ENTITY_EXTRACTION_PROMPT` or `_COMPRESSION_PROMPT` in `context_harvester.py` to Latitude-managed (lower-stakes, tackle in Phase 3)
- A/B testing infrastructure (comes after first managed prompt proves out)
- Per-span cost alerting (follow-on once token counts land in Latitude dashboard)
- Tool definition prompts in `scout_agent.py` (too large blast radius, defer)

---

## Files Changed

| File | Change | Risk |
|------|--------|------|
| `scout_telemetry.py` | Add `distinct_id` param to `capture()` | Low — additive, backward-compat |
| `scout_handlers.py` | Pass `user_id` as `distinct_id` in 2 call sites | Low |
| `context_harvester.py` | No-op for `distinct_id` (channel calls) | None |
| `scout_agent.py` | Add Latitude prompt fetch at startup + fallback | Medium — touches core `ask()` path |
| `requirements.txt` | Add `latitude-sdk~=5.0` | Low |
| `tests/test_telemetry.py` | New — 6 mock-based tests for capture() + distinct_id + prompt fallback + exception swallowing | None |
| `.env.example` | Add `LATITUDE_PROMPT_UUID`, `LATITUDE_ALERT_WEBHOOK_URL` | None |
| `docs/latitude-workflow.md` | New — improvement workflow documentation | None |

---

## Dependencies
- `latitude-sdk` Python package (prompt management, separate from `latitude-telemetry`)
- Latitude project OTLP must be enabled (currently 404 — must resolve first)
- `LATITUDE_PROMPT_UUID` env var for the managed prompt UUID
- `LATITUDE_PROJECT_ID` already in `.env` (used for Latitude-SDK init)

---

## Risk
- **Latitude fetch latency at startup:** Mitigated by local file fallback + memory cache after first fetch.
- **Prompt cache invalidation:** No issue — Anthropic caches on text content, not source.
- **OTLP 404 blocker:** All of Phase 2's value depends on resolving this first. If Latitude's OTLP is broken for this account, Phase 2 value is zero.
- **Scout downtime:** None. Every new dependency has a fallback to current behavior.
- **`_PROMPT_SHA` subprocess on startup:** `subprocess.run(["git", ...])` raises `FileNotFoundError` if git absent (e.g., production Docker). Implementation MUST wrap in `try/except (FileNotFoundError, OSError)` and default to `""`. Not doing so crashes Scout at startup.

---

## Success Metrics
1. `distinct_id` appears in Latitude dashboard for user queries (immediately verifiable post-Step 1)
2. `prompts/scout_system.md` shows edit history in Latitude UI (after Step 2)
3. At least one bad Scout answer is traced, prompt edited in Latitude, and improvement verified via `smoke_test.py` (the "aha moment" — defines Phase 2 success)

---

## Decision Audit Trail

| # | Phase | Decision | Classification | Principle | Rationale | Rejected |
|---|-------|----------|----------------|-----------|-----------|----------|
| 1 | CEO | Full Phase 2 (A) — distinct_id + managed prompt + alerting | Two-way | Completeness > boil lakes | Build now while context hot; closed loop in one PR | B: attribution-only first |
| 2 | CEO | OTLP 404 promoted to Step 0 prerequisite | One-way | Explicit | Steps 1–3 unverifiable until traces land; wrong order = build on broken pipe | Step 4 ordering |
| 3 | CEO | Rebase onto main before implementation | One-way | Pragmatic | 40 commits behind; scout_handlers.py hot; merge will fail without it | — |
| 4 | CEO | Add tests/test_telemetry.py (3 mock-based tests) | Two-way | Completeness | Infrastructure code; no-op fallback logic must be tested | Defer tests |
| 8 | ENG | Increase test count 3 → 6 | Two-way | Completeness | Coverage diagram found 13 untested paths; 6 tests required minimum | 3 tests |
| 9 | ENG | _PROMPT_SHA subprocess needs try/except FileNotFoundError | One-way | Explicit | git absent in Docker → startup crash without guard; must wrap | Unguarded |
| 10 | ENG/Outside-Voice | capture() double-execution bug — fix in Step 1 | One-way | Completeness | fn() raise inside span → caught by except → fn() called again (double side effects) | Defer to Phase 3 |
| 11 | ENG/Outside-Voice | Step 0 split: OTLP (0.1) + prompt API (0.2) separate readiness checks | One-way | Explicit | OTLP 404 fix ≠ latitude-sdk API works; two independent gates | Single Step 0 |
| 12 | ENG/Outside-Voice | Anthropic cache: assert exact text match after Latitude fetch | One-way | Explicit | Whitespace normalization by Latitude → cache miss on every ask() call | Assume compatible |
| 5 | CEO | latitude-sdk~=5.0 version pin | Two-way | Explicit | v5.10.0 latest; major version gap from latitude-telemetry v1 warrants explicit pin | Unpinned |
| 6 | CEO | Add prompt SHA to scout/agent span metadata | Two-way | Completeness | git hash-object — zero cost, shows which prompt version was used even on local fallback | Phase 3 |
| 7 | CEO | Token count metadata in spans | DEFERRED | Pragmatic | Touches ask() return path; Phase 3 material | — |

---

## GSTACK REVIEW REPORT

**Generated:** 2026-05-27 by `/autoplan`
**Final status:** APPROVED by user (D1 → Approve as-is)

### Phases Run

| Phase | Status | Key Output |
|-------|--------|------------|
| CEO Review | ✅ Completed | Full Phase 2 approved; OTLP 404 promoted to Step 0; rebase-first mandate |
| Design Review | — Skipped | No UI changes in scope |
| Eng Review | ✅ Completed | 6 tests required (up from 3); double-execution bug identified; Docker subprocess guard added |
| Outside Voice (Codex) | ✅ Completed | 9 findings; 3 critical adopted (double-exec, Step 0 split, Anthropic cache assertion) |
| DX Review | — Skipped | No developer-facing APIs or docs in scope |

### Critical Findings (all resolved in plan)

1. **`capture()` double-execution bug** — `fn()` raises inside span → `except Exception` catches it → `fn()` called second time. Fix: `_span_entered` flag separates span-init failures from `fn()` failures. Found independently by both Claude Eng and Codex.
2. **`_PROMPT_SHA` Docker crash** — `subprocess.run(["git", ...])` raises `FileNotFoundError` when git absent (production container). Fix: `try/except (FileNotFoundError, OSError)`, default to `""`.
3. **Step 0 conflation** — OTLP 404 fix does NOT confirm latitude-sdk prompt API works. Split into independent gates 0.1 and 0.2.
4. **Anthropic cache whitespace risk** — Latitude may normalize whitespace in returned prompt text, causing cache miss on every `ask()` call. Fix: assert exact text match after fetch; log SHA mismatch at startup.

### Auto-Decided Taste Call (user did not override)

- `_init_prompt()` runs at **module load time (eager)** — consistent with `_init()` pattern in `scout_telemetry.py`. Startup failures visible in logs immediately, not silently on first user query.

### Cross-Phase Theme

`capture()` double-execution bug surfaced in both Eng review and independent Codex review with zero coordination. High-confidence fix.

### Next Step

Run `/ship` in the worktree to begin implementation. Execution order:
1. Step 0.1 — Verify OTLP (curl test against gateway.latitude.so)
2. Step 0.2 — Verify latitude-sdk prompt API + transitive dep check
3. Step 0.5 — `git rebase main` (expect conflicts in `scout_handlers.py`)
4. Step 1 — `capture()` fix + `distinct_id` (scout_telemetry.py + scout_handlers.py)
5. Step 2 — `_init_prompt()` + `_PROMPT_SHA` (scout_agent.py + requirements.txt)
6. Step 3 — Latitude alert config (dashboard only, no code)
7. Tests — `tests/test_telemetry.py` (6 mock-based tests)
