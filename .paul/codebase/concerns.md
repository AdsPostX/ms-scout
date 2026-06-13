# Scout — Technical Concerns & Debt

Last audited: 2026-06-13

## Severity: HIGH

### 1. scout_agent.py is a god module (6,323 lines)
**File**: `scout_agent.py`
**Lines**: 6,323 | **Functions**: ~90

Single file owns: agent orchestration, all 20+ tool implementations, threshold management, benchmark caching, ClickHouse re-exports, system prompt loading, Latitude telemetry, retry logic, formatting helpers.

**Impact**: Any tool or threshold change requires editing a 6K-line file. Hard to unit test individual tools. Long context for Claude Code assistance.

**Recommended split**:
- `scout_tools.py` — all tool implementations
- `scout_thresholds.py` — config + override management
- `scout_agent.py` — orchestration only (~500 lines)

---

### 2. Benchmark cache not thread-safe
**File**: `scout_agent.py` (globals: `_BENCHMARKS`, `_BENCHMARKS_LOADED_AT`)

Read-modify-write on `_BENCHMARKS` dict is not protected by a lock. Multiple daemon threads could race on refresh.

**Impact**: Rare but possible cache corruption or skipped refreshes during concurrent monitor activity.

**Fix**: Add `threading.Lock()` around `_BENCHMARKS` read + refresh. ~5 line change.

---

### 3. Deferred gates with KILL-IF-UNMET approaching
**File**: `smoke_test.py` (lines 3759–3778)

| Gate | Check-in | Kill-if-unmet |
|------|----------|----------------|
| `test_fires_log_persistence` | 2026-06-14 | No |
| `test_ms_platform_campaign_creation` | 2026-06-21 | Flip to BLOCKED |
| `test_app_home_drill_modals` | 2026-07-18 | **Yes** |
| `test_alert_registry_redis` | 2026-07-18 | **Yes** |

App Home drill modals and Redis backend are `KILL-IF-UNMET: yes` at 2026-07-18 (~35 days out).

**Fix**: Track these in sprint. If gates aren't met, act on the kill condition — don't let them linger.

---

### 4. demand_feed_main.py — 5 MS_PLATFORM_TODO markers
**File**: `demand_feed_main.py` (lines 623, 1217, 1263, 1293, 1375)

Campaign creation webhook not wired. `CAMPAIGN_CREATE_DRY_RUN` defaults to `"true"`. Blocked on Vamsee delivering `CAMPAIGN_CREATE_WEBHOOK_URL` + API key.

**Check-in**: 2026-06-21. Flag as BLOCKED if not delivered by then.

---

## Severity: MEDIUM

### 5. smoke_test.py monolithic (3,972 lines)
**File**: `smoke_test.py`

Mixes integration tests, startup smoke checks, deferred stubs. Hard to maintain or add tests without wading through 4K lines.

**Fix**: Split into `tests/test_smoke_*.py` by domain (routes, digest, handlers, state, integrations).

---

### 6. ThreadPoolExecutor unbounded worker count in agent tool execution
**File**: `scout_agent.py` (~line 5978)

```python
ThreadPoolExecutor(max_workers=len(tool_blocks))
```

If Claude fires 15 tools in one round, 15 threads spawn simultaneously. No explicit shutdown call.

**Fix**: Cap at `max_workers=min(len(tool_blocks), 8)`. Add `with` context manager or explicit `.shutdown()`.

---

### 7. Global state mutations without sync
**Globals**: `_LAST_HEALTH_STATUS` (scout_bot.py), `_BENCHMARKS` (scout_agent.py)

Modified by monitor threads without locks. Low probability of race given current concurrency levels, but grows riskier as daemon count increases.

---

### 8. Daemon threads have no graceful shutdown
**File**: `scout_bot.py`

Background daemons started with `threading.Thread(daemon=True).start()` — no `join()`, no shutdown hooks, no `atexit` handler.

**Impact**: On SIGTERM (Render deploys), threads die immediately. No clean flush of pending state.

---

### 9. `alert_registry.py` lock ordering undocumented
**File**: `alert_registry.py` (lines 34, 37)

`threading.Lock()` + `BoundedSemaphore` used together. Lock ordering documented in comment but not enforced by code.

---

### 10. Threshold hardcoding in scout_agent.py
**File**: `scout_agent.py`

Key hard-coded values with no env override path:
- `MAX_ROUNDS = 12` (agent loop limit)
- `max_tokens=4096` (LLM token cap)
- `_BENCHMARKS_TTL = 3600` (1h cache)
- `timeout=3` (Latitude network call)
- `timeout=10` (urllib call)

These can only be tuned by code change + deploy.

---

### 11. No config validation for scout_thresholds.json
**File**: `config/scout_thresholds.json`

Loaded at startup without schema validation. A typo (e.g., `"cap_alert_pct": "85"` instead of `85`) silently falls back to hardcoded default. Type mismatch not caught until runtime comparison fails.

**Fix**: Add `jsonschema` validation on startup. Fail fast with a clear error.

---

### 12. queries.py has no modularity (2,506 lines)
**File**: `queries.py`

All ClickHouse SQL in one file. No grouping by domain (revenue vs publisher vs campaign). Navigable but will grow unwieldy.

**Fix**: Split into `queries_revenue.py`, `queries_publisher.py`, `queries_campaign.py`, `queries_monitor.py` — keep `queries.py` as re-export shim.

---

## Severity: LOW

### 13. Latitude telemetry delivery broken
**File**: `scout_telemetry.py`

`latitude-telemetry~=1.0` exports to a decommissioned OTLP endpoint. Spans captured but not delivered. Silently degraded.

**Fix**: Upgrade to current Latitude SDK or remove if unused.

---

### 14. prompts/scout_system.md has no version metadata
**File**: `prompts/scout_system.md` (87KB)

Single file, no version comment, no change log. Git history provides implicit versioning but no explicit metadata.

**Fix**: Add a comment header: `<!-- version: 2.1 | updated: 2026-06-13 | author: sidd -->` for audit trail.

---

### 15. Deferred stubs should use pytest.mark.skip
**File**: `smoke_test.py` (4 `pass`-only functions)

Current stubs pass silently (`pass`). Should use `@pytest.mark.skip(reason="DEFERRED: ...")` so CI visibility is explicit.

---

### 16. scripts/ and evals/ are manual-only, not in CI
**Files**: `scripts/backtest_revenue_projection.py`, `evals/run_routing_evals.py`

Both cost tokens / time. Not wired to CI. No baseline storage for eval regression detection.

---

## Deferred (intentional, tracked)

| Item | Where tracked | Gate | Expected |
|------|--------------|------|---------|
| `fires_log` persistence | smoke_test.py | autocheck 5+ unattended days | 2026-06-14 |
| App Home drill modals | smoke_test.py | Jon/Todd/Roj open Home tab | 2026-07-18 |
| alert_registry Redis | smoke_test.py | same as App Home | 2026-07-18 |
| MS platform campaign webhook | demand_feed_main.py | Vamsee delivers URL + key | 2026-06-21 |

### 17. GIPHY_API_KEY in render.yaml is dead
**File**: `render.yaml`

`GIPHY_API_KEY` is declared as an env var in render.yaml but is not referenced anywhere in the Python codebase. Safe to remove from render.yaml to reduce credential surface.

---

## Security Status: CLEAN

- Subprocess (`pdftotext`): no `shell=True`, explicit arg list, timeout enforced ✅
- Credentials: all via env vars, no hardcoded tokens ✅
- SQL: parameterized queries, SELECT-only gate on user-submitted SQL ✅
- SSRF (Google Sheets fetch): host allowlist, private IP blocked, max 3 redirect hops ✅
- Slack file downloads: `files.slack.com` host-prefix check ✅
