# Plan: PR 21 — Wire Signal Thresholds (Make @Scout config honest)

## Problem Statement

`config/scout_thresholds.json` `signals` section exposes 5 tunable values via `@Scout config`,
but the Pulse SQL queries hardcode the literal numbers. Editing the JSON has **zero effect**
on Scout's actual behavior. Two discrepancies are active bugs:

| Config key | Config value | Code value | Gap |
|---|---|---|---|
| `cap_alert_pct` | 90% | 70% (0.70 hardcoded) | Code alerts 20 points early — Sidd sees alerts the team didn't tune |
| `velocity_up_threshold_pct` | 20% | 40% (abs check) | Up-velocity misses +20%–+39% shifts entirely |
| `fill_rate_min_sessions_7d` | 5000 | 5000 (matches, decorative) | Decorative — any JSON edit silently ignored |
| `ghost_recency_hours` | 48 | 48h (`today()-2`, decorative) | Decorative — any JSON edit silently ignored |
| `velocity_down_threshold_pct` | -40% | 40 abs check (close) | Actually correct for downs, but asymmetric intent lost |

## Exact Code Locations

### scout_bot.py — 3 hardcoded values to fix

**Line 266 — cap alert threshold (BUG: 70% vs config 90%):**
```python
if cap_pct < 0.70:   # should be: if cap_pct < _CAP_ALERT_PCT / 100:
```

**Line 298 — velocity prefilter (leave alone):**
```python
HAVING revenue_30d > 5000   # revenue floor for velocity tracking — not in signals config
```

**Line 321 — velocity threshold (BUG: symmetric vs asymmetric config):**
```python
if abs(pct_delta) < 40:   # should be: asymmetric check using _VELOCITY_DOWN/_UP_THRESHOLD_PCT
```

**Line 577 — fill rate sessions floor (decorative):**
```python
HAVING sessions_7d > 5000   # should use _FILL_RATE_MIN_SESSIONS_7D
```

### queries.py — ghost recency (decorative)

**Line 76-79 — `recent_imp` CTE uses hardcoded `today() - 2` (48h):**
```python
PREWHERE toYYYYMM(created_at) >= toYYYYMM(today() - 2)
WHERE created_at >= today() - 2
```
Needs `recency_hours` parameter passed via ClickHouse parameter binding.

### scout_agent.py — `_query_ghost_campaigns` wrapper (1 line)

Currently calls `_q.ghost_campaigns(ch)` with no args.
After: reads `ghost_recency_hours` from `SCOUT_THRESHOLDS["signals"]`, passes to `_q.ghost_campaigns()`.

## Proposed Changes

### Pattern: Mirror `_load_health_cfg()` for signals (scout_bot.py)

PR 18 established a pattern for wiring config to behavior. Mirror it:

```python
def _load_signal_cfg() -> dict:
    try:
        from scout_agent import SCOUT_THRESHOLDS
        return SCOUT_THRESHOLDS.get("signals", {})
    except Exception as e:
        log.warning(f"[signals] could not load thresholds, using fallback defaults: {e}")
        return {}

_SIGNAL_CFG                  = _load_signal_cfg()
_FILL_RATE_MIN_SESSIONS_7D   = int(_SIGNAL_CFG.get("fill_rate_min_sessions_7d", 5000))
_GHOST_RECENCY_HOURS         = int(_SIGNAL_CFG.get("ghost_recency_hours", 48))
_VELOCITY_DOWN_THRESHOLD_PCT = float(_SIGNAL_CFG.get("velocity_down_threshold_pct", -40))
_VELOCITY_UP_THRESHOLD_PCT   = float(_SIGNAL_CFG.get("velocity_up_threshold_pct", 20))
_CAP_ALERT_PCT               = float(_SIGNAL_CFG.get("cap_alert_pct", 90))
```

Place immediately after `_OFFER_STALENESS_HOURS` (line 182).

### Change 1: Cap alert (scout_bot.py:266)
```python
# Before
if cap_pct < 0.70:
# After
if cap_pct < _CAP_ALERT_PCT / 100:
```

### Change 2: Velocity threshold (scout_bot.py:321)
```python
# Before
if abs(pct_delta) < 40:
# After
if pct_delta > _VELOCITY_DOWN_THRESHOLD_PCT and pct_delta < _VELOCITY_UP_THRESHOLD_PCT:
```

Note: this is a real behavior change for the UP direction:
- Current: only alert when |pct_delta| ≥ 40 (so up alerts at +40%+)
- After: alert when pct_delta ≥ 20 (up alerts at +20%+)
- Slack output capped at top 5 by magnitude — won't flood

### Change 3: Fill rate sessions floor (scout_bot.py:577)
```python
# Before (in f-string already)
HAVING sessions_7d > 5000
# After
HAVING sessions_7d > {_FILL_RATE_MIN_SESSIONS_7D}
```

### Change 4: Ghost recency (queries.py)
```python
def ghost_campaigns(ch, recency_hours: int = 48) -> list[dict]:
```
In `recent_imp` CTE — switch from `today() - 2` to ClickHouse parameter binding:
```sql
recent_imp AS (
    SELECT campaign_id, count() AS impressions_2d
    FROM adpx_impressions_details
    PREWHERE toYYYYMM(created_at) >= toYYYYMM(now() - INTERVAL {recency_hours:UInt32} HOUR)
    WHERE created_at >= now() - INTERVAL {recency_hours:UInt32} HOUR
    GROUP BY campaign_id
    HAVING impressions_2d >= 2000
)
```
Pass `parameters={"recency_hours": recency_hours}` to `ch.query()`.

### Change 5: `_query_ghost_campaigns` caller (scout_agent.py)
```python
def _query_ghost_campaigns(ch) -> list:
    recency_hours = int(SCOUT_THRESHOLDS.get("signals", {}).get("ghost_recency_hours", 48))
    return _q.ghost_campaigns(ch, recency_hours=recency_hours)
```

## Smoke Test Additions (2 tests)

Per PR 19b: behavior names, not PR numbers.

1. `signal_thresholds_loaded_from_config_not_hardcoded` — verify `_FILL_RATE_MIN_SESSIONS_7D`,
   `_CAP_ALERT_PCT`, `_VELOCITY_DOWN_THRESHOLD_PCT`, `_VELOCITY_UP_THRESHOLD_PCT` match
   `SCOUT_THRESHOLDS["signals"]` values (catches future re-hardcoding).

2. `ghost_campaigns_accepts_recency_hours_parameter` — verify `queries.ghost_campaigns` signature
   has `recency_hours` param with default 48 (import inspect, check parameter).

## Files Touched

1. `scout_bot.py` — add `_load_signal_cfg()` block + 5 constants + 3 line fixes
2. `queries.py` — `ghost_campaigns()` signature + `recent_imp` CTE + `ch.query()` call
3. `scout_agent.py` — `_query_ghost_campaigns()` reads ghost_recency_hours from SCOUT_THRESHOLDS
4. `smoke_test.py` — 2 new behavioral tests

## Out of Scope

- `HAVING revenue_30d > 5000` in velocity query (revenue floor for tracking — not in signals config)
- Velocity `top_advertisers` `delta_abs < 100` filter (hardcoded attribution filter — not a tunable signal)
- Cap `days_remaining` / `days_to_cap` math (computed from date — not threshold-based)
- Overnight signal (no thresholds in config for it — none needed)

## Decision Audit Trail

| # | Phase | Decision | Classification | Principle | Rationale | Rejected |
|---|-------|----------|-----------|-----------|----------|---------|
| 1 | Scoping | Fix `cap_alert_pct` first — 70% vs 90% is an active misinformation bug | Mechanical | P1 | Config lies to the team about when they'll be alerted | Treating as decorative |
| 2 | Scoping | Fix velocity asymmetry — `abs(pct_delta) < 40` loses the +20% intent | Mechanical | P1 | Config says 20% up-alert; code fires at 40% — a silent miss class | Leaving symmetric |
| 3 | Arch | Mirror `_load_health_cfg()` pattern (lazy import, module-level constants) | Mechanical | P2/P4 | DRY — same pattern already established and reviewed in PR 18 | Inline reads inside functions |
| 4 | Arch | Pass `recency_hours` to `queries.ghost_campaigns()` — callers own the threshold, not queries.py | Mechanical | P3 | `queries.py` is stateless by contract (zero config reads) | Importing SCOUT_THRESHOLDS into queries.py |
| 5 | Testing | Add 2 behavioral smoke tests — one for constant loading, one for signature | Mechanical | P1 | Catches re-hardcoding regression without probing external state | No tests (decorative fix doesn't need tests) |
