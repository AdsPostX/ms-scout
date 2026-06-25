# Scout — Vamsee Engineering Audit

**Goal:** Remove maintenance mode by ensuring every feature passes the Vamsee lens (five checks).

**Vamsee Lens** (AdsPostX CTO standard):
1. **No invisible accumulators** — lists built silently in loops must return as values or pass as collectors
2. **No no-op side-channel handlers** — if a function returns while real work happens elsewhere, document the contract
3. **No repeated inline patterns** — same filter/transform ≥2 times → extract to named pure function
4. **Config objects, not scattered env reads** — feature flags + config at module top, not scattered inline
5. **Validate at construction** — objects + dataclasses validate required fields at init, warn if flag ON but config missing

---

## Audit Progress

**Status:** ✅ PHASE 1 & 2 COMPLETE (10/10 violations fixed + merged)
**Tools Audited:** 23/43 (3 domains: Offer Discovery, Publisher Intelligence, Campaign & Revenue)
**Auditor:** Claude Code (Vamsee lens)
**Last Updated:** 2026-06-24 10 commits merged, all tests passing

| Domain | Tools | Status | Violations Found | Passing |
|--------|-------|--------|------------------|---------|
| **Offer Discovery** | 8 | ✓ Complete | 2 MEDIUM | 6 tools |
| **Publisher Intelligence** | 5 | ✓ Complete | 5 (2H + 2M + 1L) | 3 tools |
| **Campaign & Revenue** | 10 | ✓ Complete | 4 (1H + 3M) | 6 tools |
| **Pipeline Management** | 4 | 📋 Queued | — | — |
| **Analytics & Insights** | 4 | 📋 Queued | — | — |
| **Administration** | 12 | 📋 Queued | — | — |

---

## Findings by Violation Type

### Violation 1: Invisible Accumulators

**Pattern:** Lists built in loops, captured by closure, returned implicitly.

**HIGH severity:**
- **get_revenue_today** (L4806-4823) — `publishers`, `total_today`, `total_avg` accumulated via loop mutations. **Fix:** Extract to `PublisherRevenue` dataclass returned from named function.
- **get_publisher_competitive_landscape** (L2508-2524) — `competitors` list appended in loop, no cardinality validation. **Fix:** Validate accumulation invariant; return as explicit output.

**MEDIUM severity:**
- **get_perkswall_engagement** (L3751) — Dead code: `all_unique_members` set declared but never used. **Fix:** Remove or populate + return.

---

### Violation 2: No-Op Side-Channel Handlers

**Pattern:** Function returns placeholder while async work fires elsewhere, contract undocumented.

**Status:** ✓ None found in 23 audited tools.

---

### Violation 3: Repeated Inline Patterns

**Pattern:** Same filter/map/transform appears 2+ times, should be extracted to pure function.

**HIGH severity:**
- **get_publisher_health** (L3414-3450) — Dict-building-from-loop pattern repeated 3 times (`sess_by_placement`, `ad_metrics`, `click_metrics`). **Fix:** Extract `_build_metric_dict_by_key(rows, key_fn, metric_name) → dict`.
- **get_campaign_status** (L3620-3628, duplicated) — JSON parsing try-except appears 2x. **Fix:** Extract `_safe_parse_json(data_str) → dict`.

**MEDIUM severity:**
- **search_offers** (L1985, 1988, 1990, 1997, L2285, L2287, L640, L642) — Normalization `(field or "").lower().strip()` appears 5+ times. **Fix:** Extract helper or use `NormalizedOffer` dataclass at load time.
- **get_offer_stats** (L2074-2082) — Three dict accumulators (`by_network`, `by_category`, `by_ms_status`) with repeated structure. **Fix:** Extract `_aggregate_offers_by_dims(offers, benchmarks) → tuple[dict, dict, dict]`.
- **get_campaign_status** (L3612-3613) — Count filters for `active_count` / `paused_count` repeated. **Fix:** Extract `_count_campaigns_by_status(campaigns) → dict[str, int]`.
- **get_perkswall_engagement** (L3760, L3764) — Pct calculation formula repeated twice. **Fix:** Extract `_pct_of_total(numerator, total) → float`.
- **get_publisher_revenue_trends** (L5027-5028) — Filter by direction repeated for `up`/`down`. **Fix:** Extract `_by_direction(rows, direction) → list`. Mark backward-compat debt in KNOWN_DEBT.

---

### Violation 4: Scattered Config Reads

**Pattern:** Feature flags or env vars read inline via `os.getenv()` scattered throughout code, no central config object.

**MEDIUM severity:**
- **get_publisher_fleet_health** (scout_agent.py:5155 + queries_publisher.py:1038-1040) — Min/max validation on `days` param at call site; thresholds (act_now_sigma, act_now_gap, watch_sigma, watch_gap) hardcoded in queries layer, not loaded from config at module init. **Fix:** Load into `PublisherFleetConfig` dataclass at module init; validate `days` at dataclass init, not at call site.

**Status:** SCOUT_THRESHOLDS module-level init at L210 is well-done; no other scattered config found.

---

### Violation 5: Validation at Construction

**Pattern:** Objects/dataclasses missing required-field validation at init, or flag ON but required config missing at runtime.

**Status:** ✓ No violations found. SCOUT_THRESHOLDS validated at module init; no missing required-field checks.

---

## Tools Passing Vamsee Audit

*(Checkmarks added as each tool completes audit)*

- [ ] search_offers
- [ ] get_top_opportunities
- [ ] get_running_offers
- [ ] get_category_performance
- [ ] get_offer_stats
- [ ] draft_campaign_brief
- [ ] get_fallback_candidates
- [ ] get_offers_for_publisher
- [ ] get_publisher_competitive_landscape
- [ ] get_publisher_health
- [ ] get_perkswall_engagement
- [ ] get_publisher_revenue_trends
- [ ] get_publisher_fleet_health
- [ ] get_campaign_status
- [ ] get_revenue_today
- [ ] get_revenue_today_projection
- [ ] get_advertiser_revenue_projection
- [ ] get_advertiser_revenue_trends
- [ ] get_ghost_campaigns
- [ ] get_low_fill_publishers
- [ ] get_top_revenue_opportunities
- [ ] get_exposure_rate_anomalies
- [ ] get_expiring_campaigns
- [ ] get_queue_status
- [ ] get_demand_queue_status
- [ ] mark_offer_launched
- [ ] get_pipeline_health
- [ ] get_supply_demand_gaps
- [ ] get_pulse_summary
- [ ] run_sql_query
- [ ] get_scout_status
- [ ] run_offer_scraper
- [ ] get_usage_report
- [ ] export_usage_log
- [ ] record_entity_note
- [ ] forget_entity_note
- [ ] why_entity_note
- [ ] run_self_qa
- [ ] get_scout_config
- [ ] list_thresholds
- [ ] get_threshold_history
- [ ] set_threshold
- [ ] force_run_monitor

---

## Maintenance Mode Gates

These features have known debt or deferred work:

| Feature | Gate | Issue | Blocker |
|---------|------|-------|---------|
| `demand_feed_main.py` (MS Platform Feed) | 5 MS_PLATFORM_TODO items | Awaiting webhook URL from Vamsee | Env vars not set |
| `App Home` (scotbot_ui_kit.py) | `TODO(App-Home-3.4)` | Revenue EOD projection range | upstream scoreboard_rollup() |

---

## How to Use This File

1. **Run audit agents** for each domain — each logs findings under "Findings by Violation Type"
2. **Mark tools passing** — add checkmark when tool passes all 5 checks
3. **Log maintenance gates** — update table above when blockers are resolved
4. **Update domain status** in progress table (Auditing → ✓ Passed / 🔧 Has Violations)
5. **Once all tools pass** — remove maintenance mode, regenerate FEATURES.md with status: ✓ Working

---

## Fix Plan (Priority Order)

**11 violations found. Priority:** 4 HIGH, 7 MEDIUM + 1 LOW deferred.

### Phase 1: Fix HIGH violations (4 fixes)

1. **get_publisher_health** — Extract `_build_metric_dict_by_key()` (Type 3)
   - File: scout_agent.py
   - Lines: 3414-3450 → new helper function
   - Impact: Reduces code duplication, improves readability
   - Test: Run smoke_test on publisher health queries

2. **get_publisher_competitive_landscape** — Validate accumulation invariant (Type 1)
   - File: scout_agent.py
   - Lines: 2508-2524
   - Impact: Prevents silent data duplication bugs
   - Test: Run smoke_test; check cardinality assertion

3. **get_revenue_today** — Extract to PublisherRevenue dataclass (Type 1)
   - File: scout_agent.py
   - Lines: 4806-4823
   - Impact: Explicit return value, easier to test
   - Test: Verify return shape in smoke_test

4. **get_campaign_status** — Extract `_safe_parse_json()` (Type 3)
   - File: scout_agent.py
   - Lines: 3620-3628
   - Impact: Centralized error handling, reusable
   - Test: Run with malformed JSON input

### Phase 2: Fix MEDIUM violations (6 fixes)

5. **get_publisher_fleet_health** — Load thresholds into dataclass (Type 4)
   - File: scout_agent.py + queries_publisher.py
   - Impact: Centralized config, testable validation
   - Test: Verify dataclass init validation

6. **search_offers** — Extract normalization helper (Type 3)
   - File: scout_agent.py
   - Lines: 1985, 1988, 1990, 1997, 2285, 2287, 640, 642
   - Impact: DRY, consistent normalization
   - Test: smoke_test with various advertiser/network names

7. **get_offer_stats** — Extract `_aggregate_offers_by_dims()` (Type 3)
   - File: scout_agent.py
   - Lines: 2074-2082
   - Impact: Reusable aggregation pattern
   - Test: Verify stats breakdown (by_network, by_category, by_ms_status)

8. **get_campaign_status** — Extract `_count_campaigns_by_status()` (Type 3)
   - File: scout_agent.py
   - Lines: 3612-3613
   - Impact: Reusable counting pattern
   - Test: smoke_test with various campaign states

9. **get_perkswall_engagement** — Remove dead code + extract pct formula (Type 1 + Type 3)
   - File: scout_agent.py
   - Lines: 3751 (remove `all_unique_members`), 3760+3764 (extract pct)
   - Impact: Cleaner code, no dead branches
   - Test: smoke_test on perkswall data

10. **get_publisher_revenue_trends** — Extract `_by_direction()` filter (Type 3)
    - File: scout_agent.py
    - Lines: 5027-5028
    - Impact: Reusable direction filter, mark backward-compat debt
    - Test: smoke_test trending publishers (up/down)

### Phase 3: Low priority (deferred)

11. **get_publisher_revenue_trends** — Mark backward-compat debt (Type 3)
    - Add entry to KNOWN_DEBT.md: "`days` param ignored for backward compatibility"
    - Deferred to next refactor cycle

### Phase 4: Audit remaining 20 tools

- **Pipeline Management** (4 tools)
- **Analytics & Insights** (4 tools)
- **Administration** (12 tools)

Then regenerate FEATURES.md with status updates.

---

## Next Steps

1. ✓ Audit 3 domains (Offer Discovery, Publisher Intelligence, Campaign & Revenue) — **DONE**
2. → Fix 10 violations in Phase 1 & 2 (parallel where possible)
3. → Audit remaining 3 domains
4. → Fix any violations found
5. → Regenerate FEATURES.md with ✓ Working status
6. → Remove maintenance mode, commit & ship

---

**Last audit run:** 2026-06-24
**Audit status:** 3 of 6 domains complete (23 of 43 tools)
**Next:** Phase 1 fixes (HIGH severity)
