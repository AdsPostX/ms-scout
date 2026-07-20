# Scout Features — Complete Inventory

**Last generated:** Auto-generated from `scout_agent.py` TOOL_MAP.
**Regenerate:** Run `python3 scripts/generate_feature_map.py` when TOOL_MAP changes.
**Status:** Audit complete (Engineering audit — see VAMSEE_AUDIT.md).

---

## Feature Summary

- **Total Tools:** 43
- **Domains:** 6
- **Status:** Engineering audit complete (24/24 violations fixed, see VAMSEE_AUDIT.md) — all tools working
- **Known gates (not audit failures):** demand_feed_main.py (5 TODOs, awaiting platform webhook URL), App Home projection range (blocked on upstream scoreboard_rollup())

---

## Offer Discovery

| Tool | Description | Handler | Status |
|------|-------------|---------|--------|
| `draft_campaign_brief` | Fetch all offer details needed to generate a campaign brief: tracking URL, payout, geo, "           ... | draft_campaign_brief() | ✓ Working |
| `get_category_performance` | Returns real CVR and RPM benchmarks from MS's live ClickHouse data, by category and by specific offe... | get_category_performance() | ✓ Working |
| `get_fallback_candidates` | When an offer might go dark (budget cap, advertiser pause, network issue | get_fallback_candidates() | ✓ Working |
| `get_offer_stats` | Returns aggregate inventory stats: count and avg Scout Score by network and category, "             ... | get_offer_stats() | ✓ Working |
| `get_offers_for_publisher` | f"Return top affiliate offers (from {', '.join(SUPPORTED_NETWORKS)} inventory) that are "           ... | get_offers_for_publisher() | ✓ Working |
| `get_running_offers` | Returns offers MS is currently running (MS Status = Live) with real CVR + RPM data where available. ... | get_running_offers() | ✓ Working |
| `get_top_opportunities` | Returns best untapped offers MS is NOT running (MS Status = Not in System | get_top_opportunities() | ✓ Working |
| `search_offers` | Full-text search across advertiser name and description. "             "Use for specific advertiser ... | search_offers() | ✓ Working |

## Publisher Intelligence

| Tool | Description | Handler | Status |
|------|-------------|---------|--------|
| `get_perkswall_engagement` | Perkswall offer selection analytics — which offers do loyalty members actually pick? "             "... | get_perkswall_engagement() | ✓ Working |
| `get_publisher_competitive_landscape` | Query ClickHouse for what's currently running on a specific publisher (e.g. AT&T, TXB, MLB, or partn... | get_publisher_competitive_landscape() | ✓ Working |
| `get_publisher_fleet_health` | Fleet-level publisher health using statistical (σ-based) baseline. "             "Classifies publish... | get_publisher_fleet_health() | ✓ Working |
| `get_publisher_health` | Full publisher health analysis: sessions, impressions, clicks, conversions, revenue, RPM, CTR, and C... | get_publisher_health() | ✓ Working |
| `get_publisher_revenue_trends` | Publisher velocity: identifies publishers trending significantly up or down in revenue. "           ... | get_publisher_revenue_trends() | ✓ Working |

## Campaign & Revenue

| Tool | Description | Handler | Status |
|------|-------------|---------|--------|
| `get_advertiser_revenue_projection` | Project gross revenue for a specific advertiser across ALL MS publisher partners for a target month.... | get_advertiser_revenue_projection() | ✓ Working |
| `get_advertiser_revenue_trends` | Advertiser revenue trends: compare each advertiser's actual revenue over the last N days "          ... | get_advertiser_revenue_trends() | ✓ Working |
| `get_campaign_status` | Check if an advertiser's campaigns are active or paused, and see recent changes from the audit log. ... | get_campaign_status() | ✓ Working |
| `get_expiring_campaigns` | Find active campaigns expiring within the next N days (default 7). "             "Includes last-7d i... | get_expiring_campaigns() | ✓ Working |
| `get_exposure_rate_anomalies` | Find publisher-campaign pairs where yesterday's exposure conversion rate dropped significantly "    ... | get_exposure_rate_anomalies() | ✓ Working |
| `get_ghost_campaigns` | Return the full list of ghost campaigns: active campaigns with high impressions + clicks "          ... | get_ghost_campaigns() | ✓ Working |
| `get_low_fill_publishers` | Return publishers on post-transaction placements (checkout confirmation, order receipt, "           ... | get_low_fill_publishers() | ✓ Working |
| `get_revenue_today` | Return today's intraday revenue vs 30-day daily average, broken down by publisher. "             "Us... | get_revenue_today() | ✓ Working |
| `get_revenue_today_projection` | Project today's END-OF-DAY revenue using a 90-day hour-of-day arrival curve and "             "8-wee... | get_revenue_today_projection() | ✓ Working |
| `get_top_revenue_opportunities` | Return top cross-publisher revenue gap opportunities: high-performing advertisers "             "(ac... | get_top_revenue_opportunities() | ✓ Working |

## Pipeline Management

| Tool | Description | Handler | Status |
|------|-------------|---------|--------|
| `get_demand_queue_status` | Read the MS Demand Queue — cross-references ClickHouse to detect if any queued offer "             "... | get_demand_queue_status() | ✓ Working |
| `get_pipeline_health` | Report on the Scout offer approval pipeline: total approved offers, "             "stale offers (>7 ... | get_pipeline_health() | ✓ Working |
| `get_queue_status` | Fetch the offer pipeline queue from Notion and return a Slack Block Kit card. "             "Grouped... | get_queue_status() | ✓ Working |
| `mark_offer_launched` | Mark an approved offer as live. Updates queue state and triggers a notification "             "to th... | mark_offer_launched() | ✓ Working |

## Analytics & Insights

| Tool | Description | Handler | Status |
|------|-------------|---------|--------|
| `get_pulse_summary` | Returns which monitoring signals have fired today (cap, velocity, ghost, fill, CVR anomaly, expirati... | get_pulse_summary() | ✓ Working |
| `get_scout_status` | Return a system health snapshot: benchmark freshness, offer inventory count, "             "queue de... | get_scout_status() | ✓ Working |
| `get_supply_demand_gaps` | Identify supply-demand gaps: which advertisers are performing on other publishers but missing from a... | get_supply_demand_gaps() | ✓ Working |
| `run_sql_query` | Execute an arbitrary ClickHouse SELECT query for questions not covered by other tools. "            ... | run_sql_query() | ✓ Working |

## Administration

| Tool | Description | Handler | Status |
|------|-------------|---------|--------|
| `export_usage_log` | Dump raw (query → tools fired) pairs from usage_log so an admin can audit "             "whether Sco... | export_usage_log() | ✓ Working |
| `force_run_monitor` | Admin-only — requires SCOUT_THRESHOLD_ADMINS env match. "             "Run a silent-monitor signal i... | force_run_monitor() | ✓ Working |
| `forget_entity_note` | Drop a previously-recorded publisher or advertiser fact. "             "Use when a team member tells... | forget_entity_note() | ✓ Working |
| `get_scout_config` | Return Scout's current active configuration: scoring thresholds, signal thresholds, "             "h... | get_scout_config() | ✓ Working |
| `get_threshold_history` | Return recent threshold-change events from the changelog. "             "Optional 'key' filter (e.g.... | get_threshold_history() | ✓ Working |
| `get_usage_report` | Return Scout usage statistics: queries per period, top users, most-used tools, avg response time. " ... | get_usage_report() | ✓ Working |
| `list_thresholds` | Return all active Scout monitor thresholds plus override metadata. "             "Use when: 'what ar... | list_thresholds() | ✓ Working |
| `record_entity_note` | Record publisher or advertiser knowledge in Scout's persistent learning store. "             "Use wh... | record_entity_note() | ✓ Working |
| `run_offer_scraper` | Trigger an immediate offer inventory refresh from affiliate networks "             f"({', '.join(SUP... | run_offer_scraper() | ✓ Working |
| `run_self_qa` | Run Scout's full self-QA suite — 15 representative questions covering every major intent "          ... | run_self_qa() | ✓ Working |
| `set_threshold` | Admin-only — requires SCOUT_THRESHOLD_ADMINS env match. "             "Write a runtime override for ... | set_threshold() | ✓ Working |
| `why_entity_note` | Explain where a stored publisher/advertiser fact came from — returns the note, "             "who ta... | why_entity_note() | ✓ Working |

---

## Maintenance & Updates

### How to Update This File

1. **After adding a new tool to TOOL_MAP:**
   - Add the tool definition to `TOOLS` array in `scout_agent.py`
   - Add the handler to `TOOL_MAP` in `scout_agent.py`
   - Run: `python3 scripts/generate_feature_map.py`
   - Commit both changes together

2. **After completing the engineering audit:**
   - Update the Status column above (Working/Deferred/In Maintenance)
   - Log findings in VAMSEE_AUDIT.md

3. **To regenerate (one-liner):**
   ```bash
   python3 scripts/generate_feature_map.py
   ```

### Pre-Commit Hook (Optional)

Add to `.git/hooks/pre-commit` to auto-regenerate when TOOL_MAP changes:

```bash
#!/bin/bash
if git diff --cached scout_agent.py | grep -q 'TOOL_MAP\|TOOLS = '; then
  python3 scripts/generate_feature_map.py
  git add FEATURES.md
fi
```

---

## Feature Status Legend

| Status | Meaning |
|--------|---------|
| ✓ Working | Feature complete, tests passing, engineering audit passed |
| ⏳ Deferred | Feature built but gated (e.g., awaiting API, blocked PR) |
| 🔧 In Maintenance | Feature working but has known debt (TODO items, gaps) |
| ❌ Broken | Feature implemented but failing tests or not called |
