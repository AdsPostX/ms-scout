# Scout — Architecture

## Module Map

### Root-Level Modules

| File | Lines | Role |
|------|-------|------|
| `scout_bot.py` | 2,039 | Socket Mode listener + daemon orchestrator. Registers all Slack handlers, starts 9 background threads, runs startup smoke test. Entry point for ms-scout service. |
| `scout_agent.py` | 6,323 | **God module (known debt).** Core Claude agent: `ask()` / `ask_with_attachment()` agentic loop, all 20+ tool implementations, threshold management, benchmark caching, ClickHouse re-exports. |
| `scout_handlers.py` | 3,352 | Slack event dispatch table: @mentions, button clicks, slash commands, modal submissions. Bridges Slack events → `ask()` → `wrap_response()` → Slack post. |
| `scout_ui_kit.py` | 1,874 | **Pure UI module.** Block Kit builders: `Card`, `Severity`, `Surface`, `ResponsePattern`, `wrap_response()`. Zero Slack API calls, zero state. |
| `scout_digest.py` | 1,749 | Weekly offer digest: score offers from `offers_latest.json`, rank, deduplicate, format Block Kit, post with approve/reject buttons. |
| `scout_notion.py` | ~1,100 | Notion queue integration: AI copy generation (Claude Haiku), write approved offers to Notion DB, batch enrichment loop. |
| `scout_state.py` | ~860 | **Single I/O point** for `data/` directory. Atomic writes (`tmp → os.replace`). Thread-safe via `_PULSE_STATE_LOCK`, `_MAINTENANCE_LOCK`. |
| `scout_ch.py` | ~1,050 | ClickHouse client management. `_get_ch_client()`, `BoundedSemaphore(4)` concurrency cap, `_LoggingCHClient` wrapper. Re-exports scout_core functions. |
| `queries.py` | 2,506 | All ClickHouse SQL: parameterized queries for cap, velocity, ghost, fill, revenue, publisher health. No business logic — pure SQL generation. |
| `offer_scraper.py` | 2,392 | Affiliate network scrapers (Impact, FlexOffers, MaxBounty, CJ, ShareASale, Rakuten, AWIN, TUNE, Everflow). Produces `offers_latest.json`. Registers `normalize_geo()` into `scout_core.contracts`. |
| `demand_feed_main.py` | ~1,680 | ms-demand-feed entry point. HTTP server on port 8080 (`/health`, `/last-run`, `/digest/blocks`). Runs scraper at 06:00 CT via multiprocessing. |
| `alert_registry.py` | ~180 | Monitor alert state: `mark_firing()`, `mark_cleared()`, `current_state()`. Upstash Redis backend with in-memory fallback. Never raises. |
| `scout_types.py` | ~120 | `TypedDict` contracts: `Offer`, `FormattedOffer`, `Brief`, `PulseSignal`. Documentation only — no runtime enforcement. |
| `scout_attachments.py` | ~530 | Slack file + Google Sheets ingestion. Dispatch table `_EXTRACTORS` by MIME type. SSRF-guarded Sheets fetch. |
| `scout_telemetry.py` | ~100 | Latitude telemetry wrapper. `capture(event_name, fn)` traces Claude calls. Silently no-ops if unconfigured. |
| `scout_slack_safe.py` | ~90 | WebClient safety wrapper. `guard_web_client()` validates token + applies retry handlers. |
| `scout_images.py` | ~175 | External image sourcing: Clearbit domain → favicon, iTunes App Store icon, OG image scraping. Cached to `data/image_cache.json`. |

### scout_core/ Package

| Module | Role |
|--------|------|
| `contracts.py` | **Typed data contracts**: `RawOffer`, `NormalizedOffer`, `DigestCandidate`, `SlackDigestBlock`. Accepts `geo_normalizer` injection. Zero external deps. |
| `monitors.py` | Generic hourly monitor loop: `_run_hourly_with_web()`. 5-min poll, dedup by slot + escalation, fires resolved message when signal clears. Used by all 4 monitor signals. |
| `job_runs.py` | Best-effort telemetry writers to ClickHouse `job_runs`, `per_network_status`, `normalization_errors` tables. Never raises, never crashes callers. |

## Service Split

| Responsibility | ms-scout (scout_bot.py) | ms-demand-feed (demand_feed_main.py) |
|----------------|------------------------|--------------------------------------|
| Slack listening | ✓ Socket Mode | ✗ |
| @Scout agent | ✓ ask() loop | ✗ |
| Offer approval flow | ✓ buttons + copy gen | ✗ |
| Offer scraping | ✗ (removed) | ✓ 06:00 CT daily |
| offers_latest.json | reads | writes |
| Pulse monitors | ✓ cap/vel/ghost/fill daemons | ✗ |
| Health heartbeat | ✓ 30-min | ✗ |
| HTTP /health endpoint | ✓ Render probe | ✓ Render probe |
| Watchdog + recap daemons | ✓ | ✗ |

## Key Data Flows

### 1. @Scout Mention → Response

```
Slack @mention
  → scout_bot._handle_socket_mode_request()
  → scout_handlers.handle_event()
    → _is_under_maintenance() gate
    → _ask_with_timeout(query, timeout=90s, semaphore_cap=3)
      → thread: scout_agent.ask()
        → _get_thread_context(thread_ts)    # inject prior entity context
        → build system prompt (prompts/scout_system.md)
        → Claude tool-use loop (max 12 rounds)
          → tool call → ClickHouse via queries.py
          → feed result back to Claude
        → return AskResult(text, tools_called, payload)
    → scout_ui_kit.wrap_response(payload, surface=CHANNEL_ROOT)
    → web.chat_postMessage(channel, blocks)
    → _merge_thread_context(thread_ts, entities)
```

### 2. Offer Digest → Approval → Notion

```
Daily 07:00 CT (scout_bot._digest_poster daemon)
  → scout_digest.post_digest()
    → load offers_latest.json
    → filter by min_rpm_floor (config: 20)
    → score: payout × CONVERSION_COMPLEXITY × category_fit_bonus
    → deduplicate via fuzzy name match
    → format as Block Kit with Approve/Reject buttons
    → post to SCOUT_DIGEST_CHANNEL

User clicks "Approve"
  → scout_handlers._handle_approve()
    → scout_notion._generate_offer_copy() → Claude Haiku → marketing copy
    → scout_notion._write_to_notion_queue() → Notion DB
    → data/launched_offers.json: status = "queued"
    → post confirmation in thread
```

### 3. Monitor Alert Pipeline

```
scout_bot._monitor_{cap,velocity,ghost,fill}_hourly daemons
  → scout_core.monitors._run_hourly_with_web()
    → 5-min poll loop (business hours CT)
    → load prior slot + severity snapshot
    → queries.py → ClickHouse
    → if new advertisers OR escalation ≥ threshold:
        → alert_registry.mark_firing(name, context)
        → format alert block → post to SCOUT_MONITOR_CHANNEL
    → if results empty:
        → alert_registry.mark_cleared(name)
        → post "✅ resolved" message
```

## State Files (data/ directory)

| File | Owner | Purpose |
|------|-------|---------|
| `pending_briefs.json` | scout_handlers | Brief summaries keyed by thread_ts |
| `thread_context.json` | scout_handlers | Entity context (publisher/offer/category) per thread |
| `launched_offers.json` | scout_handlers + daemons | Offer lifecycle: queued → launched → live |
| `pulse_state.json` | daemons + alert_registry | Monitor slot state + alert snapshot |
| `watchdog_state.json` | _launch_watchdog daemon | Launch health check history |
| `learnings.json` | scout_handlers | User feedback (👍/👎) |
| `learned_benchmarks.json` | _performance_recap daemon | Actual vs estimated RPM by advertiser/category |
| `threshold_overrides.json` | scout_agent (set_threshold tool) | Runtime knob changes without redeploy |
| `maintenance_state.json` | scout_handlers | Maintenance mode gate |
| `offers_latest.json` | offer_scraper.py | Current offer inventory (~5K offers) |
| `image_cache.json` | scout_images.py | Creative asset URL cache |

## Import Dependency Graph (simplified)

```
scout_bot.py
  ├── scout_handlers (handle_event)
  ├── scout_agent (ask)
  ├── scout_notion (_copy_coalescer_loop)
  ├── scout_state
  ├── scout_ch
  └── scout_ui_kit

scout_handlers.py
  ├── scout_agent (ask, ask_with_attachment)
  ├── scout_ui_kit (wrap_response, builders)
  ├── scout_state
  └── scout_notion

scout_agent.py
  ├── scout_types
  ├── scout_ch (re-exports)
  ├── scout_images
  ├── scout_core.contracts
  ├── offer_scraper (normalize_geo)
  └── queries

scout_ch.py
  ├── queries
  └── scout_core (re-exports)

scout_core/contracts.py  # NO external deps — pure data types
scout_core/monitors.py
  ├── scout_ch._get_ch_client
  └── alert_registry

scout_core/job_runs.py
  └── scout_ch._get_ch_client (lazy)

alert_registry.py
  └── scout_state (lazy, circular-import guard)

scout_state.py  # NO imports from scout_agent
```

## Background Daemons (scout_bot.py)

| Daemon | Interval | Purpose |
|--------|----------|---------|
| `_digest_poster` | Daily 07:00 CT | Posts weekly offer digest |
| `_launch_watchdog` | Daily 10:00 CT | Checks launched offers for first impressions |
| `_performance_recap` | Daily | 7-day post-launch actual vs estimate comparison |
| `_check_stale_queue` | Daily | Nudges queued offers with no impressions after 7d |
| `_cleanup_state` | Weekly | Prunes stale thread_context + briefs |
| `_health_heartbeat` | Every 30 min | Posts degraded/ok transitions to #scout-qa |
| `_monitor_cap_hourly` | 5-min poll, 9am–5pm CT | Cap alert signal |
| `_monitor_velocity_hourly` | 5-min poll | Velocity down/up signal |
| `_monitor_ghost_hourly` | 5-min poll | Zero-conversion (ghost) signal |
| `_monitor_fill_hourly` | 5-min poll | Fill rate signal |
