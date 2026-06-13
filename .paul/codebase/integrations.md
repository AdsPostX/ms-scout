# Scout — External Integrations

## Slack (slack-sdk ~3.41)

**Connection**: Socket Mode (persistent WebSocket) via `SocketModeClient`

**Credentials**: `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`

**Key modules**: `scout_bot.py` (listener), `scout_slack_safe.py` (WebClient wrapper), `scout_handlers.py` (event logic)

**Hardcoded channel defaults** (overridable via env):
- `PULSE_CHANNEL` → C06F1RPPVBL (#revenue-operations)
- `SCOUT_DIGEST_CHANNEL` → C0ARLAMDL1K (#scout-offers)
- `SCOUT_SHADOW_CHANNEL` → C0AQEECF800 (#sidd-qa)

**Slash commands** registered at api.slack.com/apps (not in code — manifest):
`/scout-cap`, `/scout-vel`, `/scout-ghost`, `/scout-fill`, `/scout-signal-status`, `/scout-pub`, `/scout-enter`, `/scout-queue`, `/scout-status`, `/scout-help`

---

## Anthropic Claude (anthropic ~0.97)

**Credential**: `ANTHROPIC_API_KEY`

**Key modules**: `scout_agent.py` (agent loop), `scout_notion.py` (copy generation)

**Features used**:
- Tool use (20+ tools in `ask()` loop)
- Prompt caching (ephemeral, `cache_control` headers)
- Retry with exponential backoff (3 attempts, 1–4s)

**Model**: Claude (version via SDK default at ~0.97)

**Models by task**:
- `ask()` agent loop → full Claude (Opus/Sonnet via SDK default)
- `_generate_offer_copy()` → Claude Haiku (faster, cheaper copy generation)

---

## ClickHouse (clickhouse-connect ~0.15)

**Credentials**: `CH_HOST`, `CH_USER` (default: "analytics"), `CH_PASSWORD`, `CH_DATABASE` (default: "default")

**Key modules**: `scout_ch.py` (client), `queries.py` (SQL), `scout_core/job_runs.py` (telemetry writes)

**Concurrency cap**: `BoundedSemaphore(4)` in `scout_ch.py` prevents query overload.

**Key tables read**:
- `from_airbyte_campaigns` — campaign metadata
- `from_airbyte_publisher_campaigns` — publisher-campaign join
- `from_airbyte_campaign_metrics` — performance metrics
- `from_airbyte_publisher_metrics` — publisher performance
- `from_airbyte_publisher_campaign_images` — creative assets

**Tables written (telemetry)**:
- `job_runs` — daemon health telemetry
- `per_network_status` — ReplacingMergeTree for network scrape health
- `normalization_errors` — field-level ingest errors

Schema file: `migrations/2026_05_p2_3_job_runs.sql`

---

## Notion (requests library, API v1)

**Credentials**: `NOTION_TOKEN`, `NOTION_DB_ID` (offer inventory), `NOTION_QUEUE_DB_ID` (approval queue)

**Key module**: `scout_notion.py`

**Operations**:
- POST `/v1/pages` — create offer page in queue
- PATCH `/v1/pages/{id}` — update offer status
- POST `/v1/databases/{id}/query` — query offer inventory
- PATCH `/v1/blocks/{id}/children` — add blocks to pages

---

## Affiliate Networks (offer_scraper.py)

All credential patterns: `{NETWORK}_API_KEY` / `{NETWORK}_TOKEN` env vars.

| Network | Type | Key Env Vars |
|---------|------|-------------|
| Impact | REST API | `IMPACT_SID`, `IMPACT_TOKEN` |
| FlexOffers | REST API | `FLEXOFFERS_API_KEY`, `FLEXOFFERS_DOMAIN_ID` |
| MaxBounty | REST API | `MAXBOUNTY_EMAIL`, `MAXBOUNTY_PASSWORD` |
| Commission Junction | GraphQL + REST | `CJ_API_KEY`, `CJ_WEBSITE_ID`, `CJ_PUBLISHER_ID` |
| ShareASale | REST + HMAC | `SHAREASALE_API_TOKEN`, `SHAREASALE_API_SECRET`, `SHAREASALE_AFFILIATE_ID` |
| Rakuten | OAuth REST | `RAKUTEN_API_TOKEN`, `RAKUTEN_PUBLISHER_ID` |
| AWIN | REST | `AWIN_API_KEY`, `AWIN_PUBLISHER_ID` |
| TUNE (multiple) | REST | `TUNE_{NAME}_API_KEY`, `TUNE_{NAME}_NETWORK_ID`, `TUNE_{NAME}_BASE_URL` |
| Everflow (multiple) | REST | `EVERFLOW_{NAME}_API_KEY`, `EVERFLOW_{NAME}_BASE_URL` |

**TUNE instances**: AdAction Interactive, RevOffers, KashKick, Brown Boots, AdBloom, Successful Media

**Everflow instances**: GiddyUp, Accio Ads, KlayMedia, Credit.com, MWK Consulting, Pawzitivity, Aragon Premium

---

## Upstash Redis (upstash-redis ~1.1)

**Credentials**: `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN` (NOT set in render.yaml — in-memory fallback active)

**Key module**: `alert_registry.py`

**Storage**: Hash at key `scout:alert_registry`

**Fallback**: In-memory dict `_STATE` + persist to `pulse_state.json` on each change.

**Status**: Deferred (deferred gate in smoke_test.py, check-in 2026-07-18).

---

## Latitude Telemetry (latitude-telemetry ~1.0, latitude-sdk ~5.0)

**Credentials**: `LATITUDE_API_KEY`, `LATITUDE_PROJECT_ID`, `LATITUDE_PROMPT_PATH` (default: "scout/system")

**Key module**: `scout_telemetry.py`

**WARNING**: Current PyPI release (1.0.0) exports to a decommissioned OTLP endpoint — spans are captured but not delivered. Gracefully disabled if unconfigured.

---

## Google Sheets (google-api-python-client ~2.193)

**Auth**: Anonymous (public Sheets via CSV export URL)

**Module**: `scout_attachments.py`

**Endpoint**: `https://docs.google.com/spreadsheets/d/{id}/export?format=csv`

**Security**: SSRF-guarded — host allowlist, max 3 redirect hops, private IP blocked via `_resolves_to_private_ip()`.

---

## External CDN / API (scout_images.py)

| Service | Endpoint | Auth |
|---------|----------|------|
| Clearbit autocomplete | `https://autocomplete.clearbit.com/v1/companies/suggest` | None |
| Google gstatic favicon | `https://t3.gstatic.com/faviconV2` | None |
| iTunes Search | `https://itunes.apple.com/search` | None |
| quickchart.io | `https://quickchart.io/sparkline` | None (URL-encoded chart) |

---

## MS Platform Campaign Webhook (DEFERRED)

**Credentials**: `CAMPAIGN_CREATE_WEBHOOK_URL`, `CAMPAIGN_CREATE_API_KEY`, `CAMPAIGN_CREATE_DRY_RUN` (default: "true")

**Module**: `demand_feed_main.py` (5 `MS_PLATFORM_TODO` markers)

**Status**: Blocked — waiting for Vamsee to deliver webhook URL + API key. Check-in: 2026-06-21.
