# Scout — Technology Stack

## Runtime

- **Python 3.12** (`.python-version`, `pyproject.toml` target-version = "py312")
- **Render** (worker + web service, Oregon, Starter $7/mo each)
- Persistent disk: 1GB at `/opt/render/project/src/data` (both services)

## Core Dependencies (requirements.txt)

| Package | Version | Role |
|---------|---------|------|
| `anthropic` | ~0.97 | Claude API client (agent loop, copy generation) |
| `slack-sdk` | ~3.41 | Socket Mode + WebClient for all Slack I/O |
| `clickhouse-connect` | ~0.15 | ClickHouse analytics queries |
| `requests` | ~2.32 | HTTP calls (affiliate APIs, image scraping) |
| `python-dotenv` | ~1.2 | `.env` loading |
| `upstash-redis` | ~1.1 | Alert state persistence (Redis fallback to in-memory) |
| `latitude-telemetry` | ~1.0 | Claude call tracing (optional; decommissioned endpoint) |
| `latitude-sdk` | ~5.0 | Prompt fetch from Latitude platform |
| `google-api-python-client` | ~2.193 | Google Sheets CSV export |
| `google-auth-oauthlib` | ~1.3 | Google auth (anonymous Sheets only in practice) |
| `pandas` | ~2.2 | CSV/XLSX parsing, Sheets data |
| `tabulate` | ~0.9 | Markdown table formatting |
| `pdfplumber` | ~0.11 | PDF text extraction (fallback to `pdftotext` CLI) |
| `openpyxl` | ~3.1 | XLSX reading |
| `python-docx` | ~1.1 | DOCX reading |
| `xlrd` | ~2.0 | Legacy XLS reading |
| `pytz` | ~2026.1 | Timezone handling (CT for monitor windows) |
| `protobuf` | >=5.29.6,<7.0 | Transitive; pinned floor for CVE-2026-0994 |

## Dev Dependencies (requirements-dev.txt)

```
pre-commit>=3.7
ruff==0.11.10
```

## Demand-Feed Minimal Stack (requirements-demand-feed.txt)

`requests`, `python-dotenv`, `clickhouse-connect`, `anthropic~=0.97`, `slack-sdk~=3.41`, `pytz`

## Linting & Formatting

```toml
[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I"]  # Errors, undefined names, imports
ignore = ["E501"]
```

Pre-commit runs `ruff --fix` on every commit. No black, mypy, or flake8.

## Deployment (render.yaml)

```
ms-scout (worker)
  buildCommand: pip install -r requirements.txt
  startCommand: python scout_bot.py
  disk: scout-data (1GB → /opt/render/project/src/data)

ms-demand-feed (web)
  buildCommand: pip install -r requirements-demand-feed.txt
  startCommand: python demand_feed_main.py
  disk: demand-feed-data (1GB → /opt/render/project/src/data)
  port: 8080
```

## Config System

| Source | File | Editable Without Deploy? |
|--------|------|--------------------------|
| Base thresholds | `config/scout_thresholds.json` | No (git-tracked) |
| Runtime overrides | `data/threshold_overrides.json` | Yes (via `@Scout set_threshold`) |
| Static team corrections | `config/team_corrections.json` | No (currently empty) |
| Entity overrides | `data/entity_overrides.json` | Yes (auto-managed) |
| Env vars | Render dashboard | Yes (no redeploy) |

## Key Environment Variables

**Required (bot crashes without these):**
- `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`
- `ANTHROPIC_API_KEY`
- `CH_HOST`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE`

**Required for scraper:**
- `NOTION_TOKEN`, `NOTION_DB_ID`, `NOTION_QUEUE_DB_ID`
- Per-network: `IMPACT_SID/TOKEN`, `FLEXOFFERS_API_KEY`, `MAXBOUNTY_EMAIL/PASSWORD`, `CJ_API_KEY`, etc.

**Optional (graceful degradation):**
- `UPSTASH_REDIS_REST_URL/TOKEN` — alert persistence (falls back to in-memory)
- `LATITUDE_API_KEY/PROJECT_ID` — telemetry (silently disabled if absent)
- `CAMPAIGN_CREATE_WEBHOOK_URL` — MS platform integration (deferred)
- `SIDD_QA_CHANNEL_ID`, `SCOUT_SHADOW_CHANNEL`, `SCOUT_MONITOR_CHANNEL`

**Operational flags:**
- `SCOUT_ENV` (default: "development" → "production" in Render)
- `SCOUT_DISABLED_SOURCING_SIGNALS` — comma-separated signals to kill
- `SCOUT_HOURLY_SHADOW_ENABLED` (default: "true")
- `REVENUE_TRACKER_ENABLED` (default: "true")
- `DIGEST_SOURCE` ("local" or "demand_feed")
