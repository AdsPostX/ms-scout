# MomentScience Offer Intelligence — Scout

Scout scrapes affiliate offers from 9 networks nightly, scores them against real conversion data in ClickHouse, and surfaces revenue gaps and partner-ready offer recommendations directly in Slack. The team asks Scout questions in plain English — Scout pulls live offer data, compares it against active MS campaigns, and responds in the channel.

```
Scraper (9 networks) → offers_latest.json + Notion DB
                    ↓
             Scout Agent (Claude)
                    ↓
        Slack: @Scout queries · 8am Pulse · Weekly Digest
```

---

## Daily Flows

**8am Pulse** (weekdays, `#revenue-operations`)
- Revenue vs. prior week, publisher-level breakdowns
- Ghost campaigns: active campaigns with zero conversions in 24h
- Top performers and drops worth investigating

**Weekly Offer Digest** (Monday, `#scout-offers`)
- New offers across all 9 networks, deduped and scored
- Scored against CVR benchmarks from ClickHouse
- Ranked by expected revenue impact

**@Scout queries** (any channel Scout is in)
- `@Scout offers for [partner]` — returns ranked offer list for a specific publisher
- `@Scout status` — pipeline health check
- `@Scout ghost campaigns` — surfaces campaigns burning spend with no conversions
- `@Scout revenue opportunities` — uncaptured revenue gaps by publisher

---

## Networks Supported

| Network | Auth | Status |
|---|---|---|
| Impact | `IMPACT_SID` + `IMPACT_TOKEN` | ✅ Active |
| FlexOffers | `FLEXOFFERS_API_KEY` + `FLEXOFFERS_DOMAIN_ID` | ✅ Active |
| MaxBounty | `MAXBOUNTY_EMAIL` + `MAXBOUNTY_PASSWORD` | ✅ Active |
| Commission Junction (CJ) | `CJ_API_KEY` + `CJ_WEBSITE_ID` + `CJ_PUBLISHER_ID` | ✅ Active |
| ShareASale | `SHAREASALE_AFFILIATE_ID` + `SHAREASALE_API_TOKEN` + `SHAREASALE_API_SECRET` | ⚠️ Token needed |
| Rakuten | `RAKUTEN_PUBLISHER_ID` + `RAKUTEN_API_TOKEN` | ⚠️ Token needed |
| Awin | `AWIN_PUBLISHER_ID` + `AWIN_API_KEY` | ✅ Active |
| TUNE / HasOffers (AdAction, KashKick, others) | `TUNE_{NAME}_API_KEY` + `TUNE_{NAME}_BASE_URL` | ✅ Active (AdAction) |
| Everflow (GiddyUp, AccioAds, others) | `EVERFLOW_{NAME}_API_KEY` + `EVERFLOW_{NAME}_BASE_URL` | ⚠️ Keys needed |

Add new TUNE or Everflow instances by appending vars to `.env` — no code changes required.

---

## Setup

**Environment**

Copy `.env.example` (or grab from Render dashboard) and fill credentials:

```bash
cp .env.example .env
# Fill in network credentials — see Networks table above
```

Key non-network vars:
```
ANTHROPIC_API_KEY=        # Claude — Scout's brain
SLACK_BOT_TOKEN=          # xoxb-... bot token
SLACK_APP_TOKEN=          # xapp-... socket mode token
NOTION_TOKEN=             # Notion integration token
NOTION_DB_ID=             # Offer inventory database
NOTION_QUEUE_DB_ID=       # "Add to queue" staging database
CH_HOST=                  # ClickHouse Cloud host
CH_USER=                  # ClickHouse user
CH_PASSWORD=              # ClickHouse password
CH_DATABASE=default
```

**Local dev**

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run scraper once (all networks)
python3 offer_scraper.py

# Run scraper for one network
python3 offer_scraper.py --network awin --debug

# Start Scout bot (Slack socket mode)
python3 scout_bot.py

# Smoke test (6 checks)
python3 smoke_test.py
```

**Render deploy**

Configured in `render.yaml`. Push to `main` → Render auto-deploys. Environment variables live in the Render dashboard (never in the repo).

The scraper daemon runs inside the same Render service as the bot — no separate worker needed.

---

## Adding a New Affiliate Network

1. **Add credentials to `.env`** — follow the naming pattern for existing networks
2. **Write a scraper function** in `offer_scraper.py` — return a list of dicts with keys from `OFFER_FIELDS`
3. **Register it** — add an entry to `NETWORK_SCRAPERS` dict at the bottom of `offer_scraper.py`
4. **Test** — `python3 offer_scraper.py --network yournetwork --debug`

The normalized schema (`OFFER_FIELDS`) is the contract. Every network adapter maps its raw API response to these 23 fields. Fields you can't get from a network just stay empty — that's fine.

Key fields:
- `payout` — numeric, USD, per conversion
- `payout_type` — `CPA`, `CPS`, `CPL`, `RevShare`, `CPC`, `CPM`
- `tracking_url` — your affiliate link (required for Notion queue)
- `icon_url` / `hero_url` — creatives for offer briefs
- `cta` — the button copy (e.g., "Get 30% Off")
- `terms` — restrictions, caps, exclusions
- `os_targeting` / `platform_targeting` — Mobile / Desktop / All

---

## Architecture

```
offer_scraper.py        Scrapes 9 networks → normalizes → writes offers_latest.json + Notion DB
                        Runs on startup + every 6 hours via daemon thread

scout_agent.py          Claude-powered intelligence layer
                        Tools: offer search, ghost detection, ClickHouse queries, entity lookups
                        Called by scout_bot.py for @Scout messages

scout_bot.py            Slack socket mode bot + daemon orchestrator
                        Handles: @Scout mentions, slash commands, 8am Pulse cron, singleton guard

scout_digest.py         Weekly offer digest
                        Scores new offers against CVR benchmarks, deduplicates, ranks by impact
                        Posts to #scout-offers every Monday

context_harvester.py    Nightly Slack context extractor
                        Reads #revenue-operations + all internal clients-* channels
                        Builds ambient context so Scout knows what's happening with each partner

campaign_builder.py     MS Platform campaign automation (Playwright)
                        STATUS: PARKED — see file header for details
```

**Knowledge stores:**
- `data/offers_latest.json` — current offer snapshot (refreshed every 6h)
- `data/entity_overrides.json` — publisher/advertiser facts Scout has learned from the team
- `config/team_corrections.json` — static platform-wide facts (git-tracked)

**Scoring** (`scout_digest.py`):
Offers are scored 0–100 against three tiers of ClickHouse benchmarks:
1. Publisher-specific CVR for this advertiser category
2. Platform-wide CVR for the category
3. Global baseline CVR

Higher payout × higher predicted CVR = higher score = ranked higher in digest.
