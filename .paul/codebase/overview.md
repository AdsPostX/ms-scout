# Scout — Codebase Overview

Last mapped: 2026-06-13

## What Scout Is

Scout is a Slack-native AI analytics agent for MomentScience's operations team. It answers natural-language questions about campaigns, publishers, revenue, and offer performance by querying ClickHouse, then surfaces curated affiliate offers to the team via a daily digest with approve/reject buttons. Approved offers flow through a Notion queue into the live platform.

## Two Services, One System

| Service | Render Name | Entry Point | Purpose |
|---------|-------------|-------------|---------|
| Slack Bot | ms-scout (worker) | `scout_bot.py` | Socket Mode listener + all Slack interaction daemons |
| Offer Feed | ms-demand-feed (web) | `demand_feed_main.py` | Daily scraper + HTTP offer endpoint |

Both run on Render (Oregon, Starter plan), each with a 1GB persistent disk at `/opt/render/project/src/data`.

## The Core Pipeline

```
Affiliate Networks → offer_scraper.py → offers_latest.json → scout_digest.py
                                                            ↓
                                              Scout approves via Slack buttons
                                                            ↓
                                             scout_notion.py → Notion Queue
                                                            ↓
                                           MomentScience platform launches campaign
                                                            ↓
                                    scout_bot._performance_recap() tracks actuals vs Scout estimates
```

## @Scout Agent Loop

```
Slack @mention → scout_bot.py → scout_handlers.py → scout_agent.ask()
                                                           ↓
                                         Claude (tool use) → ClickHouse queries
                                                           ↓
                                         scout_ui_kit.wrap_response() → Block Kit
                                                           ↓
                                                    Slack response posted
```

## Key Numbers (as of 2026-06-13)

- **~18,500 lines** of core Python (excl. tests)
- **~18,800 lines** of tests
- **6,323 lines** in `scout_agent.py` (largest file — known debt)
- **37 test files** in `tests/`, plus `smoke_test.py` (3,972 lines)
- **~8 affiliate networks** scraped (Impact, FlexOffers, MaxBounty, CJ, ShareASale, Rakuten, AWIN, multiple TUNE/Everflow instances)
- **4 monitoring signals**: cap, velocity, ghost, fill rate
- **9 background daemons** running in scout_bot

## Current Phase

Phase 9 (audit-debt) — In Progress. 09-01 shipped. 09-02 TBD.
See `.paul/STATE.md` for full phase history.
