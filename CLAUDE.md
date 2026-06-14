# Scout — Engineering Context

Extends `~/.claude/CLAUDE.md`. Scout-specific only.

## Quickstart

In order, no skipping:

1. `python3 smoke_test.py` — green or stop. "It worked last time" is not a baseline.
2. Read what you're touching:
   - `scout_system.md` — Claude's system prompt and reasoning scope
   - `scout_agent.py:5198` (TOOL_MAP) + `:5415` (runtime additions) — what routes where
   - `scout_handlers.py:115` (_FORCE_MONITOR_FNS) — registered monitor signals
   - `alert_registry.py` — dedup, kill switches, per-monitor schedules (monitors only)
3. `/mem-search scout` — check what was already decided before re-deciding it.

## Architecture

Two services. Don't cross the boundary.

| Service       | Entry point          | Owns                                                        |
|---------------|----------------------|-------------------------------------------------------------|
| `scout-bot`   | `scout_bot.py`       | Slack event dispatch, slash command routing, digest posting |
| `demand-feed` | `demand_feed_main.py`| Monitor polling loops, signal detection, alert posting      |

**Event flow:**
```
Slack event
  └─ scout_bot.py
       ├─ slash command ──────── scout_handlers.py
       │                              └─ force signal → _FORCE_MONITOR_FNS[name]()
       └─ @mention
            ├─ file/URL ──────── ask_with_attachment() → tool loop → ScoutKit → Slack
            └─ text only ─────── ask()               → tool loop → ScoutKit → Slack
```

**Prohibited patterns — these have caused bugs:**
- Never call ClickHouse directly from `scout_bot.py`. Route through `queries_*.py`.
- Never modify `ask()` at `:6132` for attachment handling. `ask_with_attachment()` at `:6202` is the attachment path. Structural guarantee (AC-9 — no regression on text-only @mentions).
- Never `shell=True` anywhere.
- Never call `web.chat_postMessage` with hand-built blocks. Always go through `wrap_response()` — pattern enforcement is in that call.

## Engineering Gates

Pre-conditions. Stop at the gate; don't proceed without passing.

**Before writing a new ClickHouse query:**
Define the output shape (column names, types, row grain) before writing SQL. If you can't name the shape, you don't understand the question yet.

**Before patching a bug:**
Classify the layer first:
- Scout query layer (`queries_*.py`, `scout_agent.py`) — bad reasoning or wrong tool selection
- ClickHouse data layer (`scout_ch.py`, `queries_*.py`) — wrong SQL, bad join, stale schema
- Feed/scraper layer (`offer_scraper.py`, `demand_feed_main.py`) — upstream data missing or malformed

Wrong-layer patches waste time. Name the layer before writing any code.

**Before adding a new monitor signal — touch exactly these 5:**
1. Signal function in `scout_bot.py` or `demand_feed_main.py`
2. `scout_handlers.py:_FORCE_MONITOR_FNS` — register for force-run + slash command
3. `alert_registry.py` — dedup key, kill switch, schedule
4. Slack app manifest at api.slack.com/apps — add the `/scout-*` slash command
5. `smoke_test.py` — regression guard

All 5 or don't ship. A signal that can't be force-run can't be debugged.

**Before adding new infrastructure (new module, service, dependency):**
Does the existing layer solve 80% of this? If yes, extend it. If no, name what it can't do before proposing something new.

**When the elegant solution hits a constraint:**
Ship the simpler path. Name the constraint inline. Don't block the fix waiting for elegance.

## Extension Points

| Adding...              | Touch these files                                                                    |
|------------------------|--------------------------------------------------------------------------------------|
| New Claude tool        | `scout_agent.py:TOOL_MAP` + implementation in `queries_*.py` + `smoke_test.py`     |
| New monitor signal     | Signal fn + `scout_handlers.py:_FORCE_MONITOR_FNS` + `alert_registry.py` + Slack manifest + `smoke_test.py` |
| New slash command      | Handler in `scout_handlers.py` + api.slack.com/apps manifest                        |
| New attachment format  | `scout_attachments.py:_EXTRACTORS` + extractor fn + `smoke_test.py`                |
| New Slack pattern      | `scout_ui_kit.py` + pattern table below                                             |
| New ClickHouse query   | New fn in `queries_*.py` — define output shape first                                |

## Verified = Done

All three, not one:
1. `python3 smoke_test.py` green
2. Slack confirmed — @mention in test channel, response received
3. One output count spot-checked against ClickHouse directly

Code that looks right is not verified.

## Product Principle

Scout never surfaces unverified data silently. If Scout shows a number, it was verified. If Scout cannot verify, Scout says so in the response.

Implementation: (a) `PayoutResult.state` catches enrichment failures at parse time; (b) failed Impact offers appear as a footer count in the digest; (c) `/scout-health` shows the data quality section alongside module sizes.

## Response Patterns

ScoutKit (`scout_ui_kit.py`) is the only place that builds Slack blocks. Match pattern → surface → severity before writing new handlers.

| Pattern   | Surface                           | Severity        | Buttons | When                   |
|-----------|-----------------------------------|-----------------|---------|------------------------|
| `ALERT`   | `MONITOR_ALARM`                   | WARN / CRITICAL | 0       | monitor alarm fires    |
| `ANSWER`  | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO            | ≤3      | ask() reply            |
| `STATUS`  | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO / WARN     | ≤3      | `@Scout status`        |
| `CONFIRM` | `EPHEMERAL`                       | POSITIVE        | 0       | action acknowledged    |
| `EMPTY`   | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO            | 0       | no data found          |
| `ERROR`   | `EPHEMERAL`                       | CRITICAL        | 0       | ClickHouse failure     |

Actionability rule: every ALERT and ANSWER must give the reader one next action. No number without context.

Pattern mismatch raises `ValueError` at call time — callers without `pattern=` are unaffected.

```python
from scout_ui_kit import Card, Severity, Surface, ResponsePattern, wrap_response

card = Card(severity=Severity.INFO, headline="Revenue MTD", body="$847K / $1M · 71%")
_, blocks = wrap_response(card=card, surface=Surface.CHANNEL_ROOT, pattern=ResponsePattern.ANSWER)
web.chat_postMessage(channel=channel, text="Revenue MTD", blocks=blocks)
```

## Attachment Ingestion

Dispatch table: `scout_attachments.py:178` (`_EXTRACTORS`) — single source of truth. `smoke_test.test_dispatch_table_routes_each_known_format` is the regression guard.

Adding a format: one extractor fn + one row in `_EXTRACTORS`. That's the full change.

Google Sheets URLs fetched anonymously via `export?format=csv` — sheet must be shared publicly. Slack `url_private` downloads gated on `https://files.slack.com/` prefix only.

Limits: 10MB per source, 30K char extracted, 5MB raw image bytes. Single source per @mention (file takes priority over URL).

Security: SSRF protection on Sheets fetch (host allowlist, 3-redirect max, private IP blocked). Never `shell=True` in extractors.

## Slash Commands

| Command                | What it does                                              |
|------------------------|-----------------------------------------------------------|
| `/scout-cap`           | Force-run cap signal now                                  |
| `/scout-vel`           | Force-run velocity signal now                             |
| `/scout-ghost`         | Force-run ghost (zero-conversion) signal now              |
| `/scout-fill`          | Force-run fill-rate signal now                            |
| `/scout-signal-status` | List signals currently firing (reads alert_registry)      |
| `/scout-revenue`       | Prompts user to ask `@Scout revenue`                      |
| `/scout-pub [pub]`     | Revenue health card for a publisher                       |
| `/scout-enter [adv]`   | Campaign entry card for MS platform                       |
| `/scout-queue`         | Current demand queue with Notion links                    |
| `/scout-status`        | System health + benchmark freshness                       |
| `/scout-help`          | Full reference card (ephemeral)                           |

All `/scout-cap/vel/ghost/fill` route through `scout_handlers.py:_FORCE_MONITOR_FNS`.
