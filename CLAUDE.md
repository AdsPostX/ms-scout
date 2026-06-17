@~/code/mosci/CLAUDE.md

# Scout — Specific Instructions

Scout never surfaces unverified data silently. If a query result can't be validated, the response must say so explicitly.

## Repository Map

```
scout_agent.py         — main agent, ask() boundary, tool dispatch
queries_revenue.py     — SQL: revenue, CVR, fill rate
queries_monitor.py     — SQL: ghost campaigns, publisher health
queries_campaign.py    — SQL: campaign analytics
queries_publisher.py   — SQL: publisher analytics
scout_ui_kit.py        — Slack Block Kit patterns
scout_handlers.py      — @Scout routing + handler dispatch
scout_attachments.py   — file + Google Sheets ingest
offer_scraper.py       — daily scrapes + payout parsing
demand_feed_main.py    — MS Platform feed (has 5 MS_PLATFORM_TODO before live)
smoke_test.py          — gate tests (run before every push)
```

Other files present but not primary entry points: `scout_bot.py`, `scout_ch.py`, `scout_digest.py`, `scout_images.py`, `scout_notion.py`, `scout_slack_safe.py`, `scout_state.py`, `scout_telemetry.py`, `scout_types.py`, `alert_registry.py`, `queries.py`.

## Scout Quickstart

**First session:**
1. `/mem-search scout` for prior context
2. Read `scout_agent.py` SYSTEM_PROMPT + TOOLS + TOOL_MAP — always before planning anything new
3. Run `python3 smoke_test.py` to confirm baseline
4. Check `KNOWN_DEBT.md` for open TODOs before touching `demand_feed_main.py` or `scout_ui_kit.py`

**Returning to a bug:**
1. Run `python3 smoke_test.py` — confirm the failing test before writing any fix
2. Read the relevant handler in `scout_handlers.py` and the query file it calls
3. Fix, re-run smoke_test, then commit

**Building a new capability:**
1. Read `scout_agent.py` SYSTEM_PROMPT + TOOLS + TOOL_MAP — required before any new tool is planned
2. Check the Response Patterns table below — new handlers must map to an existing pattern
3. Add the new extractor/handler, add a row to the dispatch table or TOOL_MAP, add a smoke test
4. Run `python3 smoke_test.py` before pushing

## Scout Response Patterns

Scout uses ScoutKit (`scout_ui_kit.py`) for all Slack output. Every response maps to one of six patterns. Match pattern → surface → severity before writing new handlers.

| Pattern | Surface | Severity | Max blocks | Buttons | When |
|---|---|---|---|---|---|
| `ALERT` | `MONITOR_ALARM` | WARN / CRITICAL | per budget | ≤2 | monitor alarm fires |
| `ANSWER` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO | per budget | ≤3 | ask() reply |
| `STATUS` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO / WARN | per budget | ≤3 | `@Scout status` |
| `CONFIRM` | `EPHEMERAL` | POSITIVE | per budget | 0 | action acknowledged |
| `EMPTY` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO | per budget | 0 | no data found |
| `ERROR` | `EPHEMERAL` | CRITICAL | per budget | 0 | ClickHouse failure |

**Actionability rule:** every ALERT and ANSWER must give the reader one next action. No number without context.

### Code example (ANSWER pattern)

```python
from scout_ui_kit import Card, Severity, Surface, ResponsePattern, wrap_response

card = Card(severity=Severity.INFO, headline="Revenue MTD", body="$847K / $1M · 71%")
_, blocks = wrap_response(card=card, surface=Surface.CHANNEL_ROOT, pattern=ResponsePattern.ANSWER)
web.chat_postMessage(channel=channel, text="Revenue MTD", blocks=blocks)
```

### Pattern enforcement

Passing `pattern=` raises `ValueError` at call time if the surface is wrong:

```python
# This raises ValueError — ALERT requires MONITOR_ALARM, not CHANNEL_ROOT
wrap_response(card=card, surface=Surface.CHANNEL_ROOT, pattern=ResponsePattern.ALERT)
```

Existing callers that don't pass `pattern=` are unaffected.

## Scout Attachment Ingestion

Scout extracts content from @mention attachments and Google Sheets URLs and passes it as per-turn context to `ask_with_attachment()`. Read-only — no write-back to external systems.

**Required Slack scope:** `files:read` — register at api.slack.com/apps → OAuth & Permissions → Bot Token Scopes. Reinstall the app after adding.

**Supported sources:**
- Google Sheets URLs in `@mention` text — fetched anonymously via `export?format=csv`. Sheet must be shared as **anyone with the link can view**. Auto-unwraps Slack's `<url|label>` formatting and handles `#gid=` fragments.
- File attachments via `event.files[]`, routed via the dispatch table in `scout_attachments.py` (`_EXTRACTORS`): PDF, Excel (.xlsx/.xls), Word (.docx), CSV, Images (.png/.jpg/.gif/.webp via Claude vision), Text (.txt/.md/.json/.log)
- Unsupported types degrade gracefully — Scout answers the text question and prepends a one-line note.

**Adding a new format:** implement an extractor in `scout_attachments.py` + add one row to `_EXTRACTORS`. `smoke_test.test_dispatch_table_routes_each_known_format` is the regression guard.

**Limits:** 10MB per file/sheet, 30K char extracted text, 5MB raw image bytes before base64. File takes priority over URL if both present in the same @mention.

**Security guards (`scout_attachments.py`):**
- SSRF protection on Sheets fetch — host allowlist (`docs.google.com`, `accounts.google.com`), max 3 redirect hops, private/loopback/link-local IP blocked via `_resolves_to_private_ip`
- pdftotext runs via `subprocess.run` with timeout, `tempfile.mkstemp`, no shell — never `shell=True`
- Slack `url_private` downloads gated on `https://files.slack.com/` prefix only

**Boundary:** `ask()` is NOT modified — `ask_with_attachment()` is a separate function. AC-9 ("no regression on text-only @mentions") is structurally guaranteed.

## Scout Slash Commands

Registered at api.slack.com/apps — each must be added to the Slack app manifest.

| Command | What it does |
|---|---|
| `/scout-cap` | Force-run cap signal now; results post in the channel |
| `/scout-vel` | Force-run velocity signal now |
| `/scout-ghost` | Force-run ghost (zero-conversion) signal now |
| `/scout-fill` | Force-run fill-rate signal now |
| `/scout-signal-status` | List signals currently firing (reads alert_registry) |
| `/scout-revenue` | Prompts user to ask `@Scout revenue` for full response |
| `/scout-pub [publisher]` | Revenue health card for a publisher |
| `/scout-enter [advertiser]` | Campaign entry card for the MS platform |
| `/scout-queue` | Current demand queue with Notion links |
| `/scout-status` | System health + benchmark freshness |
| `/scout-help` | Full reference card (ephemeral) |

All `/scout-cap/vel/ghost/fill` commands route to `_FORCE_MONITOR_FNS` — same path as `@Scout force <signal>`. Requires the demand-feed service running with monitors initialized.

## Block Action Dispatch

Interactive button clicks route through `_BLOCK_ACTION_DISPATCH` in `scout_handlers.py`. Adding a new button requires a new entry here — and a smoke test.

| Action ID | Handler | When triggered |
|---|---|---|
| `scout_acknowledge` | `_handle_acknowledge` | Acknowledge button on ALERT card |
| `scout_snooze_open` | `_handle_snooze_open` | Snooze button on ALERT card — opens modal |
| `scout_drill_publisher` | `_handle_drill_publisher` | Drill modal on publisher revenue card |
| `scout_approve` | `_handle_approve` | Approve action |
| `scout_reject` | `_handle_reject` | Reject action |
| `scout_brief_queue` | `_handle_brief_queue` | Brief queue action |
| `home_alert_drill` | `_dispatch_home_alert_drill` | App Home alert drill |
| `pulse_ghost_brief` | `_dispatch_pulse_static` | Ghost signal brief |
| `pulse_fill_rate_brief` | `_dispatch_pulse_static` | Fill rate brief |
| `pulse_top_opps` | `_dispatch_pulse_top_opps` | Top opportunities |
| `pulse_scout_offers` | `_dispatch_pulse_scout_offers` | Scout offers pulse |
| `pulse_dig_in` | `_dispatch_pulse_dig_in` | Dig in action |

## Engineering Gates

**Before adding a new monitor signal — touch exactly these 5:**
1. Signal function in `scout_bot.py` (default) or `demand_feed_main.py` (if demand-feed-native)
2. `scout_handlers.py:_FORCE_MONITOR_FNS` — register for force-run + slash command
3. `alert_registry.py` — dedup key, kill switch, schedule
4. Slack app manifest at api.slack.com/apps — add the `/scout-*` slash command
5. `smoke_test.py` — regression guard

All 5 or don't ship. A signal that can't be force-run can't be debugged.

**Before writing a new ClickHouse query:** Define the output shape (column names, types, row grain) before writing SQL. If you can't name the shape, you don't understand the question yet.

**Before patching a bug:** Classify the layer first — Scout query layer (`scout_agent.py`), ClickHouse data layer (`scout_ch.py`, SQL), or feed/scraper layer (`demand_feed_main.py`). Wrong-layer patches waste time.

**Before adding new infrastructure:** Does the existing layer solve 80% of this? If yes, extend it. If no, name what it can't do before proposing something new.

## Prohibited Patterns

These have caused bugs — never do:
- Never call ClickHouse directly from `scout_bot.py`. Route through `queries_*.py`.
- Never modify `ask()` at `:6132` for attachment handling. `ask_with_attachment()` at `:6202` is the attachment path. Structural guarantee (AC-9 — no regression on text-only @mentions).
- Never `shell=True` anywhere.
- Never call `web.chat_postMessage` with hand-built blocks. Always go through `wrap_response()`.

## Karpathy Lens (run before building or debugging anything)

Andrej Karpathy's engineering discipline applied to Scout. Run this before writing a handler, query, or fix — not after.

1. **Data bugs first** — before touching any handler or query, suspect the data. Run the raw ClickHouse query in the MCP console. Is the row grain right? Are there nulls, dupes, unexpected zeros? Most Scout bugs are data bugs, not code bugs.
2. **Inspect before abstracting** — don't write a new query function until you've run the raw SQL and read the actual rows. If you can't describe the output shape (column names, types, row grain) from memory, you're not ready to write code.
3. **Empirical before theoretical** — don't reason about whether a signal function returns the right data. Call it: `python3 -c "from scout_agent import _get_ch_client; from scout_bot import _pulse_signal_cap; print(_pulse_signal_cap(_get_ch_client()))"`. Look at what comes back.
4. **Babysit the first fire** — after any monitor or formatter change, watch the actual Slack card post in #bot-qa. Smoke test green ≠ card looks right. The card is the product.
5. **Smallest representation first** — if a zero appears where data should be, don't add error handling. Add a `print()` at the boundary and find out why it's zero. Instrumentation before abstraction.

If a Karpathy check reveals a data problem: fix the data layer, don't paper over it in the handler.

## Vamsee Lens (run before closing any significant PR)

Vamsee Gomatam (AdsPostX CTO) reviews significant backend work. Check before closing a PAUL APPLY phase:

1. **No invisible accumulators** — if a list is built up silently inside a loop, it must be returned as a value or passed as a collector, not captured as a closure side effect.
2. **No no-op handlers with side-channel capture** — if a TOOL_MAP function returns a placeholder string while real capture happens elsewhere in the loop, document the contract explicitly in the docstring or restructure.
3. **No repeated inline comprehensions** — if the same filter/transform pattern appears ≥2 times across handlers, extract to a named pure function (`_coerce_X(raw)`) before shipping.
4. **Config objects, not scattered env reads** — feature flags gathered into a named constant block or config dataclass at module top; not scattered inline `os.getenv()` calls throughout the code.
5. **Validate at construction** — dataclasses use `__post_init__`, flags validate at startup. If the flag is on but required config is missing, warn or raise.

If any check fails: fix inline (≤5 min) or name the constraint and file the debt explicitly.
