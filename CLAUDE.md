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

## Feature Surface & Audit

**43 tools** across 6 domains. See [FEATURES.md](FEATURES.md) for the complete inventory, handler mapping, and status.
Audit Status: Complete (engineering audit) — see [VAMSEE_AUDIT.md](VAMSEE_AUDIT.md) for findings.

**To regenerate the feature map:** `python3 scripts/generate_feature_map.py` (auto-syncs TOOL_MAP changes).

## Branch Cleanup

Claude Code leaves `worktree-agent-*` and `claude/*` branches behind after every session — they don't get pruned on merge. Run `scripts/branch_cleanup.sh` after any PR merges to clear them before they pile up:

- `scripts/branch_cleanup.sh` — dry run, local branches only, prints what it would delete and what needs a human look
- `scripts/branch_cleanup.sh --remote` (or `--all`) — dry run, also scans remote-only branches on `origin`
- `scripts/branch_cleanup.sh --apply` — deletes local branches proven safe (already merged into main, or byte-identical diff against main)
- `scripts/branch_cleanup.sh --apply --remote` — same, plus deletes safe remote-only branches via `git push origin --delete`

Local-only runs print a note with the count of unscanned remote-only branches — the remote backlog is never silently skipped.

Anything with an open PR, a closed PR with a real unmerged diff, orphan/no-merge-base history, or a `backup/*` name is never auto-deleted — it prints under "needs human review" instead.

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
| `ALERT` | `MONITOR_ALARM` | WARN / CRITICAL | per budget | 0 | monitor alarm fires |
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

## Engineering Lenses
Before building or debugging anything, during every diff, and before closing any significant PR, read `~/.claude/coding.md` (Karpathy Lens + Surgical Changes Lens + Vamsee Lens).

## Pre-Commit Checklist (non-negotiable)

Before ANY commit on Scout:

1. **Run smoke tests** — always, no exceptions: `python3 smoke_test.py 2>&1 | tail -5; test ${PIPESTATUS[0]} -eq 0`. Check the exit code, not just the printed summary — `tail` alone masks a nonzero exit from `python3`. All deterministic checks must pass. If it fails, fix before committing. Post the result inline: `✅ N/N` or `🔴 N/N — [failing test name]`.
2. **For queries.py / queries_*.py changes** — verify SQL locally before committing (see SQL Hygiene below).
3. **For scout_agent.py handler changes** — import check: `python3 -c "from scout_agent import TOOL_MAP; print('OK')"`.
4. **Preview before PR** — for any web tool or demo, run a local server and screenshot via Claude Preview MCP before asking for review. No screenshot = no merge.

**Never commit debug patches.** If a commit message starts with `debug(`, it must NOT merge to main. Diagnose locally, fix, then commit the fix only.

## SQL Hygiene

Before any change to a `queries_*.py` WHERE clause, JOIN type, or column reference on `mv_adpx_users` or `from_airbyte_*`:

1. **Check the column type first** — via ClickHouse MCP, never assume. Ask: is this column Nullable? If yes AND it's a boolean/UInt8 flag column, use `(col = false OR col IS NULL)` — never `NOT col` or `col = false` alone. This pattern only applies to boolean/UInt8 flags — a Nullable String or numeric column isn't compared with `= false`; use `col IS NULL` (or `IS NOT NULL`) directly instead.
2. **Known traps (learned from prod failures):**
   - `mv_adpx_users.is_test` → `Nullable(UInt8)` → use `(is_test = false OR is_test IS NULL)`
   - `mv_adpx_users.organization` → `LowCardinality(String)` → `endsWith()` may throw — filter in Python instead
   - `adpx_conversionsdetails.pid` → NOT the publisher user_id — always filter on `user_id`
   - `revenue`, `payout` → String columns — always cast: `toFloat64OrNull(revenue)`
   - `from_airbyte_campaigns.id` → `Nullable(Int64)` → always add `AND c.id IS NOT NULL` in the WHERE clause before joining on campaign id (even with LEFT JOIN)
3. **When in doubt, filter in Python, not SQL.** SQL type errors silently kill queries in production; Python errors surface immediately in smoke tests.
4. **Document the column type in the commit message** when adding a new column reference.

## PR Discipline

**One PR = one concern.**

- New tool port → its own PR (separate from routing changes)
- Routing/intent change → its own PR
- Bug fix found while building a feature → separate commit, ideally separate PR
- Config change → its own PR (never buried in a feature PR)

**If the PR description contains "also" or "additionally" → split it.** If it breaks, do you want both things rolled back together? If no → split.

**Debug patches never merge to main.** Surface the error locally, fix it, commit the fix only.

## Worktree Hygiene

After every PR merges: `git worktree remove <path>` for that branch's worktree — it refuses safely if uncommitted changes remain, so never add `--force` to bypass that check without first inspecting what's uncommitted. Run `git worktree prune` afterward to clear stale metadata for worktrees already deleted on disk. Rule: **never leave more than 3 active worktrees** (main + current session + 1 parallel). If `git worktree list` grows beyond that, prune before starting new work — a stale worktree pile silently accumulates unmerged/unshipped diffs that become expensive to audit later.
