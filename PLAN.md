<!-- /autoplan restore point: /Users/siddharthshah/.gstack/projects/AdsPostX-ms-scout/claude-eloquent-wing-7f0dc3-autoplan-restore-20260509-221742.md -->

# Scout Restructuring Plan

**Date:** 2026-05-09
**Status:** DECISION READY

---

## The Problem in One Sentence

Scout has 185 commits, 71 of which are fixes (38% fix rate). The team doesn't trust it.
The revenue tracker is the first autonomous proactive signal — if it fires a false alarm in
#revenue-operations before the trust is rebuilt, Scout is dead.

---

## What Success Looks Like (30 days from go-live)

**Todd Bloch stops asking "@Scout why was revenue low yesterday?" because the 3pm alert answered it the day before.**

Measurable: zero "why was yesterday's revenue low" questions from Todd in the 30 days after go-live.
Falsifiable: if Todd is still asking reactively, the alert either isn't firing, isn't accurate, or isn't
actionable enough for him to trust it.

Everything else (line count, smoke tests passing, false alarm rate) is an output, not an outcome.
This single behavioral change — Todd stops asking reactively — is the outcome.

---

## Usage Validation (May 10, 2026)

Checked `data/usage_log.jsonl` (3 entries, all from Sidd) and Slack mention history.
Slack is the real signal. Results from #revenue-operations Apr 1 – May 10 (39 days):

**46 @Scout mentions from 5 distinct users:**
- **Todd Bloch** — advertiser economics (TurboTax CJ margin, Fluent payout, BofA launch checks, low-RPM campaigns). Independent power user. Uses @Scout as a ClickHouse terminal.
- **Sidd Shah** — ghost briefs, fill rate briefs, publisher analysis, follow-up questions
- **Ali Abdelfadeel** — publisher needs-attention analysis, offer detail lookups
- **Gordon Riordon** — Google Sheets → ClickHouse cross-reference
- **Jon Nolz** — contact research

**Today (May 10, 4:46pm CT):** Todd asked "@Scout yesterday was one of the lowest daily revenue we've earned this year. why?" and "@Scout why is QuinStreet/TuitionHero being reversed by $13K in May?" — the revenue tracker would have answered the first question proactively at 3pm yesterday. Todd had to ask reactively instead.

**The Apr 22–May 9 usage gap** (18 days, ~2 mentions) correlates with Scout being unreliable during PRs 22–24. The team went quiet because Scout stopped working, not because they stopped caring. Todd came back the day PR activity settled.

**Conclusion:** "Nobody uses @Scout" was wrong. @Scout is a real tool with real independent usage. SocketMode stays. `scout_agent.py` stays. The trust problem is proactive daemons posting uninvited, not the reactive layer.

## Target Architecture

Two separate services. Each does one thing. Each can fail independently.

```
ms-demand-feed     — Python cron, hits 9 networks every 6h, writes offers_latest.json
                       No Slack. No Claude. No dependencies. Just data.

ms-scout             — Two jobs only:
                       1. _revenue_tracker daemon: posts to #revenue-operations when revenue
                          is soft. 3pm CT weekdays. Once per day. Feature-flagged.
                       2. @Scout reactive agent: answers ClickHouse questions when asked.
                          No Pulse. No digest. No ghost daemon. No Notion pipeline.
                       ~500 lines total after PR 27.
```

---

## Move 1: Merge PR #62 now (today, 5 min)

PR #62 is the revenue tracker. It's tested, dark (`revenue_tracker_enabled: false`),
and ships zero behavior change. The kill switch means it cannot fire in #revenue-operations
until you explicitly flip it.

Holding it has no upside. Merging it:
- Gets it off the branch (commits ahead of main right now)
- Lets you validate at 3pm CT today by running `test_revenue_tracker.py`
- Keeps the code reviewable in isolation before the restructure changes everything

**Do this:** `gh pr merge 62 --merge`

---

## Move 2: Extract offer scraper (PR 26) — 1-2 days

`offer_scraper.py` is already 90% isolated. It has no Slack, no Claude, no ClickHouse.
It reads network APIs and writes `offers_latest.json`. It runs as a daemon in scout_bot.py
(`_run_scraper_daemon`) but doesn't need to be.

### What extraction looks like

1. New Render service: `ms-demand-feed`
2. Files to move:
   - `offer_scraper.py` (2,099 lines)
   - `run_scraper.sh`
   - Scraper-only slice of `requirements.txt` (requests, bs4, dotenv, schedule)
3. Output: write `offers_latest.json` to its own Render Disk (ms-demand-feed service only)
4. **`_run_scraper_daemon` STAYS in Scout during PR 26** — Render Disk volumes attach to one
   service only; Scout cannot read from ms-demand-feed's disk. Scout keeps writing its own
   `offers_latest.json` in parallel. Both services run the scraper until PR 27 removes it.
5. New Render cron job or background worker: runs every 6h, no web server needed
6. **Do NOT remove `_start_daemon(_run_scraper_daemon, ...)` from scout_bot.py in this PR** —
   that removal ships in PR 27 when Scout is stripped entirely and no longer needs the file

### Why this first

- Zero Claude dependency removed from Scout's startup path
- Scraper failures no longer restart the Scout process
- Clearest win, lowest risk, most separable
- Scraper is Scout's most reliable component — protect it by isolating it

---

## Pre-PR-27: Validate @Scout answers a Todd-style question correctly

Before committing to keep `scout_agent.py` (5,324 lines), verify it actually works.

Run these two questions against @Scout in #revenue-operations with a Sidd account (dev Scout):

1. `@Scout why was yesterday's revenue lower than typical?`
2. `@Scout what are the top fill rate issues today?`

**Pass criteria:**
- Returns a specific answer with actual numbers (not "I don't have data" or a tool error)
- Cites publisher names and dollar amounts, not generic advice
- Runs in <30s
- Does NOT output a pipe table (SYSTEM_PROMPT prohibits this)

**If @Scout passes:** proceed to PR 27 as planned. `scout_agent.py` stays intact.
**If @Scout fails (wrong format, no numbers, timeout, error):** diagnose first. The specific failure
determines whether this is a 30-min fix or a deeper problem. Do NOT delete the reactive layer
without knowing it works — the plan's "why this is safer" section is predicated on @Scout being
functional. Run validation BEFORE writing a single line of PR 27 deletion code.

---

## Move 3: Strip Scout to revenue monitor + reactive @Scout (PR 27) — 3-5 days

After the scraper is out, Scout has two jobs:
1. Run `_revenue_tracker` — the one proactive signal
2. Answer `@Scout` mentions — reactive analytics assistant (Sidd uses it for ad-hoc ClickHouse queries)

The trust deficit came from **proactive signals firing uninvited**, not from `@Scout` responding
when asked. Reactive tools have a different trust profile — the user controls when they fire.
Strip the proactive machinery. Keep the reactive layer.

### What stays (keep)

```
scout_bot.py            — stripped to: main(), _start_daemon(), _revenue_tracker(),
                          _format_revenue_alert(), health server, SocketMode event handler
                          (SocketMode stays — needed for @Scout mentions)
scout_agent.py          — KEEP ENTIRELY. SYSTEM_PROMPT, TOOLS, TOOL_MAP, all _query_*()
                          functions, _load_thresholds(). @Scout is the point of keeping this.
scout_handlers.py       — STRIPPED (not deleted). Keep: _handle_mention(), handle_event().
                          Remove: block action handlers for Pulse cards, home tab handler,
                          slash command handlers. ~200 lines instead of current ~800.
scout_slack_ui.py       — STRIPPED (not deleted). Keep: text formatters, _text_to_blocks(),
                          helpers @Scout uses for response formatting, AND the 4 canonical
                          Block Kit primitives (_build_signal_header, _build_item_card,
                          _build_action_row, _build_publisher_card) — these are general-purpose
                          and reusable across future MomentScience bots (see ms-slack-kit note).
                          Remove: _format_pulse_blocks(), _build_queue_card(), _build_home_view(),
                          _queue_confirm_blocks(). ~200 lines remaining.
scout_state.py          — Keep _load_revenue_alert_state(), _save_revenue_alert_date(),
                          and any state functions @Scout tools read.
queries.py              — KEEP. revenue_opportunities() is still called by get_top_revenue_opportunities()
                          agent tool. Remove only Pulse-only helpers with no agent callers.
config/scout_thresholds.json — Keep as-is. All sections remain valid.
requirements.txt        — Keep as-is. anthropic + slack_bolt/socket_mode stay (needed for @Scout).
```

### What goes (delete entirely)

```
scout_notion.py         — DELETED. No queue pipeline UI.
scout_digest.py         — DELETED. No daily digest.
context_harvester.py    — DELETED. No nightly Slack context harvest.
campaign_builder.py     — DELETED. (Was already parked.)
```

### Daemons that go

```
_check_stale_queue      — gone (Notion queue gone)
_performance_recap      — gone (digest gone)
_cleanup_state          — gone (state files simplified)
_proactive_pulse        — gone (Pulse gone — this is the trust-breaker)
_nightly_harvest        — gone (context harvester gone)
_run_scraper_daemon     — gone (moved to ms-demand-feed in Move 2)
_notion_watcher_loop    — gone (Notion gone)
_copy_coalescer_loop    — gone (AI copy pipeline gone)
_run_health_heartbeat   — KEEP (Render health probe still needed)
_benchmarks_warmer      — KEEP (benchmarks warm @Scout's get_scout_status() responses)
_revenue_tracker        — KEEP (the one proactive signal)
```

### What Scout looks like after (~500 lines total)

```python
# scout_bot.py
def main():
    ch = _get_ch_client()
    web = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    socket_client = SocketModeClient(app_token=os.environ["SLACK_APP_TOKEN"])
    _start_daemon(_revenue_tracker, name="revenue-tracker", args=(web, ch))
    _start_daemon(_run_health_heartbeat, name="health-heartbeat", args=(web,))
    _start_daemon(_benchmarks_warmer, name="benchmarks-warmer", args=(ch,))
    _start_health_server()
    socket_client.socket_mode_request_listeners.append(handle_event)
    socket_client.connect()
    threading.Event().wait()
```

Three daemons + HTTP health endpoint + SocketMode for @Scout. No Pulse. No digest.
No Notion. No home tab. No block action routing. No slash commands.

### smoke_test.py after PR 27 (named survivors)

Strip the 14 PR-numbered tests and Pulse/Notion/digest-specific tests. Surviving tests:

```
test_clickhouse_connection_available         — CH client connects without error
test_agent_tools_all_registered             — all TOOLS entries have TOOL_MAP counterparts
test_revenue_tracker_daemon_function_exists — _revenue_tracker() exists in scout_bot
test_intraday_revenue_total_query_exists    — _query_intraday_revenue_total() in scout_agent
test_intraday_revenue_by_publisher_query_exists — _query_intraday_revenue_by_publisher() in scout_agent
test_revenue_alert_state_load_save          — _load_revenue_alert_state + _save_revenue_alert_date roundtrip
test_thresholds_load_correctly              — _load_thresholds() returns all expected signal keys
test_benchmarks_warmer_daemon_registered    — _benchmarks_warmer in scout_bot
test_revenue_opportunities_query_exists     — revenue_opportunities() in queries.py (agent tool target)
```

Run `python3 smoke_test.py` → expect "PASSED 9/9". Paste output in PR description.

### ms-slack-kit — Block Kit primitives are reusable

`scout_slack_ui.py` after PR 27 contains 4 general-purpose Block Kit primitives with no
Scout-specific logic:

```python
# ms-slack-kit — MomentScience canonical Block Kit primitives
# These are Scout-independent. When a second MomentScience bot (beverly, WiWo ops bot,
# demand-feed alerter) needs Block Kit formatting, extract these to a shared package.
_build_signal_header(emoji, title, context="")
_build_item_card(name, left_body, right_body="", context="", action_button=None)
_build_action_row(buttons)
_build_publisher_card(name, delta_pct, ...)
```

Plus `_text_to_blocks(text)` and `_is_help_query(query)` — also Scout-independent.

**Extraction trigger**: when a second MomentScience bot is built. Don't extract prematurely.
Add the `# ms-slack-kit` comment block at the top of the surviving functions in PR 27 so
the boundary is clear when extraction time comes.

### ms-demand-feed intelligence boundary

ms-demand-feed is a pure collector: scrape → `offers_latest.json`. No ClickHouse. No Claude.

All intelligence stays in Scout:
- `revenue_opportunities()` in `queries.py` — fuzzy anti-join between offer catalog and live
  campaign performance. The offer ID ↔ campaign ID mapping, category benchmarks, and network
  performance comparisons all live here.
- When ms-demand-feed eventually needs to enrich offers with ClickHouse data (e.g., "which
  verticals perform best on which network"), that enrichment runs as a Scout tool call,
  not as logic in ms-demand-feed itself.

### Why this is safer than the original "strip everything" plan

The original plan removed @Scout to eliminate complexity. Usage data shows that was wrong:
- 46 mentions from 5 users in 39 days — Todd Bloch uses it daily as a ClickHouse terminal
- @Scout responds to user intent — it can't fire a false alarm uninvited
- The team's trust was broken by daemons posting uninvited (Pulse, ghost detection, digest)
- Reactive tools have a fundamentally different trust profile — the user controls when they fire
- Keeping @Scout preserves the team's ClickHouse interface with zero trust risk

The deletion count drops from ~13,000 lines to ~8,000 lines. Lower risk, same trust outcome.

---

---

## Move 4: Validate and flip the kill switch (after Move 3 ships)

1. Run `python test_revenue_tracker.py` each weekday at ~3pm CT for 5 days — kill switch stays off, output goes to `#bot-qa`
2. After each run, copy the `#bot-qa` message into `#revenue-operations` with a note: "preview of the revenue alert Scout will start sending automatically — does this look right?"
3. Let Todd and Ali react in the channel. Don't curate or pre-filter. Natural reaction is the signal.
4. Ali signs off on all 5 — not Sidd self-certifying. "Looks good" in the thread counts.
5. If all 5 confirmed: flip `revenue_tracker_enabled: true` and redeploy
6. If any fires wrong: diagnose before continuing. Do NOT skip a bad run.

**Kill on first false alarm in #revenue-operations, no exceptions.** Set `revenue_tracker_enabled: false`, redeploy, diagnose. The credibility cost of one false alarm in #revenue-operations is higher than the cost of a delayed go-live.

**Do not flip until Ali has signed off on 5 clean runs.** Self-certification is how the last 3 regressions shipped.

---

## What Does NOT Come Back

The following come back only if: (a) revenue tracker runs 30 days without a noise complaint,
AND (b) someone on the team explicitly says "I need X because Y."

```
Ghost campaign detection    — first repeated false-alarm source; NOT the same as @Scout's
                              get_ghost_campaigns() tool (that stays — user-initiated only)
Pulse morning digest        — nobody asked for it during the dark period
Fill rate / cap alerts      — revenue tracker's publisher breakdown covers the meaningful cases;
                              @Scout fill rate query still works on demand
Notion queue UI             — only if ops team explicitly requests it
```

Note: `@Scout` reactive agent is NOT in this list — it stays in PR 27. The separation
is clear: proactive signals (daemons that post uninvited) were the trust problem.
Reactive tools (respond when asked) were never the trust problem.

---

## Sequencing Summary

| Step | What | Risk | Timeline |
|---|---|---|---|
| **Now** | Merge PR #62 | None (dark) | 5 min |
| **Pre-PR-27** | Validate @Scout answers a Todd-style revenue question correctly | None (read-only) | 30 min |
| **PR 26** | Extract offer scraper to ms-demand-feed (scraper STAYS in Scout until PR 27) | Low | 1-2 days |
| **PR 27** | Strip proactive signals only; keep @Scout reactive agent + revenue tracker | Medium (deleting ~8,000 lines) | 3-5 days |
| **Validate** | 5 clean 3pm runs with flag off; Ali signs off on all 5 | None (passive) | 1 week |
| **Go live** | Flip `revenue_tracker_enabled: true` | Low (feature-flagged) | 5 min |
| **PR 28** | Ali tuning buttons (Acknowledged / Too sensitive / Run breakdown) — only if Ali reports threshold needs adjustment during the 5-run validation | Low | 1-2 days |
| **PR 62b** | Bidirectional 🟢 upside alerts — after 30 days of clean 🔴 runs | Low | 1 day |

---

## The Discipline Rule (permanent, from this point forward)

Before any capability comes back:

1. **Validate before feature.** Revenue monitor must have run 30 days clean (Ali-confirmed, not self-reported).
2. **Answer the JTBD question first.** "What does the team do differently because this exists?"
   Vague answer = don't build it. If you can't name who on the team does what differently, stop.
3. **One PR, one capability.** No compound PRs. Each signal gets its own PR, its own smoke
   test run pasted in the description.
4. **The test is the alert firing correctly.** Not "the code looks right." Run it at 3pm CT.
   Screenshot the Slack message. Paste it in the PR.

**Forcing function (what makes this stick):** Before writing a single line of new proactive signal code, Ali Abdelfadeel must explicitly say "yes, I would act on this within 24 hours" in `#revenue-operations`. Not "sounds useful." Not a thumbs-up. A named person committing to an action. Silence in 48h = feature does not exist. The default is no. The burden is on the feature to earn a yes, not on the team to veto it.

---

## Decisions — Resolved

1. **Merge PR #62 now?** → **YES.** Dark, tested, no behavior change. 5 min.
2. **Scraper first, then strip?** → **YES.** Scraper first (PR 26), then strip (PR 27).
3. **Rename repo after PR 27?** → **NO.** Keeping `ms-scout`. @Scout stays as a reactive agent
   — the name still describes what the service does. `ms-revenue-monitor` was the right name for
   the "strip everything" plan. With @Scout intact, Scout is still Scout.
4. **Ali tuning buttons (Move 3.5)?** → **PR 28, after 5 clean runs.** Don't entangle the strip
   with new UI. Build the buttons only if validation confirms the threshold needs tuning.
5. **Render disk gap** → **RESOLVED in plan.** Scraper stays in Scout during PR 26. Removed in PR 27.
6. **PR 27 deletion/strip approach** → Three passes, in this exact order:

   **Pass 0 — strip Notion dependencies from callers BEFORE deleting the file** (REQUIRED):
   `scout_handlers.py` imports `scout_notion` at lines 27-30 (10+ symbols including
   `_write_to_notion_queue`, `_generate_offer_copy`, `_fetch_notion_queue_items`, etc.).
   `_handle_approve()` calls `_write_to_notion_queue()` at line 445. Deleting `scout_notion.py`
   without removing these imports first causes an `ImportError` at startup — Scout won't boot.
   Do this first, as a standalone commit:
   - Remove the `scout_notion` import block from `scout_handlers.py` (lines 27-30)
   - Strip the Notion call from `_handle_approve()` — approve/reject @Scout opportunity responses
     become text-only (no Notion queue destination). Remove approve/reject buttons from
     `_build_opportunity_cards()` and `_build_brief_blocks()` in `scout_slack_ui.py`.
   - Strip `_build_queue_card()` and `_build_home_view()` import from `scout_handlers.py` (line 33-38)
   - Run `python smoke_test.py` and confirm zero import errors before continuing.

   **Pass 1 — full deletes** (modules now with no remaining callers):
   `scout_notion.py` → `scout_digest.py` → `context_harvester.py` → `campaign_builder.py`.
   Run `python smoke_test.py` after each. With Pass 0 complete, these are clean deletes.

   **Pass 2 — strips** (modules that stay but are gutted):
   `scout_bot.py` (remove Pulse runner, 8am scheduler, all but 3 daemon launches),
   `scout_handlers.py` (remove block action router, home tab handler, slash commands — keep `_handle_mention`),
   `scout_slack_ui.py` (remove `_format_pulse_blocks`, `_build_queue_card`, `_build_home_view`,
   `_queue_confirm_blocks` — keep all canonical primitives + @Scout response formatters).
   Run `python smoke_test.py` after each strip. Squash all three passes as one clean PR.
7. **PR 27 SocketMode decision** → SocketMode STAYS. Usage data confirmed: 46 mentions from 5 users
   in 39 days. Todd Bloch uses @Scout as a ClickHouse terminal for advertiser economics. Ali and
   Sidd use it for signal analysis. TODAY Todd asked "why was yesterday the lowest revenue day this
   year?" — that's the reactive JTBD in action. The original "remove SocketMode" direction was
   predicated on removing @Scout entirely. That premise was wrong. @Scout is kept.
   scout_bot.py keeps its SocketMode import, SocketModeClient init, and `socket_client.connect()`
   in main(). What changes: remove Pulse runner, remove 8am scheduling logic, remove all daemon
   launches except revenue_tracker, health_heartbeat, benchmarks_warmer.
8. **State file persistence** → `data/` is already on Render persistent disk (`scout-data`).
   `pulse_state.json` (containing `last_revenue_alert_date`) survives deploys. No action needed.
9. **Bidirectional alerts (Move 1.5)** → **DEFERRED to PR 62b**, after 30-day validation.
   The JTBD is "warn when revenue is soft." A 🟢 upside alert is a morale feature, not the
   JTBD. Building it now would consume a PR during the trust rebuild for a nice-to-have.
   Revisit only after 30 days of clean 🔴 runs have proven the signal is trustworthy.
   PR 62b: add `revenue_tracker_upside_threshold_pct: 120` and `direction: "soft" | "strong"`
   return field to `_query_intraday_revenue_total()` — exact spec stays in PR 62b planning.
