# Scout × Slack — Output Design System

**Date:** 2026-05-16
**Status:** Approved
**PRs:** #137 (PR-1 shipped), PR-2 through PR-4 pending

---

## Problem

Scout's Slack output has no shared layer. Every builder reinvents severity labels, timestamps, button styles, and layout. The result: monitor alarms look different from brief cards, agent responses look different from slash commands, and every new signal adds its own formatting quirks. There is no block budget enforcement — messages can silently exceed Slack's 50-block limit.

Specific confirmed issues (observed in #sidd-qa, May 2026):

- `:zap: Action: Run campaign status on X` text duplicates the suggestion buttons below it. The buttons are right; the text is noise.
- Advertiser trending (`@Scout how are advertisers trending`) dumps 25+ rows as mrkdwn bullets at channel root. No pagination, no thread split.
- Python-computed `18s ago`, `55m ago` timestamps everywhere — not timezone-aware, not clickable.
- Monitor alarms: `WARNING:` / `CRITICAL:` string labels mixed with emoji. No consistent vocabulary.
- Approve/Reject buttons are unstyled — no green/red visual signal for action weight.
- Agent responses go silent for 4–87s on slow queries. No thinking indicator.
- App Home is static — one mrkdwn section, no first-open onboarding, no live intel on return.

---

## Surfaces (confirmed active, May 2026)

| Surface | Current state | Block type |
|---|---|---|
| Channel posts — monitor alarms | 7 silent monitors (cap, velocity, ghost, fill, CVR, expiration, revenue-tracker) | section + actions |
| Thread replies — @mention | Agent answers, correctly threaded | section + actions |
| Ephemeral | /scout-pub, /scout-queue, /scout-enter, /scout-status | section |
| App Home | Queue card + health strip + examples | section (one big mrkdwn blob) |
| DM | "Try it →" from App Home opens DM with agent response | section |

Unused Slack features (available now, used nowhere):
- `reactions.add` — bot never adds its own reactions
- `views.open` — no modals anywhere
- `<!date^TS^...>` — zero timezone-aware timestamps
- `rich_text_list` — monitor alarms still use `• item\n• item` mrkdwn
- `header` block type — section titles use `*bold mrkdwn*` instead
- Button styles (`primary` / `danger`) — all buttons are unstyled

---

## Solution — Sequenced PRs

### PR-1: Delete dead Pulse renderer ✅ SHIPPED (#137)

611 lines deleted from `scout_slack_ui.py`. Baseline: ~1,226 lines. No behavioral change.

---

### PR-2: Minimal `scout_ui_kit.py` + migrate loudest two surfaces

**New file: `scout_ui_kit.py` (~180 lines)**

#### Severity enum (4 levels)

```python
class Severity(Enum):
    CRITICAL = ("🔴", "Revenue burning right now")
    WARN     = ("🟠", "Action needed within 24h")
    INFO     = ("🔵", "No action, FYI")
    POSITIVE = ("🟢", "Recovery / momentum")
```

Four, not six. WATCH vs WARN is not a distinction the team will reliably use.

CI lint blocks any `"WARNING:"` or `"CRITICAL:"` string literal in built blocks after this PR ships.

#### Card primitive

```python
Card(
    severity: Severity,
    headline: str,
    body: str,
    facts: list[tuple[str, str]],   # (label, value) pairs → section.fields
    actions: list[tuple[str, str, str]]  # (label, value, style) → actions row
)
```

`actions` is a list (plural) — supports multiple contextually-generated suggestion buttons in a single card (e.g., "Campaign status TurboTax", "Campaign status Cash App", "Lending Tree fallback"). Single-action cases pass `actions=[("label", value, "default")]`.

The `:zap: Action: Run X` text pattern is eliminated. Buttons ARE the action. Never duplicate action intent in both text and buttons.

`style` is `"default"` | `"primary"` | `"danger"`. PR-4 wires primary (affirmative) and danger (destructive) styling.

#### `ts()` helper

```python
def ts(unix_seconds: int, format: str = "{date_short_pretty} at {time}") -> str:
    return f"<!date^{unix_seconds}^{format}|{fallback}>"
```

Replaces all Python-computed `"18s ago"`, `"55m ago"` labels throughout Scout. Slack renders in the viewer's local timezone and formats on click.

#### Budget constants + `enforce()`

```python
BUDGETS = {
    Surface.CHANNEL_ROOT:   8,
    Surface.THREAD:         50,
    Surface.DM:             6,
    Surface.MONITOR_ALARM:  6,
    Surface.HOME:           30,
    Surface.EPHEMERAL:      6,
}
```

`enforce(blocks, surface)` hard-caps to the surface budget. Truncation always emits an expand path — either a "View full →" thread button or a context block noting "Showing N/M". Never a silent cut.

If no expand path can be built, falls back to a single "Results too large — narrow your query" Card rather than emitting a half-rendered message.

Every block-returning function in the kit takes a `surface` arg. The kit refuses to render if surface is unset — this forces every caller to declare intent, eliminating the current "channel-post-by-default" failure mode.

#### Surface enum (no router yet)

```python
class Surface(Enum):
    CHANNEL_ROOT  = "channel_root"
    THREAD        = "thread"
    DM            = "dm"
    MONITOR_ALARM = "monitor_alarm"
    HOME          = "home"
    EPHEMERAL     = "ephemeral"
```

Surface routing (deciding CHANNEL vs THREAD vs DM from the event context) is intentionally left to callers. The kit stays pure — no event handling, no I/O decisions. Routing moves to `scout_surface.py` in PR-4.

**Migrations in PR-2:**

- `_build_alert_block` → `Card(severity=…)`. Drop WARNING: / CRITICAL: string labels.
- Cap monitor → emits via Card (lowest-risk daemon, isolated).
- CI lint: fail on `WARNING:` / `CRITICAL:` literals in any block builder.

**Verification:**
- All smoke + unit tests green
- Force-trigger cap monitor in #scout-qa: severity emoji renders, no string label, block count ≤ 6
- `_build_alert_block` callers (brief card risk flags) render correctly

---

### PR-3: Answer + ResultTable + remaining 6 monitors + size adaptation

Ad-hoc agent queries are the noisiest surface — more frequent than monitor alarms, more variable in size, worst-formatted (long answers at channel root, no truncation, no pagination, SQL inlined).

#### Answer primitive

```python
Answer(
    question: str,          # The user's query (rendered as rich_text_quote)
    summary: str,           # Channel surface — ≤2 sentences
    facts: list[...],       # Optional Card.facts for structured data
    result_table: ResultTable | None,
    action: str = "View thread →"
)
```

Channel summary (≤8 blocks) + thread carries SQL via `rich_text_preformatted`, full results, citations, feedback buttons.

Multi-turn: Answer includes a `thread_continuation` mode that omits the opener after turn 1 — prevents 5-turn threads from accumulating 5 hero blocks.

#### ResultTable primitive

```python
ResultTable(rows, columns, max_rows=10)
```

`render(rows, surface)` picks rendering tier from row count + surface budget. Caller passes full result set; kit decides what to show where.

| Result size | Channel (≤8 blocks) | Thread | Overflow |
|---|---|---|---|
| 1–3 rows | Inline as Card.facts | — | — |
| 4–10 rows | summary + top 5 + "View full →" | Full table | — |
| 11–50 rows | summary + top 5 + "+N more · View in thread →" | Full table (max 50) | Footer: "Showing 50/N" |
| 51–200 rows | summary + top 5 + "N rows, see thread" | First 50 rows as ResultTable + CSV in `rich_text_preformatted` (max 2900 chars; if CSV exceeds limit, truncate to however many full rows fit and add "… N rows omitted") | — |
| 200+ rows | summary + "N rows is too large — narrow query or /scout-export" | skipped | Steer user to refine |
| 0 rows | "No matches for those filters in the last 7 days. Try a wider window." | — | Explicit empty state |
| Error | "Couldn't reach ClickHouse (10s). Ask again in a minute." | Full error + cached result if available | No stack traces |

#### Other PR-3 scope

- Migrate velocity, ghost, fill, CVR anomaly, expiration, revenue-tracker monitors → Card
- Add `ts()` everywhere Python computes time labels
- Wire all 4 slash commands (`/scout-pub`, `/scout-enter`, `/scout-queue`, `/scout-status`) to kit (≤6 blocks, ephemeral)
- Vague/unparseable queries → Conversation primitive (no severity dot, no card chrome — see PR-4)

**Verification:**
- 5 @mentions in #scout-qa: lookup / analysis / vague / error / slash
- Synthetic result-size matrix: 1, 5, 25, 100, 500, 0 rows, error — each within budget, overflow exercised
- Multi-turn: 4-reply thread doesn't render opener after turn 1
- All 7 monitors force-fire and render via kit
- `<!date\^` in test output for every time-bearing surface
- No channel surface exceeds 8 blocks for any synthetic input

---

### PR-4: Presence & personality pass

#### `scout_voice.py` (~60 lines)

```python
opener(signal_type: str) -> str
# "Here's what I'm seeing —" for analysis
# "Heads up —" for alarms
# "Done." for action confirmations

signoff(thread_url: str | None = None) -> str
# Returns "<thread_url|Full reasoning →>" when present, empty when not
# Never "Hope this helps!" or thumbs

thinking_ellipsis() -> str
# Returns "…" — posted as ephemeral pre-response when query exceeds 2s
```

Thinking indicator mechanics (replacing silent gap):
- Agent receives @mention → bot posts a placeholder message "…" immediately (reuses `_handle_suggestion` pattern)
- Separately: after posting the placeholder, `reactions.add` 👀 to the **user's original message** — visible acknowledgment that Scout saw the request
- On response complete: `chat.update` the placeholder with the full answer; `reactions.remove` 👀 from user's message
- The 👀 on the user's message is the "I saw this" signal. The placeholder is the "answer is coming" container. Both are already patterns users recognize — no new UX paradigm.

Voice rules (enforced via grep lint, not style guide):
- No `!` in built blocks (severity dots carry urgency)
- No "Hope this helps", "Got it!", "Sure thing", `:thumbsup:`, `:pray:`, `:rocket:`
- No em dash `—` in built blocks (loudest AI tell in Slack copy). Use period or comma.
- No en dash `–` except inside date ranges (`Jan 1 – Mar 14`)
- No `...` — use `…`
- No double-hyphen `--`
- Numbers without hedging when data is available. Hedge only when inferred, partial, or stale.
- One dry aside per response max, never on alarms or errors — style guide rule only, not CI-lintable (requires semantic judgment; enforce at code review)

Sample lines (anchors, not templates):
- Monitor alarm: `🟠 cap alert: Disney+ hit 92% of daily cap by 11am. Pacing says it tops out around 1pm.`
- Answer (observed): `WB Mason revenue is 38% below the 8wk Tuesday median. Top contributor: Disney+ ghosted yesterday (0 conv on 12k imps).`
- Empty state: `No campaigns matched those filters in the last 7 days. Try a wider window, or drop the publisher filter.`
- Error: `Couldn't reach ClickHouse (10s timeout). Ask again in a minute, or narrow the query.`
- Confirmation: `Queued. Notion row is up.`

#### `scout_surface.py` (~80 lines)

```python
def route(event: dict, signal_type: str) -> Surface:
    ...
```

Rules extracted from current `scout_handlers.py:1710-1727`:
- Monitor alarms → `CHANNEL_ROOT`
- @mention in thread → `THREAD`
- @mention in channel root → `THREAD` (forces depth, answer lives in thread not channel root)
- DM top-level message → `DM`
- Slash command response → `EPHEMERAL`
- Long agent reasoning → `THREAD` with channel summary

Not pure (depends on event shape) — lives outside `scout_ui_kit` per PR-2's purity rule.

Migrate `scout_handlers.py:_handle_agent_query` to call `scout_surface.route(event, "agent_answer")` instead of inline thread_ts math.

#### Conversation primitive (additive to kit)

```python
Conversation(message: str, facts: list | None = None, action: str | None = None)
```

Leaner than Card. No header, no severity dot, no divider. 2–3 blocks max. Conversational tone via `opener()`. Used for DM replies and ephemeral slash command responses.

`Conversation.empty(what, suggestion)` — every empty state names what's empty AND suggests next action:
- ❌ "No results." → ✅ "No campaigns matched those filters in the last 7 days. Try a wider window, or drop the publisher filter."

#### App Home — two states

**Detection:** `data/home_seen.json` — set of user IDs. One-line check in `app_home_opened` handler. First open for a user: add ID and show onboarding view. Subsequent opens: show returning view.

**First-open view (Block Kit only):**

```
[header] Meet Scout
[section] Scout knows your revenue pipeline. Ask anything in plain English — @Scout in any channel or thread.
[divider]
[section + button accessory] Watch publisher health | Try it →
[section + button accessory] Find revenue gaps | Try it →
[section + button accessory] Manage the offer queue | Try it →
[context] What Scout knows: ClickHouse · 4,660 offers · Notion queue · Impact · MaxBounty · CJ · FlexOffers
```

4 blocks. One "Try it →" per JTBD. That's it.

**Returning view (Block Kit only, pre-computed by daemons — zero ClickHouse on open):**

```
[header] Scout — {day, date}
[section] 🟢 $18,400 · 94% of 8wk Tuesday median
           Truist + Smart Wallet carrying the day
[divider]
[section.fields]  Active alerts: ✓ None today  |  Expiring soon: BofA — May 31
[section] Queue — 2 offers awaiting entry
[divider]
[context] 💡 Try: "which publishers dropped the most revenue this week?" · Rotates daily
```

6 blocks. `section.fields` renders 2-col on desktop; stacks vertically on mobile (expected Slack behavior — no fix available). Data pre-computed by existing daemons into `_HOME_CACHE`. Zero ClickHouse queries on tab open.

Discovery nudge rotates from a curated list — pulls a different query suggestion each day, grouped by JTBD (publisher health / revenue gaps / queue ops).

**PR-4 files modified:**
- NEW `scout_voice.py`
- NEW `scout_surface.py`
- `scout_ui_kit.py` — add Conversation primitive (~30 lines)
- `scout_slack_ui.py` — `_build_home_view` splits into `_build_home_first_open` + `_build_home_returning`
- `scout_handlers.py` — agent path uses `scout_surface.route()`; `app_home_opened` forks on `home_seen` state; `reactions.add` 👀 wired to placeholder flow; ephemeral "Done." acknowledgment after approve/reject
- `scout_state.py` — `_load_home_seen()` / `_mark_home_seen(user_id)` + `data/home_seen.json` registered as state file constant (per P2: only scout_state.py reads/writes data/)
- CI: lint for banned copy patterns

**Verification:**
- Force-trigger every monitor: output uses `opener()` + `signoff()`, no `!`, no em dash
- @mention in channel root → reply in new thread, not at channel root
- @mention in existing thread → reply stays in thread
- DM Scout → reply renders as Conversation (≤3 blocks)
- Open App Home as new test user → first-open view; reopen → returning view
- Grep lint passes: no banned copy in any builder

---

## Rollback Plan

| PR | Kill switch | Recovery |
|---|---|---|
| PR-1 | `git revert` — no callers existed | <2 min |
| PR-2 | `SCOUT_KIT_ENABLED=false` — flag wraps BOTH the call site (cap monitor emission) AND the renderer (`_build_alert_block` fallback). Old function stays in code for one release cycle. | <5 min |
| PR-3 | Same gate. Old per-monitor `_format_*_alert` functions stay for one release cycle. | <5 min |
| PR-4 | Three independent flags: `SCOUT_VOICE_ENABLED`, `SCOUT_SURFACE_ROUTER_ENABLED`, `SCOUT_HOME_ONBOARDING_ENABLED`. Each falls back independently. | <5 min per flag |

Cross-PR invariant: old code path stays in tree, behind env flag, for one release cycle. Cleanup PR ships after 7 days of clean Slack alarms.

---

## Deferred (not in this plan)

- Channel Canvas — separate plan, multi-PR, own state model
- Reactions as state — needs daemon, storage spec, audit trail
- Scheduled morning summary — this is Pulse under a new name. Pulse is retired.
- Modals for rejection (`views.open`) — offer queue track, Known Debt P5
- `/scout-enter` as modal — offer queue track, Known Debt P5
- Offer stale nudge daemon — offer queue track, Known Debt P5
- Channel bookmarks — set-once chrome, ship when convenient

---

## Kit Ownership Boundaries

| Callers own | Kit owns |
|---|---|
| What data to surface | How severity renders |
| What user should do next | Button styling from severity |
| Whether a fact is worth showing | Field layout, truncation |
| Which surface to post to | Budget enforcement on that surface |
| Trigger context | Final block count before send |
