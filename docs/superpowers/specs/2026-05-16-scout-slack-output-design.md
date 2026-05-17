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

`enforce(blocks, surface, thread_ts=None)` hard-caps to the surface budget. Truncation always emits an expand path — either a "View full →" thread button (requires `thread_ts` to be provided) or a context block noting "Showing N/M". Never a silent cut.

If `thread_ts` is `None` (e.g. a fresh monitor alarm posting to channel root before the message exists), the expand path cannot be a thread link. In that case the kit falls back to a "Results too large — narrow your query" Card rather than emitting a half-rendered message. Callers on CHANNEL_ROOT pass `thread_ts=None`; callers responding to an existing thread pass the thread's ts. This is the canonical enforce() contract — every caller must pass `thread_ts` explicitly or accept the fallback.

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

**Implementation constraints for PR-2:**

**Import guard (REQUIRED — prevents total silence on syntax error in kit):**

```python
# scout_slack_ui.py top of file
try:
    from scout_ui_kit import Card, Severity, Surface, BUDGETS, enforce, ts
    _KIT_AVAILABLE = True
except Exception:
    _KIT_AVAILABLE = False
```

`_KIT_AVAILABLE` is the runtime gate. `SCOUT_KIT_ENABLED` env var is the operator gate. Both must be true to use kit paths. A syntax error in `scout_ui_kit.py` sets `_KIT_AVAILABLE = False`; Scout degrades to old paths instead of going fully silent.

**`_KIT_ENABLED` single source of truth:**

```python
# scout_ui_kit.py — exported at module level
import os
_KIT_ENABLED = os.getenv("SCOUT_KIT_ENABLED", "true").lower() == "true"
```

Both `scout_bot.py` (call site) and `scout_slack_ui.py` (renderer) import `_KIT_ENABLED` from `scout_ui_kit`. No duplicate env reads. Rollback is a single `SCOUT_KIT_ENABLED=false` on Render.

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
@dataclass
class Answer:
    question: str                       # rendered as rich_text_quote
    summary: str                        # channel surface — ≤2 sentences
    facts: list[tuple[str, str]] = field(default_factory=list)
    result_table: ResultTable | None = None
    action: str = "View thread →"
    thread_continuation: bool = False   # if True, omit opener (turn 2+)
```

`Answer` is a `@dataclass`. `ResultTable` is also a `@dataclass` (see below). Both render to blocks via `Answer.render(surface, thread_ts=None)` — render logic lives inside Answer, not on a separate `.render()` call chain. Caller interface: `Answer(...).render(Surface.CHANNEL_ROOT)`.

Channel summary (≤8 blocks) + thread carries SQL via `rich_text_preformatted`, full results, citations, feedback buttons.

`thread_continuation=True` omits the opener block — prevents 5-turn threads from accumulating 5 hero blocks. `scout_handlers.py` sets this when `thread_ts` already exists and the thread has prior Scout messages.

#### ResultTable primitive

```python
@dataclass
class ResultTable:
    rows: list[dict]
    columns: list[str]
    max_rows: int = 10
```

`ResultTable` is a `@dataclass`. No public `.render()` method — Answer calls its internal `_render_for_surface(surface, thread_ts)`. Caller passes full result set; kit decides what to show where. This keeps the API consistent: Answer, Card, Conversation are all dataclasses; none have public `.render()` — they're rendered via `Answer.render()`.

| Result size | Channel (≤8 blocks) | Thread | Overflow |
|---|---|---|---|
| 1–3 rows | Inline as Card.facts | — | — |
| 4–10 rows | summary + top 5 + "View full →" | Full table | — |
| 11–50 rows | summary + top 5 + "+N more · View in thread →" | Full table (max 50) | Footer: "Showing 50/N" |
| 51–200 rows | summary + top 5 + "N rows, see thread" | Thread: first 20 rows as section text + context block with "/scout-export to get the full CSV" (readable; avoids the rich_text_preformatted wall-of-text problem — raw CSV in a code block is unreadable at 50 rows) | — |
| 200+ rows | summary + "N rows is too large — narrow query or /scout-export" | skipped | Steer user to refine |
| 0 rows | "No matches for those filters in the last 7 days. Try a wider window." | — | Explicit empty state |
| Error | "Couldn't reach ClickHouse (10s). Ask again in a minute." | Full error + cached result if available | No stack traces |

#### Other PR-3 scope

- Migrate velocity, ghost, fill, CVR anomaly, expiration, revenue-tracker monitors → Card
- Add `ts()` everywhere Python computes time labels
- Wire all 4 slash commands (`/scout-pub`, `/scout-enter`, `/scout-queue`, `/scout-status`) to kit (≤6 blocks, ephemeral)
- Vague/unparseable queries → Conversation primitive (no severity dot, no card chrome — see PR-4)

**Slash command ack/async pattern (REQUIRED — Slack enforces 3s HTTP 200 deadline):**

```python
# scout_handlers.py — all slash command handlers
def handle_slash_pub(ack, body, client, logger):
    ack()                          # HTTP 200 immediately — MUST be first line
    channel_id = body["channel_id"]
    user_id = body["user_id"]
    # now do the work — ClickHouse queries, kit rendering, etc.
    result = _fetch_pub_data(...)
    blocks = Conversation(message=...).render(Surface.EPHEMERAL)
    client.chat_postEphemeral(channel=channel_id, user=user_id, blocks=blocks)
```

`ack()` is always the first call — before any I/O. The Bolt SDK routes all `/` commands through an `ack` callback parameter. Never do work before calling `ack()`. The same pattern applies to all block_action handlers with slow I/O. This is already the pattern for `_handle_approve` and `_handle_reject` — PR-3 extends it to the 4 slash commands.

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

thinking_ellipsis(query: str) -> str
# Returns voice-consistent initial placeholder text for the rotating status
# Wraps _pick_loading_message(query) from scout_state — ensures copy follows voice rules
# The rotating mechanic itself is handled by _rotating_status (already live in production)
```

Thinking indicator mechanics (augmenting existing `_rotating_status`):
- `_rotating_status` + `_pick_loading_message` already exist in `scout_state.py` and are wired at 3 call sites in `scout_handlers.py`. The rotating placeholder is live — Scout is NOT silent while thinking.
- PR-4 augments the existing pattern with two additions:
  1. After posting the placeholder: `reactions.add` 👀 to the **user's original message** — "I saw this" signal visible to the whole thread
  2. On response complete (or error): `reactions.remove` 👀 from user's message — cleans up the receipt
- The `reactions.remove` call MUST be in the same `finally` block as `stop_rotating()`. All 3 call sites already have `finally: stop_rotating()` — add `reactions.remove` there. A query error that skips `finally` leaves a permanent 👀 on messages that never got answered.
- `thinking_ellipsis(query)` in `scout_voice.py` is the voice-layer entry point: it calls `_pick_loading_message(query)` and applies voice lint before the text reaches `_rotating_status`. One call site per handler.

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
@dataclass
class Conversation:
    message: str
    facts: list[tuple[str, str]] | None = None
    action: str | None = None

    @classmethod
    def empty(cls, what: str, suggestion: str) -> "Conversation":
        return cls(message=f"{what} {suggestion}")
```

`Conversation` is a `@dataclass`. `Conversation.empty` is a `@classmethod` on the class — this works because it's a class, not a module-level function (which cannot have classmethods). Leaner than Card: no header, no severity dot, no divider. 2–3 blocks max. Conversational tone. Used for DM replies and ephemeral slash command responses.

Empty state contract: every empty state names what's empty AND suggests next action:
- ❌ "No results." → ✅ "No campaigns matched those filters in the last 7 days. Try a wider window, or drop the publisher filter."

Voice lint also rejects bare "No results" / "Nothing found" / "Empty" in any block builder.

#### App Home — two states

**Detection:** `data/home_seen.json` — set of user IDs. One-line check in `app_home_opened` handler. First open for a user: add ID and show onboarding view. Subsequent opens: show returning view.

**First-open view (Block Kit only):**

```
[header] Meet Scout                                                      block 1
[section] Scout knows your revenue pipeline. Ask…                        block 2
[divider]                                                                block 3
[section + button accessory] Watch publisher health | Try it →           block 4
[section + button accessory] Find revenue gaps | Try it →                block 5
[section + button accessory] Manage the offer queue | Try it →           block 6
[context] What Scout knows: ClickHouse · 4,660 offers · …               block 7
```

7 blocks total. One "Try it →" per JTBD.

**"Try it →" button behavior:** Each button has a distinct `action_id`:
- `home_try_publisher_health` → posts "which publishers need attention right now?" to Scout DM and opens that DM
- `home_try_revenue_gaps` → posts "which advertisers aren't running on publishers who'd convert them?" to DM
- `home_try_offer_queue` → posts "what's in the queue right now?" to DM

Handler: `_handle_home_try(action_id, user_id)` in `scout_handlers.py`. Opens/reuses DM channel via `conversations.open`, then posts the canned query there and runs it through the agent path. This is identical to the existing "Try it →" pattern on suggestion buttons — the DM is the answer container. No modal, no channel post.

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
- Rotating thinking indicator commentary — PR-4b, after PR-4 is stable (see below)

---

## Phase 2 — Thinking Indicator Commentary (deferred, post PR-4)

PR-4 ships the basic mechanic: 👀 reaction on user's message after 2s + placeholder message that gets updated with the final answer. That's the load-bearing piece.

The rotating commentary below requires a separate background timer thread that edits the placeholder every ~5s until the query lands. That's its own concurrency concern — ship it as a clean standalone PR after PR-4 is stable.

**Copy is approved and ready to implement:**

| Elapsed | Message |
|---|---|
| 2s | `Eyes on it. Querying ClickHouse…` |
| 5s | `Copy. In the weeds. 5s.` |
| 8s | `Running recon on the impressions table…` |
| 10s | `10s. This query has more joins than a CJ report.` |
| 13s | `Still here. Ghost campaigns take time to find. They're ghosts.` |
| 16s | `16s. The click_hash whitespace isn't going to trim itself.` |
| 20s | `Easy day. 20s. Big dataset.` |
| 25s | `25s. FlexOffers has slower payouts than this query has seconds.` |
| 30s | `30s in. Scout is not bailing.` |
| 35s | `35s. Don't tell Todd.` |
| 40s | `40s. Somewhere, Todd just refreshed his dashboard.` |
| 46s | `Easy day, Chief Panic Officer. Scout's got it.` |
| 52s | `52s. This is a CVR query. They're like that.` |
| 60s | `60s. Still running. No ghost campaigns were harmed in this query.` |
| 68s | `68s. MaxBounty publishers are faster than this. Barely.` |
| 75s | `75s. This is what peak fill-rate analysis looks like.` |
| 85s | `85s. Scout is not a ghost campaign. Results incoming.` |
| 95s+ | `95s. Unprecedented. Even by Todd standards.` |

**Implementation note:** Todd references are intentional and team-approved. They reference his "Chief Panic Officer" reputation (affectionate, worksafe). Keep them at the 35–46s band where levity helps the wait. Drop them if the tone ever stops landing.

---

## Kit Ownership Boundaries

| Callers own | Kit owns |
|---|---|
| What data to surface | How severity renders |
| What user should do next | Button styling from severity |
| Whether a fact is worth showing | Field layout, truncation |
| Which surface to post to | Budget enforcement on that surface |
| Trigger context | Final block count before send |
