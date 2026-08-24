# Scout — Architecture

The missing "why" doc: service topology, concurrency model, and the
response-pattern system that governs everything Scout posts to Slack. See
`DESIGN.md` for the full redesign rationale and sequencing this doc's content
is drawn from.

## Service topology

`ms-scout` (this repo) is a Slack bot: it listens for `@Scout` mentions,
slash commands, and block actions, and answers with Slack Block Kit messages.

`scout_core/` (`contracts.py`, `monitors.py`, `job_runs.py`) is a **small
shared package, not a ms-scout-only module** — it is used by both this repo
and a separate `ms-demand-feed` service. Its own docstrings say as much:
"Shared by ms-scout (`scout_bot.py`) and ms-demand-feed
(`demand_feed_main.py`)." This boundary is easy to miss because
`scout_core/` sits inside this repo's tree, but code here should never treat
it as private to `ms-scout` — a change to `scout_core/` is a change to both
services' contract, not a local refactor.

The pipeline `scout_core/contracts.py` encodes moves offers through named
stages: scraped → normalized → digest candidate → queue draft → campaign
request. Domain boundaries in the codebase (the `queries_*.py` split, the
offers/publishers/campaigns/revenue/monitoring grouping in `DESIGN.md`'s
target package layout) follow these stages rather than a generic
data/domain/UI tech-tier split.

## Concurrency model

Interactive `ask()` calls are gated by `_ASK_SEMAPHORE`, a `BoundedSemaphore(3)`
in `scout_handlers.py`. The acquire behavior is intentionally asymmetric:

- **Fresh requests** do a non-blocking acquire and raise `AskTimeout`
  immediately if the semaphore is already at cap (3 concurrent asks in
  flight). This keeps the bot responsive under load rather than queuing
  requests silently.
- **Retry paths** use a blocking acquire with a timeout
  (`blocking_acquire_timeout_s=_CFG.ask_timeout_s`) — a retry is expected to
  wait briefly for a slot rather than fail immediately, since the user has
  already been told once that Scout is busy.

Do not change this asymmetry when touching `_ASK_SEMAPHORE` call sites — it
is deliberate, not an oversight (see `.claude/rules/scout_handlers.md`).

`_run_pulse_action` bypasses both `_ASK_SEMAPHORE` and `_ask_with_timeout` on
purpose: pulse actions are fire-and-forget UI refreshes, not interactive
asks, so they don't compete for the same concurrency budget and don't need a
timeout guard.

`_handle_event_impl` is split out from `handle_event` for crash isolation.
The broad `except` in `handle_event` exists to keep one bad event from
killing the socket-mode listener, but it must never wrap the
`client.send_socket_mode_response(ack)` call — Slack's ack has to happen
unconditionally, even if everything after it fails.

## Response patterns

All Slack output goes through `ScoutKit` (`scout_ui_kit.py`). Every response
is meant to map to one of six patterns, each tied to a specific surface and
severity:

| Pattern | Surface | Severity | Max blocks | Buttons | When |
|---|---|---|---|---|---|
| `ALERT` | `MONITOR_ALARM` | WARN / CRITICAL | per budget | 0 | monitor alarm fires |
| `ANSWER` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO | per budget | ≤3 | ask() reply |
| `STATUS` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO / WARN | per budget | ≤3 | `@Scout status` |
| `CONFIRM` | `EPHEMERAL` | POSITIVE | per budget | 0 | action acknowledged |
| `EMPTY` | `CHANNEL_ROOT` / `THREAD` / `DM` | INFO | per budget | 0 | no data found |
| `ERROR` | `EPHEMERAL` | CRITICAL | per budget | 0 | ClickHouse failure |

`wrap_response(card=..., surface=..., pattern=...)` enforces the
surface/pattern pairing structurally: passing a mismatched combination (e.g.
`pattern=ResponsePattern.ALERT` with `surface=Surface.CHANNEL_ROOT`) raises
`ValueError` at call time. This is deliberate — it turns "ALERT only ever
posts to the monitor-alarm surface" from a convention a reviewer has to
remember into a check the code enforces itself.

`wrap_response()` has no `user_id` parameter. DM-vs-broadcast behavior is
controlled entirely by the `surface` argument (`Surface.DM` vs
`Surface.CHANNEL_ROOT`/`THREAD`) — `user_id`, where passed, is only used to
build a `_mention()` prefix string for channel-facing responses (a DM reply
already notifies the recipient, so it omits the mention). Retry-path
callers must pass `surface=` explicitly rather than trying to infer it from
`user_id`.

Every `ALERT` and `ANSWER` is expected to give the reader one next action —
no number without context.

## Known gap: raw block construction

Enforcement above is opt-in, not mandatory: `wrap_response()`'s own docstring
notes that callers which omit `pattern=` are unaffected by the check. In
practice, a large share of `scout_handlers.py`'s Slack-output call sites
build blocks directly (`blocks=[...]`) instead of going through
`wrap_response()` — independently verified at 13 raw sites vs. 10
`wrap_response()` calls in that file, i.e. roughly 56% of output sites bypass
the pattern check entirely. This means the six-pattern system above
describes the intended design, not the current state of every call site.

See `DESIGN.md`'s "Output/config: make policy structural" section for the
fix plan (making `pattern=` required, adding new modal/ephemeral pattern
variants, and routing the remaining raw sites through `wrap_response`) — this
doc names the gap so it's read honestly, not to re-solve it here.
