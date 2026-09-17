# Scout — Known Debt

Open debt only — resolved items move to CHANGELOG.md when fixed.

## _BoundedRateLimitRetryHandler duplicated in scout_bot.py and scout_handlers.py

Same 15-line class exists verbatim in both files (`scout_handlers.py` from PR #329, `scout_bot.py` from PR #330) — each module constructs its own `WebClient` and needed the same uncapped-retry-sleep fix. Not unified at the time because `scout_slack_safe.py` (the module both files already share for `guard_web_client`) is scoped to response-emission invariants, not HTTP retry behavior, and unifying via `scout_handlers.py` would have required editing a file that was part of the still-open #329.

Fix once both #329 and #330 have merged: extract `_BoundedRateLimitRetryHandler` into a new small module (e.g. `scout_slack_retry.py`) and point both `WebClient` construction sites at it. Do this as its own PR, not bundled with unrelated work.

## scout_bot.py — rate-limit retries can starve the socket-mode ack pool

`SocketModeClient` (scout_bot.py:2085) dispatches every incoming Slack event to a `ThreadPoolExecutor(max_workers=10)` (slack_sdk default, unconfigured). `handle_event` — including the ack — only runs once a worker picks the queued task up. The #330 fix bounds a single rate-limit retry sleep to ~10s/attempt (~33s worst case for 3 attempts), but that sleep still occupies a pool worker for its full duration. If enough concurrent `chat_postMessage` calls get rate-limited at once (plausible during a digest fan-out or alert burst — the same traffic spike that trips Slack's rate limiter is exactly when many workers are active), all 10 workers can be asleep simultaneously. Any event past the 10th queues in the executor unstarted — not slow to ack, un-acked — for up to the worst-case sleep duration. Verified via `inspect.getsource` on `slack_sdk.socket_mode.builtin.client.SocketModeClient` and `BaseSocketModeClient.process_message`/`run_message_listeners`; `send_socket_mode_response` itself goes over the websocket, not the rate-limited `WebClient`, so this isn't an ack-mechanism bug — it's pool-saturation via a shared retry-sleep resource.

Lowering the retry cap further is a bandaid — it shrinks the window, not the coupling. The structural fix: route `chat_postMessage`/`chat_update` calls made from event handlers through a dedicated executor separate from `message_workers`, so a stuck retry consumes a slot in a pool whose only job is slow Slack calls, never the pool responsible for acking new events — same shape as the existing `_retry_after_timeout` background-handoff pattern in `scout_handlers.py`.

Fix: introduce a small dedicated executor for outbound Slack calls in event-handler paths, migrate `scout_bot.py`'s `web.chat_postMessage`/`chat_update` call sites onto it, add a smoke test that simulates worker saturation. Left as debt rather than fixed inline because it's a concurrency-architecture change (new executor + call-site migration across the file), not a mechanical fix, and #329/#330 are still open — do it as its own PR once both merge.

## demand_feed_main.py — MS Platform campaign creation (removed, not just deferred)

The `/queue/draft`, `/queue/approve`, `/queue/reject`, `/campaigns/create` REST API and its
backing `_fire_campaign_creation()`/`_handle_queue_*` handlers were **deleted** from
`demand_feed_main.py` — they were fully built and tested against mocks but nothing in
`ms-scout` ever called them in production, and they were permanently blocked on
`CAMPAIGN_CREATE_WEBHOOK_URL`, a URL the platform team never provided. Keeping ~400 lines
of unreachable code + its own test file wasn't earning its keep.

**Rebuild contract, preserved for when the platform team actually delivers the webhook:**

| Env var | Description |
|---|---|
| `CAMPAIGN_CREATE_WEBHOOK_URL` | POST endpoint on MS Platform accepting a `CampaignRequest` JSON body. |
| `CAMPAIGN_CREATE_API_KEY` | Bearer token sent as `Authorization: Bearer <token>`. |
| `CAMPAIGN_CREATE_DRY_RUN` | Safe default `"true"` → log + return preview, no HTTP call. |

**Payload shape the webhook must accept** (was `_fire_campaign_creation`'s POST body):
```json
{
  "draft_id": "<uuid>",
  "offer": { "network": "...", "offer_id": "...", "advertiser": "...", "title": "...", "payout_num": 0 },
  "ai_copy": { "headline": "...", "description": "...", "cta_yes": "...", "cta_no": "..." },
  "approver": "sidd",
  "approved_at": "2026-05-24T14:00:00+00:00",
  "dry_run": false
}
```

**If the webhook ever materializes**: this needs re-implementing against the demand-feed's
current structure, not just re-enabling old code — decide at that point whether "Approve" in
Slack should call it directly, or whether it stays gated behind the existing Notion-queue
human-ticket flow (`scout_notion.py`) as an automation layered underneath it, not a replacement.

## App Home Scoreboard (scout_ui_kit.py)

`TODO(App-Home-3.4)` at line 1708: revenue EOD projection range (`revenue_eod_projection_low_cents` / `revenue_eod_projection_high_cents`) is rendered conditionally but the upstream `scoreboard_rollup()` does not yet populate these fields.

Status: blocked on `scoreboard_rollup()` returning the projection range. The render path is already wired — once the fields exist on the rollup object, no UI changes needed.

## scout_digest.py:362 — raw ClickHouse client bypasses timeout bounds

`get_active_ms_campaigns()` builds its own `clickhouse_connect.get_client()` instead of going through `scout_ch._get_ch_client()`, so it isn't covered by `_CH_CONNECT_TIMEOUT_S` / `_CH_SEND_RECEIVE_TIMEOUT_S`. The equivalent bypass in `offer_scraper.py` was fixed as part of PR #316 (ask-timeout-resource-contention) because it's reachable from the interactive `ask()` path via the `run_offer_scraper` tool; this one is only ever called from the scheduled digest pipeline, not through `_ASK_SEMAPHORE`, so a hang here can't outlast a live `ask()` call the way the offer_scraper one could.

Fix: swap to `from scout_ch import _get_ch_client` + `ch = _get_ch_client()`, same as `offer_scraper.py`. Left as debt rather than fixed inline because `scout_digest.py`'s dedup/scoring logic is sensitive (see `.claude/rules/scout_digest.md`) and this touches the same file for an unrelated reason — do it as its own small PR.

## MS_MATCH_MAPPING_DB_ID — human-curated matching overrides (setup required)

`scout_match_mapping.py`'s `fetch_match_mapping_table()` reads a Notion database of confirmed
`(network, advertiser) → MS campaign ID` overrides, used to permanently fix a fuzzy-match
mistake instead of re-guessing it on every scrape. It fails open (empty dict) until this is
set up — no behavior change until then.

**Setup:**
1. Create a Notion database with three properties:
   - `Network` (select) — e.g. `rakuten`, `awin`, `flexoffers`
   - `Advertiser Key` (rich_text) — the advertiser name as it appears in the offer (normalized
     the same way as fuzzy matching: lowercased, non-alphanumeric stripped, common suffixes
     — ` inc`/` llc`/` ltd`/` corp`/` com`/` us` — dropped)
   - `MS Campaign ID` (number) — the `from_airbyte_campaigns.id` this advertiser actually
     corresponds to
2. Set `NOTION_TOKEN` (already used elsewhere) and `MS_MATCH_MAPPING_DB_ID` (new) in Render.
3. Add a row every time a human confirms or corrects a "Needs Review" offer in the digest
   or Notion inventory board — this is the durable fix, not a one-time cleanup.

## `is_already_in_ms()` fuzzy match (scout_digest.py) is riskier than offer_scraper.py's — flagged, not fixed here

While auditing matching quality for `offer_scraper.py`'s `match_ms_status()` (see the
`ms_match_confidence`/"Needs Review" work above), found that `scout_digest.py` has its own,
completely independent matching function — `is_already_in_ms()` (line 474) — used only to
decide whether to skip an offer from the digest entirely (not rendered anywhere, not scored).
Its fuzzy path is "at least one meaningful word overlap" between advertiser name word sets
(`_name_words()`), which is **looser** than `offer_scraper.py`'s normalized-full-string match
— e.g. "American Express" and "American Airlines" would both match on the shared word
"american" and silently skip a real, unrelated advertiser from ever appearing in the digest.

Not fixed as part of this pass: `.claude/rules/scout_digest.md` already flags this file's
matching-adjacent logic ("payout type normalization here intentionally diverges from
offer_scraper.py — do not unify without a separate investigation") as needing its own look
before merging with anything else, and a skip-from-digest bug has a different risk profile
(an advertiser silently never gets offered, vs. offer_scraper.py's risk of a wrong label on
an offer a human is actively looking at) — worth its own investigation into how often this
false-positive shape actually fires in practice before deciding on a fix.
