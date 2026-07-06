# Scout — Known Debt

## demand_feed_main.py — MS Platform Feed (NOT live)

5 MS_PLATFORM_TODO items must be resolved before flipping live. Contact the platform team for the webhook endpoint.

**Required env vars (set in Render):**

| Env var | Description |
|---|---|
| `CAMPAIGN_CREATE_WEBHOOK_URL` | POST endpoint on MS Platform accepting a `CampaignRequest` JSON body. Leave unset → dry_run mode (safe default). |
| `CAMPAIGN_CREATE_API_KEY` | Bearer token sent as `Authorization: Bearer <token>`. Leave unset → no auth header (dev/local only). |
| `CAMPAIGN_CREATE_DRY_RUN` | `"true"` (default) → log + return preview, no HTTP call. Set `"false"` AND set WEBHOOK_URL to go live. |

**Flip-live checklist (in order):**
- [ ] Get `CAMPAIGN_CREATE_WEBHOOK_URL` from platform team — confirm POST shape matches `_fire_campaign_creation` payload (`draft_id`, `offer`, `ai_copy`, `approver`, `approved_at`, `dry_run`)
- [ ] Set `CAMPAIGN_CREATE_WEBHOOK_URL` in Render
- [ ] Set `CAMPAIGN_CREATE_API_KEY` in Render (if platform requires auth)
- [ ] Keep `CAMPAIGN_CREATE_DRY_RUN=true` — test one approve, inspect `would_send` in the dry_run response
- [ ] Confirm `GET /queue/config` shows `mode: dry_run`, `webhook_url_set: true`
- [ ] Set `CAMPAIGN_CREATE_DRY_RUN=false` → live. `/queue/config` should show `mode: live`.

**Where the TODOs live in the file:**
- Line 623: startup log warns about missing env vars
- Lines 1217–1247: full env var spec + flip-live instructions block
- Line 1293: `_fire_campaign_creation()` dry_run return — review `would_send` before flipping
- Line 1375: `/queue/config` handler — mode should read `"live"` before launch

## App Home Scoreboard (scout_ui_kit.py)

`TODO(App-Home-3.4)` at line 1708: revenue EOD projection range (`revenue_eod_projection_low_cents` / `revenue_eod_projection_high_cents`) is rendered conditionally but the upstream `scoreboard_rollup()` does not yet populate these fields.

Status: blocked on `scoreboard_rollup()` returning the projection range. The render path is already wired — once the fields exist on the rollup object, no UI changes needed.

## scout_digest.py:362 — raw ClickHouse client bypasses timeout bounds

`get_active_ms_campaigns()` builds its own `clickhouse_connect.get_client()` instead of going through `scout_ch._get_ch_client()`, so it isn't covered by `_CH_CONNECT_TIMEOUT_S` / `_CH_SEND_RECEIVE_TIMEOUT_S`. The equivalent bypass in `offer_scraper.py` was fixed as part of PR #316 (ask-timeout-resource-contention) because it's reachable from the interactive `ask()` path via the `run_offer_scraper` tool; this one is only ever called from the scheduled digest pipeline, not through `_ASK_SEMAPHORE`, so a hang here can't outlast a live `ask()` call the way the offer_scraper one could.

Fix: swap to `from scout_ch import _get_ch_client` + `ch = _get_ch_client()`, same as `offer_scraper.py`. Left as debt rather than fixed inline because `scout_digest.py`'s dedup/scoring logic is sensitive (see `.claude/rules/scout_digest.md`) and this touches the same file for an unrelated reason — do it as its own small PR.

## config/scout_thresholds.json — `native_cards_enabled` dark-launched, never flipped on

PR #323 (`carousel/digest-native-cards`) shipped native Slack card/carousel rendering behind `digest.native_cards_enabled`, explicitly set to `false` by design ("classic rendering is untouched until explicitly flipped on"). No follow-up task was ever filed to turn it on — no env var exists either (`grep -rn "NATIVE_CARDS"` is empty), so the only toggle is this JSON value.

`digest.offers_per_network` is also still `3`, not the intended `10` — same config block, same fix window.

Fix: flip both values in `config/scout_thresholds.json` and watch the next live digest run before calling it done. Left as debt rather than fixed inline because flipping user-facing rendering behavior warrants its own small, watched PR — not bundled with an unrelated change.

## scout_bot.py:1661,1781 — raw `anthropic.Anthropic()` bypasses the scout_agent.py singleton

Both call sites do `import anthropic as _anthropic; _anth_client = _anthropic.Anthropic()` inline instead of going through `scout_agent._get_anthropic_client()`. This PR (Anthropic-client-construction race fix) only hardened the singleton in `scout_agent.py` — these two are a separate, lower-traffic path (not gated by `_ASK_SEMAPHORE`) and each construction opens its own httpx session, same inefficiency the singleton was built to avoid.

Fix: import `_get_anthropic_client` from `scout_agent` and pass the local `api_key`, same pattern as `scout_agent.ask()`. Left as debt rather than folded into this PR because it touches `scout_bot.py`'s own request path and widens the diff beyond the race being fixed here — do it as its own small PR.
