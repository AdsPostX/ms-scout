# Scout — Known Debt

## _BoundedRateLimitRetryHandler duplicated in scout_bot.py and scout_handlers.py

Same 15-line class exists verbatim in both files (`scout_handlers.py` from PR #329, `scout_bot.py` from PR #330) — each module constructs its own `WebClient` and needed the same uncapped-retry-sleep fix. Not unified at the time because `scout_slack_safe.py` (the module both files already share for `guard_web_client`) is scoped to response-emission invariants, not HTTP retry behavior, and unifying via `scout_handlers.py` would have required editing a file that was part of the still-open #329.

Fix once both #329 and #330 have merged: extract `_BoundedRateLimitRetryHandler` into a new small module (e.g. `scout_slack_retry.py`) and point both `WebClient` construction sites at it. Do this as its own PR, not bundled with unrelated work.

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

## scout_digest.py:1381-1383 — `_build_sourcing_intel_blocks` hand-rolls payout resolution instead of `_resolve_payout()`

PR #327/#328 consolidated the digest's other three payout-resolution call sites (`score_offer`, `build_why_text`, `build_digest_blocks`) onto the shared `_resolve_payout(offer_id, offer, payout_cache)` helper. `_build_sourcing_intel_blocks()` still calls `_parse_payout(o.get("payout"))` + `_normalize_payout_type(o.get("payout_type") or "")` directly instead.

Not a drop-in swap: `_build_sourcing_intel_blocks(signals: dict)` has no `payout_cache` parameter in its signature, and its offer dicts (`net_offers`, sourcing-signal shape) use raw `payout`/`payout_type` keys — not the `_payout_num`/`_payout_type_norm` shape `_resolve_payout` expects from scraper-normalized offers. Converting it means either threading `payout_cache` through `_build_sourcing_intel_blocks` and its caller (`build_digest_blocks` → `sourcing_blocks = _build_sourcing_intel_blocks(sourcing_signals)`, line ~1718), or writing a cache-less variant — a real design decision, not a mechanical rename.

Fix: decide cache-threading vs. cache-less variant, then unify. Left as debt rather than fixed inline because it's a different code path (sourcing-signal cards, not the main network digest) touched by an unrelated PR — do it as its own small PR.

## scout_bot.py:1661,1781 — raw `anthropic.Anthropic()` bypasses the scout_agent.py singleton

Both call sites do `import anthropic as _anthropic; _anth_client = _anthropic.Anthropic()` inline instead of going through `scout_agent._get_anthropic_client()`. This PR (Anthropic-client-construction race fix) only hardened the singleton in `scout_agent.py` — these two are a separate, lower-traffic path (not gated by `_ASK_SEMAPHORE`) and each construction opens its own httpx session, same inefficiency the singleton was built to avoid.

Fix: import `_get_anthropic_client` from `scout_agent` and pass the local `api_key`, same pattern as `scout_agent.ask()`. Left as debt rather than folded into this PR because it touches `scout_bot.py`'s own request path and widens the diff beyond the race being fixed here — do it as its own small PR.
