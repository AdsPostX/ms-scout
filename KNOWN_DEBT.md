# Scout — Known Debt

## demand_feed_main.py — MS Platform Feed (NOT live)

5 `MS_PLATFORM_TODO` code comments must be resolved before flipping live (see "Where the TODOs live" below). Contact Vamsee for the endpoint.

**Required env vars (set in Render):**

| Env var | Description |
|---|---|
| `CAMPAIGN_CREATE_WEBHOOK_URL` | POST endpoint on MS Platform accepting a `CampaignRequest` JSON body. Leave unset → dry_run mode (safe default). |
| `CAMPAIGN_CREATE_API_KEY` | Bearer token sent as `Authorization: Bearer <token>`. Leave unset → no auth header (dev/local only). |
| `CAMPAIGN_CREATE_DRY_RUN` | `"true"` (default) → log + return preview, no HTTP call. Set `"false"` AND set WEBHOOK_URL to go live. |

**Flip-live checklist (in order):**
- [ ] Get `CAMPAIGN_CREATE_WEBHOOK_URL` from Vamsee — confirm POST shape matches `_fire_campaign_creation` payload (`draft_id`, `offer`, `ai_copy`, `approver`, `approved_at`, `dry_run`)
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
