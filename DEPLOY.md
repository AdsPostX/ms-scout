# ms-demand-feed → PR 27 Transition Runbook

Checklist for cutting Scout over from its internal scraper daemon to the standalone
ms-demand-feed service. Execute phases in order. Do not advance to the next phase
until all verification steps in the current phase pass.

---

## Phase 1 — Ship Part A (PR #71 — demand-feed hardening)

- [ ] Merge `feat/demand-feed-hardening` → `claude/pr26-demand-feed` on GitHub
- [ ] In Render dashboard → **ms-demand-feed** service → Environment:
  - [ ] Add `SLACK_BOT_TOKEN` (same value as ms-scout's token)
  - [ ] Add `SLACK_ALERT_CHANNEL` = `#scout-offers`
- [ ] Trigger **Manual Deploy** on ms-demand-feed
- [ ] Wait for deploy to show **Live**
- [ ] Verify: within ~2 min of deploy, a `:white_check_mark: daily scrape complete` ping (or first-boot run) appears in `#scout-offers`
  - If no message after 5 min, check Render logs for `[demand-feed] HTTP server started on :8080`

---

## Phase 2 — Ship Part B (feat/demand-feed-pr27 — HTTP server)

- [ ] Merge `feat/demand-feed-pr27` → `claude/pr26-demand-feed` on GitHub
- [ ] In Render dashboard → **ms-demand-feed** service → Settings:
  - [ ] Confirm service type is **Web Service** (not Worker). `render.yaml` declares `type: web` — if the dashboard still shows Worker, change it manually before redeploying.
- [ ] In Render dashboard → **ms-demand-feed** → Environment:
  - [ ] Confirm `DEMAND_FEED_PORT` = `8080` is set (already in `render.yaml`; verify it isn't overridden to blank)
- [ ] Trigger **Manual Deploy** on ms-demand-feed
- [ ] Wait for deploy to show **Live**
- [ ] Verify HTTP endpoint returns valid JSON:
  ```
  curl https://ms-demand-feed.onrender.com/offers | python3 -m json.tool | head -20
  ```
  Expected: JSON array of offer objects. A 503 means the first boot scrape hasn't finished yet — wait up to 30 min and retry.

---

## Phase 3 — Cut Scout over to demand-feed

- [ ] In Render dashboard → **ms-scout** service → Environment:
  - [ ] Set `DEMAND_FEED_URL` = `https://ms-demand-feed.onrender.com`
    (no trailing slash, no `/offers` — the code appends `/offers` automatically)
- [ ] Trigger **Manual Deploy** on ms-scout
- [ ] Wait for deploy to show **Live**
- [ ] Verify Scout reads from demand-feed: send `@Scout what offers do we have?` in `#revenue-operations`
  - Confirm response lists offers (count and advertiser names should match the `/offers` endpoint output from Phase 2)
- [ ] Watch `#scout-offers` for the next Sniper digest (06:00 CT):
  - [ ] Only **one** success ping appears (from ms-demand-feed, not Scout)
  - [ ] No double digest

---

## Phase 4 — Remove scraper daemon from Scout (feat/demand-feed-daemon-removal)

Prerequisites: `claude/pr26-demand-feed` is fully merged to `main`.

- [ ] Merge `feat/demand-feed-daemon-removal` → `main` on GitHub
- [ ] Trigger **Manual Deploy** on ms-scout
- [ ] Wait for deploy to show **Live**
- [ ] Wait 24 hours
- [ ] Verify Sniper digest fired at 06:00 CT in `#revenue-operations`
- [ ] Verify `#scout-offers` shows exactly **one** success ping for the day (from ms-demand-feed only — Scout no longer runs its own scraper)

---

## Rollback Plan

If anything breaks after Phase 3, revert Scout immediately: in the Render dashboard,
go to **ms-scout** → Environment → clear (or blank out) `DEMAND_FEED_URL` → trigger
Manual Deploy. Scout will fall back to its local `data/offers_latest.json` disk
snapshot on the next `_load_offers()` call. No offer data is lost — the disk file is
unchanged. Do not touch ms-demand-feed during a Scout rollback; it can stay running.
