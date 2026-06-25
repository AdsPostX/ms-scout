# Add a New Monitor Signal to Scout

**Triggers:** "add a new monitor", "new signal", `/scout-new-monitor`

## The "All 5 or don't ship" gate

A signal that can't be force-run can't be debugged in production. All 5 touchpoints are required.

## Workflow

1. **Signal function** in `scout_bot.py` (default) or `demand_feed_main.py` (if demand-feed-native). Function must be callable standalone for force-run.

2. **Register in `scout_handlers.py:_FORCE_MONITOR_FNS`** — maps the slash command name to the signal function. Without this, the signal can't be force-run from Slack.

3. **Add entry to `alert_registry.py`**:
   - `dedup_key` — prevents duplicate fires within the cooldown window
   - `kill_switch` — env var name that disables the signal (e.g. `SCOUT_GHOST_DISABLED`)
   - `schedule` — cron expression for when it fires automatically

4. **Add slash command to Slack app manifest** at api.slack.com/apps → Slash Commands. Format: `/scout-<signal-name>`.

5. **Add smoke test** to `smoke_test.py` — at minimum, verify the signal function returns without error on a cold CH client.

6. **Verify in #bot-qa** after deploy:
   - Run `python3 smoke_test.py` — must pass
   - Force-run the signal: `/scout-<signal-name>` in #bot-qa
   - Confirm the card renders correctly (smoke green ≠ card looks right)

## Anti-patterns
- Signal with no force-run path (`_FORCE_MONITOR_FNS` entry missing)
- Signal with no kill switch (can't disable in production without a deploy)
- Signal that blocks the main thread (must be daemon-threaded)
