# Add a New Slash Command to Scout

**Triggers:** "add a slash command", "new /scout-*", `/scout-new-slash-cmd`

## Workflow

1. **Implement the handler** in `scout_handlers.py`. Handler receives `(req: SocketModeRequest, web: WebClient)`. Route via `_handle_slash_command`.

2. **Add to Slack app manifest** at api.slack.com/apps → Slash Commands:
   - Command: `/scout-<name>`
   - Request URL: (your Render Socket Mode endpoint)
   - Description: one-line summary shown to users in Slack

3. **Add a smoke test** to `smoke_test.py`.

4. **Run `python3 smoke_test.py`** — must pass before commit.

## Notes
- Slash commands return ephemeral responses by default — use `response_type: in_channel` only when the answer is relevant to the whole channel.
- If the command triggers a monitor signal, prefer routing through `_FORCE_MONITOR_FNS` rather than duplicating logic.
