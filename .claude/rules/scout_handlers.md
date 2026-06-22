---
globs: scout_handlers.py
---

# Rules for scout_handlers.py

## Concurrency model (PR #312)

- `_ASK_SEMAPHORE` is `BoundedSemaphore(3)`. New requests do a non-blocking acquire and raise `AskTimeout` immediately if at cap. Retry paths use `blocking_acquire_timeout_s=_CFG.ask_timeout_s`. Do not change this asymmetry.
- `_run_pulse_action` bypasses `_ASK_SEMAPHORE` and `_ask_with_timeout` intentionally — pulse actions are fire-and-forget UI refreshes, not interactive asks. Do NOT add a timeout guard here.
- `_handle_event_impl` is split from `handle_event` for crash isolation — keep the split. The broad `except` in `handle_event` must not wrap the `client.send_socket_mode_response(ack)` call.
- DM retry paths must NOT pass `user_id` to `wrap_response` — DMs auto-notify the recipient.
- Retry-path calls to `_ask_with_timeout` must use `blocking_acquire_timeout_s=_CFG.ask_timeout_s`.

## Block action dispatch

- `_BLOCK_ACTION_DISPATCH` dict: adding a button requires a new entry here AND a smoke test — missing either means the button silently no-ops.
- All `AskTimeout` exits must use `Card` + `wrap_response()` — no hand-built Slack blocks at callsites.
