---
globs: scout_handlers.py
---

# Rules for scout_handlers.py

## Concurrency model (PR #312)

- `_ASK_SEMAPHORE` is `BoundedSemaphore(3)`. New requests do a non-blocking acquire and raise `AskTimeout` immediately if at cap. Retry paths use `blocking_acquire_timeout_s=_CFG.ask_timeout_s`. Do not change this asymmetry.
- `_run_pulse_action` bypasses `_ASK_SEMAPHORE` and `_ask_with_timeout` intentionally — pulse actions are fire-and-forget UI refreshes, not interactive asks. Do NOT add a timeout guard here.
- `_handle_event_impl` is split from `handle_event` for crash isolation — keep the split. The broad `except` in `handle_event` must not wrap the `client.send_socket_mode_response(ack)` call.
- `wrap_response()` has no `user_id` parameter — it never did in the current signature (see `scout_ui_kit.py` ~L644-653: `card`, `surface`, `suggestions`, `elapsed_seconds`, `interpretation`, `pattern`, `agent_steps`). DM-vs-broadcast is controlled by the `surface` argument (`Surface.DM` vs `Surface.CHANNEL_ROOT`/`THREAD`), not by `user_id`. `user_id` is only used to build a `_mention()` prefix string for channel paths (see `_retry_after_timeout`'s docstring: "Pass surface explicitly; do not rely on user_id as a surface proxy"); DMs omit the mention because a DM reply auto-notifies the recipient. Always pass `surface=` explicitly at retry-path callsites.
- Retry-path calls to `_ask_with_timeout` must use `blocking_acquire_timeout_s=_CFG.ask_timeout_s`.

## Block action dispatch

- `_BLOCK_ACTION_DISPATCH` dict: adding a button requires a new entry here AND a smoke test — missing either means the button silently no-ops.
- All `AskTimeout` exits must use `Card` + `wrap_response()` — no hand-built Slack blocks at callsites.
