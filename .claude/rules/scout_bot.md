---
globs: scout_bot.py
---

# Rules for scout_bot.py

- `_handle_approve` and `_handle_brief_queue` must never block the main thread for >30s.
- New `_build_*_signal` functions must NOT duplicate SQL from `scout_agent.py` — use shared `_query_*()` functions.
- Block actions must thread-dispatch any operation >1s.
- Never add bare `except:` — always `log.warning()` with context so failures surface in Render logs.
- `_route_channel("offers")` routes to `#scout-qa` in dev, `#scout-offers` in prod — never hardcode channel IDs.
