---
globs: scout_agent.py
---

# Rules for scout_agent.py

- Every new tool needs: name, description, input_schema, TOOL_MAP entry, and handler function — missing any one silently breaks routing with no error at call time.
- Shared query functions must be prefixed `_query_` and accept `ch` (ClickHouse client) as the first arg.
- SYSTEM_PROMPT intent routing must have a numbered line for every tool — if a line is missing, ambiguous queries fall to LLM fallback instead of the right tool.
- Never put SQL inline in TOOL_MAP handlers — use or create a `_query_*()` function in the appropriate `queries_*.py` file.
- `ask()` signature changes require updating all AskTimeout catch sites in `scout_handlers.py` — grep for `AskTimeout` before touching the signature.
