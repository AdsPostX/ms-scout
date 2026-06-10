---
phase: 10-command-registry
plan: 01
type: summary
status: complete
date: 2026-06-10
---

# Phase 10-01 Summary — Command Registry

## What shipped

Three files changed. Zero new dependencies.

**scout_agent.py** — added `_COMMAND_REGISTRY`, `_match_command()`, `_format_status_response()`,
`_cmd_status()`, and wired `_match_command()` into both `ask()` and `ask_with_attachment()`
immediately after the `_route_deterministic` block.

**scout_handlers.py** — `/scout-status` slash command replaced 12-line inline formatter with a
single call to the shared `_format_status_response()`.

**smoke_test.py** — 4 new tests: alias coverage, no-LLM shape, open-query passthrough,
slash/mention format parity.

## Decisions made

| # | Decision | Why |
|---|---|---|
| 1 | Alias-exact matching (not substring) | "how's the offer status for CJ" must NOT match `status` — full message equality prevents false positives |
| 2 | Bypass LLM entirely in `_cmd_status()` | `get_scout_status()` already returns a well-structured dict; no synthesis needed; trust and determinism are the goal |
| 3 | Single formatter in scout_agent.py | Both call sites import the same function — structural guarantee of parity, not a convention |
| 4 | Only `status` in this phase | Pattern proven; `queue`, `help`, and others extend with one dict entry + one handler — no routing changes needed |

## Verification

```
python3 smoke_test.py → ALL PASS (4 new tests included)
python3 -c "from scout_agent import _match_command, _format_status_response, _cmd_status; print('ok')" → ok
grep -c "Benchmarks:.*Offers:.*Queue" scout_handlers.py → 0
grep -n "_match_command" scout_agent.py → called in ask() and ask_with_attachment()
```

## What's next

Extend the registry with `queue` and `help` when those commands need determinism.
Both follow the same pattern: entry in `_COMMAND_REGISTRY` + `_cmd_queue()`/`_cmd_help()` handler.
No changes to routing logic required.
