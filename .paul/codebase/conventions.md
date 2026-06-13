# Scout — Conventions & Patterns

## Naming

**Files:** `scout_*` prefix for all core modules. Snake case throughout.

**Functions:**
- Public: `ask()`, `wrap_response()`, `capture()`, `get_maintenance()`
- Private: `_fmt_money_short()`, `_load_entity_overrides()`, `_build_brief_blocks()`
- Prefix `_query_*` for ClickHouse helpers, `_build_*` for Block Kit builders, `_handle_*` for Slack event handlers

**Constants:** `UPPER_CASE` at module level — `MAX_ROUNDS = 12`, `BUDGETS`, `SCOUT_THRESHOLDS`

**Thread locks:** Descriptive name + suffix — `_PULSE_STATE_LOCK`, `_MAINTENANCE_LOCK`, `_COPY_CACHE_LOCK`

## Type System

**TypedDict** (`scout_types.py`): Documents dict shapes flowing between modules. No runtime enforcement. Key types: `Offer`, `FormattedOffer`, `Brief`, `PulseSignal`.

**Dataclasses** (`scout_core/contracts.py`): Typed boundary for offer pipeline. `RawOffer` → `NormalizedOffer` → `DigestCandidate`. `from_dict()` / `to_dict()` helpers.

**No Pydantic** — native Python typing only.

**Frozen dataclasses** for results that must not mutate:
```python
@dataclass(frozen=True)
class AskResult:
    text: str
    tools_called: tuple = ()
    duration_ms: int = 0
    payload: Optional[Mapping] = None  # deep-frozen via _freeze()
```

## Error Handling

**Pattern**: broad `except Exception as e:` with logging context. Never silent.
```python
try:
    result = ask(query, user_id=user_id)
except Exception as e:
    log.exception("[context] operation failed: %s", e)
    return safe_fallback
```

**Telemetry layer**: always fault-tolerant — `scout_telemetry.capture()` swallows all errors, re-raises from wrapped function.

**State I/O**: atomic writes (`write to .tmp → os.replace()`) prevent partial writes on crash.

**No custom exception types** — uses built-in exceptions.

## Logging

```python
log = logging.getLogger("scout_bot")  # named by module
log.debug("[context] message: %s", value)
log.info("[context] message")
log.warning("[telemetry] init failed: %s", exc)
log.exception("[module] operation failed")  # includes traceback
```

Context markers in brackets (`[telemetry]`, `[context]`, `[scout_state]`) for grep-based log search.

No structured logging library. Plain Python `logging` with `%`-style formatting.

## Import Order (ruff isort)

1. Standard library
2. Third-party
3. First-party (declared in `pyproject.toml` `known-first-party`)
4. No relative imports — all absolute module names

Known-first-party: `scout_agent`, `scout_bot`, `scout_handlers`, `scout_state`, `scout_types`, `scout_notion`, `scout_digest`, `queries`

## Backward-Compat Re-exports

`scout_agent.py` re-exports from `scout_ch`:
```python
from scout_ch import (  # noqa: F401 — backward compat re-exports
    _run_parallel, _get_ch_client, _LoggingCHClient, ...
)
```
Allows older callers to import from `scout_agent` while the real home is `scout_ch`.

## Module-Level Initialization

- `load_dotenv()` at top of modules that need env vars (plist env takes precedence)
- Singletons registered at import time: `_init()` in `scout_telemetry`, `set_geo_normalizer()` in `scout_core.contracts`
- Config loaded once: `_load_ui_thresholds()` → `SCOUT_THRESHOLDS` in `scout_ui_kit`

## ScoutKit (scout_ui_kit.py) Contract

**Entry point**: `wrap_response(*, card, surface, pattern=None, ...)` → `(fallback_text, list[dict])`

**Pattern ↔ Surface validation** — raises `ValueError` at call time if mismatched:
```python
# Raises: ALERT requires MONITOR_ALARM
wrap_response(card=card, surface=Surface.CHANNEL_ROOT, pattern=ResponsePattern.ALERT)
```

**Block budgets** enforced by `enforce(blocks, surface)`:
- `CHANNEL_ROOT`: 8 blocks
- `THREAD`: 50 blocks
- `DM`: 6 blocks
- Overflow → truncated + indicator appended

**Mobile-first rules** (enforced by `tests/test_kit_lint.py`):
1. CTAs in `actions` blocks only (not `section.accessory`)
2. No fenced code blocks ``` (use inline `code`)
3. `action_id` unique within view
4. `style: "danger"` only for destructive actions
5. Budget enforcement required before every post

**Pure module rule**: `scout_ui_kit` imports stdlib only. Zero Slack API calls, zero ClickHouse, zero file I/O.

## Offer Pipeline Conventions

Producer (`offer_scraper.py`) → `RawOffer` → `NormalizedOffer` → consumer (`scout_digest.py`)

`normalize_geo()` is injected at import time:
```python
# offer_scraper.py (runs at import)
from scout_core.contracts import set_geo_normalizer
set_geo_normalizer(normalize_geo)
```

Consumers call `from_dict()` / `to_dict()` helpers — never manipulate raw dicts directly.

## Configuration Merge (Layered)

```
config/scout_thresholds.json (base)
  + data/threshold_overrides.json (runtime, @Scout set_threshold)
  → merged dict (overrides win)
  → loaded via scout_agent._load_thresholds()
```

Runtime changes persist to `data/threshold_overrides.json` with audit fields: `value`, `set_by`, `set_at`, `reason`.
