# Scout — Testing

## Test Surface Summary

| Surface | File(s) | Type | Run Cost | In CI? |
|---------|---------|------|----------|--------|
| Unit tests | `tests/test_*.py` (37 files) | pytest | Free | Yes |
| Smoke test | `smoke_test.py` | Integration (ClickHouse + Slack) | Low | Post-deploy |
| NL query test | `nl_query_test.py` | Live LLM + ClickHouse | ~$0.50, ~5 min | No — manual |
| Routing evals | `evals/run_routing_evals.py` | Live LLM | Token cost | No — manual |

## Unit Tests (tests/)

**Run:**
```bash
pytest tests/
pytest tests/test_kit_lint.py        # specific file
pytest -k "test_name"               # filter
pytest --cov                        # coverage report
```

**Key test files:**

| File | What it tests |
|------|--------------|
| `test_kit_lint.py` | Block Kit mobile-first rules (unique action_ids, no fenced code, CTA placement) |
| `test_text_to_blocks.py` | Markdown → Slack blocks conversion (`_text_to_blocks`) |
| `test_ask_contract.py` | `ask()` result types, ClickHouse query validation |
| `test_intent_routing.py` | LLM intent classification → tool routing |
| `test_queue_card.py` | Notion queue card rendering |
| `test_smart_thumbs_down.py` | Feedback signal handling |
| `test_maintenance_gate.py` | Maintenance mode gate behavior |
| `test_revenue_today.py` | Revenue projection engine |
| `test_projection_engine.py` | EOD forecasting |
| `test_p2_*`, `test_p3_*` | Phase-milestone feature tests |

**Coverage config (pyproject.toml):**
```toml
source = [
  "scout_agent", "scout_bot", "scout_handlers", "scout_notion",
  "scout_digest", "scout_state", "queries"
]
omit = ["tests/*", "smoke_test.py"]
```

**conftest.py pattern** (root of tests/):
- Adds worktree root to `sys.path` so `scout_core` resolves without install
- Registers stub `geo_normalizer` so tests don't require `offer_scraper` to be importable
- Real normalizer registered at runtime by producer

## Smoke Test (smoke_test.py)

14 critical health checks run post-deploy and at startup (non-blocking thread).

**Run:**
```bash
python smoke_test.py              # quiet
python smoke_test.py --slack      # posts results to #scout-qa
python smoke_test.py --quiet      # suppress console output
```

Covers: ClickHouse connection, entity overrides, offer inventory freshness, LLM round-trip, state files readable, Slack auth, Notion DB ID.

**Deferred stubs** (bare `pass` — intentional):
```python
def test_fires_log_persistence():       pass  # GATE: autocheck unattended 5+ days
def test_app_home_drill_modals():       pass  # GATE: Jon/Todd/Roj open Home tab
def test_alert_registry_redis():        pass  # GATE: same as App Home PR 2
def test_ms_platform_campaign_creation(): pass  # GATE: Vamsee delivers webhook URL
```
Check-ins: `fires_log` 2026-06-14, App Home + Redis 2026-07-18, campaign webhook 2026-06-21.
Two of these have `KILL-IF-UNMET: yes`.

## NL Query Test (nl_query_test.py)

Live test: intent routing, tool calls, data boundary refusals.
```bash
python3 nl_query_test.py
python3 nl_query_test.py --slack  # post results to Slack
```
Not in startup smoke test. Costs ~$0.50 in LLM tokens, ~5 min.

## Routing Evals (evals/)

**Dataset:** `evals/scout_routing_evals.jsonl`

Each entry: `id`, `query`, `category`, `expected_tools_any`, `forbidden_tools`, `min_tools`

**Run:**
```bash
python3 evals/run_routing_evals.py
python3 evals/run_routing_evals.py --ids id1,id2   # filter
```

**Scoring:**
- Forbidden tool fired → FAIL
- Expected tools not in `res.tools_called` → FAIL
- `min_tools` not met → FAIL

Exit code 0 if all pass, 1 if any fail. Use before shipping planner changes.

## pytest Config (pyproject.toml)

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
```

## What's NOT Tested

- Offer scraper network calls (mocked or skipped)
- Slack WebClient calls (mocked in most tests)
- Redis backend (deferred gate — stub only)
- App Home drill modals (deferred gate — stub only)
- MS platform campaign webhook (deferred gate — stub only)
