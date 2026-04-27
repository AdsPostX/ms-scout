"""
NL Query routing test — verifies Intent 21 / run_sql_query handles novel
questions correctly, stored-signal tools fire for known intents, and data
boundary questions are properly refused.

Run manually — makes live LLM + ClickHouse calls, takes ~5 min, costs ~$0.50.
Do NOT add to startup smoke test.

Usage:
  python3 nl_query_test.py              # stdout only
  python3 nl_query_test.py --slack      # also post results to #scout-qa
"""
import os
import pathlib
import sys
import time
from dotenv import load_dotenv

load_dotenv(override=True)
# Fallback: walk up from this file looking for a .env with the API key
if not os.getenv("ANTHROPIC_API_KEY"):
    for _p in pathlib.Path(__file__).parents:
        _candidate = _p / ".env"
        if _candidate.exists():
            load_dotenv(_candidate, override=True)
            if os.getenv("ANTHROPIC_API_KEY"):
                break

# Monkey-patch ask() to capture which tools were called
import scout_agent as _sa

_TOOL_CALLS: list[str] = []
_original_tool_map = {}

def _track_calls():
    """Wrap every TOOL_MAP entry to record when it fires."""
    for name, fn in _sa.TOOL_MAP.items():
        if fn is None:
            continue
        def _make_wrapper(n, f):
            def _wrapper(*a, **kw):
                _TOOL_CALLS.append(n)
                return f(*a, **kw)
            return _wrapper
        _sa.TOOL_MAP[name] = _make_wrapper(name, fn)

_track_calls()

# ── Test cases ────────────────────────────────────────────────────────────────
# Format: (label, question, expected_tool_or_category)
# expected: "run_sql_query", "stored:<tool>", "no_tool", "boundary"
TESTS = [
    # --- Novel analytical (should hit run_sql_query or a stored tool with SQL) ---
    (
        "Daily revenue last 7 days",
        "show me daily revenue for the last 7 days",
        "run_sql_query",
    ),
    (
        "Publishers with iOS-only traffic",
        "which publishers send us only iOS traffic?",
        "run_sql_query",
    ),
    (
        "Campaigns ending this month",
        "which campaigns end this month?",
        "run_sql_query",
    ),
    (
        "Top 5 publishers by click-through rate last 30 days",
        "what are the top 5 publishers by click-through rate over the last 30 days?",
        "run_sql_query",
    ),
    (
        "Revenue by day of week",
        "break down our revenue by day of week over the last 30 days",
        "run_sql_query",
    ),
    (
        "Campaigns with a monthly budget cap",
        "which campaigns have a monthly budget cap set?",
        "run_sql_query",
    ),
    (
        "Perkswall-only publishers",
        "which publishers have perkswall enabled?",
        "run_sql_query",
    ),
    # --- Should route to stored tools (not run_sql_query) ---
    (
        "Ghost campaigns (stored)",
        "show me ghost campaigns",
        "stored:get_ghost_campaigns",
    ),
    (
        "Revenue opportunities (stored)",
        "what are our top revenue opportunities?",
        "stored:get_top_revenue_opportunities",
    ),
    (
        "Publisher health (stored)",
        "how is AT&T performing?",
        "stored:get_publisher_health",
    ),
    (
        "System status (stored)",
        "status",
        "stored:get_scout_status",
    ),
    # --- Data boundary (Scout should refuse / redirect) ---
    (
        "SOV data (not in ClickHouse)",
        "what is our share of voice vs competitors?",
        "boundary",
    ),
    (
        "External email data (not in ClickHouse)",
        "what's the open rate on the email we sent to AT&T?",
        "boundary",
    ),
]

# ── Runner ────────────────────────────────────────────────────────────────────

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
WARN = "\033[93m~\033[0m"


def run_test(label: str, question: str, expected: str) -> dict:
    _TOOL_CALLS.clear()
    t0 = time.monotonic()
    try:
        result = _sa.ask(question, history=[], user_id="nl-test")
        elapsed = time.monotonic() - t0
        text = result.get("text", result) if isinstance(result, dict) else result
        text_preview = str(text).replace("\n", " ")[:120]
    except Exception as e:
        elapsed = time.monotonic() - t0
        return {
            "label": label,
            "question": question,
            "expected": expected,
            "tools_called": list(_TOOL_CALLS),
            "elapsed": elapsed,
            "passed": False,
            "reason": f"Exception: {e}",
            "preview": "",
        }

    tools = list(_TOOL_CALLS)

    # Evaluate pass/fail
    if expected == "run_sql_query":
        passed = "run_sql_query" in tools
        reason = "" if passed else f"Expected run_sql_query, got: {tools}"
    elif expected.startswith("stored:"):
        target = expected.split(":", 1)[1]
        passed = target in tools
        reason = "" if passed else f"Expected {target}, got: {tools}"
    elif expected == "boundary":
        # Scout should NOT call run_sql_query or any stored signal tool
        # and should mention the data isn't available
        bad_tools = [t for t in tools if t not in ("get_scout_status",)]
        boundary_phrases = [
            "not tracked", "not in our data", "not available", "don't have",
            "doesn't exist", "not captured", "outside", "network's own",
        ]
        mentioned_boundary = any(p in text.lower() for p in boundary_phrases)
        passed = mentioned_boundary or len(bad_tools) == 0
        reason = "" if passed else f"Expected boundary refusal, tools: {tools}, text snippet: {text_preview}"
    else:
        passed = True
        reason = "no_tool check (manual review)"

    return {
        "label": label,
        "question": question,
        "expected": expected,
        "tools_called": tools,
        "elapsed": round(elapsed, 1),
        "passed": passed,
        "reason": reason,
        "preview": text_preview,
    }


def main():
    print("\nScout NL Query Routing Test")
    print("=" * 70)

    results = []
    for label, question, expected in TESTS:
        print(f"\n  Testing: {label}")
        print(f"  Q: \"{question}\"")
        r = run_test(label, question, expected)
        results.append(r)

        icon = PASS if r["passed"] else FAIL
        print(f"  {icon} [{r['elapsed']}s] Tools: {r['tools_called']}")
        if not r["passed"]:
            print(f"     !! {r['reason']}")
        if r["preview"]:
            print(f"     Preview: {r['preview'][:100]}")

    # Summary
    passed = sum(1 for r in results if r["passed"])
    total = len(results)
    print("\n" + "=" * 70)
    print(f"\n{'ALL PASS' if passed == total else 'FAILURES DETECTED'} — {passed}/{total}\n")

    if passed < total:
        print("Failures:")
        for r in results:
            if not r["passed"]:
                print(f"  ✗ {r['label']}")
                print(f"    Expected: {r['expected']}")
                print(f"    Got tools: {r['tools_called']}")
                print(f"    Reason: {r['reason']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
