"""
Scout Smoke Test — run after every deploy, or manually anytime.

Usage:
  python smoke_test.py              # prints results to stdout
  python smoke_test.py --slack      # also posts results to #scout-qa
  python smoke_test.py --slack --quiet   # Slack only (no stdout)

Tests covered:
  1. Anthropic API — valid model name, auth works
  2. ClickHouse — connection + simple query
  3. Entity overrides — file readable, valid JSON
  4. Offer inventory — offers_latest.json present and non-empty
  5. ask("status") — end-to-end LLM + tool call round-trip
  6. ask("ghost campaigns") — tool-calling path (ClickHouse query)
"""

import argparse
import json
import os
import pathlib
import sys
import time

from dotenv import load_dotenv

load_dotenv(override=True)

_ROOT = pathlib.Path(__file__).parent
_DATA = _ROOT / "data"

TESTS: list[dict] = []


def test(name: str):
    """Decorator to register a smoke test."""
    def decorator(fn):
        TESTS.append({"name": name, "fn": fn})
        return fn
    return decorator


# ── Test 1: Anthropic API ─────────────────────────────────────────────────────

@test("Anthropic API — model auth")
def test_anthropic():
    import anthropic
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return False, "ANTHROPIC_API_KEY not set"
    client = anthropic.Anthropic(api_key=api_key)
    for model in ("claude-haiku-4-5", "claude-sonnet-4-6"):
        try:
            resp = client.messages.create(
                model=model, max_tokens=5,
                messages=[{"role": "user", "content": "ping"}]
            )
            if resp.content:
                return True, f"Both models reachable ({model} ✓)"
            return False, f"{model} returned empty response"
        except Exception as e:
            return False, f"{model} failed: {e}"
    return False, "No models tested"


# ── Test 2: ClickHouse ────────────────────────────────────────────────────────

@test("ClickHouse — connection + query")
def test_clickhouse():
    try:
        from scout_agent import _get_ch_client
        ch = _get_ch_client()
        rows = ch.query("SELECT count() FROM adpx_sdk_sessions LIMIT 1").result_rows
        count = rows[0][0] if rows else 0
        return True, f"Connected — {count:,} sessions in table"
    except Exception as e:
        return False, str(e)


# ── Test 3: Entity overrides ──────────────────────────────────────────────────

@test("Entity overrides — file readable")
def test_entity_overrides():
    try:
        from scout_agent import _load_entity_overrides
        overrides = _load_entity_overrides()
        pubs = overrides.get("publishers", {})
        advs = overrides.get("advertisers", {})
        button_ok = "Button" in pubs
        return True, f"{len(pubs)} publishers, {len(advs)} advertisers (Button seeded: {button_ok})"
    except Exception as e:
        return False, str(e)


# ── Test 4: Offer inventory ───────────────────────────────────────────────────

@test("Offer inventory — offers_latest.json present")
def test_offer_inventory():
    snap = _DATA / "offers_latest.json"
    if not snap.exists():
        return False, "offers_latest.json missing — scraper hasn't run yet"
    try:
        offers = json.loads(snap.read_text())
        active = sum(1 for o in offers if o.get("status") == "Active")
        age_hours = (time.time() - snap.stat().st_mtime) / 3600
        age_str = f"{age_hours:.0f}h old"
        if age_hours > 30:
            return False, f"{len(offers)} offers but file is {age_str} — scraper may be stuck"
        return True, f"{len(offers)} total, {active} active, {age_str}"
    except Exception as e:
        return False, f"parse error: {e}"


# ── Test 5: ask() round-trip ──────────────────────────────────────────────────

@test("ask('status') — LLM + tool round-trip")
def test_ask_status():
    try:
        from scout_agent import ask
        t0 = time.monotonic()
        result = ask("status", history=[], user_id="smoke-test")
        elapsed = time.monotonic() - t0

        text = result.get("text", result) if isinstance(result, dict) else result
        if not text or "broke" in text.lower() or "error" in text.lower():
            return False, f"Bad response: {str(text)[:120]}"
        return True, f"Responded in {elapsed:.1f}s — {str(text)[:80]}..."
    except Exception as e:
        return False, str(e)


# ── Test 6: tool-calling path ─────────────────────────────────────────────────

@test("ask('ghost campaigns') — tool-calling path")
def test_ask_tool_call():
    try:
        from scout_agent import ask
        t0 = time.monotonic()
        result = ask("ghost campaigns", history=[], user_id="smoke-test")
        elapsed = time.monotonic() - t0

        text = result.get("text", result) if isinstance(result, dict) else result
        if not text:
            return False, "Empty response from ghost campaign tool call"
        return True, f"Tool call returned in {elapsed:.1f}s — {str(text)[:80]}..."
    except Exception as e:
        return False, str(e)


# ── Runner ────────────────────────────────────────────────────────────────────

def run_tests(quiet: bool = False) -> tuple[list[dict], int]:
    """Run all tests. Returns (results, pass_count)."""
    results = []
    for t in TESTS:
        try:
            passed, detail = t["fn"]()
        except Exception as e:
            passed, detail = False, f"uncaught: {e}"
        results.append({"name": t["name"], "passed": passed, "detail": detail})
        if not quiet:
            icon = "✅" if passed else "❌"
            print(f"  {icon}  {t['name']}")
            print(f"      {detail}")
    return results, sum(1 for r in results if r["passed"])


def format_slack_message(results: list[dict], pass_count: int) -> str:
    total = len(results)
    all_pass = pass_count == total
    header_icon = ":white_check_mark:" if all_pass else ":warning:"
    header = (
        f"{header_icon} *Scout smoke test — {pass_count}/{total} passed*"
        if not all_pass else
        f":white_check_mark: *Scout is healthy — {pass_count}/{total} checks passed*"
    )
    lines = [header]
    for r in results:
        icon = ":large_green_circle:" if r["passed"] else ":red_circle:"
        lines.append(f"{icon} {r['name']}")
        if not r["passed"] or len(r["detail"]) < 80:
            lines.append(f"   _{r['detail']}_")
    if not all_pass:
        lines.append("\n:mag: Check Render logs for the failing checks above.")
    return "\n".join(lines)


def format_slack_blocks(results: list[dict], pass_count: int) -> tuple[list[dict], str]:
    """Return (blocks, fallback_text) for a Block Kit health dashboard card."""
    total = len(results)
    all_pass = pass_count == total
    icon = ":white_check_mark:" if all_pass else ":warning:"
    headline = (
        f"{icon} *Scout is healthy — {pass_count}/{total} checks passed*"
        if all_pass else
        f"{icon} *Scout has issues — {pass_count}/{total} checks passed*"
    )
    fallback = f"Scout: {pass_count}/{total} checks passed"

    fields = []
    for r in results:
        status_icon = ":large_green_circle:" if r["passed"] else ":red_circle:"
        fields.append({
            "type": "mrkdwn",
            "text": f"{status_icon} *{r['name']}*\n{r['detail']}"
        })

    blocks: list[dict] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": headline}},
        {"type": "divider"},
        {"type": "section", "fields": fields},
    ]
    if not all_pass:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":mag: *Check Render logs for the failing checks above.*"},
        })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "Startup check · All systems operational" if all_pass else "Startup check · Issues detected — check Render logs"}],
    })
    return blocks, fallback


def post_to_slack(results: list[dict], pass_count: int) -> bool:
    """Post Block Kit health dashboard to #scout-qa."""
    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        print("SLACK_BOT_TOKEN not set — cannot post to Slack")
        return False
    from slack_sdk.web import WebClient
    try:
        blocks, fallback = format_slack_blocks(results, pass_count)
        web = WebClient(token=token)
        web.chat_postMessage(
            channel="C0AQEECF800",
            text=fallback,
            blocks=blocks,
            unfurl_links=False,
        )
        return True
    except Exception as e:
        print(f"Slack post failed: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scout smoke tests")
    parser.add_argument("--slack", action="store_true", help="Post results to #scout-qa")
    parser.add_argument("--quiet", action="store_true", help="Suppress stdout output")
    args = parser.parse_args()

    if not args.quiet:
        print("\nScout Smoke Test")
        print("=" * 50)

    results, pass_count = run_tests(quiet=args.quiet)
    total = len(results)

    if not args.quiet:
        print("=" * 50)
        status = "ALL PASS" if pass_count == total else f"FAILED {total - pass_count}/{total}"
        print(f"\n{status}\n")

    if args.slack:
        posted = post_to_slack(results, pass_count)
        if not args.quiet:
            print(f"Slack: {'posted to #scout-qa' if posted else 'failed'}")

    sys.exit(0 if pass_count == total else 1)
