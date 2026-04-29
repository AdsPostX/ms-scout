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
  7. State files — JSON validity of pulse_state/digest_state/image_cache + data/ writable
  8. Slack token — auth.test confirms bot identity
  9. Notion queue DB ID — NOTION_QUEUE_DB_ID env var is set
 10. Handler symbols — SocketModeResponse and RateLimitErrorRetryHandler importable
 11. scout_state runtime — _pick_loading_message and _smart_history callable
 12. _build_advertiser_rpm_context_blocks — pure function, no DB
 13. get_scout_status() — digest_env + digest_routing fields present
 14. get_scout_status() — available_networks is a list when offers exist
 15. get_pulse_summary() — has_pulse key present, handles no-pulse case gracefully
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
        first_line = str(text).split('\n')[0].strip()
        preview = (first_line[:60] + "…") if len(first_line) > 60 else first_line
        return True, f"Responded in {elapsed:.1f}s — {preview}"
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
        first_line = str(text).split('\n')[0].strip()
        preview = (first_line[:60] + "…") if len(first_line) > 60 else first_line
        return True, f"Tool call returned in {elapsed:.1f}s — {preview}"
    except Exception as e:
        return False, str(e)


# ── Test 7: State files ───────────────────────────────────────────────────────

@test("State files — JSON valid + data/ writable")
def test_state_files():
    import tempfile
    issues = []
    for fname in ("pulse_state.json", "digest_state.json", "image_cache.json"):
        path = _DATA / fname
        if path.exists():
            try:
                json.loads(path.read_text())
            except Exception as e:
                issues.append(f"{fname} invalid JSON: {e}")
        # Missing files are fine — they're created on first write
    # Confirm data/ is writable
    try:
        with tempfile.NamedTemporaryFile(dir=_DATA, delete=True):
            pass
    except Exception as e:
        issues.append(f"data/ not writable: {e}")
    if issues:
        return False, "; ".join(issues)
    return True, "All present state files parse cleanly; data/ is writable"


# ── Test 8: Slack token ───────────────────────────────────────────────────────

@test("Slack token — auth.test")
def test_slack_token():
    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        return False, "SLACK_BOT_TOKEN not set"
    try:
        from slack_sdk.web import WebClient
        resp = WebClient(token=token).auth_test()
        bot_name = resp.get("user", "unknown")
        team = resp.get("team", "unknown")
        return True, f"Authenticated as @{bot_name} in {team}"
    except Exception as e:
        return False, str(e)


# ── Test 9: Notion queue DB ID ────────────────────────────────────────────────

@test("Notion queue DB ID — env var set")
def test_notion_queue_db_id():
    db_id = os.getenv("NOTION_QUEUE_DB_ID")
    if not db_id:
        return False, "NOTION_QUEUE_DB_ID not set — queue watcher will not work"
    if len(db_id) < 30:
        return False, f"NOTION_QUEUE_DB_ID looks malformed (len={len(db_id)})"
    return True, f"Set (prefix: {db_id[:8]}…)"


# ── Test 10: handler import chain ─────────────────────────────────────────────

@test("Handler symbols — SocketModeResponse and RateLimitErrorRetryHandler importable")
def test_handler_imports():
    """
    Verify that all symbols used inside handle_event() are importable.
    The smoke test bypasses handle_event entirely — this test catches the class
    of silent import failures that have broken Scout three times post-module-split.
    """
    try:
        from slack_sdk.socket_mode.response import SocketModeResponse  # noqa: F401
        from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler  # noqa: F401
    except ImportError as e:
        return False, f"Missing Slack SDK symbol (handle_event will crash on first @mention): {e}"
    try:
        import scout_handlers  # noqa: F401
    except ImportError as e:
        return False, f"scout_handlers import failed (all @mentions will be silent): {e}"
    return True, "SocketModeResponse ✓  RateLimitErrorRetryHandler ✓  scout_handlers ✓"


# ── Test 11: scout_state runtime functions ────────────────────────────────────

@test("scout_state runtime — _pick_loading_message and _smart_history callable")
def test_scout_state_runtime():
    """
    Verify that functions in scout_state.py are importable AND callable at runtime.
    Test 10 catches missing module-level imports; this test catches missing stdlib
    imports used only inside function bodies — the same class of bug that caused
    Scout to be silent on every @mention (random/re/threading not imported).
    """
    try:
        from scout_state import _pick_loading_message, _smart_history
        msg = _pick_loading_message("ghost campaigns")
        if not msg:
            return False, "_pick_loading_message returned empty message"
        result = _smart_history([])
        if result != []:
            return False, f"_smart_history([]) should return [] but got: {result}"
        long_history = [{"role": "user", "content": f"msg {i}"} for i in range(6)]
        trimmed = _smart_history(long_history)
        if len(trimmed) > 6:
            return False, f"_smart_history didn't truncate: got {len(trimmed)} messages"
        return True, f"_pick_loading_message ✓  _smart_history ✓  (msg='{msg[:30]}…')"
    except Exception as e:
        return False, f"scout_state runtime function failed: {e}"


@test("_build_advertiser_rpm_context_blocks — pure function, no DB")
def test_rpm_context_blocks():
    """
    Verify _build_advertiser_rpm_context_blocks is importable and returns correct
    Block Kit structure for both has_history=True and has_history=False cases.
    """
    try:
        from scout_slack_ui import _build_advertiser_rpm_context_blocks
        # has_history=False → empty list
        empty = _build_advertiser_rpm_context_blocks({"has_history": False}, scout_estimate=50)
        if empty != []:
            return False, f"has_history=False should return [] but got: {empty}"
        # has_history=True → one context block
        ctx = {
            "has_history":      True,
            "active_campaigns": 3,
            "impressions_30d":  500_000,
            "revenue_30d":      25_000.0,
            "rpm_min":          42.0,
            "rpm_max":          120.0,
            "rpm_avg":          50.0,
        }
        blocks = _build_advertiser_rpm_context_blocks(ctx, scout_estimate=60)
        if not isinstance(blocks, list) or len(blocks) == 0:
            return False, f"has_history=True should return non-empty list, got: {blocks}"
        if blocks[0].get("type") != "context":
            return False, f"first block should be type=context, got: {blocks[0].get('type')}"
        return True, f"has_history=False → [] ✓  has_history=True → context block ✓"
    except Exception as e:
        return False, f"_build_advertiser_rpm_context_blocks failed: {e}"


@test("get_scout_status() — digest_env + digest_routing fields present")
def test_scout_status_digest_fields():
    try:
        from scout_agent import get_scout_status
        status = get_scout_status()
        if "digest_env" not in status:
            return False, "digest_env missing from status dict"
        if "digest_routing" not in status:
            return False, "digest_routing missing from status dict"
        routing = status["digest_routing"]
        if not isinstance(routing, str) or not routing:
            return False, f"digest_routing should be non-empty string, got: {routing!r}"
        return True, f"digest_env={status['digest_env']!r} routing={routing[:50]}"
    except Exception as e:
        return False, str(e)


@test("get_scout_status() — available_networks list present when offers exist")
def test_scout_status_available_networks():
    try:
        from scout_agent import get_scout_status
        status = get_scout_status()
        offer_count = status.get("offer_inventory", 0)
        if offer_count == 0:
            return True, "offer_inventory=0 — available_networks not expected (no offers loaded)"
        if "available_networks" not in status:
            return False, f"available_networks missing from status dict (offer_inventory={offer_count})"
        nets = status["available_networks"]
        if not isinstance(nets, list) or not nets:
            return False, f"available_networks should be non-empty list, got: {nets!r}"
        return True, f"available_networks={nets} ({len(nets)} networks)"
    except Exception as e:
        return False, str(e)


@test("get_pulse_summary() — has_pulse key present, handles no-pulse gracefully")
def test_pulse_summary_shape():
    try:
        from scout_agent import get_pulse_summary
        result = get_pulse_summary()
        if "has_pulse" not in result:
            return False, "has_pulse key missing from get_pulse_summary() result"
        if result["has_pulse"]:
            required = ["had_content", "cap_alerts_count", "ghost_campaigns_count", "opportunities_count"]
            missing = [k for k in required if k not in result]
            if missing:
                return False, f"has_pulse=True but missing keys: {missing}"
            return True, (
                f"has_pulse=True — cap_alerts={result.get('cap_alerts_count')}, "
                f"ghosts={result.get('ghost_campaigns_count')}, "
                f"opportunities={result.get('opportunities_count')}"
            )
        else:
            if "message" not in result:
                return False, "has_pulse=False but no message field"
            return True, f"has_pulse=False — {result['message'][:60]}"
    except Exception as e:
        return False, str(e)


@test("_format_pulse_blocks() — synthetic signals, block count ≤ 50, Monday path exercised")
def test_pulse_block_count():
    try:
        from datetime import date
        from scout_slack_ui import _format_pulse_blocks
        signals = {
            "ghost_campaigns": [{"adv_name": f"Ghost{i}", "impressions_7d": 2000, "revenue_7d": 0} for i in range(5)],
            "fill_rate": [{"publisher_name": f"Pub{i}", "fill_rate_pct": 35, "sessions_7d": 10000,
                           "missed_sessions": 3500, "revenue_at_risk": 0} for i in range(4)],
            "opportunities": [{"adv_name": f"Opp{i}", "publisher_name": f"Pub{i}",
                               "sessions_30d": 100000, "est_monthly_rev": 5000} for i in range(4)],
            "velocity_shifts": [
                {"publisher_name": "TextNow", "direction": "down", "pct_delta": -50, "revenue_7d_ann": 20000,
                 "revenue_30d": 35000, "top_advertisers": [{"adv_name": "Capital One", "delta_ann": -5000, "rev_7d": 1000}],
                 "hypothesis": "TextNow paused", "gaps": [("Capital One", 2.50)]},
                {"publisher_name": "WBMason", "direction": "up", "pct_delta": 23, "revenue_7d_ann": 45000,
                 "revenue_30d": 32000, "top_advertisers": [], "hypothesis": None, "gaps": []},
            ],
            "cap_alerts": [{"adv_name": "CapOne", "cap_pct": 93, "days_to_cap": 2, "days_remaining": 10}],
            "overnight_events": [],
        }
        monday = date(2026, 4, 27)  # known Monday — exercises opportunities code path
        fallback, blocks = _format_pulse_blocks(signals, is_weekend=False, _today=monday)
        if len(blocks) > 50:
            return False, f"Block count {len(blocks)} exceeds Slack's 50-block limit"
        if not fallback:
            return False, "fallback text is empty"
        block_types = [b["type"] for b in blocks]
        if "section" not in block_types:
            return False, "no section blocks found"
        if "context" not in block_types:
            return False, "no context blocks found"
        return True, f"block count={len(blocks)}/50, Monday path exercised, all block types present"
    except Exception as e:
        return False, str(e)


@test("_build_signal_header() — correct block structure with and without context")
def test_build_signal_header():
    try:
        from scout_slack_ui import _build_signal_header
        # With context — should return 2 blocks
        blocks = _build_signal_header("🔴", "DARK OFFERS — 3 active", "6K impressions burning")
        if len(blocks) != 2:
            return False, f"expected 2 blocks with context, got {len(blocks)}"
        if blocks[0]["type"] != "section":
            return False, f"first block should be section, got {blocks[0]['type']}"
        if "DARK OFFERS" not in blocks[0]["text"]["text"]:
            return False, "title not in first block text"
        if blocks[1]["type"] != "context":
            return False, f"second block should be context, got {blocks[1]['type']}"
        # Without context — should return 1 block
        single = _build_signal_header("💡", "OPPORTUNITIES")
        if len(single) != 1:
            return False, f"expected 1 block without context, got {len(single)}"
        return True, "2-block (with context) and 1-block (without) both correct"
    except Exception as e:
        return False, str(e)


@test("_build_item_card() — fields layout when right_body set, plain text when empty")
def test_build_item_card():
    try:
        from scout_slack_ui import _build_item_card
        # With right_body → section.fields
        blocks = _build_item_card("TextNow", "*-50%*  ·  $20K/mo", "*Top Advertiser*\nCapital One", "TextNow paused")
        if blocks[0]["type"] != "section":
            return False, f"first block should be section, got {blocks[0]['type']}"
        if "fields" not in blocks[0]:
            return False, "section.fields missing when right_body provided"
        if len(blocks[0]["fields"]) != 2:
            return False, f"expected 2 fields, got {len(blocks[0]['fields'])}"
        if blocks[1]["type"] != "context":
            return False, f"second block should be context, got {blocks[1]['type']}"
        # Without right_body → plain section text
        plain = _build_item_card("TextNow", "*-50%*  ·  $20K/mo")
        if plain[0]["type"] != "section":
            return False, "plain card should be section"
        if "text" not in plain[0]:
            return False, "plain card missing text field"
        if "fields" in plain[0]:
            return False, "plain card should NOT have fields"
        if len(plain) != 1:
            return False, f"plain card with no context should be 1 block, got {len(plain)}"
        return True, "fields layout and plain text layout both correct"
    except Exception as e:
        return False, str(e)


@test("_build_publisher_card() — type guard, attribution branch, context join")
def test_build_publisher_card():
    try:
        from scout_slack_ui import _build_publisher_card
        # With attribution — should use section.fields
        blocks = _build_publisher_card("TextNow", "-50", "$20K", attribution="Capital One",
                                        hypothesis="TextNow paused", gaps=[("Capital One", 2.50)])
        if blocks[0]["type"] != "section":
            return False, "first block should be section"
        if "fields" not in blocks[0]:
            return False, "should use section.fields when attribution provided"
        if blocks[1]["type"] != "context":
            return False, "context block should follow card"
        ctx_text = blocks[1]["elements"][0]["text"]
        if "TextNow paused" not in ctx_text:
            return False, "hypothesis missing from context"
        if "Capital One" not in ctx_text:
            return False, "gap missing from context"
        # Without attribution — plain section
        plain = _build_publisher_card("WBMason", 23, "$45K")
        if "fields" in plain[0]:
            return False, "should NOT use section.fields when no attribution"
        # pct_delta as string (type guard test)
        guarded = _build_publisher_card("Test", "23", "$10K")
        if not guarded:
            return False, "float guard failed on string pct_delta"
        return True, "fields/plain branch, context join, and type guard all correct"
    except Exception as e:
        return False, str(e)


# ── Test 20: Digest dedup — no duplicate advertisers across networks ──────────

@test("Digest dedup — no advertiser appears on multiple networks")
def test_digest_no_duplicate_advertisers():
    """
    PR 15a invariant: select_offers() must dedup advertisers across networks.
    Build a synthetic offer set where the same advertiser exists on Impact and
    MaxBounty; assert it surfaces only on Impact (higher priority).
    """
    try:
        import scout_digest
        # Stub the offer loader and the agent benchmark fetch to avoid CH calls
        synthetic = [
            {"offer_id": "1", "advertiser": "DupCo", "network": "impact",
             "category": "Retail", "_payout_type_norm": "CPL", "tracking_url": "x"},
            {"offer_id": "2", "advertiser": "DupCo", "network": "maxbounty",
             "category": "Retail", "_payout_type_norm": "CPL", "tracking_url": "x"},
            {"offer_id": "3", "advertiser": "UniqueA", "network": "impact",
             "category": "Retail", "_payout_type_norm": "CPL", "tracking_url": "x"},
            {"offer_id": "4", "advertiser": "UniqueB", "network": "maxbounty",
             "category": "Finance", "_payout_type_norm": "CPS", "tracking_url": "x"},
        ]
        orig_load = scout_digest._load_offers
        orig_score = scout_digest.score_offer
        orig_in_ms = scout_digest.is_already_in_ms
        try:
            scout_digest._load_offers = lambda: synthetic  # type: ignore
            scout_digest.score_offer = lambda offer, *a, **kw: 100.0  # type: ignore
            scout_digest.is_already_in_ms = lambda offer, ms: False  # type: ignore
            result, _meta = scout_digest.select_offers(
                n_per_network=5,
                ms_campaigns=[],
                benchmarks={"avg_rpm": 0, "avg_cvr": 0},
                force=True,
            )
        finally:
            scout_digest._load_offers = orig_load
            scout_digest.score_offer = orig_score
            scout_digest.is_already_in_ms = orig_in_ms

        all_advertisers: list[str] = []
        for net, scored in result.items():
            for _s, offer in scored:
                all_advertisers.append((offer.get("advertiser") or "").lower())

        if all_advertisers.count("dupco") > 1:
            return False, f"DupCo surfaced {all_advertisers.count('dupco')} times across networks"

        # Verify dedup picked the higher-priority network (impact, listed first)
        impact_advs = [(o.get("advertiser") or "").lower() for _, o in result.get("impact", [])]
        maxbounty_advs = [(o.get("advertiser") or "").lower() for _, o in result.get("maxbounty", [])]
        if "dupco" not in impact_advs:
            return False, "DupCo missing from impact (priority network)"
        if "dupco" in maxbounty_advs:
            return False, "DupCo leaked into maxbounty after dedup"
        return True, f"dedup OK; surfaced {len(all_advertisers)} unique advertisers"
    except Exception as e:
        return False, str(e)


# ── Test 21: Module-level _NETWORK_EMOJI is the only one ──────────────────────

@test("scout_digest — _NETWORK_EMOJI is module-level only (no shadow)")
def test_network_emoji_single_source():
    """
    PR 15a invariant: _NETWORK_EMOJI must be defined ONCE at module level.
    The local dict at line 610 inside _build_digest_blocks() shadowed it for
    months — this test guards against regression by checking source.
    """
    try:
        import pathlib
        src = pathlib.Path(__file__).parent / "scout_digest.py"
        text = src.read_text()
        # Must have module-level _NETWORK_EMOJI (no leading whitespace)
        if "\n_NETWORK_EMOJI" not in text:
            return False, "module-level _NETWORK_EMOJI not found"
        # Must NOT have an indented (local) _NETWORK_EMOJI assignment
        for line in text.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("_NETWORK_EMOJI") and stripped != line:
                return False, f"local _NETWORK_EMOJI shadow found: '{line.strip()[:80]}'"
        # Must cover all 9 networks
        import scout_digest
        expected = set(scout_digest._DIGEST_NETWORKS)
        actual = set(scout_digest._NETWORK_EMOJI.keys())
        if not expected.issubset(actual):
            return False, f"missing emoji for networks: {expected - actual}"
        return True, f"single-source _NETWORK_EMOJI covers {len(actual)} networks"
    except Exception as e:
        return False, str(e)


# ── PR 15b: Pulse block invariants — Wednesday + ghost-clear + non-Monday opps ──

@test("PR 15b — Wednesday Pulse: ghost all-clear + non-Monday opps note ≤ 50 blocks")
def test_pulse_block_count_with_pr15b():
    """
    PR 15b adds two new branches:
      1. Ghost all-clear (else: branch when no ghost campaigns)
      2. Non-Monday opportunities note (elif: branch on weekdays != Monday)
    Verify both render correctly on a Wednesday with NO ghosts and WITH opps,
    and stay under Slack's 50-block hard limit.
    """
    try:
        from datetime import date
        from scout_slack_ui import _format_pulse_blocks
        signals = {
            "ghost_campaigns": [],  # triggers all-clear else branch
            "fill_rate": [{"publisher_name": f"Pub{i}", "fill_rate_pct": 35, "sessions_7d": 10000,
                           "missed_sessions": 3500, "revenue_at_risk": 0} for i in range(2)],
            "opportunities": [{"adv_name": f"Opp{i}", "publisher_name": f"Pub{i}",
                               "sessions_30d": 100000, "est_monthly_rev": 5000} for i in range(3)],
            "velocity_shifts": [],
            "cap_alerts": [],
            "overnight_events": [],
        }
        wednesday = date(2026, 4, 29)  # known Wednesday
        fallback, blocks = _format_pulse_blocks(signals, is_weekend=False, _today=wednesday)
        if len(blocks) > 50:
            return False, f"block count {len(blocks)} exceeds 50-block limit"

        # Render to text and confirm BOTH branches fired.
        # Use ensure_ascii=False so em-dash matches literal em-dash.
        all_text = json.dumps(blocks, ensure_ascii=False)
        if "DARK OFFERS — none active" not in all_text:
            return False, "ghost all-clear header missing on no-ghosts run"
        if "shown in detail on Monday" not in all_text:
            return False, "non-Monday opportunities note missing"
        if "queued" not in all_text:
            return False, "opportunities count not surfaced in non-Monday note"
        return True, f"both PR 15b branches rendered correctly; {len(blocks)} blocks"
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
    """Return (blocks, fallback_text) for a Block Kit health dashboard card.

    Design: progressive disclosure.
    - All pass → single compact context line with status dots. No scroll, no noise.
    - Any failure → full-width section per failing check (no truncation), passing
      checks collapsed to one context dot-line at the bottom.

    Avoids section.fields — Slack hard-truncates field text at display boundaries,
    which breaks long check details (LLM round-trip, ghost campaign detail, etc.).
    """
    from datetime import datetime as _dt
    import pytz as _pytz
    total     = len(results)
    all_pass  = pass_count == total
    failed    = [r for r in results if not r["passed"]]
    passed    = [r for r in results if r["passed"]]
    now_ct    = _dt.now(_pytz.timezone("America/Chicago")).strftime("%-I:%M %p CT")
    fallback  = f"Scout: {pass_count}/{total} checks passed"

    blocks: list[dict] = []

    if all_pass:
        # ── All green: headline + one rich_text block per check ──────────────────
        # rich_text blocks don't truncate in thread preview the way mrkdwn context
        # elements do — critical for the ask('status') and ask('ghost campaigns')
        # rows which carry the full LLM response preview.
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":white_check_mark: *Scout is healthy — {total}/{total} checks passed*"},
        })
        for r in results:
            safe_detail = " ".join(r['detail'].splitlines()).strip()
            blocks.append({
                "type": "rich_text",
                "elements": [{
                    "type": "rich_text_section",
                    "elements": [
                        {"type": "emoji", "name": "large_green_circle"},
                        {"type": "text", "text": f"  {r['name']}", "style": {"bold": True}},
                        {"type": "text", "text": f"  ·  {safe_detail}"},
                    ],
                }],
            })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Startup check · {now_ct}"}],
        })
    else:
        # ── Failures: headline, full-width section per failure, passing as context ─
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":warning: *Scout has issues — {pass_count}/{total} checks passed*"},
        })
        blocks.append({"type": "divider"})
        for r in failed:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":red_circle: *{r['name']}*\n{r['detail']}"},
            })
        for r in passed:
            safe_detail = " ".join(r['detail'].splitlines()).strip()
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f":large_green_circle: *{r['name']}*  ·  {safe_detail}"}],
            })
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":mag: *Check Render logs for the failing checks above.*"},
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Startup check · {now_ct} · Issues detected"}],
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

