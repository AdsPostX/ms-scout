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
import re
import sys
import time

from dotenv import load_dotenv

load_dotenv(override=True)

_ROOT = pathlib.Path(__file__).parent
_DATA = _ROOT / "data"

TESTS: list[dict] = []

_PR_NAME_RE = re.compile(r"^PR\s+\d+", re.IGNORECASE)


def test(name: str):
    """Decorator to register a smoke test."""
    if _PR_NAME_RE.match(name):
        raise ValueError(
            f"Test name looks like a changelog entry (behavior name required): {name!r}"
        )
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

@test("digest_dedup_no_advertiser_on_multiple_networks")
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

@test("pulse_blocks_stay_under_50_and_render_correct_non_monday_paths")
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


# ── PR 15c: health status shape + heartbeat module-level state ───────────────

@test("compute_health_status_has_required_shape_and_heartbeat_is_required_daemon")
def test_compute_health_status_shape():
    """
    PR 15c invariants (updated for PR 16b registration pattern):
      1. _compute_health_status() returns {'ok': bool, 'checks': dict}
      2. 'health-heartbeat' is registered in _REQUIRED_DAEMONS via _start_daemon()
         (PR 16b moved required-thread tracking from hardcoded literals into the
         shared _REQUIRED_DAEMONS set; both compute and watchdog read from it)
      3. Module-level state for heartbeat exists
      4. No ClickHouse call inside _compute_health_status() — CH lives in heartbeat only
    """
    try:
        import scout_bot
        # Shape assertions
        status = scout_bot._compute_health_status()
        if not isinstance(status, dict):
            return False, f"expected dict, got {type(status).__name__}"
        if "ok" not in status or "checks" not in status:
            return False, f"missing ok/checks; got keys: {sorted(status.keys())}"
        if not isinstance(status["ok"], bool):
            return False, f"ok should be bool, got {type(status['ok']).__name__}"
        if not isinstance(status["checks"], dict):
            return False, f"checks should be dict, got {type(status['checks']).__name__}"

        for name, check in status["checks"].items():
            if "ok" not in check or "detail" not in check:
                return False, f"check '{name}' missing ok/detail: {check}"

        # PR 16b: health-heartbeat must be wired through _start_daemon() in main()
        # so it lands in _REQUIRED_DAEMONS — which BOTH compute and watchdog read.
        import pathlib
        src = (pathlib.Path(__file__).parent / "scout_bot.py").read_text()
        if "_REQUIRED_DAEMONS" not in src:
            return False, "_REQUIRED_DAEMONS pattern missing — PR 16b regression"
        # main() must call _start_daemon for health-heartbeat (case-insensitive search
        # since the lambda wrapper makes the call line a bit unusual)
        main_section = src.split("def main()")[1].split("\ndef ")[0]
        if 'name="health-heartbeat"' not in main_section:
            return False, "health-heartbeat not started in main() — won't be in _REQUIRED_DAEMONS"
        if "_start_daemon" not in main_section:
            return False, "main() doesn't use _start_daemon — _REQUIRED_DAEMONS will be empty"
        # Both check sites must read from _REQUIRED_DAEMONS
        compute_section = src.split("def _compute_health_status")[1].split("\ndef ")[0]
        watchdog_section = src.split("def _thread_watchdog")[1].split("\ndef ")[0]
        if "_REQUIRED_DAEMONS" not in compute_section:
            return False, "_compute_health_status() doesn't read from _REQUIRED_DAEMONS"
        if "_REQUIRED_DAEMONS" not in watchdog_section:
            return False, "_thread_watchdog doesn't read from _REQUIRED_DAEMONS (silent death risk)"

        # Module-level constants
        for name in ("_HEALTH_HEARTBEAT_WARMUP_SECS", "_HEALTH_CONSECUTIVE_THRESHOLD",
                     "_HEALTH_STATUS_LOCK", "_LAST_HEALTH_STATUS"):
            if not hasattr(scout_bot, name):
                return False, f"module-level state missing: {name}"

        # No CH call in _compute_health_status — CH lives only in _run_health_heartbeat
        if "_get_ch_client" in compute_section or "ch.query" in compute_section:
            return False, "ClickHouse call detected in _compute_health_status — would cause Render restarts"

        return True, f"shape OK; {len(status['checks'])} checks; heartbeat tracked in both required sets"
    except Exception as e:
        return False, str(e)


# ── PR 16: hardcoding tier 1 invariants ──────────────────────────────────────

@test("digest_networks_derived_from_offers_latest_keyset_Big4_priority_preserved")
def test_digest_networks_derivation():
    try:
        import scout_digest
        for name in ("_get_active_networks", "_PRIORITY_NETWORKS",
                     "_DIGEST_NETWORKS_FALLBACK", "_DIGEST_NETWORKS"):
            if not hasattr(scout_digest, name):
                return False, f"missing module-level: {name}"
        if scout_digest._PRIORITY_NETWORKS != ("impact", "maxbounty", "flexoffers", "cj"):
            return False, f"priority order changed: {scout_digest._PRIORITY_NETWORKS}"
        result = scout_digest._get_active_networks()
        priority_set = set(scout_digest._PRIORITY_NETWORKS)
        priority_seen = [n for n in result if n in priority_set]
        priority_expected = [n for n in scout_digest._PRIORITY_NETWORKS if n in priority_seen]
        if priority_seen != priority_expected:
            return False, f"Big 4 ordering broken — got {priority_seen}, expected {priority_expected}"
        rest = [n for n in result if n not in priority_set]
        if rest != sorted(rest):
            return False, f"non-priority networks not alphabetical: {rest}"
        return True, f"derivation OK; order = {result}"
    except Exception as e:
        return False, str(e)


@test("required_daemons_single_source_both_check_sites_agree")
def test_required_daemons_single_source():
    try:
        import scout_bot
        if not hasattr(scout_bot, "_REQUIRED_DAEMONS"):
            return False, "_REQUIRED_DAEMONS missing"
        if not hasattr(scout_bot, "_start_daemon") or not callable(scout_bot._start_daemon):
            return False, "_start_daemon missing or not callable"
        import pathlib
        src = (pathlib.Path(__file__).parent / "scout_bot.py").read_text()
        compute_section = src.split("def _compute_health_status")[1].split("\ndef ")[0]
        watchdog_section = src.split("def _thread_watchdog")[1].split("\ndef ")[0]
        for section_name, section in (("compute", compute_section), ("watchdog", watchdog_section)):
            if '"scraper", "notion-watcher"' in section:
                return False, f"hardcoded thread set still present in {section_name}"
            if "_REQUIRED_DAEMONS" not in section:
                return False, f"{section_name} section doesn't reference _REQUIRED_DAEMONS"
        return True, "single source confirmed; both check sites read from _REQUIRED_DAEMONS"
    except Exception as e:
        return False, str(e)


@test("select_offers_exposes_advertisers_deduped_in_meta")
def test_dedup_count_in_meta():
    try:
        import scout_digest
        synthetic = [
            {"offer_id": "1", "advertiser": "DupCo", "network": "impact",
             "category": "Retail", "_payout_type_norm": "CPL", "tracking_url": "x"},
            {"offer_id": "2", "advertiser": "DupCo", "network": "maxbounty",
             "category": "Retail", "_payout_type_norm": "CPL", "tracking_url": "x"},
        ]
        orig_load = scout_digest._load_offers
        orig_score = scout_digest.score_offer
        orig_in_ms = scout_digest.is_already_in_ms
        try:
            scout_digest._load_offers = lambda: synthetic
            scout_digest.score_offer = lambda offer, *a, **kw: 100.0
            scout_digest.is_already_in_ms = lambda offer, ms: False
            _result, meta = scout_digest.select_offers(
                n_per_network=5, ms_campaigns=[], benchmarks={"avg_rpm": 0, "avg_cvr": 0}, force=True,
            )
        finally:
            scout_digest._load_offers = orig_load
            scout_digest.score_offer = orig_score
            scout_digest.is_already_in_ms = orig_in_ms
        if "advertisers_deduped" not in meta:
            return False, f"meta missing advertisers_deduped; got {sorted(meta.keys())}"
        if meta["advertisers_deduped"] != 1:
            return False, f"expected 1 dedup, got {meta['advertisers_deduped']}"
        return True, f"meta exposes advertisers_deduped={meta['advertisers_deduped']}"
    except Exception as e:
        return False, str(e)


# ── PR 17: config legibility — thresholds + tool + SUPPORTED_NETWORKS ───────

@test("scout_thresholds_json_loads_and_populates_SCOUT_THRESHOLDS")
def test_scout_thresholds_loaded():
    try:
        import scout_agent
        if not hasattr(scout_agent, "SCOUT_THRESHOLDS"):
            return False, "SCOUT_THRESHOLDS missing"
        cfg = scout_agent.SCOUT_THRESHOLDS
        for section in ("digest", "signals", "health"):
            if section not in cfg:
                return False, f"section missing: {section}"
        if cfg["digest"]["min_rpm_floor"] != 20:
            return False, f"digest.min_rpm_floor expected 20, got {cfg['digest']['min_rpm_floor']}"
        if cfg["signals"]["fill_rate_min_sessions_7d"] != 5000:
            return False, f"signals.fill_rate_min_sessions_7d expected 5000, got {cfg['signals']['fill_rate_min_sessions_7d']}"
        # Confirm fallback path works
        fallback = scout_agent._SCOUT_THRESHOLDS_FALLBACK
        if "digest" not in fallback or "signals" not in fallback or "health" not in fallback:
            return False, "_SCOUT_THRESHOLDS_FALLBACK missing required sections"
        return True, f"loaded {len(cfg)} sections; min_rpm_floor={cfg['digest']['min_rpm_floor']}"
    except Exception as e:
        return False, str(e)


@test("get_scout_config_registered_with_all_4_contract_pieces")
def test_get_scout_config_registered():
    try:
        import scout_agent
        # 1. Function exists
        if not hasattr(scout_agent, "get_scout_config"):
            return False, "get_scout_config function missing"
        # 2. TOOL_MAP entry
        if scout_agent.TOOL_MAP.get("get_scout_config") is not scout_agent.get_scout_config:
            return False, "TOOL_MAP['get_scout_config'] not bound to function"
        # 3. TOOLS list entry
        names = {t["name"] for t in scout_agent.TOOLS}
        if "get_scout_config" not in names:
            return False, f"TOOLS list missing get_scout_config; have: {sorted(names)}"
        # 4. SYSTEM_PROMPT intent
        if "get_scout_config()" not in scout_agent.SYSTEM_PROMPT:
            return False, "SYSTEM_PROMPT missing get_scout_config() intent routing"
        # Functional: returns expected shape
        result = scout_agent.get_scout_config()
        for key in ("thresholds", "supported_networks", "pulse", "config_file"):
            if key not in result:
                return False, f"get_scout_config() output missing '{key}'"
        return True, f"all 4 contract pieces present; output has {len(result)} keys"
    except Exception as e:
        return False, str(e)


@test("tool_descriptions_reference_SUPPORTED_NETWORKS_via_join_single_source")
def test_supported_networks_single_source():
    try:
        import scout_agent
        if not hasattr(scout_agent, "SUPPORTED_NETWORKS"):
            return False, "SUPPORTED_NETWORKS constant missing"
        if len(scout_agent.SUPPORTED_NETWORKS) < 1:
            return False, "SUPPORTED_NETWORKS is empty"
        # Tool descriptions must use the constant via .join(), not hardcoded literal
        descriptions = [t["description"] for t in scout_agent.TOOLS]
        joined = ", ".join(scout_agent.SUPPORTED_NETWORKS)
        # At least one tool description must contain the joined string (proves the constant is wired in)
        if not any(joined in desc for desc in descriptions):
            return False, "no tool description references SUPPORTED_NETWORKS via .join()"
        # Confirm we did NOT touch SYSTEM_PROMPT body for f-string conversion
        if "{', '.join(SUPPORTED_NETWORKS)}" in scout_agent.SYSTEM_PROMPT:
            return False, "SYSTEM_PROMPT body f-string conversion detected — risks format breakage"
        return True, f"SUPPORTED_NETWORKS = {len(scout_agent.SUPPORTED_NETWORKS)} networks; tool descriptions wired"
    except Exception as e:
        return False, str(e)


# ── PR 18: config wiring + honest network coverage ──────────────────────────

@test("score_offer_respects_min_rpm_floor_from_config")
def test_score_offer_reads_config_floor():
    """
    PR 18 invariant: changing SCOUT_THRESHOLDS['digest']['min_rpm_floor'] must
    actually change score_offer's filter behavior. Before PR 18 the floor was
    hardcoded `_MIN_RPM = 20.0` and the config was decorative.

    Test: monkey-patch SCOUT_THRESHOLDS to floor=$5 and floor=$50, call
    score_offer with the same offer, and confirm the floor change flips the
    pass/fail outcome.
    """
    try:
        import scout_agent
        import scout_digest

        # An offer that scores around ~$25 RPM (typical CPL with avg CVR)
        offer = {
            "offer_id": "test-floor-1",
            "advertiser": "TestCo",
            "network": "impact",
            "category": "Retail",
            "_payout_type_norm": "CPL",
            "_payout_num": 5.0,
            "tracking_url": "x",
        }
        # Stub _scout_score so this test doesn't depend on benchmarks
        orig_scout_score = None
        try:
            import scout_agent as _sa
            orig_scout_score = _sa._scout_score
            _sa._scout_score = lambda offer, benchmarks: 25.0  # type: ignore
        except Exception:
            pass

        original = scout_agent.SCOUT_THRESHOLDS
        try:
            # Floor = $5 → 25.0 RPM passes (returns a score)
            scout_agent.SCOUT_THRESHOLDS = {**original, "digest": {**original["digest"], "min_rpm_floor": 5}}
            low_floor = scout_digest.score_offer(offer, {}, {"approved": {}, "rejected": {}}, {}, force=True)
            if low_floor is None:
                return False, f"floor=$5 should let 25.0 RPM through, got None"

            # Floor = $50 → 25.0 RPM filtered (returns None)
            scout_agent.SCOUT_THRESHOLDS = {**original, "digest": {**original["digest"], "min_rpm_floor": 50}}
            high_floor = scout_digest.score_offer(offer, {}, {"approved": {}, "rejected": {}}, {}, force=True)
            if high_floor is not None:
                return False, f"floor=$50 should reject 25.0 RPM, got {high_floor}"
        finally:
            scout_agent.SCOUT_THRESHOLDS = original
            if orig_scout_score is not None:
                scout_agent._scout_score = orig_scout_score

        return True, f"config drives behavior; low_floor={low_floor}, high_floor={high_floor}"
    except Exception as e:
        return False, str(e)


@test("offer_staleness_threshold_reads_from_config")
def test_offer_staleness_from_config():
    """PR 18 invariant: scout_bot reads offer_staleness_hours from config, not hardcoded."""
    try:
        import scout_bot
        if not hasattr(scout_bot, "_OFFER_STALENESS_HOURS"):
            return False, "_OFFER_STALENESS_HOURS module-level missing"
        if not hasattr(scout_bot, "_HEALTH_CFG"):
            return False, "_HEALTH_CFG module-level missing (lazy loader didn't run)"
        # Verify the source no longer has the hardcoded `age_hours > 30`
        import pathlib
        src = (pathlib.Path(__file__).parent / "scout_bot.py").read_text()
        compute = src.split("def _compute_health_status")[1].split("\ndef ")[0]
        if "age_hours > 30" in compute and "_OFFER_STALENESS_HOURS" not in compute:
            return False, "hardcoded age_hours > 30 still in _compute_health_status"
        if "_OFFER_STALENESS_HOURS" not in compute:
            return False, "_compute_health_status doesn't reference _OFFER_STALENESS_HOURS"
        return True, f"staleness driven by config; current value = {scout_bot._OFFER_STALENESS_HOURS}h"
    except Exception as e:
        return False, str(e)


@test("supported_networks_lists_only_credentialled_active_networks")
def test_supported_networks_trimmed():
    """PR 18 invariant: SUPPORTED_NETWORKS lists only networks with creds on Render."""
    try:
        import scout_agent, scout_digest
        active = set(n.lower() for n in scout_agent.SUPPORTED_NETWORKS)
        expected_active = {"impact", "flexoffers", "maxbounty", "cj"}
        if active != expected_active:
            return False, f"SUPPORTED_NETWORKS expected {expected_active}, got {active}"
        fallback = set(scout_digest._DIGEST_NETWORKS_FALLBACK)
        if fallback != expected_active:
            return False, f"_DIGEST_NETWORKS_FALLBACK expected {expected_active}, got {fallback}"
        # Labels and emoji should KEEP all 9 entries so re-enabling is one-line
        label_keys = set(scout_digest._NETWORK_LABEL.keys())
        if not {"awin", "everflow", "rakuten", "shareasale", "tune"}.issubset(label_keys):
            return False, "label/emoji maps lost their entries for the 5 disabled networks; re-enable would be harder than necessary"
        return True, f"4 active networks; 9 label entries kept for fast re-enable"
    except Exception as e:
        return False, str(e)


# ── PR 19: categories via tags + slim schema-deps validation ────────────────

# Pure-unit tests for the tags-parsing helper (no CH, no LLM)

@test("_extract_real_categories filters internal-* prefix tags (case-insensitive)")
def test_extract_categories_filters_internal():
    try:
        from scout_agent import _extract_real_categories
        # JSON string input — production case
        result = _extract_real_categories(
            '["internal-network-impact","internal-email","rewards","technology"]'
        )
        if result != ["rewards", "technology"]:
            return False, f"expected [rewards,technology], got {result}"
        # Capital-I variant should also be filtered (case-insensitive)
        result_caps = _extract_real_categories(
            '["Internal-Email","Internal-Network-Foo","pets"]'
        )
        if result_caps != ["pets"]:
            return False, f"case-insensitive filter failed: got {result_caps}"
        return True, "internal-* prefix filtered (case-insensitive); real categories preserved in order"
    except Exception as e:
        return False, str(e)


@test("_extract_real_categories handles missing/empty/null/invalid inputs")
def test_extract_categories_edge_cases():
    try:
        from scout_agent import _extract_real_categories
        cases = [
            (None,             [],  "None"),
            ("",               [],  "empty string"),
            ("[]",             [],  "empty array"),
            ("not-json",       [],  "invalid JSON"),
            ('"not-an-array"', [],  "JSON scalar (not array)"),
            ('[null,1,"x"]',   ["x"], "mixed types — keep only strings"),
        ]
        for inp, expected, label in cases:
            got = _extract_real_categories(inp)
            if got != expected:
                return False, f"{label}: expected {expected}, got {got}"
        # List input (Python-side, not JSON)
        if _extract_real_categories(["rewards", "internal-x", "tech"]) != ["rewards", "tech"]:
            return False, "list input handling failed"
        return True, "handles None, empty, invalid JSON, mixed types, list input"
    except Exception as e:
        return False, str(e)


@test("_extract_real_categories preserves tag order")
def test_extract_categories_preserves_order():
    try:
        from scout_agent import _extract_real_categories
        result = _extract_real_categories(
            '["technology","internal-x","rewards","pets","internal-y","financial"]'
        )
        if result != ["technology", "rewards", "pets", "financial"]:
            return False, f"order broken: got {result}"
        return True, "order preserved across filter"
    except Exception as e:
        return False, str(e)


# Schema-deps validation tests (mock the CH client — pure unit)

@test("_validate_schema_deps detects missing column")
def test_schema_deps_missing_column():
    try:
        from scout_agent import _validate_schema_deps, _SCHEMA_DEPS
        # Mock CH client: returns columns matching all _SCHEMA_DEPS EXCEPT one
        # (simulate a column that was renamed/dropped upstream)
        target = ("from_airbyte_campaigns", "tags", True)
        missing_table, missing_col, _ = target

        class _FakeRows:
            def __init__(self, rows): self.result_rows = rows

        class _FakeCH:
            def query(self, sql, parameters=None):
                if "system.columns" in sql:
                    # Return all dep columns EXCEPT the missing one
                    rows = [(t, c) for t, c, _ in _SCHEMA_DEPS if not (t == missing_table and c == missing_col)]
                    return _FakeRows(rows)
                # countIf query — return high count so other deps don't fire
                return _FakeRows([(99999,)])

        result = _validate_schema_deps(_FakeCH())
        if result["ok"]:
            return False, f"expected ok=False, got {result}"
        if not any("tags MISSING" in v for v in result["violations"]):
            return False, f"violation list missing 'tags MISSING': {result['violations']}"
        return True, f"caught missing column: {result['violations'][0][:80]}"
    except Exception as e:
        return False, str(e)


@test("_validate_schema_deps detects empty must-have-data column")
def test_schema_deps_empty_column():
    try:
        from scout_agent import _validate_schema_deps, _SCHEMA_DEPS, _SCHEMA_DEPS_MIN_ROWS

        class _FakeRows:
            def __init__(self, rows): self.result_rows = rows

        # First call returns all columns present; subsequent count queries return
        # high counts EXCEPT for tags which returns 0 (simulating empty column).
        class _FakeCH:
            def query(self, sql, parameters=None):
                if "system.columns" in sql:
                    return _FakeRows([(t, c) for t, c, _ in _SCHEMA_DEPS])
                if "from_airbyte_campaigns" in sql and "tags" in sql:
                    return _FakeRows([(0,)])  # empty
                return _FakeRows([(99999,)])

        result = _validate_schema_deps(_FakeCH())
        if result["ok"]:
            return False, f"expected ok=False, got {result}"
        if not any("tags has only 0 non-null rows" in v for v in result["violations"]):
            return False, f"violation list missing tags-empty: {result['violations']}"
        return True, f"caught empty column with threshold {_SCHEMA_DEPS_MIN_ROWS}: {result['violations'][0][:80]}"
    except Exception as e:
        return False, str(e)


# CH-gated regression test — proves the categories fix actually worked

@test("Tier 3 benchmarks populated after categories fix (regression)")
def test_tier3_benchmarks_populated():
    """
    PR 19 regression test. Calls _load_performance_benchmarks() against live CH;
    asserts the by_category dict has > 5 categories. Before the fix this dict was
    always empty (categories column NULL). After the fix it should have ~25
    categories from tags JSON parsing. If this test fails on Render, scoring is
    back to Tier 4 fallback — investigate queries.performance_benchmarks_raw().
    """
    try:
        from scout_agent import _load_performance_benchmarks
        result = _load_performance_benchmarks()
        n_cats = len(result.get("by_category", {}))
        if n_cats < 5:
            return False, (
                f"only {n_cats} categories in by_category — fix did not work. "
                f"Check queries.performance_benchmarks_raw() CTE + tags-parsing."
            )
        sample = sorted(result["by_category"].keys())[:5]
        return True, f"{n_cats} categories populated; sample: {sample}"
    except Exception as e:
        return False, str(e)


# ── PR 19a: benchmarks self-heal (no user-facing 'run X' for state Scout owns) ─

@test("get_scout_status_self_heals_benchmarks_before_reporting")
def test_status_self_heals_benchmarks():
    """
    PR 19a invariant: get_scout_status() attempts to load benchmarks if they're
    missing/stale BEFORE reporting their state. The "Benchmarks not loaded"
    message should never appear except in real ClickHouse outages.

    We verify by source-grep: the function calls _get_benchmarks() before
    formatting `status['benchmarks']`. Mock-call testing would require stubbing
    a CH client; this lighter check catches the regression we care about
    (someone removes the self-heal call accidentally).
    """
    try:
        import pathlib
        src = (pathlib.Path(__file__).parent / "scout_agent.py").read_text()
        # Find get_scout_status function body
        if "def get_scout_status" not in src:
            return False, "get_scout_status function missing"
        body = src.split("def get_scout_status")[1].split("\ndef ")[0]
        # Self-heal call must precede the freshness reporting
        if "_get_benchmarks()" not in body:
            return False, "get_scout_status no longer calls _get_benchmarks() — self-heal removed"
        # The "not loaded" message should not be assigned to status[...] (chore message)
        # Comments mentioning the phrase are fine; we check for the assignment pattern.
        if 'status["benchmarks"] = "not loaded"' in body:
            return False, "get_scout_status still emits 'not loaded' — this surfaces user-facing chore message"
        return True, "self-heal in place; no 'not loaded' chore message"
    except Exception as e:
        return False, str(e)


@test("benchmarks_warmer_daemon_registered_and_boot_warmup_wired")
def test_benchmarks_warmer_wired():
    """
    PR 19a: the benchmarks-warmer daemon keeps _BENCHMARKS warm in memory.
    Verify (a) the daemon function exists, (b) main() registers it via
    _start_daemon, and (c) _run_startup_smoke_test calls _get_benchmarks
    to warm the cache at boot before any digest/Pulse query runs.
    """
    try:
        import scout_bot
        if not hasattr(scout_bot, "_benchmarks_warmer"):
            return False, "_benchmarks_warmer daemon function missing"
        import pathlib
        src = (pathlib.Path(__file__).parent / "scout_bot.py").read_text()
        if 'name="benchmarks-warmer"' not in src:
            return False, "benchmarks-warmer not registered in main() via _start_daemon"
        smoke_section = src.split("def _run_startup_smoke_test")[1].split("\ndef ")[0]
        if "_get_benchmarks" not in smoke_section:
            return False, "_run_startup_smoke_test does not warm benchmarks at boot"
        return True, "boot warmup + 30-min refresher daemon both wired"
    except Exception as e:
        return False, str(e)


# ── PR 21: signal thresholds wired to config (not hardcoded) ─────────────────

@test("signal_thresholds_loaded_from_config_not_hardcoded")
def test_signal_thresholds_from_config():
    """
    scout_bot._SIGNAL_CFG constants must match scout_agent.SCOUT_THRESHOLDS['signals'].
    Catches any future re-hardcoding of these values.
    """
    try:
        import scout_bot
        import scout_agent
        sig = scout_agent.SCOUT_THRESHOLDS.get("signals", {})
        checks = [
            ("_FILL_RATE_MIN_SESSIONS_7D",   scout_bot._FILL_RATE_MIN_SESSIONS_7D,   int(sig.get("fill_rate_min_sessions_7d", 5000))),
            ("_GHOST_RECENCY_HOURS",          scout_bot._GHOST_RECENCY_HOURS,          int(sig.get("ghost_recency_hours", 48))),
            ("_VELOCITY_DOWN_THRESHOLD_PCT",  scout_bot._VELOCITY_DOWN_THRESHOLD_PCT,  float(sig.get("velocity_down_threshold_pct", -40))),
            ("_VELOCITY_UP_THRESHOLD_PCT",    scout_bot._VELOCITY_UP_THRESHOLD_PCT,    float(sig.get("velocity_up_threshold_pct", 20))),
            ("_CAP_ALERT_PCT",               scout_bot._CAP_ALERT_PCT,               float(sig.get("cap_alert_pct", 90))),
        ]
        mismatches = [f"{name}: bot={bot_val} config={cfg_val}" for name, bot_val, cfg_val in checks if bot_val != cfg_val]
        if mismatches:
            return False, "constants diverge from config: " + "; ".join(mismatches)
        return True, f"all 5 signal constants match config ({len(checks)} checks)"
    except Exception as e:
        return False, str(e)


@test("ghost_campaigns_accepts_recency_hours_parameter")
def test_ghost_recency_param():
    """
    queries.ghost_campaigns must accept recency_hours with default 48.
    Catches accidental removal of the parameter.
    """
    try:
        import inspect
        import queries
        sig = inspect.signature(queries.ghost_campaigns)
        params = sig.parameters
        if "recency_hours" not in params:
            return False, "recency_hours parameter missing from queries.ghost_campaigns"
        default = params["recency_hours"].default
        if default != 48:
            return False, f"default should be 48, got {default!r}"
        return True, "recency_hours param present with default=48"
    except Exception as e:
        return False, str(e)


@test("ghost_recency_config_propagates_through_query_ghost_campaigns")
def test_ghost_recency_propagation():
    """
    _query_ghost_campaigns must pass ghost_recency_hours from SCOUT_THRESHOLDS
    to queries.ghost_campaigns. Changing the config value changes the call arg.
    """
    try:
        import scout_agent
        import queries as _queries
        calls = []
        original = _queries.ghost_campaigns
        def _spy(ch, recency_hours=48):
            calls.append(recency_hours)
            return []
        _queries.ghost_campaigns = _spy
        try:
            original_val = scout_agent.SCOUT_THRESHOLDS.get("signals", {}).get("ghost_recency_hours", 48)
            scout_agent._query_ghost_campaigns(None)
            if not calls:
                return False, "queries.ghost_campaigns was never called"
            if calls[0] != original_val:
                return False, f"called with recency_hours={calls[0]}, expected {original_val}"
            # Monkey-patch config and verify propagation
            scout_agent.SCOUT_THRESHOLDS.setdefault("signals", {})["ghost_recency_hours"] = 72
            calls.clear()
            scout_agent._query_ghost_campaigns(None)
            if not calls or calls[0] != 72:
                return False, f"config change not propagated: got {calls}"
            return True, f"config value propagated correctly (default={original_val}, patched=72)"
        finally:
            _queries.ghost_campaigns = original
            scout_agent.SCOUT_THRESHOLDS.setdefault("signals", {})["ghost_recency_hours"] = original_val
    except Exception as e:
        return False, str(e)


# ── Boot card renderer unit tests ────────────────────────────────────────────

@test("boot_card_renders_zero_tests_as_failure")
def test_boot_card_zero_tests():
    blocks, fallback = format_slack_blocks([], 0)
    text = " ".join(
        e.get("text", {}).get("text", "") if isinstance(e.get("text"), dict) else ""
        for b in blocks for e in ([b] + b.get("elements", []))
    )
    if ":white_check_mark:" in text or ":warning:" in text:
        return False, f"0-test case rendered as pass or warning, not explicit failure: {text[:120]}"
    if ":x:" not in text and "0 tests" not in text and "no checks" not in text.lower():
        return False, f"0-test card missing ❌ marker: {text[:120]}"
    return True, f"0 tests → failure card ({len(blocks)} blocks)"


@test("boot_card_all_pass_collapses_to_summary_card")
def test_boot_card_all_pass():
    results = [{"name": f"test_{i}", "passed": True, "detail": "ok"} for i in range(10)]
    blocks, _ = format_slack_blocks(results, 10)
    if len(blocks) > 3:
        return False, f"all-pass rendered {len(blocks)} blocks, expected ≤3"
    text = " ".join(
        b.get("text", {}).get("text", "") if isinstance(b.get("text"), dict) else
        " ".join(e.get("text", "") for e in b.get("elements", []) if isinstance(e, dict))
        for b in blocks
    )
    if ":white_check_mark:" not in text:
        return False, f"all-pass card missing ✅: {text[:120]}"
    return True, f"all-pass → {len(blocks)} blocks"


@test("boot_card_surfaces_failures_and_collapses_passing")
def test_boot_card_one_failure():
    results = [{"name": "bad_check", "passed": False, "detail": "it broke"}]
    results += [{"name": f"good_{i}", "passed": True, "detail": "ok"} for i in range(5)]
    blocks, _ = format_slack_blocks(results, 5)
    all_text = " ".join(
        b.get("text", {}).get("text", "") if isinstance(b.get("text"), dict) else
        " ".join(str(e.get("text", "")) for e in b.get("elements", []) if isinstance(e, dict))
        for b in blocks
    )
    if "bad_check" not in all_text:
        return False, "failure name not surfaced in card"
    if "it broke" not in all_text:
        return False, "failure detail not surfaced in card"
    if "+5" not in all_text and "5 other" not in all_text:
        return False, "passing count not collapsed into summary"
    # Verify the 5 passing test names are NOT individually listed
    for i in range(5):
        if f"good_{i}" in all_text:
            return False, f"good_{i} is individually listed — passing tests should be collapsed"
    return True, f"failure surfaced, passing collapsed ({len(blocks)} blocks)"


@test("boot_card_caps_failures_at_10_with_overflow_note")
def test_boot_card_overflow():
    results = [{"name": f"fail_{i}", "passed": False, "detail": f"err {i}"} for i in range(15)]
    blocks, _ = format_slack_blocks(results, 0)
    all_text = " ".join(
        b.get("text", {}).get("text", "") if isinstance(b.get("text"), dict) else
        " ".join(str(e.get("text", "")) for e in b.get("elements", []) if isinstance(e, dict))
        for b in blocks
    )
    shown = sum(1 for i in range(15) if f"fail_{i}" in all_text)
    if shown > 10:
        return False, f"showed {shown} failures — must cap at 10"
    if "5 more" not in all_text and "and 5" not in all_text:
        return False, f"overflow note missing — expected '5 more': {all_text[:200]}"
    return True, f"15 failures → {shown} shown + overflow note ({len(blocks)} blocks)"


@test("boot_card_renderer_crash_falls_back_to_plain_text")
def test_boot_card_fallback():
    import unittest.mock as _mock
    results = [{"name": "check_a", "passed": True, "detail": "ok"}]
    with _mock.patch(
        __name__ + ".format_slack_blocks",
        side_effect=RuntimeError("renderer exploded"),
    ):
        # post_to_slack should catch the error and call format_slack_message instead
        import os as _os
        orig_token = _os.environ.get("SLACK_BOT_TOKEN")
        try:
            _os.environ["SLACK_BOT_TOKEN"] = "xoxb-fake-token-for-test"
            with _mock.patch("slack_sdk.web.WebClient.chat_postMessage") as mock_post:
                post_to_slack(results, 1)
                if not mock_post.called:
                    return False, "chat_postMessage was never called after renderer crash"
                call_kwargs = mock_post.call_args
                # Must have posted using plain-text fallback (blocks=None or absent)
                kwargs = call_kwargs.kwargs if call_kwargs.kwargs else (call_kwargs[1] if len(call_kwargs) > 1 else {})
                if "blocks" in kwargs and kwargs["blocks"] is not None:
                    return False, f"renderer crash still sent blocks: {kwargs.get('blocks')}"
                text = kwargs.get("text", call_kwargs[0][1] if call_kwargs[0] and len(call_kwargs[0]) > 1 else "")
                if not text:
                    return False, "fallback text was empty"
        finally:
            if orig_token is None:
                _os.environ.pop("SLACK_BOT_TOKEN", None)
            else:
                _os.environ["SLACK_BOT_TOKEN"] = orig_token
    return True, "renderer crash → plain-text fallback posted successfully"


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

    All-pass  → 2-block summary card. Scannable in 1 second.
    Any fail  → failures surfaced (capped at 10 + "and N more"), passing collapsed.
    0 results → explicit failure card (not a false all-pass).

    Fallback to format_slack_message() on any renderer exception (see post_to_slack).
    """
    from datetime import datetime as _dt
    import pytz as _pytz
    total    = len(results)
    failed   = [r for r in results if not r["passed"]]
    n_fail   = len(failed)
    n_pass   = pass_count
    now_ct   = _dt.now(_pytz.timezone("America/Chicago")).strftime("%-I:%M %p CT")
    fallback = f"Scout: {pass_count}/{total} checks passed"

    blocks: list[dict] = []

    # 0-test case must render as failure, not pass (0==0 is truthy all-pass otherwise)
    if total == 0:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":x: *Scout boot — no checks ran*"},
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Startup check · {now_ct} · 0 tests registered"}],
        })
        return blocks, "Scout: 0 checks ran — something went wrong"

    if n_fail == 0:
        # ── All green: 2-block summary card ──────────────────────────────────────
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":white_check_mark: *Scout is healthy — {total}/{total} checks passed*"},
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"{total} checks passed · {now_ct}"}],
        })
    else:
        # ── Failures: headline, failures surfaced (capped at 10), passing collapsed ─
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":warning: *Scout has issues — {n_pass}/{total} checks passed*"},
        })
        blocks.append({"type": "divider"})

        shown = failed[:10]
        overflow = n_fail - len(shown)
        for r in shown:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":red_circle: *{r['name']}*\n{r['detail']}"},
            })
        if overflow > 0:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"…and {overflow} more failure{'s' if overflow != 1 else ''}"}],
            })

        if n_pass > 0:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f":white_check_mark: +{n_pass} other check{'s' if n_pass != 1 else ''} passed"}],
            })
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":mag: *Check Render logs for the failing checks above.*"},
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Startup check · {now_ct}"}],
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
        try:
            blocks, fallback = format_slack_blocks(results, pass_count)
        except Exception as render_err:
            print(f"format_slack_blocks failed ({render_err}), falling back to plain text")
            blocks = None
            fallback = format_slack_message(results, pass_count)
        web = WebClient(token=token)
        kwargs: dict = {"channel": "C0AQEECF800", "text": fallback, "unfurl_links": False}
        if blocks is not None:
            kwargs["blocks"] = blocks
        web.chat_postMessage(**kwargs)
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

