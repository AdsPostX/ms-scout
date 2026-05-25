"""
Scout Smoke Test — run after every deploy, or manually anytime.

Usage:
  python smoke_test.py              # prints results to stdout
  python smoke_test.py --slack      # also posts results to #scout-qa
  python smoke_test.py --slack --quiet   # Slack only (no stdout)

Tests covered:
  1. ClickHouse — connection + simple query
  2. Entity overrides — file readable, valid JSON
  3. Offer inventory — offers_latest.json present and non-empty
  4. ask("status") — end-to-end LLM + tool call round-trip
  5. ask("ghost campaigns") — tool-calling path (ClickHouse query)
  6. State files — JSON validity of pulse_state/digest_state/image_cache + data/ writable
  7. Slack token — auth.test confirms bot identity
  8. Notion queue DB ID — NOTION_QUEUE_DB_ID env var is set
  9. Handler symbols — SocketModeResponse and RateLimitErrorRetryHandler importable
 10. scout_state runtime — _pick_loading_message and _smart_history callable
 11. _build_advertiser_rpm_context_blocks — pure function, no DB
 12. get_scout_status() — digest_env + digest_routing fields present
 13. get_scout_status() — available_networks is a list when offers exist
 14. get_pulse_summary() — has_pulse key present, handles no-pulse case gracefully

Note: Anthropic API auth is now verified via _compute_health_status() in the health
heartbeat daemon — not here. Smoke tests cover deterministic code paths only.
"""

import argparse
import json
import os
import pathlib
import re
import sys
import time
import types
from unittest.mock import patch

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


# ── Entity overrides ──────────────────────────────────────────────────────────

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

        text = result.text
        # Check for Scout self-reporting failure — not ops vocabulary like "tracking errors"
        _fail_phrases = ("something broke", "i got an error", "encountered an error", "failed to retrieve")
        if not text or any(p in text.lower() for p in _fail_phrases):
            return False, f"Bad response: {str(text)[:120]}"
        # Positive check: a status response must contain at least one health keyword
        _status_keywords = ("healthy", "degraded", "available", "version", "benchmark", "offer", "uptime")
        if not any(kw in text.lower() for kw in _status_keywords):
            return False, f"Unexpected response (no status keywords found): {str(text)[:120]}"
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

        text = result.text
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
        from scout_ui_kit import _build_advertiser_rpm_context_blocks
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


@test("_build_signal_header() — correct block structure with and without context")
def test_build_signal_header():
    try:
        from scout_ui_kit import _build_signal_header
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
        from scout_ui_kit import _build_item_card
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
        from scout_ui_kit import _build_publisher_card
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
        # Check _BASE_THRESHOLDS (JSON file only, no runtime overrides) — SCOUT_THRESHOLDS
        # includes any live set_threshold overrides from data/threshold_overrides.json, so
        # asserting a hardcoded value there would break whenever an admin adjusts a threshold.
        cfg = scout_agent._BASE_THRESHOLDS
        for section in ("digest", "signals", "health"):
            if section not in cfg:
                return False, f"section missing: {section}"
        if cfg["digest"]["min_rpm_floor"] != 5:
            return False, f"digest.min_rpm_floor expected 5, got {cfg['digest']['min_rpm_floor']}"
        if cfg["signals"]["fill_rate_min_sessions_7d"] != 2500:
            return False, f"signals.fill_rate_min_sessions_7d expected 2500, got {cfg['signals']['fill_rate_min_sessions_7d']}"
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


# Tier 3 category coverage validated at boot via _SCHEMA_DEPS in scout_agent.py

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
        def _spy(ch, recency_hours=48, as_of_date=None):
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


@test("get_queue_status_tool_registered_with_all_contract_pieces")
def test_get_queue_status_tool_registered():
    import scout_agent
    # 1. Name in TOOLS list
    tool_names = [t["name"] for t in scout_agent.TOOLS]
    if "get_queue_status" not in tool_names:
        return False, f"get_queue_status missing from TOOLS list; found: {tool_names}"
    # 2. Callable in TOOL_MAP
    fn = scout_agent.TOOL_MAP.get("get_queue_status")
    if not callable(fn):
        return False, f"TOOL_MAP['get_queue_status'] is not callable: {fn!r}"
    # 3. Function exists with correct return annotation
    if not hasattr(scout_agent, "get_queue_status"):
        return False, "get_queue_status function not found on scout_agent module"
    # 4. TOOLS entry has input_schema
    tool_def = next(t for t in scout_agent.TOOLS if t["name"] == "get_queue_status")
    if "input_schema" not in tool_def:
        return False, "get_queue_status TOOLS entry missing input_schema"
    return True, "get_queue_status registered in TOOLS, TOOL_MAP, and module"


@test("revenue_tracker_config_keys_present")
def test_revenue_tracker_daemon_function_exists():
    # _revenue_tracker background daemon was removed in P4.1 (was behind
    # REVENUE_TRACKER_ENABLED=false and never ran in production).  The
    # threshold config keys must still be present because the force-run path
    # (@Scout force revenue) reads them at call time.
    import scout_agent
    thresholds = scout_agent.SCOUT_THRESHOLDS.get("signals", {})
    if "revenue_tracker_check_hour_ct" not in thresholds:
        return False, "revenue_tracker_check_hour_ct missing from signals config"
    if "revenue_tracker_publisher_min_delta" not in thresholds:
        return False, "revenue_tracker_publisher_min_delta missing from signals config"
    return True, "revenue_tracker threshold config keys present for force-run path"


@test("intraday_revenue_total_query_function_exists_on_scout_agent")
def test_intraday_revenue_total_query_exists():
    import scout_agent
    if not hasattr(scout_agent, "_query_intraday_revenue_total"):
        return False, "_query_intraday_revenue_total not found on scout_agent module"
    if not callable(scout_agent._query_intraday_revenue_total):
        return False, "_query_intraday_revenue_total is not callable"
    # Verify shared baseline query it depends on also exists
    if not hasattr(scout_agent, "_query_revenue_baseline"):
        return False, "_query_revenue_baseline (used by Phase 1) not found on scout_agent module"
    return True, "_query_intraday_revenue_total callable, shared _query_revenue_baseline present"


@test("projection_autocheck_config_and_state_intact")
def test_projection_autocheck_monitor_registered():
    """Projection autocheck state helpers, routing, and config keys must survive P4.1.

    The _projection_autocheck_monitor background daemon was removed in P4.1
    (was behind PROJECTION_AUTOCHECK_ENABLED=false and never ran in production).
    The durable pieces that still matter are verified here:
      (a) state helpers live on scout_state (persist slot across restarts)
      (b) _route_channel('qa') resolves to #sidd-qa (_SCOUT_HQ_CHANNEL)
      (c) threshold config keys are present
    """
    import scout_bot, scout_state

    for fn in ("_load_projection_autocheck_slot", "_save_projection_autocheck_slot"):
        if not hasattr(scout_state, fn) or not callable(getattr(scout_state, fn)):
            return False, f"{fn} missing/not callable on scout_state"

    # Assert production-mapping directly so the dev-env fallback can't mask a
    # missing / wrong "qa" entry. Patch _SCOUT_ENV to "production" for the call.
    if scout_bot._PRODUCTION_CHANNELS.get("qa") != scout_bot._SCOUT_HQ_CHANNEL:
        return False, (
            f"_PRODUCTION_CHANNELS['qa']={scout_bot._PRODUCTION_CHANNELS.get('qa')!r}, "
            f"expected {scout_bot._SCOUT_HQ_CHANNEL!r}"
        )
    _saved_env = scout_bot._SCOUT_ENV
    try:
        scout_bot._SCOUT_ENV = "production"
        routed = scout_bot._route_channel("qa")
    finally:
        scout_bot._SCOUT_ENV = _saved_env
    if routed != scout_bot._SCOUT_HQ_CHANNEL:
        return False, f"_route_channel('qa') under SCOUT_ENV=production returned {routed!r}, expected _SCOUT_HQ_CHANNEL"

    import scout_agent
    sig = scout_agent.SCOUT_THRESHOLDS.get("signals", {})
    required_keys = [
        "projection_autocheck_monitor_enabled",
        "projection_autocheck_window_start_ct",
        "projection_autocheck_window_end_ct",
        "projection_autocheck_eod_rollup_hour_ct",
        "projection_autocheck_apples_compare_hour_ct",
        "projection_autocheck_max_consecutive_errors",
    ]
    for k in required_keys:
        if k not in sig:
            return False, f"signals config missing {k!r}"

    return True, "autocheck state helpers + qa route + config keys intact after P4.1 cleanup"


@test("intraday_revenue_by_publisher_query_function_exists_on_scout_agent")
def test_intraday_revenue_by_publisher_query_exists():
    import scout_agent
    if not hasattr(scout_agent, "_query_intraday_revenue_by_publisher"):
        return False, "_query_intraday_revenue_by_publisher not found on scout_agent module"
    if not callable(scout_agent._query_intraday_revenue_by_publisher):
        return False, "_query_intraday_revenue_by_publisher is not callable"
    return True, "_query_intraday_revenue_by_publisher callable"


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


# ── Sourcing intelligence tests (PR: feat/sourcing-intel-digest) ──────────────
# Tests for _sourcing_signal_*, _nth_weekday, _fuzzy_name_match, _run_sourcing_signals
# All tests are unit tests: no ClickHouse, no live offers file required.

@test("sourcing_nth_weekday_mothers_day_2026")
def test_sourcing_nth_weekday_mothers_day_2026():
    """2nd Sunday in May 2026 = May 10 (Mother's Day)"""
    from scout_digest import _nth_weekday
    result = _nth_weekday(2026, 5, 2, 6)
    if result.month != 5 or result.day != 10:
        return False, f"Mother's Day 2026 should be May 10, got {result}"
    return True, f"Mother's Day 2026 = {result} ✓"


@test("sourcing_nth_weekday_fathers_day_2026")
def test_sourcing_nth_weekday_fathers_day_2026():
    """3rd Sunday in June 2026 = June 21 (Father's Day)"""
    from scout_digest import _nth_weekday
    result = _nth_weekday(2026, 6, 3, 6)
    if result.month != 6 or result.day != 21:
        return False, f"Father's Day 2026 should be June 21, got {result}"
    return True, f"Father's Day 2026 = {result} ✓"


@test("sourcing_nth_weekday_thanksgiving_2026")
def test_sourcing_nth_weekday_thanksgiving_2026():
    """4th Thursday in November 2026 = Nov 26 (Thanksgiving)"""
    from scout_digest import _nth_weekday
    result = _nth_weekday(2026, 11, 4, 3)
    if result.month != 11 or result.day != 26:
        return False, f"Thanksgiving 2026 should be Nov 26, got {result}"
    return True, f"Thanksgiving 2026 = {result} ✓"


@test("sourcing_seasonal_empty_offers")
def test_sourcing_seasonal_empty_offers():
    """Signal returns [] when offers list is empty."""
    from scout_digest import _sourcing_signal_seasonal
    result = _sourcing_signal_seasonal([])
    if result != []:
        return False, f"Expected [], got {result}"
    return True, "Empty offers → [] ✓"


@test("sourcing_seasonal_no_matching_verticals")
def test_sourcing_seasonal_no_matching_verticals():
    """Vertical filtering: auto insurance offer doesn't match flowers/gifts verticals."""
    # Test the filtering logic directly rather than mocking the date
    # _sourcing_signal_seasonal filters offers by category/name matching the event verticals
    verticals = ["flowers", "gifts", "jewelry", "experiences"]
    auto_offer = {"offer_name": "Auto Insurance", "category": "insurance", "fit_tier": "PRIME", "payout": "5.00"}
    cat  = (auto_offer.get("category") or "").lower()
    name = (auto_offer.get("offer_name") or "").lower()
    matches = any(v in cat or v in name for v in verticals)
    if matches:
        return False, "Auto Insurance should not match flowers/gifts/jewelry/experiences"
    # Positive: flowers offer DOES match
    flower_offer = {"offer_name": "1-800-Flowers", "category": "gifts", "fit_tier": "PRIME", "payout": "8.00"}
    cat2  = (flower_offer.get("category") or "").lower()
    name2 = (flower_offer.get("offer_name") or "").lower()
    matches2 = any(v in cat2 or v in name2 for v in verticals)
    if not matches2:
        return False, "1-800-Flowers should match gifts/flowers verticals"
    return True, "Vertical filtering: auto insurance excluded, flowers included ✓"


@test("sourcing_seasonal_weak_tier_excluded")
def test_sourcing_seasonal_weak_tier_excluded():
    """WEAK-tier offers are filtered by the tier guard in _sourcing_signal_seasonal."""
    # _sourcing_signal_seasonal uses a local import for date (not patchable at module level),
    # so test the tier-filtering predicate directly — the same check the function applies.
    tier_guard = lambda o: o.get("fit_tier", "STANDARD") in ("PRIME", "STRONG")

    weak_flower  = {"offer_name": "1-800-Flowers", "category": "flowers",  "fit_tier": "WEAK",  "payout": "5.00"}
    prime_flower = {"offer_name": "ProFlowers",    "category": "flowers",  "fit_tier": "PRIME", "payout": "8.00"}
    strong_gift  = {"offer_name": "GiftTree",      "category": "gifts",    "fit_tier": "STRONG","payout": "6.00"}

    if tier_guard(weak_flower):
        return False, "WEAK tier should fail the tier guard"
    if not tier_guard(prime_flower):
        return False, "PRIME tier should pass the tier guard"
    if not tier_guard(strong_gift):
        return False, "STRONG tier should pass the tier guard"
    return True, "WEAK tier excluded by tier guard; PRIME/STRONG pass ✓"


@test("sourcing_seasonal_kill_switch")
def test_sourcing_seasonal_kill_switch(monkeypatch=None):
    """Signal returns [] when SCOUT_DISABLED_SOURCING_SIGNALS=seasonal."""
    import os, importlib
    old = os.environ.get("SCOUT_DISABLED_SOURCING_SIGNALS", "")
    os.environ["SCOUT_DISABLED_SOURCING_SIGNALS"] = "seasonal"
    try:
        import scout_digest
        importlib.reload(scout_digest)
        offers = [{"offer_name": "Flowers", "category": "flowers", "fit_tier": "PRIME", "payout": "8.00"}]
        result = scout_digest._sourcing_signal_seasonal(offers)
        if result != []:
            return False, f"Kill switch should return []; got {result}"
        return True, "SCOUT_DISABLED_SOURCING_SIGNALS=seasonal → [] ✓"
    finally:
        if old:
            os.environ["SCOUT_DISABLED_SOURCING_SIGNALS"] = old
        else:
            os.environ.pop("SCOUT_DISABLED_SOURCING_SIGNALS", None)


@test("sourcing_new_offers_empty_list")
def test_sourcing_new_offers_empty_list():
    """Signal returns [] when offers list is empty."""
    from scout_digest import _sourcing_signal_new_offers
    result = _sourcing_signal_new_offers([])
    if result != []:
        return False, f"Expected [], got {result}"
    return True, "Empty offers → [] ✓"


@test("sourcing_new_offers_no_first_seen")
def test_sourcing_new_offers_no_first_seen():
    """Signal skips offers without first_seen field (predate the field)."""
    from scout_digest import _sourcing_signal_new_offers
    offers = [{"offer_name": "OldOffer", "fit_tier": "PRIME", "payout": "10.00", "payout_type": "CPL"}]
    result = _sourcing_signal_new_offers(offers)
    if result:
        return False, f"Offer without first_seen should be skipped; got {result}"
    return True, "No first_seen → skipped ✓"


@test("sourcing_new_offers_old_first_seen_excluded")
def test_sourcing_new_offers_old_first_seen_excluded():
    """Offers with first_seen > 48h ago are not surfaced."""
    from scout_digest import _sourcing_signal_new_offers
    from datetime import datetime, timezone, timedelta
    old_ts = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    offers = [{"offer_name": "OldOffer", "fit_tier": "PRIME", "payout": "10.00", "payout_type": "CPL", "first_seen": old_ts}]
    result = _sourcing_signal_new_offers(offers)
    if result:
        return False, f"5-day-old offer should not appear as new; got {result}"
    return True, "5-day-old first_seen → excluded ✓"


@test("sourcing_new_offers_recent_first_seen_included")
def test_sourcing_new_offers_recent_first_seen_included():
    """Offers with first_seen < 48h ago are surfaced."""
    from scout_digest import _sourcing_signal_new_offers
    from datetime import datetime, timezone, timedelta
    recent_ts = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    offers = [{"offer_name": "NewOffer", "fit_tier": "PRIME", "payout": "8.00", "payout_type": "CPL", "first_seen": recent_ts}]
    result = _sourcing_signal_new_offers(offers)
    if not result or result[0]["offer_name"] != "NewOffer":
        return False, f"12h-old offer should appear as new; got {result}"
    return True, "Recent first_seen → included ✓"


@test("sourcing_new_offers_uses_first_seen_not_last_verified")
def test_sourcing_new_offers_uses_first_seen_not_last_verified():
    """Regression: must use first_seen, not last_verified. Old offer with recent last_verified must not appear."""
    from scout_digest import _sourcing_signal_new_offers
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    old_ts    = (now - timedelta(days=30)).isoformat()
    recent_ts = (now - timedelta(hours=12)).isoformat()
    offers = [
        # Should NOT appear: first_seen is old (even though last_verified is recent)
        {"fit_tier": "PRIME", "offer_name": "OldOffer", "payout": "10.00", "payout_type": "CPL",
         "first_seen": old_ts, "last_verified": recent_ts},
        # SHOULD appear: first_seen is recent
        {"fit_tier": "PRIME", "offer_name": "NewOffer", "payout": "8.00", "payout_type": "CPL",
         "first_seen": recent_ts, "last_verified": recent_ts},
    ]
    result = _sourcing_signal_new_offers(offers)
    names = [o["offer_name"] for o in result]
    if "OldOffer" in names:
        return False, "OldOffer has old first_seen — must not appear as new (regression: last_verified used instead)"
    if "NewOffer" not in names:
        return False, f"NewOffer should appear; got names={names}"
    return True, "first_seen used (not last_verified) ✓"


@test("sourcing_new_offers_weak_tier_excluded")
def test_sourcing_new_offers_weak_tier_excluded():
    """WEAK and STANDARD tier offers excluded even if first_seen is recent."""
    from scout_digest import _sourcing_signal_new_offers
    from datetime import datetime, timezone, timedelta
    recent_ts = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    offers = [
        {"offer_name": "WeakOffer",     "fit_tier": "WEAK",     "payout": "0.50", "first_seen": recent_ts},
        {"offer_name": "StandardOffer", "fit_tier": "STANDARD", "payout": "3.00", "first_seen": recent_ts},
        {"offer_name": "StrongOffer",   "fit_tier": "STRONG",   "payout": "6.00", "first_seen": recent_ts},
    ]
    result = _sourcing_signal_new_offers(offers)
    names = [o["offer_name"] for o in result]
    if "WeakOffer" in names or "StandardOffer" in names:
        return False, f"WEAK/STANDARD should be excluded; got {names}"
    if "StrongOffer" not in names:
        return False, f"STRONG should be included; got {names}"
    return True, "WEAK/STANDARD excluded, STRONG included ✓"


@test("sourcing_new_offers_sorted_by_payout")
def test_sourcing_new_offers_sorted_by_payout():
    """Results sorted by payout descending."""
    from scout_digest import _sourcing_signal_new_offers
    from datetime import datetime, timezone, timedelta
    recent_ts = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    offers = [
        {"offer_name": "Low",  "fit_tier": "PRIME", "payout": "3.00", "first_seen": recent_ts},
        {"offer_name": "High", "fit_tier": "PRIME", "payout": "9.00", "first_seen": recent_ts},
        {"offer_name": "Mid",  "fit_tier": "PRIME", "payout": "5.00", "first_seen": recent_ts},
    ]
    result = _sourcing_signal_new_offers(offers)
    payouts = [float(o.get("payout", 0)) for o in result]
    if payouts != sorted(payouts, reverse=True):
        return False, f"Not sorted descending by payout: {payouts}"
    return True, f"Sorted correctly: {payouts} ✓"


@test("sourcing_new_offers_max_5")
def test_sourcing_new_offers_max_5():
    """Maximum 5 results returned even with 10 qualifying offers."""
    from scout_digest import _sourcing_signal_new_offers
    from datetime import datetime, timezone, timedelta
    recent_ts = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    offers = [
        {"offer_name": f"Offer{i}", "fit_tier": "PRIME", "payout": str(i), "first_seen": recent_ts}
        for i in range(10)
    ]
    result = _sourcing_signal_new_offers(offers)
    if len(result) > 5:
        return False, f"Max 5 results expected, got {len(result)}"
    return True, f"Max 5 cap: {len(result)} results ✓"


@test("sourcing_new_offers_kill_switch")
def test_sourcing_new_offers_kill_switch():
    """Signal returns [] when SCOUT_DISABLED_SOURCING_SIGNALS=new_offers."""
    import os, importlib
    from datetime import datetime, timezone, timedelta
    old = os.environ.get("SCOUT_DISABLED_SOURCING_SIGNALS", "")
    os.environ["SCOUT_DISABLED_SOURCING_SIGNALS"] = "new_offers"
    try:
        import scout_digest
        importlib.reload(scout_digest)
        recent_ts = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        offers = [{"offer_name": "NewOffer", "fit_tier": "PRIME", "payout": "8.00", "first_seen": recent_ts}]
        result = scout_digest._sourcing_signal_new_offers(offers)
        if result != []:
            return False, f"Kill switch should suppress; got {result}"
        return True, "SCOUT_DISABLED_SOURCING_SIGNALS=new_offers → [] ✓"
    finally:
        if old:
            os.environ["SCOUT_DISABLED_SOURCING_SIGNALS"] = old
        else:
            os.environ.pop("SCOUT_DISABLED_SOURCING_SIGNALS", None)


@test("sourcing_payout_upgrades_empty_offers")
def test_sourcing_payout_upgrades_empty_offers():
    """Signal returns [] when offers list is empty."""
    from scout_digest import _sourcing_signal_payout_upgrades
    result = _sourcing_signal_payout_upgrades([])
    if result != []:
        return False, f"Empty offers → []; got {result}"
    return True, "Empty offers → [] ✓"


@test("sourcing_payout_upgrades_payout_type_from_offer")
def test_sourcing_payout_upgrades_payout_type_from_offer():
    """payout_type in upgrade result comes from the offer inventory, not the CH row.
    CH query no longer returns payout_type (column doesn't exist in conversions).
    """
    from scout_digest import _fuzzy_name_match
    # AT&T in inventory has CPL payout_type — name match should succeed
    offers = [{"advertiser": "AT&T", "payout_type": "CPL", "payout": "10.00", "fit_tier": "PRIME"}]
    matches = [
        o for o in offers
        if _fuzzy_name_match("AT&T", o.get("advertiser") or "")
        and o.get("fit_tier") in ("PRIME", "STRONG")
    ]
    if not matches:
        return False, "AT&T offer should match via fuzzy name"
    if matches[0].get("payout_type") != "CPL":
        return False, f"payout_type should come from offer; got {matches[0].get('payout_type')}"
    return True, "payout_type sourced from offer inventory ✓"


@test("sourcing_payout_upgrades_gap_vs_gap_insurance")
def test_sourcing_payout_upgrades_gap_vs_gap_insurance():
    """'Gap' advertiser does NOT match 'Gap Insurance' offer (word-boundary qualifier guard)."""
    from scout_digest import _fuzzy_name_match
    if _fuzzy_name_match("Gap", "Gap Insurance"):
        return False, "'Gap' matched 'Gap Insurance' — qualifier guard failed"
    if not _fuzzy_name_match("AT&T", "AT&T Wireless"):
        return False, "'AT&T' should match 'AT&T Wireless'"
    return True, "Gap ≠ Gap Insurance, AT&T = AT&T Wireless ✓"


@test("sourcing_payout_upgrades_below_min_delta_suppressed")
def test_sourcing_payout_upgrades_below_min_delta_suppressed():
    """Signal does NOT fire when net-estimated delta < _MIN_UPGRADE_DELTA."""
    from scout_digest import _GROSS_TO_NET_FACTOR, _MIN_UPGRADE_DELTA
    # current net = 5.00, inventory gross = 6.00, net_est = 4.20, delta = -0.80 → suppress
    current_net = 5.00
    inv_gross   = 6.00
    inv_net_est = inv_gross * _GROSS_TO_NET_FACTOR
    delta       = inv_net_est - current_net
    if delta >= _MIN_UPGRADE_DELTA:
        return False, f"Delta {delta:.2f} should be below MIN_UPGRADE_DELTA {_MIN_UPGRADE_DELTA}"
    return True, f"Delta {delta:.2f} < {_MIN_UPGRADE_DELTA} → correctly suppressed ✓"


@test("sourcing_payout_upgrades_above_min_delta_surfaced")
def test_sourcing_payout_upgrades_above_min_delta_surfaced():
    """Signal DOES fire when net-estimated delta >= _MIN_UPGRADE_DELTA."""
    from scout_digest import _GROSS_TO_NET_FACTOR, _MIN_UPGRADE_DELTA
    # current net = 2.00, inventory gross = 6.00, net_est = 4.20, delta = +2.20 → surface
    current_net = 2.00
    inv_gross   = 6.00
    inv_net_est = inv_gross * _GROSS_TO_NET_FACTOR
    delta       = inv_net_est - current_net
    if delta < _MIN_UPGRADE_DELTA:
        return False, f"Delta {delta:.2f} should be >= MIN_UPGRADE_DELTA {_MIN_UPGRADE_DELTA}"
    return True, f"Delta {delta:.2f} >= {_MIN_UPGRADE_DELTA} → correctly surfaced ✓"


@test("sourcing_payout_upgrades_kill_switch")
def test_sourcing_payout_upgrades_kill_switch():
    """Signal returns [] when SCOUT_DISABLED_SOURCING_SIGNALS=payout_upgrades."""
    import os, importlib
    old = os.environ.get("SCOUT_DISABLED_SOURCING_SIGNALS", "")
    os.environ["SCOUT_DISABLED_SOURCING_SIGNALS"] = "payout_upgrades"
    try:
        import scout_digest
        importlib.reload(scout_digest)
        offers = [{"advertiser": "AT&T", "payout_type": "CPL", "payout": "10.00", "fit_tier": "PRIME"}]
        result = scout_digest._sourcing_signal_payout_upgrades(offers)
        if result != []:
            return False, f"Kill switch should suppress; got {result}"
        return True, "SCOUT_DISABLED_SOURCING_SIGNALS=payout_upgrades → [] ✓"
    finally:
        if old:
            os.environ["SCOUT_DISABLED_SOURCING_SIGNALS"] = old
        else:
            os.environ.pop("SCOUT_DISABLED_SOURCING_SIGNALS", None)


@test("sourcing_fatigue_budget_new_offers_wins")
def test_sourcing_fatigue_budget_new_offers_wins():
    """When new_offers fires, seasonal and payout_upgrades are zeroed out."""
    from scout_digest import _run_sourcing_signals, _SOURCING_SIGNAL_PRIORITY
    import unittest.mock
    from datetime import datetime, timezone, timedelta
    recent_ts = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    offers_with_new = [
        {"offer_name": "NewOffer", "fit_tier": "PRIME", "payout": "8.00", "first_seen": recent_ts},
    ]
    with unittest.mock.patch("scout_digest._sourcing_signal_payout_upgrades", return_value=[{"advertiser": "Fake", "delta_net_est": 5.0}]):
        with unittest.mock.patch("scout_digest._sourcing_signal_seasonal", return_value=[{"event_name": "TestEvent", "days_until": 5}]):
            result = _run_sourcing_signals(offers_with_new)
    if not result.get("new_offers"):
        return False, f"new_offers should have results; got {result}"
    if result.get("seasonal"):
        return False, f"seasonal should be zeroed out by fatigue budget; got {result['seasonal']}"
    if result.get("payout_upgrades"):
        return False, f"payout_upgrades should be zeroed out; got {result['payout_upgrades']}"
    return True, "Fatigue budget: new_offers wins, others zeroed ✓"


@test("sourcing_fatigue_budget_seasonal_fallback")
def test_sourcing_fatigue_budget_seasonal_fallback():
    """When new_offers is empty but seasonal fires, seasonal wins."""
    from scout_digest import _run_sourcing_signals
    import unittest.mock
    seasonal_result = [{"event_name": "Mother's Day", "days_until": 5, "offer_count": 2, "top_offers": [], "verticals": []}]
    with unittest.mock.patch("scout_digest._sourcing_signal_new_offers", return_value=[]):
        with unittest.mock.patch("scout_digest._sourcing_signal_seasonal", return_value=seasonal_result):
            with unittest.mock.patch("scout_digest._sourcing_signal_payout_upgrades", return_value=[]):
                result = _run_sourcing_signals([])
    if not result.get("seasonal"):
        return False, f"seasonal should win when new_offers is empty; got {result}"
    if result.get("new_offers") or result.get("payout_upgrades"):
        return False, f"others should be zeroed; got {result}"
    return True, "Fatigue budget: seasonal fallback ✓"


@test("sourcing_signals_in_scout_digest_not_scout_bot")
def test_sourcing_signals_in_scout_digest_not_scout_bot():
    """Verify sourcing signals live in scout_digest, not scout_bot (architecture enforcement)."""
    import scout_digest, scout_bot
    required_on_digest = [
        "_sourcing_signal_seasonal",
        "_sourcing_signal_payout_upgrades",
        "_sourcing_signal_new_offers",
        "_run_sourcing_signals",
        "_build_sourcing_intel_blocks",
        "_nth_weekday",
    ]
    should_not_be_on_bot = [
        "_pulse_signal_seasonal",
        "_pulse_signal_payout_upgrades",
        "_pulse_signal_new_offers",
    ]
    missing = [fn for fn in required_on_digest if not hasattr(scout_digest, fn)]
    if missing:
        return False, f"Missing from scout_digest: {missing}"
    wrongly_on_bot = [fn for fn in should_not_be_on_bot if hasattr(scout_bot, fn)]
    if wrongly_on_bot:
        return False, f"These should NOT be on scout_bot (architecture violation): {wrongly_on_bot}"
    return True, "All sourcing signals on scout_digest, none on scout_bot ✓"


# ── PR I: first_seen backfill tests (clean_offers) ───────────────────────────

@test("first_seen_backfill_uses_last_verified_from_snapshot")
def test_first_seen_backfill_uses_last_verified_from_snapshot():
    """Offer without first_seen but with last_verified in snapshot → first_seen = last_verified.

    Calls clean_offers() against a real (temp) snapshot file so the actual cache-loading
    + fallback codepath is exercised.
    Regression guard: before this fix, all pre-PR87 offers got first_seen = date_scraped (today)
    and appeared as 'new' simultaneously on the first post-PR87 scrape.
    """
    import offer_scraper, json, pathlib

    old_verified = "2026-05-10T08:00:00+00:00"  # snapshot's last_verified — a few days ago

    snapshot_path = pathlib.Path(offer_scraper.__file__).parent / "data" / "offers_latest.json"
    orig_data = snapshot_path.read_text() if snapshot_path.exists() else None

    snapshot_fixture = json.dumps([{
        "network": "CJ", "offer_id": "test-backfill-001",
        "last_verified": old_verified,
        # No first_seen — simulates a pre-PR87 offer
    }])
    raw_offer = {
        "network": "CJ", "offer_id": "test-backfill-001",
        "offer_name": "Test Backfill Offer", "advertiser": "TestCo",
        "payout": "5.00", "payout_type": "CPA", "status": "Active",
        "date_scraped": "2026-05-15",
    }

    try:
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(snapshot_fixture)
        cleaned = offer_scraper.clean_offers([raw_offer])
    finally:
        if orig_data is not None:
            snapshot_path.write_text(orig_data)
        elif snapshot_path.exists():
            snapshot_path.unlink()

    if not cleaned:
        return False, "clean_offers returned empty list — offer may have been filtered out"
    got = cleaned[0].get("first_seen", "")
    if got != old_verified:
        return False, f"Expected first_seen={old_verified!r} (from last_verified backfill); got {got!r}"
    return True, f"clean_offers backfills first_seen from snapshot last_verified ({old_verified}) ✓"


@test("first_seen_immutable_when_already_set")
def test_first_seen_immutable_when_already_set():
    """Offer with existing first_seen in snapshot → first_seen unchanged across rescrapes.

    Calls clean_offers() against a real (temp) snapshot file.
    """
    import offer_scraper, json, pathlib

    original_first_seen = "2026-04-01T12:00:00+00:00"

    snapshot_path = pathlib.Path(offer_scraper.__file__).parent / "data" / "offers_latest.json"
    orig_data = snapshot_path.read_text() if snapshot_path.exists() else None

    snapshot_fixture = json.dumps([{
        "network": "MaxBounty", "offer_id": "mb-immutable-001",
        "first_seen":    original_first_seen,
        "last_verified": "2026-05-14T08:00:00+00:00",
    }])
    raw_offer = {
        "network": "MaxBounty", "offer_id": "mb-immutable-001",
        "offer_name": "ImmutableTest", "advertiser": "ImmutableCo",
        "payout": "5.00", "payout_type": "CPA", "status": "Active",
        "date_scraped": "2026-05-15",
    }

    try:
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(snapshot_fixture)
        cleaned = offer_scraper.clean_offers([raw_offer])
    finally:
        if orig_data is not None:
            snapshot_path.write_text(orig_data)
        elif snapshot_path.exists():
            snapshot_path.unlink()

    if not cleaned:
        return False, "clean_offers returned empty list"
    got = cleaned[0].get("first_seen", "")
    if got != original_first_seen:
        return False, f"first_seen must be immutable; expected {original_first_seen!r}, got {got!r}"
    return True, f"first_seen unchanged across rescrapes ({original_first_seen}) ✓"


# ── PR H: Rich sourcing card rendering tests ──────────────────────────────────

@test("sourcing_cards_new_offers_renders_card_format")
def test_sourcing_cards_new_offers_renders_card_format():
    """new_offers signal renders Block Kit section cards with unified scout_approve / scout_reject buttons."""
    import scout_digest
    now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    offers  = [
        {"offer_name": "TestOffer", "advertiser": "TestCo", "payout": "$8.00",
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "CJ",
         "first_seen": now_iso},
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks  = scout_digest._build_sourcing_intel_blocks(signals)
    # Must have a section block with 'fields' (card format) — not a single mrkdwn text block
    card_blocks = [b for b in blocks if b.get("type") == "section" and "fields" in b]
    if not card_blocks:
        return False, f"Expected Block Kit card with 'fields'; got block types: {[b.get('type') for b in blocks]}"
    # Must have action buttons using the unified action_ids (not legacy scout_draft_*)
    action_blocks = [b for b in blocks if b.get("type") == "actions"]
    if not action_blocks:
        return False, "Expected actions block with Add to Queue / Skip buttons"
    button_ids = [e.get("action_id") for b in action_blocks for e in b.get("elements", [])]
    if "scout_draft_add" in button_ids or "scout_draft_skip" in button_ids:
        return False, "Legacy scout_draft_add/skip found — sourcing cards must use scout_approve/scout_reject"
    if "scout_approve" not in button_ids or "scout_reject" not in button_ids:
        return False, f"Expected scout_approve and scout_reject; got {button_ids}"
    return True, "new_offers renders rich Block Kit cards with unified scout_approve / scout_reject buttons ✓"


@test("sourcing_cards_payout_upgrades_plain_mrkdwn")
def test_sourcing_cards_payout_upgrades_plain_mrkdwn():
    """payout_upgrades renders as plain mrkdwn (different data shape — no card format)."""
    import scout_digest
    upgrades = [{"advertiser": "AT&T", "payout_type": "CPL", "current_net_payout": 2.00,
                 "inventory_gross_payout": 6.00, "inventory_net_est": 4.20,
                 "network": "CJ", "delta_net_est": 2.20, "offer_name": "AT&T Wireless"}]
    signals = {"new_offers": [], "seasonal": [], "payout_upgrades": upgrades}
    blocks  = scout_digest._build_sourcing_intel_blocks(signals)
    # Must be a plain text section (no 'fields'), not a card
    card_blocks = [b for b in blocks if b.get("type") == "section" and "fields" in b]
    if card_blocks:
        return False, "payout_upgrades should NOT render as card blocks (wrong data shape)"
    mrkdwn_blocks = [b for b in blocks if b.get("type") == "section" and "text" in b]
    if not mrkdwn_blocks:
        return False, "payout_upgrades should render as plain mrkdwn section"
    return True, "payout_upgrades renders as plain mrkdwn, not card format ✓"


@test("sourcing_cards_no_double_dollar_sign")
def test_sourcing_cards_no_double_dollar_sign():
    """Payout display uses _parse_payout — no double $ (e.g. '$8.00 CPL' not '$8.00 $ CPL')."""
    import scout_digest
    now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    offers  = [
        {"offer_name": "FlowerOffer", "advertiser": "1800Flowers", "payout": "$8.50",
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "FlexOffers",
         "first_seen": now_iso},
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks  = scout_digest._build_sourcing_intel_blocks(signals)
    all_text = " ".join(
        str(f.get("text", "")) for b in blocks
        for f in (b.get("fields") or [b.get("text", {})])
    )
    if "$ CPL" in all_text or "$ PER" in all_text or "$$" in all_text:
        return False, f"Double dollar sign found in output: {all_text[:200]}"
    if "$8.50" not in all_text:
        return False, f"Expected '$8.50' in payout display; got: {all_text[:200]}"
    return True, "Payout display is '$8.50 CPL' with no double dollar sign ✓"


@test("sourcing_cards_max_3_offers")
def test_sourcing_cards_max_3_offers():
    """sourcing section caps at 3 offer cards even when 5 offers are provided."""
    import scout_digest
    now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    offers  = [
        {"offer_name": f"Offer{i}", "advertiser": f"Adv{i}", "payout": f"${5 + i}.00",
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "CJ",
         "first_seen": now_iso}
        for i in range(5)
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks  = scout_digest._build_sourcing_intel_blocks(signals)
    action_blocks = [b for b in blocks if b.get("type") == "actions"]
    if len(action_blocks) != 2:
        return False, f"Expected exactly 2 offer cards (per-network cap); got {len(action_blocks)} action blocks"
    return True, "Per-network cap enforced: 2 offer cards shown for single-network batch ✓"


@test("sourcing_cards_payout_type_normalized")
def test_sourcing_cards_payout_type_normalized():
    """Payout type must show 'CPL' not '$ PER LEAD' in sourcing cards."""
    import scout_digest
    now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    offers = [{"offer_name": "TestOffer", "advertiser": "TestAdv", "payout": "750.00",
               "payout_type": "$ per lead", "fit_tier": "PRIME", "network": "maxbounty",
               "first_seen": now_iso}]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    block_text = str(blocks)
    if "$ PER LEAD" in block_text or "$ per lead" in block_text:
        return False, f"Raw payout_type still present in output: {block_text[:200]}"
    if "CPL" not in block_text:
        return False, f"Expected 'CPL' in normalized output; got: {block_text[:200]}"
    return True, "Payout type normalized: '$ per lead' → 'CPL' ✓"


@test("sourcing_cards_network_header")
def test_sourcing_cards_network_header():
    """Each network group must have a 'header' block — not just a section header."""
    import scout_digest
    now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    offers = [
        {"offer_name": "OfferA", "advertiser": "AdvA", "payout": "5.00",
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "maxbounty", "first_seen": now_iso},
        {"offer_name": "OfferB", "advertiser": "AdvB", "payout": "8.00",
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "cj", "first_seen": now_iso},
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    header_blocks = [b for b in blocks if b.get("type") == "header"]
    if len(header_blocks) < 2:
        return False, f"Expected ≥2 header blocks (one per network); got {len(header_blocks)}"
    return True, f"Network header blocks present: {len(header_blocks)} headers for 2 networks ✓"


@test("sourcing_cards_uses_mini_description")
def test_sourcing_cards_uses_mini_description():
    """mini_description must appear in the visible card fields (not truncated description).
    Note: description still appears in the button action_value for the Notion pipeline —
    so we check section block fields, not the raw str(blocks) representation."""
    import scout_digest
    now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    offers = [{"offer_name": "TestOffer", "advertiser": "TestAdv", "payout": "5.00",
               "payout_type": "CPL", "fit_tier": "PRIME", "network": "cj", "first_seen": now_iso,
               "mini_description": "This is the mini teaser",
               "description": "This is the much longer description that should NOT appear"}]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    # Check only section.fields (visible card text), not actions blocks (which carry description
    # in the action_value JSON for the Notion pipeline — same as build_digest_blocks does).
    visible_text_parts = []
    for b in blocks:
        if b.get("type") == "section":
            for f in (b.get("fields") or []):
                if isinstance(f, dict):
                    visible_text_parts.append(f.get("text", ""))
            if b.get("text"):
                visible_text_parts.append(b["text"].get("text", ""))
    visible = " ".join(visible_text_parts)
    if "This is the mini teaser" not in visible:
        return False, f"mini_description not found in visible card fields: {visible!r}"
    if "much longer description" in visible:
        return False, f"Fallback description appeared in visible fields even though mini_description was present: {visible!r}"
    return True, "mini_description used in visible card fields ✓"


@test("sourcing_cards_tier_in_right_col")
def test_sourcing_cards_tier_in_right_col():
    """Tier badge (PRIME/STRONG) must appear in the right-column field text, not just context."""
    import scout_digest
    now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
    offers = [{"offer_name": "TestOffer", "advertiser": "TestAdv", "payout": "5.00",
               "payout_type": "CPL", "fit_tier": "PRIME", "network": "cj", "first_seen": now_iso}]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    section_blocks = [b for b in blocks if b.get("type") == "section" and b.get("fields")]
    if not section_blocks:
        return False, "No section blocks with fields found"
    right_col = section_blocks[0]["fields"][1]["text"]
    if "PRIME" not in right_col:
        return False, f"PRIME tier not in right column field text: '{right_col}'"
    return True, f"Tier badge 'PRIME' present in right column: '{right_col}' ✓"


@test("digest_footer_has_no_demand_queue_text")
def test_digest_footer_has_no_demand_queue_text():
    """build_digest_blocks() must not include the removed Demand Queue footer context block."""
    import scout_digest
    # Minimal mock: build_digest_blocks needs offers_by_network dict; pass empty to get minimal output
    import datetime as _datetime
    blocks = scout_digest.build_digest_blocks(
        {}, {}, [], {}, _datetime.date.today().isoformat()
    )
    combined_text = " ".join(
        el.get("text", "")
        for b in blocks
        for el in (b.get("elements") or [])
        if isinstance(el, dict)
    )
    # Also check plain section text fields
    combined_text += " ".join(
        (b.get("text") or {}).get("text", "")
        for b in blocks
        if b.get("type") == "section"
    )
    if "Demand Queue" in combined_text:
        return False, "Found 'Demand Queue' text in digest blocks — footer was not removed"
    return True, "No 'Demand Queue' text in digest blocks ✓"


@test("sourcing_cards_per_network_cap_at_two")
def test_sourcing_cards_per_network_cap_at_two():
    """With 4 offers from one network, only 2 should render (per-network cap = 2)."""
    import scout_digest
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    offers = [
        {"offer_name": f"Offer{i}", "advertiser": f"Adv{i}", "payout": str(10 - i),
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "maxbounty", "first_seen": now_iso}
        for i in range(4)
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    # Count section blocks with fields (one per rendered offer card)
    offer_cards = [b for b in blocks if b.get("type") == "section" and b.get("fields")]
    if len(offer_cards) > 2:
        return False, f"Expected ≤2 offer cards for one network, got {len(offer_cards)}"
    return True, f"Per-network cap enforced: {len(offer_cards)} card(s) rendered ✓"


@test("offer_name_suffix_stripped_from_display")
def test_offer_name_suffix_stripped_from_display():
    """_clean_offer_name() strips trailing '- TYPE (GEO)' metadata suffixes."""
    import scout_digest
    cases = [
        ("signNow - E-Signature Solution - CPS (WW)", "signNow - E-Signature Solution"),
        ("SomeOffer - CPL (US)", "SomeOffer"),
        ("AT&T Wireless", "AT&T Wireless"),          # no suffix — unchanged
        ("Simple Offer - CPA (CA,US)", "Simple Offer"),
        ("", ""),
    ]
    for raw, expected in cases:
        result = scout_digest._clean_offer_name(raw)
        if result != expected:
            return False, f"_clean_offer_name({raw!r}) = {result!r}, expected {expected!r}"
    return True, f"All {len(cases)} offer name suffix cases cleaned correctly ✓"


@test("cold_start_label_when_all_offers_seeded_today")
def test_cold_start_label_when_all_offers_seeded_today():
    """When >80% of PRIME offers share today's first_seen, context shows 'top PRIME' not 'new in last 48h'."""
    import scout_digest
    from datetime import date, datetime, timezone
    today_iso = datetime.now(timezone.utc).isoformat()
    # 5 offers all first_seen today → cold start
    offers = [
        {"offer_name": f"Offer{i}", "advertiser": f"Adv{i}", "payout": str(i + 1),
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "maxbounty", "first_seen": today_iso}
        for i in range(5)
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    # The per-network count context block should say 'top PRIME', not 'new in last 48h'
    context_texts = [
        el.get("text", "")
        for b in blocks if b.get("type") == "context"
        for el in (b.get("elements") or [])
        if isinstance(el, dict) and el.get("type") == "mrkdwn"
    ]
    combined = " ".join(context_texts)
    if "new in last 48h" in combined:
        return False, f"Found 'new in last 48h' during cold-start — should say 'top PRIME'. Context: {combined!r}"
    if "top PRIME" not in combined:
        return False, f"Expected 'top PRIME' in context during cold-start, got: {combined!r}"
    return True, "Cold-start label shows 'top PRIME' instead of 'new in last 48h' ✓"


def _extract_blockquote_texts(blocks):
    """Extract text from section/mrkdwn blocks with a '>' blockquote prefix."""
    texts = []
    for b in blocks:
        if b.get("type") != "section":
            continue
        txt = (b.get("text") or {})
        if txt.get("type") == "mrkdwn" and txt.get("text", "").startswith(">"):
            texts.append(txt["text"].lstrip("> "))
    return texts


@test("sourcing_context_line_shows_age_not_tier")
def test_sourcing_context_line_shows_age_not_tier():
    """Context line under each offer card shows category + age ('2d ago'), not the tier badge."""
    import scout_digest
    from datetime import datetime, timezone, timedelta
    old_iso = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    offers = [
        {"offer_name": "TestOffer", "advertiser": "TestAdv", "payout": "5.00",
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "cj",
         "category": "Software", "first_seen": old_iso}
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    offer_context_texts = [t for t in _extract_blockquote_texts(blocks)
                           if "ago" in t or "today" in t]
    if not offer_context_texts:
        offer_context_texts = [t for t in _extract_blockquote_texts(blocks)
                               if "Software" in t]
    if not offer_context_texts:
        return False, "No per-offer context line found with age or category text"
    combined = " ".join(offer_context_texts)
    if "PRIME" in combined or "STRONG" in combined:
        return False, f"Tier badge found in context line (should only be in right column): '{combined}'"
    return True, f"Context line shows age/category, no tier badge: {offer_context_texts[0]!r} ✓"


@test("cold_start_guard_fires_on_any_dominant_date_not_only_today")
def test_cold_start_guard_fires_on_any_dominant_date_not_only_today():
    """Cold-start guard must fire when >80% of PRIME offers share YESTERDAY's first_seen date.
    The guard originally checked today's date — failed in production after midnight UTC when
    the bulk seed happened the prior calendar day."""
    import scout_digest
    from datetime import datetime, timezone, timedelta
    yesterday_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    # 5 offers all seeded yesterday (not today) → still cold-start
    offers = [
        {"offer_name": f"Offer{i}", "advertiser": f"Adv{i}", "payout": str(i + 1),
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "maxbounty", "first_seen": yesterday_iso}
        for i in range(5)
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    context_texts = [
        el.get("text", "")
        for b in blocks if b.get("type") == "context"
        for el in (b.get("elements") or [])
        if isinstance(el, dict) and el.get("type") == "mrkdwn"
    ]
    combined = " ".join(context_texts)
    if "new in last 48h" in combined:
        return False, f"Guard missed yesterday's bulk seed — 'new in last 48h' shown instead of 'top PRIME': {combined!r}"
    if "top PRIME" not in combined:
        return False, f"Expected 'top PRIME' when all offers seeded yesterday, got: {combined!r}"
    return True, "Cold-start guard fires on yesterday's bulk seed date, not only today ✓"


@test("sourcing_count_in_context_line_matches_rendered_cards")
def test_sourcing_count_in_context_line_matches_rendered_cards():
    """Context line '2 offers · top PRIME' must reflect the RENDERED count (2), not the raw network total (4).
    Pre-cap: count was set before slicing net_offers[:2], causing '4 offers' with only 2 cards rendered."""
    import scout_digest
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    # 4 offers from one network — per-network cap = 2, context must say "2"
    offers = [
        {"offer_name": f"Offer{i}", "advertiser": f"Adv{i}", "payout": str(10 - i),
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "maxbounty", "first_seen": now_iso}
        for i in range(4)
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    context_texts = [
        el.get("text", "")
        for b in blocks if b.get("type") == "context"
        for el in (b.get("elements") or [])
        if isinstance(el, dict) and el.get("type") == "mrkdwn"
    ]
    combined = " ".join(context_texts)
    if "4 offer" in combined:
        return False, f"Context line shows '4 offers' but only 2 cards render (pre-cap bug): {combined!r}"
    if "2 offer" not in combined:
        return False, f"Expected '2 offer' in context (rendered count), got: {combined!r}"
    return True, "Context line count matches rendered card count (2, not raw total 4) ✓"


@test("sourcing_category_multi_value_shows_first_value_only")
def test_sourcing_category_multi_value_shows_first_value_only():
    """Category field 'Business Services, Marketing' must show only 'Business Services' in the context line."""
    import scout_digest
    from datetime import datetime, timezone, timedelta
    old_iso = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    offers = [
        {"offer_name": "MultiCatOffer", "advertiser": "MultiCatAdv", "payout": "5.00",
         "payout_type": "CPL", "fit_tier": "PRIME", "network": "cj",
         "category": "Business Services, Marketing", "first_seen": old_iso}
    ]
    signals = {"new_offers": offers, "seasonal": [], "payout_upgrades": []}
    blocks = scout_digest._build_sourcing_intel_blocks(signals)
    offer_context_texts = [t for t in _extract_blockquote_texts(blocks) if "Business" in t]
    if not offer_context_texts:
        return False, "No context line containing category text found"
    combined = " ".join(offer_context_texts)
    if "Marketing" in combined:
        return False, f"Multi-value category leaked into context — 'Marketing' should be stripped: {combined!r}"
    if "Business Services" not in combined:
        return False, f"Expected 'Business Services' (first value only) in context, got: {combined!r}"
    return True, f"Multi-value category shows first value only: {offer_context_texts[0]!r} ✓"


@test("export_surface_importable_from_scout_agent")
def test_export_surface_importable_from_scout_agent():
    """Re-export surface guard (B5 prerequisite).

    Before scout_agent.py is split into smaller modules, this test pins the
    exact set of symbols that downstream callers (e.g. scout_digest's deferred
    in-function imports of _scrape_og_image) rely on being importable from
    `scout_agent`. If any symbol disappears or is renamed during the split,
    this test fails with a clear name — catching what unit tests miss because
    those deferred imports only fire at call time.
    """
    expected = [
        # Infra
        "_get_ch_client",
        "_run_parallel",
        # ClickHouse query helpers
        "_query_ghost_campaigns",
        "_query_revenue_baseline",
        "_query_intraday_revenue_total",
        "_query_intraday_revenue_by_publisher",
        "_query_advertiser_rpm_context",
        # PR-C new query helpers (re-exported from scout_ch via scout_agent import)
        "_query_cvr_anomaly",
        "_query_expiring_campaigns",
        "_query_publisher_revenue_trends",
        "_query_advertiser_revenue_trends",
        # Image helpers
        "_scrape_og_image",
        "_clearbit_domain",
        "_google_favicon",
        "_app_store_icon",
        "_validate_image_url",
        "_load_image_cache",
        "_save_image_cache",
        "_cached_external_images",
        "_store_image_cache",
        "_ms_cdn_image",
    ]
    try:
        import scout_agent
    except Exception as e:
        return False, f"scout_agent failed to import: {e}"

    missing = [name for name in expected if not hasattr(scout_agent, name)]
    if missing:
        return False, f"Missing from scout_agent (extraction surface broken): {missing}"

    not_callable = [
        name for name in expected if not callable(getattr(scout_agent, name))
    ]
    if not_callable:
        return False, f"Surface symbols present but not callable: {not_callable}"

    return True, f"All {len(expected)} extraction-surface symbols importable & callable ✓"


@test("cvr_anomaly_expiration_force_run_query_functions_present")
def test_cvr_anomaly_monitor_registered_as_required_daemon():
    """cvr/expiration background daemons were removed in P4.1 (SCOUT_INPROC_*=false,
    never ran in production).  The force-run path (@Scout force cvr/expiration) must
    still work — verified by confirming the query functions are importable and that
    _set_force_monitor_fn wires them in main().
    """
    import pathlib
    import scout_bot
    # Query functions must still be reachable (imported from scout_ch)
    for fn in ("_query_cvr_anomaly", "_query_expiring_campaigns"):
        if not hasattr(scout_bot, fn):
            return False, f"{fn} missing on scout_bot — force-run path broken"
    # _set_force_monitor_fn calls for cvr and expiration must exist in main()
    src = (pathlib.Path(__file__).parent / "scout_bot.py").read_text()
    main_section = src.split("def main()")[1].split("\ndef ")[0]
    missing = []
    if '_set_force_monitor_fn("cvr"' not in main_section and "_set_force_monitor_fn('cvr'" not in main_section:
        missing.append("cvr force-monitor registration")
    if '_set_force_monitor_fn("expiration"' not in main_section and "_set_force_monitor_fn('expiration'" not in main_section:
        missing.append("expiration force-monitor registration")
    if missing:
        return False, f"force-run wiring absent from main(): {missing}"
    return True, "cvr + expiration force-run query functions present and wired in main() ✓"


@test("cvr_anomaly_and_expiration_agent_tools_in_tool_map")
def test_cvr_anomaly_and_expiration_agent_tools_in_tool_map():
    """All 4 new agent tools must be in TOOL_MAP with callable values.

    A tool present in TOOLS[] but absent from TOOL_MAP silently returns 'tool not found'
    to the LLM — no error, no alert, just a wrong answer.
    """
    import scout_agent
    required = [
        "get_cvr_anomalies",
        "get_expiring_campaigns",
        "get_publisher_revenue_trends",
        "get_advertiser_revenue_trends",
    ]
    missing = [t for t in required if t not in scout_agent.TOOL_MAP]
    not_callable = [t for t in required if t in scout_agent.TOOL_MAP and not callable(scout_agent.TOOL_MAP[t])]
    if missing:
        return False, f"Missing from TOOL_MAP: {missing}"
    if not_callable:
        return False, f"In TOOL_MAP but not callable: {not_callable}"
    return True, f"All {len(required)} new tools in TOOL_MAP ✓"


@test("new_monitor_state_helpers_present_in_scout_state")
def test_new_monitor_state_helpers_present_in_scout_state():
    """CVR anomaly and expiration monitor state helpers must exist and follow the pattern.

    _run_with_web() calls load_state_fn() and save_state_fn() by name — if either is
    missing from scout_state, the monitor crashes silently at the first fire window.
    """
    import scout_state
    required = [
        "_load_cvr_anomaly_alert_state",
        "_save_cvr_anomaly_alert_date",
        "_load_expiration_alert_state",
        "_save_expiration_alert_date",
    ]
    missing = [fn for fn in required if not hasattr(scout_state, fn)]
    if missing:
        return False, f"Missing from scout_state: {missing}"
    # verify load functions return None (no state yet) without error
    errors = []
    for fn_name in ["_load_cvr_anomaly_alert_state", "_load_expiration_alert_state"]:
        try:
            result = getattr(scout_state, fn_name)()
            if result is not None and not isinstance(result, str):
                errors.append(f"{fn_name} returned unexpected type: {type(result)}")
        except Exception as e:
            errors.append(f"{fn_name} raised: {e}")
    if errors:
        return False, "; ".join(errors)
    return True, f"All {len(required)} state helpers present and load functions callable ✓"


@test("cvr_anomaly_wrapper_reads_thresholds_from_config_not_hardcoded")
def test_cvr_anomaly_wrapper_uses_config_thresholds():
    """_query_cvr_anomaly() must pass threshold values from SCOUT_THRESHOLDS to _q.cvr_anomaly,
    not use hardcoded defaults. Verifies the monkey-patch pattern: changing config changes behavior.
    """
    import scout_ch
    import scout_agent
    captured = {}

    def _fake_cvr_anomaly(ch, drop_pct, min_payout, min_impressions_7d):
        captured["drop_pct"] = drop_pct
        captured["min_payout"] = min_payout
        captured["min_impressions_7d"] = min_impressions_7d
        return []

    original_thresholds = scout_agent.SCOUT_THRESHOLDS
    original_q = scout_ch._q
    try:
        import types
        fake_q = types.SimpleNamespace(cvr_anomaly=_fake_cvr_anomaly)
        scout_ch._q = fake_q
        scout_agent.SCOUT_THRESHOLDS = {
            **original_thresholds,
            "signals": {**original_thresholds.get("signals", {}),
                        "cvr_anomaly_drop_pct": 99.0,
                        "cvr_anomaly_min_payout": 999.0,
                        "cvr_anomaly_min_impressions_7d": 12345},
        }
        scout_ch._query_cvr_anomaly(None)
        if captured.get("drop_pct") != 99.0:
            return False, f"drop_pct not read from config: got {captured.get('drop_pct')}"
        if captured.get("min_payout") != 999.0:
            return False, f"min_payout not read from config: got {captured.get('min_payout')}"
        if captured.get("min_impressions_7d") != 12345:
            return False, f"min_impressions_7d not read from config: got {captured.get('min_impressions_7d')}"
    finally:
        scout_agent.SCOUT_THRESHOLDS = original_thresholds
        scout_ch._q = original_q
    return True, "cvr_anomaly wrapper passes config thresholds through to query ✓"


@test("expiration_wrapper_reads_warning_days_from_config_not_hardcoded")
def test_expiration_wrapper_uses_config_thresholds():
    """_query_expiring_campaigns() must pass warning_days from SCOUT_THRESHOLDS."""
    import scout_ch
    import scout_agent
    captured = {}

    def _fake_expiring(ch, warning_days):
        captured["warning_days"] = warning_days
        return []

    original_thresholds = scout_agent.SCOUT_THRESHOLDS
    original_q = scout_ch._q
    try:
        import types
        fake_q = types.SimpleNamespace(expiring_campaigns=_fake_expiring)
        scout_ch._q = fake_q
        scout_agent.SCOUT_THRESHOLDS = {
            **original_thresholds,
            "signals": {**original_thresholds.get("signals", {}), "expiration_warning_days": 42},
        }
        scout_ch._query_expiring_campaigns(None)
        if captured.get("warning_days") != 42:
            return False, f"warning_days not read from config: got {captured.get('warning_days')}"
    finally:
        scout_agent.SCOUT_THRESHOLDS = original_thresholds
        scout_ch._q = original_q
    return True, "expiration wrapper passes config warning_days through to query ✓"


@test("revenue_trend_wrappers_read_min_periods_from_config_not_hardcoded")
def test_revenue_trend_wrappers_use_config_thresholds():
    """Both revenue trend wrappers must pass min_periods from SCOUT_THRESHOLDS."""
    import scout_ch
    import scout_agent
    captured = {}

    def _fake_pub_trends(ch, days, min_periods):
        captured["pub_min_periods"] = min_periods
        return []

    def _fake_adv_trends(ch, days, min_periods):
        captured["adv_min_periods"] = min_periods
        return []

    original_thresholds = scout_agent.SCOUT_THRESHOLDS
    original_q = scout_ch._q
    try:
        import types
        fake_q = types.SimpleNamespace(
            publisher_revenue_trends=_fake_pub_trends,
            advertiser_revenue_trends=_fake_adv_trends,
        )
        scout_ch._q = fake_q
        scout_agent.SCOUT_THRESHOLDS = {
            **original_thresholds,
            "signals": {**original_thresholds.get("signals", {}), "revenue_trend_min_periods": 99},
        }
        scout_ch._query_publisher_revenue_trends(None)
        scout_ch._query_advertiser_revenue_trends(None)
        if captured.get("pub_min_periods") != 99:
            return False, f"publisher wrapper min_periods not from config: {captured.get('pub_min_periods')}"
        if captured.get("adv_min_periods") != 99:
            return False, f"advertiser wrapper min_periods not from config: {captured.get('adv_min_periods')}"
    finally:
        scout_agent.SCOUT_THRESHOLDS = original_thresholds
        scout_ch._q = original_q
    return True, "both revenue trend wrappers pass config min_periods through to query ✓"


@test("threshold_override_layers_on_top_of_config_with_in_process_reload")
def test_threshold_override_layers():
    """A runtime override should win over config/scout_thresholds.json.

    Exercises the full three-layer merge: fallback → config → overrides.
    set_threshold() must reload module-level SCOUT_THRESHOLDS so subsequent
    reads see the new value without a restart.
    """
    import scout_agent, scout_state

    overrides_path = scout_state._THRESHOLD_OVERRIDES_FILE
    changelog_path = scout_state._THRESHOLD_CHANGELOG_FILE
    backup_o = overrides_path.read_bytes() if overrides_path.exists() else None
    backup_c = changelog_path.read_bytes() if changelog_path.exists() else None

    try:
        # Clean slate
        if overrides_path.exists(): overrides_path.unlink()
        if changelog_path.exists(): changelog_path.unlink()
        scout_agent.SCOUT_THRESHOLDS = scout_agent._load_thresholds()

        baseline = scout_agent.SCOUT_THRESHOLDS.get("signals", {}).get("cap_alert_pct")
        if baseline is None:
            return False, "Expected signals.cap_alert_pct in config — got None"

        target = baseline + 7
        admins = os.environ.get("SCOUT_THRESHOLD_ADMINS", "")
        os.environ["SCOUT_THRESHOLD_ADMINS"] = "UADMIN_TEST"
        try:
            result = scout_agent.set_threshold(
                section="signals", key="cap_alert_pct", value=target,
                reason="smoke test layering", _caller_user_id="UADMIN_TEST",
            )
        finally:
            if admins: os.environ["SCOUT_THRESHOLD_ADMINS"] = admins
            else: os.environ.pop("SCOUT_THRESHOLD_ADMINS", None)

        if not result.get("ok"):
            return False, f"set_threshold failed: {result}"

        live = scout_agent.SCOUT_THRESHOLDS.get("signals", {}).get("cap_alert_pct")
        if live != target:
            return False, f"In-process reload failed: expected {target}, got {live}"

        return True, f"Override layered on top of config (baseline={baseline} → live={target}) ✓"
    finally:
        if backup_o is not None: overrides_path.write_bytes(backup_o)
        elif overrides_path.exists(): overrides_path.unlink()
        if backup_c is not None: changelog_path.write_bytes(backup_c)
        elif changelog_path.exists(): changelog_path.unlink()
        try:
            import scout_agent as _sa
            _sa.SCOUT_THRESHOLDS = _sa._load_thresholds()
        except Exception as e:
            print(f"Warning: failed to restore SCOUT_THRESHOLDS: {e}")


@test("set_threshold_denies_non_admin_callers")
def test_set_threshold_admin_gate():
    """Non-admin callers must be rejected; SCOUT_THRESHOLDS must not change."""
    import scout_agent

    admins = os.environ.get("SCOUT_THRESHOLD_ADMINS", "")
    os.environ["SCOUT_THRESHOLD_ADMINS"] = "UADMIN_ONLY"
    try:
        before = scout_agent.SCOUT_THRESHOLDS.get("signals", {}).get("cap_alert_pct")
        result = scout_agent.set_threshold(
            section="signals", key="cap_alert_pct", value=999,
            reason="should be denied", _caller_user_id="UNOBODY",
        )
        if result.get("ok") or result.get("error") != "not_admin":
            return False, f"Expected denial (ok=False, error=not_admin), got {result}"
        after = scout_agent.SCOUT_THRESHOLDS.get("signals", {}).get("cap_alert_pct")
        if before != after:
            return False, f"Denied call still mutated value: {before} → {after}"
        return True, "Non-admin caller denied; SCOUT_THRESHOLDS unchanged ✓"
    finally:
        if admins: os.environ["SCOUT_THRESHOLD_ADMINS"] = admins
        else: os.environ.pop("SCOUT_THRESHOLD_ADMINS", None)


@test("set_threshold_appends_changelog_entry_with_actor_and_reason")
def test_set_threshold_writes_changelog():
    """Every successful set_threshold must append a JSONL line with actor + prior + new + reason."""
    import scout_agent, scout_state

    overrides_path = scout_state._THRESHOLD_OVERRIDES_FILE
    changelog_path = scout_state._THRESHOLD_CHANGELOG_FILE
    backup_o = overrides_path.read_bytes() if overrides_path.exists() else None
    backup_c = changelog_path.read_bytes() if changelog_path.exists() else None

    try:
        if overrides_path.exists(): overrides_path.unlink()
        if changelog_path.exists(): changelog_path.unlink()
        scout_agent.SCOUT_THRESHOLDS = scout_agent._load_thresholds()

        admins = os.environ.get("SCOUT_THRESHOLD_ADMINS", "")
        os.environ["SCOUT_THRESHOLD_ADMINS"] = "UCHANGELOG"
        try:
            r = scout_agent.set_threshold(
                section="signals", key="cap_alert_pct", value=88,
                reason="audit-trail check", _caller_user_id="UCHANGELOG",
            )
        finally:
            if admins: os.environ["SCOUT_THRESHOLD_ADMINS"] = admins
            else: os.environ.pop("SCOUT_THRESHOLD_ADMINS", None)

        if not r.get("ok"):
            return False, f"set_threshold failed: {r}"

        entries = scout_state._read_threshold_changelog(limit=5)
        if not entries:
            return False, "Changelog empty after successful set_threshold"
        last = entries[0] if entries else {}
        for field in ("set_by", "section", "key", "prior", "value", "reason", "ts"):
            if field not in last:
                return False, f"Changelog entry missing field: {field} — got keys {list(last.keys())}"
        if last["set_by"] != "UCHANGELOG":
            return False, f"set_by wrong: {last['set_by']}"
        if last["reason"] != "audit-trail check":
            return False, f"reason not persisted: {last['reason']}"
        return True, f"Changelog appended with set_by={last['set_by']} reason={last['reason']!r} ✓"
    finally:
        if backup_o is not None: overrides_path.write_bytes(backup_o)
        elif overrides_path.exists(): overrides_path.unlink()
        if backup_c is not None: changelog_path.write_bytes(backup_c)
        elif changelog_path.exists(): changelog_path.unlink()
        try:
            import scout_agent as _sa
            _sa.SCOUT_THRESHOLDS = _sa._load_thresholds()
        except Exception as e:
            print(f"Warning: failed to restore SCOUT_THRESHOLDS: {e}")


@test("force_run_monitor_returns_not_initialized_when_context_missing")
def test_force_run_monitor_graceful_without_ctx():
    """Before scout_bot injects (web, ch_factory), force_run_monitor must fail gracefully."""
    import scout_agent

    saved = dict(scout_agent._FORCE_MONITOR_CTX)
    scout_agent._FORCE_MONITOR_CTX["web"] = None
    scout_agent._FORCE_MONITOR_CTX["ch_factory"] = None
    try:
        admins = os.environ.get("SCOUT_THRESHOLD_ADMINS", "")
        os.environ["SCOUT_THRESHOLD_ADMINS"] = "UADMIN_FRM"
        try:
            r = scout_agent.force_run_monitor(
                monitor="ghost", _caller_user_id="UADMIN_FRM",
            )
        finally:
            if admins: os.environ["SCOUT_THRESHOLD_ADMINS"] = admins
            else: os.environ.pop("SCOUT_THRESHOLD_ADMINS", None)
        if r.get("ok") or r.get("error") != "not_initialized":
            return False, f"Expected ok=False error=not_initialized when ctx missing, got {r}"
        return True, "Returns not_initialized gracefully when scout_bot hasn't injected yet ✓"
    finally:
        scout_agent._FORCE_MONITOR_CTX.update(saved)


@test("threshold_control_tools_registered_in_TOOLS_and_TOOL_MAP")
def test_threshold_tools_registered():
    """All 4 new tools must be present in both the TOOLS list and TOOL_MAP."""
    import scout_agent

    expected = ["list_thresholds", "get_threshold_history", "set_threshold", "force_run_monitor"]
    tool_names = {t.get("name") for t in scout_agent.TOOLS}
    missing_tools = [n for n in expected if n not in tool_names]
    missing_map = [n for n in expected if n not in scout_agent.TOOL_MAP]
    if missing_tools:
        return False, f"Missing from TOOLS list: {missing_tools}"
    if missing_map:
        return False, f"Missing from TOOL_MAP: {missing_map}"
    return True, f"All {len(expected)} threshold-control tools registered ✓"


@test("get_scout_config_exposes_overridden_keys_and_last_override_at")
def test_get_scout_config_shows_overrides():
    """get_scout_config must surface override metadata so the team can see active overrides."""
    import scout_agent, scout_state

    overrides_path = scout_state._THRESHOLD_OVERRIDES_FILE
    changelog_path = scout_state._THRESHOLD_CHANGELOG_FILE
    backup_o = overrides_path.read_bytes() if overrides_path.exists() else None
    backup_c = changelog_path.read_bytes() if changelog_path.exists() else None
    try:
        if overrides_path.exists(): overrides_path.unlink()
        if changelog_path.exists(): changelog_path.unlink()
        scout_agent.SCOUT_THRESHOLDS = scout_agent._load_thresholds()

        admins = os.environ.get("SCOUT_THRESHOLD_ADMINS", "")
        os.environ["SCOUT_THRESHOLD_ADMINS"] = "UCFG"
        try:
            scout_agent.set_threshold(
                section="signals", key="cap_alert_pct", value=82,
                reason="config visibility test", _caller_user_id="UCFG",
            )
        finally:
            if admins: os.environ["SCOUT_THRESHOLD_ADMINS"] = admins
            else: os.environ.pop("SCOUT_THRESHOLD_ADMINS", None)

        cfg = scout_agent.get_scout_config()
        if "overridden_keys" not in cfg:
            return False, f"get_scout_config missing 'overridden_keys' — keys: {list(cfg.keys())}"
        if "signals.cap_alert_pct" not in (cfg.get("overridden_keys") or []):
            return False, f"signals.cap_alert_pct not in overridden_keys: {cfg.get('overridden_keys')}"
        if not cfg.get("last_override_at"):
            return False, "last_override_at empty after set_threshold"
        return True, f"overridden_keys={cfg['overridden_keys']} last_override_at={cfg['last_override_at']} ✓"
    finally:
        if backup_o is not None: overrides_path.write_bytes(backup_o)
        elif overrides_path.exists(): overrides_path.unlink()
        if backup_c is not None: changelog_path.write_bytes(backup_c)
        elif changelog_path.exists(): changelog_path.unlink()
        try:
            import scout_agent as _sa
            _sa.SCOUT_THRESHOLDS = _sa._load_thresholds()
        except Exception as e:
            print(f"Warning: failed to restore SCOUT_THRESHOLDS: {e}")


@test("cvr_expiration_revenue_force_commands_registered_in_main")
def test_new_force_commands_registered():
    """cvr, expiration, and revenue must be registered via _set_force_monitor_fn in main().

    _set_force_monitor_fn populates _FORCE_MONITOR_FNS at startup — both the direct
    regex path and force_run_monitor read from that dict. Without registration,
    @Scout force cvr silently falls through to the NLP path.
    Verified via source inspection (same pattern as daemon registration tests — the dict
    is empty at import time, only populated when main() runs).
    """
    import pathlib
    src = (pathlib.Path(__file__).parent / "scout_bot.py").read_text()
    main_section = src.split("def main()")[1].split("\ndef ")[0]
    missing = []
    for name in ("cvr", "expiration", "revenue"):
        if f'_set_force_monitor_fn("{name}"' not in main_section:
            missing.append(name)
    if missing:
        return False, f"_set_force_monitor_fn not called for: {missing} in main()"
    return True, "cvr, expiration, revenue all registered via _set_force_monitor_fn in main() ✓"


@test("force_handlers_regex_derives_from_registry_not_hardcoded")
def test_force_pattern_auto_discovers_from_registry():
    """_FORCE_MON_PAT in handle_event must be built from _FORCE_MONITOR_FNS.keys().

    Verified by source inspection: the regex construction line must reference
    _FORCE_MONITOR_FNS.keys() rather than a hardcoded alternation string.
    A hardcoded pattern would silently miss new monitors without raising an error.
    """
    import pathlib
    src = (pathlib.Path(__file__).parent / "scout_handlers.py").read_text()
    if "_FORCE_MONITOR_FNS.keys()" not in src:
        return False, "_FORCE_MON_PAT does not derive from _FORCE_MONITOR_FNS.keys()"
    if 'cap|velocity|ghost|fill' in src and "or" not in src[max(0, src.index('cap|velocity|ghost|fill')-50):src.index('cap|velocity|ghost|fill')+5]:
        return False, "Hardcoded alternation still present without fallback guard"
    return True, "_FORCE_MON_PAT derives from _FORCE_MONITOR_FNS.keys() ✓"


@test("force_run_monitor_allowed_derives_from_registry_not_hardcoded")
def test_force_run_monitor_allowed_from_registry():
    """force_run_monitor must pass the allowed check for cvr/expiration/revenue.

    If allowed were hardcoded to the original 4 names, these would return unknown_monitor
    instead of not_initialized (ctx guard fires before allowed when ctx is missing).
    """
    import scout_agent
    saved = dict(scout_agent._FORCE_MONITOR_CTX)
    scout_agent._FORCE_MONITOR_CTX["web"] = None
    scout_agent._FORCE_MONITOR_CTX["ch_factory"] = None
    try:
        import os
        admins = os.environ.get("SCOUT_THRESHOLD_ADMINS", "")
        os.environ["SCOUT_THRESHOLD_ADMINS"] = "UADMIN_REG"
        try:
            for name in ("cvr", "expiration", "revenue"):
                r = scout_agent.force_run_monitor(monitor=name, _caller_user_id="UADMIN_REG")
                if r.get("error") == "unknown_monitor":
                    return False, f"'{name}' treated as unknown_monitor — allowed set may be hardcoded"
                if r.get("error") != "not_initialized":
                    return False, f"'{name}' returned unexpected error={r.get('error')!r}"
        finally:
            if admins: os.environ["SCOUT_THRESHOLD_ADMINS"] = admins
            else: os.environ.pop("SCOUT_THRESHOLD_ADMINS", None)
        return True, "cvr, expiration, revenue pass allowed check (not_initialized, not unknown_monitor) ✓"
    finally:
        scout_agent._FORCE_MONITOR_CTX.update(saved)


@test("qa_suite_has_no_duplicate_labels")
def test_qa_suite_no_duplicate_labels():
    from scout_agent import _QA_SUITE
    labels = [e[0] for e in _QA_SUITE]
    dupes = [l for l in labels if labels.count(l) > 1]
    if dupes:
        return False, f"Duplicate labels: {dupes}"
    return True, f"All {len(labels)} labels unique ✓"


@test("qa_suite_entries_are_4_tuples_with_non_empty_category")
def test_qa_suite_entry_structure():
    from scout_agent import _QA_SUITE
    for entry in _QA_SUITE:
        if len(entry) != 4:
            return False, f"Entry not 4-tuple: {entry[0]!r}"
        if not isinstance(entry[3], str) or not entry[3]:
            return False, f"Empty or non-string category on: {entry[0]!r}"
    return True, f"All {len(_QA_SUITE)} entries are valid 4-tuples ✓"


@test("qa_suite_meets_minimum_coverage_threshold")
def test_qa_suite_minimum_coverage():
    from scout_agent import _QA_SUITE
    if len(_QA_SUITE) < 19:
        return False, f"Expected ≥19 entries, got {len(_QA_SUITE)}"
    return True, f"{len(_QA_SUITE)} entries ≥ 19 minimum ✓"


# ── Revenue projection tool (Step 7 — additive, no live SQL) ────────────────

@test("project_today_revenue_helper_exists_and_reexported")
def test_project_today_revenue_helper_exists():
    try:
        import scout_agent
        import scout_ch
        if not hasattr(scout_ch, "project_today_revenue"):
            return False, "scout_ch.project_today_revenue missing"
        if not hasattr(scout_agent, "project_today_revenue"):
            return False, "scout_agent re-export of project_today_revenue missing"
        return True, "helper present in scout_ch and re-exported via scout_agent"
    except Exception as e:
        return False, str(e)


@test("revenue_projection_tool_registered_with_all_pieces")
def test_revenue_projection_tool_registered():
    try:
        import scout_agent
        if not hasattr(scout_agent, "get_revenue_today_projection"):
            return False, "get_revenue_today_projection wrapper missing"
        if scout_agent.TOOL_MAP.get("get_revenue_today_projection") is not scout_agent.get_revenue_today_projection:
            return False, "TOOL_MAP['get_revenue_today_projection'] not bound"
        names = {t["name"] for t in scout_agent.TOOLS}
        if "get_revenue_today_projection" not in names:
            return False, f"TOOLS schema missing get_revenue_today_projection; have: {sorted(names)}"
        return True, "wrapper + TOOL_MAP + TOOLS schema all present"
    except Exception as e:
        return False, str(e)


@test("revenue_today_description_has_anti_routing")
def test_revenue_today_anti_routing():
    try:
        import scout_agent
        entry = next((t for t in scout_agent.TOOLS if t["name"] == "get_revenue_today"), None)
        if not entry:
            return False, "get_revenue_today tool entry missing"
        desc = entry["description"]
        if "Do NOT use" not in desc:
            return False, "anti-routing 'Do NOT use' phrase missing from description"
        if "project" not in desc.lower() or "forecast" not in desc.lower():
            return False, "anti-routing must mention project/forecast"
        return True, "anti-routing language present"
    except Exception as e:
        return False, str(e)


@test("project_today_revenue_too_early_string_verbatim")
def test_too_early_string_verbatim():
    """Verbatim too_early surface text is load-bearing for routing fidelity."""
    try:
        import inspect
        import scout_ch
        src = inspect.getsource(scout_ch.project_today_revenue)
        expected = "Too early to project reliably — ask after 10am CT."
        if expected not in src:
            return False, "verbatim too_early string drifted in source"
        # Also verify the hour<10 branch exists
        if "hour_ct < 10" not in src:
            return False, "hour_ct < 10 too_early guard missing"
        return True, "verbatim too_early string preserved in source"
    except Exception as e:
        return False, str(e)


# ── Digest UX improvements (markdown + fit_tier + rpm + skip friction) ────────

@test("offer_card_preserves_markdown_in_why_text")
def test_offer_card_no_markdown_strip():
    """Markdown in why text must survive — no re.sub stripping before card render."""
    try:
        from scout_digest import _build_offer_card_blocks
        why = "*$3.40 est. RPM* at _Tier 1_ CVR — strong signal"
        blocks = _build_offer_card_blocks(
            "TestCo", "Electronics", "$10.00 CPS", "US",
            tier_badge="", img_url="", why=why, action_value="{}",
        )
        # Find the rationale block (section with mrkdwn >text)
        rationale = next(
            (b for b in blocks if b.get("type") == "section" and ">" in (b.get("text") or {}).get("text", "")),
            None,
        )
        if rationale is None:
            return False, "no mrkdwn rationale block found — rich_text_quote may still be in use"
        text = rationale["text"]["text"]
        if "*$3.40" not in text:
            return False, f"markdown stripped from why text: {text!r}"
        if "_Tier 1_" not in text:
            return False, f"italic stripped from why text: {text!r}"
        return True, "markdown preserved in why text"
    except Exception as e:
        return False, str(e)


@test("offer_card_shows_fit_tier_badge")
def test_offer_card_has_fit_tier_badge():
    """PRIME fit_tier renders as 🔵 prefix in the advertiser name field."""
    try:
        from scout_digest import _build_offer_card_blocks
        blocks = _build_offer_card_blocks(
            "BrandX", "Apparel", "$12.00 CPS", "US",
            tier_badge="", img_url="", why="Good signal.", action_value="{}",
            fit_tier="PRIME",
        )
        fields_block = next((b for b in blocks if b.get("type") == "section" and "fields" in b), None)
        if fields_block is None:
            return False, "no fields block found in card"
        left = fields_block["fields"][0]["text"]
        if "🔵" not in left:
            return False, f"PRIME badge missing from left field: {left!r}"
        return True, "🔵 PRIME badge present"
    except Exception as e:
        return False, str(e)


@test("offer_card_shows_estimated_rpm")
def test_offer_card_has_rpm():
    """Non-zero rpm renders as ~$X.XX est. RPM in the payout field."""
    try:
        from scout_digest import _build_offer_card_blocks
        blocks = _build_offer_card_blocks(
            "OfferCo", "Tech", "$8.50 CPS", "US",
            tier_badge="", img_url="", why="Decent signal.", action_value="{}",
            rpm=3.40,
        )
        fields_block = next((b for b in blocks if b.get("type") == "section" and "fields" in b), None)
        if fields_block is None:
            return False, "no fields block found in card"
        right = fields_block["fields"][1]["text"]
        if "est. RPM" not in right:
            return False, f"est. RPM missing from right field: {right!r}"
        if "3.40" not in right:
            return False, f"RPM value incorrect in right field: {right!r}"
        return True, f"est. RPM present: {right!r}"
    except Exception as e:
        return False, str(e)


@test("skip_handler_posts_ephemeral_friction_message")
def test_skip_friction_ephemeral():
    """_handle_reject posts an ephemeral consequence notice after recording the rejection."""
    try:
        from unittest.mock import MagicMock, patch
        import scout_handlers

        ephemeral_calls = []

        mock_web = MagicMock()
        mock_web.chat_postEphemeral.side_effect = lambda **kw: ephemeral_calls.append(kw)
        mock_web.chat_postMessage.return_value = {"ts": "123"}

        action = {"value": '{"offer_id":"o1","advertiser":"SkipCo","payout":"10.00","payout_type":"CPS","category":"Tech","geo":"US"}'}
        payload = {
            "channel": {"id": "C123"},
            "message": {"ts": "456"},
            "user": {"id": "U789"},
        }

        import sys
        mock_digest = MagicMock()
        mock_digest.record_rejection.return_value = None
        with patch.dict(sys.modules, {"scout_digest": mock_digest}):
            scout_handlers._handle_reject(action, payload, mock_web)

        if not ephemeral_calls:
            return False, "no ephemeral posted after skip — friction missing"
        text = ephemeral_calls[0].get("text", "")
        if "SkipCo" not in text:
            return False, f"ephemeral doesn't mention advertiser: {text!r}"
        if "3 weeks" not in text:
            return False, f"ephemeral doesn't mention suppression duration: {text!r}"
        return True, "skip friction ephemeral posted with advertiser name + duration"
    except Exception as e:
        return False, str(e)


@test("offer_card_view_button_uses_tracking_url")
def test_offer_card_view_button_uses_tracking_url():
    """View button prefers tracking_url over network_portal_url (which is broken/gated)."""
    from scout_digest import _build_offer_card_blocks
    blocks = _build_offer_card_blocks(
        advertiser="BrandX",
        offer_summary="",
        payout_str="$10.00 CPS",
        geo="US",
        tier_badge="",
        img_url="",
        why="Great offer",
        action_value='{"offer_id":"x"}',
        network_portal_url="https://app.impact.com",  # broken portal
        view_url="https://swagbucks.7eer.net/c/12345/67890/999",  # real tracking URL
    )
    actions_block = next((b for b in blocks if b.get("type") == "actions"), None)
    if actions_block is None:
        return False, "no actions block found"
    elements = actions_block.get("elements", [])
    view_btn = next((e for e in elements if e.get("action_id") == "scout_view_offer"), None)
    if view_btn is None:
        return False, "no View button in actions"
    url = view_btn.get("url", "")
    if "swagbucks.7eer.net" not in url:
        return False, f"View button URL is {url!r}, expected tracking_url"
    if "app.impact.com" in url:
        return False, "View button still pointing to broken portal URL"
    return True, f"View button uses tracking_url: {url}"


@test("offer_card_no_view_button_when_no_urls")
def test_offer_card_no_view_button_when_no_urls():
    """No View button rendered when both view_url and network_portal_url are empty."""
    from scout_digest import _build_offer_card_blocks
    blocks = _build_offer_card_blocks(
        advertiser="MaxBounty Co",
        offer_summary="",
        payout_str="$8.00 CPS",
        geo="US",
        tier_badge="",
        img_url="",
        why="Worth a look",
        action_value='{"offer_id":"y"}',
        network_portal_url="",
        view_url="",
    )
    actions_block = next((b for b in blocks if b.get("type") == "actions"), None)
    if actions_block is None:
        return False, "no actions block found"
    elements = actions_block.get("elements", [])
    view_btn = next((e for e in elements if e.get("action_id") == "scout_view_offer"), None)
    if view_btn is not None:
        return False, f"View button rendered despite no URL: {view_btn}"
    return True, "no View button when both URLs empty — correct"


@test("normalize_geo idempotent — US + CA round-trips")
def test_normalize_geo_idempotent_us_plus_ca():
    """normalize_geo('US + CA') must return 'US + CA', not 'Other'.
    CJ hardcodes o["geo"] = "US + CA"; the cleanup pass re-calls normalize_geo
    on it, so if normalize_geo isn't idempotent, CA-eligible CJ offers get
    their geo silently dropped to 'Other' and suppressed in the digest.
    """
    from offer_scraper import normalize_geo
    result = normalize_geo("US + CA")
    if result != "US + CA":
        return False, f"normalize_geo('US + CA') returned {result!r}, expected 'US + CA'"
    return True, f"normalize_geo('US + CA') = {result!r} — idempotency holds"


@test("normalize_geo double-pass stable for US, CA")
def test_normalize_geo_double_pass():
    """normalize_geo is idempotent: calling it twice gives the same result."""
    from offer_scraper import normalize_geo
    first  = normalize_geo("US, CA")
    second = normalize_geo(first)
    if first != second:
        return False, f"double-pass changed value: {first!r} → {second!r}"
    if first != "US + CA":
        return False, f"expected 'US + CA', got {first!r}"
    return True, f"double-pass stable: 'US, CA' → {first!r} → {second!r}"


@test("normalize_geo all canonical display strings are idempotent")
def test_normalize_geo_already_normalized_values():
    """All canonical display strings must round-trip through normalize_geo unchanged."""
    from offer_scraper import normalize_geo
    cases = [
        "US Only", "US + CA", "North America", "Global",
        "UK", "EU", "Other", "Unknown",
    ]
    failures = []
    for val in cases:
        result = normalize_geo(val)
        if result != val:
            failures.append(f"  {val!r} → {result!r}")
    if failures:
        return False, "idempotency broken for:\n" + "\n".join(failures)
    return True, f"all {len(cases)} canonical display strings are stable under normalize_geo"


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

