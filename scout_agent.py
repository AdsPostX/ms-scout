"""
Scout — MomentScience Offer Intelligence Agent
Answers natural language questions about the offer inventory using Claude + tools.
Data source: data/offers_latest.json (written by offer_scraper.py after each daily run)
Performance benchmarks: queried from ClickHouse at startup, cached in memory.
"""

from __future__ import annotations

import copy
import difflib
import json
import logging
import os
import pathlib
import re
import threading
import time
import urllib.parse
import urllib.request
import datetime as _dt_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from zoneinfo import ZoneInfo
from types import MappingProxyType
from typing import Mapping, Optional
import anthropic
from scout_types import FormattedOffer, Brief  # type: ignore[import]  # noqa: F401
from dotenv import load_dotenv
import queries as _q
from scout_ch import (  # noqa: F401 — backward compat re-exports
    _run_parallel, _get_ch_client, _LoggingCHClient,
    _query_ghost_campaigns, _query_revenue_baseline,
    _query_intraday_revenue_total, _query_intraday_revenue_by_publisher,
    project_today_revenue,
    _query_advertiser_rpm_context,
    _query_cvr_anomaly, _query_expiring_campaigns,
    _query_publisher_revenue_trends, _query_advertiser_revenue_trends,
    _query_revenue_sparkline_series,
)
from scout_images import (  # noqa: F401 — backward compat re-exports
    _scrape_og_image, _clearbit_domain, _google_favicon, _app_store_icon,
    _validate_image_url, _load_image_cache, _save_image_cache,
    _cached_external_images, _store_image_cache, _ms_cdn_image,
)

load_dotenv()  # plist env vars (SCOUT_ENV, etc.) take precedence over .env

from scout_tools_definitions import TOOLS, SUPPORTED_NETWORKS  # noqa: E402

# Register the canonical geo normalizer with scout_core.contracts so any
# NormalizedOffer.normalize_geo(...) call resolves to the same implementation
# offer_scraper uses for Notion writes. Producers (this module + demand_feed_main)
# own this wiring — scout_core stays unaware of offer_scraper to keep the
# contracts layer import-cheap and dependency-free.
from scout_core.contracts import set_geo_normalizer as _set_geo_normalizer
from offer_scraper import normalize_geo as _normalize_geo
_set_geo_normalizer(_normalize_geo)

log = logging.getLogger("scout_agent")


# ── Environment config — read once at import, warn if required vars absent ────
_ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
_PULSE_ENABLED        = os.getenv("PULSE_ENABLED", "true").lower() == "true"
_SCOUT_SHADOW_CHANNEL = os.getenv("SCOUT_SHADOW_CHANNEL", "#scout-qa")

if not _ANTHROPIC_API_KEY:
    log.warning("[scout_agent] ANTHROPIC_API_KEY not set — Scout cannot respond to any queries")


# ── Part 4 (plan v3): typed boundary contract for ask() ──────────────────────
# Replaces the old Union[str, dict] return shape that left tools_called gated on
# a defensive isinstance check in scout_handlers, producing empty usage_log rows
# (see plan v3 §4, P1 boundary discipline). `payload` carries the legacy
# structured dispatch dict (brief / opportunities / text_with_context) so
# scout_handlers can keep rendering Slack UI from one source of truth.
def _freeze(value):
    """Recursively wrap dicts in MappingProxyType and lists in tuples so a
    handler can't mutate nested payload values (offers, copy, suggestions).
    Top-level MappingProxyType alone is shallow — CodeRabbit PR #69 caught this."""
    if isinstance(value, Mapping):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    return value


@dataclass(frozen=True)
class AskResult:
    text: str
    tools_called: tuple = ()
    duration_ms: int = 0
    payload: Optional[Mapping] = None
    chart_url: str = ""
    agent_steps: Optional[list] = None

    def __post_init__(self) -> None:
        # Defense-in-depth: callers may pass a list; coerce to tuple so handlers
        # cannot mutate telemetry mid-flight. Deep-freeze payload so nested
        # offers/copy/suggestions are also immutable (CodeRabbit on PR #69).
        if not isinstance(self.tools_called, tuple):
            object.__setattr__(self, "tools_called", tuple(self.tools_called))
        if self.payload is not None and not isinstance(self.payload, MappingProxyType):
            object.__setattr__(self, "payload", _freeze(dict(self.payload)))


def _extract_chart_url(result: object) -> str:
    """Return chart_url from a tool result dict, or empty string."""
    return result.get("chart_url", "") if isinstance(result, dict) else ""


from scout_thresholds import _manager, AmbiguousThresholdKey


def _is_admin(user_id: str) -> bool:
    """True if user_id is in SCOUT_THRESHOLD_ADMINS (comma-separated allowlist).

    Falls back to SCOUT_ADMIN_USER_ID (single-admin legacy env) so existing admins
    don't lose access. Empty user_id always returns False.
    """
    if not user_id:
        return False
    allow = {x.strip() for x in os.getenv("SCOUT_THRESHOLD_ADMINS", "").split(",") if x.strip()}
    legacy = os.getenv("SCOUT_ADMIN_USER_ID", "").strip()
    if legacy:
        allow.add(legacy)
    return user_id in allow


# Force-monitor injection — scout_bot sets these at startup so the force_run_monitor
# agent tool can invoke the same monitor lambdas that scout_handlers uses. None until
# scout_bot.main() calls _set_force_monitor_ctx().
_FORCE_MONITOR_CTX: dict = {"web": None, "ch_factory": None}


def _set_force_monitor_ctx(web, ch_factory) -> None:
    """Inject WebClient + ClickHouse factory so force_run_monitor can call monitor fns."""
    _FORCE_MONITOR_CTX["web"] = web
    _FORCE_MONITOR_CTX["ch_factory"] = ch_factory


SNAPSHOT_PATH = pathlib.Path(__file__).parent / "data" / "offers_latest.json"

# Compiled once at module level — strips <<<SUGGESTIONS [...]  SUGGESTIONS>>> blocks from responses
_SUGG_RE = re.compile(r'<<<SUGGESTIONS\s*(\[.*?\])\s*SUGGESTIONS>>>', re.DOTALL)

# ── Scout Score ───────────────────────────────────────────────────────────────


_PROMPT_PATH = pathlib.Path(__file__).parent / "prompts" / "scout_system.md"

# Compute git hash of the local prompt file at startup (zero-cost debug aid).
# If git is not on PATH (e.g. production Docker image), silently defaults to "".
try:
    import subprocess as _subprocess
    _PROMPT_SHA = _subprocess.run(
        ["git", "hash-object", str(_PROMPT_PATH)],
        capture_output=True, text=True, cwd=str(pathlib.Path(__file__).parent),
        timeout=3,
    ).stdout.strip()
except (FileNotFoundError, OSError, Exception):
    _PROMPT_SHA = ""


def _init_prompt() -> str:
    """
    Fetch the Scout system prompt from Latitude's managed prompt store if
    LATITUDE_PROMPT_PATH is set and latitude-sdk is installed.

    LATITUDE_PROMPT_PATH should be the Latitude prompt path, e.g. "scout/system".

    Falls back to the local prompts/scout_system.md file in all error cases:
    - ImportError (latitude-sdk not installed)
    - LATITUDE_PROMPT_PATH not set
    - Network error or timeout
    - Empty response from Latitude
    - Any unexpected exception

    Anthropic prompt-caching note: cache_control: ephemeral is keyed on prompt
    TEXT content, not source. After fetching from Latitude we compare the
    returned text against the local file (modulo trailing whitespace). A
    mismatch means Latitude normalised the content — every ask() will be a
    cache miss — and we log a SHA warning so the problem is visible.
    """
    _local_text = _PROMPT_PATH.read_text(encoding="utf-8")
    _prompt_path = os.environ.get("LATITUDE_PROMPT_PATH", "")
    if not _prompt_path:
        log.debug("[agent] LATITUDE_PROMPT_PATH not set — using local prompt file")
        return _local_text

    try:
        from latitude_sdk import Latitude, LatitudeOptions, GetPromptOptions  # type: ignore[import]
        _api_key = os.environ.get("LATITUDE_API_KEY", "")
        _project_id = os.environ.get("LATITUDE_PROJECT_ID", "")
        if not _api_key or not _project_id:
            log.debug("[agent] LATITUDE_API_KEY or LATITUDE_PROJECT_ID missing — using local prompt")
            return _local_text

        import asyncio as _asyncio

        async def _fetch() -> str:
            _lat = Latitude(_api_key, LatitudeOptions(project_id=int(_project_id)))
            _result = await _lat.prompts.get(
                _prompt_path,
                GetPromptOptions(project_id=int(_project_id)),
            )
            return str(_result.content or "")

        _fetched = _asyncio.run(_fetch())
        if not _fetched.strip():
            log.warning(
                "[agent] Latitude returned empty prompt (path=%s) — using local", _prompt_path
            )
            return _local_text

        # Whitespace normalisation guard: if Latitude changed the content the
        # Anthropic prompt cache will miss on every ask() call.
        import hashlib as _hashlib
        _fetched_sha = _hashlib.sha256(_fetched.encode()).hexdigest()[:12]
        _file_sha = _hashlib.sha256(_local_text.encode()).hexdigest()[:12]
        if _fetched.strip() != _local_text.strip():
            log.warning(
                "[agent] Latitude prompt content differs from local file — "
                "Anthropic cache will miss on every ask(). "
                "fetched_sha=%s local_sha=%s git_sha=%s",
                _fetched_sha, _file_sha, _PROMPT_SHA,
            )
        else:
            log.info(
                "[agent] prompt loaded from Latitude (path=%s git_sha=%s)",
                _prompt_path, _PROMPT_SHA,
            )
        return _fetched

    except ImportError:
        log.warning("[agent] latitude-sdk not installed — using local prompt file")
        return _local_text
    except Exception as exc:
        log.warning("[agent] Latitude prompt fetch failed: %s — using local file", exc)
        return _local_text


SYSTEM_PROMPT = _init_prompt()


# Tools the LLM should NOT auto-select — callable via TOOL_MAP by direct handlers only.
# Keeps the LLM-visible surface lean and prevents routing confusion on
# admin/write/redundant tools.
_INTERNAL_TOOLS: dict[str, dict] = {
    t["name"]: t
    for t in TOOLS
    if t["name"] in {
        "get_demand_queue_status",
        "mark_offer_launched",
        "get_usage_report",
        "export_usage_log",
        "record_entity_note",
        "forget_entity_note",
        "why_entity_note",
        "run_self_qa",
        "get_low_fill_publishers",
        "force_run_monitor",
    }
}

# Remove internal tools from the LLM-visible TOOLS list
TOOLS = [t for t in TOOLS if t["name"] not in _INTERNAL_TOOLS]

# ── Thread-level intent memory ────────────────────────────────────────────────
# Follow-up messages in the same Slack thread inherit the classified intent
# without re-classifying, keeping conversation context stable.
_THREAD_INTENTS: dict[str, str] = {}
_THREAD_INTENTS_LOCK = threading.Lock()

# ── Intent router ─────────────────────────────────────────────────────────────
# 9 intent buckets. Order matters: fleet_health MUST come before publisher_health
# so fleet queries don't get captured by the broader "how are/is" signals first.
_INTENT_ROUTER: dict[str, dict] = {
    "campaign_pacing": {
        "signals": [
            "campaign pacing", "projected revenue", "how much will",
            "advertiser revenue", "budget pace", "projection", "on track",
            "pace", "budget status",
        ],
        "primary_tools": [
            "get_advertiser_revenue_projection", "get_revenue_today_projection",
            "get_campaign_status", "get_ghost_campaigns",
        ],
        "context": (
            "You are answering a campaign pacing or revenue projection question. "
            "IMPORTANT: if the query mentions a specific advertiser name (e.g. Hulu, Impact, "
            "Disney+, TurboTax), use get_advertiser_revenue_projection — not "
            "get_revenue_today_projection. get_revenue_today_projection is only for "
            "platform-wide 'today' estimates with no named advertiser. "
            "Lead with the current vs expected pace. Flag any cap warnings. "
            "Show publisher breakdown if available. Always return visible output."
        ),
    },
    "fleet_health": {
        "signals": [
            "all publishers", "fleet health", "fleet", "publisher overview",
            "monday report", "publishers doing", "how are publishers", "publisher fleet",
        ],
        "primary_tools": [
            "get_publisher_fleet_health", "get_publisher_health",
        ],
        "context": (
            "You are answering a fleet-level publisher health question. "
            "Use get_publisher_fleet_health — it returns a formatted ranked fleet summary "
            "with at-risk publishers first. Always return visible output."
        ),
    },
    "publisher_health": {
        "signals": [
            "publisher health", "publisher performance", "publisher snapshot",
            "fill rate", "impressions down", "low fill", "how is", "how are", "showing up",
        ],
        "primary_tools": [
            "get_publisher_health", "get_publisher_revenue_trends", "get_publisher_fleet_health",
        ],
        "context": (
            "You are answering a publisher health question. Lead with the publisher name, "
            "current revenue vs expected, and fill rate. Surface anomalies first. "
            "Always return visible output."
        ),
    },
    "revenue_anomaly": {
        "signals": [
            "revenue down", "revenue drop", "anomaly", "spike", "unusual",
            "what happened", "why is revenue", "revenue alert", "revenue dip",
        ],
        "primary_tools": [
            "get_revenue_today", "get_exposure_rate_anomalies",
            "get_advertiser_revenue_trends", "get_publisher_revenue_trends",
        ],
        "context": (
            "You are diagnosing a revenue anomaly. Start with what changed and when. "
            "Surface the largest contributors to the delta. Suggest a root cause hypothesis. "
            "Always return visible output."
        ),
    },
    "offer_performance": {
        "signals": [
            "top performing offers", "performing offers", "top offers", "best offers",
            "offer performance", "offers ranking", "offer stats", "offer cvr", "which offers",
        ],
        "primary_tools": [
            "get_top_opportunities", "get_offer_stats",
            "get_category_performance", "get_running_offers",
        ],
        "context": (
            "You are answering an offer performance question. Rank offers by revenue "
            "contribution or CVR. Include offer name, payout, and network. "
            "Always return visible output."
        ),
    },
    "publisher_offer_fit": {
        "signals": [
            "offer fit", "right offers", "offers for publisher", "publisher offers",
            "offer match", "which offers for", "offers for",
        ],
        "primary_tools": [
            "get_offers_for_publisher", "get_publisher_competitive_landscape",
            "get_fallback_candidates",
        ],
        "context": (
            "You are answering a publisher-offer fit question. Show the top-fit offers "
            "for the publisher. Include CVR benchmark, payout, and why each offer fits. "
            "Always return visible output."
        ),
    },
    "traffic_quality": {
        "signals": [
            "traffic quality", "fraud", "invalid traffic", "ivt",
            "low quality", "bad traffic", "suspicious",
        ],
        "primary_tools": [
            "get_publisher_health", "get_exposure_rate_anomalies", "get_supply_demand_gaps",
        ],
        "context": (
            "You are investigating traffic quality. Surface CVR anomalies and fill rate "
            "outliers. Flag any publishers with unusual patterns. Always return visible output."
        ),
    },
    "ab_test": {
        "signals": [
            "a/b test", "ab test", "experiment", "test result", "variant",
            "control vs test", "which version",
        ],
        "primary_tools": [
            "get_perkswall_engagement", "get_offer_stats", "get_publisher_health",
        ],
        "context": (
            "You are answering an A/B test question. Compare the variants directly. "
            "Lead with which version is winning and by how much. Always return visible output."
        ),
    },
    "competitive_stack": {
        "signals": [
            "competitive", "competition", "other offers", "impression share",
            "competing offers", "what else is running", "what are others paying",
        ],
        "primary_tools": [
            "get_publisher_competitive_landscape", "get_supply_demand_gaps",
            "get_top_opportunities",
        ],
        "context": (
            "You are answering a competitive landscape question. Show what's competing for "
            "impressions at this publisher or in this category. Include payout comparison. "
            "Always return visible output."
        ),
    },
}


def _classify_intent(
    query: str,
    thread_ts: str | None = None,
) -> tuple[str | None, dict | None]:
    """
    Classify a query into an intent bucket for tool narrowing.

    Returns (intent_name, intent_dict) or (None, None) if no match.
    Thread memory: follow-up messages in the same Slack thread inherit the
    classified intent without re-classifying, keeping conversation context stable.
    Signals are sorted longest-first so specific phrases beat short generic ones.
    """
    q = query.lower()

    # Signal matching — longest signals first so specific phrases beat short ones
    for intent_name, intent_dict in _INTENT_ROUTER.items():
        signals = sorted(intent_dict["signals"], key=len, reverse=True)
        for signal in signals:
            if signal in q:
                # Update thread memory whenever a signal matches
                if thread_ts:
                    with _THREAD_INTENTS_LOCK:
                        _THREAD_INTENTS[thread_ts] = intent_name
                return intent_name, intent_dict

    # No signal matched — fall back to thread memory for ambiguous follow-ups
    if thread_ts:
        with _THREAD_INTENTS_LOCK:
            if thread_ts in _THREAD_INTENTS:
                name = _THREAD_INTENTS[thread_ts]
                return name, _INTENT_ROUTER.get(name)

    return None, None


# ── Tool implementations ─────────────────────────────────────────────────────


# Risk flags: keyed by trigger keyword lists.
# Shown on the brief to flag post-transaction fit issues before launch.


_TRACKING_DOMAINS = {
    # Known affiliate tracking domains — URLs on these are real tracking links
    "impact.com", "sjv.io", "pxf.io", "bn5x.net", "ibfwsl.net",
    "maxbounty.com", "flexoffers.com", "jdoqocy.com", "tkqlhce.com",
    "launchingdeals.com", "adspostx.com", "pubtailer.com",
    "collectsavings.com", "referral.", "go.",
}

_CLICK_ID_PATTERNS = ("{click_id}", "{subid}", "subId", "clickid", "click_id", "aff_id")


# ── Brand image sourcing ──────────────────────────────────────────────────────
# Auto-sources logo + icon so brief creators don't have to hunt for assets.
#
# Priority chain (never fake — empty string beats a wrong/blurry image):
#   hero_url: MS CDN  →  App Store 512px  →  Google gstatic 256px  →  ""
#   icon_url: MS CDN  →  Google gstatic 256px  →  App Store 512px  →  ""
#
# Why these two sources:
#   - App Store (itunes.apple.com/search): 512x512 standardized square icon,
#     brand colors, no API key, most major advertisers have an iOS app.
#   - Google gstatic favicon: resolves from domain, no API key, 256px option,
#     best for the 24px icon slot where quality matters less.
#   - Clearbit logo API (logo.clearbit.com): defunct — DNS returns NXDOMAIN.
#     Clearbit autocomplete still works and gives us the domain for gstatic.


# ── Demand Queue lifecycle tools ─────────────────────────────────────────────


def get_scout_config() -> dict:
    """
    Return Scout's current active configuration (PR 17b).

    Sources:
      - SCOUT_THRESHOLDS — loaded from config/scout_thresholds.json at module import
      - SUPPORTED_NETWORKS — single source of supported affiliate networks
      - Pulse schedule — read from env (PULSE_ENABLED) + the canonical 8am CT slot
      - Live network list — derived from offers_latest.json keyset (PR 16a)

    Returns a flat dict the agent renders into a Slack-friendly summary card.
    Caller is the Claude agent — it formats the response per SYSTEM_PROMPT rules.
    """
    try:
        # Live network keyset from current inventory
        try:
            from scout_digest import _DIGEST_NETWORKS as _live_networks  # type: ignore
            live_networks = list(_live_networks)
        except Exception as e:
            log.debug("get_scout_config live_networks swallowed: %s", e)
            live_networks = []

        # Override metadata (PR-B): which thresholds have runtime overrides
        overridden_keys: list[str] = []
        last_override_at: str = ""
        try:
            import scout_state
            ov = scout_state._load_threshold_overrides() or {}
            for section, keys in ov.items():
                if not isinstance(keys, dict):
                    continue
                for key, entry in keys.items():
                    if isinstance(entry, dict) and "value" in entry:
                        overridden_keys.append(f"{section}.{key}")
                        ts = entry.get("set_at", "")
                        if ts and ts > last_override_at:
                            last_override_at = ts
        except Exception as e:
            log.debug("get_scout_config override metadata swallowed: %s", e)

        return {
            "thresholds": _manager.load(),
            "supported_networks": list(SUPPORTED_NETWORKS),
            "active_networks_in_inventory": live_networks,
            "pulse": {
                "enabled": _PULSE_ENABLED,
                "schedule": "8am CT daily",
                "opportunities_displayed": "Mondays only (computed daily)",
            },
            "config_file": str(_manager._thresholds_file.relative_to(pathlib.Path(__file__).parent)),
            "overridden_keys": overridden_keys,
            "last_override_at": last_override_at,
            "data_quality": _manager.data_quality_tier(days_of_data=999),
        }
    except Exception as e:
        log.warning(f"get_scout_config failed: {e}")
        return {"error": str(e), "thresholds": {}}


# ── Threshold control surface (PR-B) ─────────────────────────────────────────
# Four tools the agent calls when admins ask Scout to tune monitor thresholds:
#   list_thresholds        — read-only, anyone
#   get_threshold_history  — read-only, anyone
#   set_threshold          — admin-only (SCOUT_THRESHOLD_ADMINS)
#   force_run_monitor      — admin-only

def list_thresholds() -> dict:
    """Return all active thresholds (after override merge) plus override metadata.

    Anyone can call. Mirrors get_scout_config but trimmed to threshold concerns.
    """
    try:
        import scout_state
        ov = scout_state._load_threshold_overrides() or {}
    except Exception as e:
        log.warning(f"list_thresholds override read failed: {e}")
        ov = {}

    overridden: dict = {}
    for section, keys in ov.items():
        if not isinstance(keys, dict):
            continue
        for key, entry in keys.items():
            if isinstance(entry, dict) and "value" in entry:
                overridden[f"{section}.{key}"] = {
                    "value": entry.get("value"),
                    "set_by": entry.get("set_by", ""),
                    "set_at": entry.get("set_at", ""),
                    "reason": entry.get("reason", ""),
                }

    return {
        "thresholds": _manager.load(),
        "overridden": overridden,
        "config_file": str(_manager._thresholds_file.relative_to(pathlib.Path(__file__).parent)),
        "override_file": "data/threshold_overrides.json",
    }


def get_threshold_history(key: str = "", limit: int = 50) -> dict:
    """Return recent threshold change events from data/threshold_changelog.jsonl.

    key: optional filter like 'signals.cap_alert_pct' — exact match against entry["key"].
    limit: max entries (newest first). Default 50.
    """
    try:
        import scout_state
        entries = scout_state._read_threshold_changelog(limit=max(1, min(int(limit or 50), 500)),
                                                       key=(key or None))
        return {"entries": entries, "count": len(entries), "filter": key or "all"}
    except Exception as e:
        log.warning(f"get_threshold_history failed: {e}")
        return {"error": str(e), "entries": [], "count": 0}


def set_threshold(section: str = "", key: str = "", value=None, reason: str = "",
                  _caller_user_id: str = "") -> dict:
    """Admin-only: write a runtime override for one threshold and reload the cache."""
    if not _is_admin(_caller_user_id):
        return {"ok": False, "error": "not_admin",
                "message": ":lock: Threshold changes are admin-only (set SCOUT_THRESHOLD_ADMINS)."}
    return _manager.set_threshold(
        section=section, key=key, value=value, reason=reason,
        _caller_user_id=_caller_user_id,
    )


def force_run_monitor(monitor: str = "", _caller_user_id: str = "") -> dict:
    """Admin-only: invoke any registered monitor immediately.

    Uses the same lambda registry as scout_handlers' force commands.
    Auto-discovers registered monitors so new _set_force_monitor_fn registrations
    are available here without any code change.
    """
    if not _is_admin(_caller_user_id):
        return {"ok": False, "error": "not_admin",
                "message": ":lock: Force-run is admin-only (set SCOUT_THRESHOLD_ADMINS)."}

    web = _FORCE_MONITOR_CTX.get("web")
    if web is None:
        return {"ok": False, "error": "not_initialized",
                "message": "Force-monitor context not injected yet — Scout still warming up."}

    name = _norm(monitor)
    import scout_handlers
    allowed = set(scout_handlers._FORCE_MONITOR_FNS.keys())
    if name not in allowed:
        return {"ok": False, "error": "unknown_monitor",
                "message": f"monitor must be one of: {sorted(allowed)}"}

    try:
        fn = scout_handlers._FORCE_MONITOR_FNS.get(name)
        if fn is None:
            return {"ok": False, "error": "not_registered",
                    "message": f"Monitor '{name}' not registered (scout_bot startup may have skipped it)."}
        fn(web, _SCOUT_SHADOW_CHANNEL, "")
        return {"ok": True, "monitor": name, "by_user_id": _caller_user_id,
                "message": f"Force-ran {name} monitor — results posted to {_SCOUT_SHADOW_CHANNEL}."}
    except Exception as e:
        log.warning(f"force_run_monitor({name}) failed: {e}")
        return {"ok": False, "error": "execution_failed", "message": str(e)}


_LARGE_TABLES = (
    "adpx_sdk_sessions",
    "adpx_impressions_details",
    "adpx_tracked_clicks",
    "adpx_conversionsdetails",
)


_POST_TX_PLACEMENTS = (
    "'checkout_confirmation_page'", "'order_confirmation'", "'order-confirmation'",
    "'buy_flow_thank_you'", "'buyflowthankyou'", "'acctmgmt_payment_confirmation'",
    "'acctmgmtpaymentconfirmation'", "'receipt'", "'visit-receipt'", "'visit_receipt'",
    "'parking_pass_receipt'", "'order-receipt'", "'receipt-parkingdotcom'",
    "'post_checkout_receipt'", "'post_transaction'", "'post_transaction_page'",
    "'metropolis_transaction_details'", "'7eleven-fuel-transactionreceipt-bottom'",
    "'7Eleven_Fuel_TransactionReceipt_Bottom'", "'conv-orderconfirmation'",
    "'thank_you'", "'message_confirmation'", "'registration_complete'",
    "'order_status_offers'",
)


# ── Domain tool imports (explicit — no wildcard) ─────────────────────────────
from scout_tools_revenue import (
    get_pulse_summary,
    get_advertiser_revenue_projection,
    get_top_revenue_opportunities,
    get_revenue_today,
    get_revenue_today_projection,
    get_publisher_revenue_trends,
    get_advertiser_revenue_trends,
)
from scout_tools_publisher import (
    get_publisher_competitive_landscape,
    get_publisher_health,
    get_perkswall_engagement,
    get_low_fill_publishers,
    get_offers_for_publisher,
    get_exposure_rate_anomalies,
    get_publisher_fleet_health,
)
from scout_tools_campaigns import (
    get_queue_status,
    get_demand_queue_status,
    mark_offer_launched,
    get_ghost_campaigns,
    get_campaign_status,
    get_expiring_campaigns,
)
from scout_tools_offers import (
    search_offers,
    get_top_opportunities,
    get_running_offers,
    get_category_performance,
    get_offer_stats,
    draft_campaign_brief,
    get_fallback_candidates,
    get_supply_demand_gaps,
    get_pipeline_health,
    run_offer_scraper,
    # Private helpers re-exported for backward-compat (scout_digest + smoke_test import these)
    _scout_score,
    _network_portal_url,
    _norm,  # re-exported — scout_handlers imports _norm from scout_agent
)
from scout_tools_admin import (
    run_sql_query,
    get_scout_status,
    get_usage_report,
    export_usage_log,
    record_entity_note,
    forget_entity_note,
    why_entity_note,
    run_self_qa,
    # Re-exported for backward-compat (smoke_test imports _QA_SUITE from scout_agent)
    _QA_SUITE,
)


# ── Tool dispatch ─────────────────────────────────────────────────────────────

TOOL_MAP = {
    "search_offers": search_offers,
    "get_top_opportunities": get_top_opportunities,
    "get_running_offers": get_running_offers,
    "get_category_performance": get_category_performance,
    "get_offer_stats": get_offer_stats,
    "draft_campaign_brief": draft_campaign_brief,
    "get_publisher_competitive_landscape": get_publisher_competitive_landscape,
    "get_fallback_candidates": get_fallback_candidates,
    "get_queue_status": get_queue_status,
    "get_demand_queue_status": get_demand_queue_status,
    "mark_offer_launched": mark_offer_launched,
    "get_revenue_today": get_revenue_today,
    "get_revenue_today_projection": get_revenue_today_projection,
    "get_publisher_health": get_publisher_health,
    "get_campaign_status": get_campaign_status,
    "get_perkswall_engagement": get_perkswall_engagement,
    "get_supply_demand_gaps": get_supply_demand_gaps,
    "run_sql_query": run_sql_query,
    "get_scout_status": get_scout_status,
    "get_advertiser_revenue_projection": get_advertiser_revenue_projection,
    "get_ghost_campaigns": get_ghost_campaigns,
    "get_low_fill_publishers": get_low_fill_publishers,
    "get_top_revenue_opportunities": get_top_revenue_opportunities,
    "run_offer_scraper": run_offer_scraper,
    "get_pipeline_health": get_pipeline_health,
    "get_usage_report": get_usage_report,
    "export_usage_log": export_usage_log,
    "record_entity_note": record_entity_note,
    "forget_entity_note": forget_entity_note,
    "why_entity_note": why_entity_note,
    "get_offers_for_publisher": get_offers_for_publisher,
    "get_pulse_summary": get_pulse_summary,
    "get_exposure_rate_anomalies": get_exposure_rate_anomalies,
    "get_expiring_campaigns": get_expiring_campaigns,
    "get_publisher_revenue_trends": get_publisher_revenue_trends,
    "get_advertiser_revenue_trends": get_advertiser_revenue_trends,
    "get_publisher_fleet_health": get_publisher_fleet_health,
    "get_scout_config": None,   # registered below after function definition
    "run_self_qa": run_self_qa,  # registered below after function definition
}


# ── Self-QA suite ─────────────────────────────────────────────────────────────

# Every major intent Scout supports, plus data-boundary probes.
# Format: (label, question, pass_hints)
# pass_hints: strings that should appear in a passing response (any one match = pass)
TOOL_MAP["get_scout_config"] = get_scout_config
TOOL_MAP["list_thresholds"] = list_thresholds
TOOL_MAP["get_threshold_history"] = get_threshold_history
TOOL_MAP["set_threshold"] = set_threshold
TOOL_MAP["force_run_monitor"] = force_run_monitor

_TOOL_STEP_LABELS: dict[str, str] = {
    # Revenue
    "get_revenue_today": "Revenue check",
    "get_revenue_today_projection": "Revenue projection",
    "get_advertiser_revenue_projection": "Advertiser projection",
    "get_advertiser_revenue_trends": "Advertiser trends",
    "get_publisher_revenue_trends": "Publisher trends",
    "get_top_revenue_opportunities": "Revenue opportunities",
    # Signals
    "get_ghost_campaigns": "Ghost campaigns",
    "get_low_fill_publishers": "Fill rate",
    "get_exposure_rate_anomalies": "Exposure anomalies",
    "get_publisher_fleet_health": "Fleet health",
    # Publisher
    "get_publisher_health": "Publisher health",
    "get_publisher_competitive_landscape": "Competitive landscape",
    "get_offers_for_publisher": "Publisher offers",
    "get_perkswall_engagement": "Perkswall engagement",
    # Campaigns / demand
    "get_campaign_status": "Campaign status",
    "get_expiring_campaigns": "Expiring campaigns",
    "get_top_opportunities": "Opportunity scan",
    "get_supply_demand_gaps": "Supply/demand gaps",
    "get_fallback_candidates": "Fallback candidates",
    "get_demand_queue_status": "Demand queue",
    "get_queue_status": "Queue status",
    # Offers
    "search_offers": "Offer search",
    "get_running_offers": "Running offers",
    "get_offer_stats": "Offer stats",
    "get_category_performance": "Category performance",
    # Ops / admin
    "draft_campaign_brief": "Campaign brief",
    "get_usage_report": "Usage report",
    "get_pulse_summary": "Pulse summary",
    "get_scout_status": "Scout status",
    "get_pipeline_health": "Pipeline health",
    # Free-form SQL
    "run_sql_query": "Run SQL",
}

_TOOL_SKIP_SYNTHESIS: frozenset[str] = frozenset({
    "force_run_monitor",
    "get_scout_config",
    "list_thresholds",
    "set_threshold",
    "get_threshold_history",
})


def _synthesize_agent_steps(tool_call_log: list[tuple[str, any]]) -> list[dict]:
    """Derive agent steps from raw tool call log — sole source of the reasoning chain."""
    steps = []
    for name, result in tool_call_log:
        if name in _TOOL_SKIP_SYNTHESIS:
            continue
        label = _TOOL_STEP_LABELS.get(name) or name.removeprefix("get_").replace("_", " ").title()
        if isinstance(result, str) and (result.lower().startswith("error") or "failed" in result.lower()):
            status = "fail"
        elif isinstance(result, dict) and ("error" in result or "err" in result):
            status = "fail"
        elif not result or result == {} or result == []:
            status = "warn"
        else:
            status = "pass"
        if isinstance(result, dict) and "formatted" in result:
            raw = str(result["formatted"])
            finding = next((ln.strip() for ln in raw.splitlines() if ln.strip()), raw)[:80]
        elif isinstance(result, list):
            finding = f"{len(result)} result{'s' if len(result) != 1 else ''}"
        elif isinstance(result, dict):
            if status == "fail":
                # Show the actual error, not the next non-error key (e.g. sql_run: SELECT...)
                err = result.get("error") or result.get("err") or ""
                finding = str(err)[:80] if err else "—"
            else:
                if "finding" in result:
                    finding = str(result["finding"])[:80]
                elif "summary" in result:
                    finding = str(result["summary"])[:80]
                else:
                    finding = next(
                        (f"{k}: {v}" for k, v in result.items()
                         if k not in {"error", "err", "raw"} and not isinstance(v, (list, dict))),
                        "—"
                    )[:80]
        else:
            # For formatted strings, skip pure title lines (e.g. "*Report Title*") —
            # they repeat the label. Use the first content line instead.
            lines = [ln.strip() for ln in str(result).splitlines() if ln.strip()]
            content = next(
                (ln for ln in lines if not (ln.startswith("*") and ln.endswith("*") and "·" not in ln and "%" not in ln and "$" not in ln and not any(c.isdigit() for c in ln))),
                lines[0] if lines else str(result)
            )
            finding = content[:80]
        steps.append({"label": label, "status": status, "finding": finding})

    # Collapse consecutive same-label/same-status runs (e.g. 5× SQL retry) into one step
    if len(steps) > 1:
        collapsed = [dict(steps[0])]
        for step in steps[1:]:
            prev = collapsed[-1]
            if prev["label"] == step["label"] and prev["status"] == step["status"]:
                n = prev.get("_count", 1) + 1
                collapsed[-1] = {**prev, "_count": n, "finding": f"×{n}: {step['finding']}"}
            else:
                collapsed.append(dict(step))
        steps = [{k: v for k, v in s.items() if k != "_count"} for s in collapsed]

    return steps


def _run_tool(name: str, inputs: dict, _caller_user_id: str = "",
              _caller_permalink: str = ""):
    fn = TOOL_MAP.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    # Inject caller identity for admin-gated tools so the model doesn't need to
    # manually extract and pass the user_id from the injected context prefix.
    if name in {"get_usage_report", "export_usage_log"} and not inputs.get("requesting_user_id"):
        inputs = {**inputs, "requesting_user_id": _caller_user_id}
    # Plan v3 §3.4: inject Slack provenance (caller user_id + permalink) into
    # entity-note tools so `added_by` and `permalink` are populated automatically.
    if name in {"record_entity_note", "forget_entity_note"}:
        inputs = {**inputs, "_caller_user_id": _caller_user_id,
                  "_caller_permalink": _caller_permalink}
    # PR-B: admin-gated threshold control surface — inject caller identity so the
    # tool can check SCOUT_THRESHOLD_ADMINS without the LLM supplying user_id.
    if name in {"set_threshold", "force_run_monitor"}:
        inputs = {**inputs, "_caller_user_id": _caller_user_id}
    try:
        return fn(**inputs)
    except Exception as e:
        # Catch CHBusyError (and any other CH-layer signal) so a saturated
        # ClickHouse pool surfaces as a tool-level error the LLM can summarize
        # — not an uncaught exception that kills the whole agent turn.
        from scout_ch import CHBusyError
        if isinstance(e, CHBusyError):
            log.warning(f"_run_tool({name}): CH busy — {e}")
            return {"error": "ClickHouse is under pressure right now; the query was queued past the timeout. Try again in a minute or narrow the scope."}
        raise


# ── Agent loop ────────────────────────────────────────────────────────────────

def _extract_copy_from_text(text: str) -> dict:
    """
    Parse titles, CTAs, targeting, and bottom line from Claude's plain-text brief output.
    Used as fallback when Claude doesn't emit <<<BRIEF_JSON>>>.
    """
    copy: dict = {"titles": [], "ctas": [], "targeting": "", "bottom_line": ""}

    # Titles: numbered list — "1. text" or "1) text"
    title_matches = re.findall(r'(?:^|\n)\s*\d+[.)]\s+(.+?)(?=\n\s*\d+[.)]|\n\n|$)', text, re.MULTILINE)
    copy["titles"] = [t.strip() for t in title_matches[:3] if t.strip()]

    # CTAs: Yes: "..." / No: "..."
    cta_matches = re.findall(r'[Yy]es:\s*["\u201c]([^"\u201d]+)["\u201d]\s*/\s*[Nn]o:\s*["\u201c]([^"\u201d]+)["\u201d]', text)
    copy["ctas"] = [{"yes": y.strip(), "no": n.strip()} for y, n in cta_matches[:2]]

    # Targeting: line starting with "Targeting:"
    targeting_match = re.search(r'[Tt]argeting:\s*(.+?)(?=\n[A-Z*_]|\n\n|$)', text, re.DOTALL)
    if targeting_match:
        copy["targeting"] = targeting_match.group(1).strip()[:300]

    # Bottom line: last substantive paragraph (not the "Reply @Scout..." instruction)
    paragraphs = [p.strip() for p in text.split("\n") if p.strip() and "Reply" not in p and "launch this" not in p.lower()]
    if paragraphs:
        copy["bottom_line"] = paragraphs[-1][:200]

    return copy


def _extract_thread_entities(tool_results: list) -> dict:
    """
    Extract named entities from tool results accumulated during an agent turn.
    Tool-agnostic — works across get_publisher_competitive_landscape,
    draft_campaign_brief, search_offers, get_running_offers, etc.
    Returns a flat dict of resolved entities (empty if nothing found).
    scout_bot.py merges this into thread_context.json so follow-ups like
    "@Scout yes, $50 CPA" work without repeating publisher/offer/payout.
    """
    ctx: dict = {}
    for result in tool_results:
        if not isinstance(result, dict):
            # List results (e.g. search_offers) — grab category from first item
            if isinstance(result, list) and result:
                first = result[0] if isinstance(result[0], dict) else {}
                if first.get("category"):
                    ctx["category"] = first["category"]
            continue

        # Publisher competitive landscape
        if "publisher_id" in result:
            if result.get("publisher"):
                ctx["publisher"]    = result["publisher"]
            if result.get("publisher_id") is not None:
                ctx["publisher_id"] = result["publisher_id"]
            scenario = result.get("payout_scenario") or {}
            if scenario.get("offer"):
                ctx["offer"] = scenario["offer"]
            if scenario.get("current_payout") is not None:
                ctx["payout"]      = scenario["current_payout"]
                ctx["payout_type"] = "CPA"
            if scenario.get("hypothetical_payout") is not None:
                ctx.setdefault("scenarios_run", [])
                hyp = scenario["hypothetical_payout"]
                if hyp not in ctx["scenarios_run"]:
                    ctx["scenarios_run"].append(hyp)

        # Campaign brief (draft_campaign_brief result)
        if result.get("advertiser"):
            ctx["offer"] = result["advertiser"]
            if result.get("payout_num") is not None:
                ctx["payout"] = result["payout_num"]
            ptype = (result.get("payout_type") or "").upper()
            if ptype:
                ctx["payout_type"] = ptype

        # Offer search / running offers — capture category signal
        if result.get("category"):
            ctx["category"] = result["category"]

        # mark_offer_launched result — scout_bot.py posts the launch notification
        if result.get("status") == "launched" and result.get("advertiser"):
            ctx["launched_offer"] = result

    return {k: v for k, v in ctx.items() if v is not None}


def _select_model(user_message: str) -> str:
    """
    Route queries to the right model based on complexity.

    Haiku  → simple single-intent lookups (fast, cheap)
    Sonnet → everything else, including all multi-part questions

    Multi-part questions (3+ question marks, bullet lists, prep-for-call context)
    always go to Sonnet — they require decomposition, synthesis across multiple
    tool results, and graceful handling of unanswerable sub-questions.
    """
    msg = user_message.lower()

    # Multi-part signals → always Sonnet regardless of other signals
    question_count = msg.count("?")
    has_bullets = any(line.strip().startswith(("•", "-", "*")) for line in msg.splitlines())
    has_prep_context = any(p in msg for p in ["call tomorrow", "prep", "review", "questions from", "help me answer"])
    if question_count >= 3 or has_bullets or has_prep_context:
        return "claude-sonnet-4-6"

    simple = ["status", "queue", "is scout", "help", "paused", "active",
              "how many", "count", "list all", "what is the cap"]
    complex_ = ["health", "competitive", "projection", "revenue", "rpm",
                "trend", "brief", "compare", "analyze", "performance",
                "velocity", "benchmark", "opportunity", "why"]
    if sum(1 for p in simple if p in msg) > sum(1 for p in complex_ if p in msg):
        return "claude-haiku-4-5"
    return "claude-sonnet-4-6"


# ── Deterministic pre-router ─────────────────────────────────────────────────
# Handles only structured set_threshold commands. All other queries go to the LLM.

_SET_RE_FULL  = re.compile(
    r"^set\s+([\w.]+)\s+to\s+(-?\d+(?:\.\d+)?|true|false)\s+because\s+(.+)$",
    re.IGNORECASE,
)
_SET_RE_SHORT = re.compile(
    r"^set\s+([\w.]+)\s+to\s+(-?\d+(?:\.\d+)?|true|false)\s*$",
    re.IGNORECASE,
)


def _format_dict_response(title: str, data: dict) -> str:
    """Slack-friendly key-value dump for control-surface tool results.

    Fenced code blocks are intentionally avoided: _escape_md_code() in
    scout_ui_kit collapses them to their first content line (the opening '{'),
    which would render the entire dict as a lone '{' in Slack.
    """
    def _fmt(v: object) -> str:
        if isinstance(v, dict):
            return " · ".join(f"{k}: {v2}" for k, v2 in v.items())
        if isinstance(v, list):
            joined = ", ".join(str(x) for x in v[:8])
            return joined + ("…" if len(v) > 8 else "")
        return str(v)

    lines = [f"*{title}*"]
    for key, val in (data or {}).items():
        lines.append(f"• *{key}*: {_fmt(val)}")
    return "\n".join(lines)


_NETWORK_DISPLAY_NAMES: dict[str, str] = {
    "cj":         "CJ",
    "maxbounty":  "MaxBounty",
    "impact":     "Impact",
    "flexoffers": "FlexOffers",
    "rakuten":    "Rakuten",
    "awin":       "Awin",
    "shareasale": "ShareASale",
    "pepperjam":  "Pepperjam",
}


def _route_deterministic(user_message: str, user_id: str, on_stage=None) -> Optional[AskResult]:
    """Match raw user text against control-surface verbs and execute directly.

    Returns AskResult on hit; None to let the LLM handle the query. Must be
    called on the RAW message (no date/caller/channel prefix) so exact-match
    phrases like `alert thresholds` survive.
    """
    raw = re.sub(r"<@[A-Z0-9]+>", "", user_message or "").strip()
    if not raw:
        return None

    # set_threshold — admin-gated, deterministic arg parsing
    m = _SET_RE_FULL.match(raw)
    if m:
        dotted, raw_val, reason = m.group(1), m.group(2), m.group(3).strip()
        try:
            section, key = _manager._split_key(dotted)
        except AmbiguousThresholdKey as exc:
            return AskResult(
                text=f":warning: {exc}",
                tools_called=(), duration_ms=0,
            )
        value = _manager.coerce_value(raw_val)
        result = set_threshold(section=section, key=key, value=value,
                               reason=reason, _caller_user_id=user_id)
        if result.get("ok"):
            text = (f":white_check_mark: `{section}.{key}` set: "
                    f"`{result.get('prior')}` → `{result.get('value')}` "
                    f"(reason: {result.get('reason')}).")
        else:
            text = f":warning: {result.get('message') or result.get('error', 'set_threshold failed')}"
        return AskResult(text=text, tools_called=("set_threshold",), duration_ms=0)

    if _SET_RE_SHORT.match(raw):
        return AskResult(
            text=(":warning: Reason required: please re-send as "
                  "`set <key> to <value> because <reason>`."),
            tools_called=(), duration_ms=0,
        )

    return None


def _build_prefix_context(user_id: str, user_tz: str) -> str:
    """Build date/caller/corrections prefix prepended to user_message.

    Pure helper extracted from ask() so other entry points (e.g. attachment-bearing
    variants) can reuse identical context construction without duplicating logic.
    """
    corrections_ctx = _manager.corrections_context()
    caller_ctx      = f"[Caller Slack user_id: {user_id}]\n" if user_id else ""
    # Inject current CT date/time so the model never guesses "today" from UTC.
    # CT is the data anchor (all ClickHouse data is America/Chicago). If the
    # caller's Slack timezone differs, append their local time as a second line
    # so the model correctly interprets natural language like "this afternoon".
    _now_ct = datetime.now(ZoneInfo("America/Chicago"))
    _ct_date_str = f"{_now_ct.strftime('%A, %B')} {_now_ct.day}, {_now_ct.year}"
    _ct_time_str = _now_ct.strftime("%I:%M%p").lstrip("0").lower()
    date_ctx = (
        f"[Business date: {_ct_date_str} (America/Chicago) — "
        f"all revenue/data reporting uses this date; "
        f"current CT time: {_ct_time_str} ct]\n"
    )
    if user_tz and user_tz != "America/Chicago":
        try:
            _now_user = datetime.now(ZoneInfo(user_tz))
            _tz_abbr = _now_user.strftime("%Z")
            _user_time_str = _now_user.strftime("%I:%M%p").lstrip("0").lower()
            date_ctx += (
                f"[Requester's local time: {_user_time_str} "
                f"{_tz_abbr} ({user_tz}) — use this when interpreting relative "
                f"time references like 'this morning', 'tonight', '5pm']\n"
            )
        except Exception:
            pass  # unknown tz string — skip, CT context is still present
    return date_ctx + caller_ctx + corrections_ctx


def _build_initial_messages(user_message: str, history: list | None, prefix: str) -> list[dict]:
    """Build the Anthropic messages array: history + prefix-augmented user turn.

    Pure function — returns a fresh list, does not mutate inputs.
    """
    effective_message = (prefix + user_message) if prefix else user_message
    return list(history or []) + [{"role": "user", "content": effective_message}]


def _run_tool_loop(
    messages: list,
    client,
    system_prompt: str,
    intent_name,
    intent_dict,
    ask_tools,
    _start_ms: float,
    _tools_called: list,
    user_message: str = "",
    _brief_results: list | None = None,
    _opportunity_offers: list | None = None,
    _all_tool_results: list | None = None,
    user_id: str = "",
    permalink: str = "",
    on_stage=None,
) -> AskResult:
    """Run the bounded tool-use loop and synthesize the final AskResult.

    All previously-closure-scoped state from ask() is passed explicitly. Mutable
    accumulators default to None (instantiated locally) so callers can supply
    pre-seeded lists or rely on per-call lists.
    """
    def _dur() -> int:
        return int((time.monotonic() - _start_ms) * 1000)

    if _brief_results is None:
        _brief_results = []
    if _opportunity_offers is None:
        _opportunity_offers = []
    if _all_tool_results is None:
        _all_tool_results = []
    _tool_call_log: list[tuple[str, any]] = []
    _chart_url: str = ""

    # user_message is the RAW pre-prefix string — passed explicitly so the
    # MAX_ROUNDS warning log line matches pre-refactor behavior (logging the
    # user's actual question, not the prefix-augmented effective_message).

    _intent_name, _intent_dict = intent_name, intent_dict
    _ask_tools = ask_tools

    MAX_ROUNDS = 12  # hard cap — prevents runaway loops on complex / ambiguous queries
    _round = 0

    if _intent_dict:
        # Two-block system array: intent context is small/dynamic (no cache),
        # SYSTEM_PROMPT is large/static (always cached). Keeps one shared cache
        # entry for SYSTEM_PROMPT regardless of which intent bucket fires.
        _system_blocks = [
            {"type": "text", "text": _intent_dict["context"]},
            {"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}},
        ]
    else:
        _system_blocks = [
            {"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}},
        ]

    _stage_labels = None  # lazy import to avoid circular at module load

    while _round < MAX_ROUNDS:
        _round += 1
        if _round > 1 and on_stage:
            try:
                on_stage("")  # clear tool label — rotating falls back to cycling
            except Exception:
                pass
        for attempt in range(4):
            try:
                response = client.messages.create(
                    model=_select_model(user_message),
                    max_tokens=4096,
                    cache_control={"type": "ephemeral"},  # automatic caching for growing message history
                    system=_system_blocks,
                    tools=_ask_tools,
                    messages=messages,
                )
                break
            except anthropic.APIConnectionError:
                if attempt < 3:
                    wait = 2 ** attempt
                    log.warning(f"Anthropic connection error, retry {attempt + 1}/3 in {wait}s")
                    time.sleep(wait)
                else:
                    raise
            except anthropic.APIStatusError as e:
                if e.status_code in (429, 500, 502, 503, 529) and attempt < 3:
                    wait = 2 ** attempt
                    log.warning(f"Anthropic {e.status_code}, retry {attempt + 1}/3 in {wait}s (attempt {attempt + 1}/3)")
                    time.sleep(wait)
                else:
                    raise

        if response.stop_reason == "end_turn":
            text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    text = block.text
                    break

            # If draft_campaign_brief was called at any point, always return a
            # structured brief dict — regardless of whether Claude used <<<BRIEF_JSON>>>.
            # This ensures _PENDING_BRIEFS is populated in scout_bot.py so "launch this" works.
            if _brief_results:
                brief_data = _brief_results[0]  # use first result (primary offer)
                copy_data: dict = {}

                # Preferred: Claude emitted structured JSON
                if "<<<BRIEF_JSON" in text and "BRIEF_JSON>>>" in text:
                    try:
                        json_str = text.split("<<<BRIEF_JSON")[1].split("BRIEF_JSON>>>")[0].strip()
                        copy_data = json.loads(json_str)
                        log.info(f"Parsed BRIEF_JSON for {brief_data.get('advertiser')}")
                    except Exception as e:
                        log.warning(f"Failed to parse BRIEF_JSON: {e} — extracting from plain text")
                    # Strip the sentinel block from text so it never leaks into Slack
                    # when a caller (e.g. App Home Try-it modal) renders response.text
                    # instead of response.payload.
                    text = re.sub(
                        r"<<<BRIEF_JSON.*?BRIEF_JSON>>>",
                        "",
                        text,
                        flags=re.DOTALL,
                    ).strip()

                # Fallback: extract copy from Claude's plain-text response.
                # Check both new schema (title) and old schema (titles) — either means we have copy.
                has_copy = copy_data and (copy_data.get("title") or copy_data.get("titles"))
                if not has_copy:
                    copy_data = _extract_copy_from_text(text)
                    log.info(f"Extracted copy from plain text for {brief_data.get('advertiser')}: "
                             f"title={bool(copy_data.get('title'))}, titles={len(copy_data.get('titles', []))}")

                _fallback_text = text or (
                    f"Campaign Brief — {brief_data.get('advertiser', 'Offer')} "
                    f"({brief_data.get('network', '').title()}, "
                    f"{brief_data.get('payout', 'Rate TBD')}, "
                    f"{brief_data.get('geo', '')})"
                )
                return AskResult(
                    text=_fallback_text,
                    tools_called=_tools_called,
                    duration_ms=_dur(),
                    chart_url=_chart_url,
                    payload={
                        "type": "brief",
                        "brief_data": brief_data,
                        "copy": copy_data,
                        # Full Claude text as fallback so Slack shows something useful
                        # even if Block Kit rendering fails
                        "fallback_text": _fallback_text,
                    },
                    agent_steps=_synthesize_agent_steps(_tool_call_log) or None,
                )

            # Parse and strip <<<SUGGESTIONS [...]  SUGGESTIONS>>> block from text.
            # Claude appends this to every non-brief response; scout_bot.py renders
            # them as Slack buttons so the user can explore without retyping.
            suggestions: list = []
            sugg_match = _SUGG_RE.search(text)
            if sugg_match:
                try:
                    suggestions = json.loads(sugg_match.group(1))
                except Exception as e:
                    log.debug("ask suggestions parse swallowed: %s", e)
                    suggestions = []
                text = text[:sugg_match.start()].rstrip()

            # Opportunity cards — structured list so scout_bot.py can render per-offer cards.
            if _opportunity_offers:
                return AskResult(
                    text=text or "",
                    tools_called=_tools_called,
                    duration_ms=_dur(),
                    chart_url=_chart_url,
                    payload={
                        "type": "opportunities",
                        "text": text or "",
                        "offers": _opportunity_offers,
                        "suggestions": suggestions,
                    },
                    agent_steps=_synthesize_agent_steps(_tool_call_log) or None,
                )

            # General entity extraction — runs over all tool results from this turn.
            # Tool-agnostic: picks up publisher, offer, payout, category from any tool.
            # Returns payload {"type": "text_with_context", ...} so scout_bot.py can
            # persist the entities to thread_context.json for follow-up queries.
            if not _brief_results:
                extracted = _extract_thread_entities(_all_tool_results)
                if extracted or suggestions:
                    return AskResult(
                        text=text or "(no response)",
                        tools_called=_tools_called,
                        duration_ms=_dur(),
                        chart_url=_chart_url,
                        payload={
                            "type": "text_with_context",
                            "text": text or "(no response)",
                            "extracted_context": extracted,
                            "suggestions": suggestions,
                        },
                        agent_steps=_synthesize_agent_steps(_tool_call_log) or None,
                    )

            return AskResult(
                text=text or "(no response)",
                tools_called=_tools_called,
                duration_ms=_dur(),
                chart_url=_chart_url,
                agent_steps=_synthesize_agent_steps(_tool_call_log) or None,
            )

        # Process tool calls
        tool_blocks = [(i, block) for i, block in enumerate(response.content)
                       if block.type == "tool_use"]
        tool_results = []

        if tool_blocks and on_stage:
            if _stage_labels is None:
                try:
                    from scout_state import _STAGE_LABELS as _sl
                    _stage_labels = _sl
                except Exception:
                    _stage_labels = {}
            _first_tool = tool_blocks[0][1].name
            try:
                on_stage(_stage_labels.get(_first_tool, "Working…"))
            except Exception:
                pass

        if len(tool_blocks) > 1:
            # Multiple tool calls — run in parallel, then reassemble in original order
            # Falls back to sequential if executor is unavailable (e.g. during shutdown)
            try:
                with ThreadPoolExecutor(max_workers=min(len(tool_blocks), 8)) as executor:
                    futures = {
                        executor.submit(_run_tool, block.name, block.input, user_id, permalink): (i, block)
                        for i, block in tool_blocks
                    }
                    results_map = {}
                    for future in as_completed(futures):
                        i, block = futures[future]
                        result = future.result()
                        results_map[i] = (block, result)

                for i, _ in sorted(tool_blocks, key=lambda x: x[0]):
                    block, result = results_map[i]
                    _tools_called.append(block.name)
                    _tool_call_log.append((block.name, result))
                    if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                        _brief_results.append(result)
                    if block.name == "get_top_opportunities" and isinstance(result, list) and not _opportunity_offers:
                        _opportunity_offers.extend(result)
                    if block.name == "get_offers_for_publisher" and isinstance(result, dict) and result.get("offers") and not _opportunity_offers:
                        _opportunity_offers.extend(result["offers"])
                    _chart_url = _extract_chart_url(result) or _chart_url
                    _all_tool_results.append(result)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result),
                    })
            except RuntimeError:
                # Interpreter shutting down — fall back to sequential
                for i, block in tool_blocks:
                    _tools_called.append(block.name)
                    result = _run_tool(block.name, block.input, user_id, permalink)
                    _tool_call_log.append((block.name, result))
                    if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                        _brief_results.append(result)
                    _chart_url = _extract_chart_url(result) or _chart_url
                    _all_tool_results.append(result)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result),
                    })
        else:
            # Single tool call — keep sequential path unchanged
            for i, block in tool_blocks:
                _tools_called.append(block.name)
                result = _run_tool(block.name, block.input, user_id, permalink)
                _tool_call_log.append((block.name, result))
                if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                    _brief_results.append(result)  # collect all, use first for primary
                if block.name == "get_top_opportunities" and isinstance(result, list) and not _opportunity_offers:
                    _opportunity_offers.extend(result)
                if block.name == "get_offers_for_publisher" and isinstance(result, dict) and result.get("offers") and not _opportunity_offers:
                    _opportunity_offers.extend(result["offers"])
                _chart_url = _extract_chart_url(result) or _chart_url
                _all_tool_results.append(result)  # accumulate all for entity extraction
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result),
                })

        if not tool_results:
            for block in response.content:
                if hasattr(block, "text"):
                    return AskResult(
                        text=block.text,
                        tools_called=_tools_called,
                        duration_ms=_dur(),
                        chart_url=_chart_url,
                        agent_steps=_synthesize_agent_steps(_tool_call_log) or None,
                    )
            return AskResult(text="(no response)", tools_called=_tools_called, duration_ms=_dur(), chart_url=_chart_url, agent_steps=_synthesize_agent_steps(_tool_call_log) or None)

        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

    # Round cap hit — return a graceful degraded response rather than dying silently
    log.warning(f"ask() hit MAX_ROUNDS ({MAX_ROUNDS}) for query: {user_message[:120]!r}")
    return AskResult(
        text=(
            "I gathered a lot of data on this but hit my analysis limit before finishing the synthesis. "
            "Try breaking the question into smaller parts — e.g. ask about revenue performance separately "
            "from recommendations, or ask about a specific publisher or campaign directly."
        ),
        tools_called=_tools_called,
        duration_ms=_dur(),
        chart_url=_chart_url,
        agent_steps=_synthesize_agent_steps(_tool_call_log) or None,
    )


# ── Command registry ──────────────────────────────────────────────────────────
# Maps canonical command names to their alias sets.
# Matching is alias-exact: the full normalized message must equal one alias —
# not merely contain a keyword. "how's the offer status for CJ" does NOT match
# "status". This is structurally different from _classify_intent signal matching,
# which does substring search for open queries.
_COMMAND_REGISTRY: dict[str, dict] = {
    "status": {
        "aliases": frozenset({
            "status", "scout status", "how are you", "are you ok",
            "are you healthy", "system health", "system status",
            "health check", "scout health check",
        }),
        "description": "System health snapshot: benchmarks, offer inventory, queue, ClickHouse",
    },
}


def _match_command(raw: str) -> tuple[str | None, dict | None]:
    """Match a normalized user message against the command registry.

    Alias-exact: the entire normalized message must equal one alias.
    Substring matching is intentionally avoided so queries like
    'what's the revenue status today' never collide with 'status'.

    Returns (command_name, registry_entry) on hit, (None, None) on miss.
    Must be called on the raw message with @mention stripped.
    """
    normalized = _norm(re.sub(r"<@[A-Z0-9]+>", "", raw or ""))
    if not normalized:
        return None, None
    for name, entry in _COMMAND_REGISTRY.items():
        if normalized in entry["aliases"]:
            return name, entry
    return None, None


def _format_status_response(s: dict) -> str:
    """Format get_scout_status() dict into canonical Scout status text.

    Single source of truth for both @Scout status (mention path via _cmd_status)
    and /scout-status (slash command path via _handle_slash_command).
    """
    ch_stat = s.get("clickhouse", "unknown")
    ch_icon = ":white_check_mark:" if ch_stat == "ok" else ":warning:"
    bm_age  = s.get("benchmarks", "unknown")
    offers  = s.get("offer_inventory", 0)
    queue   = s.get("queue_depth", 0)
    warns   = s.get("warnings") or []
    lines   = [
        ":satellite: *Scout Status*",
        f"Benchmarks: `{bm_age}`  ·  Offers: `{offers:,}`  ·  Queue: `{queue} pending`  ·  ClickHouse: {ch_icon}",
    ]
    for w in warns:
        lines.append(f":warning: {w}")
    return "\n".join(lines)


def _cmd_status() -> tuple[str, tuple]:
    """Canonical handler for @Scout status.

    Returns (text, tools_called). Timing is the caller's responsibility —
    ask() uses _dur() and ask_with_attachment() uses its own _start_ms.
    """
    s = get_scout_status()
    return _format_status_response(s), ("get_scout_status",)


def ask(user_message: str, history: list | None = None, user_id: str = "",
        permalink: str = "", user_tz: str = "", thread_ts: str = "",
        on_stage=None) -> AskResult:
    """
    Send a message to Scout and get a response.
    history: optional list of prior {"role": "user"/"assistant", "content": str} messages
             from the Slack thread, providing conversation context.
    user_id: Slack user ID of the caller — injected into context so tools like
             get_usage_report can enforce admin-only access.

    Returns: AskResult — typed boundary contract carrying text, tools_called list,
    duration_ms, and optional payload dict for structured Slack rendering
    (brief / opportunities / text_with_context). See plan v3 §4.
    """
    _start_ms = time.monotonic()
    _tools_called: list = []
    def _dur() -> int:
        return int((time.monotonic() - _start_ms) * 1000)

    # Deterministic pre-router: control-surface verbs (thresholds/config/status/set)
    # are matched against the RAW user_message before any LLM call. Context prefixes
    # added below would defeat exact-match. LLM stays as fallback for misses.
    _routed = _route_deterministic(user_message, user_id, on_stage=on_stage)
    if _routed is not None:
        return AskResult(
            text=_routed.text,
            tools_called=_routed.tools_called,
            duration_ms=_dur(),
            payload=_routed.payload,
        )

    # Command registry — named operational invocations bypass LLM synthesis.
    # Runs after _route_deterministic (same raw-message requirement) and before
    # _classify_intent (which is for open queries only).
    _cmd_name, _ = _match_command(user_message)
    if _cmd_name == "status":
        text, tools_called = _cmd_status()
        return AskResult(text=text, tools_called=tools_called, duration_ms=_dur())

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return AskResult(
            text="ANTHROPIC_API_KEY not set — Scout can't respond.",
            tools_called=[], duration_ms=_dur(),
        )

    client = anthropic.Anthropic(
        api_key=api_key,
        default_headers={"anthropic-beta": "prompt-caching-2024-07-31"}
    )
    prefix = _build_prefix_context(user_id, user_tz)
    # Intent classification — narrow tool surface and prepend focused context.
    # Runs once per ask() call, outside the retry loop.
    _intent_name, _intent_dict = _classify_intent(user_message, thread_ts=thread_ts or None)
    _ask_tools = TOOLS
    if _intent_dict:
        _primary = set(_intent_dict["primary_tools"])
        _narrowed = [t for t in TOOLS if t["name"] in _primary]
        if _narrowed:  # fall back to full TOOLS if no bucket tools found in public list
            _ask_tools = _narrowed
    messages = _build_initial_messages(user_message, history, prefix)
    return _run_tool_loop(
        messages, client, SYSTEM_PROMPT, _intent_name, _intent_dict,
        _ask_tools, _start_ms, _tools_called,
        user_message=user_message,
        user_id=user_id, permalink=permalink,
        on_stage=on_stage,
    )


def ask_with_attachment(
    user_message: str,
    history: list | None = None,
    user_id: str = "",
    permalink: str = "",
    user_tz: str = "",
    thread_ts: str = "",
    attached_text: str | None = None,
    attached_image: dict | None = None,
    on_stage=None,
) -> AskResult:
    """Variant of ask() that supports per-turn attached content (file or sheet).

    Falls back to ask() when no attachment is present, so callers can use this
    unconditionally without paying any cost when there's no attachment.

    Note on _smart_history non-collision: attached content is injected into
    messages[-1] (the current user turn) AFTER _build_initial_messages composes
    the history+prefix layer. The attachment is per-turn and never enters the
    history list — do not cache or hoist the effective message string, or the
    "attachments are turn-scoped" invariant breaks.
    """
    # No attachments → delegate to vanilla ask(), zero new code path
    if attached_text is None and attached_image is None:
        return ask(
            user_message,
            history=history,
            user_id=user_id,
            permalink=permalink,
            user_tz=user_tz,
            thread_ts=thread_ts,
            on_stage=on_stage,
        )

    # Same setup as ask() — pre-router check, client, prefix context, intent
    _start_ms = time.monotonic()
    _tools_called: list = []
    _routed = _route_deterministic(user_message, user_id, on_stage=on_stage)
    if _routed is not None:
        return _routed  # control-surface verbs never attach files

    _cmd_name, _ = _match_command(user_message)
    if _cmd_name == "status":
        text, tools_called = _cmd_status()
        return AskResult(
            text=text,
            tools_called=tools_called,
            duration_ms=int((time.monotonic() - _start_ms) * 1000),
        )

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return AskResult(
            text="ANTHROPIC_API_KEY not set — Scout can't respond.",
            tools_called=[],
            duration_ms=int((time.monotonic() - _start_ms) * 1000),
        )

    client = anthropic.Anthropic(
        api_key=api_key,
        default_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
    )
    prefix = _build_prefix_context(user_id, user_tz)
    _intent_name, _intent_dict = _classify_intent(user_message, thread_ts=thread_ts or None)
    _ask_tools = TOOLS
    if _intent_dict:
        _primary = set(_intent_dict["primary_tools"])
        _narrowed = [t for t in TOOLS if t["name"] in _primary]
        if _narrowed:
            _ask_tools = _narrowed

    # Cap attached_text defense-in-depth (scout_attachments also caps)
    if attached_text and len(attached_text) > 30_000:
        attached_text = attached_text[:30_000] + "…[trimmed]"

    # Use PR-A's _build_initial_messages for the standard history+prefix layer
    messages = _build_initial_messages(user_message, history, prefix)

    # MUTATE the final user message in messages[-1] to inject attachment content
    if attached_image:
        # Convert final user turn from string to content-block list
        original_text = messages[-1]["content"]
        if attached_text:
            original_text = (
                f"[Attached file content follows between fences:]\n"
                f"```\n{attached_text}\n```\n\n"
                f"{original_text}"
            )
        messages[-1]["content"] = [
            {"type": "image", "source": {
                "type": "base64",
                "media_type": attached_image["media_type"],
                "data": attached_image["b64"],
            }},
            {"type": "text", "text": original_text},
        ]
    elif attached_text:
        # Text-only attachment — prepend fenced block to the user message string
        messages[-1]["content"] = (
            f"[Attached file content follows between fences:]\n"
            f"```\n{attached_text}\n```\n\n"
            f"{messages[-1]['content']}"
        )

    log.info(
        f"ask_with_attachment: attached_text={len(attached_text) if attached_text else 0}c, "
        f"attached_image={'present' if attached_image else 'absent'}"
    )

    return _run_tool_loop(
        messages, client, SYSTEM_PROMPT, _intent_name, _intent_dict,
        _ask_tools, _start_ms, _tools_called,
        user_message=user_message,
        user_id=user_id, permalink=permalink,
        on_stage=on_stage,
    )


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What are the top finance opportunities we don't run yet?"
    print(f"\nQuery: {query}\n")
    print(ask(query).text)
