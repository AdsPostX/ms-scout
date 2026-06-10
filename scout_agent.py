"""
Scout — MomentScience Offer Intelligence Agent
Answers natural language questions about the offer inventory using Claude + tools.
Data source: data/offers_latest.json (written by offer_scraper.py after each daily run)
Performance benchmarks: queried from ClickHouse at startup, cached in memory.
"""

from __future__ import annotations

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
)
from scout_images import (  # noqa: F401 — backward compat re-exports
    _scrape_og_image, _clearbit_domain, _google_favicon, _app_store_icon,
    _validate_image_url, _load_image_cache, _save_image_cache,
    _cached_external_images, _store_image_cache, _ms_cdn_image,
)

load_dotenv()  # plist env vars (SCOUT_ENV, etc.) take precedence over .env

# Register the canonical geo normalizer with scout_core.contracts so any
# NormalizedOffer.normalize_geo(...) call resolves to the same implementation
# offer_scraper uses for Notion writes. Producers (this module + demand_feed_main)
# own this wiring — scout_core stays unaware of offer_scraper to keep the
# contracts layer import-cheap and dependency-free.
from scout_core.contracts import set_geo_normalizer as _set_geo_normalizer
from offer_scraper import normalize_geo as _normalize_geo
_set_geo_normalizer(_normalize_geo)

log = logging.getLogger("scout_agent")


def _fmt_rev(amount: float | None) -> str:
    """Format a revenue float as a compact human-readable dollar string."""
    if amount is None:
        return "$?"
    if amount >= 10_000:
        return f"${round(amount / 100) * 100 / 1000:.0f}K"
    if amount >= 1_000:
        rounded = round(amount / 100) * 100
        return f"${rounded:,.0f}"
    return f"${amount:,.0f}"


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

    def __post_init__(self) -> None:
        # Defense-in-depth: callers may pass a list; coerce to tuple so handlers
        # cannot mutate telemetry mid-flight. Deep-freeze payload so nested
        # offers/copy/suggestions are also immutable (CodeRabbit on PR #69).
        if not isinstance(self.tools_called, tuple):
            object.__setattr__(self, "tools_called", tuple(self.tools_called))
        if self.payload is not None and not isinstance(self.payload, MappingProxyType):
            object.__setattr__(self, "payload", _freeze(dict(self.payload)))


# ── PR 17c / PR 18: SUPPORTED_NETWORKS — single source ───────────────────────
# ACTIVE networks only (creds present on Render → scraper actually returns offers).
# PR 18 trimmed this from 9 → 4: ShareASale, Rakuten, AWIN, Tune, Everflow all
# silently no-op when their API credentials aren't set on Render. Listing them as
# "supported" was misleading because the digest header showed them but the scraper
# returned []. See Known Debt in CLAUDE.md for the credential checklist.
#
# When credentials are added on Render, append the network name here AND to
# _DIGEST_NETWORKS_FALLBACK in scout_digest.py.
#
# Used in tool description strings and function docstrings ONLY (not the
# SYSTEM_PROMPT body — converting that to an f-string would require escaping
# every {} in the SQL/JSON examples and risks silent format breakage).
SUPPORTED_NETWORKS: tuple[str, ...] = (
    "Impact", "FlexOffers", "MaxBounty", "CJ",
)


# ── PR 17a: Scout thresholds — loaded once at module import ──────────────────
# Edit config/scout_thresholds.json + redeploy on Render to change live values.
# The @Scout config tool reads SCOUT_THRESHOLDS at runtime so the team can audit
# what's currently active without reading source.
_SCOUT_THRESHOLDS_FILE = pathlib.Path(__file__).parent / "config" / "scout_thresholds.json"

_SCOUT_THRESHOLDS_FALLBACK: dict = {
    "digest": {
        "min_rpm_floor": 20,
        "offers_per_network": 3,
        "max_per_category": 2,
        "max_per_payout_type": 2,
    },
    "signals": {
        "fill_rate_min_sessions_7d": 2500,
        "ghost_recency_hours": 48,
        "velocity_down_threshold_pct": -25,
        "velocity_up_threshold_pct": 20,
        "cap_alert_pct": 85,
    },
    "health": {
        "offer_staleness_hours": 30,
        "heartbeat_interval_minutes": 30,
        "heartbeat_warmup_seconds": 300,
        "heartbeat_consecutive_threshold": 2,
    },
}


def _load_base_thresholds() -> dict:
    """Allowlist of valid `section.key` pairs: fallback ← config/scout_thresholds.json.

    Excludes runtime overrides so write-validation cannot be tricked by a previously
    persisted bad key in data/threshold_overrides.json.
    """
    try:
        if not _SCOUT_THRESHOLDS_FILE.exists():
            log.warning(f"[config] {_SCOUT_THRESHOLDS_FILE} missing — using fallback thresholds")
            base = {k: dict(v) for k, v in _SCOUT_THRESHOLDS_FALLBACK.items()}
        else:
            loaded = json.loads(_SCOUT_THRESHOLDS_FILE.read_text())
            loaded.pop("_doc", None)
            base = {k: dict(v) for k, v in _SCOUT_THRESHOLDS_FALLBACK.items()}
            for section, values in loaded.items():
                if section in base and isinstance(values, dict):
                    base[section].update(values)
                else:
                    base[section] = values
    except Exception as e:
        log.warning(f"[config] _load_base_thresholds() failed on config file, using fallback: {e}")
        base = {k: dict(v) for k, v in _SCOUT_THRESHOLDS_FALLBACK.items()}
    return base


def _load_thresholds() -> dict:
    """Load Scout thresholds: base schema ← data/threshold_overrides.json.

    Runtime overrides from the `set_threshold` agent tool are layered last, so they
    win over the git-tracked config. Override file shape:
      {"signals": {"cap_alert_pct": {"value": 80, "set_by": "U123", "set_at": "...", "reason": "..."}}}
    """
    merged = _load_base_thresholds()

    # Layer runtime overrides on top (lazy import — scout_state has no scout_agent dep)
    try:
        import scout_state
        overrides = scout_state._load_threshold_overrides()
        for section, keys in (overrides or {}).items():
            if not isinstance(keys, dict):
                continue
            if section not in merged or not isinstance(merged[section], dict):
                merged[section] = {}
            for key, entry in keys.items():
                if isinstance(entry, dict) and "value" in entry:
                    merged[section][key] = entry["value"]
    except Exception as e:
        log.warning(f"[config] _load_thresholds() failed applying overrides: {e}")

    return merged


# Base schema (no overrides) — the source of truth for write-validation in
# set_threshold. SCOUT_THRESHOLDS below is the merged read-view callers actually see.
_BASE_THRESHOLDS: dict = _load_base_thresholds()
SCOUT_THRESHOLDS: dict = _load_thresholds()


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
_PULSE_STATE_PATH = pathlib.Path(__file__).parent / "data" / "pulse_state.json"

# ── Performance benchmark cache (refreshed hourly) ───────────────────────────
# Maps: category → {cvr_pct, rpm, sample_size}
#        offer_impact_id → {cvr_pct, rpm, adv_name}
_BENCHMARKS: dict = {}
_BENCHMARKS_LOADED_AT: float = 0.0
_BENCHMARKS_TTL = 3600  # 1 hour

# ── Data quality tier helper ──────────────────────────────────────────────────

def _data_quality_tier(days_of_data: int, sessions: int = 0) -> dict:
    """
    Compute confidence tier for a data window.
    Used by tools to populate data_quality in return values.
    Claude uses this to emit the CONFIDENCE LINE rule in responses.
    """
    if days_of_data >= 14 and sessions >= 1000:
        tier, emoji = "strong", ":large_green_circle:"
    elif days_of_data >= 7 and sessions >= 100:
        tier, emoji = "directional", ":large_yellow_circle:"
    else:
        tier, emoji = "thin", ":red_circle:"
    if sessions > 0:
        note = f"{days_of_data} days · {sessions:,} sessions"
    else:
        note = f"{days_of_data} days"
    return {"tier": tier, "emoji": emoji, "days_of_data": days_of_data, "sessions": sessions, "note": note}


# ── Learnings injection ───────────────────────────────────────────────────────

_LEARNINGS_PATH = pathlib.Path(__file__).parent / "data" / "learnings.json"
_LEARNED_BENCHMARKS_PATH = pathlib.Path(__file__).parent / "data" / "learned_benchmarks.json"
_TEAM_CORRECTIONS_PATH = pathlib.Path(__file__).parent / "config" / "team_corrections.json"
_ENTITY_OVERRIDES_PATH = pathlib.Path(__file__).parent / "data" / "entity_overrides.json"


def _load_entity_overrides() -> dict:
    """Load publisher/advertiser knowledge store. Returns empty structure if missing or corrupt."""
    try:
        if _ENTITY_OVERRIDES_PATH.exists():
            return json.loads(_ENTITY_OVERRIDES_PATH.read_text())
    except Exception as e:
        log.debug("_load_entity_overrides swallowed: %s", e)
    return {"publishers": {}, "advertisers": {}}


def _save_entity_overrides(overrides: dict) -> None:
    """Atomic write to entity_overrides.json using temp+rename (safe on Linux/Render)."""
    _ENTITY_OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _ENTITY_OVERRIDES_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(overrides, indent=2))
    tmp.replace(_ENTITY_OVERRIDES_PATH)


def _get_corrections_context() -> str:
    """
    Load high-confidence corrections from two sources and return as a grounding
    context string prepended to user queries in ask().

    Sources (merged, static corrections listed first):
    - config/team_corrections.json — static team knowledge, committed to git
    - data/learnings.json — runtime corrections learned from feedback
    Returns empty string if no corrections or files missing.
    """
    corrections: list = []
    try:
        # Static team corrections (git-tracked, always present after deploy)
        if _TEAM_CORRECTIONS_PATH.exists():
            data = json.loads(_TEAM_CORRECTIONS_PATH.read_text())
            corrections += [c for c in data.get("corrections", []) if c.get("confidence") == "high"]
    except Exception as e:
        log.debug("_get_corrections_context team_corrections swallowed: %s", e)
    try:
        # Runtime corrections (accumulated from team feedback via @Scout learn)
        if _LEARNINGS_PATH.exists():
            data = json.loads(_LEARNINGS_PATH.read_text())
            corrections += [c for c in data.get("corrections", []) if c.get("confidence") == "high"]
    except Exception as e:
        log.debug("_get_corrections_context learnings swallowed: %s", e)
    # Entity overrides (publisher + advertiser notes recorded by the team via @Scout)
    # Plan v3 §3.5: emit provenance (added_by + added date) inline so the LLM can
    # cite "[learned from <user> on <date>]" when it surfaces an override fact.
    try:
        overrides = _load_entity_overrides()
        for pub, data in overrides.get("publishers", {}).items():
            prov = f" [learned from {data.get('added_by','?')} on {data.get('added','?')}]"
            corrections.append({"confidence": "high",
                                 "correction": f"Publisher {pub}: {data['note']}{prov}"})
        for adv, data in overrides.get("advertisers", {}).items():
            prov = f" [learned from {data.get('added_by','?')} on {data.get('added','?')}]"
            corrections.append({"confidence": "high",
                                 "correction": f"Advertiser {adv}: {data['note']}{prov}"})
    except Exception as e:
        log.debug("_get_corrections_context overrides swallowed: %s", e)
    if not corrections:
        return ""
    lines = [f"- {c['correction']}" for c in corrections[-16:]]
    return (
        "TEAM CORRECTIONS (from prior feedback — treat these as ground truth):\n"
        + "\n".join(lines)
        + "\n\n"
    )



def _merge_learned_benchmarks() -> None:
    """
    Merge data/learned_benchmarks.json into _BENCHMARKS at startup.
    Learned benchmarks have lower weight than ClickHouse actuals
    but override category defaults. Called once after _load_performance_benchmarks().
    """
    global _BENCHMARKS
    try:
        if not _LEARNED_BENCHMARKS_PATH.exists():
            return
        lb = json.loads(_LEARNED_BENCHMARKS_PATH.read_text())
        if not lb:
            return
        learned = _BENCHMARKS.setdefault("by_learned_actuals", {})
        for key, entry in lb.items():
            learned[key] = {
                "avg_cvr_pct": 0.0,  # CVR not tracked in simple recap
                "avg_rpm": entry.get("rpm_actual_avg", 0.0),
                "sample_campaigns": entry.get("sample_count", 0),
            }
        log.info(f"Merged {len(lb)} learned benchmark entries into _BENCHMARKS")
    except Exception as e:
        log.warning(f"_merge_learned_benchmarks failed: {e}")


# ── PR 19: tags-as-categories helper + schema-deps validation ────────────────

def _extract_real_categories(tags_value) -> list[str]:
    """
    Parse the from_airbyte_campaigns.tags JSON array string and return the
    real category tags (filter out `internal-*` system tags used for network
    and channel metadata).

    Pure function, no I/O. Used by tests and (where useful) by ad-hoc Python
    consumers; the production scoring path does the same filter in SQL via
    queries.performance_benchmarks_raw().

    Examples:
      '["internal-network-impact", "internal-email", "rewards", "technology"]'
        → ['rewards', 'technology']
      '["pets", "non-profit"]'        → ['pets', 'non-profit']
      '[]' or None or '' or 'invalid' → []
    """
    if not tags_value:
        return []
    try:
        parsed = json.loads(tags_value) if isinstance(tags_value, str) else tags_value
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    # Case-insensitive filter — production has both "internal-email" and "Internal-Email"
    # variants from differently-typed platform entries.
    return [t for t in parsed if isinstance(t, str) and not t.lower().startswith("internal-")]


# Columns Scout depends on. (table, column, must_have_data).
# must_have_data=True means the column must have at least 100 non-null rows;
# below that threshold _validate_schema_deps fires loud at boot. Catches the
# categories-NULL class of silent failure (PR 19 root cause).
_SCHEMA_DEPS: list[tuple[str, str, bool]] = [
    # from_airbyte_campaigns — driver of benchmarks + scoring
    ("from_airbyte_campaigns",      "id",                    False),
    ("from_airbyte_campaigns",      "adv_name",              True),
    ("from_airbyte_campaigns",      "tags",                  True),   # PR 19: source of category data
    ("from_airbyte_campaigns",      "internal_network_name", True),
    ("from_airbyte_campaigns",      "deleted_at",            False),
    ("from_airbyte_campaigns",      "categories",            False),  # known empty (use tags)
    # Activity tables — driver of CVR/RPM
    ("adpx_sdk_sessions",           "user_id",               True),
    ("adpx_sdk_sessions",           "placement",             True),
    ("adpx_impressions_details",    "campaign_id",           True),
    ("adpx_impressions_details",    "pid",                   True),
    ("adpx_conversionsdetails",     "campaign_id",           True),
    ("adpx_conversionsdetails",     "revenue",               True),
    ("adpx_conversionsdetails",     "click_hash",            True),
    ("adpx_conversionsdetails",     "session_id",            True),
    ("adpx_tracked_clicks",         "campaign_id",           True),
    ("adpx_tracked_clicks",         "click_hash",            True),
    ("adpx_tracked_clicks",         "session_id",            True),
    # Publisher resolution
    ("from_airbyte_users",          "id",                    True),
    ("from_airbyte_users",          "organization",          True),
    # CVR anomaly + expiration monitors (PR-C)
    ("adpx_conversionsdetails",     "payout",                True),
    ("from_airbyte_campaigns",      "end_date",              False),  # NULL for open-ended campaigns
    ("from_airbyte_campaigns",      "status",                True),
]

_SCHEMA_DEPS_MIN_ROWS = 100  # threshold for must_have_data; below this fires alert


def _validate_schema_deps(ch) -> dict:
    """
    Boot-time check (PR 19): for each column Scout depends on, confirm it exists
    in system.columns and (where flagged) has at least _SCHEMA_DEPS_MIN_ROWS non-null
    rows. Returns {"ok": bool, "violations": [str], "warnings": [str], "checked": int}.

    Does NOT block boot. Caller logs and posts to #scout-qa via _run_startup_smoke_test.
    Catches the "Scout reads a column that's NULL/missing/renamed" class of silent
    failure that bit us with categories (PR 19 root cause).
    """
    violations: list[str] = []
    warnings: list[str] = []
    try:
        tables = sorted({t for t, _, _ in _SCHEMA_DEPS})
        # Single batched query for all tables we care about
        col_rows = ch.query(
            "SELECT table, name FROM system.columns "
            "WHERE database = 'default' AND table IN {tables: Array(String)}",
            parameters={"tables": tables},
        ).result_rows
        live = {(t, c) for t, c in col_rows}
        for table, col, must_have_data in _SCHEMA_DEPS:
            if (table, col) not in live:
                violations.append(f"{table}.{col} MISSING from system.columns")
                continue
            if must_have_data:
                try:
                    n = ch.query(
                        f"SELECT countIf({col} IS NOT NULL) FROM default.{table}"
                    ).result_rows[0][0]
                except Exception as e:
                    warnings.append(f"{table}.{col} count check failed: {e}")
                    continue
                if n < _SCHEMA_DEPS_MIN_ROWS:
                    violations.append(
                        f"{table}.{col} has only {n} non-null rows "
                        f"(need ≥{_SCHEMA_DEPS_MIN_ROWS}). "
                        f"Scout may silently fail to use this data."
                    )
    except Exception as e:
        warnings.append(f"schema validation crashed: {e}")
    return {
        "ok": not violations,
        "violations": violations,
        "warnings": warnings,
        "checked": len(_SCHEMA_DEPS),
    }


def _load_performance_benchmarks() -> dict:
    """
    Query ClickHouse for real CVR + RPM benchmarks grounded in actual MS conversion data.

    Returns four lookup tiers — used in priority order by _scout_score():
      1. by_offer_impact_id   — exact offer match (highest confidence)
      2. by_adv_name          — same advertiser, different offer (high confidence)
      3. by_category_payout   — (category, payout_type) combo (medium confidence)
      4. by_payout_type       — payout type only across all offers (low confidence fallback)

    Category and payout_type come directly from from_airbyte_campaigns — no keyword heuristics.
    The old keyword-to-category mapping is removed; it missed ~40% of offers and could not be maintained.
    """
    try:
        ch = _get_ch_client()

        # SQL lives in queries.performance_benchmarks_raw() — any threshold or
        # window change belongs there. Tuple columns:
        # (id, adv_name, impact_id, category, impression_count, cvr_pct, rpm)
        rows = _q.performance_benchmarks_raw(ch)

        # Tier 1: exact offer (by Impact network ID)
        by_offer: dict = {}
        # Tier 2: advertiser-level (all offers from same adv_name)
        by_adv: dict = {}
        # Tier 3: category — real avg CVR across all offers in this category on MS
        by_cat: dict = {}

        for _id, adv_name, impact_id, category, impressions, cvr_pct, rpm in rows:
            cvr = float(cvr_pct or 0)
            rpm_val = float(rpm or 0)
            imp = int(impressions or 0)
            entry = {"adv_name": adv_name, "cvr_pct": cvr, "rpm": rpm_val, "impressions": imp,
                     "category": category}

            # Tier 1
            if impact_id:
                by_offer[impact_id] = entry

            # Tier 2 — keep the highest-RPM offer per advertiser as representative.
            # Highest RPM (not highest impressions) is the right benchmark: it reflects what
            # a well-matched new offer from this advertiser could realistically achieve on MS.
            adv_key = (adv_name or "").lower().strip()
            if adv_key and (adv_key not in by_adv or rpm_val > by_adv[adv_key]["rpm"]):
                by_adv[adv_key] = entry

            # Tier 3 — accumulate by category for averaging
            cat_key = (category or "").strip()
            if cat_key:
                if cat_key not in by_cat:
                    by_cat[cat_key] = {"total_cvr": 0.0, "total_rpm": 0.0, "count": 0}
                by_cat[cat_key]["total_cvr"] += cvr
                by_cat[cat_key]["total_rpm"] += rpm_val
                by_cat[cat_key]["count"] += 1

        # Finalise averaged tier 3
        category_benchmarks = {
            cat: {
                "avg_cvr_pct": round(v["total_cvr"] / v["count"], 4),
                "avg_rpm":     round(v["total_rpm"] / v["count"], 2),
                "sample_campaigns": v["count"],
            }
            for cat, v in by_cat.items() if v["count"] > 0
        }

        if not category_benchmarks:
            log.warning(
                f"Tier 3 benchmarks empty across {len(rows)} campaigns. "
                "Expected ~25 categories from tags JSON parsing in "
                "queries.performance_benchmarks_raw() — check the SQL CTE there. "
                "Verified Apr 2026: data lives in `tags`, not `categories`."
            )

        # Tier 4: network-agnostic overall CVR baseline.
        # Used when no offer/advertiser/category match exists — the case for every
        # MaxBounty, FlexOffers, ShareASale, Rakuten, and Awin offer (MS has never run them).
        # Low confidence (0.35) so they rank below real-data offers but still surface.
        overall = _q.benchmark_overall_cvr(ch)
        by_payout_type = {"_all": overall} if overall else {}
        if overall:
            log.info(
                f"Tier 4 baseline: {overall['cvr_pct']:.4f}% CVR / "
                f"${overall['rpm']:.2f} RPM across {overall['campaigns']} MS campaigns"
            )

        result = {
            "by_offer_impact_id":    by_offer,
            "by_adv_name":           by_adv,
            "by_category_payout":    {},          # not available without payout_type column
            "by_payout_type":        by_payout_type,
            "by_category":           category_benchmarks,
        }
        log.info(
            f"Benchmarks loaded: {len(by_offer)} offers, {len(by_adv)} advertisers, "
            f"{len(category_benchmarks)} categories, "
            f"{'Tier4 baseline active' if by_payout_type else 'Tier4 unavailable'}"
        )
        return result

    except Exception as e:
        log.warning(f"Could not load performance benchmarks from ClickHouse: {e}")
        return {"by_offer_impact_id": {}, "by_adv_name": {}, "by_category_payout": {},
                "by_payout_type": {}, "by_category": {}}  # empty — will use no-data path in _scout_score


def _get_benchmarks() -> dict:
    global _BENCHMARKS, _BENCHMARKS_LOADED_AT
    if not _BENCHMARKS or (time.time() - _BENCHMARKS_LOADED_AT) > _BENCHMARKS_TTL:
        _BENCHMARKS = _load_performance_benchmarks()
        _merge_learned_benchmarks()  # overlay actuals from 14-day recaps
        _BENCHMARKS_LOADED_AT = time.time()
    return _BENCHMARKS


# Compiled once at module level — strips <<<SUGGESTIONS [...]  SUGGESTIONS>>> blocks from responses
_SUGG_RE = re.compile(r'<<<SUGGESTIONS\s*(\[.*?\])\s*SUGGESTIONS>>>', re.DOTALL)

# ── Scout Score ───────────────────────────────────────────────────────────────

def _scout_score(offer: dict, benchmarks: dict) -> float:
    """
    Estimated RPM = payout × context_cvr × 1000 × confidence_weight.

    CVR is sourced from real MS conversion data in four tiers (never hardcoded):
      1. Exact offer match        — real CVR for this specific offer (500+ impressions)
      2. Same advertiser          — real CVR for other offers from same brand
      3. Category × payout type   — real avg CVR for e.g. "Finance CPL" offers on MS
      4. Payout type only         — real avg CVR for all CPL / CPS / etc. offers on MS

    If the offer is flagged as high-friction (B2B, loan, medical, biz-opp), returns 0
    so the caller displays "Not estimated" rather than an inflated number.
    """
    payout = offer.get("_payout_num") or 0
    if payout == 0:
        return 0.0

    # High-friction offers: suppress score entirely rather than mislead.
    # B2B, loan, medical, and biz-opp convert 50-70% below consumer post-transaction.
    # Better to show "Not estimated" than anchor on a number that will disappoint.
    risk = _get_risk_flag(
        offer.get("advertiser", ""),
        offer.get("category", ""),
        offer.get("description", ""),
    )
    _HIGH_FRICTION = ("B2B intent", "Loan/credit", "Medical program", "Biz-opp", "Insurance")
    if any(tag in risk for tag in _HIGH_FRICTION):
        return 0.0

    offer_id     = str(offer.get("offer_id", ""))
    payout_type  = (offer.get("_payout_type_norm") or "").lower().strip()
    category     = (offer.get("category") or "").strip()
    adv_name     = (offer.get("advertiser") or "").lower().strip()

    by_offer     = benchmarks.get("by_offer_impact_id", {})
    by_adv       = benchmarks.get("by_adv_name", {})
    by_cat       = benchmarks.get("by_category", {})

    # ── Tier 1: exact offer ───────────────────────────────────────────────────
    if offer_id in by_offer:
        bench = by_offer[offer_id]
        cvr   = bench["cvr_pct"] / 100
        confidence = 1.0
        source = f"Real MS data ({bench['impressions']:,} impressions)"

    # ── Tier 2: same advertiser, different offer ──────────────────────────────
    elif adv_name in by_adv:
        bench = by_adv[adv_name]
        cvr   = bench["cvr_pct"] / 100
        confidence = 0.85
        source = f"Same advertiser benchmark ({bench['impressions']:,} impressions)"

    # ── Tier 3: category average — real CVR across this category on MS ────────
    elif category and category in by_cat:
        bench = by_cat[category]
        cvr   = bench["avg_cvr_pct"] / 100
        confidence = 0.65
        source = f"{category} category benchmark ({bench['sample_campaigns']} offers)"

    # ── Tier 4: overall MS CVR baseline — covers all non-MS-run networks ─────
    # MaxBounty / FlexOffers / ShareASale / Rakuten / Awin advertisers have no
    # offer/advertiser/category match. Use the aggregate CVR across 670+ MS campaigns
    # as a low-confidence prior so they rank against each other (and below real-data offers).
    elif benchmarks.get("by_payout_type", {}).get("_all"):
        base = benchmarks["by_payout_type"]["_all"]
        cvr  = base["cvr_pct"] / 100
        confidence = 0.35
        source = f"MS overall baseline ({base['campaigns']} campaigns, low confidence)"

    else:
        # No real data at any tier — return 0 so caller shows "Not estimated"
        return 0.0

    # Payout reliability: CPS (% of sale) payout is uncertain because the sale
    # amount varies; apply a discount to avoid overconfident RPM estimates
    if "sale" in payout_type or "%" in payout_type:
        confidence *= 0.8

    estimated_rpm = payout * cvr * 1000 * confidence
    log.debug(f"Scout Score [{offer.get('advertiser')}]: ${estimated_rpm:.0f} RPM | {source} | confidence={confidence:.2f}")
    return round(estimated_rpm, 4)


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


def get_supply_demand_gaps(
    publisher_name: str = "",
    advertiser_name: str = ""
) -> str:
    """
    Identify supply-demand gaps:
    - Publisher-first: which advertisers are performing elsewhere but NOT in this publisher?
    - Advertiser-first: which publishers is this advertiser NOT running in?
    Also surfaces dead weight: provisioned but zero impressions in 30 days.
    Provide either publisher_name OR advertiser_name, not both.
    """
    ch = _get_ch_client()

    if publisher_name and not advertiser_name:
        # --- PUBLISHER-FIRST MODE ---

        # Step 1: Resolve publisher
        pub_results = _q.publisher_lookup_by_name(ch, publisher_name)
        if not pub_results:
            return f"No publisher found matching '{publisher_name}'."
        pub_id  = pub_results[0]["id"]
        pub_org = pub_results[0]["organization"]
        pub_pid = str(pub_id)

        # Step 2: Advertisers already active in this publisher (for fuzzy dedup)
        existing = _q.publisher_existing_advertisers(ch, pub_id)

        # Step 3: Gap opportunities, dead weight, and session volume (sequential — CH client not thread-safe)
        gap_data     = _q.supply_gap_opportunities(ch, pub_id)
        dead_data    = _q.supply_dead_weight(ch, pub_id, pub_pid)
        sessions_30d = _q.publisher_sessions_30d(ch, pub_id)

        # Filter gaps to advertisers not already in this publisher.
        # Fuzzy match: suppress if existing adv name is a substring of the candidate
        # or vice versa — catches "Disney+" suppressing "Disney+ and Hulu" variants.
        def _already_provisioned(adv_name: str) -> bool:
            a = adv_name.lower()
            return any(a == ex or a in ex or ex in a for ex in existing)

        gaps         = [d for d in gap_data if not _already_provisioned(d["adv_name"])]
        daily_sessions = sessions_30d / 30 if sessions_30d else 0

        lines = [f"*{pub_org} — Supply Gap Analysis* (30-day data)\n"]

        if gaps:
            lines.append(":large_green_circle: *GAP OPPORTUNITIES* (performing on 2+ other publishers, not here)")
            total_est = 0
            for d in gaps[:10]:
                adv, pub_count, rev, rpm = d["adv_name"], d["pub_count"], d["revenue_30d"], d["rpm"]
                est_daily = round(daily_sessions * (rpm / 1000), 0) if rpm else 0
                total_est += est_daily
                lines.append(
                    f"• *{adv}* — ${rev:,.0f}/mo elsewhere · RPM ${rpm:.2f} across {pub_count} publishers"
                    + (f" → *est. ${est_daily:,.0f}/day at {pub_org} volume*" if est_daily > 0 else "")
                )
            if total_est > 0:
                lines.append(f"\n:zap: Top gaps combined: est. *${total_est:,.0f}/day* incremental revenue potential.")
        else:
            lines.append(":white_check_mark: No major gap opportunities found — coverage looks solid.")

        if dead_data:
            lines.append("\n:large_yellow_circle: *DEAD WEIGHT* (provisioned here, zero impressions in 30 days)")
            for d in dead_data:
                since = d["provisioned_since"]
                since_str = since.strftime("%b %d") if hasattr(since, "strftime") else str(since)
                lines.append(f"• *{d['adv_name']}* — active since {since_str}, 0 impressions. Remove or investigate.")

        return "\n".join(lines)

    elif advertiser_name and not publisher_name:
        # --- ADVERTISER-FIRST MODE ---

        # Publishers where advertiser IS running (active)
        active_data    = _q.advertiser_active_publishers(ch, advertiser_name)
        active_pub_ids = [d["publisher_id"] for d in active_data]
        active_pub_names = [d["organization"] for d in active_data]

        if not active_pub_ids:
            return f"No active publisher campaigns found for '{advertiser_name}'."

        # Publishers with significant traffic NOT running this advertiser
        missing_data = _q.publishers_missing_advertiser(ch, active_pub_ids)

        lines = [f"*{advertiser_name} — Publisher Gap Analysis* (30-day data)\n",
                 f":white_check_mark: Currently running in: {', '.join(active_pub_names[:8])}\n"]

        if missing_data:
            lines.append(":large_green_circle: *NOT RUNNING IN* (publishers with >1K sessions/mo)")
            for d in missing_data[:10]:
                lines.append(f"• *{d['organization']}* — {d['sessions_30d']:,} sessions/mo")
            lines.append(
                f"\n:zap: {len(missing_data)} publishers with meaningful traffic not running {advertiser_name}."
            )
        else:
            lines.append(":white_check_mark: Running in all major publishers — no significant gaps found.")

        return "\n".join(lines)

    else:
        return (
            "Please provide either a publisher_name (e.g. 'TextNow') or an advertiser_name (e.g. 'Scrambly'), "
            "not both and not neither."
        )


TOOLS = [
    {
        "name": "search_offers",
        "description": (
            "Full-text search across advertiser name and description. "
            "Use for specific advertiser lookups or keyword searches. "
            "Leave query empty ('') to browse all offers with only filters applied. "
            "Optional filters: network, category, min_payout, max_payout, ms_status. "
            "Returns results ranked by Scout Score (estimated RPM), not raw payout."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term — advertiser name or keyword. Use '' to browse all offers."},
                "network": {"type": "string", "description": "Optional network filter — pass whatever network name the user mentioned (e.g. 'cj', 'impact'). Fuzzy matching handles normalization. Check available_networks in get_scout_status() to see what's in inventory."},
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness, Retail"},
                "min_payout": {"type": "number", "description": "Minimum payout amount (floor)"},
                "max_payout": {"type": "number", "description": "Maximum payout amount (ceiling), e.g. 0.05 for ≤$0.05"},
                "ms_status": {"type": "string", "description": "Live, In System, or Not in System"},
                "limit": {"type": "integer", "description": "Max results (default 5)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_top_opportunities",
        "description": (
            "Returns best untapped offers MS is NOT running (MS Status = Not in System), "
            "ranked by Scout Score (estimated RPM = payout × predicted CVR). "
            "Use for prospecting: 'what should we go after?', 'best opportunities in X vertical'. "
            "Optional filters: category, geo."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness"},
                "geo": {"type": "string", "description": "e.g. US Only, Global"},
                "limit": {"type": "integer", "description": "Max results (default 5)"},
            },
        },
    },
    {
        "name": "get_running_offers",
        "description": (
            "Returns offers MS is currently running (MS Status = Live) with real CVR + RPM data where available. "
            "Use to benchmark payouts, see what verticals are covered, check if MS has an offer from a specific advertiser, "
            "or understand what's actually performing. Optional filter: category."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "e.g. Finance, Health & Wellness"},
            },
        },
    },
    {
        "name": "get_category_performance",
        "description": (
            "Returns real CVR and RPM benchmarks from MS's live ClickHouse data, by category and by specific offer. "
            "Use this to answer questions about what performs well for MS, "
            "to contextualize a new offer's expected value, or to compare verticals. "
            "This is the most data-driven signal available — prioritize it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Optional: filter to a specific category"},
            },
        },
    },
    {
        "name": "get_offer_stats",
        "description": (
            "Returns aggregate inventory stats: count and avg Scout Score by network and category, "
            "MS Status breakdown, and top 5 highest-value offers. "
            "Use for strategic / high-level questions about the inventory."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_publisher_competitive_landscape",
        "description": (
            "Query ClickHouse for what's currently running on a specific publisher (e.g. AT&T, TXB, MLB, or partner ID 6103), "
            "ranked by RPM. Answers questions like: 'would a higher payout help us win more AT&T impressions?', "
            "'how competitive is the TurboTax offer on AT&T?', 'how many impressions would we get?', "
            "'what does partner 6103 run?'. "
            "Supply offer_name + hypothetical_payout to get a rank-change + impression share projection. "
            "Use publisher_id when a numeric partner ID is given (e.g. 6103); use publisher_name otherwise."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK) e.g. 'AT&T', 'TXB'. Omit if using publisher_id."},
                "publisher_id": {"type": "integer", "description": "Numeric publisher/partner ID (e.g. 6103). Use when the user provides a partner number."},
                "offer_name": {"type": "string", "description": "Optional: offer/advertiser to rank in the competitive set e.g. 'TurboTax'"},
                "hypothetical_payout": {"type": "number", "description": "Optional: new payout to test (e.g. 40.0 for $40 CPA)"},
                "weeks": {"type": "integer", "description": "Optional: projection window in weeks (default 2)"},
            },
        },
    },
    {
        "name": "get_fallback_candidates",
        "description": (
            "When an offer might go dark (budget cap, advertiser pause, network issue), find the best replacement. "
            "Returns (1) same advertiser on a different network — plug-and-play swap, "
            "and (2) top category substitutes not currently live in MS, ranked by Scout Score. "
            "Use for: 'what's our fallback if X goes dark?', 'backup for Y', 'if X hits cap what do we run?', "
            "'what do we replace X with?', 'contingency plan for Y'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "offer_name": {"type": "string", "description": "The offer that may go dark"},
                "category": {"type": "string", "description": "Override category if needed"},
                "payout_type": {"type": "string", "description": "Optional: filter subs by payout type (CPA, CPL, etc.)"},
            },
            "required": ["offer_name"],
        },
    },
    {
        "name": "draft_campaign_brief",
        "description": (
            "Fetch all offer details needed to generate a campaign brief: tracking URL, payout, geo, "
            "description, network, offer ID, and real MS performance data. "
            "Use when asked to 'build', 'create a brief for', 'I like [offer], build it', or similar. "
            "Returns structured data — you then generate the copy, titles, and CTAs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser": {"type": "string", "description": "Advertiser name (partial match OK)"},
                "network": {"type": "string", "description": "Optional network filter — pass whatever network name the user mentioned (e.g. 'cj', 'impact'). Fuzzy matching handles normalization."},
            },
            "required": ["advertiser"],
        },
    },
    {
        "name": "get_queue_status",
        "description": (
            "Fetch the offer pipeline queue from Notion and return a Slack Block Kit card. "
            "Grouped by pipeline stage: Awaiting Entry → In Platform → Test Offer ON → Live. "
            "Use for: 'what's in the queue?', 'what's pending?', 'queue status', 'pipeline', "
            "'what's been approved?', 'what's waiting to go live?'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_demand_queue_status",
        "description": (
            "Read the MS Demand Queue — cross-references ClickHouse to detect if any queued offer "
            "is already live (impressions > 0 since approval date). "
            "Use for impression-based queries: 'is X live?', 'how many impressions since X was approved?'. "
            "For general queue visibility use get_queue_status() instead."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "mark_offer_launched",
        "description": (
            "Mark an approved offer as live. Updates queue state and triggers a notification "
            "to the person who approved it + AdOps. Thread-only — no channel noise. "
            "Use when: 'TurboTax is live', 'confirm X is live', 'mark X as launched', "
            "'X went live', 'X is running'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser": {"type": "string", "description": "Advertiser name (partial match OK)"},
            },
            "required": ["advertiser"],
        },
    },
    {
        "name": "get_advertiser_revenue_projection",
        "description": (
            "Project gross revenue for a specific advertiser across ALL MS publisher partners for a target month. "
            "Uses last 30 days as the baseline (avg daily revenue × days in month). "
            "Checks campaign end dates (warns if campaigns end before month-end) and monthly budget caps. "
            "Returns: projected total revenue, breakdown by publisher, cap warnings, end-date warnings. "
            "Use for: 'projected revenue for Disney+ in April', 'how much will TurboTax generate this month', "
            "'gross revenue forecast for X across all partners', 'what's the April projection for X'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser_name": {"type": "string", "description": "Advertiser/offer name (partial match OK) e.g. 'Disney+', 'TurboTax'"},
                "month": {"type": "string", "description": "Target month e.g. 'April 2026' or '2026-04'. Defaults to next calendar month."},
            },
            "required": ["advertiser_name"],
        },
    },
    {
        "name": "get_publisher_health",
        "description": (
            "Full publisher health analysis: sessions, impressions, clicks, conversions, revenue, RPM, CTR, and CVR. "
            "Breaks down by placement (e.g. FuelHub vs TransactionReceipt) and OS (iOS/Android). "
            "Includes click position data (which carousel slot gets clicked). "
            "Use for: 'how is [publisher] doing', 'performance for [publisher]', "
            "'breakdown by placement', 'full funnel for [publisher]', "
            "'[publisher] placement performance', 'what placement drives most revenue on [publisher]'. "
            "This is the default tool for any publisher performance query — always call this before offer-level analysis."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK) e.g. '7-Eleven', 'AT&T'. Omit if using publisher_id."},
                "publisher_id": {"type": "integer", "description": "Numeric publisher ID. Use when user provides a partner number."},
                "days": {"type": "integer", "description": "Lookback window in days (default 14)"},
                "geo_state": {"type": "string", "description": "Optional: filter to a US state e.g. 'California', 'TX'"},
            },
        },
    },
    {
        "name": "get_campaign_status",
        "description": (
            "Check if an advertiser's campaigns are active or paused, and see recent changes from the audit log. "
            "Use for: 'is [offer] paused?', 'confirm [offer] is paused', 'is [offer] still live?', "
            "'what happened to [offer]?', 'when was [offer] paused?', 'confirm all [offer] campaigns are killed'. "
            "Returns current is_active status for each publisher campaign + last 30 days of change history."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "advertiser_name": {"type": "string", "description": "Advertiser/offer name (partial match OK) e.g. 'TurboTax', 'Hulu'"},
            },
            "required": ["advertiser_name"],
        },
    },
    {
        "name": "get_perkswall_engagement",
        "description": (
            "Perkswall offer selection analytics — which offers do loyalty members actually pick? "
            "Queries user_selected_perks to show offer selections, unique members engaged, and selection rates. "
            "Use for: 'which perks are [publisher] users picking?', 'Perkswall engagement for [publisher]', "
            "'what do loyalty members select on [publisher]?', 'top selected perks on [publisher]'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {"type": "string", "description": "Publisher name (partial match OK)"},
                "publisher_id": {"type": "integer", "description": "Numeric publisher ID"},
                "days": {"type": "integer", "description": "Lookback window in days (default 30)"},
            },
        },
    },
    {
        "name": "get_supply_demand_gaps",
        "description": (
            "Identify supply-demand gaps: which advertisers are performing on other publishers but missing from a given publisher, "
            "or which publishers an advertiser is not running in. Also surfaces dead weight (provisioned but zero impressions in 30 days). "
            "Provide publisher_name OR advertiser_name, not both."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {
                    "type": "string",
                    "description": "Publisher to analyze (e.g. 'TextNow', 'Pinger'). Leave blank if using advertiser_name."
                },
                "advertiser_name": {
                    "type": "string",
                    "description": "Advertiser to analyze (e.g. 'Scrambly', 'BLD'). Leave blank if using publisher_name."
                }
            },
            "required": []
        },
    },
    {
        "name": "run_sql_query",
        "description": (
            "Execute an arbitrary ClickHouse SELECT query for questions not covered by other tools. "
            "Use the DATA DICTIONARY in your context to write correct SQL. "
            "Use for: any novel analytical question, multi-table joins, custom date ranges, "
            "cap/schedule config inspection, per-campaign payout lookups, "
            "serving group analysis, custom report recreation, or any query not covered by existing tools. "
            "Safety: SELECT-only, 500 row max by default. Always include a description of what you're querying. "
            "After getting results, present them clearly and add a sourcing callout: "
            "'> Queried: [description] — live ClickHouse'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Valid ClickHouse SQL SELECT statement. Use the DATA DICTIONARY for table/column names.",
                },
                "description": {
                    "type": "string",
                    "description": "One-line description of what this query retrieves, e.g. 'TurboTax campaign end dates and cap configs'",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Max rows to return (default 500, max 2000)",
                },
            },
            "required": ["sql", "description"],
        },
    },
    {
        "name": "get_ghost_campaigns",
        "description": (
            "Return the full list of ghost campaigns: active campaigns with high impressions + clicks "
            "but near-zero revenue (< $5 in last 7 days), older than 7 days. Includes per-campaign "
            "pixel/postback diagnosis. "
            "Use for: 'ghost brief', 'ghost campaigns', 'what campaigns are earning nothing', "
            "'campaigns with no revenue', 'show me the ghosts', 'zero revenue campaigns'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_low_fill_publishers",
        "description": (
            "Return publishers on post-transaction placements (checkout confirmation, order receipt, "
            "thank you pages, etc.) where fill rate is below 15% — meaning more than 85% of "
            "checkout sessions are receiving no offer. Uses a 7-day lookback window with a minimum of "
            "2,500 sessions over 7 days to filter out low-volume noise. "
            "Includes missed session count and revenue-at-risk estimate. "
            "Use for: 'fill rate', 'low fill rate', 'which publishers have low fill', 'sessions not getting offers', "
            "'offer fill', 'checkout fill', 'confirmation page fill', 'publishers underserving'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_top_revenue_opportunities",
        "description": (
            "Return top cross-publisher revenue gap opportunities: high-performing advertisers "
            "(active on 2+ publishers, >$10K/30d revenue) that are NOT yet active in high-volume publishers "
            "(>100K sessions/30d). Ranked by estimated monthly revenue. Shows total revenue at risk. "
            "Use for: 'revenue opportunities', 'what are we missing', 'what should we add', 'net-new revenue', "
            "'supply gaps', 'where should we add advertisers', 'uncaptured revenue', 'largest gaps'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_scout_status",
        "description": (
            "Return a system health snapshot: benchmark freshness, offer inventory count, "
            "queue depth, ClickHouse connectivity, and any data quality warnings. "
            "Use for: '@Scout status', 'how are you doing?', 'is Scout healthy?', "
            "'benchmark freshness', 'system check'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_revenue_today",
        "description": (
            "Return today's intraday revenue vs 30-day daily average, broken down by publisher. "
            "Use ONLY for 'how is revenue today / right now / so far'. "
            "Do NOT use for 'project / estimate / forecast / EOD / end of day / how will today land / "
            "after it ends' — use get_revenue_today_projection for those. "
            "Use for: 'how is revenue today', 'how are we doing today', 'how we looking', "
            "'today's revenue', 'revenue so far today', 'what's revenue at', 'how we doing'. "
            "Do NOT use run_sql_query for today's revenue — this tool exists specifically for this question."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_revenue_today_projection",
        "description": (
            "Project today's END-OF-DAY revenue using a 90-day hour-of-day arrival curve and "
            "8-week same-weekday median baseline. Leads with one number plus a confidence range, "
            "pace vs typical at this hour, and typical same-weekday EOD comparison. "
            "Refuses to project before 10am CT "
            "or when the curve sample is thin. "
            "Use for: 'project today's revenue', 'estimate today's revenue', 'EOD revenue', "
            "'what will today land at', 'how much will we make today', 'forecast today', "
            "'after it ends', 'how much do you estimate our revenue for today'. "
            "Do NOT use for 'revenue so far today' — that's get_revenue_today."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "run_offer_scraper",
        "description": (
            "Trigger an immediate offer inventory refresh from affiliate networks "
            f"({', '.join(SUPPORTED_NETWORKS)}). Takes ~2 minutes. Run when offer inventory "
            "is empty or stale. Updates offers_latest.json and posts the Scout Signal digest. "
            "Use for: 'refresh offers', 'run scraper', 'update offer inventory', "
            "'load benchmarks', 'inventory is empty', 'reload offers', 'fetch latest offers'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_pipeline_health",
        "description": (
            "Report on the Scout offer approval pipeline: total approved offers, "
            "stale offers (>7 days without Live/Done status), and oldest pending. "
            "Use for: 'pipeline health', 'how many offers went live', 'what is stuck in the queue', "
            "'pipeline status', 'are we launching offers', 'offer queue status'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_usage_report",
        "description": (
            "Return Scout usage statistics: queries per period, top users, most-used tools, avg response time. "
            "Admin-only — requires SCOUT_ADMIN_USER_ID env var match. "
            "Use for: 'scout usage', 'usage report', 'who uses scout', 'usage stats', "
            "'how often is scout used', 'who asks the most questions'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "requesting_user_id": {
                    "type": "string",
                    "description": "Slack user ID of the person asking — for admin authorization check.",
                }
            },
        },
    },
    {
        "name": "export_usage_log",
        "description": (
            "Dump raw (query → tools fired) pairs from usage_log so an admin can audit "
            "whether Scout's tool routing matched user intent. Admin-only. "
            "Use for: 'export usage', 'dump usage log', 'show usage entries', 'audit tool routing', "
            "'what tools fired for recent queries'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days":  {"type": "integer", "description": "Lookback window in days (default 30, clamped to 1..365).", "minimum": 1, "maximum": 365},
                "limit": {"type": "integer", "description": "Max entries to return (default 200, newest last; clamped to 1..500).", "minimum": 1, "maximum": 500},
                "requesting_user_id": {"type": "string", "description": "Slack user ID for admin gate."}
            },
        },
    },
    {
        "name": "record_entity_note",
        "description": (
            "Record publisher or advertiser knowledge in Scout's persistent learning store. "
            "Use when a team member explains a publisher's integration quirk, SDK limitation, or signal distortion, "
            "OR an advertiser's budget cap pattern, seasonality, attribution issue, or payout reliability. "
            "Publisher notes: set exclude_from_fill_rate=True when high session count with low fill is expected behavior "
            "(e.g., pre-purchase SDK calls, non-standard integration). "
            "Advertiser notes: budget caps, seasonal patterns, attribution quirks, campaign status context. "
            "Use for: '[entity] has a known limitation', 'note that [entity] does X', "
            "'log this about [entity]', 'exclude [publisher] from fill rate', "
            "'[advertiser] caps every [month]', '[advertiser] has attribution issues', "
            "'remember that [entity]...', 'scout, [entity] does X because...'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "entity_name": {
                    "type": "string",
                    "description": "Publisher or advertiser name (e.g., 'Button', 'TurboTax').",
                },
                "entity_type": {
                    "type": "string",
                    "enum": ["publisher", "advertiser"],
                    "description": "'publisher' for SDK/platform integrators, 'advertiser' for campaign/budget owners.",
                },
                "note": {
                    "type": "string",
                    "description": "The knowledge to record — what the team knows about this entity.",
                },
                "exclude_from_fill_rate": {
                    "type": "boolean",
                    "description": "Publishers only: True to suppress from Pulse fill rate signals (pre-purchase or non-standard integrations).",
                },
            },
            "required": ["entity_name", "entity_type", "note"],
        },
    },
    {
        "name": "forget_entity_note",
        "description": (
            "Drop a previously-recorded publisher or advertiser fact. "
            "Use when a team member tells Scout to forget, retract, or remove a learned note. "
            "Triggers: 'forget that about [entity]', 'scout, that was wrong about [entity]', "
            "'remove the note about [entity]', 'scratch that for [entity]', "
            "'unlearn [entity]', 'never mind about [entity]'. "
            "Idempotent — friendly message if no note exists."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "entity_name": {
                    "type": "string",
                    "description": "Publisher or advertiser name whose note should be dropped.",
                },
                "entity_type": {
                    "type": "string",
                    "enum": ["publisher", "advertiser"],
                    "description": "'publisher' or 'advertiser'.",
                },
            },
            "required": ["entity_name", "entity_type"],
        },
    },
    {
        "name": "why_entity_note",
        "description": (
            "Explain where a stored publisher/advertiser fact came from — returns the note, "
            "who taught Scout (Slack user_id), when, and the Slack permalink if available. "
            "Use when a team member challenges or audits Scout's beliefs about an entity. "
            "Triggers: 'why do you think [X] about [entity]', 'where did you learn that about [entity]', "
            "'who told you [entity] [does X]', 'source for [entity]', 'scout, justify [entity]'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "entity_name": {
                    "type": "string",
                    "description": "Publisher or advertiser name to audit.",
                },
                "entity_type": {
                    "type": "string",
                    "enum": ["publisher", "advertiser"],
                    "description": "Optional. Omit to search both sections.",
                },
            },
            "required": ["entity_name"],
        },
    },
    {
        "name": "get_offers_for_publisher",
        "description": (
            f"Return top affiliate offers (from {', '.join(SUPPORTED_NETWORKS)} inventory) that are "
            "a good fit for a specific publisher but not yet provisioned in their campaign set. "
            "Scored by estimated RPM using real MS conversion benchmarks. "
            "DIFFERENT from get_supply_demand_gaps — this surfaces net-new affiliate inventory, "
            "not advertisers already on the MS platform. "
            "Use for: 'offers for [partner]', 'what should we add to [partner]', "
            "'recommend offers for [partner]', 'what can we run on [partner]', "
            "'what's a good fit for [partner]', 'pitch ideas for [partner]', "
            "'affiliate offers for [partner]', 'new offers for [partner]'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "publisher_name": {
                    "type": "string",
                    "description": "The publisher/partner name (e.g., 'TextNow', 'PCH', 'Metropolis').",
                }
            },
            "required": ["publisher_name"],
        },
    },
    {
        "name": "run_self_qa",
        "description": (
            "Run Scout's full self-QA suite — 15 representative questions covering every major intent "
            "(system status, ghost campaigns, offer search, revenue analysis, publisher health, "
            "campaign status, revenue projection, supply gaps, perkswall, pipeline health, "
            "multi-part question protocol, and data boundary tests). "
            "Each question is evaluated for pass/fail by checking expected content signals. "
            "Use when the user says: 'QA yourself', 'self test', 'run QA', 'test yourself', "
            "'run the QA suite', 'scout QA', 'run self-qa', 'check yourself'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_pulse_summary",
        "description": (
            "Returns which monitoring signals have fired today (cap, velocity, ghost, fill, CVR anomaly, expiration). "
            "Use for: 'what did Scout flag today', 'what alerted this morning', 'what signals fired', "
            "'did anything fire', 'any alerts today', 'recent Scout signals', 'monitor recap'. "
            "Returns fired_today dict with per-signal booleans and currently_active list. "
            "Returns has_pulse=False with a message when no signals have fired yet today."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_scout_config",
        "description": (
            "Return Scout's current active configuration: scoring thresholds, signal thresholds, "
            "health-check intervals, supported networks, and Pulse schedule. "
            "Use when the team asks: 'what are Scout's thresholds', 'what's the fill rate cutoff', "
            "'how does Scout decide which offers to surface', 'what's the RPM floor', "
            "'what networks does Scout support', 'show me Scout's config', 'what are the velocity "
            "thresholds', 'when does the pulse run', 'health check settings'. "
            "Reads from config/scout_thresholds.json — always reflects current production values, "
            "no need to dig through source code."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_exposure_rate_anomalies",
        "description": (
            "Find publisher-campaign pairs where yesterday's exposure conversion rate dropped significantly "
            "vs. the 7-day baseline. Exposure CVR = conversions / impressions (measures what fraction of "
            "ad exposures convert — intentionally uses impressions denominator for anomaly detection). "
            "NOTE: this is NOT the canonical CVR (conversions / clicks). Use run_sql_query with "
            "CVR = conversions/clicks when answering general CVR questions. "
            "Only surfaces high-value campaigns (avg payout >= $50) with enough volume "
            "(7d impressions >= 5000) to make the signal actionable. "
            "Use when the team asks: 'which campaigns dropped CVR', 'conversion rate anomalies', "
            "'why are conversions down for X', 'CVR drops', 'postback issues', "
            "'which campaigns stopped converting', 'CVR regression'. "
            "Returns publisher, campaign, exposure CVR yesterday vs 7d average, delta %, and payout."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "min_impressions_7d": {
                    "type": "integer",
                    "description": "Minimum 7d impressions to include a campaign. Default: 5000.",
                },
                "min_payout": {
                    "type": "number",
                    "description": "Minimum avg payout per conversion in USD. Default: 50.",
                },
                "drop_pct": {
                    "type": "number",
                    "description": "Minimum CVR drop percentage to flag. Default: 30.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_expiring_campaigns",
        "description": (
            "Find active campaigns expiring within the next N days (default 7). "
            "Includes last-7d impression volume, active publisher count, and revenue "
            "so you can distinguish high-impact expirations from low-traffic ones. "
            "Use when the team asks: 'what campaigns are expiring', 'upcoming campaign endings', "
            "'campaigns ending this week', 'expiration warnings', 'renewal needed', "
            "'which offers are about to expire', 'campaign end dates'. "
            "Returns campaign, advertiser, end date, days remaining, impression/publisher/revenue activity."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "warning_days": {
                    "type": "integer",
                    "description": "Look-ahead window in days. Default: 7.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_publisher_revenue_trends",
        "description": (
            "Publisher velocity: identifies publishers trending significantly up or down in revenue. "
            "Uses canonical annualized comparison: ((rev_7d / 7) × 30 − rev_30d) / rev_30d × 100. "
            "Fires for publishers with pct_delta < −25% (velocity down) or > +20% (velocity up), "
            "minimum $5K revenue over the past 30 days. "
            "Use when the team asks: 'which publishers are trending up/down', 'revenue trends', "
            "'publisher velocity', 'who dropped revenue', 'which publishers improved this week', "
            "'publisher performance trends'. "
            "Returns publisher_name, rev_7d, rev_30d, pct_delta, direction ('up' or 'down')."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Ignored — always uses 7d/30d canonical window.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_advertiser_revenue_trends",
        "description": (
            "Advertiser revenue trends: compare each advertiser's actual revenue over the last N days "
            "against their historical median for the same period length (8 prior periods). "
            "Aggregated across all publishers — cross-publisher view of advertiser performance. "
            "Use when the team asks: 'which advertisers are trending up/down', 'advertiser revenue trends', "
            "'Capital One revenue vs historical', 'which advertisers improved this week', "
            "'advertiser performance compared to baseline', 'who dropped advertiser-side revenue'. "
            "Returns advertiser name, actual revenue, expected revenue, delta %, and trend direction."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Period length in days. Default: 7.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_publisher_fleet_health",
        "description": (
            "Fleet-level publisher health using statistical (σ-based) baseline. "
            "Classifies publishers into Act Now (>=2σ drop, >=$500 gap) and Watch (>=1.5σ, >=$200). "
            "Use for: 'how are all publishers doing?', 'Monday health report', 'fleet health', "
            "'which publishers need attention?', 'publisher overview'. "
            "Optional: days (default 7, max 90)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 7, "minimum": 1, "maximum": 90},
            },
            "required": [],
        },
    },
    {
        "name": "list_thresholds",
        "description": (
            "Return all active Scout monitor thresholds plus override metadata. "
            "Use when: 'what are the current thresholds', 'list scout thresholds', "
            "'show me threshold overrides', 'which thresholds have been changed', "
            "'what's the current cap_alert_pct', 'show monitor settings'."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_threshold_history",
        "description": (
            "Return recent threshold-change events from the changelog. "
            "Optional 'key' filter (e.g. 'signals.cap_alert_pct') and 'limit' (default 50). "
            "Use when: 'who changed the threshold', 'threshold history', 'why is X set to Y', "
            "'when did we tune the cap alert', 'show changelog for fill_rate_min_sessions_7d'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": "Optional filter like 'signals.cap_alert_pct'. Omit for all changes.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max entries to return, newest first. Default 50, max 500.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "set_threshold",
        "description": (
            "Admin-only — requires SCOUT_THRESHOLD_ADMINS env match. "
            "Write a runtime override for one Scout threshold; persisted to "
            "data/threshold_overrides.json and reloaded immediately. Always require a reason. "
            "Use when an admin says: 'change cap_alert_pct to 80', 'set the fill rate threshold to 3000', "
            "'tune ghost_recency_hours to 72', 'lower velocity_down_threshold_pct to -30'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "section": {
                    "type": "string",
                    "description": "Top-level section in scout_thresholds.json (e.g. 'signals', 'digest', 'health').",
                },
                "key": {
                    "type": "string",
                    "description": "Threshold name within the section (e.g. 'cap_alert_pct').",
                },
                "value": {
                    "description": "New value (number for numeric thresholds; type matches the config schema).",
                },
                "reason": {
                    "type": "string",
                    "description": "Why the threshold is changing — recorded permanently in the changelog.",
                },
            },
            "required": ["section", "key", "value", "reason"],
        },
    },
    {
        "name": "force_run_monitor",
        "description": (
            "Admin-only — requires SCOUT_THRESHOLD_ADMINS env match. "
            "Run a silent-monitor signal immediately and post results to #scout-qa. "
            "Use when an admin says: 'force run the cap monitor', 'rerun ghost detection now', "
            "'test the fill rate alert', 'fire velocity monitor on demand', 'force run cvr'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "monitor": {
                    "type": "string",
                    "description": "Which monitor to fire (e.g. cap, velocity, ghost, fill, cvr, expiration, revenue).",
                },
            },
            "required": ["monitor"],
        },
    },
]

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


def _load_offers() -> list:
    """Load offers from DEMAND_FEED_URL when set; fall back to disk snapshot.

    Logs source + offer count so the P1.2 cutover is verifiable from Render
    logs without grepping for stale-file timestamps.
    """
    url = os.getenv("DEMAND_FEED_URL")
    if url:
        endpoint = f"{url.rstrip('/')}/offers"
        # Strip userinfo/query/fragment before logging — endpoint may contain
        # credentials or tokens that must not leak to Render logs.
        _p = urllib.parse.urlparse(endpoint)
        _netloc = _p.hostname or ""
        if _p.port:
            _netloc = f"{_netloc}:{_p.port}"
        safe_endpoint = urllib.parse.urlunparse((_p.scheme, _netloc, _p.path, "", "", ""))
        # Reject non-http(s) schemes — urlopen() otherwise accepts file://,
        # ftp://, etc., which would let a misconfigured env var read local
        # files or hit arbitrary hosts.
        if _p.scheme not in ("http", "https"):
            log.warning(f"[scout_agent] DEMAND_FEED_URL has unsupported scheme {_p.scheme!r}; falling back to disk snapshot at {SNAPSHOT_PATH}")
        else:
            try:
                with urllib.request.urlopen(endpoint, timeout=10) as resp:
                    offers = json.loads(resp.read())
                log.info(f"[scout_agent] loaded {len(offers)} offers from {safe_endpoint}")
                return offers
            except Exception as exc:
                log.warning(f"[scout_agent] DEMAND_FEED_URL fetch failed ({type(exc).__name__}: {exc}); falling back to disk snapshot at {SNAPSHOT_PATH}")
    if not SNAPSHOT_PATH.exists():
        log.warning(f"[scout_agent] no offers source available — DEMAND_FEED_URL unset and {SNAPSHOT_PATH} missing")
        return []
    with open(SNAPSHOT_PATH) as f:
        offers = json.load(f)
    log.info(f"[scout_agent] loaded {len(offers)} offers from disk snapshot {SNAPSHOT_PATH}")
    return offers


def _norm(s: str) -> str:
    return s.lower().strip() if s else ""


# ── Tool implementations ─────────────────────────────────────────────────────

def search_offers(
    query: str,
    network: str = None,
    category: str = None,
    min_payout: float = None,
    max_payout: float = None,
    ms_status: str = None,
    limit: int = 5,
) -> list:
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    q = _norm(query)
    results = []
    for o in offers:
        text = _norm(o.get("advertiser", "")) + " " + _norm(o.get("description", ""))
        if q and q not in text:
            continue
        if network and _norm(o.get("network", "")) != _norm(network):
            continue
        if category and _norm(category) not in _norm(o.get("category", "")):
            continue
        payout_num = o.get("_payout_num") or 0
        if min_payout and payout_num < min_payout:
            continue
        if max_payout is not None and payout_num > max_payout:
            continue
        if ms_status and _norm(o.get("_ms_status", "")) != _norm(ms_status):
            continue
        results.append(o)

    results.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    return _format_offers(results[:limit], benchmarks)


def get_top_opportunities(category: str = None, geo: str = None, limit: int = 5) -> list:
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    results = []
    for o in offers:
        if o.get("_ms_status") != "Not in System":
            continue
        if category and _norm(category) not in _norm(o.get("category", "")):
            continue
        if geo and _norm(geo) not in _norm(o.get("geo", "")):
            continue
        results.append(o)

    results.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    return _format_offers(results[:limit], benchmarks)


def get_running_offers(category: str = None) -> list:
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    results = [
        o for o in offers
        if o.get("_ms_status") == "Live"
        and (not category or _norm(category) in _norm(o.get("category", "")))
    ]
    results.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    return _format_offers(results, benchmarks)


def get_category_performance(category: str = None) -> dict:
    benchmarks = _get_benchmarks()
    by_cat = benchmarks.get("by_category", {})
    by_offer = benchmarks.get("by_offer_impact_id", {})

    if category:
        cat_key = next((k for k in by_cat if _norm(category) in _norm(k)), None)
        cat_data = {cat_key: by_cat[cat_key]} if cat_key else {}
    else:
        cat_data = by_cat

    # Also include top individual offer benchmarks
    top_offers = sorted(by_offer.items(), key=lambda x: x[1].get("rpm", 0), reverse=True)[:10]

    return {
        "category_benchmarks": cat_data,
        "note": "CVR and RPM are real MS performance data from ClickHouse (Jan 2025+). Use RPM to estimate expected value of new offers: RPM = payout × (CVR/100) × 1000.",
        "top_performing_offers_by_rpm": [
            {"impact_id": k, **v} for k, v in top_offers
        ],
    }


def get_offer_stats() -> dict:
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    if not offers:
        return {"error": "No offer data available"}

    by_network: dict = {}
    by_category: dict = {}
    by_ms_status: dict = {}

    for o in offers:
        net = o.get("network", "unknown")
        score = _scout_score(o, benchmarks)
        payout = o.get("_payout_num") or 0
        cats = o.get("_categories") or [o.get("category", "Other")]
        ms = o.get("_ms_status", "Unknown")

        by_network.setdefault(net, {"count": 0, "total_score": 0, "total_payout": 0})
        by_network[net]["count"] += 1
        by_network[net]["total_score"] += score
        by_network[net]["total_payout"] += payout

        for cat in (cats if isinstance(cats, list) else [cats]):
            by_category.setdefault(cat, {"count": 0, "total_score": 0})
            by_category[cat]["count"] += 1
            by_category[cat]["total_score"] += score

        by_ms_status[ms] = by_ms_status.get(ms, 0) + 1

    top5 = sorted(offers, key=lambda x: _scout_score(x, benchmarks), reverse=True)[:5]

    return {
        "total_offers": len(offers),
        "by_network": {
            k: {
                "count": v["count"],
                "avg_scout_score_rpm": round(v["total_score"] / v["count"], 2),
                "avg_payout": round(v["total_payout"] / v["count"], 2),
            }
            for k, v in sorted(by_network.items(), key=lambda x: -x[1]["count"])
        },
        "by_category": {
            k: {
                "count": v["count"],
                "avg_scout_score_rpm": round(v["total_score"] / v["count"], 2),
            }
            for k, v in sorted(by_category.items(), key=lambda x: -x[1]["count"])
            if k and k != "Other"
        },
        "ms_status_breakdown": by_ms_status,
        "top_5_by_scout_score": _format_offers(top5, benchmarks),
    }


def _format_offers(offers: list, benchmarks: dict) -> list[FormattedOffer]:
    """Return a compact, readable version of each offer for the LLM, including Scout Score context."""
    by_offer = benchmarks.get("by_offer_impact_id", {})
    by_cat = benchmarks.get("by_category", {})
    out = []
    for o in offers:
        offer_id = str(o.get("offer_id", ""))
        category = o.get("category", "")
        score = _scout_score(o, benchmarks)

        # Performance context
        if offer_id in by_offer:
            perf = by_offer[offer_id]
            perf_note = f"Real MS data: {perf['cvr_pct']}% CVR, ${perf['rpm']} RPM"
        elif category in by_cat:
            cat_perf = by_cat[category]
            perf_note = f"Category benchmark: {cat_perf['avg_cvr_pct']}% CVR avg, ${cat_perf['avg_rpm']} RPM avg"
        else:
            perf_note = "No MS performance data for this category yet"

        advertiser = o.get("advertiser", "")
        out.append({
            "advertiser": advertiser,
            "network": o.get("network", ""),
            "offer_id": offer_id,
            "payout": o.get("_raw_payout") or o.get("payout") or "Rate TBD",
            "payout_num": o.get("_payout_num"),
            "payout_type": o.get("_payout_type_norm") or o.get("payout_type", ""),
            "category": category,
            "geo": o.get("geo", ""),
            "ms_status": o.get("_ms_status", ""),
            "ms_internal_name": o.get("_ms_internal_name", ""),
            "fit_tier": o.get("fit_tier", "STANDARD"),
            "last_verified": o.get("last_verified"),
            "scout_score_rpm": score,
            "performance_context": perf_note,
            "risk_flag": _get_risk_flag(advertiser, category, o.get("description", "")),
            "icon_url":  o.get("icon_url", ""),
            "hero_url":  o.get("hero_url", ""),
        })
    return out


def _format_payout(payout_num, payout_type_norm: str, raw_payout: str) -> str:
    """Normalize payout display: '$300 / lead' not '300 $ per lead'."""
    if not payout_num:
        return raw_payout or "Rate TBD"
    ptype = (payout_type_norm or "").lower()
    num = float(payout_num)
    fmt = f"{num:,.0f}" if num >= 1 and num == int(num) else f"{num:,.2f}"
    if "lead" in ptype or "cpl" in ptype:
        return f"${fmt} / lead"
    elif "click" in ptype or "cpc" in ptype:
        return f"${fmt} / click"
    elif "sale" in ptype or "%" in ptype:
        return f"{num}% of sale"
    elif "install" in ptype or "cpi" in ptype:
        return f"${fmt} / install"
    elif "impression" in ptype or "cpm" in ptype:
        return f"${fmt} CPM"
    return f"${fmt}"


# Risk flags: keyed by trigger keyword lists.
# Shown on the brief to flag post-transaction fit issues before launch.
_RISK_PATTERNS = [
    (["employer", "hiring", "recruitment", "recruiter", "payroll", "crm", "erp", "b2b", "business banking"],
     "B2B intent — post-transaction CVR typically 50-70% lower than consumer offers; test conservatively"),
    (["loan", "lending", "mortgage", "refinance", "credit repair", "debt consolidation"],
     "Loan/credit offers are high-friction; monitor CVR closely and verify compliance"),
    (["glp-1", "weight loss program", "prescription", "telehealth", "medical weight"],
     "Medical program — high-intent required; verify geo/age compliance before launch"),
    (["insurance", "life insurance", "auto insurance", "home insurance"],
     "Insurance sign-ups are high-friction; expect below-category CVR post-transaction"),
    (["work from home", "earn from home", "make money online", "business opportunity", "profit scaling"],
     "Biz-opp offer — elevated brand risk; evaluate publisher fit carefully"),
]


def _get_risk_flag(advertiser: str, category: str, description: str) -> str:
    """Return a one-line risk warning if the offer is a poor post-transaction fit."""
    combined = f"{advertiser} {category} {description}".lower()
    for keywords, flag in _RISK_PATTERNS:
        if any(kw in combined for kw in keywords):
            return flag
    return ""


def _network_portal_url(network: str, offer_id: str) -> str:
    """Construct a direct link to the offer in the network's portal."""
    n = network.lower()
    if n == "maxbounty":
        return ""  # URL structure changed post-mrge acquisition — use Offer ID for manual lookup
    elif n == "impact":
        return "https://app.impact.com"
    elif n == "flexoffers" and offer_id:
        return f"https://www.flexoffers.com/affiliate-programs/{offer_id}/"
    return ""


_TRACKING_DOMAINS = {
    # Known affiliate tracking domains — URLs on these are real tracking links
    "impact.com", "sjv.io", "pxf.io", "bn5x.net", "ibfwsl.net",
    "maxbounty.com", "flexoffers.com", "jdoqocy.com", "tkqlhce.com",
    "launchingdeals.com", "adspostx.com", "pubtailer.com",
    "collectsavings.com", "referral.", "go.",
}

_CLICK_ID_PATTERNS = ("{click_id}", "{subid}", "subId", "clickid", "click_id", "aff_id")


def _validated_tracking_url(network: str, platform_url: str, scraper_url: str) -> str:
    """
    Return the best tracking URL for the brief, or a fallback message.
    Platform URL (from MS) is always preferred — it has {click_id} template.
    Scraper URL is only used if it looks like a real affiliate link, not the advertiser's site.
    """
    if platform_url:
        return platform_url

    if scraper_url:
        url_lower = scraper_url.lower()
        # Accept if it looks like a tracking link: known domain or click_id pattern
        if any(d in url_lower for d in _TRACKING_DOMAINS):
            return scraper_url
        if any(p.lower() in url_lower for p in _CLICK_ID_PATTERNS):
            return scraper_url
        # Reject — it's the advertiser's website, not an affiliate link
        log.info(f"Rejected non-tracking URL for {network} offer: {scraper_url[:60]}")

    return "Not available — pull from network portal"


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


def draft_campaign_brief(advertiser: str, network: str = None) -> dict:
    """
    Fetch all offer details needed to generate a campaign brief.
    Matches by partial advertiser name (case-insensitive), picks highest Scout Score match.
    """
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    q = _norm(advertiser)
    matches = [
        o for o in offers
        if q in _norm(o.get("advertiser", ""))
        and (not network or _norm(network) == _norm(o.get("network", "")))
    ]
    if not matches:
        return {"error": f"No offer found matching '{advertiser}'. Try a partial name or different spelling."}

    matches.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)
    o = matches[0]
    offer_id = str(o.get("offer_id", ""))
    net = (o.get("network") or "").lower()
    by_offer   = benchmarks.get("by_offer_impact_id", {})
    by_adv     = benchmarks.get("by_adv_name", {})
    by_cat_pt  = benchmarks.get("by_category_payout", {})
    by_pt      = benchmarks.get("by_payout_type", {})

    adv_key    = (o.get("advertiser") or "").lower().strip()
    category   = (o.get("category") or "").strip()
    payout_type = (o.get("_payout_type_norm") or "").lower().strip()

    if offer_id in by_offer:
        perf = by_offer[offer_id]
        perf_context = f"Real MS data: {perf['cvr_pct']}% CVR, ${perf['rpm']} RPM ({perf['impressions']:,} impressions)"
    elif adv_key in by_adv:
        perf = by_adv[adv_key]
        perf_context = f"Same advertiser benchmark: {perf['cvr_pct']}% CVR, ${perf['rpm']} RPM"
    elif (category, payout_type) in by_cat_pt:
        perf = by_cat_pt[(category, payout_type)]
        perf_context = f"{category} {payout_type} benchmark: {perf['avg_cvr_pct']}% avg CVR ({perf['sample_campaigns']} offers)"
    elif payout_type in by_pt:
        perf = by_pt[payout_type]
        perf_context = f"{payout_type} avg across all categories: {perf['avg_cvr_pct']}% avg CVR ({perf['sample_campaigns']} offers)"
    else:
        perf_context = "No MS performance data at any tier"

    icon_url = o.get("icon_url", "")
    hero_url = o.get("hero_url", "")

    score = _scout_score(o, benchmarks)

    # Pull platform copy + CDN images from MS ClickHouse.
    # The MS platform has approved title/CTA/tracking URL already — use them as source of truth.
    platform_title = platform_cta_yes = platform_cta_no = ""
    platform_landing_url = restrictions = platform_image = ""
    ms_id = None
    try:
        ch = _get_ch_client()
        p_rows = ch.query(
            """
            SELECT id, title, cta_yes, cta_no, landing_url, internal_notes
            FROM default.from_airbyte_campaigns
            WHERE lower(adv_name) LIKE lower(concat('%', {adv:String}, '%'))
              AND deleted_at IS NULL
              AND status != 'inactive'
            ORDER BY id DESC LIMIT 1
            """,
            parameters={"adv": o.get("advertiser", "")},
        ).result_rows
        if p_rows:
            ms_id, platform_title, platform_cta_yes, platform_cta_no, platform_landing_url, ms_notes = p_rows[0]
            platform_title = platform_title or ""
            platform_cta_yes = platform_cta_yes or ""
            platform_cta_no = platform_cta_no or ""
            platform_landing_url = platform_landing_url or ""
            restrictions = (ms_notes or "").strip()
            # CDN image: prefer publisher-specific creative, fall back to campaign-level
            img_rows = ch.query(
                "SELECT url FROM default.from_airbyte_publisher_campaign_images"
                " WHERE campaign_id = {cid:Int64} AND deleted_at IS NULL LIMIT 1",
                parameters={"cid": int(ms_id)},
            ).result_rows
            if img_rows:
                platform_image = img_rows[0][0] or ""
    except Exception as e:
        from scout_ch import CHBusyError
        if isinstance(e, CHBusyError):
            raise
        log.warning(f"draft_campaign_brief: platform lookup failed: {e}")

    # Image sourcing — see _clearbit_domain / _app_store_icon / _google_favicon for source rationale
    if platform_image:
        hero_url = icon_url = platform_image
    else:
        advertiser_name = o.get("advertiser", "")

        # hero_url: try MS CDN first (1,032 CDN images already in ClickHouse)
        cdn_hero = _ms_cdn_image(str(ms_id) if ms_id else "")
        if cdn_hero:
            hero_url = cdn_hero

        # Check external image cache before hitting iTunes / Clearbit / gstatic
        cached = _cached_external_images(advertiser_name)
        if cached:
            if not hero_url:
                hero_url = cached.get("hero_url", "")
            icon_url = cached.get("icon_url", "")
        else:
            app_icon = _app_store_icon(advertiser_name)
            domain   = _clearbit_domain(advertiser_name)
            favicon  = _google_favicon(domain) if domain else ""

            # hero: MS CDN (already set above) > App Store 512px > gstatic 256px > empty
            if not hero_url:
                if app_icon:
                    hero_url = app_icon
                elif favicon and _validate_image_url(favicon):
                    hero_url = favicon

            # icon: App Store > gstatic > empty
            # App Store preferred over gstatic because it's matched by brand name,
            # not domain — avoids wrong favicon when Clearbit autocomplete is off
            # (e.g., "Square" resolves to squarespace.com via Clearbit, but the
            # App Store correctly returns Square's payments app)
            if app_icon:
                icon_url = app_icon
            elif favicon and _validate_image_url(favicon):
                icon_url = favicon

            # Store results so next brief for same advertiser skips API calls
            _store_image_cache(advertiser_name, hero_url, icon_url)

    # Proactive fallback intelligence — surface at brief creation (highest-intent moment)
    fallback = get_fallback_candidates(o.get("advertiser", ""), category=o.get("category"))
    fallback_same_brand = fallback.get("same_brand_alts", [])[:1]
    fallback_category_subs = fallback.get("category_alts", [])[:2]

    return {
        "advertiser": o.get("advertiser"),
        "network": o.get("network"),
        "offer_id": offer_id,
        "payout": _format_payout(o.get("_payout_num"), o.get("_payout_type_norm"), o.get("_raw_payout") or str(o.get("payout", ""))),
        "payout_num": o.get("_payout_num"),
        "payout_type": o.get("_payout_type_norm") or "",
        "geo": o.get("geo"),
        "tracking_url": _validated_tracking_url(net, platform_landing_url, o.get("tracking_url", "")),
        "description": (o.get("description") or "")[:300],
        "category": o.get("category"),
        "ms_status": o.get("_ms_status"),
        "performance_context": perf_context,
        "scout_score_rpm": score,
        "portal_url": _network_portal_url(net, offer_id),
        "risk_flag": _get_risk_flag(
            o.get("advertiser", ""),
            o.get("category", ""),
            o.get("description", ""),
        ),
        "icon_url":   icon_url,
        "hero_url":   hero_url,
        "banner_url": o.get("banner_url", ""),  # raw network banner creative (not brand logo)
        # Platform copy — use these directly; generate only when empty
        "platform_title": platform_title,
        "platform_cta_yes": platform_cta_yes,
        "platform_cta_no": platform_cta_no,
        "restrictions": restrictions,
        "fallback_same_brand": fallback_same_brand,
        "fallback_category_subs": fallback_category_subs,
    }


def get_publisher_competitive_landscape(
    publisher_name: str = None,
    publisher_id: int = None,
    offer_name: str = None,
    hypothetical_payout: float = None,
    weeks: int = 2,
) -> dict:
    """
    Query ClickHouse for what's competing on a given publisher right now.
    Returns: ranked offer list by RPM, publisher weekly impression volume,
    and (if offer_name + hypothetical_payout supplied) where that offer would rank
    at current vs. hypothetical payout.

    BLUF output: "At $40 CPA, TurboTax would rank #3 of 8 on AT&T (~12% share).
                  At $35 it ranks #5 (~7% share). AT&T runs ~180K impressions/week."
    """
    try:
        ch = _get_ch_client()

        # Step 1: find publisher — by numeric ID or name.
        # IMPORTANT: adpx_impressions_details.pid stores the numeric user id as a string,
        # NOT the hex sdk_id. Always use str(id) as the pid for impression queries.
        if not publisher_name and not publisher_id:
            return {"error": "Provide either publisher_name or publisher_id."}

        if publisher_id:
            pub_result = _q.publisher_lookup_by_id(ch, int(publisher_id))
            if not pub_result:
                return {"error": f"No publisher found with ID {publisher_id}."}
            pub_rows = [(pub_result["id"], pub_result["organization"], pub_result["sdk_id"])]
        else:
            raw = _q.publisher_lookup_by_name(ch, publisher_name)
            if not raw:
                return {"error": f"No publisher found matching '{publisher_name}'. Try a shorter name e.g. 'AT&T' or 'TXB'."}
            pub_rows = [(r["id"], r["organization"], r["sdk_id"]) for r in raw]

        if not pub_rows:
            return {"error": f"No publisher found matching '{publisher_name}'. Try a shorter name e.g. 'AT&T' or 'TXB'."}

        # Pick the best row in a single pass — no extra ClickHouse roundtrips.
        # Priority: non-test/demo row whose id has the most recent impressions.
        candidate_ids = [str(row[0]) for row in pub_rows]
        vol_map = _q.publisher_impression_volume(ch, candidate_ids, days=7)

        # Score each candidate: deprioritize test/demo, then rank by volume
        def _candidate_score(row) -> tuple:
            org = (row[1] or "").lower()
            is_noise = int("test" in org or "demo" in org)
            volume = vol_map.get(str(row[0]), 0)
            return (is_noise, -volume)  # lower is better

        chosen = sorted(pub_rows, key=_candidate_score)[0]

        pub_id_int, pub_full_name, pub_sdk_id = chosen[0], chosen[1], chosen[2]
        # pid in impressions table = numeric user id as string
        pub_pid = str(pub_id_int)

        # Step 2 + Step 3: weekly impression volume + provisioned campaigns (sequential)
        # SQL lives in queries.py — parameterized, no f-strings
        vol_data = _q.publisher_weekly_impressions(ch, pub_pid, days=28)
        prov_data = _q.publisher_provisioned_campaigns(ch, pub_id_int)

        vol_rows = [(d["week"], d["impressions"]) for d in vol_data]
        prov_rows = [(d["campaign_id"], d["adv_name"], d["payout"]) for d in prov_data]

        weekly_impressions = int(sum(r[1] for r in vol_rows) / max(len(vol_rows), 1)) if vol_rows else 0

        # Step 4: determine which provisioned campaigns have recent impressions (serving now).
        serving_map: dict = {}  # campaign_id → impression count
        rpm_map: dict = {}      # campaign_id → RPM (only for serving campaigns)

        if prov_data:
            prov_ids = [d["campaign_id"] for d in prov_data]
            serving_map = _q.publisher_serving_campaign_impressions(ch, pub_pid, prov_ids, days=14)

            if serving_map:
                serving_ids = list(serving_map.keys())
                rpm_map = _q.publisher_campaign_rpms(ch, pub_pid, serving_ids, days=14)

        # Build unified list — serving first (by RPM), then provisioned-only (by payout desc).
        competitors = []
        for row in prov_rows:
            campaign_id, adv_name, payout = row
            cid = str(campaign_id)
            impressions_2w = serving_map.get(cid, 0)
            is_serving = impressions_2w > 0
            rpm = rpm_map.get(cid, 0.0)
            competitors.append({
                "advertiser": adv_name,
                "campaign_id": cid,
                "provisioned": True,
                "is_serving": is_serving,
                "impressions_2w": impressions_2w,
                "rpm": rpm,
                "payout": float(payout) if payout else None,
            })
        competitors.sort(key=lambda x: (0 if x["is_serving"] else 1, -x["rpm"], -(x["payout"] or 0)))

        serving_count = sum(1 for c in competitors if c["is_serving"])
        result = {
            "publisher": pub_full_name,
            "publisher_pid": pub_pid,
            "weekly_impressions_avg": weekly_impressions,
            "projected_impressions_2w": weekly_impressions * weeks,
            "provisioned_campaigns": competitors,
            "provisioned_count": len(competitors),
            "serving_count": serving_count,
            # payout_scenario logic below expects active_competitors
            "active_competitors": [c for c in competitors if c["is_serving"]],
            "competitor_count": serving_count,
        }

        # Step 4: if offer + payout provided, compute rank scenarios
        if offer_name and hypothetical_payout is not None:
            benchmarks = _get_benchmarks()

            # Find the offer from snapshot — use it to get category/network context for scoring
            matched_offer = next(
                (o for o in _load_offers() if offer_name.lower() in (o.get("advertiser") or "").lower()),
                None,
            )
            current_payout = matched_offer.get("_payout_num") if matched_offer else None

            def _est_rpm(payout: float) -> float:
                synth = {**(matched_offer or {}), "_payout_num": payout}
                return round(_scout_score(synth, benchmarks), 2)

            hyp_rpm = _est_rpm(hypothetical_payout)
            cur_rpm = _est_rpm(current_payout) if current_payout else None

            rpms = sorted([c["rpm"] for c in competitors if c["rpm"] > 0], reverse=True)

            def _rank(rpm: float) -> int:
                return sum(1 for r in rpms if r > rpm) + 1

            def _share_pct(rank: int, total: int) -> float:
                if total == 0:
                    return 0.0
                # Simple linear decay: rank 1 gets 2x share of last, proportional
                weight = max(total - rank + 1, 1)
                total_weight = sum(range(1, total + 2))
                return round(weight / total_weight * 100, 1)

            n = len(rpms)
            hyp_rank = _rank(hyp_rpm)
            hyp_share = _share_pct(hyp_rank, n)

            result["payout_scenario"] = {
                "offer": offer_name,
                "hypothetical_payout": hypothetical_payout,
                "hypothetical_rpm": hyp_rpm,
                "hypothetical_rank": hyp_rank,
                "hypothetical_impression_share_pct": hyp_share,
                "projected_impressions_2w": round(weekly_impressions * weeks * hyp_share / 100),
            }

            if cur_rpm is not None:
                cur_rank = _rank(cur_rpm)
                cur_share = _share_pct(cur_rank, n)
                result["payout_scenario"]["current_payout"] = current_payout
                result["payout_scenario"]["current_rpm"] = cur_rpm
                result["payout_scenario"]["current_rank"] = cur_rank
                result["payout_scenario"]["current_impression_share_pct"] = cur_share
                result["payout_scenario"]["current_impressions_2w"] = round(
                    weekly_impressions * weeks * cur_share / 100
                )

        return result

    except Exception as e:
        from scout_ch import CHBusyError
        if isinstance(e, CHBusyError):
            raise
        log.warning(f"get_publisher_competitive_landscape failed: {e}")
        return {"error": str(e)}


def get_fallback_candidates(
    offer_name: str,
    category: str = None,
    payout_type: str = None,
    limit: int = 4,
) -> dict:
    """
    Given an offer that's live (or might go dark), find the best replacements.

    Priority order:
    1. Same advertiser on a different network (Sam's Club on MaxBounty when Rakuten hits cap)
    2. Same category + similar payout type, not currently live in MS, ranked by Scout Score

    Returns two lists — 'same_brand_alts' and 'category_alts' — so Scout can present
    a tiered answer: same brand first (plug-and-play), then category substitutes.
    """
    offers = _load_offers()
    benchmarks = _get_benchmarks()
    q = _norm(offer_name)

    # Find the primary offer to infer category/payout_type/network
    primary = None
    for o in offers:
        if q in _norm(o.get("advertiser", "")):
            primary = o
            break

    inferred_category = category or (primary.get("category") if primary else None)
    inferred_ptype = payout_type or (primary.get("_payout_type_norm") if primary else None)
    primary_network = _norm(primary.get("network") or "") if primary else None

    # Tier 1: same advertiser, different network
    same_brand = [
        o for o in offers
        if q in _norm(o.get("advertiser", ""))
        and _norm(o.get("network") or "") != primary_network
    ]
    same_brand.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)

    # Tier 2: same category, not already live, ranked by Scout Score
    cat_subs = [
        o for o in offers
        if o.get("_ms_status") != "Live"
        and inferred_category and _norm(inferred_category) in _norm(o.get("category") or "")
        and q not in _norm(o.get("advertiser") or "")
        and (not inferred_ptype or _norm(inferred_ptype) in _norm(o.get("_payout_type_norm") or ""))
    ]
    cat_subs.sort(key=lambda x: _scout_score(x, benchmarks), reverse=True)

    return {
        "primary_offer": offer_name,
        "primary_network": primary_network,
        "primary_category": inferred_category,
        "same_brand_alts": _format_offers(same_brand[:limit], benchmarks),
        "category_alts": _format_offers(cat_subs[:limit], benchmarks),
        "note": "same_brand_alts = same advertiser on a different network (plug-and-play swap). category_alts = next best in vertical if brand unavailable on any network.",
    }


# ── Demand Queue lifecycle tools ─────────────────────────────────────────────

_LAUNCHED_OFFERS_PATH = pathlib.Path(__file__).parent / "data" / "launched_offers.json"


def _load_launched_offers_state() -> dict:
    try:
        if _LAUNCHED_OFFERS_PATH.exists():
            return json.loads(_LAUNCHED_OFFERS_PATH.read_text())
    except Exception as e:
        log.debug("_load_launched_offers_state swallowed: %s", e)
    return {}


def _load_pulse_state_local() -> dict:
    """Read pulse_state.json directly. Scout-agent local copy — avoids importing scout_state."""
    try:
        if _PULSE_STATE_PATH.exists():
            return json.loads(_PULSE_STATE_PATH.read_text())
    except Exception as e:
        log.debug("_load_pulse_state_local swallowed: %s", e)
    return {}


def get_pulse_summary() -> dict:
    """
    Return a summary of which monitoring signals have fired today.
    Reads from per-signal state keys written by the individual monitor daemons.

    Returns:
      has_pulse (bool): True if any signal fired today
      had_content (bool): same as has_pulse
      fired_today (dict): per-signal boolean — which fired today
      currently_active (list[str]): alert names currently firing in the registry
      message (str): human-readable summary
    """
    try:
        # alert_registry imported locally — it initialises the registry on import and
        # must not run at module load time (it has side-effects / may not be available
        # in test environments that only import scout_agent without the daemon running).
        import alert_registry as _ar

        state = _load_pulse_state_local()
        today = _dt_mod.datetime.now(ZoneInfo("America/Chicago")).date().isoformat()

        # Cap: after Phase 2, key is last_cap_alert_slot (YYYY-MM-DDTHH prefix)
        # Before Phase 2, key is last_cap_alert_date (YYYY-MM-DD)
        _cap_slot = state.get("last_cap_alert_slot", "")
        _cap_date = state.get("last_cap_alert_date", "")
        cap_fired = (_cap_slot[:10] == today) if _cap_slot else (_cap_date == today)

        fired_today = {
            "cap": cap_fired,
            "velocity_down": state.get("last_velocity_down_alert_date") == today,
            "ghost": state.get("last_ghost_alert_date") == today,
            "fill_rate": state.get("last_fill_alert_date") == today,
            "cvr_anomaly": state.get("last_cvr_anomaly_alert_date") == today,
            "expiration": state.get("last_expiration_alert_date") == today,
        }

        any_fired = any(fired_today.values())

        try:
            currently_active = [s.alert_name for s in _ar.current_state()]
        except Exception:
            currently_active = []

        if any_fired:
            fired_names = [k for k, v in fired_today.items() if v]
            msg = "Today's signals fired: " + ", ".join(fired_names)
        else:
            msg = "No monitoring signals have fired today yet."

        return {
            "has_pulse": any_fired,
            "had_content": any_fired,
            "fired_today": fired_today,
            "currently_active": currently_active,
            "message": msg,
        }
    except Exception as e:
        log.warning(f"get_pulse_summary failed: {e}")
        return {"has_pulse": False, "message": f"Could not read pulse state: {e}"}


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
            "thresholds": SCOUT_THRESHOLDS,
            "supported_networks": list(SUPPORTED_NETWORKS),
            "active_networks_in_inventory": live_networks,
            "pulse": {
                "enabled": os.getenv("PULSE_ENABLED", "true").lower() == "true",
                "schedule": "8am CT daily",
                "opportunities_displayed": "Mondays only (computed daily)",
            },
            "config_file": str(_SCOUT_THRESHOLDS_FILE.relative_to(pathlib.Path(__file__).parent)),
            "overridden_keys": overridden_keys,
            "last_override_at": last_override_at,
            "data_quality": _data_quality_tier(days_of_data=999),  # config is static — N/A applies
        }
    except Exception as e:
        log.warning(f"get_scout_config failed: {e}")
        return {"error": str(e), "thresholds": _SCOUT_THRESHOLDS_FALLBACK}


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
        "thresholds": SCOUT_THRESHOLDS,
        "overridden": overridden,
        "config_file": str(_SCOUT_THRESHOLDS_FILE.relative_to(pathlib.Path(__file__).parent)),
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
    """Admin-only: write a runtime override for one threshold and reload SCOUT_THRESHOLDS.

    Override persists in data/threshold_overrides.json and is layered on top of
    config/scout_thresholds.json at every _load_thresholds() call. The append-only
    changelog records the actor, prior value, new value, and reason.
    """
    global SCOUT_THRESHOLDS
    if not _is_admin(_caller_user_id):
        return {"ok": False, "error": "not_admin",
                "message": ":lock: Threshold changes are admin-only (set SCOUT_THRESHOLD_ADMINS)."}

    section = (section or "").strip()
    key = (key or "").strip()
    if not section or not key:
        return {"ok": False, "error": "missing_args",
                "message": "section and key are required (e.g. section='signals', key='cap_alert_pct')."}
    if value is None:
        return {"ok": False, "error": "missing_value", "message": "value is required."}
    if not reason or not reason.strip():
        return {"ok": False, "error": "missing_reason",
                "message": "reason is required so the changelog stays useful."}

    # Reject unknown keys with closest-match suggestion. Validate against the
    # BASE schema (fallback + config file), not the post-override merge — otherwise
    # a previously persisted typo in data/threshold_overrides.json would mask
    # itself by appearing "known."
    known_section = _BASE_THRESHOLDS.get(section)
    if not isinstance(known_section, dict):
        sections = list(_BASE_THRESHOLDS.keys())
        suggestions = difflib.get_close_matches(section, sections, n=1, cutoff=0.6)
        hint = f" Did you mean `{suggestions[0]}`?" if suggestions else ""
        return {"ok": False, "error": "unknown_section",
                "message": f"Unknown section `{section}` (valid: {', '.join(sections)}).{hint}"}
    if key not in known_section:
        keys = list(known_section.keys())
        suggestions = difflib.get_close_matches(key, keys, n=1, cutoff=0.6)
        hint = f" Did you mean `{section}.{suggestions[0]}`?" if suggestions else ""
        return {"ok": False, "error": "unknown_key",
                "message": f"Unknown key `{section}.{key}`.{hint}"}

    # Capture prior value (post-override merge — what callers actually saw)
    prior = SCOUT_THRESHOLDS.get(section, {}).get(key) if isinstance(SCOUT_THRESHOLDS.get(section), dict) else None

    try:
        import scout_state
        overrides = scout_state._load_threshold_overrides() or {}
        if section not in overrides or not isinstance(overrides[section], dict):
            overrides[section] = {}
        ts = datetime.now(timezone.utc).isoformat()
        overrides[section][key] = {
            "value": value,
            "set_by": _caller_user_id or "unknown",
            "set_at": ts,
            "reason": reason.strip(),
        }
        scout_state._save_threshold_overrides(overrides)

        scout_state._append_threshold_changelog({
            "ts": ts,
            "key": f"{section}.{key}",
            "section": section,
            "name": key,
            "prior": prior,
            "value": value,
            "set_by": _caller_user_id or "unknown",
            "reason": reason.strip(),
            "action": "set",
        })

        # Reload module-level SCOUT_THRESHOLDS so this process sees the change
        SCOUT_THRESHOLDS = _load_thresholds()

        return {
            "ok": True,
            "section": section,
            "key": key,
            "prior": prior,
            "value": value,
            "set_by": _caller_user_id,
            "set_at": ts,
            "reason": reason.strip(),
        }
    except Exception as e:
        log.warning(f"set_threshold failed: {e}")
        return {"ok": False, "error": "write_failed", "message": str(e)}


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

    name = (monitor or "").strip().lower()
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
        fn(web, os.getenv("SCOUT_SHADOW_CHANNEL", "#scout-qa"), "")
        return {"ok": True, "monitor": name, "by_user_id": _caller_user_id,
                "message": f"Force-ran {name} monitor — results posted to {os.getenv('SCOUT_SHADOW_CHANNEL', '#scout-qa')}."}
    except Exception as e:
        log.warning(f"force_run_monitor({name}) failed: {e}")
        return {"ok": False, "error": "execution_failed", "message": str(e)}


def get_queue_status() -> dict:
    """
    Fetch the offer pipeline queue from Notion and return a Block Kit card.
    Shared render path with App Home tab and /scout-queue — same data, same view.
    Returns a dict with 'blocks' (list) and 'text' (fallback string).
    """
    from scout_notion import _fetch_notion_queue_items
    from scout_ui_kit import _build_queue_card

    items = _fetch_notion_queue_items()
    blocks = _build_queue_card(items)
    if items is None:
        text = "Could not reach Notion — queue data unavailable."
    elif not items:
        text = "Queue is clear — nothing awaiting entry or in platform."
    else:
        text = f"Offer pipeline: {len(items)} offer{'s' if len(items) != 1 else ''} in queue."
    return {"blocks": blocks, "text": text}


def get_demand_queue_status() -> dict:
    """
    Read the MS Demand Queue state from launched_offers.json.
    Cross-references ClickHouse for impressions since each offer's approved_at date —
    if impressions > 0 the offer is likely live. Returns pending items with status.
    """
    state = _load_launched_offers_state()
    pending = [
        {**{"advertiser": k}, **v}
        for k, v in state.items()
        if v.get("status") == "queued"
    ]

    if not pending:
        return {"pending": [], "count": 0}

    # Batch ClickHouse check: impressions per advertiser since approved_at
    impression_counts: dict = {}
    try:
        ch = _get_ch_client()
        # Find campaign IDs matching each advertiser name, then count impressions
        # since that offer's approved_at date
        for item in pending:
            adv = item["advertiser"]
            approved_at = item.get("approved_at", "2000-01-01T00:00:00")
            try:
                rows = ch.query(
                    """
                    SELECT count() AS imp_count
                    FROM default.adpx_impressions_details imp
                    JOIN default.mv_adpx_campaigns c ON toInt64(imp.campaign_id) = toInt64(c.id)
                    WHERE c.adv_name ILIKE %(adv)s
                      AND imp.created_at >= parseDateTimeBestEffort(%(approved_at)s)
                      AND toYYYYMM(imp.created_at) >= toYYYYMM(parseDateTimeBestEffort(%(approved_at)s))
                    """,
                    parameters={"adv": f"%{adv}%", "approved_at": approved_at},
                ).result_rows
                impression_counts[adv] = rows[0][0] if rows else 0
            except Exception as e:
                log.debug(f"CH impression check failed for {adv}: {e}")
                impression_counts[adv] = 0
    except Exception as e:
        log.warning(f"get_demand_queue_status: ClickHouse unavailable: {e}")

    from datetime import datetime, timezone as _tz

    result_items = []
    for item in pending:
        adv = item["advertiser"]
        imp = impression_counts.get(adv, 0)
        approved_at_str = item.get("approved_at", "")
        days_queued = 0
        if approved_at_str:
            try:
                approved_dt = datetime.fromisoformat(approved_at_str.replace("Z", "+00:00"))
                days_queued = (datetime.now(_tz.utc) - approved_dt).days
            except Exception as e:
                log.debug("get_demand_queue_status approved_at parse swallowed: %s", e)
                days_queued = 0

        result_items.append({
            "advertiser":  adv,
            "payout":      item.get("payout", ""),
            "network":     item.get("network", ""),
            "brief_url":   item.get("thread_url", ""),
            "notion_url":  item.get("notion_url", ""),
            "approved_by": item.get("approved_by", ""),
            "approved_at": approved_at_str,
            "days_queued": days_queued,
            "status":      "likely_live" if imp > 0 else "pending",
            "impressions_since_approval": imp,
        })

    return {"pending": result_items, "count": len(result_items)}


def mark_offer_launched(advertiser: str) -> dict:
    """
    Mark an approved offer as live. Updates launched_offers.json status to 'launched'.
    scout_bot.py reads the result and sends a targeted notification to the approver + AdOps.
    """
    state = _load_launched_offers_state()

    # Fuzzy match — handles "TurboTax" matching "TurboTax 2025" etc.
    key = next(
        (k for k in state if advertiser.lower() in k.lower() or k.lower() in advertiser.lower()),
        advertiser,
    )

    entry = state.get(key, {})
    entry.update({"status": "launched", "launched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")})
    state[key] = entry

    try:
        _LAUNCHED_OFFERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _LAUNCHED_OFFERS_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2))
        os.replace(tmp, _LAUNCHED_OFFERS_PATH)
    except Exception as e:
        log.warning(f"mark_offer_launched write failed: {e}")

    return {
        "status":      "launched",
        "advertiser":  key,
        "approved_by": entry.get("approved_by"),
        "thread_url":  entry.get("thread_url"),
        "notion_url":  entry.get("notion_url", ""),
        "payout":      entry.get("payout"),
        "network":     entry.get("network"),
    }


def get_advertiser_revenue_projection(
    advertiser_name: str,
    month: str = None,
) -> dict:
    """
    Project gross revenue for an advertiser across all MS publishers for a target month.

    Uses last 30 days as the baseline (avg daily revenue × days in month).
    Checks:
      - Campaign end dates (excludes/warns on campaigns ending before month)
      - Monthly budget caps from capping_config JSON
    Returns projected totals, publisher breakdown, cap warnings, end-date warnings.
    """
    import calendar as _cal
    import json as _json
    from datetime import date

    ch = _get_ch_client()
    today = date.today()

    # ── Parse target month ────────────────────────────────────────────────────
    target_year, target_month_num = today.year, today.month + 1
    if target_month_num > 12:
        target_month_num, target_year = 1, today.year + 1

    if month:
        import re as _re
        m = _re.search(r'(\d{4})[/-](\d{1,2})', month)
        if m:
            target_year, target_month_num = int(m.group(1)), int(m.group(2))
        else:
            month_map = {n.lower(): i for i, n in enumerate(_cal.month_name) if n}
            for name, num in month_map.items():
                if name in month.lower():
                    target_month_num = num
                    yr = _re.search(r'\d{4}', month)
                    if yr:
                        target_year = int(yr.group())
                    break

    days_in_month = _cal.monthrange(target_year, target_month_num)[1]
    month_start   = date(target_year, target_month_num, 1)
    month_end     = date(target_year, target_month_num, days_in_month)
    month_label   = f"{_cal.month_name[target_month_num]} {target_year}"

    # ── Steps 1 + 2 run in parallel — both depend only on advertiser_name ───────
    # Step 1: 30-day baseline — impressions + revenue per publisher
    # Step 2: Campaign end dates + monthly caps
    def _fetch_baseline():
        return ch.query(
            """
            SELECT
                cast(i.pid AS String)          AS publisher_pid,
                any(u.organization)            AS publisher_name,
                count()                        AS impressions_30d,
                count(DISTINCT i.session_id)   AS sessions_30d,
                coalesce(sum(toFloat64OrNull(cd.revenue)), 0) AS revenue_30d,
                coalesce(sum(toFloat64OrNull(cd.payout)),  0) AS payout_30d,
                count(DISTINCT cd.id)           AS conversions_30d,
                coalesce(any(clicks_agg.clicks_30d), 0) AS clicks_30d
            FROM adpx_impressions_details i
            JOIN from_airbyte_campaigns c
                ON i.campaign_id = cast(c.id AS UInt64)
            LEFT JOIN from_airbyte_users u
                ON i.pid = toString(u.id)
            LEFT JOIN adpx_conversionsdetails cd
                ON i.session_id = cd.session_id
                AND cd.campaign_id = i.campaign_id
                AND toYYYYMM(cd.created_at) >= toYYYYMM(today() - 44)
            LEFT JOIN (
                SELECT cast(tc.user_id AS String) AS pub_id, count() AS clicks_30d
                FROM adpx_tracked_clicks tc
                INNER JOIN from_airbyte_campaigns c2 ON tc.campaign_id = cast(c2.id AS UInt64)
                WHERE c2.adv_name ILIKE %(adv)s
                  AND c2.deleted_at IS NULL
                  AND tc.created_at >= today() - 30
                GROUP BY tc.user_id
            ) clicks_agg ON clicks_agg.pub_id = cast(i.pid AS String)
            WHERE c.adv_name ILIKE %(adv)s
              AND c.deleted_at IS NULL
              AND i.created_at >= today() - 30
              AND toYYYYMM(i.created_at) >= toYYYYMM(today() - 30)
            GROUP BY publisher_pid
            ORDER BY revenue_30d DESC
            LIMIT 30
            """,
            parameters={"adv": f"%{advertiser_name}%"},
        ).result_rows

    def _fetch_cap_data():
        return ch.query(
            """
            SELECT id, adv_name, end_date, capping_config
            FROM from_airbyte_campaigns
            WHERE adv_name ILIKE %(adv)s
              AND deleted_at IS NULL
              AND (end_date IS NULL OR end_date >= %(month_start)s)
            """,
            parameters={"adv": f"%{advertiser_name}%", "month_start": str(month_start)},
        ).result_rows

    baseline_rows = []
    cap_rows = []
    try:
        baseline_rows, cap_rows = _run_parallel([_fetch_baseline, _fetch_cap_data])
    except Exception as e:
        log.warning(f"get_advertiser_revenue_projection parallel fetch failed: {e}")
        try:
            baseline_rows = _fetch_baseline()
        except Exception as e2:
            log.warning(f"get_advertiser_revenue_projection baseline failed: {e2}")
            return {"error": str(e2), "advertiser": advertiser_name, "month": month_label}
        try:
            cap_rows = _fetch_cap_data()
        except Exception as e2:
            log.warning(f"get_advertiser_revenue_projection cap query failed: {e2}")

    if not baseline_rows:
        return {
            "advertiser": advertiser_name,
            "month": month_label,
            "error": f"No impression data for '{advertiser_name}' in the last 30 days. Check spelling or try a partial name.",
        }

    # ── Process cap/end-date results ──────────────────────────────────────────
    cap_warnings       = []
    end_date_warnings  = []
    monthly_cap_total  = None

    for row in cap_rows:
        cid, adv, end_dt, cap_cfg = row
        if end_dt and end_dt < month_end:
            end_date_warnings.append(
                f"Campaign {cid} ends {end_dt} — won't run full month"
            )
        if cap_cfg:
            try:
                cfg = _json.loads(cap_cfg) if isinstance(cap_cfg, str) else cap_cfg
                mb = (cfg.get("month") or {}).get("budget")
                if mb and float(mb) > 0:
                    cap_warnings.append(
                        f"Campaign {cid}: ${float(mb):,.0f} monthly budget cap"
                    )
                    monthly_cap_total = (monthly_cap_total or 0) + float(mb)
            except Exception as e:
                log.debug("_fetch_cap_data swallowed: %s", e)

    # ── Step 3: Projection ────────────────────────────────────────────────────
    total_revenue_30d     = sum(r[4] for r in baseline_rows)
    total_payout_30d      = sum(r[5] for r in baseline_rows)
    total_impressions_30d = sum(r[2] for r in baseline_rows)
    total_sessions_30d    = sum(r[3] for r in baseline_rows)
    total_conversions_30d = sum(r[6] for r in baseline_rows)
    total_clicks_30d      = sum(r[7] for r in baseline_rows)

    uncapped_projected_revenue = round((total_revenue_30d / 30) * days_in_month, 2)
    uncapped_projected_payout  = round((total_payout_30d  / 30) * days_in_month, 2)

    cap_applied = bool(monthly_cap_total and uncapped_projected_revenue > monthly_cap_total)
    projected_revenue = monthly_cap_total if cap_applied else uncapped_projected_revenue
    projected_payout  = uncapped_projected_payout  # payout cap not modeled separately

    by_publisher = []
    for row in baseline_rows[:10]:
        pub_pid, pub_name, impr, sess, rev, pay, convs, clicks = row
        by_publisher.append({
            "publisher":          pub_name or f"Partner {pub_pid}",
            "impressions_30d":    impr,
            "revenue_30d":        round(rev, 2),
            "projected_revenue":  round((rev / 30) * days_in_month, 2),
            "conversions_30d":    convs,
            "share_pct":          round(rev / total_revenue_30d * 100, 1) if total_revenue_30d else 0,
        })

    result = {
        "advertiser":                advertiser_name,
        "month":                     month_label,
        "days_in_month":             days_in_month,
        "publisher_count":           len(baseline_rows),
        # Actuals (30-day)
        "revenue_30d":               round(total_revenue_30d, 2),
        "payout_30d":                round(total_payout_30d, 2),
        "impressions_30d":           total_impressions_30d,
        "conversions_30d":           total_conversions_30d,
        # Projections
        "projected_revenue":         round(projected_revenue, 2),
        "uncapped_projected_revenue": uncapped_projected_revenue,
        "projected_payout":          round(projected_payout, 2),
        "projected_impressions":     int((total_impressions_30d / 30) * days_in_month),
        "cap_applied":               cap_applied,
        "monthly_cap_total":         monthly_cap_total,
        # Performance metrics (used for payout impact math)
        "avg_daily_revenue":         round(total_revenue_30d / 30, 2),
        "avg_rpm":                   round(total_revenue_30d / max(total_impressions_30d, 1) * 1000, 4),
        "avg_cvr":                   round(total_conversions_30d / max(total_clicks_30d, 1) * 100, 4),
        # Breakdown + warnings
        "by_publisher":              by_publisher,
        "cap_warnings":              cap_warnings,
        "end_date_warnings":         end_date_warnings,
        "methodology":               "30-day avg daily revenue × days in month. Cap applied where monthly_cap_total < uncapped projection.",
        "data_quality":              _data_quality_tier(30, total_sessions_30d),
    }

    # Build formatted Slack output for LLM synthesis
    _lines = []
    if result.get("cap_applied"):
        _monthly_cap = result.get("monthly_cap_total", 0)
        _avg_daily = result.get("avg_daily_revenue", 0)
        _uncapped = result.get("uncapped_projected_revenue", 0)
        _delta = (_uncapped or 0) - (_monthly_cap or 0)
        _adv = result.get("advertiser", advertiser_name)
        _lines.append(
            f":red_circle: *Budget cap is the story.* {_adv} capped at "
            f"*${_monthly_cap:,.0f}*/mo — run rate *${_avg_daily:,.0f}/day* "
            f"(~${_uncapped:,.0f} uncapped). "
            f":zap: Lift cap or spin uncapped campaign to unlock ~${_delta:,.0f}."
        )
    else:
        _proj = result.get("projected_revenue", 0)
        _avg = result.get("avg_daily_revenue", 0)
        _adv = result.get("advertiser", advertiser_name)
        _m = result.get("month", month_label)
        _lines.append(
            f"{_adv} projects *${_proj:,.0f}* for {_m} at *${_avg:,.0f}/day*."
        )
    # Publisher breakdown top 5
    _breakdown = result.get("by_publisher", [])
    if _breakdown:
        _lines.append("")
        for _pub in _breakdown[:5]:
            _pname = _pub.get("publisher", "Unknown")
            _prev = _pub.get("revenue_30d", 0)
            _pshare = _pub.get("share_pct", 0)
            _lines.append(f"• {_pname}: ${_prev:,.0f} ({_pshare:.0f}%)")
    result["formatted"] = "\n".join(_lines)

    return result


def get_publisher_health(
    publisher_name: str = None,
    publisher_id: int = None,
    days: int = 14,
    geo_state: str = None,
) -> dict:
    """
    Full publisher health analysis: sessions, impressions, clicks, conversions, revenue, RPM, CTR, CVR.
    Breaks down by placement and OS. Includes click position data.
    """
    try:
        ch = _get_ch_client()

        # ── Resolve publisher name → ID ───────────────────────────────────────
        pid = publisher_id
        pub_name = None
        if pid is None and publisher_name:
            pub_results = _q.publisher_lookup_by_name(ch, publisher_name)
            if not pub_results:
                return {"error": f"No publisher found matching '{publisher_name}'"}
            # Disambiguate: pick the candidate with the most recent sessions.
            # Without this, accounts with the same name return in arbitrary order and
            # the wrong (inactive) account gets picked — e.g. TextNow 2527 vs 1952.
            if len(pub_results) > 1:
                candidate_pids = [str(r["id"]) for r in pub_results]
                vol_map = _q.publisher_impression_volume(ch, candidate_pids, days=7)
                best_pid_str = max(candidate_pids, key=lambda p: vol_map.get(p, 0))
                best = next((r for r in pub_results if str(r["id"]) == best_pid_str), pub_results[0])
            else:
                best = pub_results[0]
            pid = int(best["id"])
            pub_name = best["organization"]
        elif pid is not None:
            pub_result = _q.publisher_lookup_by_id(ch, int(pid))
            pub_name = pub_result["organization"] if pub_result else f"Partner {pid}"

        if pid is None:
            return {"error": "Must provide publisher_name or publisher_id"}

        # ── Partition filter ──────────────────────────────────────────────────
        from datetime import date
        today = date.today()
        # partition for sessions (go back days + a little buffer)
        partition = int(today.strftime("%Y%m")) - (1 if today.day <= days else 0)
        extended_partition = partition - 1  # extra month for downstream lag

        # ── Fetch all data via queries.py (sequential — CH client not thread-safe) ──
        placement_names = _q.publisher_placement_names(ch, int(pid))
        q1_data = _q.publisher_health_sessions(ch, int(pid), partition, days, geo_state)
        q2_data = _q.publisher_health_ad_metrics(ch, int(pid), str(pid), partition, extended_partition, days, geo_state)
        q3_data = _q.publisher_health_click_metrics(ch, int(pid), partition, days, geo_state)

        # ── Combine results ───────────────────────────────────────────────────
        # Build placement-keyed dicts
        sess_by_placement: dict = {}
        os_by_placement: dict = {}
        for row in q1_data:
            p = row["placement"] or "unknown"
            sess_by_placement[p] = sess_by_placement.get(p, 0) + row["sessions"]
            os_by_placement.setdefault(p, {})
            os_val = row["os"] or "unknown"
            os_by_placement[p][os_val] = os_by_placement[p].get(os_val, 0) + row["sessions"]

        ad_metrics: dict = {}
        for row in q2_data:
            p = row["placement"] or "unknown"
            ad_metrics[p] = {
                "impressions": row["impressions"],
                "conversions": row["conversions"],
                "revenue":     row["revenue"],
                "payout":      row["payout"],
            }

        click_metrics: dict = {}
        for row in q3_data:
            p = row["placement"] or "unknown"
            click_metrics[p] = {
                "clicks":           row["clicks"],
                "converted_clicks": row["converted_clicks"],
                "avg_position":     row["avg_position"],
            }

        # Aggregate OS split across all placements
        os_totals: dict = {}
        for p_os in os_by_placement.values():
            for os_val, cnt in p_os.items():
                os_totals[os_val] = os_totals.get(os_val, 0) + cnt
        total_sess_all = sum(os_totals.values()) or 1
        os_split = sorted(
            [{"os": k, "sessions": v, "share_pct": round(v / total_sess_all * 100, 1)} for k, v in os_totals.items()],
            key=lambda x: -x["sessions"],
        )

        # Build per-placement breakdown
        all_placements = set(sess_by_placement.keys()) | set(ad_metrics.keys()) | set(click_metrics.keys())
        total_revenue = sum(ad_metrics.get(p, {}).get("revenue", 0) for p in all_placements)
        total_impressions = sum(ad_metrics.get(p, {}).get("impressions", 0) for p in all_placements)
        publisher_avg_rpm = (total_revenue / total_impressions * 1000) if total_impressions else 0

        by_placement = []
        for p in all_placements:
            sess = sess_by_placement.get(p, 0)
            ad = ad_metrics.get(p, {"impressions": 0, "conversions": 0, "revenue": 0.0, "payout": 0.0})
            cl = click_metrics.get(p, {"clicks": 0, "converted_clicks": 0, "avg_position": 0.0})
            impr = ad["impressions"]
            rev = ad["revenue"]
            rpm = round(rev / impr * 1000, 2) if impr else 0.0
            ctr = round(cl["clicks"] / impr * 100, 2) if impr else 0.0
            cvr = round(ad["conversions"] / cl["clicks"] * 100, 2) if cl["clicks"] else 0.0

            anomaly = None
            if publisher_avg_rpm > 0 and rpm > 0 and rpm > publisher_avg_rpm * 5:
                ratio = round(rpm / publisher_avg_rpm, 1)
                anomaly = f"{ratio}x higher RPM than publisher avg"

            display_name = placement_names.get(p, p)
            by_placement.append({
                "placement":       display_name,
                "placement_slug":  p,
                "sessions":        sess,
                "impressions":     impr,
                "clicks":          cl["clicks"],
                "conversions":     ad["conversions"],
                "revenue":         round(rev, 2),
                "rpm":             rpm,
                "ctr_pct":         ctr,
                "cvr_pct":         cvr,
                "avg_position":    cl["avg_position"],
                "anomaly":         anomaly,
            })

        by_placement.sort(key=lambda x: -x["revenue"])

        # Overall rollup
        total_sessions = sum(sess_by_placement.values())
        total_clicks = sum(cl["clicks"] for cl in click_metrics.values())
        total_conversions = sum(ad_metrics.get(p, {}).get("conversions", 0) for p in all_placements)
        total_payout = sum(ad_metrics.get(p, {}).get("payout", 0) for p in all_placements)
        all_positions = [cl["avg_position"] for cl in click_metrics.values() if cl["avg_position"] > 0]
        avg_click_position = round(sum(all_positions) / len(all_positions), 1) if all_positions else 0.0

        overall_rpm = round(total_revenue / total_impressions * 1000, 2) if total_impressions else 0.0
        overall_ctr = round(total_clicks / total_impressions * 100, 2) if total_impressions else 0.0
        overall_cvr = round(total_conversions / total_clicks * 100, 2) if total_clicks else 0.0

        # Top placement note
        top_placement_note = ""
        if len(by_placement) >= 2:
            top = by_placement[0]
            bottom = by_placement[-1]
            if bottom["rpm"] > 0 and top["rpm"] > 0:
                ratio = round(top["rpm"] / bottom["rpm"], 1)
                top_placement_note = (
                    f"{top['placement']} generates {ratio}x RPM of {bottom['placement']}"
                )

        return {
            "publisher":     pub_name or f"Partner {pid}",
            "days":          days,
            "geo_state":     geo_state or None,
            "overall": {
                "sessions":           total_sessions,
                "impressions":        total_impressions,
                "clicks":             total_clicks,
                "conversions":        total_conversions,
                "revenue":            round(total_revenue, 2),
                "payout":             round(total_payout, 2),
                "rpm":                overall_rpm,
                "ctr_pct":            overall_ctr,
                "cvr_pct":            overall_cvr,
                "avg_click_position": avg_click_position,
            },
            "by_placement":          by_placement,
            "os_split":              os_split,
            "top_placements_note":   top_placement_note,
            "data_quality":          _data_quality_tier(days, total_sessions),
        }
    except Exception as e:
        log.exception("get_publisher_health failed")
        return {"error": str(e), "publisher_name": publisher_name, "publisher_id": publisher_id}


def get_campaign_status(advertiser_name: str) -> dict:
    """
    Check if an advertiser's campaigns are active/paused and show recent changes from audit log.
    """
    try:
        import json as _json
        ch = _get_ch_client()

        # ── Queries 1 + 2 run in parallel — both depend only on advertiser_name ─
        # Query 1: current status from publisher_campaigns
        # Query 2: recent audit log entries
        def _fetch_q1():
            return ch.query(
                """
                SELECT
                    pc.id, pc.campaign_id, pc.is_active,
                    c.adv_name,
                    pc.updated_at, pc.deleted_at
                FROM from_airbyte_publisher_campaigns pc
                JOIN from_airbyte_campaigns c ON toInt64(pc.campaign_id) = c.id
                WHERE c.adv_name ILIKE {adv: String}
                  AND c.deleted_at IS NULL
                ORDER BY pc.updated_at DESC
                LIMIT 50
                """,
                parameters={"adv": f"%{advertiser_name}%"},
            ).result_rows

        def _fetch_q2():
            return ch.query(
                """
                SELECT
                    entity, type, old_data, new_data, created_at, user_type, user_role
                FROM adpx_system_activity_logs
                WHERE (lower(new_data) LIKE lower({adv_pat: String}) OR lower(old_data) LIKE lower({adv_pat: String}))
                  AND created_at >= today() - 30
                ORDER BY created_at DESC
                LIMIT 20
                """,
                parameters={"adv_pat": f"%{advertiser_name}%"},
            ).result_rows

        q1_rows = []
        q2_rows = []
        try:
            q1_rows, q2_rows = _run_parallel([_fetch_q1, _fetch_q2])
        except Exception as e:
            log.warning(f"get_campaign_status parallel fetch failed: {e}")
            try:
                q1_rows = _fetch_q1()
            except Exception as e2:
                log.warning(f"get_campaign_status q1 failed: {e2}")
            try:
                q2_rows = _fetch_q2()
            except Exception as e2:
                log.warning(f"get_campaign_status q2 failed: {e2}")

        # ── Build campaigns list ──────────────────────────────────────────────
        campaigns = []
        for row in q1_rows:
            pc_id, camp_id, is_active, adv_name, updated_at, deleted_at = row
            campaigns.append({
                "publisher_campaign_id": int(pc_id) if pc_id else None,
                "campaign_id":           int(camp_id) if camp_id else None,
                "adv_name":              adv_name or advertiser_name,
                "is_active":             bool(is_active),
                "last_updated":          str(updated_at) if updated_at else None,
                "is_deleted":            deleted_at is not None,
            })

        active_count = sum(1 for c in campaigns if c["is_active"] and not c["is_deleted"])
        paused_count = sum(1 for c in campaigns if not c["is_active"] and not c["is_deleted"])

        # ── Parse audit log ───────────────────────────────────────────────────
        recent_changes = []
        for row in q2_rows:
            entity, change_type_raw, old_data_str, new_data_str, created_at, user_type, user_role = row
            try:
                old_data = _json.loads(old_data_str) if old_data_str else {}
            except Exception as e:
                log.debug("get_campaign_status old_data parse swallowed: %s", e)
                old_data = {}
            try:
                new_data = _json.loads(new_data_str) if new_data_str else {}
            except Exception as e:
                log.debug("get_campaign_status new_data parse swallowed: %s", e)
                new_data = {}

            # Determine change type
            old_active = old_data.get("is_active")
            new_active = new_data.get("is_active")
            if old_active is True and new_active is False:
                c_type = "paused"
                summary = f"Campaign paused by {user_role or user_type or 'system'}"
            elif old_active is False and new_active is True:
                c_type = "resumed"
                summary = f"Campaign resumed by {user_role or user_type or 'system'}"
            elif "capping_config" in new_data or "capping_config" in old_data:
                c_type = "budget_changed"
                summary = "Budget/cap configuration changed"
            else:
                c_type = "other"
                summary = f"{change_type_raw or 'Change'} by {user_role or user_type or 'system'}"

            recent_changes.append({
                "entity":      entity or "",
                "change_type": c_type,
                "timestamp":   str(created_at) if created_at else None,
                "summary":     summary,
            })

        # ── Status summary ────────────────────────────────────────────────────
        last_change_str = ""
        if recent_changes:
            last = recent_changes[0]
            last_change_str = f" Last change: {last['change_type']} ({last['timestamp'][:10] if last['timestamp'] else 'unknown'})."
        status_summary = f"{active_count} active, {paused_count} paused.{last_change_str}"

        return {
            "advertiser":     advertiser_name,
            "campaign_count": len(campaigns),
            "active_count":   active_count,
            "paused_count":   paused_count,
            "campaigns":      campaigns,
            "recent_changes": recent_changes,
            "status_summary": status_summary,
        }
    except Exception as e:
        log.exception("get_campaign_status failed")
        return {"error": str(e), "advertiser": advertiser_name}


def get_perkswall_engagement(
    publisher_name: str = None,
    publisher_id: int = None,
    days: int = 30,
) -> dict:
    """
    Perkswall offer selection analytics — which offers do loyalty members actually pick?
    """
    try:
        ch = _get_ch_client()

        # ── Resolve publisher name → ID ───────────────────────────────────────
        pid = publisher_id
        pub_name = None
        if pid is None and publisher_name:
            pub_results = _q.publisher_lookup_by_name(ch, publisher_name)
            if not pub_results:
                return {"error": f"No publisher found matching '{publisher_name}'"}
            # Disambiguate: pick the candidate with the most recent sessions.
            if len(pub_results) > 1:
                candidate_pids = [str(r["id"]) for r in pub_results]
                vol_map = _q.publisher_impression_volume(ch, candidate_pids, days=7)
                best_pid_str = max(candidate_pids, key=lambda p: vol_map.get(p, 0))
                best = next((r for r in pub_results if str(r["id"]) == best_pid_str), pub_results[0])
            else:
                best = pub_results[0]
            pid = int(best["id"])
            pub_name = best["organization"]
        elif pid is not None:
            pub_result = _q.publisher_lookup_by_id(ch, int(pid))
            pub_name = pub_result["organization"] if pub_result else f"Partner {pid}"

        if pid is None:
            return {"error": "Must provide publisher_name or publisher_id"}

        # ── Partition filter ──────────────────────────────────────────────────
        from datetime import date
        today = date.today()
        partition = int(today.strftime("%Y%m")) - (1 if today.day <= days else 0)

        # ── Query: perk selections by offer ───────────────────────────────────
        sel_rows = ch.query(
            """
            SELECT
                sp.campaign_id,
                any(c.adv_name)                        AS offer_name,
                count()                                AS selections,
                count(DISTINCT sp.pub_user_id)         AS unique_members,
                count(DISTINCT sp.session_id)          AS sessions_with_selection
            FROM from_airbyte_user_selected_perks sp
            JOIN from_airbyte_campaigns c ON toInt64(sp.campaign_id) = c.id
            WHERE sp.user_id = {pid: UInt64}
              AND sp.created_at >= today() - {days: UInt32}
            GROUP BY sp.campaign_id
            ORDER BY selections DESC
            LIMIT 20
            """,
            parameters={"pid": int(pid), "days": days},
        ).result_rows

        # ── Total sessions for selection rate ─────────────────────────────────
        sess_row = ch.query(
            """
            SELECT count() AS total_sessions, count(DISTINCT session_id) AS unique_sessions
            FROM adpx_sdk_sessions
            PREWHERE user_id = {pid: UInt64}
                AND toYYYYMM(created_at) >= {partition: UInt32}
            WHERE created_at >= today() - {days: UInt32}
            """,
            parameters={"pid": int(pid), "partition": partition, "days": days},
        ).result_rows
        total_sessions = int(sess_row[0][0]) if sess_row else 0
        total_sessions_safe = total_sessions or 1

        # ── Build offer breakdown ─────────────────────────────────────────────
        by_offer = []
        total_selections = 0
        all_unique_members = set()
        for campaign_id, offer_name, selections, unique_members, sessions_with_selection in sel_rows:
            total_selections += int(selections)
            by_offer.append({
                "offer":                    offer_name or f"Campaign {campaign_id}",
                "campaign_id":              int(campaign_id) if campaign_id else None,
                "selections":               int(selections),
                "unique_members":           int(unique_members),
                "sessions_with_selection":  int(sessions_with_selection),
                "selection_rate_pct":       round(int(selections) / total_sessions_safe * 100, 2),
            })

        total_unique_members = sum(o["unique_members"] for o in by_offer)
        selection_rate_pct = round(total_selections / total_sessions_safe * 100, 2)

        # ── Insight ───────────────────────────────────────────────────────────
        insight = ""
        if by_offer:
            top = by_offer[0]
            insight = (
                f"{top['offer']} selected by {top['unique_members']:,} unique members "
                f"({top['selection_rate_pct']}% of sessions)"
            )

        return {
            "publisher":               pub_name or f"Partner {pid}",
            "publisher_id":            int(pid),
            "days":                    days,
            "total_sessions":          total_sessions,
            "total_selections":        total_selections,
            "selection_rate_pct":      selection_rate_pct,
            "unique_members_engaged":  total_unique_members,
            "by_offer":                by_offer,
            "insight":                 insight,
        }
    except Exception as e:
        log.exception("get_perkswall_engagement failed")
        return {"error": str(e), "publisher_name": publisher_name, "publisher_id": publisher_id}


_LARGE_TABLES = (
    "adpx_sdk_sessions",
    "adpx_impressions_details",
    "adpx_tracked_clicks",
    "adpx_conversionsdetails",
)


def _validate_sql_query(sql: str) -> list:
    """
    Lightweight safety validator for run_sql_query.

    Returns a list of warning strings (empty = clean). NEVER blocks execution —
    callers should log the warnings but proceed. Heuristics:
      - References a large table without PREWHERE → warn
      - References a large table without any created_at filter → warn
      - Lacks any LIMIT clause → warn
    """
    warnings: list = []
    if not sql or not isinstance(sql, str):
        return warnings

    sql_upper = sql.upper()
    has_prewhere = bool(re.search(r"\bPREWHERE\b", sql_upper))
    # Only treat created_at as a date filter when it appears in a PREWHERE/WHERE
    # clause — references in SELECT columns or ORDER BY don't filter rows.
    has_created_at = bool(
        re.search(r"\b(?:PREWHERE|WHERE)\b[\s\S]*?\bCREATED_AT\b", sql_upper)
    )
    has_limit = bool(re.search(r"\bLIMIT\b", sql_upper))

    for table in _LARGE_TABLES:
        if re.search(r"\b" + re.escape(table) + r"\b", sql, re.IGNORECASE):
            if not has_prewhere:
                warnings.append(f"missing PREWHERE on large table: {table}")
            if not has_created_at:
                warnings.append(f"no date filter on large table: {table}")

    if not has_limit:
        warnings.append("no LIMIT clause")

    return warnings


def run_sql_query(sql: str, description: str = "", max_rows: int = 500) -> dict:
    """
    Execute an arbitrary SELECT query against ClickHouse.
    Safety: SELECT-only, 500 row default max, 30s timeout.
    Returns structured results for Claude to format.
    """
    import re as _re

    # Safety guard — SELECT only
    sql_stripped = sql.strip()
    first_word = sql_stripped.split()[0].upper() if sql_stripped else ""
    if first_word not in ("SELECT", "WITH"):
        return {
            "error": "Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, etc.",
            "sql": sql_stripped,
        }

    # Safety gate — log warnings for queries hitting large tables without filters.
    # Logs only — does NOT block execution.
    for _warning in _validate_sql_query(sql_stripped):
        log.warning("sql_query safety: %s", _warning)

    # Inject LIMIT if not present
    sql_upper = sql_stripped.upper()
    has_limit = bool(_re.search(r'\bLIMIT\b', sql_upper))
    if not has_limit:
        sql_stripped = sql_stripped.rstrip(";") + f"\nLIMIT {max_rows}"

    try:
        ch = _get_ch_client()
        result = ch.query(sql_stripped, settings={"max_execution_time": 30})
        rows = result.result_rows
        try:
            col_names = list(result.column_names)
        except (AttributeError, TypeError):
            col_names = []

        truncated = len(rows) >= max_rows

        # Sanitize values — ClickHouse returns date/datetime as Python objects
        def _sanitize(v):
            if isinstance(v, (_dt_mod.date, _dt_mod.datetime)):
                return str(v)
            if isinstance(v, (list, tuple)):
                return [_sanitize(x) for x in v]
            return v

        # Convert rows to list of dicts for readability
        if col_names:
            rows_as_dicts = [
                {k: _sanitize(v) for k, v in zip(col_names, row)}
                for row in rows[:max_rows]
            ]
        else:
            rows_as_dicts = [[_sanitize(v) for v in row] for row in rows[:max_rows]]

        # Strip internal ID columns — never surface user_id, publisher_id, campaign_id, etc. to LLM
        import re as _re_id
        _ID_SUFFIX = _re_id.compile(r'(?:^|_)id$', _re_id.IGNORECASE)
        if col_names:
            _keep = [c for c in col_names if not _ID_SUFFIX.search(c)]
            rows_as_dicts = [{k: v for k, v in row.items() if not _ID_SUFFIX.search(k)} for row in rows_as_dicts]
            col_names = _keep

        return {
            "description": description,
            "sql_run": sql_stripped,
            "row_count": len(rows_as_dicts),
            "truncated": truncated,
            "truncation_note": f"Results limited to {max_rows} rows. Add LIMIT to your query to control this." if truncated else None,
            "columns": col_names,
            "rows": rows_as_dicts,
            "data_quality": {
                "tier": "free_form",
                "note": f"Live query — {len(rows_as_dicts)} rows.",
            },
        }
    except Exception as e:
        err = str(e)
        return {
            "error": err,
            "sql_run": sql_stripped,
            "description": description,
            "hint": "Check table/column names against the DATA DICTIONARY. Common issues: wrong join type (pid vs user_id), missing PREWHERE, type mismatch (toFloat64OrNull for revenue).",
        }


def get_ghost_campaigns() -> str:
    """
    Return full ghost campaign list with per-campaign diagnosis.
    Ghost = actively serving impressions + clicks but generating near-zero revenue
    (< $5 in last 7 days), older than 7 days (not a new launch).

    NOTE: SQL lives in _query_ghost_campaigns(). Any threshold or filter change belongs there.
    This function is a formatting wrapper only.
    """
    ch = _get_ch_client()
    try:
        rows = _query_ghost_campaigns(ch)
    except Exception as e:
        return f"Ghost campaign query failed: {e}"

    if not rows:
        return (
            "*Ghost Campaign Report*\n\n"
            ":white_check_mark: No ghost campaigns detected — all active campaigns with "
            "high engagement are generating revenue."
        )

    lines = [f"*Ghost Campaign Report — Full List* ({len(rows)} campaigns)\n"]
    for r in rows:
        adv_name    = r["adv_name"]
        campaign_id = r["campaign_id"]
        imps        = r["impressions_7d"]
        imps_2d     = r["impressions_2d"]
        clicks      = r["clicks_7d"]
        rev         = r["revenue_7d"]
        first_date  = r["first_impression_date"]
        pub_ids     = r["publisher_ids"]
        pub_names   = r["publisher_names"]

        imp_str    = f"{imps / 1000:.0f}K" if imps >= 1000 else str(imps)
        imp_2d_str = f"{imps_2d / 1000:.1f}K" if imps_2d >= 1000 else str(imps_2d)
        rev_str    = f"${rev:.2f}" if rev > 0 else "$0"

        # Publisher context — deduplicate and format as "Name (#ID)"
        seen, pub_parts = set(), []
        for pid, pname in zip(pub_ids, pub_names):
            if pid not in seen and pname:
                seen.add(pid)
                pub_parts.append(f"{pname} (#{pid})")
        pub_str = ", ".join(pub_parts[:3]) if pub_parts else "unknown publisher"

        if clicks > 0 and rev == 0:
            hypothesis = "Zero conversions in 7 days — postback not firing post-click. Check postback URL config."
        else:
            hypothesis = "Clicks converting but revenue not flowing — check campaign payout config."

        lines.append(
            f"• *{adv_name}* · Campaign #{campaign_id} · {pub_str}\n"
            f"  {imp_str} impressions (7d) · {imp_2d_str} in last 48h · {clicks:,} clicks · {rev_str} · since {first_date}\n"
            f"  ↳ _{hypothesis}_"
        )

    lines.append(
        "\n:zap: Start with the highest-impression campaigns — they're burning the most inventory. "
        "Pull the postback URL for each campaign from the network dashboard and confirm pixel fires."
    )
    return "\n".join(lines)


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


def get_low_fill_publishers() -> str:
    """
    Return publishers with fill rate < 15% over last 7 days (canonical: 7d window, 2,500 min sessions).
    Fill rate = % of sessions that received at least one offer impression.
    Low fill on a checkout/receipt page = burned traffic with no monetization attempt.

    Uses fill_rate_publishers() canonical query (7-day window, minimum 2,500 sessions/7d).
    Previously used low_fill_publishers() (30-day window, 10,000 minimum sessions) — deprecated.
    """
    ch = _get_ch_client()
    try:
        # Canonical fill rate query: 7d window, 2500 min sessions, <15% threshold, entity overrides.
        # Any threshold change belongs in config/scout_thresholds.json (fill_rate_min_sessions_7d).
        data = _q.fill_rate_publishers(ch, limit=15)
    except Exception as e:
        return f"Fill rate query failed: {e}"

    if not data:
        return (
            "*Publisher Fill Rate Report*\n\n"
            ":white_check_mark: All publishers are filling at ≥15% over the last 7 days — "
            "no low-fill anomalies detected."
        )

    total_missed = sum(int(d["missed_sessions"]) for d in data)

    lines = [
        "*Publisher Fill Rate Report — Low Fill on Post-Transaction Pages*\n",
        f"{len(data)} publisher{'s' if len(data) != 1 else ''} below 15% fill · "
        f"{total_missed / 1_000_000:.1f}M missed sessions/7d\n",
    ]

    for d in data:
        pub_id   = d["publisher_id"]
        pub_name = d["publisher_name"]
        sessions = d["sessions_7d"]
        fill_pct = d["fill_rate_pct"]
        missed   = d["missed_sessions"]

        sessions_str = f"{int(sessions) / 1_000_000:.1f}M" if sessions >= 1_000_000 else f"{int(sessions) / 1000:.0f}K"
        missed_str   = f"{int(missed) / 1_000_000:.1f}M" if missed >= 1_000_000 else f"{int(missed) / 1000:.0f}K"

        if fill_pct < 2:
            hypothesis = "Near-zero fill — SDK likely not showing offers at all. Check SDK integration, geo/OS targeting config, or advertiser supply."
        elif fill_pct < 10:
            hypothesis = "Very low fill — most sessions get no offer. Check advertiser targeting restrictions (geo/OS/device), cap exhaustion, or SDK render failure."
        else:
            hypothesis = "Below-normal fill — some sessions served, but majority missed. May be geo/device targeting mismatch or insufficient advertiser supply."

        lines.append(
            f"• *{pub_name or f'Pub #{pub_id}'}* · Pub #{pub_id}\n"
            f"  {sessions_str} sessions/7d · {fill_pct:.1f}% fill · {missed_str} sessions missed\n"
            f"  ↳ _{hypothesis}_"
        )

    lines.append(
        "\n:zap: Start with the highest missed-session publishers — they represent the largest "
        "uncaptured monetization surface. Check SDK integration logs and advertiser targeting config."
    )
    return "\n".join(lines)


def get_top_revenue_opportunities() -> str:
    """
    Return the top cross-publisher revenue gap opportunities.
    Finds high-performing advertisers (2+ publishers, >$10K/30d) not active
    in high-volume publishers (>100K sessions/30d). Ranked by estimated monthly revenue.
    """
    ch = _get_ch_client()
    try:
        data = _q.revenue_opportunities(ch)
    except Exception as e:
        return f"Revenue opportunities query failed: {e}"

    if not data:
        return (
            "*Revenue Opportunity Report*\n\n"
            ":white_check_mark: No obvious cross-publisher gaps detected — "
            "all high-performing advertisers appear active in major publishers."
        )

    total_est = sum(row["est_monthly_rev"] for row in data)
    lines = [
        f"*Revenue Opportunity Report — Cross-Publisher Gaps* ({len(data)} opportunities)\n",
        f"Total estimated monthly revenue at risk: *${total_est / 1000:.0f}K/mo*\n",
        "_Advertisers already earning on 2+ publishers — the revenue pattern is proven, "
        "the distribution gap is the opportunity._\n",
    ]

    for row in data:
        pub_name  = row["publisher_name"]
        pub_id    = row["publisher_id"]
        adv_name  = row["adv_name"]
        adv_rev   = row["adv_total_rev_30d"]
        est_rev   = row["est_monthly_rev"]
        pub_count = row["adv_pub_count"]
        sessions  = row["sessions_30d"]
        sessions_str = f"{int(sessions) / 1_000_000:.1f}M" if sessions >= 1_000_000 else f"{int(sessions) / 1000:.0f}K"
        adv_rev_str  = f"${float(adv_rev) / 1000:.0f}K" if float(adv_rev) >= 1000 else f"${float(adv_rev):.0f}"
        est_rev_str  = f"${float(est_rev) / 1000:.0f}K" if float(est_rev) >= 1000 else f"${float(est_rev):.0f}"
        lines.append(
            f"• Add *{adv_name}* → *{pub_name or f'Pub #{pub_id}'}*\n"
            f"  {adv_name} earns {adv_rev_str}/30d across {pub_count} publishers · est. *{est_rev_str}/mo* if added\n"
            f"  {pub_name}: {sessions_str} sessions/30d · not currently running this advertiser"
        )

    lines.append(
        "\n:zap: Prioritize by session volume × avg revenue. "
        "Confirm no geo/OS exclusions exist before requesting provisioning."
    )
    return "\n".join(lines)


def get_scout_status() -> dict:
    """
    System health snapshot: benchmark freshness, offer inventory, queue depth,
    ClickHouse connectivity, and data quality warnings.
    """
    import time as _time
    from datetime import datetime, timezone

    status: dict = {}

    # PR 19a: self-heal — if benchmarks haven't loaded in this process yet OR
    # are stale, trigger a reload BEFORE reporting status. _get_benchmarks()
    # respects its TTL and will only hit CH if needed. This means status check
    # never reports "not loaded" except in real CH outage scenarios.
    if not _BENCHMARKS_LOADED_AT or (_time.time() - _BENCHMARKS_LOADED_AT) > _BENCHMARKS_TTL:
        try:
            _get_benchmarks()  # populates _BENCHMARKS + _BENCHMARKS_LOADED_AT
        except Exception as e:
            log.warning(f"[status] benchmark self-heal failed: {e}")

    # Benchmark freshness (post self-heal attempt)
    age_secs = _time.time() - _BENCHMARKS_LOADED_AT if _BENCHMARKS_LOADED_AT else None
    if age_secs is None:
        # Self-heal failed → real ClickHouse problem (heartbeat already alerts)
        status["benchmarks"] = "load failed (ClickHouse issue — heartbeat will alert)"
    elif age_secs < 120:
        status["benchmarks"] = f"{int(age_secs)}s ago"
    elif age_secs < 3600:
        status["benchmarks"] = f"{int(age_secs / 60)}m ago"
    else:
        status["benchmarks"] = f"{age_secs / 3600:.1f}h ago"

    # Benchmark coverage
    bench = _BENCHMARKS or {}
    status["benchmark_coverage"] = {
        "by_offer":    len(bench.get("by_offer_impact_id", {})),
        "by_advertiser": len(bench.get("by_adv_name", {})),
        "by_category_payout": len(bench.get("by_category_payout", {})),
        "by_payout_type": len(bench.get("by_payout_type", {})),
    }

    # Offer inventory
    offers = _load_offers()
    status["offer_inventory"] = len(offers)
    if offers:
        advertisers = {o.get("advertiser") for o in offers}
        status["unique_advertisers"] = len(advertisers)
        networks = {}
        for o in offers:
            n = (o.get("network") or "unknown").lower()
            networks[n] = networks.get(n, 0) + 1
        status["by_network"] = networks
        status["available_networks"] = sorted(networks.keys())

    # Offer file age — how long ago the last successful scrape wrote the snapshot
    if SNAPSHOT_PATH.exists():
        _age_secs = _time.time() - SNAPSHOT_PATH.stat().st_mtime
        if _age_secs < 3600:
            status["offers_age"] = f"{int(_age_secs / 60)}m ago"
        elif _age_secs < 86400:
            status["offers_age"] = f"{_age_secs / 3600:.1f}h ago"
        else:
            status["offers_age"] = f"{_age_secs / 86400:.1f}d ago — consider refreshing"
    else:
        status["offers_age"] = "no snapshot — run @Scout refresh offers"

    # Unconfigured networks (creds absent → scraper silently skips them)
    import os as _os
    _missing_nets = []
    if not _os.getenv("RAKUTEN_API_TOKEN"):
        _missing_nets.append("rakuten")
    if not (_os.getenv("AWIN_PUBLISHER_ID") and _os.getenv("AWIN_API_KEY")):
        _missing_nets.append("awin")
    if _missing_nets:
        status["unconfigured_networks"] = _missing_nets
        warnings = status.get("warnings", [])
        warnings.append(
            f"Creds missing for: {', '.join(_missing_nets)} — inventory excludes these networks"
        )
        status["warnings"] = warnings

    # Demand queue
    state = _load_launched_offers_state()
    queued    = [k for k, v in state.items() if v.get("status") == "queued"]
    launched  = [k for k, v in state.items() if v.get("status") == "launched"]
    status["queue_depth"] = len(queued)
    status["launched_count"] = len(launched)
    if queued:
        status["queue_items"] = queued

    # ClickHouse connectivity
    try:
        ch = _get_ch_client()
        rows = ch.query("SELECT 1").result_rows
        status["clickhouse"] = "ok" if rows else "degraded"
    except Exception as e:
        status["clickhouse"] = f"unavailable: {str(e)[:80]}"

    # Data quality warnings — extend rather than overwrite so earlier warnings survive
    warnings = list(status.get("warnings", []))
    if bench and not bench.get("by_offer_impact_id"):
        warnings.append("No Tier 1 (exact offer) benchmarks — all scoring from Tier 2+")
    cats_null = sum(1 for o in offers if not o.get("category"))
    if cats_null > 0:
        pct = cats_null / max(len(offers), 1) * 100
        warnings.append(f"{cats_null} offers ({pct:.0f}%) have no category — Tier 3 scoring disabled for these")
    if warnings:
        status["warnings"] = warnings

    status["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        import scout_digest as _sd
        status["digest_env"] = _sd._SCOUT_ENV
        if _sd._SCOUT_ENV != "production":
            status["digest_routing"] = f"#scout-qa (SCOUT_ENV={_sd._SCOUT_ENV!r} — not production)"
        else:
            status["digest_routing"] = f"#scout-offers ({_sd._digest_channel()})"
    except Exception as _e:
        log.warning(f"[status] scout_digest unavailable: {_e}")
        status["digest_env"]     = "unavailable"
        status["digest_routing"] = "unavailable"

    return status


def run_offer_scraper() -> str:
    """
    Trigger an immediate offer inventory refresh from affiliate networks
    (see SUPPORTED_NETWORKS at top of scout_agent.py for the canonical list).
    Run when offer inventory is empty or stale.
    Takes ~2 minutes. Writes data/offers_latest.json and posts digest.
    """
    import scout_bot as _sb
    _running = getattr(_sb, "_SCRAPER_RUNNING", None)
    if _running is not None and _running.is_set():
        return (
            ":hourglass_flowing_sand: Scraper is already running. "
            "Check back in a few minutes — offer inventory will be fresh when it completes."
        )
    try:
        from offer_scraper import run_headless
        log.info("[scraper] on-demand refresh triggered via @Scout")
        run_headless()
        # Report results
        if SNAPSHOT_PATH.exists():
            import json as _json
            from collections import Counter
            offers = _json.loads(SNAPSHOT_PATH.read_text())
            active = sum(1 for o in offers if o.get("status") == "Active")
            networks = Counter((o.get("network") or "unknown").lower() for o in offers)
            net_str = "  ·  ".join(f"{k}: {v}" for k, v in sorted(networks.items(), key=lambda x: -x[1]))

            try:
                import scout_digest as _sd
                if _sd._SCOUT_ENV != "production":
                    digest_note = f"\n:warning: Digest routing to #scout-qa (SCOUT_ENV={_sd._SCOUT_ENV!r} — set SCOUT_ENV=production on Render)"
                else:
                    digest_note = f"\n:newspaper: Digest posts to #scout-offers if new offers detected."
            except Exception as _e:
                log.warning(f"[scraper] scout_digest unavailable for digest routing note: {_e}")
                digest_note = ""

            return (
                f":white_check_mark: Offer inventory refreshed — {len(offers)} total offers, "
                f"{active} active.\n{net_str}{digest_note}"
            )
        return (
            ":white_check_mark: Scraper ran. No offers_latest.json found — "
            "check Render logs for network errors."
        )
    except Exception as e:
        log.error(f"[scraper] on-demand run failed: {e}", exc_info=True)
        return f":x: Scraper failed: {e}. Check Render logs for details."


def get_pipeline_health() -> str:
    """
    Report on the Scout offer approval pipeline: how many offers are approved,
    how many are stale (>7 days without a Live/Done status), and the oldest pending.
    Reads from the Notion Scout Demand Queue database.
    """
    import os, requests as _req, json as _json
    from datetime import datetime, timezone, timedelta

    notion_token = os.getenv("NOTION_TOKEN")
    db_id = os.getenv("NOTION_QUEUE_DB_ID")
    if not notion_token or not db_id:
        return (":warning: Pipeline health unavailable — `NOTION_QUEUE_DB_ID` not configured. "
                "Add it to Render env vars.")

    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    resp = _req.post(
        f"https://api.notion.com/v1/databases/{db_id}/query",
        headers=headers, json={"page_size": 100}
    )
    if not resp.ok:
        return f":x: Notion query failed: {resp.status_code}"

    pages = resp.json().get("results", [])
    now = datetime.now(timezone.utc)
    stale = []

    for page in pages:
        props = page.get("properties", {})
        status_prop = props.get("Status", {})
        status_val = ""
        if status_prop.get("type") == "select" and status_prop.get("select"):
            status_val = status_prop["select"].get("name", "")
        if status_val.lower() in ("live", "done", "launched"):
            continue
        created = page.get("created_time", "")
        if not created:
            continue
        age = now - datetime.fromisoformat(created.replace("Z", "+00:00"))
        if age > timedelta(days=7):
            adv = ""
            for key in ("Offer", "Name", "title"):
                tp = props.get(key, {})
                if tp.get("type") == "title":
                    items = tp.get("title", [])
                    adv = items[0]["plain_text"] if items else ""
                    break
            stale.append((adv or "Unknown", age.days))

    stale.sort(key=lambda x: x[1], reverse=True)
    total = len(pages)
    lines = [f"*Scout Offer Pipeline* — *{total}* offers approved total"]
    if not stale:
        lines.append(":white_check_mark: Pipeline clear — no offers stale beyond 7 days.")
        lines.append("\n:zap: *Action:* Pipeline looks healthy. Consider adding Commission Junction to expand offer supply.")
    else:
        lines.append(f":warning: *{len(stale)} offers pending >7 days* without a Live status.")
        for adv, age_days in stale[:5]:
            lines.append(f"• {adv} — *{age_days} days* without Live status")
        if len(stale) > 5:
            lines.append(f"• ...and {len(stale)-5} more")
        lines.append(f"\n:zap: *Action:* Mark offers Live in Notion once entered in MS platform, or ping Gordon for a status update.")
    return "\n".join(lines)


def get_usage_report(requesting_user_id: str = "") -> str:
    """
    Return Scout usage statistics. Admin-only (SCOUT_ADMIN_USER_ID env var).
    Shows: queries per period, top users, most-used tools, avg response time.
    """
    import os, pathlib, json as _json
    from collections import Counter
    from datetime import datetime, timezone, timedelta

    admin_uid = os.getenv("SCOUT_ADMIN_USER_ID", "")
    if not admin_uid or requesting_user_id != admin_uid:
        return ":lock: Usage reports are admin-only."

    log_path = pathlib.Path(__file__).parent / "data" / "usage_log.jsonl"
    if not log_path.exists():
        return "No usage data yet — logging started after this deploy. Check back after a few queries."

    records = []
    for line in log_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(_json.loads(line))
        except Exception as e:
            log.debug("get_usage_report malformed line swallowed: %s", e)
    now = datetime.utcnow()  # naive UTC to match stored timestamps (utcnow().isoformat())
    cutoff_7d  = now - timedelta(days=7)
    cutoff_30d = now - timedelta(days=30)
    recent_7d  = [r for r in records if datetime.fromisoformat(r["ts"]) >= cutoff_7d]
    recent_30d = [r for r in records if datetime.fromisoformat(r["ts"]) >= cutoff_30d]

    user_counts = Counter(r.get("user_name", r.get("user_id", "unknown")) for r in recent_30d)
    tool_counts = Counter(t for r in recent_30d for t in (r.get("tools") or []))
    avg_ms = int(sum(r.get("ms", 0) for r in recent_7d) / max(len(recent_7d), 1))

    lines = [f"*Scout Usage Report*\n"]
    lines.append(f"• *{len(recent_7d)}* queries last 7 days, *{len(recent_30d)}* last 30 days")
    lines.append(f"• Avg response time (7d): *{avg_ms // 1000}s*\n")
    lines.append("*Top users (30d):*")
    for name, count in user_counts.most_common(8):
        lines.append(f"• {name} — *{count}* queries")
    if tool_counts:
        lines.append("\n*Top tools called (30d):*")
        for tool, count in tool_counts.most_common(10):
            lines.append(f"• {tool} — *{count}x*")
    return "\n".join(lines)


def export_usage_log(days: int = 30, limit: int = 200,
                     requesting_user_id: str = "") -> str:
    """
    Dump raw (query → tools fired) pairs from usage_log.jsonl so an admin can
    eyeball whether Scout's tool routing is matching user intent. Admin-only.
    Returns a Slack-formatted block: one line per query, newest last.
    """
    import os, pathlib, json as _json
    from datetime import datetime, timedelta

    admin_uid = os.getenv("SCOUT_ADMIN_USER_ID", "")
    if not admin_uid or requesting_user_id != admin_uid:
        return ":lock: Usage export is admin-only."

    log_path = pathlib.Path(__file__).parent / "data" / "usage_log.jsonl"
    if not log_path.exists():
        return "No usage log yet."

    try:
        days_i = max(1, min(int(days or 30), 365))
    except (TypeError, ValueError):
        days_i = 30
    try:
        limit_i = max(1, min(int(limit or 200), 500))
    except (TypeError, ValueError):
        limit_i = 200

    cutoff = datetime.utcnow() - timedelta(days=days_i)
    rows = []
    for line in log_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = _json.loads(line)
            if datetime.fromisoformat(r["ts"]) >= cutoff:
                rows.append(r)
        except (ValueError, KeyError, TypeError, _json.JSONDecodeError) as e:
            log.debug("export_usage_log malformed row skipped: %s", e)
            continue

    rows = rows[-limit_i:]
    if not rows:
        return f"No usage entries in the last {days_i} days."

    lines = [f"*Scout usage export — last {days_i}d, {len(rows)} entries (newest last):*", "```"]
    for r in rows:
        ts    = r.get("ts", "")[:19]
        who   = r.get("user_name") or r.get("user_id", "?")
        q     = (r.get("query") or "").replace("\n", " ")[:140]
        tools = ",".join(r.get("tools") or []) or "<none>"
        ms    = r.get("ms", 0)
        lines.append(f"{ts}  {who:<14}  [{tools}]  ({ms}ms)  {q}")
    lines.append("```")
    return "\n".join(lines)


def record_entity_note(entity_name: str, entity_type: str, note: str,
                       exclude_from_fill_rate: bool = False,
                       _caller_user_id: str = "",
                       _caller_permalink: str = "") -> str:
    """
    Record publisher or advertiser knowledge in Scout's persistent learning store.
    Writes immediately and shows exactly what was stored (write-confirm-correct pattern).
    Calling again overwrites the previous entry — idempotent upsert.
    entity_type: 'publisher' or 'advertiser'
    exclude_from_fill_rate: publishers only — True suppresses from Pulse fill rate signals.

    Plan v3 §3.4 — provenance: `added_by` is the caller's Slack user_id (when
    available); falls back to "scout-agent" only if no caller is wired through.
    `permalink` records the Slack message that taught Scout this fact, so
    `why_entity_note` can return a clickable receipt.
    """
    import datetime as _dt

    overrides = _load_entity_overrides()
    section = "publishers" if entity_type.lower() == "publisher" else "advertisers"
    entry = {
        "note": note,
        "exclude_from_fill_rate": exclude_from_fill_rate if section == "publishers" else False,
        "added": _dt.date.today().isoformat(),
        "added_by": _caller_user_id or "scout-agent",
    }
    if _caller_permalink:
        entry["permalink"] = _caller_permalink
    overrides.setdefault(section, {})[entity_name] = entry
    _save_entity_overrides(overrides)

    lines = [f":white_check_mark: *{entity_name}* ({entity_type}) logged:"]
    lines.append(f"> _{note}_")
    if exclude_from_fill_rate and section == "publishers":
        lines.append(":no_entry_sign: Excluded from Pulse fill rate signals starting tomorrow's 8am run.")
    lines.append("_Reply to correct if I got anything wrong — I'll overwrite it, "
                 "or say `@Scout forget that about " + entity_name + "` to drop it._")
    return "\n".join(lines)


def forget_entity_note(entity_name: str, entity_type: str,
                       _caller_user_id: str = "",
                       _caller_permalink: str = "") -> str:
    """
    Plan v3 §3.4 — drop a previously-recorded publisher/advertiser fact.
    No-op (with friendly message) if the entry doesn't exist. Records a
    deletion audit row to data/entity_overrides_audit.jsonl for review.
    """
    import datetime as _dt

    overrides = _load_entity_overrides()
    section = "publishers" if entity_type.lower() == "publisher" else "advertisers"
    bucket = overrides.get(section) or {}
    if entity_name not in bucket:
        return (f":mag: I had no note for *{entity_name}* ({entity_type}) — "
                "nothing to forget.")
    dropped = bucket.pop(entity_name)
    overrides[section] = bucket
    _save_entity_overrides(overrides)

    # Append-only audit so we can review later who dropped what
    try:
        audit_path = pathlib.Path(__file__).parent / "data" / "entity_overrides_audit.jsonl"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("a") as _fh:
            _fh.write(json.dumps({
                "ts": _dt.datetime.utcnow().isoformat() + "Z",
                "action": "forget",
                "section": section,
                "entity": entity_name,
                "dropped": dropped,
                "by_user_id": _caller_user_id or "",
                "permalink": _caller_permalink or "",
            }) + "\n")
    except Exception as e:
        log.debug("forget_entity_note audit swallowed: %s", e)

    return (f":wastebasket: Forgot the note about *{entity_name}* ({entity_type}). "
            f"Was: _{dropped.get('note','(no note)')}_")


def why_entity_note(entity_name: str, entity_type: str = "") -> str:
    """
    Plan v3 §3.4 — explain where a publisher/advertiser fact came from.
    Returns the stored note plus provenance (who taught Scout, when, and the
    Slack permalink if available). Searches both sections when type omitted.
    """
    overrides = _load_entity_overrides()
    sections = (["publishers", "advertisers"] if not entity_type
                else ["publishers" if entity_type.lower() == "publisher" else "advertisers"])
    hits = []
    for section in sections:
        bucket = overrides.get(section) or {}
        if entity_name in bucket:
            row = bucket[entity_name]
            line = (f"*{entity_name}* ({section[:-1]}): _{row.get('note','(no note)')}_\n"
                    f":bookmark: learned from `{row.get('added_by','?')}` "
                    f"on {row.get('added','?')}")
            if row.get("permalink"):
                line += f" — <{row['permalink']}|Slack receipt>"
            hits.append(line)
    if not hits:
        return f":mag: I don't have a note for *{entity_name}*. Nothing to explain."
    return "\n\n".join(hits)


def get_offers_for_publisher(publisher_name: str) -> dict:  # returns dict since PR #18; old str annotation was stale
    """
    Return top affiliate offers (from SUPPORTED_NETWORKS inventory) that are
    a good fit for this publisher but not yet provisioned in their campaign set.
    Scored by estimated RPM using real MS conversion benchmarks (_scout_score).
    Different from get_supply_demand_gaps — surfaces NET-NEW affiliate inventory,
    not advertisers already on the MS platform.
    """
    import json as _json

    if not SNAPSHOT_PATH.exists():
        return (
            f"Offer inventory is empty — the scraper hasn't run yet on Render. "
            f"Run `@Scout refresh offers` to fetch now (~2 min), "
            f"or wait for the 6am CT daily auto-refresh."
        )

    try:
        all_offers = _json.loads(SNAPSHOT_PATH.read_text())
    except Exception as e:
        return f"Offer inventory file is corrupt: {e}. Try `@Scout refresh offers`."

    active = [o for o in all_offers if o.get("status") == "Active"]
    if not active:
        return "Offer inventory has 0 active offers. Try `@Scout refresh offers` to fetch latest."

    ch = _get_ch_client()

    # Resolve publisher
    pub_results = _q.publisher_lookup_by_name(ch, publisher_name)
    if not pub_results:
        return f"Publisher '{publisher_name}' not found in MomentScience."
    pub_id  = pub_results[0]["id"]
    pub_org = pub_results[0]["organization"]

    # Pull top-converting categories for this publisher from ClickHouse.
    # Used downstream to re-rank offers by audience fit, not just RPM.
    # Non-fatal — falls back to RPM-only ranking if query fails.
    top_categories: list[str] = _q.publisher_top_categories(ch, pub_id)

    # Existing advertiser set for this publisher (active campaigns only, lowercase)
    existing_adv: set[str] = _q.publisher_existing_advertisers(ch, pub_id)

    # Filter to net-new advertisers (not already provisioned for this publisher)
    def _is_new(offer: dict) -> bool:
        adv = (offer.get("advertiser") or "").lower()
        return not any(adv in ex or ex in adv for ex in existing_adv)

    candidates = [o for o in active if _is_new(o)]

    if not candidates:
        return (
            f"All active affiliate offers in the inventory are already provisioned in {pub_org}. "
            f"Use `@Scout revenue opportunities` for cross-publisher advertiser gaps, "
            f"or `@Scout what advertisers aren't in {pub_org}` for the provisioning view."
        )

    # Score using existing benchmark infrastructure
    benchmarks = _load_performance_benchmarks()
    scored_all = [(o, _scout_score(o, benchmarks)) for o in candidates]

    # Split: confirmed rate (score > 0, has real payout) vs uncontracted (Rate TBD)
    confirmed = [(o, s) for o, s in scored_all if s > 0]
    confirmed.sort(key=lambda x: x[1], reverse=True)
    confirmed_top = confirmed[:8]

    # Uncontracted = active, net-new, no payout confirmed yet — show top 5 by category fit
    uncontracted = [
        o for o, s in scored_all if s == 0
        and (o.get("_raw_payout") or "").lower() in ("rate tbd", "tbd", "", "?")
    ]
    uncontracted = uncontracted[:5]

    if not confirmed_top and not uncontracted:
        return (
            f"{len(candidates)} net-new affiliate offers found for {pub_org}, "
            f"but none have MS benchmark data yet (no prior run history). "
            f"Try `@Scout revenue opportunities` for cross-publisher revenue gaps with proven estimates."
        )

    _NETWORK_EMOJI = {"impact": "⚡", "maxbounty": "💰", "flexoffers": "🔗", "cj": "🌐"}

    # Surface category signals so the model can reason about audience fit
    category_signal = ""
    if top_categories:
        category_signal = f"\n*📊  Top-Converting Categories on {pub_org} (last 6mo):* {', '.join(top_categories)}"

    lines = [
        f"*🎯  {pub_org} — Offer Recommendations*",
        f"_{len(candidates)} net-new candidates screened · {len(confirmed_top)} with confirmed rates · "
        f"{len(uncontracted)} uncontracted_",
        category_signal,
        "",
    ]

    # ── Section 1: Confirmed rates — ranked by Scout Score ────────────────────
    if confirmed_top:
        lines.append("*✅  Confirmed Rate — Ready to Pitch*")
        lines.append("─" * 32)
        for i, (o, score) in enumerate(confirmed_top, 1):
            advertiser  = o.get("advertiser") or "Unknown"
            raw_payout  = o.get("_raw_payout") or o.get("payout") or "?"
            category    = o.get("category") or "Uncategorized"
            geo         = (o.get("geo") or "").strip()
            network     = o.get("network") or ""
            net_emoji   = _NETWORK_EMOJI.get(network.lower(), "•")
            net_label   = network.title()
            geo_str     = f" · {geo}" if geo and geo.lower() not in ("us", "usa", "united states") else " · US"
            lines.append(
                f"*{i}. {advertiser}*   {net_emoji} {net_label}\n"
                f"   `{raw_payout}` · {category}{geo_str} · est. *${score:.2f} RPM*"
            )
        lines.append("")

    # ── Section 2: Uncontracted — apply to unlock ─────────────────────────────
    if uncontracted:
        lines.append("*🔍  Uncontracted — Apply to Unlock Rate*")
        lines.append(
            "_These are in the affiliate network's marketplace but need a contract "
            "before they can be pitched. Once approved, the daily scrape picks up the rate._"
        )
        lines.append("─" * 32)
        for o in uncontracted:
            advertiser = o.get("advertiser") or "Unknown"
            category   = o.get("category") or "Uncategorized"
            geo        = (o.get("geo") or "").strip()
            network    = o.get("network") or ""
            net_emoji  = _NETWORK_EMOJI.get(network.lower(), "•")
            geo_str    = f" · {geo}" if geo and geo.lower() not in ("us", "usa", "united states") else " · US"
            lines.append(f"• *{advertiser}*   {net_emoji} {network.title()} · {category}{geo_str}")
        lines.append("")

    # ── Footer CTA ────────────────────────────────────────────────────────────
    if confirmed_top:
        top_name = confirmed_top[0][0].get("advertiser") or "top offer"
        lines.append(f":zap:  `@Scout brief {top_name}` to build a campaign brief · "
                     f"`@Scout brief [name]` for any other offer above")
    else:
        lines.append(":zap:  Apply for contracts in Impact's publisher portal to unlock rates — "
                     "Scout will pick them up automatically on the next daily scrape.")

    return {
        "pub_name": pub_org,
        "total_candidates": len(candidates),
        "confirmed_count": len(confirmed_top),
        "uncontracted_count": len(uncontracted),
        "category_signals": top_categories,
        "offers": _format_offers([o for o, _ in confirmed_top], benchmarks),
        "uncontracted": [
            {
                "advertiser": o.get("advertiser") or "Unknown",
                "network":    o.get("network") or "",
                "category":   o.get("category") or "Uncategorized",
                "geo":        (o.get("geo") or "").strip(),
            }
            for o in uncontracted
        ],
        "summary": "\n".join(l for l in lines if l),
    }


def get_revenue_today() -> dict:
    """
    Return today's intraday revenue by publisher vs 30-day rolling average.
    Returns formatted Slack mrkdwn in the 'formatted' key for LLM synthesis.

    Format spec:
        *$14K today*, 58% of daily avg. Still early.
        ---
        🟢 *AT&T* · $3,400 · 343 conversions
        🟡 *AT&T Buy Flow* · $515 · 41 conversions
        > 4 others · $1,155 combined
        ---
        ⚠️ *TuitionHero*: invalid conversions. $5,500 excluded until ops confirms netting.

    Signal thresholds vs publisher 30-day avg:
        🟢 ≥ 80%   🟡 40–79%   🔴 < 40%

    Revenue rounding:
        ≥ $10K → $XK (nearest $100)   ≥ $1K → $X,X00 (nearest $100)   < $1K → exact
    """
    import datetime as _dt_mod

    def _signal(today_rev: float, avg_rev: float) -> str:
        if avg_rev <= 0:
            return "🟢"
        pct = today_rev / avg_rev
        if pct >= 0.80:
            return "🟢"
        elif pct >= 0.40:
            return "🟡"
        return "🔴"

    try:
        ch = _get_ch_client()

        # Let ClickHouse own timezone math — avoids DST drift from Python UTC offset
        today_sql = """
SELECT
    c.user_id,
    u.organization AS publisher_name,
    sum(toFloat64OrNull(c.revenue)) AS today_rev,
    count() AS conversions
FROM adpx_conversionsdetails c
LEFT JOIN from_airbyte_users u ON u.id = toInt64(c.user_id)
PREWHERE toYYYYMM(c.created_at) = toYYYYMM(toDate(toTimeZone(now(), 'America/Chicago')))
WHERE toDate(toTimeZone(c.created_at, 'America/Chicago'))
      = toDate(toTimeZone(now(), 'America/Chicago'))
GROUP BY c.user_id, u.organization
HAVING today_rev > 0
ORDER BY today_rev DESC
"""

        # 30-day avg divided by 30 calendar days (includes zero-revenue days in denominator)
        avg_sql = """
SELECT
    c.user_id,
    sum(toFloat64OrNull(c.revenue)) / 30 AS avg_daily_rev
FROM adpx_conversionsdetails c
PREWHERE toYYYYMM(c.created_at) >= toYYYYMM(
    toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 35 DAY
)
WHERE toDate(toTimeZone(c.created_at, 'America/Chicago'))
      >= toDate(toTimeZone(now(), 'America/Chicago')) - INTERVAL 30 DAY
  AND toDate(toTimeZone(c.created_at, 'America/Chicago'))
      < toDate(toTimeZone(now(), 'America/Chicago'))
GROUP BY c.user_id
"""

        today_rows = ch.query(today_sql).result_rows
        avg_rows = ch.query(avg_sql).result_rows

        # Build avg lookup: user_id → avg_daily_rev
        avg_lookup: dict[int, float] = {int(r[0]): float(r[1] or 0) for r in avg_rows}

        # Today rows: (user_id, publisher_name, today_rev, conversions)
        publishers = []
        total_today = 0.0
        total_avg = 0.0
        for row in today_rows:
            uid = int(row[0])
            name = (row[1] or "").strip() or "Unknown Partner"
            rev = float(row[2] or 0)
            convs = int(row[3] or 0)
            avg = avg_lookup.get(uid, 0.0)
            publishers.append({
                "uid": uid,
                "name": name,
                "rev": rev,
                "convs": convs,
                "avg": avg,
                "signal": _signal(rev, avg),
            })
            total_today += rev
            total_avg += avg

        # Empty / early state
        if not publishers:
            return {
                "formatted": "_No revenue data yet today. Check back after 9am CT._",
            }

        # Headline
        pct_of_avg = (total_today / total_avg * 100) if total_avg > 0 else None
        import datetime as _dt_now
        from zoneinfo import ZoneInfo as _ZoneInfo
        _now_ct = _dt_now.datetime.now(_ZoneInfo("America/Chicago"))
        hour_ct = _now_ct.hour
        time_note = "Still early." if hour_ct < 12 else ("Midday pace." if hour_ct < 17 else "")
        headline_pct = f", {pct_of_avg:.0f}% of daily avg. {time_note}".strip() if pct_of_avg else "."
        headline = f"*{_fmt_rev(total_today)} today*{headline_pct}"

        # Top 3 inline, remainder grouped
        lines: list[str] = [headline, "---"]
        top3 = publishers[:3]
        rest = publishers[3:]
        for p in top3:
            lines.append(f"{p['signal']} *{p['name']}* · {_fmt_rev(p['rev'])} · {p['convs']:,} conversions")

        if rest:
            rest_total = sum(p["rev"] for p in rest)
            lines.append(f"> {len(rest)} others · {_fmt_rev(rest_total)} combined")

        return {"formatted": "\n".join(lines)}

    except Exception as e:
        from scout_ch import CHBusyError
        if isinstance(e, CHBusyError):
            raise
        log.exception("get_revenue_today failed")
        return {
            "formatted": "⚠️ Revenue data unavailable — query failed. Try again or check ClickHouse.",
        }


def get_revenue_today_projection() -> dict:
    """
    Project today's end-of-day revenue using a 60-day hour-of-day arrival curve
    and 8-week same-weekday median baseline. Returns formatted Slack mrkdwn
    in the 'formatted' key for LLM synthesis.

    Status dispatch:
        ok                    → "Projected EOD: *$X* (range $Y-$Z based on ±10% pace error).
                                 Currently *$A* — tracking *B%* of typical for this hour.
                                 Typical {weekday} lands ~*$C*."
        too_early             → formatted helper string (before 10am CT)
        insufficient_history  → formatted helper string
        unstable / error      → formatted helper string
    """
    try:
        ch = _get_ch_client()
        result = project_today_revenue(ch)
        status = result.get("status")

        if status != "ok":
            formatted = result.get("formatted") or "⚠️ Projection unavailable."
            return {"formatted": formatted}

        projected = result.get("projected_full_day") or 0.0
        today_rev = result.get("today_revenue") or 0.0
        dow_median = result.get("dow_median")
        pct_expected = result.get("pct_of_expected")
        as_of_ct = result.get("as_of_ct", "")

        # ±10% pace error band (gates loosen post-backtest in Step 6)
        low = projected * 0.90
        high = projected * 1.10

        import datetime as _dt
        from zoneinfo import ZoneInfo as _ZI
        weekday = _dt.datetime.now(_ZI("America/Chicago")).strftime("%A")

        pace_line = (
            f"Currently *{_fmt_rev(today_rev)}* — tracking *{pct_expected:.0f}%* of typical for this hour."
            if pct_expected is not None
            else f"Currently *{_fmt_rev(today_rev)}*."
        )
        median_line = (
            f"Typical {weekday} lands ~*{_fmt_rev(dow_median)}*."
            if dow_median
            else ""
        )

        lines = [
            f"Projected EOD: *{_fmt_rev(projected)}* (range {_fmt_rev(low)}-{_fmt_rev(high)} based on ±10% pace error).",
            pace_line,
        ]
        if median_line:
            lines.append(median_line)
        if as_of_ct:
            lines.append(f"_As of {as_of_ct} CT._")

        warning = result.get("warning")
        if warning:
            lines.append(f"⚠️ {warning}")

        return {"formatted": "\n".join(lines)}

    except Exception:
        log.exception("get_revenue_today_projection failed")
        return {
            "formatted": "⚠️ Projection unavailable — query failed. Try again or check ClickHouse.",
        }


def get_exposure_rate_anomalies(
    min_impressions_7d: int = None,
    min_payout: float = None,
    drop_pct: float = None,
) -> dict:
    """
    Find publisher-campaign pairs where yesterday's exposure CVR dropped vs. 7d baseline.

    Exposure CVR = conversions / impressions (exposure rate, NOT canonical CVR).
    Canonical CVR = conversions / clicks. These are different metrics:
    - exposure_cvr measures what fraction of ad impressions led to a conversion.
    - canonical CVR measures what fraction of clicks led to a conversion.
    This tool uses exposure_cvr intentionally for anomaly detection signal quality.

    Threshold defaults come from scout_thresholds.json; caller can override per-call.
    """
    try:
        ch = _get_ch_client()
        rows = _query_cvr_anomaly(
            ch,
            drop_pct=drop_pct,
            min_payout=min_payout,
            min_impressions_7d=min_impressions_7d,
        )
        if not rows:
            return {"anomalies": [], "count": 0, "summary": "No exposure CVR anomalies detected."}
        return {
            "anomalies": rows,
            "count": len(rows),
            "summary": f"{len(rows)} publisher-campaign pair(s) with significant exposure CVR drops.",
        }
    except Exception as e:
        log.exception("get_exposure_rate_anomalies failed")
        return {"error": str(e), "anomalies": []}


def get_expiring_campaigns(warning_days: int = None) -> dict:
    """
    Find active campaigns expiring within the next N days.
    Default window comes from scout_thresholds.json; caller can override.
    """
    try:
        ch = _get_ch_client()
        t = SCOUT_THRESHOLDS.get("signals", {})
        window = int(warning_days if warning_days is not None else t.get("expiration_warning_days", 7))
        rows = _query_expiring_campaigns(ch, warning_days=window)
        if not rows:
            return {"campaigns": [], "count": 0, "summary": f"No active campaigns expiring in the next {window} days."}
        return {
            "campaigns": rows,
            "count": len(rows),
            "window_days": window,
            "summary": f"{len(rows)} active campaign(s) expiring within {window} days.",
        }
    except Exception as e:
        log.exception("get_expiring_campaigns failed")
        return {"error": str(e), "campaigns": []}


def get_publisher_revenue_trends(days: int = 7) -> dict:
    """
    Publisher velocity alerts: publishers with significant revenue change vs prior 30-day baseline.
    Uses canonical annualized comparison: ((rev_7d/7)*30 - rev_30d) / rev_30d * 100.
    Threshold: -25% down, +20% up (from config/scout_thresholds.json).

    Replaces period-median algorithm (publisher_revenue_trends deprecated). Both up and
    down publishers are returned with a 'direction' field ('up'|'down').

    Note: the `days` parameter is accepted for backward compatibility but ignored —
    the canonical function uses a fixed 7-day/30-day window.
    """
    try:
        ch = _get_ch_client()
        rows = _q.velocity_alerts(ch)
        if not rows:
            return {"trends": [], "count": 0, "summary": "No publisher velocity anomalies detected."}
        down = [r for r in rows if r["direction"] == "down"]
        up = [r for r in rows if r["direction"] == "up"]
        return {
            "trends": rows,
            "count": len(rows),
            "down_count": len(down),
            "up_count": len(up),
            "summary": f"{len(rows)} publishers with velocity anomalies: {len(down)} down, {len(up)} up.",
        }
    except Exception as e:
        log.exception("get_publisher_revenue_trends failed")
        return {"error": str(e), "trends": []}


def _format_fleet_health(result: dict, days: int) -> str:
    """Render fleet health result as Slack mrkdwn. Called by get_publisher_fleet_health."""
    lines = [f"*Publisher Fleet Health — last {days} day{'s' if days != 1 else ''}*"]

    if result.get("insufficient_history"):
        lines.append(
            "_⚠️ Not enough history yet — need at least 3 weeks of data at $500+/week._"
        )
        return "\n".join(lines)

    total_gap = result.get("total_gap", 0.0)
    total = result.get("total_publishers", 0)
    act_now = result.get("act_now", [])
    watch = result.get("watch", [])
    healthy = result.get("healthy_top5", [])
    as_of = (result.get("as_of", "") or "")[:10]
    affected = len(act_now) + len(watch)

    # ── Headline ─────────────────────────────────────────────────────────
    if result.get("platform_alarm"):
        lines.append("")
        lines.append(
            f":rotating_light: *{len(act_now)} publishers in freefall simultaneously* — "
            "likely a platform-wide tracking issue, not individual publisher problems. "
            "Check ClickHouse connectivity and pixel/postback health first."
        )
    elif total_gap > 0:
        pub_word = "publisher" if affected == 1 else "publishers"
        lines.append(
            f":red_circle: *${total_gap:,.0f} behind baseline* across {affected} {pub_word}"
        )
    else:
        lines.append(":large_green_circle: All publishers tracking on or above baseline")

    def _pub_line(pub: dict) -> str:
        tier = f" [T{pub['tier']}]" if pub.get("tier") else ""
        sigma = pub["sigma_score"]
        sigma_str = f"{abs(sigma):.1f}σ" if sigma != -99.0 else "∞σ"
        return (
            f"• {pub['publisher_name']}{tier} · "
            f"*${pub['dollar_gap']:,.0f} gap* · "
            f"{pub['delta_pct']:+.1f}% · "
            f"_{sigma_str} below normal_"
        )

    # ── Act Now ──────────────────────────────────────────────────────────
    if act_now:
        lines.append("")
        label = "publisher" if len(act_now) == 1 else "publishers"
        lines.append(f":rotating_light: *Act Now* ({len(act_now)} {label})")
        for pub in act_now:
            lines.append(_pub_line(pub))

    # ── Watch ────────────────────────────────────────────────────────────
    if watch:
        lines.append("")
        label = "publisher" if len(watch) == 1 else "publishers"
        lines.append(f":eyes: *Watch* ({len(watch)} {label})")
        for pub in watch:
            lines.append(_pub_line(pub))

    if not act_now and not watch:
        lines.append("")
        lines.append(":white_check_mark: No publishers need attention this week")

    # ── Healthy compact line ──────────────────────────────────────────────
    if healthy:
        lines.append("")
        gains = " · ".join(
            f"{p['publisher_name']} +${p['revenue_actual'] - p['revenue_expected']:,.0f}"
            for p in healthy[:3]
        )
        extra = f" · +{len(healthy) - 3} more" if len(healthy) > 3 else ""
        lines.append(f":white_check_mark: *Healthy* — {gains}{extra}")

    # ── Action line ───────────────────────────────────────────────────────
    if act_now and total_gap > 0:
        top = act_now[0]
        tier_str = f" [T{top['tier']}]" if top.get("tier") else ""
        pct_of_gap = top["dollar_gap"] / total_gap * 100 if total_gap > 0 else 0
        lines.append(
            f":zap: *{top['publisher_name']}{tier_str}* is the top priority — "
            f"${top['dollar_gap']:,.0f} gap ({pct_of_gap:.0f}% of total shortfall)."
        )
    elif watch:
        lines.append(
            f":zap: *Action:* Investigate {watch[0]['publisher_name']} — "
            f"${watch[0]['dollar_gap']:,.0f} gap."
        )

    # ── Footer ────────────────────────────────────────────────────────────
    lines.append(f"_{total} publishers tracked · as of {as_of}_")
    return "\n".join(lines)


def get_publisher_fleet_health(
    days: int = 7,
) -> dict:
    """
    Fleet-level publisher health using σ-based statistical baseline.

    Rolling 4-week same-period average as baseline. Only surfaces publishers
    with >= $500/week baseline AND >= 1.5σ below normal — separates real
    signal from normal variance.

    Classifies:
    - Act Now: >= 2σ below normal AND >= $500 dollar gap
    - Watch:   >= 1.5σ below normal AND >= $200 dollar gap
    - Healthy: on or above baseline (top 3 gains shown)
    - Platform alarm: > 5 Act Now publishers simultaneously

    Returns formatted Slack text in the 'formatted' key.
    """
    try:
        days = max(1, min(90, int(days)))

        from queries import get_publisher_fleet_health_data
        ch = _get_ch_client()
        result = get_publisher_fleet_health_data(ch, days=days)

        return {"formatted": _format_fleet_health(result, days)}

    except Exception:
        log.exception("get_publisher_fleet_health failed")
        return {
            "formatted": "⚠️ Fleet health unavailable — query failed. Try again or check ClickHouse.",
        }


def get_advertiser_revenue_trends(days: int = 7) -> dict:
    """
    Advertiser revenue trends: actual vs. historical median, aggregated cross-publisher.
    Uses period-median algorithm (no canonical advertiser velocity function yet; see
    velocity_alerts() in queries.py for publisher-level canonical annualized velocity).
    """
    try:
        ch = _get_ch_client()
        rows = _query_advertiser_revenue_trends(ch, days=int(days))
        if not rows:
            return {"trends": [], "count": 0, "days": days, "summary": "No advertiser revenue trend data available."}
        down = [r for r in rows if r["trend"] == "down"]
        up = [r for r in rows if r["trend"] == "up"]
        return {
            "trends": rows,
            "count": len(rows),
            "days": days,
            "down_count": len(down),
            "up_count": len(up),
            "summary": f"{len(rows)} advertisers with trend data: {len(down)} down, {len(up)} up.",
        }
    except Exception as e:
        log.exception("get_advertiser_revenue_trends failed")
        return {"error": str(e), "trends": []}


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
    "run_self_qa": None,  # registered below after function definition
}


# ── Self-QA suite ─────────────────────────────────────────────────────────────

# Every major intent Scout supports, plus data-boundary probes.
# Format: (label, question, pass_hints)
# pass_hints: strings that should appear in a passing response (any one match = pass)
_QA_SUITE: list[tuple[str, str, list[str], str]] = [
    # ── Core health ──────────────────────────────────────────────────────────
    ("System status",
     "status",
     ["healthy"],
     "Core Health"),

    # ── Dark offers ───────────────────────────────────────────────────────────
    ("Dark offers",
     "ghost campaigns",
     ["ghost", "campaign", "impression", "revenue", "postback", "no ghost"],
     "Core Health"),

    # ── Scout threshold config ────────────────────────────────────────────────
    ("Scout threshold config",
     "what are Scout's current alert thresholds?",
     ["threshold", "config/scout_thresholds", "cvr", "fill", "velocity", "cap", "expiration"],
     "Core Health"),

    # ── Offer search — vertical ───────────────────────────────────────────────
    ("Offer search — finance vertical",
     "best offers for a financial services partner",
     ["capital one", "rocket", "payout", "rpm", "cpa", "offer"],
     "Offer Intelligence"),

    # ── Offer search — specific publisher ────────────────────────────────────
    ("Offers for named publisher",
     "what offers should we pitch to AT&T?",
     ["offer", "payout", "rpm", "finance", "campaign", "epc"],
     "Offer Intelligence"),

    # ── Supply/demand gaps ────────────────────────────────────────────────────
    ("Supply demand gaps",
     "where are our biggest supply gaps right now?",
     ["gap", "publisher", "offer", "category", "missing", "opportunity"],
     "Offer Intelligence"),

    # ── Offer inventory count ─────────────────────────────────────────────────
    ("Offer inventory count",
     "how many offers do we have in the inventory and which network has the most?",
     ["offer", "network", "cj", "impact", "total", "active"],
     "Offer Intelligence"),

    # ── Pipeline health ───────────────────────────────────────────────────────
    ("Pipeline health",
     "what is the health of our offer pipeline?",
     ["pipeline", "offer", "notion", "queue", "network", "active"],
     "Offer Intelligence"),

    # ── Revenue drop analysis ─────────────────────────────────────────────────
    ("WoW revenue drop",
     "which publishers dropped the most revenue this week vs last week?",
     ["publisher", "revenue", "drop", "week", "$"],
     "Revenue & Publisher"),

    # ── Publisher health ──────────────────────────────────────────────────────
    ("Publisher health",
     "how is TextNow performing?",
     ["textnow", "impression", "revenue", "click", "cvr", "funnel"],
     "Revenue & Publisher"),

    # ── Campaign status ───────────────────────────────────────────────────────
    ("Campaign status check",
     "is Capital One Shopping still active?",
     ["capital one", "active", "campaign", "status", "paused"],
     "Revenue & Publisher"),

    # ── Revenue projection ────────────────────────────────────────────────────
    ("Revenue projection",
     "project Truist revenue for this month",
     ["truist", "revenue", "$", "projection", "month", "forecast"],
     "Revenue & Publisher"),

    # ── Perkswall ────────────────────────────────────────────────────────────
    ("Perkswall engagement",
     "how is perkswall performing?",
     ["perkswall", "engagement", "click", "impression", "session"],
     "Revenue & Publisher"),

    # ── Multi-part question (new protocol test) ───────────────────────────────
    ("Multi-part question decomposition",
     "For our Intuit TurboTax review: what revenue did they drive this year? What were the top publishers? What offers worked best?",
     ["turbotax", "revenue", "publisher", "campaign", "$"],
     "Revenue & Publisher"),

    # ── CVR anomalies ────────────────────────────────────────────────────────
    ("CVR anomaly detection",
     "are there any campaigns with unusual drops in conversion rate recently?",
     ["cvr", "campaign", "drop", "impression", "anomaly", "conversion", "no anomaly", "no campaign"],
     "Revenue & Publisher"),

    # ── Expiring campaigns ───────────────────────────────────────────────────
    ("Expiring campaign warnings",
     "which campaigns are expiring in the next two weeks?",
     ["expir", "campaign", "end", "day", "active", "no campaign", "none expiring"],
     "Revenue & Publisher"),

    # ── Publisher revenue trends ─────────────────────────────────────────────
    ("Publisher revenue trends",
     "which publishers have shown declining revenue trends recently?",
     ["publisher", "trend", "revenue", "decline", "period", "down", "no publisher", "stable"],
     "Revenue & Publisher"),

    # ── Data boundary: SOV (should gracefully decline) ────────────────────────
    ("Data boundary — SOV",
     "what is our share of voice vs competitors?",
     ["don't have", "not tracked", "not in", "isn't in", "network", "dashboard", "sov"],
     "Data Boundaries"),

    # ── Data boundary: strategic question ────────────────────────────────────
    ("Data boundary — strategic intent",
     "what does AT&T want from us next year?",
     ["don't have", "not in", "judgment", "can't", "call", "data"],
     "Data Boundaries"),
]


def run_self_qa() -> dict:
    """
    Run Scout's full QA suite against itself and return a structured report.
    Each test calls ask() with a representative question, checks the response
    for expected content, and records pass/fail + elapsed time.
    Called when a user says 'QA yourself', 'self test', 'run QA suite'.
    """
    import time as _time

    results = []
    total = len(_QA_SUITE)

    for label, question, pass_hints, _category in _QA_SUITE:
        t0 = _time.monotonic()
        try:
            response = ask(question, history=[], user_id="self-qa")
            elapsed = _time.monotonic() - t0

            # Part 4: ask() returns AskResult; payload carries structured
            # dispatch (brief/opportunities) — prefer fallback_text when present
            # so QA scores the human-facing string, not the dataclass repr.
            payload = response.payload or {}
            text = payload.get("fallback_text") or response.text

            text_lower = text.lower()
            responded = len(text.strip()) > 40
            hint_match = any(h.lower() in text_lower for h in pass_hints)
            passed = responded and hint_match
            snippet = text[:120].replace("\n", " ")

        except Exception as e:
            elapsed = _time.monotonic() - t0
            passed = False
            snippet = f"ERROR: {e}"

        results.append({
            "label": label,
            "question": question,
            "passed": passed,
            "elapsed": round(elapsed, 1),
            "snippet": snippet,
        })

    passed_count = sum(1 for r in results if r["passed"])
    return {
        "total": total,
        "passed": passed_count,
        "failed": total - passed_count,
        "results": results,
    }


TOOL_MAP["run_self_qa"] = run_self_qa
TOOL_MAP["get_scout_config"] = get_scout_config
TOOL_MAP["list_thresholds"] = list_thresholds
TOOL_MAP["get_threshold_history"] = get_threshold_history
TOOL_MAP["set_threshold"] = set_threshold
TOOL_MAP["force_run_monitor"] = force_run_monitor


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


def _coerce_threshold_value(raw: str):
    """Parse a stringified threshold value into bool/int/float, falling back to
    the raw string if no numeric form matches. `"true"`/`"false"` are case-
    insensitive; `.` anywhere in the input forces float interpretation."""
    low = raw.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    if "." in raw:
        try:
            return float(raw)
        except ValueError:
            return raw
    try:
        return int(raw)
    except ValueError:
        return raw


class AmbiguousThresholdKey(ValueError):
    """Bare key matches more than one section — caller must disambiguate."""
    def __init__(self, key: str, sections: list[str]):
        self.key = key
        self.sections = sections
        super().__init__(
            f"`{key}` exists in multiple sections ({', '.join(sections)}); "
            f"qualify it as `<section>.{key}`."
        )


def _split_dotted_key(dotted: str) -> tuple[str, str]:
    """Split 'signals.cap_alert_pct' → ('signals', 'cap_alert_pct').

    Bare keys are resolved by searching the base schema for a unique match
    across sections. Raises AmbiguousThresholdKey if multiple sections own a
    key with the same name. Falls back to ('signals', dotted) if no section
    owns the key — `set_threshold` will then surface a proper unknown-key
    error with a suggestion."""
    if "." in dotted:
        section, _, key = dotted.partition(".")
        return section, key
    owners = [sec for sec, body in _BASE_THRESHOLDS.items()
              if isinstance(body, dict) and dotted in body]
    if len(owners) == 1:
        return owners[0], dotted
    if len(owners) > 1:
        raise AmbiguousThresholdKey(dotted, owners)
    return "signals", dotted


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
            section, key = _split_dotted_key(dotted)
        except AmbiguousThresholdKey as exc:
            return AskResult(
                text=f":warning: {exc}",
                tools_called=(), duration_ms=0,
            )
        value = _coerce_threshold_value(raw_val)
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
    corrections_ctx = _get_corrections_context()
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
                    payload={
                        "type": "brief",
                        "brief_data": brief_data,
                        "copy": copy_data,
                        # Full Claude text as fallback so Slack shows something useful
                        # even if Block Kit rendering fails
                        "fallback_text": _fallback_text,
                    },
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
                    payload={
                        "type": "opportunities",
                        "text": text or "",
                        "offers": _opportunity_offers,
                        "suggestions": suggestions,
                    },
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
                        payload={
                            "type": "text_with_context",
                            "text": text or "(no response)",
                            "extracted_context": extracted,
                            "suggestions": suggestions,
                        },
                    )

            return AskResult(
                text=text or "(no response)",
                tools_called=_tools_called,
                duration_ms=_dur(),
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
                with ThreadPoolExecutor(max_workers=len(tool_blocks)) as executor:
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
                    if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                        _brief_results.append(result)
                    if block.name == "get_top_opportunities" and isinstance(result, list) and not _opportunity_offers:
                        _opportunity_offers.extend(result)
                    if block.name == "get_offers_for_publisher" and isinstance(result, dict) and result.get("offers") and not _opportunity_offers:
                        _opportunity_offers.extend(result["offers"])
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
                    if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                        _brief_results.append(result)
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
                if block.name == "draft_campaign_brief" and isinstance(result, dict) and "advertiser" in result:
                    _brief_results.append(result)  # collect all, use first for primary
                if block.name == "get_top_opportunities" and isinstance(result, list) and not _opportunity_offers:
                    _opportunity_offers.extend(result)
                if block.name == "get_offers_for_publisher" and isinstance(result, dict) and result.get("offers") and not _opportunity_offers:
                    _opportunity_offers.extend(result["offers"])
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
                    )
            return AskResult(text="(no response)", tools_called=_tools_called, duration_ms=_dur())

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
    normalized = re.sub(r"<@[A-Z0-9]+>", "", raw or "").strip().lower()
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
