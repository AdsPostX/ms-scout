"""
ThresholdManager — config, overrides, benchmarks, and entity knowledge for Scout.

Extracted from scout_agent.py (phase 13-01). All callers import from here;
no wildcard re-export shim exists.
"""
from __future__ import annotations

import copy
import difflib
import json
import logging
import pathlib
import threading
import time
from datetime import datetime, timezone

log = logging.getLogger("scout_thresholds")

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

# Columns Scout depends on. (table, column, must_have_data).
# must_have_data=True means the column must have at least _SCHEMA_DEPS_MIN_ROWS non-null rows.
_SCHEMA_DEPS: list[tuple[str, str, bool]] = [
    # from_airbyte_campaigns — driver of benchmarks + scoring
    ("from_airbyte_campaigns",      "id",                    False),
    ("from_airbyte_campaigns",      "adv_name",              True),
    ("from_airbyte_campaigns",      "tags",                  True),   # source of category data
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
    # CVR anomaly + expiration monitors
    ("adpx_conversionsdetails",     "payout",                True),
    ("from_airbyte_campaigns",      "end_date",              False),  # NULL for open-ended campaigns
    ("from_airbyte_campaigns",      "status",                True),
]

_SCHEMA_DEPS_MIN_ROWS = 100


class AmbiguousThresholdKey(ValueError):
    """Bare key matches more than one section — caller must disambiguate."""

    def __init__(self, key: str, sections: list[str]):
        self.key = key
        self.sections = sections
        super().__init__(
            f"`{key}` exists in multiple sections ({', '.join(sections)}); "
            f"qualify it as `<section>.{key}`."
        )


class ThresholdManager:
    """Config/override/benchmark state for Scout. One singleton per process."""

    def __init__(
        self,
        config_dir: pathlib.Path = pathlib.Path(__file__).parent / "config",
        data_dir: pathlib.Path = pathlib.Path(__file__).parent / "data",
    ) -> None:
        self._config_dir = config_dir
        self._data_dir = data_dir
        self._thresholds_cache: dict | None = None
        self._benchmarks_cache: dict = {}
        self._benchmarks_loaded_at: float = 0.0
        self._benchmarks_ttl: int = 3600
        self._benchmarks_lock = threading.Lock()
        # Paths derived from constructor arguments — testable without patching globals
        self._thresholds_file = config_dir / "scout_thresholds.json"
        self._team_corrections_path = config_dir / "team_corrections.json"
        self._learnings_path = data_dir / "learnings.json"
        self._learned_benchmarks_path = data_dir / "learned_benchmarks.json"
        self._entity_overrides_path = data_dir / "entity_overrides.json"
        self._launched_offers_path = data_dir / "launched_offers.json"
        self._pulse_state_path = data_dir / "pulse_state.json"

    # ── Config loading ────────────────────────────────────────────────────────

    def _load_base(self) -> dict:
        """Allowlist of valid `section.key` pairs: fallback ← config/scout_thresholds.json.

        Excludes runtime overrides so write-validation cannot be tricked by a previously
        persisted bad key in data/threshold_overrides.json.
        """
        try:
            if not self._thresholds_file.exists():
                log.warning(
                    f"[config] {self._thresholds_file} missing — using fallback thresholds"
                )
                base = {k: dict(v) for k, v in _SCOUT_THRESHOLDS_FALLBACK.items()}
            else:
                loaded = json.loads(self._thresholds_file.read_text())
                loaded.pop("_doc", None)
                base = {k: dict(v) for k, v in _SCOUT_THRESHOLDS_FALLBACK.items()}
                for section, values in loaded.items():
                    if section in base and isinstance(values, dict):
                        base[section].update(values)
                    else:
                        base[section] = values
        except Exception as e:
            log.warning(f"[config] _load_base() failed on config file, using fallback: {e}")
            base = {k: dict(v) for k, v in _SCOUT_THRESHOLDS_FALLBACK.items()}
        return base

    def load(self) -> dict:
        """Load Scout thresholds: base schema ← data/threshold_overrides.json. Cached.

        Runtime overrides from `set_threshold` are layered last. Cache is invalidated
        by set_threshold() after each write.
        """
        if self._thresholds_cache is not None:
            return self._thresholds_cache
        merged = self._load_base()
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
            log.warning(f"[config] load() failed applying overrides: {e}")
        self._thresholds_cache = merged
        return self._thresholds_cache

    # ── Data quality ──────────────────────────────────────────────────────────

    def data_quality_tier(self, days_of_data: int, sessions: int = 0) -> dict:
        """Compute confidence tier for a data window. Used by tools to populate data_quality."""
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
        return {
            "tier": tier,
            "emoji": emoji,
            "days_of_data": days_of_data,
            "sessions": sessions,
            "note": note,
        }

    # ── Entity overrides ──────────────────────────────────────────────────────

    def entity_overrides(self) -> dict:
        """Load publisher/advertiser knowledge store. Returns empty structure if missing."""
        try:
            if self._entity_overrides_path.exists():
                return json.loads(self._entity_overrides_path.read_text())
        except Exception as e:
            log.debug("entity_overrides swallowed: %s", e)
        return {"publishers": {}, "advertisers": {}}

    def save_entity_overrides(self, overrides: dict) -> None:
        """Atomic write to entity_overrides.json using temp+rename (safe on Linux/Render)."""
        self._entity_overrides_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._entity_overrides_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(overrides, indent=2))
        tmp.replace(self._entity_overrides_path)

    def corrections_context(self) -> str:
        """Load high-confidence corrections and return as a grounding context string."""
        corrections: list = []
        try:
            if self._team_corrections_path.exists():
                data = json.loads(self._team_corrections_path.read_text())
                corrections += [
                    c for c in data.get("corrections", []) if c.get("confidence") == "high"
                ]
        except Exception as e:
            log.debug("corrections_context team_corrections swallowed: %s", e)
        try:
            if self._learnings_path.exists():
                data = json.loads(self._learnings_path.read_text())
                corrections += [
                    c for c in data.get("corrections", []) if c.get("confidence") == "high"
                ]
        except Exception as e:
            log.debug("corrections_context learnings swallowed: %s", e)
        try:
            overrides = self.entity_overrides()
            for pub, data in overrides.get("publishers", {}).items():
                prov = f" [learned from {data.get('added_by','?')} on {data.get('added','?')}]"
                corrections.append(
                    {"confidence": "high",
                     "correction": f"Publisher {pub}: {data['note']}{prov}"}
                )
            for adv, data in overrides.get("advertisers", {}).items():
                prov = f" [learned from {data.get('added_by','?')} on {data.get('added','?')}]"
                corrections.append(
                    {"confidence": "high",
                     "correction": f"Advertiser {adv}: {data['note']}{prov}"}
                )
        except Exception as e:
            log.debug("corrections_context overrides swallowed: %s", e)
        if not corrections:
            return ""
        lines = [f"- {c['correction']}" for c in corrections[-16:]]
        return (
            "TEAM CORRECTIONS (from prior feedback — treat these as ground truth):\n"
            + "\n".join(lines)
            + "\n\n"
        )

    # ── Benchmarks ────────────────────────────────────────────────────────────

    def merge_learned_benchmarks(self) -> None:
        """Merge data/learned_benchmarks.json into the benchmarks cache."""
        try:
            if not self._learned_benchmarks_path.exists():
                return
            lb = json.loads(self._learned_benchmarks_path.read_text())
            if not lb:
                return
            learned = self._benchmarks_cache.setdefault("by_learned_actuals", {})
            for key, entry in lb.items():
                learned[key] = {
                    "avg_cvr_pct": 0.0,
                    "avg_rpm": entry.get("rpm_actual_avg", 0.0),
                    "sample_campaigns": entry.get("sample_count", 0),
                }
            log.info(f"Merged {len(lb)} learned benchmark entries into benchmarks cache")
        except Exception as e:
            log.warning(f"merge_learned_benchmarks failed: {e}")

    def _load_benchmarks_file(self) -> dict:
        """Query ClickHouse for real CVR + RPM benchmarks grounded in actual MS conversion data.

        Returns four lookup tiers — used in priority order by _scout_score():
          1. by_offer_impact_id   — exact offer match (highest confidence)
          2. by_adv_name          — same advertiser, different offer (high confidence)
          3. by_category_payout   — (category, payout_type) combo (medium confidence)
          4. by_payout_type       — payout type only across all offers (low confidence fallback)
        """
        try:
            # Lazy imports — avoids circular import at module load time.
            # scout_ch lazy-imports from scout_thresholds; doing it at the top of this
            # file would create a module-load-time cycle.
            from scout_ch import _get_ch_client
            import queries as _q_mod
            ch = _get_ch_client()

            rows = _q_mod.performance_benchmarks_raw(ch)

            by_offer: dict = {}
            by_adv: dict = {}
            by_cat: dict = {}

            for _id, adv_name, impact_id, category, impressions, cvr_pct, rpm in rows:
                cvr = float(cvr_pct or 0)
                rpm_val = float(rpm or 0)
                imp = int(impressions or 0)
                entry = {
                    "adv_name": adv_name,
                    "cvr_pct": cvr,
                    "rpm": rpm_val,
                    "impressions": imp,
                    "category": category,
                }

                if impact_id:
                    by_offer[impact_id] = entry

                adv_key = (adv_name or "").lower().strip()
                if adv_key and (adv_key not in by_adv or rpm_val > by_adv[adv_key]["rpm"]):
                    by_adv[adv_key] = entry

                cat_key = (category or "").strip()
                if cat_key:
                    if cat_key not in by_cat:
                        by_cat[cat_key] = {"total_cvr": 0.0, "total_rpm": 0.0, "count": 0}
                    by_cat[cat_key]["total_cvr"] += cvr
                    by_cat[cat_key]["total_rpm"] += rpm_val
                    by_cat[cat_key]["count"] += 1

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

            overall = _q_mod.benchmark_overall_cvr(ch)
            if "error" in overall:
                log.warning(
                    f"Tier 4 baseline query failed, skipping fallback benchmark: {overall['error']}"
                )
                overall = {}
            by_payout_type = {"_all": overall} if overall else {}
            if overall:
                log.info(
                    f"Tier 4 baseline: {overall['cvr_pct']:.4f}% CVR / "
                    f"${overall['rpm']:.2f} RPM across {overall['campaigns']} MS campaigns"
                )

            result = {
                "by_offer_impact_id":  by_offer,
                "by_adv_name":         by_adv,
                "by_category_payout":  {},
                "by_payout_type":      by_payout_type,
                "by_category":         category_benchmarks,
            }
            log.info(
                f"Benchmarks loaded: {len(by_offer)} offers, {len(by_adv)} advertisers, "
                f"{len(category_benchmarks)} categories, "
                f"{'Tier4 baseline active' if by_payout_type else 'Tier4 unavailable'}"
            )
            return result

        except Exception as e:
            log.warning(f"Could not load performance benchmarks from ClickHouse: {e}")
            return {
                "by_offer_impact_id": {},
                "by_adv_name": {},
                "by_category_payout": {},
                "by_payout_type": {},
                "by_category": {},
            }

    def benchmarks(self) -> dict:
        with self._benchmarks_lock:
            if (
                not self._benchmarks_cache
                or (time.time() - self._benchmarks_loaded_at) > self._benchmarks_ttl
            ):
                self._benchmarks_cache = self._load_benchmarks_file()
                self.merge_learned_benchmarks()
                self._benchmarks_loaded_at = time.time()
            return copy.deepcopy(self._benchmarks_cache)

    # ── Tags/categories helper ────────────────────────────────────────────────

    def extract_real_categories(self, tags_value) -> list[str]:
        """Parse from_airbyte_campaigns.tags JSON array, filter out `internal-*` system tags."""
        if not tags_value:
            return []
        try:
            parsed = json.loads(tags_value) if isinstance(tags_value, str) else tags_value
        except (json.JSONDecodeError, TypeError):
            return []
        if not isinstance(parsed, list):
            return []
        return [t for t in parsed if isinstance(t, str) and not t.lower().startswith("internal-")]

    # ── Schema validation ─────────────────────────────────────────────────────

    def validate_schema_deps(self, ch) -> dict:
        """Boot-time check: confirm columns Scout depends on exist and have data."""
        violations: list[str] = []
        warnings: list[str] = []
        try:
            tables = sorted({t for t, _, _ in _SCHEMA_DEPS})
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

    # ── State file readers ────────────────────────────────────────────────────

    def load_launched_offers_state(self) -> dict:
        try:
            if self._launched_offers_path.exists():
                return json.loads(self._launched_offers_path.read_text())
        except Exception as e:
            log.debug("load_launched_offers_state swallowed: %s", e)
        return {}

    def load_pulse_state(self) -> dict:
        """Read pulse_state.json directly. Avoids importing scout_state."""
        try:
            if self._pulse_state_path.exists():
                return json.loads(self._pulse_state_path.read_text())
        except Exception as e:
            log.debug("load_pulse_state swallowed: %s", e)
        return {}

    # ── Threshold tools (called by TOOL_MAP) ─────────────────────────────────

    def list_thresholds(self) -> dict:
        """Return all active thresholds (after override merge) plus override metadata."""
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
            "thresholds": self.load(),
            "overridden": overridden,
            "config_file": str(
                self._thresholds_file.relative_to(self._config_dir.parent)
            ),
            "override_file": "data/threshold_overrides.json",
        }

    def threshold_history(self, key: str = "", limit: int = 50) -> dict:
        """Return recent threshold change events from data/threshold_changelog.jsonl."""
        try:
            import scout_state
            entries = scout_state._read_threshold_changelog(
                limit=max(1, min(int(limit or 50), 500)),
                key=(key or None),
            )
            return {"entries": entries, "count": len(entries), "filter": key or "all"}
        except Exception as e:
            log.warning(f"threshold_history failed: {e}")
            return {"error": str(e), "entries": [], "count": 0}

    def set_threshold(
        self,
        section: str = "",
        key: str = "",
        value=None,
        reason: str = "",
        _caller_user_id: str = "",
    ) -> dict:
        """Write a runtime override for one threshold and invalidate the in-process cache.

        Admin gate is enforced by the scout_agent.py wrapper before this is called.
        """
        section = (section or "").strip()
        key = (key or "").strip()
        if not section or not key:
            return {
                "ok": False,
                "error": "missing_args",
                "message": "section and key are required (e.g. section='signals', key='cap_alert_pct').",
            }
        if value is None:
            return {"ok": False, "error": "missing_value", "message": "value is required."}
        if not reason or not reason.strip():
            return {
                "ok": False,
                "error": "missing_reason",
                "message": "reason is required so the changelog stays useful.",
            }

        base = self._load_base()
        known_section = base.get(section)
        if not isinstance(known_section, dict):
            sections = list(base.keys())
            suggestions = difflib.get_close_matches(section, sections, n=1, cutoff=0.6)
            hint = f" Did you mean `{suggestions[0]}`?" if suggestions else ""
            return {
                "ok": False,
                "error": "unknown_section",
                "message": f"Unknown section `{section}` (valid: {', '.join(sections)}).{hint}",
            }
        if key not in known_section:
            keys = list(known_section.keys())
            suggestions = difflib.get_close_matches(key, keys, n=1, cutoff=0.6)
            hint = f" Did you mean `{section}.{suggestions[0]}`?" if suggestions else ""
            return {
                "ok": False,
                "error": "unknown_key",
                "message": f"Unknown key `{section}.{key}`.{hint}",
            }

        current = self.load()
        prior = (
            current.get(section, {}).get(key)
            if isinstance(current.get(section), dict)
            else None
        )

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

            scout_state._append_threshold_changelog(
                {
                    "ts": ts,
                    "key": f"{section}.{key}",
                    "section": section,
                    "name": key,
                    "prior": prior,
                    "value": value,
                    "set_by": _caller_user_id or "unknown",
                    "reason": reason.strip(),
                    "action": "set",
                }
            )

            # Invalidate cache so the next load() re-fetches with the new override
            self._thresholds_cache = None

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

    # ── Value coercion + key parsing ──────────────────────────────────────────

    def coerce_value(self, raw: str):
        """Parse a stringified threshold value into bool/int/float, falling back to raw string."""
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

    def _split_key(self, dotted: str) -> tuple[str, str]:
        """Split 'signals.cap_alert_pct' → ('signals', 'cap_alert_pct').

        Bare keys are resolved by searching the base schema for a unique match
        across sections. Raises AmbiguousThresholdKey if multiple sections own a
        key with the same name.
        """
        if "." in dotted:
            section, _, key = dotted.partition(".")
            return section, key
        base = self._load_base()
        owners = [sec for sec, body in base.items()
                  if isinstance(body, dict) and dotted in body]
        if len(owners) == 1:
            return owners[0], dotted
        if len(owners) > 1:
            raise AmbiguousThresholdKey(dotted, owners)
        return "signals", dotted


_manager = ThresholdManager()
