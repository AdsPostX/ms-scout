# Changelog

Resolved items moved out of `KNOWN_DEBT.md` once fixed. Each entry keeps its original resolution narrative.

## scout_handlers.py — final response routing quadruplicated (DM path, channel path, `_handle_suggestion`, `_handle_home_try_query`)

**Date:** not recorded in original entry.

**Resolved** by extracting `_render_and_post_response(web, response, *, surface, channel, thread_ts, placeholder_ts, elapsed, ...)`. On closer inspection while doing the extraction, the duplication was actually **four** near-identical copies, not three: the DM path and channel path inside `_handle_event_impl`, `_handle_suggestion`, and a fourth copy in `_handle_home_try_query`'s legacy path that had gone unnoticed because it wraps itself in its own outer try/except and so never crashed loudly — it just silently swallowed render failures via `log.exception()` with no user-facing error card, a milder instance of the same 2026-07-09 outage bug class. All four call sites now point at the single shared function; the App Home path gets the same `_post_error_update` error-card behavior as the other three as part of this fix.

Original text for history: the post-ask routing chain — brief / opportunities / plain-text rendering plus the `launched_offer` rocket-notification block — existed in near-identical copies at the DM path (~L3215) and channel path (~L3380) inside `_handle_event_impl`, `_handle_suggestion` (~L1290), and `_handle_home_try_query`. The cost was no longer theoretical: PR #332 (frozen-placeholder guard for the 2026-07-09 outage class) had to write the same try/except twice, and `_handle_suggestion` still needed a third copy in its own follow-up PR because it's a separate transcription of the same logic. When a safety fix must be applied N times and the Nth copy gets missed, the duplication is the bug — the missed `_handle_home_try_query` copy proved it.

The exception guard and the routing-guarded AST smoke test (`test_handle_event_response_routing_guarded`) now live in exactly one place (`_render_and_post_response`), and any future entry point is covered by construction.

## scout_digest.py:1381-1383 — `_build_sourcing_intel_blocks` hand-rolls payout resolution instead of `_resolve_payout()`

**Date:** not recorded in original entry.

**Resolved** by threading `payout_cache` into `_build_sourcing_intel_blocks()` and its call site, then swapping the hand-rolled `_parse_payout`/`_normalize_payout_type` pair for `_resolve_payout(offer_id, offer, payout_cache)`, same as `score_offer`/`build_why_text`/`build_digest_blocks`. Sourcing-signal offer dicts don't reliably carry the scraper-normalized `_payout_num`/`_payout_type_norm` fields (existing smoke tests construct them with only raw `payout`/`payout_type`), so `_resolve_payout()` itself was widened with a third fallback tier onto those raw fields — safe for the other three callers since it only engages when both `payout_cache` and `_payout_num` come up empty.

Original text for history: PR #327/#328 consolidated the digest's other three payout-resolution call sites (`score_offer`, `build_why_text`, `build_digest_blocks`) onto the shared `_resolve_payout(offer_id, offer, payout_cache)` helper. `_build_sourcing_intel_blocks()` still calls `_parse_payout(o.get("payout"))` + `_normalize_payout_type(o.get("payout_type") or "")` directly instead.

Not a drop-in swap: `_build_sourcing_intel_blocks(signals: dict)` has no `payout_cache` parameter in its signature, and its offer dicts (`net_offers`, sourcing-signal shape) use raw `payout`/`payout_type` keys — not the `_payout_num`/`_payout_type_norm` shape `_resolve_payout` expects from scraper-normalized offers. Converting it means either threading `payout_cache` through `_build_sourcing_intel_blocks` and its caller (`build_digest_blocks` → `sourcing_blocks = _build_sourcing_intel_blocks(sourcing_signals)`, line ~1718), or writing a cache-less variant — a real design decision, not a mechanical rename.

## scout_tools_offers.py vs. scout_agent.py — 4 duplicated functions, one already drifted

**Date:** not recorded in original entry.

**Resolved** by having `scout_agent.py` import `_norm`, `_scout_score`, `_format_offers`, and `_get_risk_flag` from `scout_tools_offers.py` (same pattern already used for `_dedupe_by_advertiser`) instead of carrying its own copies. The `_scout_score` divergence was reconciled by switching `scout_tools_offers.py`'s copy to call `_norm()` for `payout_type`/`adv_name`, matching what `scout_agent.py`'s copy already did; `scout_tools_offers.py`'s more defensive `_norm()` (coerces non-string input via `str()`) is now the single canonical version, so the `AttributeError`-on-non-string landmine described below no longer exists. All four inline defs were deleted from `scout_agent.py`; `python3 smoke_test.py` and the `TOOL_MAP` import check both pass.

Note: `draft_campaign_brief` (scout_tools_offers.py:612-614) has its own separate inline `.lower().strip()` normalization on local variables — a different, unrelated pattern instance, not one of the four functions unified here. Left untouched; not part of this fix's scope.

Original text for history: `scout_tools_offers.py` is live — `scout_agent.py:67` imports `_dedupe_by_advertiser` from it directly, and `tests/test_demand_feed_http.py` imports the module itself to exercise `_load_offers()`. But `scout_agent.py` also carried its own inline copies of four other functions that exist in `scout_tools_offers.py`, verified via direct `diff -u` on each pair (not inferred from a prior report):

- `_format_offers` (scout_tools_offers.py:465 / scout_agent.py:2293, 44 lines) — byte-identical
- `_get_risk_flag` (scout_tools_offers.py:528 / scout_agent.py:2372, 9 lines) — byte-identical
- `_scout_score` (scout_tools_offers.py:74 / scout_agent.py:728, ~79 lines) — **not** byte-identical: `scout_agent.py`'s copy normalizes `payout_type`/`adv_name` through a local `_norm()` helper, `scout_tools_offers.py`'s copy still inlines `.lower().strip()`. Same runtime result today, but it means the two functions had already been edited independently once — which is exactly how these things drift further.
- `_norm` (scout_tools_offers.py:299 / scout_agent.py:2082) — duplicated **and already behaviorally diverged**:
  ```python
  # scout_tools_offers.py
  def _norm(s) -> str:
      return str(s or "").strip().lower()

  # scout_agent.py
  def _norm(s: str) -> str:
      return s.lower().strip() if s else ""
  ```
  Both agree on string/empty/None input. `scout_agent.py`'s version raised `AttributeError` on a truthy non-string input (e.g. an int payout-type code); `scout_tools_offers.py`'s coerced via `str()` first. No call site ever passed a non-string value, so this hadn't fired in prod — but it was a live landmine, not a cosmetic difference.

`_dedupe_by_advertiser` was already correctly shared (imported, not duplicated) — this entry was only about the four functions above.

## config/scout_thresholds.json — `native_cards_enabled` dark-launched, never flipped on

**Date:** stale-as-of note dated 2026-07-29 in the original entry; no PR reference recorded.

**Resolved** — verified live in `config/scout_thresholds.json`: `native_cards_enabled` is now `true` and `offers_per_network` is `10`; both flags described below as unflipped have since been flipped on.

Original text for history: PR #323 (`carousel/digest-native-cards`) shipped native Slack card/carousel rendering behind `digest.native_cards_enabled`, explicitly set to `false` by design ("classic rendering is untouched until explicitly flipped on"). No follow-up task was ever filed to turn it on — no env var exists either (`grep -rn "NATIVE_CARDS"` is empty), so the only toggle is this JSON value.

`digest.offers_per_network` was also still `3`, not the intended `10` — same config block, same fix window.
