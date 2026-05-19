<!-- /autoplan restore point: /Users/siddharthshah/.gstack/projects/AdsPostX-ms-scout/claude-digest-no-score-reasons-autoplan-restore-20260519-005931.md -->

# Scout Digest — Slack UX Improvements

**Date:** 2026-05-19
**Branch:** `claude/digest-no-score-reasons`
**Status:** PROPOSED — pending full review
**Worktree:** `/Users/siddharthshah/code/ms-scout/.claude/worktrees/hungry-rhodes-5b70a0/`

---

## Problem in One Sentence

The Scout weekly digest renders 9 scoreable offers but presents them in a way that (a) strips all markdown formatting from the analyst's scoring rationale, (b) hides every signal that would let a human judge whether to trust the recommendation, and (c) gives the reviewer no way to react except "Queue" or "Skip" — neither of which has any triage context attached.

---

## Context

Scout Signal Phase A just landed (PR #164 Impact payout fix, PR #165 Tier 4 CVR baseline, CJ env var set). Pipeline now produces offers with verified payouts and scoreable CVR context. The next problem is the digest itself: even with accurate data, the Slack output doesn't communicate *why* an offer was ranked, making every card look identical regardless of whether it's a Tier 1 (MS-validated CVR) or Tier 4 (global baseline, confidence=0.35) recommendation.

**The 9-card digest after Phase A fixes was read but not triaged.** The reason: the digest doesn't pass the "reasonable person can trust and evaluate" bar — signals are invisible, formatting is broken, and the triage affordances are inadequate for the Phase B goal (observed triage sessions generating labeled data).

---

## Current State: Audit Findings

### What the digest currently shows per offer card:

```text
[section/fields]
  *Advertiser Name*               $12.50 CPS
  Electronics · Impact             US Only

[rich_text_quote]
  We've run this before — try it. ← markdown stripped, flat text

[actions]
  [Add to Queue]  [Skip]
```

### 9 design gaps identified:

| # | Gap | Root cause | Impact |
|---|-----|-----------|--------|
| G1 | Markdown stripped from why text | `re.sub(r'[*_]', '', why)` before `rich_text_quote` | Tier 1 ("*$3.40 RPM* at *2.1% CVR*") reads as flat noise |
| G2 | Scout Score / RPM never shown | No RPM in card render at all | Primary ranking signal invisible to reviewer |
| G3 | All tiers look identical | No confidence gradient shown | Tier 1 indistinguishable from Tier 4 (confidence=0.35) |
| G4 | fit_tier computed but hidden | `fit_tier` field exists, never rendered on main cards | PRIME vs WEAK context lost |
| G5 | "NEW THIS WEEK" visually buried | Tiny context block with comma-separated names | New offers don't get special visual treatment |
| G6 | Payout upgrade cards have no actions | `_build_sourcing_intel_blocks()` renders plain mrkdwn | "Payout went up" is actionable but can't be acted on |
| G7 | Skip has no friction | Fat-finger suppresses offer for weeks; no confirmation | Accidental skips permanently suppress good offers |
| G8 | Summary header is a dense wall | Single `section/mrkdwn` with all stats concatenated | "4,662 offers scored · 9 qualifying · 3 networks" reads as noise |
| G9 | No "Investigate" action | Only Queue or Skip | No way to flag "interesting but need to verify" |

---

## Proposed Changes (7 steps)

### Step 1 — Restore markdown in `why` text (5 min)

**File:** `scout_digest.py` — `_build_offer_card_blocks()` (line ~788)

**Current:** builds `rich_text_section` with `rich_text_quote`, strips `*` and `_` via `re.sub`.

**Change:** Replace the `rich_text_quote` block with a `section`/`mrkdwn` block using `>` blockquote prefix. Remove the `re.sub(r'[*_]', '', why)` call — pass `why` text unchanged.

```python
# BEFORE (approx):
why_clean = re.sub(r'[*_]', '', why)
blocks.append({
    "type": "rich_text",
    "elements": [{"type": "rich_text_quote", "elements": [{"type": "text", "text": why_clean}]}]
})

# AFTER:
blocks.append({
    "type": "section",
    "text": {"type": "mrkdwn", "text": f">{why}"}
})
```

**Outcome:** `*$3.40 est. RPM*` renders bold, `_Tier 1_` renders italic, `>` gives visual indentation.

---

### Step 2 — Add fit_tier badge + est. RPM to offer card fields (15 min)

**File:** `scout_digest.py` — `_build_offer_card_blocks()` (line ~788)

**Current left column:** `*Advertiser Name*`
**Current right column:** `$12.50 CPS`

**New left column:** `● PRIME  *Advertiser Name*` (where `●` maps to fit_tier emoji/dot)
**New right column:** `$12.50 CPS  ~$3.40 RPM`

fit_tier → symbol mapping (inline constant in the function):
- `PRIME` → `🔵`
- `STRONG` → `🟢`
- `STANDARD` → `⚪`
- `WEAK` → `🔴`
- (missing) → `⚫`

RPM display: computed from the offer's `scout_score` field (already populated). Format: `~$X.XX est. RPM`. Show if `scout_score > 0`, hide if zero/missing.

**No new data fetching** — `fit_tier` and `scout_score` are already on the offer dict from `offer_scraper.py`.

---

### Step 3 — Skip friction ephemeral ~~Add overflow menu~~ (shipped as skip friction)

**Status:** SHIPPED (as skip friction — overflow menu cut, see below)

**File:** `scout_handlers.py` — `_handle_reject()`

After every Skip, the handler posts an ephemeral confirmation:
> *OfferX* won't appear for ~3 weeks unless payout improves ≥15%.

This replaces the proposed overflow menu (⋮). The overflow menu was cut because Investigate/Copy URL/Remind are hollow without Phase C backend infrastructure — ephemeral confirmations provide the minimum viable friction and triage signal with zero new state.

**Not shipped — overflow menu:** No `overflow` action element in `_build_offer_card_blocks()`. No `overflow_action` handler in `scout_handlers.py`. No `remind_next_week` set in `scout_state.py`. Deferred to Phase C.

---

### Step 4 — Redesign "NEW THIS WEEK" section (20 min)

**File:** `scout_digest.py` — `build_digest_blocks()` (line ~655), `post_digest()` (line ~1497)

**Current:** Single `context` block with comma-separated advertiser names ("NEW: AdvertiserA, AdvertiserB, AdvertiserC from Impact, MaxBounty")

**New:** Per-network header context showing new count + names, using emoji network indicator:
```text
🆕  *3 new this week*
▸ Impact: RetailerX, BrandY
▸ MaxBounty: OfferZ
```

Implementation: group new offers by network, render one line per network with ▸ prefix. Use `section/mrkdwn` instead of `context` so the text isn't suppressed/gray.

---

### Step 5 — ~~Make payout upgrade cards actionable~~ (cut from this PR)

**Status:** CUT — payout upgrade records in `_build_sourcing_intel_blocks()` don't carry `offer_id` or `tracking_url` at this layer. Adding buttons requires upstream data plumbing. Deferred to Phase C.

---

### Step 6 — Split summary header into scannable stats (10 min)

**File:** `scout_digest.py` — `build_digest_blocks()` header section (line ~655)

**Current:** `"4,662 offers scored · 9 qualifying · 3 networks with results · 2 pipeline errors"`

**New:** Two-field section:
```text
Left:  📊 *4,662 scored*  ·  *9 qualifying*
Right: ✅ 3 networks  ·  ⚠️ 2 errors
```

Use `section` with `fields` array (2 elements). Errors field is only shown if error count > 0. The split makes the "errors" signal visually distinct from the "scored" signal.

---

### Step 7 — Smoke test updates (5 min)

**File:** `smoke_test.py`

Add tests:
- `test_offer_card_no_markdown_strip` — create an offer with `*bold*` in why text, run through `_build_offer_card_blocks()`, assert `*bold*` is present in output (not stripped)
- `test_offer_card_has_fit_tier_badge` — offer with `fit_tier="PRIME"`, assert `🔵` in rendered blocks
- `test_offer_card_has_rpm` — offer with `scout_score > 0`, assert `est. RPM` in rendered blocks
- `test_overflow_menu_present` — assert `overflow` action element in card blocks

---

## What Is NOT in This Plan (deferred / cut)

- **Overflow menu (⋮)** — cut entirely; no `overflow` element in cards, no overflow handler in `scout_handlers.py`, no `remind_next_week` state. Phase C.
- **Payout upgrade buttons** — cut; upgrade records lack `offer_id`/`tracking_url`. Phase C.
- **Phase B observation layer** (Slack reactions → `digest_decisions` ClickHouse table) — separate PR
- **Backend queue for Investigate** — v1 would be ephemeral only; real queue in Phase C

---

## Files Touched

| File | Changes |
|------|---------|
| `scout_digest.py` | Steps 1, 2, 4, 6 + post-open fixes: View button (`view_url`), truncation (78→160), Clearbit logo pipeline, `⚫` badge fix |
| `scout_handlers.py` | Step 3 (skip friction ephemeral) — no overflow handler added |
| `smoke_test.py` | Step 7 — 6 new deterministic tests (markdown, badge, RPM, skip friction, view button priority, no-URL case) |

---

## Success Criteria

- `python3 smoke_test.py` — green
- Force-trigger a test digest (`FORCE_DIGEST=1 python3 -c "from scout_digest import post_digest; post_digest()"` or equivalent) and confirm in `#sidd-qa`:
  - Why text renders with bold/italic formatting
  - Offer cards show fit_tier badge and est. RPM
  - Overflow menu appears on cards
  - NEW THIS WEEK section is per-network, not comma-list
  - Summary header is split into two fields

---

## Risks

- **Slack mrkdwn `>` blockquote** renders in desktop/web but may not render in mobile. Acceptable — desktop is primary triage surface.
- **Overflow menu** requires `actions` block in Bolt event routing. The existing `@app.action()` handler in `scout_handlers.py` handles buttons; overflow needs an additional `@app.action("overflow_action")` handler. If missed, clicks are silently ignored.
- **fit_tier on older cached offers** — offers scraped before `_compute_fit_tier()` was added may not have `fit_tier`. Guard with `offer.get("fit_tier", "")` and use `⚫` symbol for missing.
