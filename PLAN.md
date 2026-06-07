<!-- /autoplan restore point: /Users/siddharthshah/.gstack/projects/AdsPostX-ms-scout/feat-scout-file-upload-autoplan-restore-20260607-080758.md -->
# Scout Attachment Ingestion — Gordon Plan (v2.2)

**Date:** 2026-06-07
**Branches:**
- `refactor/scout-ask-extract` (PR-A — refactor, ships first)
- `feat/scout-file-upload` (PR-B — feature, depends on PR-A; current worktree)

**Status:** DRAFT v2.2 — Slack-search reframe (v2), /autoplan security & test hardening (v2.1), trust-rebuild 2-PR split (v2.2). Awaiting Phase 0 (Slack scope) + PR-A merge before PR-B APPLY.

---

## Ship Strategy — 2-PR split (trust-rebuild rationale)

Scout has earned a reputation for fragility (last 3 production breaks were small-change-broke-hot-path failures). The /autoplan review identified `ask()` modification as the only real risk source in this feature. The team's confidence won't be rebuilt by a quiet successful ship — it's rebuilt by **visible discipline**: don't touch hot paths without first making them safe to touch.

**Ship strategy:**

| PR | Scope | Behavior change? | Risk |
|---|---|---|---|
| **PR-A** | Refactor `ask()` — extract `_build_initial_messages()` + `_run_tool_loop()`. `ask()` becomes thin wrapper. | **NONE** (proven by existing smoke tests) | Near-zero — pure refactor with full test coverage |
| **PR-B** | New `scout_attachments.py`, handler wiring, NEW `ask_with_attachment()` that uses PR-A's helpers. `ask()` is NOT modified. | Adds attachment ingestion | Low — confined to new code paths |

**What this kills:** the AC-9 contract ("no regression on text-only @mentions") becomes **structurally guaranteed** instead of contractually promised. `ask()` is not touched in PR-B; therefore text-only behavior cannot regress.

**What this costs:** ~1-2h extra refactor work, two PRs to review instead of one, ships in ~2 days instead of ~1.

**Why this is worth it specifically now:** Scout's CLAUDE.md Engineering Principle P1 (boundary validation) and P5 (self-healing systems) call for exactly this discipline. The pattern also creates a portable precedent: next attachment-like feature (Notion docs, Loom transcripts, future MCP inputs) follows the same refactor-then-feature shape. Trust compounds.

---

## The Real JTBD Signal (verified)

**Source:** [#revenue-operations, 2026-04-03 14:14 PDT, msg `1775250881.595939`](https://momentscience.slack.com/archives/C06F1RPPVBL/p1775250881595939)

Gordon @mentioned Scout with a **Google Sheets URL** (not a file attachment) and a multi-step ask:

1. Ingest data from a linked Google Sheet
2. Cross-reference click-ids in column D against ClickHouse `adpx_conversionsdetails`
3. Populate device type into column M; add a note in column N for missing conversions

Sidd's reply at the time: *"scout def aint setup to ingest and analyze… interesting use case, will noodle with this."*

Gordon's revealing follow-up: *"I've not connected clickhouse to claude, I should probably do that."*

**Translation of the actual JTBD:** "I want Claude-with-ClickHouse access from my environment, and Scout was the closest substitute available." Scout-in-Slack is Gordon's only practical AI surface; expanding it to ingest tabular data he points at (URL OR attachment) extends that without forcing him out of Slack.

---

## Problem in One Sentence

Scout cannot ingest tabular or document data that arrives with an @mention — neither via Google Sheets URLs nor via Slack file attachments — leaving users on locked-down environments without a way to mix outside data with Scout's ClickHouse access.

---

## Goal

When a user @mentions Scout with either (a) a Google Sheets URL in the message text OR (b) a file attachment, Scout fetches the content, extracts text/data, and uses it as context for the answer. Single source per @mention in v1. **No write-back to external systems** (read-only).

---

## Acceptance Criteria

**AC-1 — Google Sheets URL (the Gordon case):** @mention contains a Google Sheets URL + a question → Scout fetches via `export?format=csv`, summarizes shape (rows, columns, head, describe), answers using the data.

**AC-2 — Auth-gated Sheets:** Sheet requires login → friendly error: "I couldn't access that sheet — share it as 'anyone with the link can view' and try again." No partial answer.

**AC-3 — PDF brief analysis:** @mention with PDF + question → Scout extracts text via `pdftotext`, answers referencing PDF content.

**AC-4 — Image / screenshot:** @mention with PNG/JPG → Scout passes to Claude as a vision content block; answer references visible content.

**AC-5 — CSV file attachment:** @mention with `.csv` attached → same extraction path as Sheets URL (shape + head + describe).

**AC-6 — Unsupported type:** unknown mimetype OR a non-Sheets URL with no file → answer not blocked; one-line note: "Couldn't read attachment.zip (type not supported yet)" / "I only handle Google Sheets URLs right now — paste the data inline or attach a CSV."

**AC-7 — Size cap:** file > 10MB OR sheet CSV > 10MB → friendly rejection; no ask() call with content.

**AC-8 — Download failure:** Sheets fetch / Slack `url_private` returns 4xx/5xx → log + text-only fallback + one-line note.

**AC-9 — No regression:** text-only @mentions with no URL and no file → behavior identical to today (zero latency, zero shape change in `ask()` inputs).

**AC-10 — No write-back:** Gordon's original ask included "populate column M" — explicitly out of scope. Scout's answer says "I can read your sheet but can't write back to it. Here's what I found:" then proceeds with read-only analysis.

---

## Files Touched

| File | Change | Risk |
|---|---|---|
| `scout_attachments.py` | **NEW** — unified dispatch: URL detector + extractors (Sheets, PDF, CSV, image, text) | Low (isolated) |
| `scout_handlers.py` | Pre-`_ask_with_timeout`: check `event.files[]` AND scan `event.text` for Sheets URLs; pass extracted content through | Medium (touches DM + mention paths) |
| `scout_agent.py` | NEW `ask_with_attachment()` accepts `attached_text` + `attached_image`; image becomes vision content block. `ask()` is NOT modified (v2.2 boundary) | Low (new code path) |
| `smoke_test.py` | 3 new tests (Sheets URL wiring, file wiring, size-cap rejection) | None (additive) |
| `requirements.txt` | `pdfplumber` as `pdftotext` fallback (if not already present) | Low |
| `CLAUDE.md` | Document new `files:read` Slack scope; document Sheets share requirement | None |

---

## Implementation Plan

---

# PR-A — Refactor `ask()` (ships first, pure refactor, no behavior change)

**Branch:** `refactor/scout-ask-extract` (new worktree from `main`)
**Effort:** ~1-2h
**Risk:** Near-zero — behavior identical, covered by existing smoke tests

## Goal

Extract two helpers from `ask()` in `scout_agent.py` so future features (PR-B and onward) can compose the message-construction step without modifying the tool-use loop:

1. `_build_initial_messages(user_message, history, prefix_ctx) -> list[dict]` — builds the Anthropic `messages` array (date/caller/channel/corrections prefix + history + final user turn). Pure function.
2. `_run_tool_loop(messages, client, system_prompt, intent_name, intent_dict, ask_tools, _start_ms, _tools_called) -> AskResult` — runs the 12-round tool-use loop, returns AskResult.

`ask()` becomes a thin wrapper:
```python
def ask(user_message, history=None, user_id="", permalink="", user_tz="", thread_ts=""):
    # ... existing deterministic pre-router + client setup + prefix context build ...
    messages = _build_initial_messages(user_message, history, prefix_ctx)
    return _run_tool_loop(messages, client, SYSTEM_PROMPT, intent_name, intent_dict, _ask_tools, _start_ms, _tools_called)
```

## Acceptance Criteria (PR-A only)

**PR-A-AC-1:** All existing `smoke_test.py` tests pass without modification.

**PR-A-AC-2:** Calling `ask("revenue MTD")` returns the same `AskResult.text` shape as before (no behavior diff).

**PR-A-AC-3:** Calling `ask("revenue MTD", history=[{"role":"user","content":"q"},{"role":"assistant","content":"a"}])` returns the same result as before (thread history path unchanged).

**PR-A-AC-4:** Code review confirms the diff is purely structural: lines moved, not changed. Zero new logic.

## Tasks (PR-A)

### R1: Identify the extraction boundaries in `ask()` (~15 min)
- Read `scout_agent.py` `ask()` body (~L5783-end-of-function)
- Mark the boundary: everything from "messages = list(history or []) + [{...}]" up to but NOT including the tool-use loop entry → goes into `_build_initial_messages`
- Mark the boundary: the tool-use loop itself (the `while _round < MAX_ROUNDS` block) → goes into `_run_tool_loop`
- Confirm: the deterministic pre-router, client setup, prefix context build, intent classification, and the post-loop AskResult construction stay in `ask()`

### R2: Extract `_build_initial_messages` (~20 min)
- Move the message-construction code into a module-level function
- Signature: `def _build_initial_messages(user_message: str, history: list | None, prefix: str) -> list[dict]`
- Pure function: no side effects, no I/O, deterministic
- `ask()` calls it: `messages = _build_initial_messages(user_message, history, prefix)`
- **Also extract `_build_prefix_context(user_id: str, user_tz: str) -> str`** — the date/caller/channel/corrections concatenation. Pure function. Required by `ask_with_attachment` in PR-B, so it must be a callable module-level helper, not inlined.
- Verify with `python -c "from scout_agent import _build_initial_messages, _build_prefix_context; print('imports ok')"`

### R3: Extract `_run_tool_loop` (~30 min)
- Move the `while _round < MAX_ROUNDS` block + all tool dispatch + result accumulation into a module-level function
- Signature: `def _run_tool_loop(messages, client, system_prompt, intent_name, intent_dict, ask_tools, _start_ms, _tools_called, _brief_results=None, _opportunity_offers=None, _all_tool_results=None) -> AskResult`
- All previously-closure-scoped variables become explicit parameters
- `ask()` calls it: `return _run_tool_loop(messages, client, SYSTEM_PROMPT, _intent_name, _intent_dict, _ask_tools, _start_ms, _tools_called)`

### R4: Verify behavior identical (~15 min)
- Run `python smoke_test.py` — full suite passes
- Run `python -m smoke_test test_ask_status` — single-test focused run
- Run `python -m smoke_test test_ask_tool_call` — tool-use path verified

### R5: Open PR-A (~10 min)
- Title: `refactor(scout): extract _build_initial_messages + _run_tool_loop from ask()`
- Body references this plan + notes "zero behavior change, prepares for PR-B (file/URL ingestion)"
- Merge after CI green + one-line approval

## Boundaries (PR-A)

**DO NOT CHANGE:**
- `ask()`'s deterministic pre-router (`_route_deterministic`)
- The prefix context construction (date_ctx, caller_ctx, channel_ctx, corrections_ctx)
- The intent classifier (`_classify_intent`)
- ANY tool function in `TOOL_MAP`
- ANY public symbol of `scout_agent.py`

**SCOPE LIMITS:**
- This PR adds NO new functionality
- This PR fixes NO bugs (even if you find one — open a separate issue)
- This PR adds NO tests (existing smoke_test.py is the proof)

## Verification (PR-A)
- [ ] `python smoke_test.py` — all tests pass, output identical to pre-refactor
- [ ] `git diff main -- scout_agent.py` shows ONLY structural changes (no `+ if ` / `+ for ` / `+ return ` outside of the new function bodies)
- [ ] One-line code review confirms "refactor only, no logic change"

---

# PR-B — Feature Implementation (depends on PR-A merged)

The original phases (now numbered as PR-B phases) follow. **`ask()` is NOT modified in PR-B.** Phase 3 below is restructured to use PR-A's helpers.

### Phase 0 (Prerequisite — manual gate): Add `files:read` to Slack app

**Who:** Sidd
**Where:** api.slack.com/apps → Scout app → OAuth & Permissions → Bot Token Scopes → add `files:read` → reinstall
**Why a gate:** without the scope, `url_private` returns 403 and file-attachment path is dead. Sheets path works without it (no Slack auth needed for public sheets).
**Signal:** "scope added"

---

### Phase 1: scout_attachments.py — unified extraction module (~1.5h)

**Public surface:**
```python
@dataclass
class AttachmentResult:
    kind: Literal["text", "image", "unsupported", "auth_required", "too_large", "error"]
    source: Literal["file", "sheets_url"]
    name: str
    text: str | None = None
    image_b64: str | None = None
    image_media_type: str | None = None
    error: str | None = None

def detect_sheets_url(text: str) -> str | None:
    """Returns the first Google Sheets URL in text, or None."""

def extract_sheets_url(url: str) -> AttachmentResult:
    """Fetch via export?format=csv; treat as CSV; cap 30K chars."""

def extract_file(file_obj: dict, bot_token: str) -> AttachmentResult:
    """Slack event.files[i] dict; dispatch by mimetype."""
```

**Sheets URL handling:**
- Regex: `https?://docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+)` — extract sheet ID
- Optional `gid` param for specific tab: `[?&]gid=(\d+)`
- Build export URL: `https://docs.google.com/spreadsheets/d/{ID}/export?format=csv` (append `&gid={gid}` if present)
- Anonymous GET via `urllib.request` — no auth header
- 200 → parse via pandas, emit "Shape + Columns + Head + Stats" summary, cap 30K chars
- 401/403/302-to-login → `kind="auth_required"` (Sheets redirects unauth requests to a login page)
- Detect login redirect: response URL contains `accounts.google.com` OR body starts with `<!DOCTYPE html>` → treat as auth_required
- Other failures → `kind="error"`

**File dispatch (same as v1 plan):**
- PDF (`application/pdf`) → `pdftotext` via subprocess (CLAUDE.md global rule); fallback `pdfplumber`. Cap 30K chars.
- CSV → pandas `read_csv`; same summary format as Sheets path (DRY: shared `_summarize_dataframe(df)` helper).
- Plain text / JSON / markdown → read raw, cap 30K chars.
- Image (`image/png|jpeg|gif|webp`) → base64-encode raw bytes, cap 5MB pre-encode.
- Anything else → `kind="unsupported"`.

**Hard gates:**
- Size guard BEFORE download: file `size > 10MB` → `kind="too_large"` (no fetch).
- Sheets size guard: stream first N bytes; if content-length header > 10MB → reject.
- Module-level constants for caps (one place to tune).

**Verify:** `python -c "from scout_attachments import detect_sheets_url, extract_sheets_url, extract_file; print('imports ok')"` + REPL with public Sheets URL fixture + PDF/CSV/PNG fixtures.

---

### Phase 2: Wire detection + extraction into _handle_event_impl (~1.5h)

In `scout_handlers.py`, after `is_mention` / `is_dm` detection (~L2213) and BEFORE history-building (~L2637):

1. `files = event.get("files") or []`
2. `sheets_url = scout_attachments.detect_sheets_url(event.get("text", ""))`
3. **Priority:** if `files` exists → process file. Else if `sheets_url` → process Sheets. (Don't process both; if both present, mention "I see both a file and a Sheets URL — using the file" and process the file.)
4. Call `extract_file(...)` or `extract_sheets_url(...)`; get `AttachmentResult`.
5. Build three locals:
   - `attached_text: str | None`
   - `attached_image: dict | None`
   - `attachment_note: str | None` (for unsupported / auth_required / too_large / error / extra-content cases)
6. For `kind="too_large"` or `kind="auth_required"`: post friendly response immediately, do NOT call ask().
7. Otherwise pass `attached_text=..., attached_image=...` through `_ask_with_timeout` at BOTH call sites (L2709 DM, L2825 mention).
8. If `attachment_note` exists AND ask() returned successfully → prepend as italic context line above the main answer.
9. **Write-back guard (AC-10):** if `source="sheets_url"` AND `attached_text` is not None, prepend a sentinel line into `attached_text` itself: `"NOTE: read-only — Scout cannot write back to this sheet."` Tells Claude not to claim it'll update column M even if Gordon asks.

**Do NOT** pass file/sheet content into the thread-history layer — per-turn, not per-thread.

**Verify:** manual @mention in #scout-test with a small public Google Sheet + question → Scout's reply references actual sheet content.

---

### Phase 3: NEW `ask_with_attachment()` — uses PR-A helpers, does NOT modify `ask()` (~45m)

**Critical architectural shift (post-/autoplan trust-rebuild decision):** `ask()` is NOT modified in PR-B. Instead, a new function `ask_with_attachment()` composes PR-A's helpers (`_build_initial_messages` + `_run_tool_loop`) with attachment-aware message construction.

Add to `scout_agent.py`:

```python
def ask_with_attachment(
    user_message: str,
    history: list | None = None,
    user_id: str = "",
    permalink: str = "",
    user_tz: str = "",
    thread_ts: str = "",
    attached_text: str | None = None,
    attached_image: dict | None = None,
) -> AskResult:
    """Variant of ask() that supports per-turn attached content (file or sheet).
    Falls back to ask() when no attachment is present, so callers can use this
    unconditionally without paying any cost when there's no attachment.
    """
    # No attachments → delegate to vanilla ask(), zero new code path
    if attached_text is None and attached_image is None:
        return ask(user_message, history=history, user_id=user_id,
                   permalink=permalink, user_tz=user_tz, thread_ts=thread_ts)

    # Same setup as ask() — pre-router check, client, prefix context, intent
    _start_ms = time.monotonic()
    _tools_called: list = []
    _routed = _route_deterministic(user_message, user_id)
    if _routed is not None:
        return _routed  # control-surface verbs never attach files

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"),
                                  default_headers={"anthropic-beta": "prompt-caching-2024-07-31"})
    prefix = _build_prefix_context(user_message, user_id, user_tz)  # date/caller/channel/corrections
    _intent_name, _intent_dict = _classify_intent(user_message, thread_ts=thread_ts or None)
    _ask_tools = TOOLS

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

    return _run_tool_loop(messages, client, SYSTEM_PROMPT, _intent_name, _intent_dict,
                          _ask_tools, _start_ms, _tools_called)
```

**Why this is safer than modifying `ask()`:**
- Zero diff to `ask()` — its tests prove it works, and its tests still run
- AC-9 is **structurally guaranteed**: text-only callers continue calling `ask()` directly via `_ask_with_timeout`; only attachment-bearing callers route through `ask_with_attachment`
- `_build_prefix_context` is a small helper extracted from `ask()` in PR-A as part of R2 (so it's a pure function reusable here)
- The image content-block construction is contained in ONE place, exercised by ONE smoke test

**Handler change (already specified in Phase 2):** when attachments are present, call `ask_with_attachment(...)`; else call `ask(...)` as today. The `_ask_with_timeout` wrapper accepts either via `**kwargs`.

**Avoid:** putting attached content into `history` (per-turn). Avoid passing image as base64 inside a text string.

**Verify:** `python -c "from scout_agent import ask_with_attachment; print(ask_with_attachment.__doc__)"` + smoke_test from Phase 5 passes.

**Note:** the small extraction of `_build_prefix_context` (the date/caller/channel/corrections concatenation) is added to PR-A's R2 since it's a pure function. If we discover during PR-A review that this extraction is contentious, fall back to inlining the prefix construction inside `ask_with_attachment()` — costs ~5 duplicated lines, doesn't change PR-B's risk profile.

---

### Phase 4: _ask_with_timeout passthrough verification (~15m)

`_ask_with_timeout` already uses `**kwargs` (L116). Confirm passthrough — zero code change, just verification + a comment block documenting supported kwargs.

**Verify:** `grep -n "attached_text\|attached_image" scout_handlers.py` — appears at both DM and mention sites.

---

### Phase 5: Smoke tests (~1.5h)

Add to `smoke_test.py`:

**`test_sheets_url_wiring`:** (the Gordon case)
1. Mock `urllib.request.urlopen` for `docs.google.com/spreadsheets/.../export?format=csv` returning a CSV with known marker text `"GORDON_FIXTURE_MARKER"`.
2. Mock `scout_agent.ask` to capture kwargs.
3. Synthetic `event` with `text="@scout analyze https://docs.google.com/spreadsheets/d/FAKE_ID/edit"`.
4. Assert `ask` called with `attached_text` containing `"GORDON_FIXTURE_MARKER"` AND the read-only sentinel.

**`test_sheets_auth_required`:**
1. Mock urlopen to return a login redirect (response URL contains `accounts.google.com`).
2. Assert `extract_sheets_url` returns `kind="auth_required"`.
3. Assert handler posts friendly message and does NOT call ask().

**`test_file_attachment_wiring`:** (the original)
1. Minimal in-memory PDF fixture with `"MOMENTSCIENCE_FIXTURE_KEYWORD"`.
2. Mock urlopen for Slack url_private.
3. Synthetic event with `files=[{...}]`.
4. Assert ask called with `attached_text` containing the keyword.

**`test_attachment_too_large`:**
- file_obj with `size=20MB` → expect `kind="too_large"` without fetch (mock urlopen with `side_effect=AssertionError`).

Follow existing `test()` decorator pattern.

**Verify:** `python smoke_test.py` — all pass; 4 new tests show as PASS.

---

### Phase 6: CLAUDE.md documentation (~15m)

Add to Scout's CLAUDE.md:
- **New Slack scope:** `files:read` — needed for file-attachment ingestion
- **New behavior:** Scout reads Google Sheets URLs in @mentions; sheet must be shared as "anyone with the link can view"
- **Explicit non-feature:** Scout cannot write back to Google Sheets

Keep edits surgical.

**Verify:** `grep -n "files:read\|Google Sheets" CLAUDE.md` — appears with clear notes.

---

## Boundaries

**DO NOT CHANGE:**
- Thread-history building (`scout_handlers.py:2637-2700`) — file/sheet content is per-turn
- `scout_state.py` thread-context persistence — orthogonal
- `scout_bot.py` digest/alert daemons — attachment ingestion is interactive only
- ClickHouse query layer — extracted content goes to Claude as context, never into SQL
- `_smart_history` trimming — unrelated

**SCOPE LIMITS:**
- One source per @mention (file OR sheet, not both)
- Google Sheets only (no Excel Online, no Airtable, no generic CSV URLs in v1 — defer until asked)
- Public/link-shared sheets only (no service-account OAuth in v1)
- **No write-back to any external system** — explicit in AC-10, surfaced to user via sentinel line
- No persistent storage of fetched content
- No App Home file upload (Slack doesn't support file pickers there)
- No OCR for image-only PDFs
- No multi-tab Sheets ingestion (first tab or specified `gid` only)

---

## Verification Checklist

Before declaring complete:
- [ ] `python smoke_test.py` — all tests pass, 4 new ones included
- [ ] Live: @mention Scout with a public Google Sheet URL + question → answer references actual sheet content
- [ ] Live: @mention Scout with a private/auth-required Sheet → friendly auth_required message, no partial answer
- [ ] Live: @mention with a 15MB sheet (force size cap) → friendly rejection
- [ ] Live: @mention with a PDF → answer references PDF content
- [ ] Live: @mention with a PNG screenshot → answer references visible content
- [ ] Live: @mention with NO file and NO URL → existing behavior unchanged (timing + shape)
- [ ] Render logs show `ask: attached_text=..., source=...` breadcrumbs only for ingestion turns
- [ ] CLAUDE.md mentions `files:read` AND Sheets share requirement AND no write-back

---

## Success Criteria

1. **The real Gordon case works end-to-end:** he can @mention Scout with a Sheets URL + a ClickHouse cross-reference question, and get an answer without leaving Slack
2. No regression on text-only @mention latency (zero work when no file AND no Sheets URL)
3. No new persistence layer added
4. No write-back to external systems (security posture preserved)
5. All 10 ACs verified
6. Plan executed inside one PR on `feat/scout-file-upload`

---

## Risk Summary

| Phase | Risk | Mitigation |
|---|---|---|
| 0 | Slack scope mismatch | Manual checkpoint with explicit signal |
| 1 | Sheets export URL behavior varies by share setting | Login-redirect detection + auth_required path |
| 2 | Touches both DM + mention dispatch | Manual Slack test per AC |
| 3 | Modifies ask() — hot path | Defense-in-depth cap + log breadcrumb; smoke_test covers wiring |
| 4 | Pure verification | None |
| 5 | Additive tests | None |
| 6 | Docs only | None |

---

## Decision Audit (v2 reflects Slack-search findings)

| # | Decision | Classification | Rationale |
|---|---|---|---|
| 1 | Add Sheets URL ingestion as primary input path | One-way | Gordon's actual moment used a URL, not a file — verified via Slack search |
| 2 | Keep file attachment ingestion as parallel path | Two-way | Shares extraction layer; catches future "I attached a CSV" case for ~zero extra cost |
| 3 | No write-back to Google Sheets | One-way | Gordon de-escalated his own ask ("just curious what Scout would do"); write-back is a 2-day security rabbit hole |
| 4 | Read-only sentinel injected into `attached_text` when source=sheets_url | One-way | Prevents Claude from confidently claiming "I updated column M" when it can't |
| 5 | Public/link-shared sheets only | Two-way | Service-account OAuth = separate plan; Gordon's example was `?usp=sharing` (link-shared) |
| 6 | Single source per @mention (file OR sheet) | Two-way | Multi-source = follow-up if asked |
| 7 | pdftotext via subprocess (not pdfplumber primary) | One-way | Global CLAUDE.md rule |
| 8 | 10MB cap for both sources | Two-way | Tunable constant |
| 9 | Image goes as Anthropic content block, not text | One-way | Claude vision API requirement |
| 10 | `files:read` manual checkpoint (not auto) | One-way | Slack admin UI action |
| 11 | Content NOT injected into thread history | One-way | Per-turn context, not conversation memory |
| 12 | Smoke tests mock at urllib boundary | One-way | Fast, isolates Scout's own logic |
| 13 | Google Sheets only in v1 (no Excel/Airtable/generic CSV URLs) | Two-way | Match the verified JTBD signal exactly; expand on demand |

---

## v2.1 Updates from /autoplan Review

**Auto-decided (CONFIRMED CONCERN, both voices):**

### SSRF mitigation — added to Phase 1

When fetching Sheets URLs, `urllib.request` follows redirects by default and has no host validation. A malicious URL crafted to look like Sheets could redirect to internal services (Slack metadata IP `169.254.169.254`, RFC1918 ranges, link-local). Mitigation:

- Use a custom `HTTPRedirectHandler` that VALIDATES every hop's host
- Allowlist: hop hostnames must be in `{"docs.google.com", "accounts.google.com"}` — anything else → `kind="error"` with `error="redirect_blocked: {host}"`
- Block RFC1918 (10.*, 172.16-31.*, 192.168.*), link-local (169.254.*), localhost
- Max 3 hops; deeper = `kind="error"`
- Resolve the final URL's hostname via `socket.getaddrinfo` to enumerate ALL A/AAAA records (IPv4 + IPv6) and reject if ANY resolves to private/loopback/link-local/multicast — gethostbyname only checks one IPv4, allowing dual-stack hosts to bypass the check

Acceptance: a new smoke test `test_ssrf_redirect_blocked` asserts that a mock Sheets export returning a 302 to `http://169.254.169.254/` is rejected with `kind="error"`.

### Slack URL unwrapping — added to Phase 1 `detect_sheets_url`

Slack delivers URLs in `event.text` as `<https://docs.google.com/spreadsheets/d/ID/edit>` (auto-linkified) or `<https://docs.google.com/.../edit|My Sheet>` (named link). The regex must unwrap these before matching:

```python
# Strip Slack's <url> or <url|label> wrapping before regex match
unwrapped = re.sub(r'<(https?://[^|>]+)(?:\|[^>]*)?>', r'\1', text)
match = re.search(r'https?://docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+)', unwrapped)
```

Multi-URL handling: if multiple Sheets URLs match, use the FIRST. Document in code comment.

Defer until requested: handling `/d/e/{ID}/pub` (published) format vs `/d/{ID}/edit` (regular). v1 supports `/d/{ID}/edit` only.

### Image content-block smoke test — added to Phase 5

New test `test_image_content_block_shape`:
1. Synthetic event with a PNG attachment (1×1 transparent pixel, ~70 bytes).
2. Mock `urllib.request.urlopen` to return the PNG bytes.
3. Mock `scout_agent.ask` to capture the constructed `messages` array.
4. Assert: `messages[-1]["content"]` is a LIST (not a string), contains exactly 2 elements, first has `type="image"` with `source.type="base64"` and `source.media_type="image/png"`, second has `type="text"`.

This is the path most likely to fail silently — Anthropic's content-block format is exacting and getting it wrong returns a 400 that surfaces as a generic "ask failed" in Slack.

### pdftotext hardening — added to Phase 1

`pdftotext` subprocess on user-delivered bytes:
- Write bytes to a tempfile via `tempfile.mkstemp(suffix=".pdf")` — never pipe through stdin, never construct shell strings
- Run via `subprocess.run([...], timeout=10, capture_output=True, check=False)` — explicit list args, never `shell=True`
- Clean up tempfile in a `finally` block
- If `subprocess.TimeoutExpired` → `kind="error"` with `error="pdf_parse_timeout"` (don't kill the whole turn)

### `_smart_history` non-collision — documented in Phase 3

Subagent flagged: prepending `[Attached content...]` fence into `effective_message` would pollute the entity-extraction regex in `_smart_history`. **Analysis:** this is mitigated by the existing thread-history architecture. The fence is only used in THIS turn's `messages` array (in-memory, one turn). Future turns rebuild history from Slack via `conversations_replies` — Slack stores the USER'S original text, not Scout's fenced version. The fence never enters the entity extractor.

Add a code comment in Phase 3 documenting this so future contributors don't add a "let's cache the effective_message" optimization that would break the property.

---

## Taste decisions surfaced (require user judgment)

| # | Decision | Recommendation | Alternative |
|---|---|---|---|
| T1 | Collapse v1 to Sheets-URL-only OR keep both paths? | **Keep both** — extraction layer is shared, file path costs ~30 lines | Subagent argues N=1 doesn't justify aspirational PDF/image paths |
| T2 | One `scout_attachments.py` OR two modules (`scout_sheets.py` + `scout_files.py`)? | **One module** — they share dataframe summary + size cap + result shape | Subagent argues different I/O surfaces shouldn't pre-DRY |
| T3 | Reframe problem statement as N=1 (Gordon-specific) vs "users on locked-down environments" (plural)? | **Reframe to N=1** — be honest; speculation about other users belongs in success criteria, not problem statement | Keep plural framing as forward-looking |

---

## User Challenges (both voices recommend changing your stated direction)

**Challenge UC1: Help Gordon set up Claude Desktop + ClickHouse MCP instead of building Scout-side ingestion.**

- **You said:** Build Scout-side attachment ingestion because "Scout-in-Slack is Gordon's only AI surface."
- **Both voices recommend:** Spend 30 minutes setting up Claude Desktop + ClickHouse MCP for Gordon. He literally said "I should probably do that" in the original Slack thread. If he adopts it, this entire plan becomes unnecessary.
- **Why:** N=1 user-evidenced signal + Gordon explicitly named the alternative + Scout maintenance cost grows with every new ingestion path.
- **What we might be missing:** Gordon's corporate laptop may genuinely block Claude Desktop (firewall, antivirus, no admin rights). His ClickHouse access via Claude Desktop may not actually be reachable from his machine even with MCP. Sidd has context on his actual environment that the models don't.
- **If we're wrong, the cost is:** Sidd spends 30 minutes on Gordon's machine, discovers Claude Desktop works, kills this plan and saves ~5h. OR Claude Desktop doesn't work in his environment, Sidd wasted 30 minutes, plan proceeds.

**Challenge UC2: Read-only ships ~40% of Gordon's JTBD (he asked for write-back to columns M and N).**

- **You said:** No write-back, AC-10 explicit, ship read-only.
- **Both voices recommend:** Either ship nothing (write-back is the headline feature for Gordon) OR add a "copy-paste this CSV block back into your sheet" affordance so he can complete the loop manually.
- **Why:** Shipping read-only-with-apology trains Gordon (and the next 5 users) that Scout is incomplete for this workflow.
- **What we might be missing:** Sidd knows whether Gordon would actually use read-only analysis as a starting point or just bail. The "copy-paste back" affordance might be 10 lines, or might require its own design pass.
- **If we're wrong, the cost is:** Feature ships, nobody uses it because it doesn't close the loop. Or feature ships, Gordon uses the read-only output and is genuinely helped.

---

## Updated Risk Summary (post-review)

| Phase | Risk | Mitigation |
|---|---|---|
| 0 | Slack scope mismatch | Manual checkpoint |
| 1 | **SSRF via Sheets fetch redirects** | Allowlist + IP block + max-hops (NEW) |
| 1 | Sheets export URL behavior varies | Login-redirect detection |
| 1 | **Slack URL wrapping breaks regex** | Unwrap step (NEW) |
| 1 | **pdftotext on user bytes** | mkstemp + timeout + no shell (NEW) |
| 2 | Touches DM + mention dispatch | Manual Slack test per AC |
| 3 | Modifies ask() hot path | Defense-in-depth cap + log + smoke test |
| 5 | **Image content-block format unverified** | Dedicated shape test (NEW) |

---

## Locked Decisions (post-/autoplan, 2026-06-07)

User responded to the 5 surfaced decisions:

| # | Decision | Resolution | Rationale |
|---|---|---|---|
| UC1 | Help Gordon set up Claude Desktop + ClickHouse MCP first? | **REJECTED** | "If he would have been able to get Claude/ClickHouse there he would have by now." Gordon's corporate environment blocks both Claude Desktop and ClickHouse access — confirmed by 2 months elapsed since he said "I should probably do that" with no movement. Environment is the constraint, not motivation. Scout-in-Slack IS his only AI surface. |
| UC2 | Add "copy-paste CSV back" affordance for write-back? | **REJECTED** (v1) | Read-only is sufficient for v1; revisit if Gordon (or another user) explicitly asks for write-back help after ship. |
| T1 | Keep both Sheets URL + file paths? | **YES** | Shared extraction layer; ~30 extra lines; catches the next constrained user's case for free. |
| T2 | One unified `scout_attachments.py` module? | **YES** | They share dataframe summary + size cap + result shape; splitting now is premature abstraction. |
| T3 | Reframe problem statement as Gordon-specific (N=1)? | **YES** | Honest about verified signal; "users on locked-down environments" remains as a forward-looking success criterion, not a load-bearing premise. |

**Net effect:** plan ships as v2.1 with the 5 auto-decided security/test fixes baked in. All 5 surfaced decisions resolved. Ready for Phase 0 (Slack scope add) → APPLY.

---

## Next Step (v2.2)

Approved v2.2 with all decisions locked + 2-PR split for trust-rebuild:

**Step 1 — PR-A (refactor, ships first):**
```bash
cd ~/code/ms-scout
git worktree add .claude/worktrees/scout-ask-refactor -b refactor/scout-ask-extract origin/main
cd .claude/worktrees/scout-ask-refactor
# Execute tasks R1-R5 above
# Open PR, merge after CI green + 1-line review
```

**Step 2 — Phase 0 (manual Slack scope, can happen in parallel with PR-A):**
Add `files:read` in api.slack.com/apps. Reply "scope added" when done.

**Step 3 — PR-B (feature, depends on PR-A merged):**
```bash
cd ~/code/ms-scout/.claude/worktrees/scout-file-upload
git fetch origin main  # pull in PR-A's merge
git rebase origin/main  # current branch was cut from main pre-PR-A
# Execute PR-B Phases 1-6 above
# Open PR, merge after smoke_test + live verifies
```

Phase 0 (Slack scope) can be done in parallel with PR-A — they're independent.

---

## GSTACK REVIEW REPORT

**Generated:** 2026-06-07 by `/autoplan`
**Reviews run:** CEO (strategic) + Engineering (code architecture/security)
**Design review:** SKIPPED — no UI scope (Slack Block Kit additions are existing surface)
**DX review:** SKIPPED — Scout is internal team tooling, no developer-facing surface change
**Outside Voice:** Codex unavailable (rate-limited until Jul 2026), tagged `[codex-unavailable]` — Claude subagent ran as primary outside voice, tagged `[subagent-only]`

**Post-review architectural decision (v2.2):** Split into PR-A (pure refactor) + PR-B (feature) to eliminate the `ask()` modification risk identified in Eng review. Rationale: trust-rebuild context where visible discipline > quiet successful ship. AC-9 ("no regression on text-only @mentions") becomes structurally guaranteed instead of contractually promised.

### Auto-Decided Improvements (5)

| # | Phase | What | Principle |
|---|---|---|---|
| AD1 | Phase 1 | SSRF guards (allowlist + IP block + max-hops + DNS re-check) | P1 completeness, P5 explicit |
| AD2 | Phase 1 | Slack `<url\|label>` unwrap in detect_sheets_url | P1 completeness |
| AD3 | Phase 5 | Image content-block shape smoke test | P1 completeness |
| AD4 | Phase 1 | pdftotext hardening (mkstemp + timeout + no shell) | P5 explicit |
| AD5 | Phase 3 | Document _smart_history non-collision (architectural explainer) | P5 explicit (prevent future regression) |

### Taste Decisions Surfaced (3) — see "Taste decisions" section above
T1, T2, T3 — recommendations given, alternatives noted

### User Challenges Surfaced (2) — see "User Challenges" section above
UC1: Claude Desktop + ClickHouse MCP alternative
UC2: Read-only ships only ~40% of stated JTBD

### Consensus tables

**CEO:** 4/6 CONFIRMED CONCERN (premises, right-problem, alternatives, read-only-coverage), 2/6 DISAGREE → TASTE (scope, trajectory)
**Eng:** 4/6 CONFIRMED MUST-FIX (SSRF, Slack unwrap, image-block test, pdftotext), 1/6 ARCHITECTURE EXPLAINS (history collision), 1/6 DISAGREE → TASTE (module split)

### Decision Audit Trail

| # | Phase | Decision | Classification | Principle | Rationale |
|---|---|---|---|---|---|
| 1 | Eng | Add SSRF guards | Mechanical | P1, P5 | Real attack surface; non-negotiable |
| 2 | Eng | Slack URL unwrap | Mechanical | P1 | Plan would silently miss Slack's wrapping in production |
| 3 | Eng | Image content-block shape test | Mechanical | P1 | Most-likely-to-fail path was untested |
| 4 | Eng | pdftotext hardening | Mechanical | P5 | Cheap safety; subprocess on user bytes |
| 5 | Eng | Document _smart_history non-collision | Mechanical | P5 | Subagent concern mitigated by existing arch; document why |
| 6 | CEO | Scope: collapse v1 to Sheets-only? | Taste (T1) | — | Surfaced to user; recommend keep-both |
| 7 | Eng | Architecture: one module vs two? | Taste (T2) | — | Surfaced to user; recommend keep-unified |
| 8 | CEO | Reframe N=1 vs plural problem statement? | Taste (T3) | — | Surfaced to user; recommend reframe |
| 9 | CEO | Claude Desktop + MCP alternative for Gordon? | User Challenge UC1 — RESOLVED REJECTED | User context | Gordon's corporate env blocks Claude/ClickHouse; 2 months elapsed with no setup = he can't, not won't |
| 10 | CEO | Read-only vs full write-back loop? | User Challenge UC2 — RESOLVED REJECTED | P3 pragmatic | Read-only sufficient for v1; revisit on explicit ask |
| 11 | CEO | Keep both Sheets + file paths? | Taste T1 — RESOLVED YES | P2 boil lakes | Shared extraction; ~30 extra lines covers future user |
| 12 | Eng | One unified module? | Taste T2 — RESOLVED YES | P5 explicit | Premature abstraction to split now |
| 13 | CEO | Reframe N=1? | Taste T3 — RESOLVED YES | P1 completeness (honest framing) | Verified signal is N=1 |
| 14 | Eng | Modify `ask()` OR split into refactor PR + feature PR? | RESOLVED 2-PR SPLIT | P1 boundary validation, P5 self-healing | Trust-rebuild context: visible discipline > quiet successful ship. `ask()` modification was the ONLY real risk source identified by /autoplan; eliminating it by structural refactor makes AC-9 guaranteed instead of promised. ~1-2h extra cost for portable precedent. |

### Next Step

Respond to UC1, UC2 + pick T1, T2, T3. Then `/ship` to execute (Phase 0 scope still required first).
