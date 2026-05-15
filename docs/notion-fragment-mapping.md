# Notion Fragment → Internal Activation API — Field Mapping

Audit date: 2026-05-14
Code state: post-PR #105 (commit fbc22a8)

## Overview

`_handle_approve(action, payload, web)` in `scout_handlers.py:592` is the single approval handler. It serves two upstream call paths that both emit `action_id == "scout_approve"`:

1. **Offer queue cards** — Block Kit `build_digest_blocks` in `scout_digest.py:737-746` (live since PR #23).
2. **Sourcing signal cards** — Block Kit builder in `scout_digest.py:1239-1249` (live since PR #105).

The handler reads the JSON `action.value` payload, calls `_fetch_brief_for_approve()` (which tries `draft_campaign_brief` from `scout_agent.py`, then falls back to the payload itself), and writes a Notion page via `_write_to_notion_queue` in `scout_notion.py:460`.

## Canonical Field Set

The plan's canonical fields, mapped against what code actually does today.

| Notion field | Source in payload / brief_data | Always present? | Notes |
|---|---|---|---|
| Advertiser name | `brief_data["advertiser"]` → page `Name` title + `Approved By` body line | Yes | Both paths populate `advertiser`. Notion `Name` property is composed: `"{advertiser} — {payout} · {network}"`. |
| Offer URL (tracking_url) | `brief_data["tracking_url"]` → body block under "Destination URL" | Yes | Written into page body as inline code block. **Not stored as a Notion property** — only in body markup. Sourcing path falls back to `deep_link_url` if `tracking_url` empty (`scout_digest.py:1246`). |
| Payout amount | `brief_data["payout_num"]` → Notion property `Payout` (number) | Yes (when numeric) | Empty property dropped if `payout_num == 0`. |
| Payout type | `brief_data["payout_type"]` → Notion property `Payout Type` (select), defaulted to `"CPA"` | Yes | Both paths normalize via `_normalize_payout_type()` before serializing. Handler defaults missing values to `"CPA"`. |
| Category / vertical | `brief_data["category"]` → body line `"Category: {value}"` | Conditional (only if present) | **Now uniform after this audit.** Sourcing path always did `.split(",")[0].strip()`; offer-queue path passed raw. Normalization moved into `_handle_approve` (see "Code edits made"). **Not stored as a Notion property** — only in body. |
| Fit tier (Prime/Strong/Directional) | Not in payload, not in `brief_data`, not written | **Missing — recommend adding** | Sourcing cards display `o.get("fit_tier")` at digest render time (`scout_digest.py:1232`), but the value is NOT serialized into `action_value`. Offer-queue path has no `fit_tier` source. Per the plan: if absent, OMIT — don't default. To add: include `fit_tier` in both upstream `action_value` dicts in `scout_digest.py`, then surface it as a Notion property in `_write_to_notion_queue` (omit when empty). |
| Source tag (`"queue-approved"` / `"sourcing-approved"`) | `offer["source"]` after normalization in `_handle_approve` | Yes (defaulted) | **Now set in handler.** Sourcing builder tags `source: "sourcing_signal"` → handler maps to `"sourcing-approved"`. Offer-queue builder omits → handler defaults to `"queue-approved"`. **Currently not written as a Notion property** — recommend adding to `_write_to_notion_queue` properties dict. |
| Approved at timestamp | `now_iso = datetime.now(UTC)` → Notion property `Date Approved` (date) + body line | Yes | Uses `%Y-%m-%d` date in property (no time component). `_record_queued_offer` separately persists a full ISO datetime to `launched_offers.json` for lifecycle tracking — that timestamp is more precise but isn't on the Notion page. |
| Scout session_id | Not in payload, not in `brief_data`, not written | **Missing — recommend adding** | Slack block_action `payload` does carry container info but no Scout session_id. If session traceability is required for the activation API, the upstream `action_value` builders in `scout_digest.py` would need to include the digest's `session_id` at card-build time. |

### Additional fields actually written to Notion (not on plan's canonical list)

| Notion property | Source | Notes |
|---|---|---|
| `Status` (select) | Always `"Awaiting Entry"` at creation | State machine — updated to `"Live"` later by `_update_notion_status`. |
| `Network` (select) | `brief_data["network"]` (title-cased) | Dropped if empty. |
| `Scout Score RPM` (number) | `copy_data["rpm"]` (from `brief_data["scout_score_rpm"]`) | Dropped if zero. |
| `Approved By` (rich_text) | Resolved Slack display name (`real_name` → `display_name` → user_id) | Avoids raw `<@U…>` mention in Notion. |
| `Brief Link` (url) | `_slack_thread_url(channel, message_ts)` | Dropped if empty. |

Body content (`children`) also includes: Campaign Config (internal name, partner name, advertiser, destination URL, goal type, payout, optional goal title), Platform Settings (network, network offer ID), Scout Intelligence (RPM, performance context, category, risk note, approval line, brief thread bookmark), and Copy (AI-generated callouts or pending placeholder).

## Origin Tag

Both call paths route through `_handle_approve`. After this audit, the handler normalizes `offer["source"]` into one of two canonical values:

- `"queue-approved"` — offer queue card button (`build_digest_blocks` in `scout_digest.py`)
- `"sourcing-approved"` — sourcing signal card button (sourcing intel builder in `scout_digest.py`)

**Vamsee — recommended treatment in the internal activation API:**

- Both origins should hit the same activation endpoint with the same JSON shape.
- The `source` field is metadata only — not for routing logic. Activation rules (creative checks, payout validation, geo gating) apply identically.
- Use `source` for reporting only: "what % of approved offers came from sourcing intel vs operator-curated queue?" Surface this on the activation dashboard.
- Do NOT split into two pipelines. The whole point of PR #105 was unification.

## Suggested Internal API Contract

When pulling an approved offer's fragment from Notion (today's state — properties only):

```json
{
  "notion_page_id": "abc123...",
  "advertiser": "TruthFinder",
  "name": "TruthFinder — $2.50 · Impact",
  "status": "Awaiting Entry",
  "network": "Impact",
  "payout": 2.50,
  "payout_type": "CPL",
  "scout_score_rpm": 18.40,
  "date_approved": "2026-05-14",
  "approved_by": "Siddharth Shah",
  "brief_link": "https://momentscience.slack.com/archives/.../p..."
}
```

**Fields the activation API needs but are NOT currently on Notion properties** (would have to be parsed from body text or added as properties):

- `tracking_url` — in body block, not a property
- `category` — in body block, not a property
- `source` — not written at all (handler now normalizes but `_write_to_notion_queue` doesn't surface it)
- `fit_tier` — not serialized anywhere in the approval path
- `offer_id` (network's program ID) — in body block, not a property
- `scout_session_id` — not captured anywhere

**Recommendation:** add `Source`, `Category`, `Offer URL`, `Network Offer ID`, and `Fit Tier` as Notion properties on the queue DB. That makes the fragment self-describing without body-text parsing.

## Verification

To confirm a single offer's fragment is well-formed:

1. Approve an offer via queue card in `#bot-qa` (offer-queue path).
2. Approve a sourcing signal card in `#bot-qa` (sourcing path).
3. Compare the resulting Notion fragments — they should be structurally identical except for the (proposed) `Source` property.

Today the structural-identity holds for everything `_write_to_notion_queue` actually writes (Name, Status, Network, Payout, Payout Type, Scout Score RPM, Date Approved, Approved By, Brief Link). After this audit's handler-side normalization, the body-text `Category` line is also identical across paths (first-comma value, stripped).

## Open Questions for Vamsee

1. **Should `source` (`queue-approved` / `sourcing-approved`) become a first-class Notion property on the Scout Demand Queue DB?** Right now it's normalized in the handler but never reaches Notion. If yes, who owns adding the property in the Notion UI before we update `_write_to_notion_queue`?
2. **Is `fit_tier` (Prime/Strong/Directional) required for activation, or nice-to-have?** Sourcing cards have it at display time but it's not serialized. If required, we need to (a) add it to `action_value` in both upstream builders and (b) add a Notion property — and confirm whether the offer-queue path can compute `fit_tier` retroactively or should leave it empty.
3. **Does the activation API need `scout_session_id` for traceability, or is `Brief Link` (the Slack thread URL) sufficient as an audit anchor?** If session_id is required, the digest builders need to embed it in `action_value` at card-build time — it's not currently anywhere in the approval path.
