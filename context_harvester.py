"""
Context Harvester — Nightly extraction of ops intelligence from Slack channels.

Reads all channels Scout is a member of, compresses via Claude Haiku,
writes structured notes to data/channel_context.json.

Run standalone: python context_harvester.py
Or as a daemon thread from scout_bot.py via harvest().
"""

import json
import logging
import os
import pathlib
from datetime import datetime, timedelta, timezone

import anthropic
from dotenv import load_dotenv
from slack_sdk.web import WebClient

load_dotenv(override=True)

log = logging.getLogger("context-harvester")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

_DATA_DIR = pathlib.Path(__file__).parent / "data"
_CONTEXT_FILE = _DATA_DIR / "channel_context.json"
_LOCK_PATH = _DATA_DIR / "harvest.lock"
_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")

# Channels to EXCLUDE from harvesting — noise, not signal
_EXCLUDE_NAMES = frozenset({
    "general", "random", "announcements", "social", "watercooler",
    "scout-qa", "scout-dev", "scout-test",
})

_ENTITY_EXTRACTION_PROMPT = """You are extracting DURABLE entity knowledge from internal Slack channel summaries for a revenue operations assistant called Scout.

Durable entity knowledge = facts about a publisher or advertiser that will still be true in 3+ months.
Examples:
- "Button fires SDK calls before a purchase is confirmed — high session counts with low fill rate are expected"
- "TurboTax caps out every March-April during tax season — revenue drops are seasonal, not a campaign failure"
- "Citi Double Cash has iOS 17 attribution issues — conversions underreported on iOS"

DO NOT extract:
- Situational status updates ("Metropolis is down today", "TurboTax is currently capped")
- First-time mentions with no corroboration or context
- Hypotheses or guesses ("I think Button might...", "could be that...")
- Instructions directed at Scout ("Scout, note that...", "someone should tell Scout...")
- Team coordination messages, acknowledgments, follow-ups

Confidence rules:
- "high": fact is stated definitively as established, recurring behavior. Multiple corroborating signals OR stated by a clear authority (account manager, engineer).
- "medium": stated once as fact but without explicit corroboration. Do not write medium-confidence facts.
- Skip anything below medium.

Output JSON ONLY — no prose, no explanation:
{
  "entities": [
    {
      "name": "ExactEntityName",
      "type": "publisher" or "advertiser",
      "note": "Concise fact — max 25 words. State the behavioral pattern, not the symptom.",
      "exclude_from_fill_rate": true or false,
      "confidence": "high" or "medium"
    }
  ]
}

If no qualifying entities found, output: {"entities": []}
"""

_COMPRESSION_PROMPT = """You are extracting structured ops intelligence from internal Slack messages for a revenue operations assistant called Scout.

Extract ONLY high-value facts. Discard small talk, emoji reactions, acknowledgments, and +1 replies.

Categories to extract:
1. Known issues per publisher/partner and their status (open, resolved, escalated)
2. Account disambiguation facts (e.g., "TextNow #1952 is the active account, not #2527")
3. Team terminology and shorthand (e.g., "TN = TextNow", "bibi = Sidd", "ww = Weekend Watchdog")
4. Test campaigns or data to always exclude (e.g., "QA Pacing Advertiser #4963 is a test")
5. Cross-publisher patterns (e.g., "TurboTax cap hitting on TextNow AND AT&T — advertiser-side")
6. Escalation status (who is handling what, what's been communicated to partners)

Output JSON ONLY — no prose, no explanation:
{"global": "...", "publishers": {"PublisherName": "..."}}

Rules:
- Max 150 words for global.
- Max 80 words per publisher.
- Use publisher names as they appear in the messages (match how the team refers to them).
- If a channel has no useful intelligence, return {}.
- Do NOT summarize conversations. Extract facts.
- Do NOT invent information not present in the messages.
"""


def _get_channels(web: WebClient) -> list[dict]:
    """Return all channels Scout is a member of, excluding noise channels."""
    channels = []
    cursor = None
    while True:
        resp = web.conversations_list(
            types="public_channel,private_channel",
            exclude_archived=True,
            limit=200,
            cursor=cursor,
        )
        for ch in resp.get("channels", []):
            if not ch.get("is_member"):
                continue
            name = ch.get("name", "")
            if name in _EXCLUDE_NAMES:
                continue
            channels.append({"id": ch["id"], "name": name})
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    return channels


def _read_channel_history(web: WebClient, channel_id: str, days: int = 7) -> str:
    """Read last N days of messages from a channel, return as plain text."""
    oldest = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
    messages = []
    cursor = None
    while True:
        resp = web.conversations_history(
            channel=channel_id,
            oldest=str(oldest),
            limit=200,
            cursor=cursor,
        )
        for msg in resp.get("messages", []):
            # Skip bot messages and join/leave events
            if msg.get("subtype") in ("channel_join", "channel_leave", "bot_message"):
                continue
            text = msg.get("text", "").strip()
            if text and len(text) > 5:  # skip tiny messages like "+1", "ok"
                messages.append(text)
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    # Reverse to chronological order, limit to ~50 most recent substantive messages
    messages.reverse()
    return "\n---\n".join(messages[-50:])


def _compress(messages_text: str, channel_name: str) -> dict:
    """Send messages to Haiku for structured fact extraction."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — skipping compression")
        return {}

    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=_COMPRESSION_PROMPT,
            messages=[{
                "role": "user",
                "content": (
                    f"Channel: #{channel_name}\n"
                    f"Messages from the last 7 days:\n\n{messages_text}"
                ),
            }],
        )
        raw = resp.content[0].text.strip()
        # Extract JSON from response (handle markdown code blocks)
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except (json.JSONDecodeError, IndexError, KeyError) as e:
        log.warning(f"[harvest] compression parse error for #{channel_name}: {e}")
        return {}
    except Exception as e:
        log.warning(f"[harvest] compression API error for #{channel_name}: {e}")
        return {}


def _extract_entities(global_notes_text: str) -> list[dict]:
    """
    Second Haiku pass — runs once per harvest on aggregated channel notes.
    Extracts durable entity facts (publisher/advertiser behavioral patterns).
    Returns list of entity dicts: {name, type, note, exclude_from_fill_rate, confidence}.
    Only high-confidence entries are returned.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key or not global_notes_text.strip():
        return []

    client = anthropic.Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=1024,
            system=_ENTITY_EXTRACTION_PROMPT,
            messages=[{
                "role": "user",
                "content": (
                    "Channel intelligence from the last 7 days across all Scout-accessible channels:\n\n"
                    + global_notes_text[:3000]  # cap at ~750 words — entity extraction, not full text
                ),
            }],
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)
        entities = data.get("entities", [])
        # Only return high-confidence entries
        return [e for e in entities if e.get("confidence") == "high"]
    except (json.JSONDecodeError, IndexError, KeyError) as e:
        log.warning(f"[harvest] entity extraction parse error: {e}")
        return []
    except Exception as e:
        log.warning(f"[harvest] entity extraction API error: {e}")
        return []


def harvest() -> dict:
    """
    Main entry point. Read channels, compress, write context file.
    Returns dict with 'context' (channel_context.json content) and 'audit' (entity write summary).
    """
    _LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    _LOCK_PATH.write_text(json.dumps({"started": datetime.now(timezone.utc).isoformat(), "pid": os.getpid()}))
    try:
        return _harvest_inner()
    finally:
        _LOCK_PATH.unlink(missing_ok=True)


def _harvest_inner() -> dict:
    """Inner harvest logic — called by harvest() inside lock/finally wrapper."""
    if not _BOT_TOKEN:
        log.error("[harvest] SLACK_BOT_TOKEN not set — cannot harvest")
        return {}

    web = WebClient(token=_BOT_TOKEN)
    channels = _get_channels(web)
    log.info(f"[harvest] found {len(channels)} channels Scout is a member of")

    # Aggregate context across all channels
    global_notes: list[str] = []
    publisher_notes: dict[str, list[str]] = {}

    import time as _time

    for i, ch in enumerate(channels):
        name = ch["name"]
        log.info(f"[harvest] reading #{name} ({i + 1}/{len(channels)})...")
        try:
            text = _read_channel_history(web, ch["id"], days=7)
        except Exception as e:
            err_str = str(e)
            if "ratelimited" in err_str.lower():
                log.warning(f"[harvest] rate limited on #{name} — sleeping 30s and retrying")
                _time.sleep(30)
                try:
                    text = _read_channel_history(web, ch["id"], days=7)
                except Exception:
                    log.warning(f"[harvest] #{name} — skipping after retry failure")
                    continue
            else:
                log.warning(f"[harvest] #{name} — read error: {e}")
                continue
        if not text:
            continue  # silently skip empty channels — no log noise

        extracted = _compress(text, name)
        if not extracted:
            continue

        # Throttle between channels to avoid Slack rate limits
        _time.sleep(1.2)

        # Merge global notes
        g = extracted.get("global", "").strip()
        if g:
            global_notes.append(f"[#{name}] {g}")

        # Merge publisher-specific notes
        for pub_name, notes in extracted.get("publishers", {}).items():
            if notes and notes.strip():
                publisher_notes.setdefault(pub_name, []).append(notes.strip())

    # Build final context
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    expires_str = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")

    context = {
        "harvested_at": today_str,
        "expires_at": expires_str,
        "global": " ".join(global_notes)[:600],  # hard cap ~150 words
        "publishers": {
            name: " ".join(notes)[:320]  # hard cap ~80 words per publisher
            for name, notes in publisher_notes.items()
        },
    }

    # Write channel_context.json atomically
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = _CONTEXT_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(context, indent=2))
        os.replace(tmp, _CONTEXT_FILE)
        log.info(
            f"[harvest] wrote channel_context.json — "
            f"global: {len(context['global'])} chars, "
            f"publishers: {len(context['publishers'])} entries"
        )
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    # --- Entity extraction pass ---
    # Run once on aggregated global notes to find durable behavioral facts.
    # Writes to entity_overrides.json. Never overwrites manual (scout-agent/seed) entries.
    audit_entries: list[dict] = []  # for the nightly audit post in scout_bot.py
    global_text = context["global"]
    if global_text:
        try:
            from scout_agent import _load_entity_overrides, _save_entity_overrides
            import datetime as _dt

            entities = _extract_entities(global_text)
            overrides = _load_entity_overrides()

            for entity in entities:
                name = entity.get("name", "").strip()
                etype = entity.get("type", "").lower()
                note = entity.get("note", "").strip()
                excl = bool(entity.get("exclude_from_fill_rate", False))
                if not name or not note or etype not in ("publisher", "advertiser"):
                    continue

                section = "publishers" if etype == "publisher" else "advertisers"
                existing = overrides.get(section, {}).get(name, {})
                added_by = existing.get("added_by", "")

                if added_by in ("scout-agent", "seed"):
                    log.info(f"[harvest] entity {name!r} — manual entry exists ({added_by}), skipping")
                    audit_entries.append({"name": name, "type": etype, "action": "skipped", "reason": f"manual entry exists ({added_by})"})
                    continue

                overrides.setdefault(section, {})[name] = {
                    "note": note,
                    "exclude_from_fill_rate": excl if etype == "publisher" else False,
                    "added": _dt.date.today().isoformat(),
                    "added_by": "harvester",
                }
                log.info(f"[harvest] entity {name!r} ({etype}) written to entity_overrides.json")
                audit_entries.append({"name": name, "type": etype, "action": "written", "note": note})

            if any(e["action"] == "written" for e in audit_entries):
                _save_entity_overrides(overrides)

        except Exception as e:
            log.warning(f"[harvest] entity extraction/write failed (non-fatal): {e}")

    return {"context": context, "audit": audit_entries}


def is_stale() -> bool:
    """Check if context file needs refresh. Returns False if harvest is in progress."""
    if _LOCK_PATH.exists():
        try:
            lock_data = json.loads(_LOCK_PATH.read_text())
            started = datetime.fromisoformat(lock_data["started"])
            age_hours = (datetime.now(timezone.utc) - started).total_seconds() / 3600
            if age_hours < 0.5:  # 30 minutes — harvests complete in minutes, not hours
                log.info("[harvest] lock file present and fresh — harvest in progress, skipping")
                return False  # harvest in progress
            else:
                log.warning("[harvest] stale lock file (>30min) — crash detected, cleaning up")
                _LOCK_PATH.unlink(missing_ok=True)
        except Exception:
            _LOCK_PATH.unlink(missing_ok=True)  # corrupt lock — clean up and proceed
    if not _CONTEXT_FILE.exists():
        return True
    try:
        data = json.loads(_CONTEXT_FILE.read_text())
        return data.get("harvested_at", "") != datetime.now(timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return True


if __name__ == "__main__":
    result = harvest()
    if result:
        print(json.dumps(result, indent=2))
    else:
        print("No context harvested.")
