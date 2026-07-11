"""
scout_slack_safe — hard structural invariant for Slack response emission.

Every chat_postMessage / chat_update / chat_postEphemeral call that escapes
Scout must have either non-empty blocks OR non-empty text. Blank responses
(both empty) are substituted with a tracked incident warning so users always
see something — never a silent blank message in #revenue-operations.

This module is also the single chokepoint for outbound typography: every
payload is run through scout_ui_kit.sanitize_blocks/normalize_typography
(strips em/en dashes) and any \\n---\\n divider marker is converted into a
real Block Kit divider block. Because guard_web_client wraps the WebClient
INSTANCE, this applies regardless of which module holds the reference and
calls it — callers cannot bypass it by forgetting a helper.

Usage:
    from scout_slack_safe import guard_web_client
    web = WebClient(token=...)
    guard_web_client(web)   # wraps chat_postMessage, chat_update, chat_postEphemeral

The guard logs every substitution at WARNING with an incident_id so post-hoc
investigation has a stable handle.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from typing import Any, Callable

from scout_ui_kit import normalize_typography, sanitize_blocks

log = logging.getLogger(__name__)

_GUARDED_ATTR = "_scout_invariant_guarded"
_GUARDED_METHODS = ("chat_postMessage", "chat_update", "chat_postEphemeral")
_DIVIDER_RE = re.compile(r"\n-{3,}\n")


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, dict)):
        return len(value) == 0
    return False


def _incident_block(incident_id: str, method: str) -> tuple[str, list[dict]]:
    text = f":warning: Scout had a problem rendering this — incident `{incident_id}`"
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":warning: *Scout had a problem rendering this.*\n"
                    f"Incident `{incident_id}` ({method}). "
                    f"Try the query again, or ping @sidd if it keeps happening."
                ),
            },
        }
    ]
    return text, blocks


def _split_section_on_dividers(block: dict) -> list[dict]:
    """Expand a section block into [section, divider, section, ...] wherever
    its text contains a \\n---\\n marker. Non-matching blocks pass through untouched.
    """
    text_obj = block.get("text")
    if block.get("type") != "section" or not isinstance(text_obj, dict):
        return [block]
    raw = text_obj.get("text")
    if not isinstance(raw, str) or not _DIVIDER_RE.search(raw):
        return [block]

    parts = _DIVIDER_RE.split(raw)
    expanded: list[dict] = []
    for i, part in enumerate(parts):
        part = part.strip("\n")
        if part:
            expanded.append({**block, "text": {**text_obj, "text": part}})
        if i < len(parts) - 1:
            expanded.append({"type": "divider"})
    return expanded


def _convert_dividers(blocks: Any) -> Any:
    if not isinstance(blocks, list):
        return blocks
    expanded: list[dict] = []
    for block in blocks:
        expanded.extend(_split_section_on_dividers(block))
    return expanded


def _wrap(method_name: str, original: Callable) -> Callable:
    def guarded(*args, **kwargs):
        if kwargs.get("blocks"):
            kwargs["blocks"] = sanitize_blocks(_convert_dividers(kwargs["blocks"]))
        if isinstance(kwargs.get("text"), str):
            kwargs["text"] = normalize_typography(_DIVIDER_RE.sub("\n\n", kwargs["text"]))

        text = kwargs.get("text")
        blocks = kwargs.get("blocks")
        attachments = kwargs.get("attachments")

        if _is_empty(text) and _is_empty(blocks) and _is_empty(attachments):
            incident_id = uuid.uuid4().hex[:8]
            sub_text, sub_blocks = _incident_block(incident_id, method_name)
            log.warning(
                "scout_slack_safe: blank payload substituted incident=%s method=%s "
                "channel=%s ts=%s",
                incident_id,
                method_name,
                kwargs.get("channel"),
                kwargs.get("ts"),
            )
            kwargs["text"] = sub_text
            kwargs["blocks"] = sub_blocks
        elif _is_empty(text) and not _is_empty(blocks):
            kwargs["text"] = "Scout response"

        return original(*args, **kwargs)

    guarded.__name__ = f"guarded_{method_name}"
    guarded.__qualname__ = guarded.__name__
    return guarded


def guard_web_client(web: Any) -> Any:
    """Wrap a slack_sdk.WebClient instance so blank responses become incidents.

    Idempotent — re-wrapping the same client is a no-op. Returns the client
    for chaining convenience.
    """
    if web is None:
        return web
    if getattr(web, _GUARDED_ATTR, False):
        return web
    if os.getenv("SCOUT_SLACK_GUARD_DISABLED", "").lower() in {"1", "true", "yes"}:
        log.warning("scout_slack_safe: guard disabled via env var")
        setattr(web, _GUARDED_ATTR, True)
        return web

    for name in _GUARDED_METHODS:
        original = getattr(web, name, None)
        if original is None or not callable(original):
            continue
        setattr(web, name, _wrap(name, original))

    setattr(web, _GUARDED_ATTR, True)
    return web
