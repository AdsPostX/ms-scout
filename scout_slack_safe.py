"""
scout_slack_safe — hard structural invariant for Slack response emission.

Every chat_postMessage / chat_update / chat_postEphemeral call that escapes
Scout must have either non-empty blocks OR non-empty text. Blank responses
(both empty) are substituted with a tracked incident warning so users always
see something — never a silent blank message in #revenue-operations.

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
import uuid
from typing import Any, Callable

log = logging.getLogger(__name__)

_GUARDED_ATTR = "_scout_invariant_guarded"
_GUARDED_METHODS = ("chat_postMessage", "chat_update", "chat_postEphemeral")


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


def _wrap(method_name: str, original: Callable) -> Callable:
    def guarded(*args, **kwargs):
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
