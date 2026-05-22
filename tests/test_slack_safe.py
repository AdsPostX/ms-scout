"""Tests for scout_slack_safe — the non-empty Slack response invariant."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scout_slack_safe import guard_web_client  # noqa: E402


class FakeWebClient:
    def __init__(self):
        self.calls: list[dict] = []

    def chat_postMessage(self, **kwargs):
        self.calls.append({"method": "chat_postMessage", **kwargs})
        return {"ok": True}

    def chat_update(self, **kwargs):
        self.calls.append({"method": "chat_update", **kwargs})
        return {"ok": True}

    def chat_postEphemeral(self, **kwargs):
        self.calls.append({"method": "chat_postEphemeral", **kwargs})
        return {"ok": True}


def _last(web: FakeWebClient) -> dict:
    return web.calls[-1]


def test_blank_text_and_blocks_substituted_with_incident_block():
    web = FakeWebClient()
    guard_web_client(web)

    web.chat_postMessage(channel="C1", text="", blocks=[])

    call = _last(web)
    assert call["text"].startswith(":warning:")
    assert "incident" in call["text"]
    assert len(call["blocks"]) == 1
    assert call["blocks"][0]["type"] == "section"
    assert ":warning:" in call["blocks"][0]["text"]["text"]


def test_none_text_and_none_blocks_substituted():
    web = FakeWebClient()
    guard_web_client(web)

    web.chat_postMessage(channel="C1")

    call = _last(web)
    assert call["text"].startswith(":warning:")
    assert len(call["blocks"]) == 1


def test_whitespace_only_text_substituted():
    web = FakeWebClient()
    guard_web_client(web)

    web.chat_update(channel="C1", ts="123.456", text="   \n  ", blocks=None)

    call = _last(web)
    assert call["text"].startswith(":warning:")


def test_valid_text_passes_through_untouched():
    web = FakeWebClient()
    guard_web_client(web)

    web.chat_postMessage(channel="C1", text="Hello world")

    call = _last(web)
    assert call["text"] == "Hello world"
    assert call.get("blocks") is None


def test_valid_blocks_with_empty_text_gets_fallback_text():
    web = FakeWebClient()
    guard_web_client(web)

    real_blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "hi"}}]
    web.chat_postMessage(channel="C1", text=None, blocks=real_blocks)

    call = _last(web)
    assert call["blocks"] == real_blocks
    assert call["text"], "must have a non-empty fallback text for push previews"


def test_guard_is_idempotent():
    web = FakeWebClient()
    guard_web_client(web)
    first = web.chat_postMessage
    guard_web_client(web)
    second = web.chat_postMessage
    assert first is second


def test_attachments_alone_pass_through():
    web = FakeWebClient()
    guard_web_client(web)

    web.chat_postMessage(channel="C1", attachments=[{"text": "legacy"}])

    call = _last(web)
    assert call.get("attachments") == [{"text": "legacy"}]
    # text/blocks should not have been substituted to the incident payload
    assert not (call.get("text", "") or "").startswith(":warning:")


def test_chat_update_blank_substituted():
    web = FakeWebClient()
    guard_web_client(web)

    web.chat_update(channel="C1", ts="1.2", text="", blocks=[])

    call = _last(web)
    assert call["text"].startswith(":warning:")
    assert call["method"] == "chat_update"
