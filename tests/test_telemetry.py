"""
tests/test_telemetry.py — Unit tests for scout_telemetry.capture() and
scout_agent._init_prompt().

All tests are mock-based; no network calls, no Latitude API key required.
"""

from __future__ import annotations

import builtins
import importlib
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure repo root is on sys.path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reload_telemetry():
    """Force a fresh import of scout_telemetry (bypasses module cache)."""
    if "scout_telemetry" in sys.modules:
        del sys.modules["scout_telemetry"]
    import scout_telemetry
    return scout_telemetry


# ---------------------------------------------------------------------------
# Test 1: no-op passthrough when _telemetry is None
# ---------------------------------------------------------------------------


def test_capture_no_telemetry_is_passthrough():
    """capture() must call fn() and return its value when _telemetry is None."""
    import scout_telemetry

    original = scout_telemetry._telemetry
    try:
        scout_telemetry._telemetry = None
        result = scout_telemetry.capture("test/path", lambda: "result")
        assert result == "result"
    finally:
        scout_telemetry._telemetry = original


# ---------------------------------------------------------------------------
# Test 2: distinct_id is forwarded to span()
# ---------------------------------------------------------------------------


def test_capture_distinct_id_forwarded():
    """capture() must pass distinct_id kwarg to _telemetry.span()."""
    import scout_telemetry

    mock_span = MagicMock()
    mock_span.__enter__ = MagicMock(return_value=mock_span)
    mock_span.__exit__ = MagicMock(return_value=False)

    mock_telemetry = MagicMock()
    mock_telemetry.span = MagicMock(return_value=mock_span)

    original = scout_telemetry._telemetry
    try:
        scout_telemetry._telemetry = mock_telemetry
        result = scout_telemetry.capture(
            "scout/agent",
            lambda: "answer",
            metadata={"user_id": "U123"},
            distinct_id="U123",
        )
        assert result == "answer"
        mock_telemetry.span.assert_called_once_with(
            "scout/agent",
            distinct_id="U123",
            metadata={"user_id": "U123"},
        )
    finally:
        scout_telemetry._telemetry = original


# ---------------------------------------------------------------------------
# Test 3: distinct_id=None is safe (no exception)
# ---------------------------------------------------------------------------


def test_capture_distinct_id_none_is_safe():
    """capture() with distinct_id=None must not raise and must return fn() value."""
    import scout_telemetry

    mock_span = MagicMock()
    mock_span.__enter__ = MagicMock(return_value=mock_span)
    mock_span.__exit__ = MagicMock(return_value=False)

    mock_telemetry = MagicMock()
    mock_telemetry.span = MagicMock(return_value=mock_span)

    original = scout_telemetry._telemetry
    try:
        scout_telemetry._telemetry = mock_telemetry
        result = scout_telemetry.capture("scout/agent", lambda: "ok", distinct_id=None)
        assert result == "ok"
    finally:
        scout_telemetry._telemetry = original


# ---------------------------------------------------------------------------
# Test 4: span-init exception is swallowed; fn() still executes
# ---------------------------------------------------------------------------


def test_capture_span_init_exception_swallowed():
    """If _telemetry.span() raises, capture() logs and falls back to fn()."""
    import scout_telemetry

    mock_telemetry = MagicMock()
    mock_telemetry.span = MagicMock(side_effect=RuntimeError("connection refused"))

    original = scout_telemetry._telemetry
    try:
        scout_telemetry._telemetry = mock_telemetry
        result = scout_telemetry.capture("scout/agent", lambda: "fallback")
        assert result == "fallback"
    finally:
        scout_telemetry._telemetry = original


# ---------------------------------------------------------------------------
# Test 5: _init_prompt() falls back to local file when LATITUDE_PROMPT_PATH unset
# ---------------------------------------------------------------------------


def test_init_prompt_fallback_no_path(tmp_path, monkeypatch):
    """_init_prompt() must return local file content when LATITUDE_PROMPT_PATH is unset."""
    # Point _PROMPT_PATH at a temp file so we don't need the real 990-line file
    fake_prompt = "# fake system prompt\nYou are Scout."
    fake_md = tmp_path / "scout_system.md"
    fake_md.write_text(fake_prompt, encoding="utf-8")

    monkeypatch.delenv("LATITUDE_PROMPT_PATH", raising=False)

    import scout_agent

    original_path = scout_agent._PROMPT_PATH
    scout_agent._PROMPT_PATH = fake_md
    try:
        result = scout_agent._init_prompt()
        assert result == fake_prompt
    finally:
        scout_agent._PROMPT_PATH = original_path


# ---------------------------------------------------------------------------
# Test 6: _init_prompt() falls back to local file on ImportError for latitude_sdk
# ---------------------------------------------------------------------------


def test_init_prompt_fallback_on_import_error(tmp_path, monkeypatch):
    """_init_prompt() must return local file content when latitude_sdk is not installed."""
    fake_prompt = "# fake system prompt\nYou are Scout."
    fake_md = tmp_path / "scout_system.md"
    fake_md.write_text(fake_prompt, encoding="utf-8")

    monkeypatch.setenv("LATITUDE_PROMPT_PATH", "scout/system")
    monkeypatch.setenv("LATITUDE_API_KEY", "dummy-key")
    monkeypatch.setenv("LATITUDE_PROJECT_ID", "1")

    import scout_agent

    original_path = scout_agent._PROMPT_PATH
    scout_agent._PROMPT_PATH = fake_md

    _real_import = builtins.__import__

    def _mock_import(name, *args, **kwargs):
        if name == "latitude_sdk":
            raise ImportError("latitude-sdk not installed")
        return _real_import(name, *args, **kwargs)

    try:
        with patch("builtins.__import__", side_effect=_mock_import):
            result = scout_agent._init_prompt()
        assert result == fake_prompt
    finally:
        scout_agent._PROMPT_PATH = original_path
