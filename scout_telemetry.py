"""
scout_telemetry.py — Latitude telemetry singleton for Scout.

Initializes once at import time. All Anthropic SDK calls in this process
are automatically traced via OpenTelemetry instrumentation.

Use capture() to attach a named span to a specific prompt:

    from scout_telemetry import capture
    result = capture("scout/agent", lambda: ask(query, user_id=user_id))

Named spans in use:
    scout/agent             — every user-facing query through ask()
    scout/entity-parse      — "remember" command entity extraction
    scout/context-compress  — channel context compression (harvest)
    scout/entity-extract    — entity fact extraction (harvest)

If LATITUDE_API_KEY is not set, capture() is a no-op pass-through.
All exceptions from the telemetry layer are swallowed — Scout never
fails because Latitude is down.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

_telemetry = None


def _init() -> None:
    global _telemetry
    api_key = os.getenv("LATITUDE_API_KEY")
    project = os.getenv("LATITUDE_PROJECT_ID")
    if not api_key:
        log.debug("[telemetry] LATITUDE_API_KEY not set — tracing disabled")
        return
    try:
        from latitude_telemetry import Telemetry, TelemetryOptions, Instrumentors
        _telemetry = Telemetry(
            api_key,
            TelemetryOptions(instrumentors=[Instrumentors.Anthropic]),
        )
        _telemetry.instrument()
        log.info("[telemetry] Latitude initialized (project=%s)", project)
    except ImportError:
        log.warning("[telemetry] latitude-telemetry not installed — tracing disabled")
    except Exception as exc:
        log.warning("[telemetry] init failed: %s — tracing disabled", exc)


def capture(path: str, fn, metadata: dict | None = None, distinct_id: str | None = None):
    """
    Wrap a callable with a named Latitude span.

    Falls back to a plain call if telemetry is off or errors.
    Always returns the callable's return value.

    distinct_id: optional user identifier forwarded to Latitude for per-user filtering.
    """
    if _telemetry is None:
        return fn()
    _span_entered = False
    try:
        _span = _telemetry.span(path, distinct_id=distinct_id, metadata=metadata or {})
        _span.__enter__()
        _span_entered = True
        result = fn()
        _span.__exit__(None, None, None)
        return result
    except Exception as exc:
        if not _span_entered:
            # Span init failed — fall back silently; fn() not yet called
            log.warning("[telemetry] capture(%s) span-init error: %s — running untraced", path, exc)
            return fn()
        # fn() itself raised — close span and re-raise (never double-execute)
        try:
            _span.__exit__(type(exc), exc, exc.__traceback__)
        except Exception:
            pass
        raise


_init()
