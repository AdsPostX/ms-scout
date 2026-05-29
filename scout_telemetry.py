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

Note on OTLP delivery: latitude-telemetry 1.0.0 (the current PyPI release)
exports to gateway.latitude.so/api/v2/otlp/v1/traces which has been
decommissioned — spans are structured and captured correctly but silently
dropped at export. The fix (ingest.latitude.so endpoint + new API) is in the
package's unreleased v3.0.0a8. Upgrade latitude-telemetry when a new PyPI
release is available. The _init_prompt() managed-prompt feature is unaffected
(it uses latitude-sdk REST API directly, which works fine with the current key).
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

_telemetry = None

# Current PyPI release (1.0.0) exports to a decommissioned endpoint.
# Spans are captured but not delivered until latitude-telemetry is updated.
_OTLP_ENDPOINT_LIVE = False  # flip to True once a working PyPI release is installed


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
        if _OTLP_ENDPOINT_LIVE:
            log.info("[telemetry] Latitude initialized (project=%s)", project)
        else:
            log.warning(
                "[telemetry] Latitude initialized (project=%s) — OTLP export inactive: "
                "latitude-telemetry 1.0.0 uses a decommissioned endpoint. "
                "Spans are captured but not delivered. Upgrade the package when available.",
                project,
            )
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
