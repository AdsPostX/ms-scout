"""Best-effort writers for the P2.3 job-run telemetry tables.

Three thin functions that demand-feed daemons (scraper, harvester,
hourly_shadow, revenue_tracker, autocheck — P2.5-P2.8) call to record
"how did my last run go" into ClickHouse.

Hard guarantee: telemetry writes never raise. A broken ClickHouse must
not break a scraper. Every public function wraps `client.insert(...)`
in try/except and logs on failure.

Schema lives in `migrations/2026_05_p2_3_job_runs.sql`. The plan owner
applies the DDL manually; this module assumes the tables exist.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger(__name__)


# Column order matches the CREATE TABLE in migrations/2026_05_p2_3_job_runs.sql.
# Keep these in sync if you add columns (additive only — never remove).
_JOB_RUNS_COLS = (
    "ts",
    "job_name",
    "network",
    "status",
    "error",
    "payload_hash",
    "duration_ms",
)

_PER_NETWORK_STATUS_COLS = (
    "network",
    "last_successful_scrape",
    "last_failure_ts",
    "last_failure_reason",
    "last_offer_count",
    "updated_at",
)

_NORMALIZATION_ERRORS_COLS = (
    "ts",
    "network",
    "offer_id",
    "field",
    "raw_value",
    "error",
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_client(client: Any) -> Optional[Any]:
    """Return the passed-in client or lazily construct the default one.

    Import is local so importing `scout_core.job_runs` never triggers a
    ClickHouse connect at module load (matches scout_ch.py's pattern).
    """
    if client is not None:
        return client
    try:
        from scout_ch import _get_ch_client  # local import — see scout_ch.py
        return _get_ch_client()
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("job_runs: could not construct CH client: %s", exc)
        return None


def record_job_run(
    job_name: str,
    network: str = "",
    status: str = "success",
    error: str = "",
    duration_ms: int = 0,
    payload_hash: str = "",
    *,
    client: Any = None,
) -> None:
    """Append one row to `job_runs`. Best-effort — never raises."""
    ch = _resolve_client(client)
    if ch is None:
        return
    row = [
        _now_utc(),
        job_name,
        network or "",
        status,
        error or "",
        payload_hash or "",
        int(duration_ms or 0),
    ]
    try:
        ch.insert("job_runs", [row], column_names=list(_JOB_RUNS_COLS))
    except Exception as exc:
        log.warning(
            "job_runs: insert failed for job=%s status=%s: %s",
            job_name, status, exc,
        )


def update_network_status(
    network: str,
    success: bool = True,
    error: Optional[str] = None,
    offer_count: int = 0,
    *,
    client: Any = None,
) -> None:
    """Upsert latest-known health for `network` via ReplacingMergeTree.

    On success: bumps last_successful_scrape and last_offer_count.
    On failure: sets last_failure_ts + last_failure_reason; leaves the
                last successful scrape timestamp at "epoch" so the row
                shape stays consistent for downstream queries.
    """
    ch = _resolve_client(client)
    if ch is None:
        return
    now = _now_utc()
    if success:
        row = [
            network,
            now,                              # last_successful_scrape
            None,                             # last_failure_ts
            "",                               # last_failure_reason
            int(offer_count or 0),
            now,                              # updated_at
        ]
    else:
        # Mark the failure; preserve a non-null sentinel for last_successful_scrape
        # because the column is non-nullable in the DDL. Use epoch — callers can
        # filter `last_successful_scrape > toDateTime64('1970-01-02', 3)` to skip.
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        row = [
            network,
            epoch,
            now,
            (error or "")[:2000],             # cap reason length defensively
            int(offer_count or 0),
            now,
        ]
    try:
        ch.insert(
            "per_network_status",
            [row],
            column_names=list(_PER_NETWORK_STATUS_COLS),
        )
    except Exception as exc:
        log.warning(
            "job_runs: per_network_status upsert failed for network=%s: %s",
            network, exc,
        )


def record_normalization_error(
    network: str,
    offer_id: str,
    field: str,
    raw_value: str,
    error: str,
    *,
    client: Any = None,
) -> None:
    """Append one row to `normalization_errors`. Best-effort — never raises."""
    ch = _resolve_client(client)
    if ch is None:
        return
    row = [
        _now_utc(),
        network,
        offer_id or "",
        field,
        ("" if raw_value is None else str(raw_value))[:4000],
        (error or "")[:2000],
    ]
    try:
        ch.insert(
            "normalization_errors",
            [row],
            column_names=list(_NORMALIZATION_ERRORS_COLS),
        )
    except Exception as exc:
        log.warning(
            "job_runs: normalization_errors insert failed for "
            "network=%s offer=%s field=%s: %s",
            network, offer_id, field, exc,
        )
