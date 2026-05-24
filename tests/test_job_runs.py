"""Tests for scout_core.job_runs — P2.3 best-effort telemetry writers.

Pins two behaviors we rely on across every demand-feed daemon:
  * The writer calls `client.insert(table, [row], column_names=[...])`
    with the right table, the right column count, and the values in
    the order matching `migrations/2026_05_p2_3_job_runs.sql`.
  * If the client raises, the writer swallows it. A broken ClickHouse
    must not break a scraper.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from scout_core.job_runs import (
    _JOB_RUNS_COLS,
    _NORMALIZATION_ERRORS_COLS,
    _PER_NETWORK_STATUS_COLS,
    record_job_run,
    record_normalization_error,
    update_network_status,
)


def _make_client():
    client = MagicMock()
    client.insert = MagicMock()
    return client


def test_record_job_run_inserts_row_with_expected_columns():
    client = _make_client()
    record_job_run(
        "scraper",
        network="Impact",
        status="success",
        duration_ms=1234,
        payload_hash="abc",
        client=client,
    )

    client.insert.assert_called_once()
    args, kwargs = client.insert.call_args
    assert args[0] == "job_runs"
    rows = args[1]
    assert len(rows) == 1
    row = rows[0]
    assert kwargs["column_names"] == list(_JOB_RUNS_COLS)
    assert len(row) == len(_JOB_RUNS_COLS)
    # ts is a tz-aware UTC datetime — column 0
    assert isinstance(row[0], datetime) and row[0].tzinfo is not None
    assert row[1] == "scraper"
    assert row[2] == "Impact"
    assert row[3] == "success"
    assert row[4] == ""           # error defaulted
    assert row[5] == "abc"        # payload_hash
    assert row[6] == 1234         # duration_ms (int)


def test_record_job_run_swallows_client_exceptions():
    client = _make_client()
    client.insert.side_effect = RuntimeError("CH down")

    # Must NOT raise — telemetry is best-effort.
    record_job_run("scraper", status="error", error="boom", client=client)

    client.insert.assert_called_once()


def test_update_network_status_success_writes_last_successful_scrape():
    client = _make_client()
    update_network_status("CJ", success=True, offer_count=42, client=client)

    args, kwargs = client.insert.call_args
    assert args[0] == "per_network_status"
    row = args[1][0]
    assert kwargs["column_names"] == list(_PER_NETWORK_STATUS_COLS)
    assert row[0] == "CJ"
    assert isinstance(row[1], datetime) and row[1].tzinfo is not None
    assert row[2] is None                 # last_failure_ts left null on success
    assert row[3] == ""                   # last_failure_reason
    assert row[4] == 42                   # last_offer_count
    assert isinstance(row[5], datetime)   # updated_at


def test_update_network_status_failure_writes_reason_and_epoch_sentinel():
    client = _make_client()
    update_network_status(
        "Impact",
        success=False,
        error="timeout after 30s",
        client=client,
    )

    row = client.insert.call_args[0][1][0]
    assert row[0] == "Impact"
    # last_successful_scrape set to epoch sentinel (not None — column is non-nullable)
    assert row[1] == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert isinstance(row[2], datetime)            # last_failure_ts populated
    assert row[3] == "timeout after 30s"


def test_update_network_status_swallows_client_exceptions():
    client = _make_client()
    client.insert.side_effect = ConnectionError("nope")

    update_network_status("Impact", success=False, error="x", client=client)


def test_record_normalization_error_inserts_row():
    client = _make_client()
    record_normalization_error(
        "CJ",
        offer_id="999",
        field="payout",
        raw_value="see terms",
        error="not numeric",
        client=client,
    )

    args, kwargs = client.insert.call_args
    assert args[0] == "normalization_errors"
    row = args[1][0]
    assert kwargs["column_names"] == list(_NORMALIZATION_ERRORS_COLS)
    assert isinstance(row[0], datetime)
    assert row[1] == "CJ"
    assert row[2] == "999"
    assert row[3] == "payout"
    assert row[4] == "see terms"
    assert row[5] == "not numeric"


def test_record_normalization_error_swallows_exceptions():
    client = _make_client()
    client.insert.side_effect = ValueError("bad schema")

    record_normalization_error(
        "X", offer_id="1", field="geo", raw_value="??", error="bad",
        client=client,
    )


def test_no_client_no_call_no_raise(monkeypatch):
    """If scout_ch import or _get_ch_client fails, writers degrade silently."""
    import scout_core.job_runs as jr

    def _boom():
        raise RuntimeError("no CH in this env")

    # Patch the lazy import target — _resolve_client catches and returns None.
    monkeypatch.setattr(jr, "_resolve_client", lambda client: None)

    record_job_run("scraper")
    update_network_status("CJ")
    record_normalization_error("CJ", "1", "payout", "x", "err")
