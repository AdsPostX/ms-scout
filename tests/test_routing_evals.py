"""Lint the routing eval set so CI catches drift.

The runner (`evals/run_routing_evals.py`) calls ask() and is too slow/costly
for CI. This test only validates the eval file's structure and that every
referenced tool name still exists in TOOL_MAP — so renaming a tool fails
fast instead of every eval silently scoring "none of expected_any fired".
"""
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
EVALS_PATH = ROOT / "evals" / "scout_routing_evals.jsonl"


def _load():
    return [json.loads(line) for line in EVALS_PATH.read_text().splitlines() if line.strip()]


def test_eval_file_exists_and_nonempty():
    assert EVALS_PATH.exists(), f"missing {EVALS_PATH}"
    rows = _load()
    assert 10 <= len(rows) <= 30, f"expected 10-30 evals, got {len(rows)}"


def test_required_keys_present():
    required = {"id", "user", "query", "expected_tools_any", "forbidden_tools", "expected_signal", "category"}
    for r in _load():
        missing = required - set(r.keys())
        assert not missing, f"eval {r.get('id')} missing keys: {missing}"


def test_ids_unique():
    ids = [r["id"] for r in _load()]
    assert len(ids) == len(set(ids)), f"duplicate eval IDs: {sorted(ids)}"


def test_referenced_tools_exist_in_tool_map():
    from scout_agent import TOOL_MAP
    tools = set(TOOL_MAP.keys())
    bad = []
    for r in _load():
        for t in (r.get("expected_tools_any") or []) + (r.get("forbidden_tools") or []):
            if t not in tools:
                bad.append((r["id"], t))
    assert not bad, f"evals reference tools not in TOOL_MAP: {bad}"


def test_rigidity_case_forbids_entity_note_lookup():
    rows = {r["id"]: r for r in _load()}
    assert "sidd-ecowwerce-fit" in rows, "primary rigidity eval missing"
    assert "why_entity_note" in rows["sidd-ecowwerce-fit"]["forbidden_tools"], (
        "ecowwerce-fit must forbid entity-note lookup — that's the rigidity case"
    )
