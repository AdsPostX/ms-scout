#!/usr/bin/env python3
"""Run Scout routing evals against the live ask() function.

Reads evals/scout_routing_evals.jsonl, calls ask() for each entry, and scores
whether the tools that fired match the expected_tools_any / forbidden_tools
contract. Prints a table and exits non-zero on any failure.

Purpose: regression harness for the plan-then-act planner (task #4). Run before
and after the planner change to confirm we haven't broken the PMF cases (Todd
Truist, supply-gaps) while fixing the rigidity cases (ecowwerce-fit).

Costs LLM tokens — not part of CI. Run manually:
    python3 evals/run_routing_evals.py [--ids id1,id2]
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scout_agent import ask  # noqa: E402

EVALS_PATH = ROOT / "evals" / "scout_routing_evals.jsonl"


def load_evals():
    rows = []
    for line in EVALS_PATH.read_text().splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def score(entry: dict, tools_called: list[str]) -> tuple[bool, str]:
    expected_any = set(entry.get("expected_tools_any") or [])
    forbidden = set(entry.get("forbidden_tools") or [])
    fired = set(tools_called)

    if forbidden & fired:
        return False, f"forbidden tool fired: {sorted(forbidden & fired)}"
    if expected_any and not (expected_any & fired):
        return False, f"none of expected_any fired (expected one of {sorted(expected_any)}, got {sorted(fired) or 'nothing'})"
    min_tools = int(entry.get("min_tools") or 0)
    if min_tools and len(tools_called) < min_tools:
        return False, f"expected >={min_tools} tools, got {len(tools_called)}: {tools_called}"
    return True, "ok"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids", help="Comma-separated subset of eval IDs to run.")
    args = ap.parse_args()

    rows = load_evals()
    if args.ids:
        wanted = set(args.ids.split(","))
        rows = [r for r in rows if r["id"] in wanted]
        if not rows:
            print(f"no evals matched --ids={args.ids}")
            return 2

    print(f"Running {len(rows)} routing evals…\n")
    results = []
    t_start = time.monotonic()
    for r in rows:
        t0 = time.monotonic()
        try:
            res = ask(r["query"], user_id="EVAL_USER")
            fired = list(res.tools_called or [])
            ok, reason = score(r, fired)
            err = None
        except Exception as e:
            fired, ok, reason, err = [], False, f"exception: {e!r}", str(e)
        elapsed = int((time.monotonic() - t0) * 1000)
        results.append({"id": r["id"], "ok": ok, "reason": reason, "fired": fired, "ms": elapsed, "category": r.get("category", "")})
        mark = "✅" if ok else "❌"
        print(f"  {mark}  {r['id']:<28}  [{','.join(fired) or '-'}]  ({elapsed}ms)  {reason if not ok else ''}")

    total_ms = int((time.monotonic() - t_start) * 1000)
    passed = sum(1 for r in results if r["ok"])
    print(f"\n{passed}/{len(results)} passed in {total_ms}ms\n")

    # Group by category so regressions are easy to read
    by_cat: dict[str, list] = {}
    for r in results:
        by_cat.setdefault(r["category"] or "uncategorized", []).append(r)
    print("By category:")
    for cat, items in sorted(by_cat.items()):
        cp = sum(1 for i in items if i["ok"])
        print(f"  {cat:<32}  {cp}/{len(items)}")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
