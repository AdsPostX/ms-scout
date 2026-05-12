#!/usr/bin/env python3
"""
Part 3.2 — One-time cleanup: drop all harvester-authored rows in
data/entity_overrides.json.

The Trust Reset (Scout adoption plan v3, Part 3) kills the harvester
auto-write path. Existing rows authored by the harvester were never
human-approved and must not continue acting as ground truth.

Drop policy (D2 = "blind drop"):
    - publishers / advertisers entries with added_by == "harvester" → removed
    - added_by in {"seed", "scout-agent", "<user_id>"} → kept

Idempotent. Safe to run multiple times. Writes a sibling backup file
data/entity_overrides.json.pre-trust-reset.bak the first time it removes
anything.

Usage (on Render shell or locally):
    python3 scripts/drop_harvester_entries.py            # apply
    python3 scripts/drop_harvester_entries.py --dry-run  # report only
"""
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import sys
from datetime import datetime

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
TARGET = REPO_ROOT / "data" / "entity_overrides.json"
BACKUP = REPO_ROOT / "data" / "entity_overrides.json.pre-trust-reset.bak"
SECTIONS = ("publishers", "advertisers")
DROP_VALUE = "harvester"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be dropped; do not write")
    args = parser.parse_args()

    if not TARGET.exists():
        print(f"[trust-reset] {TARGET} not found — nothing to do.")
        return 0

    data = json.loads(TARGET.read_text())
    dropped: dict[str, list[str]] = {s: [] for s in SECTIONS}
    kept: dict[str, dict[str, int]] = {s: {} for s in SECTIONS}

    for section in SECTIONS:
        bucket = data.get(section) or {}
        survivors: dict[str, dict] = {}
        for name, row in bucket.items():
            added_by = (row or {}).get("added_by", "unknown")
            if added_by == DROP_VALUE:
                dropped[section].append(name)
            else:
                survivors[name] = row
                kept[section][added_by] = kept[section].get(added_by, 0) + 1
        data[section] = survivors

    total_dropped = sum(len(v) for v in dropped.values())
    print(f"[trust-reset] dropping {total_dropped} harvester-authored entries")
    for section in SECTIONS:
        if dropped[section]:
            print(f"  {section}: {len(dropped[section])} → {dropped[section]}")
        print(f"  {section} kept: {kept[section]}")

    if total_dropped == 0:
        print("[trust-reset] nothing to drop — already clean.")
        return 0

    if args.dry_run:
        print("[trust-reset] --dry-run set; no files written.")
        return 0

    # Backup once. If a backup already exists from a prior run, leave it.
    if not BACKUP.exists():
        shutil.copy2(TARGET, BACKUP)
        print(f"[trust-reset] backup → {BACKUP.name}")

    TARGET.write_text(json.dumps(data, indent=2) + "\n")
    print(f"[trust-reset] wrote {TARGET} at {datetime.utcnow().isoformat()}Z")
    return 0


if __name__ == "__main__":
    sys.exit(main())
