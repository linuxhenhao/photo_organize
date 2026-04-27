#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Repair target_items.meta_json fingerprint.modified_at for rows whose "
            "cached fingerprint was lost during initcache regrouping."
        )
    )
    parser.add_argument(
        "--db-path",
        default="/volume3/DocsAndMedia/Multimedia/repo/repo.db",
        help="SQLite catalog path",
    )
    parser.add_argument(
        "--media-root",
        default="/volume3/DocsAndMedia/Multimedia",
        help="Root path used to resolve non-absolute target_path values",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Apply updates in place. Without this flag the script is a dry run.",
    )
    return parser.parse_args()


@dataclass
class RepairStats:
    scanned: int = 0
    needs_repair: int = 0
    repaired: int = 0
    missing_file: int = 0
    size_mismatch: int = 0
    invalid_json: int = 0


def resolve_target_path(media_root: str, target_path: str) -> str:
    raw = PurePosixPath(target_path or "")
    if raw.is_absolute():
        return str(raw)
    return os.path.join(media_root, *raw.parts)


def chrono_like_rfc3339_from_ns(timestamp_ns: int) -> str:
    seconds, nanos = divmod(timestamp_ns, 1_000_000_000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if nanos == 0:
        return f"{base}+00:00"
    if nanos % 1_000_000 == 0:
        frac = f"{nanos // 1_000_000:03d}"
    elif nanos % 1_000 == 0:
        frac = f"{nanos // 1_000:06d}"
    else:
        frac = f"{nanos:09d}"
    return f"{base}.{frac}+00:00"


def load_meta(meta_json: str, stats: RepairStats) -> dict:
    if not meta_json:
        return {}
    try:
        value = json.loads(meta_json)
    except json.JSONDecodeError:
        stats.invalid_json += 1
        return {}
    return value if isinstance(value, dict) else {}


def missing_modified_at(meta: dict) -> bool:
    fingerprint = meta.get("fingerprint")
    if not isinstance(fingerprint, dict):
        return True
    value = fingerprint.get("modified_at")
    return not isinstance(value, str) or not value


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA busy_timeout = 5000")
    cur = conn.cursor()
    stats = RepairStats()
    updates: list[tuple[str, int]] = []

    rows = cur.execute(
        """
        SELECT id, target_path, size_bytes, meta_json
        FROM target_items
        """
    )

    for item_id, target_path, size_bytes, meta_json in rows:
        stats.scanned += 1
        meta = load_meta(meta_json or "", stats)
        if not missing_modified_at(meta):
            continue

        stats.needs_repair += 1
        resolved_path = resolve_target_path(args.media_root, target_path)
        try:
            st = os.stat(resolved_path)
        except FileNotFoundError:
            stats.missing_file += 1
            continue

        if int(st.st_size) != int(size_bytes):
            stats.size_mismatch += 1
            continue

        fingerprint = meta.get("fingerprint")
        if not isinstance(fingerprint, dict):
            fingerprint = {}
            meta["fingerprint"] = fingerprint
        fingerprint["size_bytes"] = int(size_bytes)
        fingerprint["modified_at"] = chrono_like_rfc3339_from_ns(st.st_mtime_ns)
        updates.append((json.dumps(meta, ensure_ascii=False, separators=(",", ":")), item_id))

    if args.write and updates:
        conn.executemany(
            "UPDATE target_items SET meta_json = ?1 WHERE id = ?2",
            updates,
        )
        conn.commit()
        stats.repaired = len(updates)
    else:
        conn.rollback()

    print(f"scanned {stats.scanned}")
    print(f"needs_repair {stats.needs_repair}")
    print(f"repaired {stats.repaired}")
    print(f"missing_file {stats.missing_file}")
    print(f"size_mismatch {stats.size_mismatch}")
    print(f"invalid_json {stats.invalid_json}")
    print(f"dry_run {0 if args.write else 1}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
