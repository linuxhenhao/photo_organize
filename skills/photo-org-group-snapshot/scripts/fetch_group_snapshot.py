#!/usr/bin/env python3

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from pathlib import PurePosixPath


REMOTE_QUERY_SCRIPT = r"""
import base64
import json
import os
import sqlite3
import sys

db_path = sys.argv[1]
group_id = int(sys.argv[2])
include_operations = sys.argv[3] == "1"
media_root = sys.argv[4]

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

def normalize_row(row):
    result = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            result[key] = base64.b64encode(value).decode("ascii")
            result[key + "_encoding"] = "base64"
        else:
            result[key] = value
    return result

def resolve_remote_path(target_path, group_id):
    relative_target = (target_path or "").lstrip("/")
    basename = os.path.basename(relative_target)
    candidates = []
    if relative_target:
        candidates.append(os.path.join(media_root, relative_target))
    if basename:
        candidates.append(
            os.path.join(
                media_root,
                "repo/.photo-org/trash",
                f"group-{group_id}",
                basename,
            )
        )
    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if os.path.isfile(candidate):
            return candidate
    return None

items_query = '''
SELECT
    ti.id,
    ti.target_path,
    ti.size_bytes,
    ti.mime_type,
    ti.created_at,
    ti.exact_hash,
    ti.phash,
    ti.phash_bits,
    ti.width,
    ti.height,
    ti.group_id,
    ti.keep_state,
    ti.is_group_primary,
    ti.group_status,
    ti.origin_source_id,
    ti.meta_json,
    fc.akaze_status,
    fc.akaze_keypoints,
    fc.akaze_descriptors AS akaze_descriptors_b64,
    length(fc.akaze_descriptors) AS akaze_descriptor_bytes,
    fc.feature_version,
    fc.updated_at AS feature_updated_at
FROM target_items ti
LEFT JOIN feature_cache fc
    ON fc.exact_hash = ti.exact_hash
   AND fc.size_bytes = ti.size_bytes
WHERE ti.group_id = ?
ORDER BY ti.is_group_primary DESC, ti.id
'''

summary_query = '''
SELECT
    COUNT(*) AS item_count,
    COUNT(DISTINCT exact_hash) AS distinct_exact_hashes,
    COUNT(DISTINCT phash) AS distinct_phashes,
    MIN(width) AS min_width,
    MAX(width) AS max_width,
    MIN(height) AS min_height,
    MAX(height) AS max_height,
    MIN(size_bytes) AS min_size_bytes,
    MAX(size_bytes) AS max_size_bytes
FROM target_items
WHERE group_id = ?
'''

operations_query = '''
SELECT id, kind, created_at, payload_json
FROM operations_log
WHERE payload_json LIKE ?
ORDER BY id DESC
LIMIT 50
'''

items = []
for row in conn.execute(items_query, [group_id]):
    item = normalize_row(row)
    resolved_path = resolve_remote_path(item.get("target_path"), group_id)
    item["resolved_remote_path"] = resolved_path
    item["remote_file_exists"] = resolved_path is not None
    item["resolved_remote_size_bytes"] = (
        os.path.getsize(resolved_path) if resolved_path is not None else None
    )
    items.append(item)
summary_row = conn.execute(summary_query, [group_id]).fetchone()
summary = normalize_row(summary_row) if summary_row is not None else {}

operations = []
if include_operations:
    pattern = f'%\"group_id\":{group_id}%'
    operations = [normalize_row(row) for row in conn.execute(operations_query, [pattern])]

payload = {
    "group_id": group_id,
    "summary": summary,
    "items": items,
    "operations_log": operations,
}

print(json.dumps(payload, ensure_ascii=False))
"""

REMOTE_READ_FILE_SCRIPT = r"""
import sys

path = sys.argv[1]

with open(path, "rb") as handle:
    while True:
        chunk = handle.read(1024 * 1024)
        if not chunk:
            break
        sys.stdout.buffer.write(chunk)
"""


TARGET_COLUMNS = [
    ("id", "INTEGER"),
    ("target_path", "TEXT"),
    ("size_bytes", "INTEGER"),
    ("mime_type", "TEXT"),
    ("created_at", "TEXT"),
    ("exact_hash", "TEXT"),
    ("phash", "TEXT"),
    ("phash_bits", "INTEGER"),
    ("width", "INTEGER"),
    ("height", "INTEGER"),
    ("group_id", "INTEGER"),
    ("keep_state", "TEXT"),
    ("is_group_primary", "INTEGER"),
    ("group_status", "TEXT"),
    ("origin_source_id", "INTEGER"),
    ("meta_json", "TEXT"),
]

FEATURE_COLUMNS = [
    ("exact_hash", "TEXT"),
    ("size_bytes", "INTEGER"),
    ("akaze_status", "TEXT"),
    ("akaze_keypoints", "INTEGER"),
    ("akaze_descriptors_b64", "TEXT"),
    ("akaze_descriptor_bytes", "INTEGER"),
    ("feature_version", "INTEGER"),
    ("feature_updated_at", "TEXT"),
]

OPERATIONS_COLUMNS = [
    ("id", "INTEGER"),
    ("kind", "TEXT"),
    ("created_at", "TEXT"),
    ("payload_json", "TEXT"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch a remote photo-org group snapshot over SSH and store "
            "target_items + feature_cache metadata locally."
        )
    )
    parser.add_argument("group_id", type=int, help="Remote target_items.group_id to fetch")
    parser.add_argument(
        "--host",
        default="nas-photo",
        help="SSH host that can access the remote SQLite catalog (default: nas-photo)",
    )
    parser.add_argument(
        "--db-path",
        default="/volume3/DocsAndMedia/Multimedia/repo/repo.db",
        help="Remote SQLite catalog path",
    )
    parser.add_argument(
        "--media-root",
        default="/volume3/DocsAndMedia/Multimedia",
        help="Remote media root that contains repo/ and thumbnails/ trees",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("downloads"),
        help="Base output directory; group data is written under group-<id>/",
    )
    parser.add_argument(
        "--ssh-bin",
        default="ssh",
        help="SSH executable to use (default: ssh)",
    )
    parser.add_argument(
        "--skip-operations-log",
        action="store_true",
        help="Do not query matching operations_log rows",
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Do not download the remote item files",
    )
    return parser.parse_args()


def run_remote_query(
    host: str,
    ssh_bin: str,
    db_path: str,
    group_id: int,
    include_operations: bool,
    media_root: str,
) -> dict:
    cmd = [
        ssh_bin,
        host,
        "python3",
        "-",
        db_path,
        str(group_id),
        "1" if include_operations else "0",
        media_root,
    ]
    result = subprocess.run(
        cmd,
        input=REMOTE_QUERY_SCRIPT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def safe_target_relative_path(target_path: str, item_id: int) -> Path:
    raw = PurePosixPath(target_path or "")
    parts = [part for part in raw.parts if part not in ("", ".")]
    if any(part == ".." for part in parts):
        raise ValueError(f"unsafe target_path for item {item_id}: {target_path!r}")
    if not parts:
        return Path(f"id-{item_id}")
    return Path(*parts)


def download_remote_file(host: str, ssh_bin: str, remote_path: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [ssh_bin, host, "python3", "-", remote_path]
    with local_path.open("wb") as handle:
        subprocess.run(
            cmd,
            input=REMOTE_READ_FILE_SCRIPT.encode("utf-8"),
            check=True,
            stdout=handle,
            stderr=subprocess.PIPE,
        )


def download_group_files(host: str, ssh_bin: str, files_dir: Path, items: list[dict]) -> tuple[int, int]:
    downloaded = 0
    missing = 0
    for item in items:
        resolved_remote_path = item.get("resolved_remote_path")
        if not resolved_remote_path:
            missing += 1
            continue
        relative_path = safe_target_relative_path(item.get("target_path", ""), item.get("id", 0))
        local_path = files_dir / relative_path
        download_remote_file(host, ssh_bin, resolved_remote_path, local_path)
        item["downloaded_local_path"] = str(local_path)
        downloaded += 1
    return downloaded, missing


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def create_snapshot_db(path: Path, payload: dict) -> None:
    if path.exists():
        path.unlink()

    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE target_items_snapshot ("
            + ", ".join(f"{name} {kind}" for name, kind in TARGET_COLUMNS)
            + ")"
        )
        conn.execute(
            "CREATE TABLE feature_cache_snapshot ("
            + ", ".join(f"{name} {kind}" for name, kind in FEATURE_COLUMNS)
            + ", PRIMARY KEY (exact_hash, size_bytes))"
        )
        conn.execute(
            "CREATE TABLE operations_log_snapshot ("
            + ", ".join(f"{name} {kind}" for name, kind in OPERATIONS_COLUMNS)
            + ")"
        )

        metadata_rows = {
            "group_id": str(payload["group_id"]),
            "fetched_at_utc": payload["fetched_at_utc"],
            "remote_host": payload["remote_host"],
            "remote_db_path": payload["remote_db_path"],
            "remote_media_root": payload["remote_media_root"],
        }
        conn.executemany(
            "INSERT INTO metadata (key, value) VALUES (?, ?)",
            metadata_rows.items(),
        )

        target_insert = (
            "INSERT INTO target_items_snapshot ("
            + ", ".join(name for name, _ in TARGET_COLUMNS)
            + ") VALUES ("
            + ", ".join("?" for _ in TARGET_COLUMNS)
            + ")"
        )
        feature_insert = (
            "INSERT INTO feature_cache_snapshot ("
            + ", ".join(name for name, _ in FEATURE_COLUMNS)
            + ") VALUES ("
            + ", ".join("?" for _ in FEATURE_COLUMNS)
            + ")"
        )
        operations_insert = (
            "INSERT INTO operations_log_snapshot ("
            + ", ".join(name for name, _ in OPERATIONS_COLUMNS)
            + ") VALUES ("
            + ", ".join("?" for _ in OPERATIONS_COLUMNS)
            + ")"
        )

        feature_seen = set()
        for item in payload["items"]:
            conn.execute(
                target_insert,
                [item.get(name) for name, _ in TARGET_COLUMNS],
            )

            feature_key = (item.get("exact_hash"), item.get("size_bytes"))
            if feature_key not in feature_seen:
                feature_seen.add(feature_key)
                conn.execute(
                    feature_insert,
                    [item.get(name) for name, _ in FEATURE_COLUMNS],
                )

        for row in payload.get("operations_log", []):
            conn.execute(
                operations_insert,
                [row.get(name) for name, _ in OPERATIONS_COLUMNS],
            )

        conn.commit()
    finally:
        conn.close()


def create_manifest(path: Path, payload: dict) -> None:
    summary = payload.get("summary", {})
    download_summary = payload.get("download_summary", {})
    lines = [
        f"group_id: {payload['group_id']}",
        f"remote_host: {payload['remote_host']}",
        f"remote_db_path: {payload['remote_db_path']}",
        f"remote_media_root: {payload['remote_media_root']}",
        f"fetched_at_utc: {payload['fetched_at_utc']}",
        f"item_count: {summary.get('item_count', 0)}",
        f"distinct_exact_hashes: {summary.get('distinct_exact_hashes', 0)}",
        f"distinct_phashes: {summary.get('distinct_phashes', 0)}",
        f"operations_log_rows: {len(payload.get('operations_log', []))}",
        f"downloaded_files: {download_summary.get('downloaded_files', 0)}",
        f"missing_remote_files: {download_summary.get('missing_remote_files', 0)}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    output_dir = args.out_dir / f"group-{args.group_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = run_remote_query(
        host=args.host,
        ssh_bin=args.ssh_bin,
        db_path=args.db_path,
        group_id=args.group_id,
        include_operations=not args.skip_operations_log,
        media_root=args.media_root,
    )
    payload["remote_host"] = args.host
    payload["remote_db_path"] = args.db_path
    payload["remote_media_root"] = args.media_root
    payload["fetched_at_utc"] = datetime.now(timezone.utc).isoformat()

    json_path = output_dir / "group.json"
    sqlite_path = output_dir / "group.sqlite"
    manifest_path = output_dir / "manifest.txt"
    files_dir = output_dir / "files"

    if args.skip_images:
        payload["download_summary"] = {
            "downloaded_files": 0,
            "missing_remote_files": sum(1 for item in payload["items"] if not item.get("resolved_remote_path")),
        }
    else:
        downloaded_files, missing_remote_files = download_group_files(
            host=args.host,
            ssh_bin=args.ssh_bin,
            files_dir=files_dir,
            items=payload["items"],
        )
        payload["download_summary"] = {
            "downloaded_files": downloaded_files,
            "missing_remote_files": missing_remote_files,
        }

    write_json(json_path, payload)
    create_snapshot_db(sqlite_path, payload)
    create_manifest(manifest_path, payload)

    summary = payload.get("summary", {})
    print(f"wrote: {json_path}")
    print(f"wrote: {sqlite_path}")
    print(f"wrote: {manifest_path}")
    print(
        "summary: "
        f"items={summary.get('item_count', 0)}, "
        f"exact_hashes={summary.get('distinct_exact_hashes', 0)}, "
        f"phashes={summary.get('distinct_phashes', 0)}, "
        f"operations={len(payload.get('operations_log', []))}, "
        f"downloaded_files={payload['download_summary']['downloaded_files']}, "
        f"missing_remote_files={payload['download_summary']['missing_remote_files']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
