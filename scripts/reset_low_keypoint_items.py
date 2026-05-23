#!/usr/bin/env python3

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CandidateRow:
    target_item_id: int
    target_path: str
    exact_hash: str
    size_bytes: int
    group_id: int | None
    group_status: str
    akaze_status: str
    akaze_keypoints: int | None
    feature_version: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete target_items plus matching low-keypoint feature_cache rows so a later "
            "initcache run re-ingests and recomputes them from disk."
        )
    )
    parser.add_argument("--db", required=True, help="Path to catalog.db / repo.db")
    parser.add_argument(
        "--max-keypoints",
        type=int,
        default=24,
        help="Match feature_cache rows with ready status and akaze_keypoints <= this value",
    )
    parser.add_argument(
        "--target-path-like",
        action="append",
        default=[],
        help="Optional SQL LIKE filter on target_items.target_path; may be repeated",
    )
    parser.add_argument(
        "--group-id",
        type=int,
        action="append",
        default=[],
        help="Optional exact group_id filter; may be repeated",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row limit after filtering; 0 means no limit",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete rows. Without this flag, the script only prints a dry-run preview.",
    )
    return parser.parse_args()


def build_query(args: argparse.Namespace) -> tuple[str, list[object]]:
    query = """
        SELECT
            ti.id,
            ti.target_path,
            ti.exact_hash,
            ti.size_bytes,
            ti.group_id,
            ti.group_status,
            fc.akaze_status,
            fc.akaze_keypoints,
            fc.feature_version
        FROM target_items ti
        JOIN feature_cache fc
          ON fc.exact_hash = ti.exact_hash
         AND fc.size_bytes = ti.size_bytes
        WHERE fc.akaze_status = 'ready'
          AND fc.akaze_keypoints IS NOT NULL
          AND fc.akaze_keypoints <= ?
    """
    params: list[object] = [args.max_keypoints]

    if args.target_path_like:
        query += " AND (" + " OR ".join("ti.target_path LIKE ?" for _ in args.target_path_like) + ")"
        params.extend(args.target_path_like)

    if args.group_id:
        query += " AND (" + " OR ".join("ti.group_id = ?" for _ in args.group_id) + ")"
        params.extend(args.group_id)

    query += " ORDER BY fc.akaze_keypoints ASC, ti.target_path ASC"
    if args.limit > 0:
        query += " LIMIT ?"
        params.append(args.limit)

    return query, params


def load_candidates(conn: sqlite3.Connection, args: argparse.Namespace) -> list[CandidateRow]:
    query, params = build_query(args)
    rows = conn.execute(query, params).fetchall()
    return [
        CandidateRow(
            target_item_id=row[0],
            target_path=row[1],
            exact_hash=row[2],
            size_bytes=row[3],
            group_id=row[4],
            group_status=row[5],
            akaze_status=row[6],
            akaze_keypoints=row[7],
            feature_version=row[8],
        )
        for row in rows
    ]


def print_preview(rows: list[CandidateRow]) -> None:
    if not rows:
        print("No matching rows.")
        return

    print(f"Matched target_items: {len(rows)}")
    feature_keys = {(row.exact_hash, row.size_bytes) for row in rows}
    print(f"Distinct feature_cache keys: {len(feature_keys)}")
    print("")
    for row in rows:
        print(
            f"id={row.target_item_id} "
            f"group_id={row.group_id} "
            f"group_status={row.group_status} "
            f"akaze={row.akaze_status}/{row.akaze_keypoints} "
            f"size={row.size_bytes} "
            f"path={row.target_path}"
        )


def delete_rows(conn: sqlite3.Connection, rows: list[CandidateRow]) -> tuple[int, int]:
    target_ids = [row.target_item_id for row in rows]
    feature_keys = sorted({(row.exact_hash, row.size_bytes) for row in rows})

    with conn:
        deleted_target_items = 0
        for target_id in target_ids:
            deleted_target_items += conn.execute(
                "DELETE FROM target_items WHERE id = ?",
                (target_id,),
            ).rowcount

        deleted_feature_cache = 0
        for exact_hash, size_bytes in feature_keys:
            deleted_feature_cache += conn.execute(
                "DELETE FROM feature_cache WHERE exact_hash = ? AND size_bytes = ?",
                (exact_hash, size_bytes),
            ).rowcount

    return deleted_target_items, deleted_feature_cache


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)
    try:
        try:
            rows = load_candidates(conn, args)
        except sqlite3.OperationalError as err:
            print(
                f"query failed for {db_path}: {err}. "
                "Expected a photo-org catalog DB with target_items and feature_cache tables.",
                file=sys.stderr,
            )
            return 2
        print_preview(rows)
        if not args.apply:
            print("")
            print("Dry run only. Re-run with --apply to delete these rows.")
            return 0

        if not rows:
            return 0

        deleted_target_items, deleted_feature_cache = delete_rows(conn, rows)
        print("")
        print(
            "Deleted "
            f"target_items={deleted_target_items}, "
            f"feature_cache={deleted_feature_cache}"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
