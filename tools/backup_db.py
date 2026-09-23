#!/usr/bin/env python3
"""
Database Backup

Takes a consistent snapshot of bookie_bot.db (safe while the bot is running),
verifies it, gzips it into the backup directory, and prunes old snapshots.

Usage:
    python -m tools.backup_db --dest /mnt/seagate/backups/bookie_bot
    python -m tools.backup_db --dest DIR --keep 30 --require-mount /mnt/seagate
"""

import argparse
import gzip
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from bot.db.database import DB_PATH

PREFIX = "bookie_bot-"
SUFFIX = ".db.gz"


def snapshot(src: str, dest_dir: Path) -> Path:
    """Copy src via the SQLite backup API, check integrity, and gzip into dest_dir."""
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    final = dest_dir / f"{PREFIX}{stamp}{SUFFIX}"

    with tempfile.TemporaryDirectory() as tmp:
        raw = os.path.join(tmp, "snapshot.db")
        with sqlite3.connect(src) as source, sqlite3.connect(raw) as target:
            source.backup(target)
            result = target.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            raise RuntimeError(f"Snapshot failed integrity check: {result}")

        partial = final.with_suffix(".partial")
        with open(raw, "rb") as f_in, gzip.open(partial, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        partial.rename(final)

    return final


def prune(dest_dir: Path, keep: int) -> list[Path]:
    """Delete all but the newest `keep` snapshots."""
    snapshots = sorted(dest_dir.glob(f"{PREFIX}*{SUFFIX}"))
    stale = snapshots[:-keep] if keep > 0 else []
    for path in stale:
        path.unlink()
    return stale


def main() -> int:
    parser = argparse.ArgumentParser(description="Back up the bookie bot database")
    parser.add_argument("--dest", required=True, type=Path, help="Backup directory")
    parser.add_argument("--keep", type=int, default=30, help="Snapshots to keep (default 30)")
    parser.add_argument("--require-mount", help="Abort unless this path is a mounted filesystem")
    args = parser.parse_args()

    if args.require_mount and not os.path.ismount(args.require_mount):
        print(f"ERROR: {args.require_mount} is not mounted; refusing to back up", file=sys.stderr)
        return 1
    if not os.path.exists(DB_PATH):
        print(f"ERROR: {DB_PATH} not found (run from the repo root)", file=sys.stderr)
        return 1

    args.dest.mkdir(parents=True, exist_ok=True)
    path = snapshot(DB_PATH, args.dest)
    print(f"Wrote {path} ({path.stat().st_size / 1e6:.1f} MB)")

    for stale in prune(args.dest, args.keep):
        print(f"Pruned {stale.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
