"""One-shot ETL run, designed to be started by Windows Task Scheduler.

What one run does:
  1. Takes a lock so two runs can never overlap.
  2. Connects to the hosted PostgreSQL (retries, in case the PC just woke up).
  3. Creates / updates the schema from sql/schema.sql.
  4. Imports every NEW or CHANGED .xlsx in WATCH_DIR, oldest export first.
  5. Exits (exit code 0 = ok, 1 = at least one file failed).

Usage:
    python -m ekal_pipeline.run_once              # normal scheduled run
    python -m ekal_pipeline.run_once --init-db    # only create the schema
    python -m ekal_pipeline.run_once --force      # re-import files already done
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ekal_pipeline.pipeline import (
    ROOT,
    apply_schema,
    create_database_engine,
    import_workbook,
    logger,
)


def configured_path(name: str, default: str) -> Path:
    value = Path(os.getenv(name, default))
    return value if value.is_absolute() else (ROOT / value).resolve()


WATCH_DIR = configured_path("WATCH_DIR", "data/incoming")
STATE_DIR = configured_path("STATE_DIR", ".state")
STATE_FILE = STATE_DIR / "last_processed.json"
LOCK_FILE = STATE_DIR / "run.lock"

MIN_FILE_AGE_SECONDS = 10   # a file modified more recently than this may still be downloading
DB_CONNECT_ATTEMPTS = 4
DB_CONNECT_WAIT_SECONDS = 30


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_export_date(path: Path) -> Optional[datetime]:
    """Read the export time from names like ``..._DD-MM-YYYY-HH.MM_Ekal.xlsx``.

    The time is treated as UTC. That is not necessarily the real timezone, but
    it is applied identically to every file, so the ordering stays correct.
    """
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})-(\d{2})\.(\d{2})", path.name)
    if not match:
        return None
    day, month, year, hour, minute = (int(g) for g in match.groups())
    try:
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None


def export_timestamp(path: Path) -> datetime:
    """Timestamp used as ``_last_import_ts``: the export date from the file
    name, or the file's modified time when the name has no date."""
    return extract_export_date(path) or datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def sort_files_by_export_date(paths: list[Path]) -> list[Path]:
    """Oldest export first, so a newer export is always applied last."""
    keyed = []
    for p in paths:
        try:
            keyed.append((export_timestamp(p), p.name, p))
        except OSError:
            continue
    keyed.sort(key=lambda item: (item[0], item[1]))
    return [p for _, _, p in keyed]


def candidate_files() -> list[Path]:
    return [
        p for p in WATCH_DIR.glob("*.xlsx")
        if p.is_file() and not p.name.startswith("~$")   # skip Excel's temp lock files
    ]


def is_ready(path: Path) -> bool:
    """True when the file is fully written and not locked by another program."""
    try:
        if time.time() - path.stat().st_mtime < MIN_FILE_AGE_SECONDS:
            logger.warning("Skipping %s for now: modified in the last %ds (still downloading?).",
                           path.name, MIN_FILE_AGE_SECONDS)
            return False
        with path.open("rb") as stream:
            stream.read(1024)
        return True
    except OSError as exc:
        logger.warning("Skipping %s for now: cannot read it (%s). Is it open in Excel?", path.name, exc)
        return False


# ---------------------------------------------------------------------------
# State: remembers which files were already imported (by name + content hash)
# ---------------------------------------------------------------------------

def target_fingerprint(engine) -> str:
    """Identifies WHICH database we are loading into (password excluded).

    If you ever point the ETL at a different / emptied database, the saved
    state no longer applies and every file is imported again automatically.
    """
    return hashlib.sha256(engine.url.render_as_string(hide_password=True).encode()).hexdigest()[:16]


def read_state(fingerprint: str) -> dict:
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if data.get("target") != fingerprint:
        if data:
            logger.info("Database target changed since the last run; re-importing all files.")
        return {}
    files = data.get("files")
    return files if isinstance(files, dict) else {}


def write_state(fingerprint: str, files: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps({"target": fingerprint, "files": files}, indent=2), encoding="utf-8")
    temp.replace(STATE_FILE)


# ---------------------------------------------------------------------------
# Single-run lock (released automatically by the OS if the process dies)
# ---------------------------------------------------------------------------

class RunLock:
    def __init__(self, path: Path):
        self.path = path
        self._handle = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists() or self.path.stat().st_size == 0:
            self.path.write_bytes(b"0")          # the lock needs at least one byte to lock
        handle = open(self.path, "r+b")
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            return False
        self._handle = handle
        return True

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            self._handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._handle, fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def wait_for_database(engine) -> None:
    """Retry the first connection: right after the PC wakes up, Wi-Fi may not be ready."""
    for attempt in range(1, DB_CONNECT_ATTEMPTS + 1):
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            return
        except Exception as exc:
            if attempt == DB_CONNECT_ATTEMPTS:
                raise
            logger.warning("Database not reachable (attempt %d/%d): %s. Retrying in %ds.",
                           attempt, DB_CONNECT_ATTEMPTS, str(exc).splitlines()[0], DB_CONNECT_WAIT_SECONDS)
            time.sleep(DB_CONNECT_WAIT_SECONDS)


def run(force: bool = False, init_only: bool = False) -> int:
    lock = RunLock(LOCK_FILE)
    if not lock.acquire():
        logger.info("Another ETL run is still in progress; exiting without doing anything.")
        return 0

    engine = None
    try:
        WATCH_DIR.mkdir(parents=True, exist_ok=True)
        engine = create_database_engine()
        wait_for_database(engine)
        apply_schema(engine)
        if init_only:
            return 0

        fingerprint = target_fingerprint(engine)
        done = {} if force else read_state(fingerprint)
        files = sort_files_by_export_date(candidate_files())
        if not files:
            logger.info("No .xlsx files found in %s.", WATCH_DIR)
            return 0

        imported = skipped = failed = waiting = 0
        for path in files:
            if not is_ready(path):
                waiting += 1
                continue
            try:
                digest = sha256_file(path)
                if done.get(path.name) == digest:
                    skipped += 1
                    continue
                logger.info("Importing %s", path.name)
                import_workbook(path, engine, import_ts=export_timestamp(path))
                done[path.name] = digest
                write_state(fingerprint, done)      # save progress after every file
                imported += 1
            except Exception:
                failed += 1
                logger.exception("Could not import %s (it will be retried on the next run).", path.name)

        logger.info("Run finished: imported=%d, unchanged=%d, waiting=%d, failed=%d",
                    imported, skipped, waiting, failed)
        return 1 if failed else 0
    except Exception:
        logger.exception("ETL run failed before importing anything.")
        return 1
    finally:
        if engine is not None:
            engine.dispose()
        lock.release()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import new/changed Ekal workbooks into PostgreSQL, then exit.")
    parser.add_argument("--init-db", action="store_true", help="Only create the schema, then exit.")
    parser.add_argument("--force", action="store_true", help="Re-import files even if already imported.")
    args = parser.parse_args()
    sys.exit(run(force=args.force, init_only=args.init_db))


if __name__ == "__main__":
    main()
