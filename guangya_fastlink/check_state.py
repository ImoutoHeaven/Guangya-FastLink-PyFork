from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from guangya_fastlink.models import atomic_write_export, iter_export_records


SCHEMA_VERSION = 2
SQLITE_HEADER = b"SQLite format 3\x00"
MALFORMED_STATE_ERROR = "malformed check state"
MISMATCH_ERROR = "check state scope mismatch"


SCHEMA_SQL = """
CREATE TABLE job (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    target_parent_id TEXT NOT NULL,
    compare_mode TEXT NOT NULL,
    planning_complete INTEGER NOT NULL DEFAULT 0,
    check_complete INTEGER NOT NULL DEFAULT 0,
    total_files INTEGER NOT NULL DEFAULT 0,
    total_dirs INTEGER NOT NULL DEFAULT 0,
    delta_files INTEGER NOT NULL DEFAULT 0,
    missing_dirs INTEGER NOT NULL DEFAULT 0,
    last_flush_at TEXT
);

CREATE TABLE dirs (
    path TEXT PRIMARY KEY,
    parent_path TEXT NOT NULL,
    name TEXT NOT NULL,
    remote_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE files (
    path TEXT PRIMARY KEY,
    parent_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    gcid TEXT NOT NULL,
    size INTEGER NOT NULL,
    source_file_id TEXT NOT NULL,
    source_parent_id TEXT NOT NULL,
    cid TEXT NOT NULL,
    whole_cid TEXT NOT NULL,
    triple_cid TEXT NOT NULL,
    md5 TEXT NOT NULL,
    download_url TEXT NOT NULL,
    source_guangya INTEGER NOT NULL,
    remote_scanned INTEGER NOT NULL DEFAULT 0,
    remote_present INTEGER NOT NULL DEFAULT 0,
    remote_res_type INTEGER,
    remote_gcid TEXT,
    remote_size INTEGER,
    status TEXT NOT NULL DEFAULT 'pending'
);
"""


@dataclass
class CheckState:
    connection: sqlite3.Connection
    path: Path

    @property
    def scope(self) -> dict[str, str]:
        row = self.connection.execute(
            "SELECT source_sha256, target_parent_id, compare_mode FROM job WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise ValueError(MALFORMED_STATE_ERROR)
        return {
            "source_sha256": str(row[0]),
            "target_parent_id": str(row[1]),
            "compare_mode": str(row[2]),
        }

    @property
    def complete(self) -> bool:
        row = self.connection.execute(
            "SELECT check_complete FROM job WHERE singleton = 1"
        ).fetchone()
        return bool(row and row[0])

    def close(self) -> None:
        self.connection.close()

    def directory_rows(self):
        return self.connection.execute(
            """
            SELECT path, parent_path, name, remote_id, status FROM dirs
            ORDER BY LENGTH(path) - LENGTH(REPLACE(path, '/', '')), path
            """
        ).fetchall()

    def mark_directory(self, path: str, *, status: str, remote_id: str | None) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE dirs SET status = ?, remote_id = ? WHERE path = ?",
                (status, remote_id, path),
            )
            self.connection.execute(
                "UPDATE job SET last_flush_at = ? WHERE singleton = 1", (_now(),)
            )

    def pending_file_batches(self, *, batch_size: int = 500):
        while rows := self.connection.execute(
            """
            SELECT path, parent_path, file_name, gcid, size FROM files
            WHERE remote_scanned = 0 ORDER BY path LIMIT ?
            """,
            (batch_size,),
        ).fetchall():
            yield rows

    def record_remote_files(self, rows: list[tuple]) -> None:
        if not rows:
            return
        with self.connection:
            self.connection.executemany(
                """
                UPDATE files SET remote_scanned = 1, remote_present = ?,
                    remote_res_type = ?, remote_gcid = ?, remote_size = ?
                WHERE path = ?
                """,
                [
                    (
                        int(bool(present)),
                        res_type,
                        remote_gcid,
                        remote_size,
                        path,
                    )
                    for path, present, res_type, remote_gcid, remote_size in rows
                ],
            )
            self.connection.execute(
                "UPDATE job SET last_flush_at = ? WHERE singleton = 1", (_now(),)
            )

    def finish(self) -> dict[str, int]:
        unscanned = self.connection.execute(
            "SELECT COUNT(*) FROM files WHERE remote_scanned = 0"
        ).fetchone()[0]
        if unscanned:
            raise RuntimeError("remote check snapshot is incomplete")
        compare_mode = self.scope["compare_mode"]
        with self.connection:
            if compare_mode == "exist_only":
                self.connection.execute(
                    """
                    UPDATE files SET status = CASE
                        WHEN remote_present = 1 AND remote_res_type = 1
                        THEN 'aligned' ELSE 'delta' END
                    """
                )
            else:
                self.connection.execute(
                    """
                    UPDATE files SET status = CASE
                        WHEN remote_present = 1 AND remote_res_type = 1
                            AND UPPER(remote_gcid) = UPPER(gcid)
                            AND remote_size = size
                        THEN 'aligned' ELSE 'delta' END
                    """
                )
        delta = self.connection.execute(
            "SELECT COUNT(*) FROM files WHERE status = 'delta'"
        ).fetchone()[0]
        missing = self.connection.execute(
            "SELECT COUNT(*) FROM dirs WHERE status = 'missing'"
        ).fetchone()[0]
        with self.connection:
            self.connection.execute(
                """
                UPDATE job SET check_complete = 1, delta_files = ?, missing_dirs = ?,
                    last_flush_at = ? WHERE singleton = 1
                """,
                (delta, missing, _now()),
            )
        return {"delta_files": int(delta), "missing_dirs": int(missing)}

    def summary(self) -> dict[str, int]:
        row = self.connection.execute(
            "SELECT delta_files, missing_dirs FROM job WHERE singleton = 1"
        ).fetchone()
        return {"delta_files": int(row[0]), "missing_dirs": int(row[1])}

    def write_delta(self, output_path: Path) -> bool:
        summary = self.connection.execute(
            "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files WHERE status = 'delta'"
        ).fetchone()
        total_files, total_size = int(summary[0]), int(summary[1])
        if not total_files:
            if output_path.exists():
                output_path.unlink()
            return False
        rows = self.connection.execute(
            """
            SELECT path, gcid, size, source_file_id, source_parent_id, cid,
                   whole_cid, triple_cid, md5, download_url, source_guangya
            FROM files WHERE status = 'delta' ORDER BY path
            """
        )

        def records():
            for row in rows:
                yield {
                    "path": row[0],
                    "gcid": row[1],
                    "size": row[2],
                    "source_file_id": row[3],
                    "source_parent_id": row[4],
                    "cid": row[5],
                    "whole_cid": row[6],
                    "triple_cid": row[7],
                    "md5": row[8],
                    "download_url": row[9],
                    "source_guangya": bool(row[10]),
                }

        atomic_write_export(
            output_path,
            records=records(),
            total_files=total_files,
            total_size=total_size,
        )
        return True


def open_or_plan_check_state(
    *,
    state_path: Path,
    source_path: Path,
    source_sha256: str,
    target_parent_id: str,
    compare_mode: str,
) -> CheckState:
    exists = state_path.exists()
    if exists and not _is_sqlite(state_path):
        raise ValueError("unsupported state-file format")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(state_path)
    try:
        if exists:
            row = connection.execute(
                "SELECT schema_version, source_sha256, target_parent_id, compare_mode FROM job WHERE singleton = 1"
            ).fetchone()
            if row is None or int(row[0]) != SCHEMA_VERSION:
                raise ValueError(MALFORMED_STATE_ERROR)
            if str(row[1]) != source_sha256 or str(row[2]) != target_parent_id:
                raise ValueError(MISMATCH_ERROR)
            if str(row[3]) != compare_mode:
                with connection:
                    connection.execute(
                        """
                        UPDATE job SET compare_mode = ?, check_complete = 0,
                            delta_files = 0, missing_dirs = 0, last_flush_at = ?
                        WHERE singleton = 1
                        """,
                        (compare_mode, _now()),
                    )
        else:
            connection.executescript(SCHEMA_SQL)
            connection.execute(
                """
                INSERT INTO job (
                    singleton, schema_version, source_file, source_sha256,
                    target_parent_id, compare_mode
                ) VALUES (1, ?, ?, ?, ?, ?)
                """,
                (
                    SCHEMA_VERSION,
                    str(source_path),
                    source_sha256,
                    target_parent_id,
                    compare_mode,
                ),
            )
            connection.commit()
        state = CheckState(connection=connection, path=state_path)
        planned = connection.execute(
            "SELECT planning_complete FROM job WHERE singleton = 1"
        ).fetchone()[0]
        if not planned:
            _plan(state, source_path)
        return state
    except BaseException:
        connection.close()
        if not exists:
            try:
                os.unlink(state_path)
            except OSError:
                pass
        raise


def _plan(state: CheckState, source_path: Path) -> None:
    connection = state.connection
    dirs: set[str] = set()
    total = 0
    try:
        for record in iter_export_records(source_path):
            parent_parts = (
                record.relative_parent_dir.split("/")
                if record.relative_parent_dir
                else []
            )
            for index in range(len(parent_parts)):
                path = "/".join(parent_parts[: index + 1])
                if path in dirs:
                    continue
                connection.execute(
                    "INSERT INTO dirs (path, parent_path, name) VALUES (?, ?, ?)",
                    (path, "/".join(parent_parts[:index]), parent_parts[index]),
                )
                dirs.add(path)
            connection.execute(
                """
                INSERT INTO files (
                    path, parent_path, file_name, gcid, size, source_file_id,
                    source_parent_id, cid, whole_cid, triple_cid, md5,
                    download_url, source_guangya
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.path,
                    record.relative_parent_dir,
                    record.file_name,
                    record.gcid,
                    record.size,
                    record.source_file_id,
                    record.source_parent_id,
                    record.cid,
                    record.whole_cid,
                    record.triple_cid,
                    record.md5,
                    record.download_url,
                    int(record.source_guangya),
                ),
            )
            total += 1
        if total == 0:
            raise ValueError("files must not be empty")
        collision = connection.execute(
            "SELECT files.path FROM files JOIN dirs ON dirs.path = files.path LIMIT 1"
        ).fetchone()
        if collision:
            raise ValueError(f"file-directory collision: {collision[0]}")
        connection.execute(
            """
            UPDATE job SET planning_complete = 1, total_files = ?, total_dirs = ?,
                last_flush_at = ? WHERE singleton = 1
            """,
            (total, len(dirs), _now()),
        )
        connection.commit()
    except sqlite3.IntegrityError as exc:
        connection.rollback()
        raise ValueError("duplicate normalized path") from exc
    except BaseException:
        connection.rollback()
        raise


def _is_sqlite(path: Path) -> bool:
    if not path.is_file():
        return False
    with path.open("rb") as handle:
        return handle.read(len(SQLITE_HEADER)) == SQLITE_HEADER


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
