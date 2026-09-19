from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from guangya_fastlink.models import (
    SourceRecord,
    atomic_write_export,
    iter_export_records,
)


SCHEMA_VERSION = 1
SQLITE_HEADER = b"SQLite format 3\x00"
MALFORMED_STATE_ERROR = "malformed import state"
MISMATCH_ERROR = "import state scope mismatch"


SCHEMA_SQL = """
CREATE TABLE job (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    target_parent_id TEXT NOT NULL,
    planning_complete INTEGER NOT NULL DEFAULT 0,
    total_files INTEGER NOT NULL DEFAULT 0,
    total_folders INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    not_reusable_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    last_flush_at TEXT
);

CREATE TABLE folders (
    folder_key TEXT PRIMARY KEY,
    parent_key TEXT NOT NULL,
    remote_folder_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT
);

CREATE TABLE files (
    record_key TEXT PRIMARY KEY,
    source_index INTEGER NOT NULL,
    path TEXT NOT NULL UNIQUE,
    file_name TEXT NOT NULL,
    relative_parent_dir TEXT NOT NULL,
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
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    retries INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX files_status_path ON files(status, path);
"""


@dataclass
class ImportState:
    connection: sqlite3.Connection
    path: Path
    workers: int = 5

    @property
    def scope(self) -> dict[str, str]:
        row = self.connection.execute(
            "SELECT source_sha256, target_parent_id FROM job WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise ValueError(MALFORMED_STATE_ERROR)
        return {"source_sha256": str(row[0]), "target_parent_id": str(row[1])}

    @property
    def target_parent_id(self) -> str:
        return self.scope["target_parent_id"]

    @property
    def stats(self) -> dict[str, int]:
        row = self.connection.execute(
            "SELECT total_files, completed_count, not_reusable_count, failed_count FROM job WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise ValueError(MALFORMED_STATE_ERROR)
        return {
            "total": int(row[0]),
            "completed": int(row[1]),
            "not_reusable": int(row[2]),
            "failed": int(row[3]),
        }

    def close(self) -> None:
        self.connection.close()

    def pending_folder_rows(self) -> list[tuple[str, str]]:
        rows = self.connection.execute(
            """
            SELECT folder_key, parent_key FROM folders WHERE status = 'pending'
            ORDER BY LENGTH(folder_key) - LENGTH(REPLACE(folder_key, '/', '')), folder_key
            """
        ).fetchall()
        return [(str(key), str(parent)) for key, parent in rows]

    def folder_map(self) -> dict[str, str]:
        result = {"": self.target_parent_id}
        rows = self.connection.execute(
            "SELECT folder_key, remote_folder_id FROM folders WHERE status = 'created'"
        ).fetchall()
        for key, remote_id in rows:
            if remote_id is None:
                raise ValueError(MALFORMED_STATE_ERROR)
            result[str(key)] = str(remote_id)
        return result

    def record_folder_created(self, folder_key: str, remote_id: str) -> None:
        self.connection.execute(
            "UPDATE folders SET status = 'created', remote_folder_id = ?, error = NULL WHERE folder_key = ?",
            (remote_id, folder_key),
        )
        self._touch_and_commit()

    def count_pending(self) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) FROM files WHERE status = 'pending'"
        ).fetchone()
        return int(row[0]) if row else 0

    def target_file_paths(self, *, include_retryable: bool) -> list[str]:
        statuses = (
            "('pending', 'failed', 'not_reusable')"
            if include_retryable
            else "('pending')"
        )
        rows = self.connection.execute(
            f"SELECT path FROM files WHERE status IN {statuses} ORDER BY path"
        ).fetchall()
        return [str(row[0]) for row in rows]

    def target_folder_paths(self) -> list[str]:
        rows = self.connection.execute(
            "SELECT folder_key FROM folders ORDER BY folder_key"
        ).fetchall()
        return [str(row[0]) for row in rows]

    def iter_pending_records(self, *, batch_size: int = 500):
        cursor = self.connection.execute(
            """
            SELECT source_index, path, file_name, relative_parent_dir, gcid, size,
                   source_file_id, source_parent_id, cid, whole_cid, triple_cid,
                   md5, download_url, source_guangya
            FROM files WHERE status = 'pending' ORDER BY path
            """
        )
        while rows := cursor.fetchmany(batch_size):
            for row in rows:
                yield SourceRecord(
                    source_index=int(row[0]),
                    path=str(row[1]),
                    file_name=str(row[2]),
                    relative_parent_dir=str(row[3]),
                    gcid=str(row[4]),
                    size=int(row[5]),
                    source_file_id=str(row[6]),
                    source_parent_id=str(row[7]),
                    cid=str(row[8]),
                    whole_cid=str(row[9]),
                    triple_cid=str(row[10]),
                    md5=str(row[11]),
                    download_url=str(row[12]),
                    source_guangya=bool(row[13]),
                )

    def flush_outcomes(self, outcomes: list[dict]) -> None:
        if not outcomes:
            return
        deltas = {"completed": 0, "not_reusable": 0, "failed": 0}
        with self.connection:
            for outcome in outcomes:
                status = str(outcome["status"])
                if status not in deltas:
                    raise ValueError(f"invalid terminal status: {status}")
                self.connection.execute(
                    "UPDATE files SET status = ?, error = ?, retries = ? WHERE record_key = ? AND status = 'pending'",
                    (
                        status,
                        outcome.get("error"),
                        int(outcome.get("retries", 0)),
                        str(outcome["record_key"]),
                    ),
                )
                if self.connection.execute("SELECT changes()").fetchone()[0]:
                    deltas[status] += 1
            self.connection.execute(
                """
                UPDATE job SET completed_count = completed_count + ?,
                    not_reusable_count = not_reusable_count + ?,
                    failed_count = failed_count + ?, last_flush_at = ?
                WHERE singleton = 1
                """,
                (
                    deltas["completed"],
                    deltas["not_reusable"],
                    deltas["failed"],
                    _now(),
                ),
            )

    def reset_retryable(self) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE files SET status = 'pending', error = NULL, retries = 0 WHERE status IN ('failed', 'not_reusable')"
            )
            self.connection.execute(
                "UPDATE job SET failed_count = 0, not_reusable_count = 0, last_flush_at = ? WHERE singleton = 1",
                (_now(),),
            )

    def write_retry_export(self, output_path: Path) -> bool:
        summary = self.connection.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files
            WHERE status IN ('failed', 'not_reusable')
            """
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
            FROM files WHERE status IN ('failed', 'not_reusable') ORDER BY path
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

    def _touch_and_commit(self) -> None:
        self.connection.execute(
            "UPDATE job SET last_flush_at = ? WHERE singleton = 1", (_now(),)
        )
        self.connection.commit()


def open_or_plan_import_state(
    *,
    state_path: Path,
    source_path: Path,
    source_sha256: str,
    target_parent_id: str,
) -> ImportState:
    exists = state_path.exists()
    if exists and not _is_sqlite(state_path):
        raise ValueError("unsupported state-file format")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(state_path, check_same_thread=False)
    try:
        if exists:
            _validate_existing(connection, source_sha256, target_parent_id)
        else:
            connection.executescript(SCHEMA_SQL)
            connection.execute(
                """
                INSERT INTO job (
                    singleton, schema_version, source_file, source_sha256,
                    target_parent_id, planning_complete
                ) VALUES (1, ?, ?, ?, ?, 0)
                """,
                (SCHEMA_VERSION, str(source_path), source_sha256, target_parent_id),
            )
            connection.commit()
        state = ImportState(connection=connection, path=state_path)
        planning_complete = connection.execute(
            "SELECT planning_complete FROM job WHERE singleton = 1"
        ).fetchone()
        if not planning_complete or not bool(planning_complete[0]):
            _plan_import(state=state, source_path=source_path)
        return state
    except BaseException:
        connection.close()
        if not exists:
            try:
                os.unlink(state_path)
            except OSError:
                pass
        raise


def _plan_import(*, state: ImportState, source_path: Path) -> None:
    connection = state.connection
    connection.execute("DELETE FROM folders")
    connection.execute("DELETE FROM files")
    folder_keys: set[str] = set()
    total = 0
    try:
        for record in iter_export_records(source_path):
            parent_parts = (
                record.relative_parent_dir.split("/")
                if record.relative_parent_dir
                else []
            )
            for index in range(len(parent_parts)):
                folder_key = "/".join(parent_parts[: index + 1])
                if folder_key in folder_keys:
                    continue
                parent_key = "/".join(parent_parts[:index])
                connection.execute(
                    "INSERT INTO folders (folder_key, parent_key) VALUES (?, ?)",
                    (folder_key, parent_key),
                )
                folder_keys.add(folder_key)
            connection.execute(
                """
                INSERT INTO files (
                    record_key, source_index, path, file_name, relative_parent_dir,
                    gcid, size, source_file_id, source_parent_id, cid, whole_cid,
                    triple_cid, md5, download_url, source_guangya
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.key,
                    record.source_index,
                    record.path,
                    record.file_name,
                    record.relative_parent_dir,
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
            "SELECT files.path FROM files JOIN folders ON folders.folder_key = files.path LIMIT 1"
        ).fetchone()
        if collision:
            raise ValueError(f"file-directory collision: {collision[0]}")
        connection.execute(
            """
            UPDATE job SET planning_complete = 1, total_files = ?, total_folders = ?,
                completed_count = 0, not_reusable_count = 0, failed_count = 0,
                last_flush_at = ? WHERE singleton = 1
            """,
            (total, len(folder_keys), _now()),
        )
        connection.commit()
    except sqlite3.IntegrityError as exc:
        connection.rollback()
        raise ValueError("duplicate normalized path") from exc
    except BaseException:
        connection.rollback()
        raise


def _validate_existing(
    connection: sqlite3.Connection, source_sha256: str, target_parent_id: str
) -> None:
    try:
        row = connection.execute(
            "SELECT schema_version, source_sha256, target_parent_id FROM job WHERE singleton = 1"
        ).fetchone()
    except sqlite3.DatabaseError as exc:
        raise ValueError(MALFORMED_STATE_ERROR) from exc
    if row is None or int(row[0]) != SCHEMA_VERSION:
        raise ValueError(MALFORMED_STATE_ERROR)
    if str(row[1]) != source_sha256 or str(row[2]) != target_parent_id:
        raise ValueError(MISMATCH_ERROR)


def _is_sqlite(path: Path) -> bool:
    if not path.is_file():
        return False
    with path.open("rb") as handle:
        return handle.read(len(SQLITE_HEADER)) == SQLITE_HEADER


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
