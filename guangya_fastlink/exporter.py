from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path

from guangya_fastlink.api import DecisionKind
from guangya_fastlink.models import (
    normalize_gcid,
    normalize_remote_id,
    normalize_relative_path,
    parse_size,
    write_export_json,
)
from guangya_fastlink.runner import call_with_retries, safe_print


@dataclass(frozen=True)
class DirectoryTask:
    file_id: str
    relative_dir: str


@dataclass
class ExportState:
    source_parent_id: str
    output_file: str
    records_file: str
    pending_dirs: list[dict]
    seen_dir_ids: list[str]
    files_written: int = 0
    dirs_completed: int = 0
    output_committed: bool = False


def run_export(*, client, config) -> int:
    state_path = config.state_file
    records_path = state_path.with_suffix(".records.jsonl")
    finalize_db = state_path.with_suffix(".finalize.sqlite3")
    temp_output = config.output_file.with_name(f".{config.output_file.name}.tmp")
    state = _load_or_initialize_state(
        state_path=state_path,
        records_path=records_path,
        source_parent_id=config.source_parent_id,
        output_file=config.output_file,
    )
    if state.output_committed:
        if not config.output_file.exists():
            raise ValueError("committed export output is missing")
        _cleanup_export_artifacts(records_path, finalize_db, temp_output, state_path)
        return 0

    _prepare_sidecar(records_path)
    try:
        while state.pending_dirs:
            tasks = [
                DirectoryTask(**item) for item in state.pending_dirs[: config.workers]
            ]
            first_error: BaseException | None = None
            with ThreadPoolExecutor(max_workers=config.workers) as executor:
                futures = {
                    executor.submit(
                        _scan_directory,
                        client=client,
                        task=task,
                        max_retries=config.max_retries,
                    ): task
                    for task in tasks
                }
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        child_dirs, records = future.result()
                        _commit_directory(
                            state=state,
                            state_path=state_path,
                            records_path=records_path,
                            task=task,
                            child_dirs=child_dirs,
                            records=records,
                        )
                        safe_print(
                            f"Export progress: dirs={state.dirs_completed} files={state.files_written}"
                        )
                    except BaseException as exc:
                        if first_error is None:
                            first_error = exc
            if first_error is not None:
                raise first_error
        if state.files_written == 0:
            raise RuntimeError("zero files exported")
        _finalize_export(
            state=state,
            state_path=state_path,
            records_path=records_path,
            finalize_db=finalize_db,
            temp_output=temp_output,
            output_file=config.output_file,
        )
    except KeyboardInterrupt:
        _flush_state(state_path, state)
        raise

    _cleanup_export_artifacts(records_path, finalize_db, temp_output, state_path)
    return 0


def _scan_directory(*, client, task: DirectoryTask, max_retries: int):
    items = []
    page = 0
    while True:
        decision = call_with_retries(
            lambda: client.list_page(parent_id=task.file_id, page=page),
            max_retries=max_retries,
        )
        if decision.kind is DecisionKind.CREDENTIAL_FATAL:
            raise RuntimeError(decision.error or "credential failure")
        if decision.kind is not DecisionKind.COMPLETED:
            raise RuntimeError(decision.error or "directory scan failed")
        page_items = decision.payload["items"]
        total = decision.payload["total"]
        items.extend(page_items)
        if not page_items or len(items) >= total:
            break
        page += 1

    child_dirs = []
    records = []
    for item in items:
        if not isinstance(item, dict):
            raise RuntimeError("invalid file-list item")
        name = item.get("fileName")
        try:
            file_id = normalize_remote_id(item.get("fileId"))
        except ValueError as exc:
            raise RuntimeError("file-list item missing name or id") from exc
        if not isinstance(name, str) or not name:
            raise RuntimeError("file-list item missing name or id")
        relative_path = name if not task.relative_dir else f"{task.relative_dir}/{name}"
        relative_path = normalize_relative_path(relative_path)
        if item.get("resType") == 2:
            child_dirs.append(
                DirectoryTask(file_id=str(file_id), relative_dir=relative_path)
            )
            continue
        if item.get("resType") != 1:
            raise RuntimeError(f"unknown resType for {relative_path}")
        records.append(
            {
                "path": relative_path,
                "gcid": normalize_gcid(item.get("gcid")),
                "size": parse_size(item.get("fileSize")),
                "source_file_id": str(file_id),
                "source_parent_id": str(item.get("parentId", task.file_id)),
                "source_guangya": True,
            }
        )
    return child_dirs, records


def _commit_directory(
    *,
    state: ExportState,
    state_path: Path,
    records_path: Path,
    task: DirectoryTask,
    child_dirs: list[DirectoryTask],
    records: list[dict],
) -> None:
    with records_path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())

    state.pending_dirs.remove(asdict(task))
    seen = set(state.seen_dir_ids)
    for child in child_dirs:
        if child.file_id in seen:
            continue
        state.pending_dirs.append(asdict(child))
        state.seen_dir_ids.append(child.file_id)
        seen.add(child.file_id)
    state.files_written += len(records)
    state.dirs_completed += 1
    _flush_state(state_path, state)


def _finalize_export(
    *,
    state: ExportState,
    state_path: Path,
    records_path: Path,
    finalize_db: Path,
    temp_output: Path,
    output_file: Path,
) -> None:
    for path in (finalize_db, temp_output):
        if path.exists():
            path.unlink()
    connection = sqlite3.connect(finalize_db)
    try:
        connection.execute(
            """
            CREATE TABLE records (
                path TEXT PRIMARY KEY, gcid TEXT NOT NULL, size INTEGER NOT NULL,
                source_file_id TEXT NOT NULL, source_parent_id TEXT NOT NULL,
                source_guangya INTEGER NOT NULL
            )
            """
        )
        with records_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                record = json.loads(raw_line)
                normalized = (
                    normalize_relative_path(record.get("path")),
                    normalize_gcid(record.get("gcid")),
                    parse_size(record.get("size")),
                    str(record.get("source_file_id", "")),
                    str(record.get("source_parent_id", "")),
                    int(bool(record.get("source_guangya", False))),
                )
                existing = connection.execute(
                    "SELECT gcid, size, source_file_id, source_parent_id, source_guangya FROM records WHERE path = ?",
                    (normalized[0],),
                ).fetchone()
                if existing is not None:
                    if tuple(existing) != normalized[1:]:
                        raise RuntimeError(
                            f"source tree changed during export: {normalized[0]}"
                        )
                    continue
                connection.execute(
                    "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?)", normalized
                )
        connection.commit()
        total_files, total_size = connection.execute(
            "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM records"
        ).fetchone()
        if not total_files:
            raise RuntimeError("zero files exported")

        def rows():
            cursor = connection.execute(
                "SELECT path, gcid, size, source_file_id, source_parent_id, source_guangya FROM records ORDER BY path"
            )
            for row in cursor:
                yield {
                    "path": row[0],
                    "gcid": row[1],
                    "size": row[2],
                    "source_file_id": row[3],
                    "source_parent_id": row[4],
                    "source_guangya": bool(row[5]),
                }

        temp_output.parent.mkdir(parents=True, exist_ok=True)
        with temp_output.open("w", encoding="utf-8") as handle:
            write_export_json(
                handle,
                records=rows(),
                total_files=int(total_files),
                total_size=int(total_size),
            )
            handle.flush()
            os.fsync(handle.fileno())
        output_file.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temp_output, output_file)
    finally:
        connection.close()
    state.output_committed = True
    _flush_state(state_path, state)


def _load_or_initialize_state(
    *,
    state_path: Path,
    records_path: Path,
    source_parent_id: str,
    output_file: Path,
) -> ExportState:
    expected_output = str(output_file.resolve())
    expected_records = str(records_path.resolve())
    if state_path.exists():
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            state = ExportState(**payload)
        except Exception as exc:
            raise ValueError("malformed export state") from exc
        if (
            state.source_parent_id != source_parent_id
            or state.output_file != expected_output
            or state.records_file != expected_records
            or (not state.output_committed and not records_path.exists())
        ):
            raise ValueError("export state scope mismatch")
        return state
    if records_path.exists():
        raise ValueError("orphan export sidecar")
    state = ExportState(
        source_parent_id=source_parent_id,
        output_file=expected_output,
        records_file=expected_records,
        pending_dirs=[asdict(DirectoryTask(source_parent_id, ""))],
        seen_dir_ids=[source_parent_id],
    )
    records_path.parent.mkdir(parents=True, exist_ok=True)
    records_path.touch()
    _flush_state(state_path, state)
    return state


def _prepare_sidecar(path: Path) -> None:
    with path.open("r+b") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        if position == 0:
            return
        handle.seek(-1, os.SEEK_END)
        if handle.read(1) == b"\n":
            return
        while position > 0:
            start = max(0, position - 8192)
            handle.seek(start)
            chunk = handle.read(position - start)
            newline = chunk.rfind(b"\n")
            if newline >= 0:
                handle.truncate(start + newline + 1)
                return
            position = start
        handle.truncate(0)


def _flush_state(path: Path, state: ExportState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(asdict(state), handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def _cleanup_export_artifacts(*paths: Path) -> None:
    cleanup_failed = False
    for path in paths[:-1]:
        try:
            if path.exists():
                path.unlink()
        except OSError as exc:
            cleanup_failed = True
            safe_print(f"Cleanup warning: {path}: {exc}")
    if cleanup_failed:
        return
    state_path = paths[-1]
    try:
        if state_path.exists():
            state_path.unlink()
    except OSError as exc:
        safe_print(f"Cleanup warning: {state_path}: {exc}")
