from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from guangya_fastlink.check_state import open_or_plan_check_state
from guangya_fastlink.import_state import open_or_plan_import_state
from guangya_fastlink.models import inspect_export_scope, normalize_gcid, parse_size
from guangya_fastlink.runner import (
    CredentialFatalError,
    RemoteTreeCoordinator,
    create_remote_directories,
    run_import_files,
    safe_print,
)


@dataclass(frozen=True)
class BatchJob:
    json_path: Path
    relative_path: Path
    state_path: Path
    output_path: Path


def discover_jobs(
    *, input_dir: Path, state_dir: Path, output_dir: Path | None = None, suffix: str
) -> list[BatchJob]:
    input_dir = input_dir.resolve()
    state_dir = state_dir.resolve()
    shared_artifact_root = output_dir is None
    output_dir = output_dir.resolve() if output_dir else state_dir
    if _paths_overlap(input_dir, state_dir) or _paths_overlap(input_dir, output_dir):
        raise ValueError("input, state, and output directories must be disjoint")
    if not shared_artifact_root and _paths_overlap(state_dir, output_dir):
        raise ValueError("input, state, and output directories must be disjoint")
    paths = sorted(
        (path for path in input_dir.rglob("*.json") if path.is_file()),
        key=lambda path: path.relative_to(input_dir).as_posix(),
    )
    if not paths:
        raise ValueError("no json files found")
    jobs = []
    artifact_paths: set[Path] = set(paths)
    for path in paths:
        relative = path.relative_to(input_dir)
        state_path = state_dir / relative.with_suffix(suffix)
        if output_dir == state_dir:
            artifact_path = state_path.with_suffix(".retry.export.json")
        else:
            artifact_path = output_dir / relative.with_suffix(".delta.export.json")
        if state_path in artifact_paths or artifact_path in artifact_paths:
            raise ValueError(f"batch artifact collision: {relative.as_posix()}")
        artifact_paths.update({state_path, artifact_path})
        jobs.append(
            BatchJob(
                json_path=path,
                relative_path=relative,
                state_path=state_path,
                output_path=artifact_path,
            )
        )
    return jobs


def run_batch_import(*, config, client) -> int:
    try:
        jobs = discover_jobs(
            input_dir=config.input_dir,
            state_dir=config.state_dir,
            suffix=".import-state.sqlite3",
        )
        config.state_dir.mkdir(parents=True, exist_ok=True)
        planned, preflight_failed = _preflight_import(jobs=jobs, config=config)
    except (ValueError, OSError) as exc:
        safe_print(f"Batch startup failed: {exc}")
        return 1
    safe_print(f"Batch startup: jobs={len(jobs)}")
    if config.dry_run:
        safe_print("Dry run: no remote mutations performed")
        return 1 if preflight_failed else 0
    coordinator = RemoteTreeCoordinator(client)

    def child(job: BatchJob):
        state = None
        try:
            scope = inspect_export_scope(job.json_path)
            state = open_or_plan_import_state(
                state_path=job.state_path,
                source_path=job.json_path,
                source_sha256=scope.source_sha256,
                target_parent_id=config.target_parent_id,
            )
            state.workers = config.workers
            if config.retry_failed:
                state.reset_retryable()
            create_remote_directories(
                state=state,
                coordinator=coordinator,
                max_retries=config.max_retries,
            )
            summary = run_import_files(
                state=state,
                client=client,
                coordinator=coordinator,
                max_retries=config.max_retries,
                flush_every=config.flush_every,
            )
            stats = state.stats
            state.write_retry_export(job.output_path)
            failed = bool(summary["credential_fatal"] or stats["failed"])
            return {
                "failed": failed,
                "credential_fatal": bool(summary["credential_fatal"]),
                "not_reusable": stats["not_reusable"],
            }
        except CredentialFatalError:
            return {"failed": True, "credential_fatal": True, "not_reusable": 0}
        except Exception as exc:
            safe_print(f"Batch job failed: {job.relative_path.as_posix()}: {exc}")
            return {"failed": True, "credential_fatal": False, "not_reusable": 0}
        finally:
            if state is not None:
                state.close()

    summary = _run_parallel(
        jobs=planned, parallelism=config.json_parallelism, child=child
    )
    summary["failed"] += preflight_failed
    safe_print(
        f"Batch summary: total={len(jobs)} completed={summary['completed']} "
        f"completed_with_not_reusable={summary['completed_with_not_reusable']} "
        f"failed={summary['failed']} not_reusable_files={summary['not_reusable']}"
    )
    return 1 if summary["failed"] or summary["credential_fatal"] else 0


def _preflight_import(*, jobs: list[BatchJob], config) -> tuple[list[BatchJob], int]:
    planned = []
    files_by_job: dict[BatchJob, list[str]] = {}
    folders_by_job: dict[BatchJob, list[str]] = {}
    failed = 0
    for job in jobs:
        state = None
        try:
            scope = inspect_export_scope(job.json_path)
            state = open_or_plan_import_state(
                state_path=job.state_path,
                source_path=job.json_path,
                source_sha256=scope.source_sha256,
                target_parent_id=config.target_parent_id,
            )
            files_by_job[job] = state.target_file_paths(
                include_retryable=config.retry_failed
            )
            folders_by_job[job] = state.target_folder_paths()
            planned.append(job)
        except Exception as exc:
            failed += 1
            safe_print(f"Batch preflight failed: {job.relative_path.as_posix()}: {exc}")
        finally:
            if state is not None:
                state.close()
    files: set[str] = set()
    folders = {path for job in planned for path in folders_by_job.get(job, [])}
    for job in planned:
        for path in files_by_job.get(job, []):
            if path in files:
                raise ValueError(f"target path collision: {path}")
            files.add(path)
    for path in files:
        if path in folders or any(parent in files for parent in _parents(path)):
            raise ValueError(f"file-directory collision: {path}")
    return planned, failed


def run_batch_check(*, config, client) -> int:
    try:
        jobs = discover_jobs(
            input_dir=config.input_dir,
            state_dir=config.state_dir,
            output_dir=config.output_dir,
            suffix=".check-state.sqlite3",
        )
        config.state_dir.mkdir(parents=True, exist_ok=True)
        config.output_dir.mkdir(parents=True, exist_ok=True)
    except (ValueError, OSError) as exc:
        safe_print(f"Batch startup failed: {exc}")
        return 1
    coordinator = RemoteTreeCoordinator(client)

    def child(job: BatchJob):
        state = None
        try:
            scope = inspect_export_scope(job.json_path)
            state = open_or_plan_check_state(
                state_path=job.state_path,
                source_path=job.json_path,
                source_sha256=scope.source_sha256,
                target_parent_id=config.target_parent_id,
                compare_mode=config.compare_mode,
            )
            if not state.complete:
                _run_check(
                    state=state,
                    coordinator=coordinator,
                    target_parent_id=config.target_parent_id,
                    max_retries=config.max_retries,
                    workers=config.workers,
                )
            summary = state.summary()
            state.write_delta(job.output_path)
            return {
                "failed": False,
                "credential_fatal": False,
                **summary,
            }
        except CredentialFatalError:
            return {"failed": True, "credential_fatal": True}
        except Exception as exc:
            safe_print(f"Check job failed: {job.relative_path.as_posix()}: {exc}")
            return {"failed": True, "credential_fatal": False}
        finally:
            if state is not None:
                state.close()

    summary = _run_parallel(jobs=jobs, parallelism=config.json_parallelism, child=child)
    safe_print(
        f"Batch check summary: total={len(jobs)} completed={summary['completed']} "
        f"with_delta={summary['jobs_with_delta']} failed={summary['failed']} "
        f"delta_files={summary['delta_files']} missing_dirs={summary['missing_dirs']}"
    )
    return 1 if summary["failed"] or summary["credential_fatal"] else 0


def _run_check(
    *,
    state,
    coordinator,
    target_parent_id: str,
    max_retries: int,
    workers: int,
) -> None:
    directory_ids = {"": target_parent_id}
    directory_status = {"": "present"}
    for path, parent_path, name, remote_id, status in state.directory_rows():
        path = str(path)
        parent_path = str(parent_path)
        status = str(status)
        if status == "present" and remote_id is not None:
            directory_ids[path] = str(remote_id)
            directory_status[path] = "present"
            continue
        if status == "missing" or directory_status.get(parent_path) == "missing":
            state.mark_directory(path, status="missing", remote_id=None)
            directory_status[path] = "missing"
            continue
        parent_id = directory_ids[parent_path]
        found = coordinator.find_directory(
            parent_id=parent_id, name=str(name), max_retries=max_retries
        )
        if found is None:
            state.mark_directory(path, status="missing", remote_id=None)
            directory_status[path] = "missing"
        else:
            state.mark_directory(path, status="present", remote_id=found)
            directory_status[path] = "present"
            directory_ids[path] = found

    def scan_file(row):
        path, parent_path, file_name, _gcid, _size = row
        parent_path = str(parent_path)
        if directory_status.get(parent_path) == "missing":
            return str(path), False, None, None, None
        item = coordinator.get_child(
            parent_id=directory_ids[parent_path],
            name=str(file_name),
            max_retries=max_retries,
        )
        if item is None:
            return str(path), False, None, None, None
        remote_gcid = None
        remote_size = None
        if item["resType"] == 1:
            try:
                remote_gcid = normalize_gcid(item.get("gcid"))
                remote_size = parse_size(item.get("fileSize"))
            except ValueError:
                pass
        return (
            str(path),
            True,
            int(item["resType"]),
            remote_gcid,
            remote_size,
        )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for rows in state.pending_file_batches():
            state.record_remote_files(list(executor.map(scan_file, rows)))
    state.finish()


def _run_parallel(*, jobs, parallelism: int, child) -> dict[str, int | bool]:
    completed = 0
    completed_with_not_reusable = 0
    failed = 0
    credential_fatal = False
    not_reusable = 0
    delta_files = 0
    missing_dirs = 0
    jobs_with_delta = 0
    next_index = 0
    active: dict[Future, BatchJob] = {}
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        while next_index < len(jobs) and len(active) < parallelism:
            job = jobs[next_index]
            active[executor.submit(child, job)] = job
            next_index += 1
        while active:
            done, _ = wait(active, return_when=FIRST_COMPLETED)
            for future in done:
                active.pop(future)
                result = future.result()
                if result.get("failed"):
                    failed += 1
                elif result.get("not_reusable"):
                    completed_with_not_reusable += 1
                else:
                    completed += 1
                credential_fatal = credential_fatal or bool(
                    result.get("credential_fatal")
                )
                not_reusable += int(result.get("not_reusable", 0))
                delta_files += int(result.get("delta_files", 0))
                missing_dirs += int(result.get("missing_dirs", 0))
                jobs_with_delta += int(result.get("delta_files", 0) > 0)
            while (
                not credential_fatal
                and next_index < len(jobs)
                and len(active) < parallelism
            ):
                job = jobs[next_index]
                active[executor.submit(child, job)] = job
                next_index += 1
    return {
        "completed": completed,
        "completed_with_not_reusable": completed_with_not_reusable,
        "failed": failed,
        "credential_fatal": credential_fatal,
        "not_reusable": not_reusable,
        "delta_files": delta_files,
        "missing_dirs": missing_dirs,
        "jobs_with_delta": jobs_with_delta,
    }


def _parents(path: str):
    for parent in PurePosixPath(path).parents:
        value = parent.as_posix()
        if value in {"", "."}:
            break
        yield value


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _paths_overlap(first: Path, second: Path) -> bool:
    return _is_within(first, second) or _is_within(second, first)
