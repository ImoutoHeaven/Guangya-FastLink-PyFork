from __future__ import annotations

import hashlib
import os
import stat as stat_module
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from guangya_fastlink.models import atomic_write_export


MIN_BLOCK_SIZE = 256 * 1024
MAX_BLOCK_SIZE = 2 * 1024 * 1024
MAX_BLOCKS_BEFORE_GROWTH = 512


@dataclass(frozen=True)
class FileSnapshot:
    path: Path
    relative_path: str
    size: int
    mtime_ns: int
    device: int
    inode: int


@dataclass(frozen=True)
class LocalExportSummary:
    total_files: int
    total_size: int
    elapsed_seconds: float


def default_workers() -> int:
    return max(1, min(8, os.cpu_count() or 1))


def gcid_block_size(size: int) -> int:
    if size < 0:
        raise ValueError("size must be >= 0")
    block_size = MIN_BLOCK_SIZE
    while size > block_size * MAX_BLOCKS_BEFORE_GROWTH and block_size < MAX_BLOCK_SIZE:
        block_size <<= 1
    return block_size


def scan_local_files(source_dir: Path, output_file: Path) -> list[FileSnapshot]:
    root = source_dir.resolve()
    output = _output_path(output_file)
    if not root.is_dir():
        raise ValueError("source directory does not exist")
    if output == root:
        raise ValueError("output file must be distinct from source directory")
    snapshots = []
    for path in root.rglob("*"):
        if path == output:
            continue
        try:
            stat = path.stat()
        except FileNotFoundError as exc:
            raise RuntimeError(f"source tree changed during scan: {path}") from exc
        if not stat_module.S_ISREG(stat.st_mode):
            continue
        snapshots.append(
            FileSnapshot(
                path=path,
                relative_path=path.relative_to(root).as_posix(),
                size=int(stat.st_size),
                mtime_ns=int(stat.st_mtime_ns),
                device=int(stat.st_dev),
                inode=int(stat.st_ino),
            )
        )
    return sorted(snapshots, key=lambda item: item.relative_path)


def calculate_gcid(snapshot: FileSnapshot, *, workers: int = 1) -> str:
    if workers < 1:
        raise ValueError("workers must be >= 1")
    _assert_unchanged(snapshot, snapshot.path.stat())
    block_size = gcid_block_size(snapshot.size)
    block_count = (snapshot.size + block_size - 1) // block_size
    chunk_workers = min(workers, max(1, block_count))
    outer = hashlib.sha1(usedforsecurity=False)
    total_read = 0

    with snapshot.path.open("rb") as handle:
        _assert_unchanged(snapshot, os.fstat(handle.fileno()))
        if chunk_workers == 1:
            while block := handle.read(block_size):
                outer.update(hashlib.sha1(block, usedforsecurity=False).digest())
                total_read += len(block)
        else:
            total_read = _hash_blocks_parallel(
                handle=handle,
                block_size=block_size,
                workers=chunk_workers,
                outer=outer,
            )
        _assert_unchanged(snapshot, os.fstat(handle.fileno()))

    _assert_unchanged(snapshot, snapshot.path.stat())
    if total_read != snapshot.size:
        raise RuntimeError(f"file changed during calculation: {snapshot.path}")
    return outer.hexdigest().upper()


def generate_local_export(
    *, source_dir: Path, output_file: Path, workers: int | None = None
) -> LocalExportSummary:
    worker_count = default_workers() if workers is None else int(workers)
    if worker_count < 1:
        raise ValueError("workers must be >= 1")
    snapshots = scan_local_files(source_dir, output_file)
    if not snapshots:
        raise ValueError("no files found")
    total_size = sum(snapshot.size for snapshot in snapshots)
    file_workers = min(worker_count, len(snapshots))
    chunk_workers = max(1, worker_count // file_workers)
    started = time.perf_counter()

    def hash_snapshot(snapshot: FileSnapshot) -> dict:
        return {
            "path": snapshot.relative_path,
            "gcid": calculate_gcid(snapshot, workers=chunk_workers),
            "size": snapshot.size,
        }

    with ThreadPoolExecutor(max_workers=file_workers) as executor:
        atomic_write_export(
            _output_path(output_file),
            records=executor.map(hash_snapshot, snapshots),
            total_files=len(snapshots),
            total_size=total_size,
            script_version="local-calculator",
            source_tag="local",
        )
    return LocalExportSummary(
        total_files=len(snapshots),
        total_size=total_size,
        elapsed_seconds=time.perf_counter() - started,
    )


def run_local_generate(*, config) -> int:
    summary = generate_local_export(
        source_dir=config.source_dir,
        output_file=config.output_file,
        workers=config.workers,
    )
    mib = summary.total_size / (1024 * 1024)
    rate = mib / summary.elapsed_seconds if summary.elapsed_seconds else float("inf")
    print(
        f"Generated: files={summary.total_files} size={summary.total_size} "
        f"elapsed={summary.elapsed_seconds:.3f}s throughput={rate:.1f}MiB/s"
    )
    return 0


def _hash_blocks_parallel(*, handle, block_size: int, workers: int, outer) -> int:
    pending = deque()
    total_read = 0
    exhausted = False
    with ThreadPoolExecutor(max_workers=workers) as executor:
        while pending or not exhausted:
            while not exhausted and len(pending) < workers * 2:
                block = handle.read(block_size)
                if not block:
                    exhausted = True
                    break
                total_read += len(block)
                pending.append(executor.submit(_sha1_digest, block))
            if pending:
                outer.update(pending.popleft().result())
    return total_read


def _sha1_digest(data: bytes) -> bytes:
    return hashlib.sha1(data, usedforsecurity=False).digest()


def _output_path(path: Path) -> Path:
    absolute = path if path.is_absolute() else Path.cwd() / path
    return absolute.parent.resolve() / absolute.name


def _assert_unchanged(snapshot: FileSnapshot, stat) -> None:
    identity = (
        int(stat.st_size),
        int(stat.st_mtime_ns),
        int(stat.st_dev),
        int(stat.st_ino),
    )
    expected = (
        snapshot.size,
        snapshot.mtime_ns,
        snapshot.device,
        snapshot.inode,
    )
    if identity != expected:
        raise RuntimeError(f"file changed during calculation: {snapshot.path}")
