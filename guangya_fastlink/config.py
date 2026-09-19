from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def normalize_parent_id(value: object) -> str:
    normalized = str(value).strip()
    if normalized.lower() == "root":
        return ""
    if not normalized:
        raise ValueError("parent id is required; use 'root' for the root directory")
    return normalized


@dataclass(frozen=True)
class ImportConfig:
    file_path: Path
    target_parent_id: str
    state_file: Path
    workers: int
    max_retries: int
    flush_every: int
    retry_failed: bool
    dry_run: bool
    command: str = "import_json"


@dataclass(frozen=True)
class ExportConfig:
    source_parent_id: str
    output_file: Path
    state_file: Path
    workers: int
    max_retries: int
    command: str = "export_json"


@dataclass(frozen=True)
class BatchImportConfig:
    input_dir: Path
    target_parent_id: str
    state_dir: Path
    workers: int
    json_parallelism: int
    max_retries: int
    flush_every: int
    retry_failed: bool
    dry_run: bool
    command: str = "batch_import_json"


@dataclass(frozen=True)
class BatchCheckConfig:
    input_dir: Path
    target_parent_id: str
    state_dir: Path
    output_dir: Path
    workers: int
    json_parallelism: int
    max_retries: int
    compare_mode: str
    command: str = "batch_check_json"


@dataclass(frozen=True)
class LocalGenerateConfig:
    source_dir: Path
    output_file: Path
    workers: int
    command: str = "generate_json"


def build_config(args):
    if args.workers < 1:
        raise ValueError("workers must be >= 1")
    if hasattr(args, "max_retries") and args.max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if hasattr(args, "flush_every") and args.flush_every < 1:
        raise ValueError("flush_every must be >= 1")
    if hasattr(args, "json_parallelism") and args.json_parallelism < 1:
        raise ValueError("json_parallelism must be >= 1")

    if args.command == "import_json":
        file_path = Path(args.file).resolve()
        state_file = Path(args.state_file).resolve()
        retry_file = state_file.with_suffix(".retry.export.json")
        if file_path in {state_file, retry_file}:
            raise ValueError("input file must be distinct from import state artifacts")
        return ImportConfig(
            file_path=file_path,
            target_parent_id=normalize_parent_id(args.target_parent_id),
            state_file=state_file,
            workers=args.workers,
            max_retries=args.max_retries,
            flush_every=args.flush_every,
            retry_failed=args.retry_failed,
            dry_run=args.dry_run,
        )
    if args.command == "export_json":
        output = Path(args.output_file).resolve()
        state = Path(args.state_file).resolve()
        artifacts = [output, *_export_artifacts(state, output)]
        if len(set(artifacts)) != len(artifacts):
            raise ValueError("output_file must be distinct from export state artifacts")
        return ExportConfig(
            source_parent_id=normalize_parent_id(args.source_parent_id),
            output_file=output,
            state_file=state,
            workers=args.workers,
            max_retries=args.max_retries,
        )
    if args.command == "batch_import_json":
        return BatchImportConfig(
            input_dir=Path(args.input_dir).resolve(),
            target_parent_id=normalize_parent_id(args.target_parent_id),
            state_dir=Path(args.state_dir).resolve(),
            workers=args.workers,
            json_parallelism=args.json_parallelism,
            max_retries=args.max_retries,
            flush_every=args.flush_every,
            retry_failed=args.retry_failed,
            dry_run=args.dry_run,
        )
    if args.command == "batch_check_json":
        if args.exist_only and args.with_checksum:
            raise ValueError("--exist-only and --with-checksum are mutually exclusive")
        return BatchCheckConfig(
            input_dir=Path(args.input_dir).resolve(),
            target_parent_id=normalize_parent_id(args.target_parent_id),
            state_dir=Path(args.state_dir).resolve(),
            output_dir=Path(args.output_dir).resolve(),
            workers=args.workers,
            json_parallelism=args.json_parallelism,
            max_retries=args.max_retries,
            compare_mode="with_checksum" if args.with_checksum else "exist_only",
        )
    if args.command == "generate_json":
        raw_output = Path(args.output_file)
        if not raw_output.is_absolute():
            raw_output = Path.cwd() / raw_output
        return LocalGenerateConfig(
            source_dir=Path(args.source_dir).resolve(),
            output_file=raw_output.parent.resolve() / raw_output.name,
            workers=args.workers,
        )
    raise ValueError(f"unsupported command: {args.command}")


def _export_artifacts(state: Path, output: Path) -> list[Path]:
    return [
        state,
        state.with_suffix(".records.jsonl"),
        state.with_suffix(".finalize.sqlite3"),
        output.with_name(f".{output.name}.tmp"),
    ]
