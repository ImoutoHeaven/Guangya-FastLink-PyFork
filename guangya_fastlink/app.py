from __future__ import annotations

from pathlib import Path

from guangya_fastlink.api import GuangyaClient
from guangya_fastlink.auth import load_credentials
from guangya_fastlink.cli import parse_args
from guangya_fastlink.import_state import open_or_plan_import_state
from guangya_fastlink.models import inspect_export_scope
from guangya_fastlink.runner import (
    CredentialFatalError,
    RemoteTreeCoordinator,
    create_remote_directories,
    run_import_files,
    safe_print,
)


def build_client():
    credentials = load_credentials()
    return GuangyaClient(
        host=credentials.host,
        access_token=credentials.access_token,
    )


def run_cli(argv=None) -> int:
    state = None
    finalize_import = False
    try:
        _args, config = parse_args(argv)
        if config.command == "generate_json":
            from guangya_fastlink.local_calculator import run_local_generate

            return run_local_generate(config=config)
        if config.command == "export_json":
            from guangya_fastlink.exporter import run_export

            return run_export(client=build_client(), config=config)
        if config.command == "batch_import_json":
            from guangya_fastlink.batch import run_batch_import

            return run_batch_import(
                config=config,
                client=None if config.dry_run else build_client(),
            )
        if config.command == "batch_check_json":
            from guangya_fastlink.batch import run_batch_check

            return run_batch_check(config=config, client=build_client())
        if config.command == "compare_folder":
            from guangya_fastlink.exporter import run_compare_folder

            return run_compare_folder(client=build_client(), config=config)

        scope = inspect_export_scope(config.file_path)
        state = open_or_plan_import_state(
            state_path=config.state_file,
            source_path=config.file_path,
            source_sha256=scope.source_sha256,
            target_parent_id=config.target_parent_id,
        )
        state.workers = config.workers
        if config.dry_run:
            safe_print(
                f"Startup: files={state.stats['total']} pending={state.count_pending()} "
                f"folders={len(state.pending_folder_rows())} workers={state.workers}"
            )
            safe_print("Dry run: no remote mutations performed")
            return 0
        client = build_client()
        finalize_import = True
        if config.retry_failed:
            state.reset_retryable()
        safe_print(
            f"Startup: files={state.stats['total']} pending={state.count_pending()} "
            f"folders={len(state.pending_folder_rows())} workers={state.workers}"
        )
        coordinator = RemoteTreeCoordinator(client)
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
        return 1 if summary["credential_fatal"] or state.stats["failed"] else 0
    except (ValueError, RuntimeError, OSError, CredentialFatalError) as exc:
        safe_print(f"Error: {exc}")
        return 1
    finally:
        if state is not None:
            try:
                if finalize_import:
                    retry_path = Path(state.path).with_suffix(".retry.export.json")
                    if state.write_retry_export(retry_path):
                        safe_print(f"Retry export: {retry_path}")
                stats = state.stats
                safe_print(
                    f"Summary: completed={stats['completed']} "
                    f"not_reusable={stats['not_reusable']} failed={stats['failed']}"
                )
            finally:
                state.close()
