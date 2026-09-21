from __future__ import annotations

import argparse

from guangya_fastlink.config import build_config
from guangya_fastlink.local_calculator import default_workers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="guangya-fastlink")
    commands = parser.add_subparsers(dest="command", required=True)

    import_json = commands.add_parser("import-json")
    import_json.set_defaults(command="import_json")
    import_json.add_argument("--file", required=True)
    import_json.add_argument("--target-parent-id", required=True)
    import_json.add_argument("--state-file", required=True)
    _add_transfer_options(import_json)
    import_json.add_argument("--retry-failed", action="store_true")
    import_json.add_argument("--dry-run", action="store_true")

    export_json = commands.add_parser("export-json")
    export_json.set_defaults(command="export_json")
    export_json.add_argument("--source-parent-id", required=True)
    export_json.add_argument("--output-file", required=True)
    export_json.add_argument("--state-file", required=True)
    export_json.add_argument("--workers", type=int, default=5)
    export_json.add_argument("--max-retries", type=int, default=5)

    batch_import = commands.add_parser("batch-import-json")
    batch_import.set_defaults(command="batch_import_json")
    batch_import.add_argument("--input-dir", required=True)
    batch_import.add_argument("--target-parent-id", required=True)
    batch_import.add_argument("--state-dir", required=True)
    _add_transfer_options(batch_import)
    batch_import.add_argument("--json-parallelism", type=int, default=2)
    batch_import.add_argument("--retry-failed", action="store_true")
    batch_import.add_argument("--dry-run", action="store_true")

    batch_check = commands.add_parser("batch-check-json")
    batch_check.set_defaults(command="batch_check_json")
    batch_check.add_argument("--input-dir", required=True)
    batch_check.add_argument("--target-parent-id", required=True)
    batch_check.add_argument("--state-dir", required=True)
    batch_check.add_argument("--output-dir", required=True)
    batch_check.add_argument("--workers", type=int, default=5)
    batch_check.add_argument("--json-parallelism", type=int, default=2)
    batch_check.add_argument("--max-retries", type=int, default=5)
    batch_check.add_argument("--exist-only", action="store_true")
    batch_check.add_argument("--with-checksum", action="store_true")

    compare_folder = commands.add_parser("compare-folder")
    compare_folder.set_defaults(command="compare_folder", compare_mode="local_only")
    compare_folder.add_argument("--local-folder", required=True)
    compare_folder.add_argument("--remote-folder-id", required=True)
    compare_folder.add_argument("--max-retries", type=int, default=5)
    compare_direction = compare_folder.add_mutually_exclusive_group()
    compare_direction.add_argument(
        "--local-only", action="store_const", const="local_only", dest="compare_mode"
    )
    compare_direction.add_argument(
        "--remote-only", action="store_const", const="remote_only", dest="compare_mode"
    )

    generate_json = commands.add_parser("generate-json")
    generate_json.set_defaults(command="generate_json")
    generate_json.add_argument("--source-dir", required=True)
    generate_json.add_argument("--output-file", required=True)
    generate_json.add_argument("--workers", type=int, default=default_workers())
    return parser


def _add_transfer_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--flush-every", type=int, default=100)


def parse_args(argv=None):
    args = build_parser().parse_args(argv)
    return args, build_config(args)
