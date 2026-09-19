from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from guangya_fastlink.local_calculator import (
    calculate_gcid,
    gcid_block_size,
    generate_local_export,
    scan_local_files,
)
import guangya_fastlink.local_calculator as local_calculator_module
from guangya_fastlink.models import iter_export_records


def reference_gcid(data: bytes, block_size: int) -> str:
    outer = hashlib.sha1(usedforsecurity=False)
    for offset in range(0, len(data), block_size):
        outer.update(
            hashlib.sha1(
                data[offset : offset + block_size], usedforsecurity=False
            ).digest()
        )
    return outer.hexdigest().upper()


@pytest.mark.parametrize(
    ("size", "expected"),
    [
        (0, 256 * 1024),
        (128 * 1024 * 1024, 256 * 1024),
        (128 * 1024 * 1024 + 1, 512 * 1024),
        (256 * 1024 * 1024, 512 * 1024),
        (256 * 1024 * 1024 + 1, 1024 * 1024),
        (512 * 1024 * 1024, 1024 * 1024),
        (512 * 1024 * 1024 + 1, 2 * 1024 * 1024),
        (10 * 1024 * 1024 * 1024, 2 * 1024 * 1024),
    ],
)
def test_gcid_block_size_boundaries(size, expected):
    assert gcid_block_size(size) == expected


def test_calculate_gcid_matches_independent_reference(tmp_path):
    path = tmp_path / "data.bin"
    data = bytes(range(256)) * 5000
    path.write_bytes(data)
    [snapshot] = scan_local_files(tmp_path, tmp_path / "out.json")

    assert calculate_gcid(snapshot, workers=1) == reference_gcid(
        data, gcid_block_size(len(data))
    )
    assert calculate_gcid(snapshot, workers=4) == reference_gcid(
        data, gcid_block_size(len(data))
    )


def test_empty_file_gcid_is_sha1_of_empty_input(tmp_path):
    path = tmp_path / "empty.bin"
    path.touch()
    [snapshot] = scan_local_files(tmp_path, tmp_path / "out.json")
    assert (
        calculate_gcid(snapshot, workers=4)
        == hashlib.sha1(usedforsecurity=False).hexdigest().upper()
    )


def test_file_change_after_scan_is_rejected(tmp_path):
    path = tmp_path / "changing.bin"
    path.write_bytes(b"before")
    [snapshot] = scan_local_files(tmp_path, tmp_path / "out.json")
    path.write_bytes(b"after-change")

    with pytest.raises(RuntimeError, match="changed during calculation"):
        calculate_gcid(snapshot, workers=1)


def test_generate_local_export_is_sorted_importable_and_excludes_output(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "z.bin").write_bytes(b"z")
    nested = source / "dir"
    nested.mkdir()
    (nested / "a.bin").write_bytes(b"a")
    output = source / "export.json"
    output.write_text("old output must be excluded", encoding="utf-8")

    summary = generate_local_export(
        source_dir=source,
        output_file=output,
        workers=2,
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert summary.total_files == 2
    assert summary.total_size == 2
    assert payload["scriptVersion"] == "local-calculator"
    assert payload["sourceTag"] == "local"
    assert [entry["path"] for entry in payload["files"]] == [
        "/dir/a.bin",
        "/z.bin",
    ]
    assert payload["files"][0]["gcid"] == reference_gcid(b"a", 256 * 1024)
    assert [record.path for record in iter_export_records(output)] == [
        "dir/a.bin",
        "z.bin",
    ]


def test_generate_local_export_rejects_empty_directory(tmp_path):
    with pytest.raises(ValueError, match="no files found"):
        generate_local_export(
            source_dir=tmp_path,
            output_file=tmp_path / "out.json",
            workers=1,
        )


def test_output_file_cannot_be_source_directory(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"a")
    with pytest.raises(ValueError, match="distinct"):
        generate_local_export(
            source_dir=tmp_path,
            output_file=tmp_path,
            workers=1,
        )


def test_output_symlink_is_replaced_without_touching_its_target(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    target = source / "a.bin"
    target.write_bytes(b"important source bytes")
    output = source / "out.json"
    try:
        os.symlink("a.bin", output)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    generate_local_export(source_dir=source, output_file=output, workers=2)

    assert target.read_bytes() == b"important source bytes"
    assert not output.is_symlink()
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert [entry["path"] for entry in payload["files"]] == ["/a.bin"]


def test_direct_script_help_works_outside_repository(tmp_path):
    script = Path(local_calculator_module.__file__).resolve()
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, str(script), "-h"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--source-dir" in result.stdout
    assert "--output-file" in result.stdout
