from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import replace
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


@pytest.mark.parametrize("change", ["size", "mtime"])
def test_file_change_after_scan_is_rejected(tmp_path, change):
    path = tmp_path / "changing.bin"
    path.write_bytes(b"before")
    [snapshot] = scan_local_files(tmp_path, tmp_path / "out.json")
    if change == "size":
        path.write_bytes(b"after-change")
    else:
        os.utime(path, ns=(path.stat().st_atime_ns, snapshot.mtime_ns + 1_000_000_000))

    with pytest.raises(RuntimeError, match="changed during calculation"):
        calculate_gcid(snapshot, workers=1)


@pytest.mark.parametrize("workers", [1, 4])
def test_generate_accepts_reassigned_scan_inode(tmp_path, monkeypatch, workers):
    source = tmp_path / "source"
    source.mkdir()
    data = bytes(range(256)) * 5000
    (source / "file.bin").write_bytes(data)
    output = tmp_path / "export.json"
    original_scan = scan_local_files

    def stale_identity(source_dir, output_file):
        return [replace(item, inode=item.inode - 3750)
                for item in original_scan(source_dir, output_file)]

    monkeypatch.setattr(local_calculator_module, "scan_local_files", stale_identity)
    summary = generate_local_export(source_dir=source, output_file=output, workers=workers)
    payload = json.loads(output.read_text())
    assert payload["files"][0]["gcid"] == reference_gcid(data, gcid_block_size(len(data)))
    assert int(payload["files"][0]["size"]) == payload["totalSize"] == summary.total_size == len(data)


@pytest.mark.parametrize("field", ["st_size", "st_mtime_ns", "st_dev", "st_ino"])
@pytest.mark.parametrize("workers", [1, 4])
def test_generate_rejects_descriptor_change_and_preserves_output(tmp_path, monkeypatch, field, workers):
    from types import SimpleNamespace

    source = tmp_path / "source"
    source.mkdir()
    (source / "file.bin").write_bytes(b"x" * (1024 * 1024))
    output = tmp_path / "export.json"
    output.write_text("previous output")
    original_fstat = os.fstat
    calls = 0

    def changing_fstat(fd):
        nonlocal calls
        calls += 1
        observed = original_fstat(fd)
        values = {name: getattr(observed, name) for name in
                  ("st_mode", "st_size", "st_mtime_ns", "st_dev", "st_ino")}
        if calls == 2:
            values[field] += 1
        return SimpleNamespace(**values)

    monkeypatch.setattr(os, "fstat", changing_fstat)
    with pytest.raises(RuntimeError, match="changed during calculation"):
        generate_local_export(source_dir=source, output_file=output, workers=workers)
    assert output.read_text() == "previous output"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["export.json", "source"]


@pytest.mark.parametrize("change", ["path_replacement", "read_count"])
def test_calculate_rejects_replacement_or_wrong_byte_count(tmp_path, monkeypatch, change):
    path = tmp_path / "file.bin"
    data = b"x" * (1024 * 1024)
    path.write_bytes(data)
    [snapshot] = scan_local_files(tmp_path, tmp_path / "out.json")
    original_hash = local_calculator_module._hash_blocks_parallel

    def changed_read(**kwargs):
        count = original_hash(**kwargs)
        if change == "read_count":
            return count - 1
        replacement = tmp_path / "replacement.bin"
        replacement.write_bytes(data)
        os.utime(replacement, ns=(path.stat().st_atime_ns, snapshot.mtime_ns))
        os.replace(replacement, path)
        return count

    monkeypatch.setattr(local_calculator_module, "_hash_blocks_parallel", changed_read)
    with pytest.raises(RuntimeError, match="changed during calculation"):
        calculate_gcid(snapshot, workers=4)


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
    blocker = tmp_path / "blocked"
    blocker.mkdir()
    (blocker / "ijson.py").write_text(
        'raise ImportError("ijson intentionally unavailable")', encoding="utf-8"
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(blocker)
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


def test_direct_script_generation_needs_no_third_party_packages(tmp_path):
    script = Path(local_calculator_module.__file__).resolve()
    blocker = tmp_path / "blocked"
    blocker.mkdir()
    (blocker / "ijson.py").write_text(
        'raise ImportError("ijson intentionally unavailable")', encoding="utf-8"
    )
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.bin").write_bytes(b"abc")
    output = tmp_path / "out.json"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(blocker)

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--source-dir",
            str(source),
            "--output-file",
            str(output),
            "--workers",
            "2",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["files"][0]["gcid"] == "0D3CED9BEC10A777AEC23CCC353A8C08A633045E"
