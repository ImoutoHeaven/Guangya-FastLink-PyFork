from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, TextIO

import ijson

from guangya_fastlink import __version__


GCID_RE = re.compile(r"^[0-9a-fA-F]{40}$")
ASCII_DIGITS_RE = re.compile(r"^[0-9]+$")
MAX_SQLITE_INTEGER = 2**63 - 1
MALFORMED_EXPORT_ERROR = "malformed export json"


@dataclass(frozen=True)
class SourceRecord:
    source_index: int
    path: str
    file_name: str
    relative_parent_dir: str
    gcid: str
    size: int
    source_file_id: str = ""
    source_parent_id: str = ""
    cid: str = ""
    whole_cid: str = ""
    triple_cid: str = ""
    md5: str = ""
    download_url: str = ""
    source_guangya: bool = False

    @property
    def key(self) -> str:
        return f"{self.source_index}\t{self.path}\t{self.gcid}\t{self.size}"


@dataclass(frozen=True)
class ExportScope:
    source_sha256: str


def normalize_relative_path(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("path must be a string")
    if "\\" in value:
        raise ValueError("invalid path")
    normalized = value.lstrip("/")
    if not normalized or normalized.endswith("/") or "//" in normalized:
        raise ValueError("invalid path")
    if any(part in {"", ".", ".."} for part in normalized.split("/")):
        raise ValueError("invalid path")
    return normalized


def normalize_gcid(value: object) -> str:
    if not isinstance(value, str) or not GCID_RE.fullmatch(value):
        raise ValueError("gcid must be 40 hex chars")
    return value.upper()


def parse_size(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("size must not be boolean")
    if isinstance(value, int):
        if value < 0 or value > MAX_SQLITE_INTEGER:
            raise ValueError(f"size must be between 0 and {MAX_SQLITE_INTEGER}")
        return value
    if isinstance(value, str) and ASCII_DIGITS_RE.fullmatch(value):
        parsed = int(value)
        if parsed <= MAX_SQLITE_INTEGER:
            return parsed
        raise ValueError(f"size must be between 0 and {MAX_SQLITE_INTEGER}")
    raise ValueError("size must be a non-negative integer or decimal-digit string")


def normalize_remote_id(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("remote fileId must be a non-empty scalar")
    normalized = str(value).strip()
    if not normalized or (isinstance(value, int) and value < 0):
        raise ValueError("remote fileId must be a non-empty scalar")
    return normalized


def _optional_text(entry: dict, key: str) -> str:
    value = entry.get(key, "")
    if value is None:
        return ""
    if not isinstance(value, (str, int)):
        raise ValueError(f"{key} must be a string when present")
    return str(value).strip()


def normalize_record(entry: object, *, source_index: int) -> SourceRecord:
    if not isinstance(entry, dict):
        raise ValueError("file entries must be objects")
    path = normalize_relative_path(entry.get("path"))
    gcid = normalize_gcid(entry.get("gcid"))
    size = parse_size(entry.get("size"))
    parts = path.split("/")
    source_guangya = entry.get("sourceGuangya", False)
    if not isinstance(source_guangya, bool):
        raise ValueError("sourceGuangya must be boolean when present")
    return SourceRecord(
        source_index=source_index,
        path=path,
        file_name=parts[-1],
        relative_parent_dir="/".join(parts[:-1]),
        gcid=gcid,
        size=size,
        source_file_id=_optional_text(entry, "fileId"),
        source_parent_id=_optional_text(entry, "parentId"),
        cid=_optional_text(entry, "cid"),
        whole_cid=_optional_text(entry, "wholeCid"),
        triple_cid=_optional_text(entry, "tripleCid"),
        md5=_optional_text(entry, "md5"),
        download_url=_optional_text(entry, "downloadUrl"),
        source_guangya=source_guangya,
    )


def inspect_export_scope(path: Path) -> ExportScope:
    hasher = hashlib.sha256()
    saw_root = False
    saw_files = False
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        with path.open("rb") as handle:
            for prefix, event, _value in ijson.parse(handle):
                if not saw_root:
                    if prefix != "" or event != "start_map":
                        raise ValueError("export root must be an object")
                    saw_root = True
                if prefix == "files":
                    if event != "start_array":
                        raise ValueError("files must be an array")
                    saw_files = True
                    break
    except ijson.JSONError as exc:
        raise ValueError(MALFORMED_EXPORT_ERROR) from exc
    if not saw_root:
        raise ValueError("export root must be an object")
    if not saw_files:
        raise ValueError("files must be an array")
    return ExportScope(source_sha256=hasher.hexdigest())


def iter_export_records(path: Path) -> Iterator[SourceRecord]:
    try:
        with path.open("rb") as handle:
            for index, entry in enumerate(ijson.items(handle, "files.item"), start=1):
                yield normalize_record(entry, source_index=index)
    except ijson.JSONError as exc:
        raise ValueError(MALFORMED_EXPORT_ERROR) from exc


def record_to_json(record: SourceRecord | dict) -> dict:
    def get(name: str, default=""):
        if isinstance(record, dict):
            return record.get(name, default)
        return getattr(record, name, default)

    entry: dict[str, object] = {
        "size": str(int(get("size", 0))),
        "path": f"/{normalize_relative_path(get('path'))}",
        "gcid": normalize_gcid(get("gcid")),
    }
    optional = {
        "fileId": get("source_file_id"),
        "cid": get("cid"),
        "wholeCid": get("whole_cid"),
        "tripleCid": get("triple_cid"),
        "md5": get("md5"),
        "parentId": get("source_parent_id"),
        "downloadUrl": get("download_url"),
    }
    for key, value in optional.items():
        if value not in (None, ""):
            entry[key] = str(value)
    if bool(get("source_guangya", False)):
        entry["sourceGuangya"] = True
    return entry


def format_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    unit_index = 0
    while unit_index < len(units) - 1 and value >= 1024:
        value /= 1024
        unit_index += 1
    return f"{size} B" if unit_index == 0 else f"{value:.2f} {units[unit_index]}"


def write_export_json(
    handle: TextIO,
    *,
    records: Iterable[SourceRecord | dict],
    total_files: int,
    total_size: int,
    script_version: str = __version__,
    script_author: str = "GuangyaFastLink-PyFork",
    source_tag: str | None = "guangya",
) -> None:
    header = {
        "scriptVersion": script_version,
        "scriptAuthor": script_author,
        "totalFilesCount": total_files,
        "totalSize": total_size,
        "formattedTotalSize": format_size(total_size),
    }
    handle.write("{\n")
    for key, value in header.items():
        handle.write(f"  {json.dumps(key)}: {json.dumps(value, ensure_ascii=False)},\n")
    handle.write('  "files": [\n')
    first = True
    for record in records:
        if not first:
            handle.write(",\n")
        handle.write(f"    {json.dumps(record_to_json(record), ensure_ascii=False)}")
        first = False
    handle.write("\n  ]")
    if source_tag is not None:
        handle.write(",\n")
        handle.write(f'  "sourceTag": {json.dumps(source_tag, ensure_ascii=False)}')
    handle.write("\n}\n")


def atomic_write_export(
    output_path: Path,
    *,
    records: Iterable[SourceRecord | dict],
    total_files: int,
    total_size: int,
    script_version: str = __version__,
    script_author: str = "GuangyaFastLink-PyFork",
    source_tag: str | None = "guangya",
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=output_path.parent, delete=False
        ) as handle:
            temp_path = Path(handle.name)
            write_export_json(
                handle,
                records=records,
                total_files=total_files,
                total_size=total_size,
                script_version=script_version,
                script_author=script_author,
                source_tag=source_tag,
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, output_path)
    except BaseException:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise
