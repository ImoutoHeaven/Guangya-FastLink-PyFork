from __future__ import annotations

import io
import json

import pytest

from guangya_fastlink.models import (
    iter_export_records,
    normalize_relative_path,
    parse_size,
    write_export_json,
)


GCID = "58A3F526EE3C569FEECCDFD66DAC9631614E7578"


def test_userscript_shape_round_trips(tmp_path):
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "scriptVersion": "1.1.2",
                "scriptAuthor": "sumuve",
                "files": [
                    {
                        "size": "123",
                        "path": "/dir/file.rar",
                        "gcid": GCID.lower(),
                        "fileId": "100",
                        "parentId": "200",
                        "sourceGuangya": True,
                    }
                ],
                "sourceTag": "guangya",
            }
        ),
        encoding="utf-8",
    )

    [record] = list(iter_export_records(source))
    assert record.path == "dir/file.rar"
    assert record.gcid == GCID
    assert record.size == 123
    assert record.source_file_id == "100"

    output = io.StringIO()
    write_export_json(output, records=[record], total_files=1, total_size=123)
    payload = json.loads(output.getvalue())
    assert payload["sourceTag"] == "guangya"
    assert payload["files"] == [
        {
            "size": "123",
            "path": "/dir/file.rar",
            "gcid": GCID,
            "fileId": "100",
            "parentId": "200",
            "sourceGuangya": True,
        }
    ]


@pytest.mark.parametrize(
    "value",
    ["", "/", "a/", "a//b", "a/../b", "a/./b", "a\\b"],
)
def test_invalid_paths_are_rejected(value):
    with pytest.raises(ValueError, match="invalid path"):
        normalize_relative_path(value)


def test_size_must_fit_sqlite_integer():
    with pytest.raises(ValueError, match="size must be between"):
        parse_size(str(2**63))
