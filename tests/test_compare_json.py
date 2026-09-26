from __future__ import annotations

import json

import pytest

from guangya_fastlink import app
from guangya_fastlink.api import Decision, DecisionKind


SHARED = "A" * 40
LOCAL_ONLY = "B" * 40
REMOTE_ONLY = "C" * 40


class TreeClient:
    def __init__(self):
        self.items = {
            "": [
                {"fileId": "dir", "fileName": "deep", "resType": 2},
                {
                    "fileId": "shared",
                    "fileName": "renamed (1).bin",
                    "resType": 1,
                    "gcid": SHARED,
                    "fileSize": 99,
                },
                {
                    "fileId": "collision",
                    "fileName": "collision.bin",
                    "resType": 1,
                    "gcid": REMOTE_ONLY,
                    "fileSize": 7,
                },
            ],
            "dir": [
                {
                    "fileId": "extra",
                    "fileName": "extra.bin",
                    "resType": 1,
                    "gcid": REMOTE_ONLY.lower(),
                    "fileSize": 7,
                },
            ],
        }
        self.calls = []

    def list_page(self, *, parent_id, page, page_size=50):
        self.calls.append((parent_id, page))
        items = self.items[parent_id]
        # Small pages exercise pagination without a large fixture.
        return Decision(
            DecisionKind.COMPLETED,
            payload={"items": items[page : page + 1], "total": len(items)},
        )


def test_json_compare_uses_gcid_and_keeps_output_side_paths(tmp_path, monkeypatch):
    source = tmp_path / "local.json"
    source.write_text(
        json.dumps(
            {
                "files": [
                    {
                        "path": "/collection/original.bin",
                        "gcid": SHARED.lower(),
                        "size": "1",
                    },
                    {"path": "/another/copy.bin", "gcid": SHARED, "size": "1"},
                    {"path": "/nested/missing.bin", "gcid": LOCAL_ONLY, "size": "7"},
                    {
                        "path": "/collision.bin",
                        "gcid": LOCAL_ONLY,
                        "size": "7",
                        "downloadUrl": "https://example.test/source.bin",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "missing.json"
    argv = [
        "compare-folder",
        "--local-json",
        str(source),
        "--remote-folder-id",
        "root",
        "--output-file",
        str(output),
    ]
    client = TreeClient()
    monkeypatch.setattr(app, "build_client", lambda: client)

    for direction in ([], ["--local-only"]):
        assert app.run_cli([*argv, *direction]) == 0
        result = json.loads(output.read_text(encoding="utf-8"))
        assert [r["path"] for r in result["files"]] == [
            "/collision.bin",
            "/nested/missing.bin",
        ]
        assert result["files"][0]["downloadUrl"] == "https://example.test/source.bin"
        assert result["totalFilesCount"] == 2
        assert result["totalSize"] == 14
        assert result["sourceTag"] == "local"

    assert app.run_cli([*argv, "--remote-only"]) == 0
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["files"] == [
        {
            "path": "/collision.bin",
            "gcid": REMOTE_ONLY,
            "size": "7",
            "fileId": "collision",
            "sourceGuangya": True,
        },
        {
            "path": "/deep/extra.bin",
            "gcid": REMOTE_ONLY,
            "size": "7",
            "fileId": "extra",
            "parentId": "dir",
            "sourceGuangya": True,
        },
    ]
    assert result["sourceTag"] == "guangya"
    assert ("", 2) in client.calls and ("dir", 0) in client.calls

    # A new remote copy covers all local paths with that GCID on the next run.
    client.items[""].append(
        {
            "fileId": "arrived",
            "fileName": "flat.bin",
            "resType": 1,
            "gcid": LOCAL_ONLY,
            "fileSize": 7,
        }
    )
    assert app.run_cli(argv) == 0
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["files"] == []
    assert result["totalFilesCount"] == result["totalSize"] == 0


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"files": {}},
        {
            "files": [
                {"path": "/bad.bin", "size": "1", "gcid": "bad"},
            ]
        },
    ],
)
def test_invalid_local_json_preserves_previous_output(tmp_path, monkeypatch, payload):
    source, output = tmp_path / "local.json", tmp_path / "out.json"
    source.write_text(json.dumps(payload), encoding="utf-8")
    output.write_text("previous output", encoding="utf-8")
    client = TreeClient()
    monkeypatch.setattr(app, "build_client", lambda: client)
    assert (
        app.run_cli(
            [
                "compare-folder",
                "--local-json",
                str(source),
                "--remote-folder-id",
                "root",
                "--output-file",
                str(output),
            ]
        )
        == 1
    )
    assert output.read_text(encoding="utf-8") == "previous output"
    assert client.calls == []


def test_empty_json_and_remote_failure_preserve_correct_output(tmp_path, monkeypatch):
    source, output = tmp_path / "local.json", tmp_path / "out.json"
    source.write_text('{"files": []}', encoding="utf-8")
    client = TreeClient()
    monkeypatch.setattr(app, "build_client", lambda: client)
    argv = [
        "compare-folder",
        "--local-json",
        str(source),
        "--remote-folder-id",
        "root",
        "--output-file",
        str(output),
    ]
    assert app.run_cli(argv) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["files"] == []
    assert app.run_cli([*argv, "--remote-only"]) == 0
    previous = output.read_bytes()
    assert json.loads(previous)["totalFilesCount"] == 3

    client.items["dir"][0]["gcid"] = "invalid"
    assert app.run_cli(argv) == 1
    assert output.read_bytes() == previous

    monkeypatch.setattr(
        client,
        "list_page",
        lambda **kw: Decision(
            DecisionKind.COMPLETED, payload={"items": [], "total": 3}
        ),
    )
    assert app.run_cli(argv) == 1
    assert output.read_bytes() == previous

    monkeypatch.setattr(
        client,
        "list_page",
        lambda **kw: Decision(DecisionKind.CREDENTIAL_FATAL, error="invalid token"),
    )
    assert app.run_cli(argv) == 1
    assert output.read_bytes() == previous

    # Later empty pages can omit total; retain the earlier advertised count.
    monkeypatch.setattr(
        client,
        "list_page",
        lambda **kw: Decision(
            DecisionKind.COMPLETED,
            payload={
                "items": client.items[""][1:2] if kw["page"] == 0 else [],
                "total": 3 if kw["page"] == 0 else 0,
            },
        ),
    )
    assert app.run_cli(argv) == 1
    assert output.read_bytes() == previous


def test_empty_remote_returns_all_local_records(tmp_path, monkeypatch):
    source, output = tmp_path / "local.json", tmp_path / "out.json"
    records = [{"path": "/a.bin", "size": "1", "gcid": SHARED}]
    source.write_text(json.dumps({"files": records}), encoding="utf-8")
    client = TreeClient()
    client.items = {"": []}
    monkeypatch.setattr(app, "build_client", lambda: client)
    argv = [
        "compare-folder",
        "--local-json",
        str(source),
        "--remote-folder-id",
        "root",
        "--output-file",
        str(output),
    ]
    assert app.run_cli(argv) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["files"] == records
    assert app.run_cli([*argv, "--remote-only"]) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["files"] == []
