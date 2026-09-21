from __future__ import annotations

import json

from guangya_fastlink import app
from guangya_fastlink.api import Decision, DecisionKind


GCID = "58A3F526EE3C569FEECCDFD66DAC9631614E7578"


class FakeClient:
    def __init__(self):
        self.children = {"": {}}
        self.transfer_calls = 0

    def list_page(self, *, parent_id, page, page_size=50):
        items = (
            list(self.children.setdefault(parent_id, {}).values()) if page == 0 else []
        )
        return Decision(
            DecisionKind.COMPLETED,
            payload={"items": items, "total": len(items)},
        )

    def mkdir(self, *, parent_id, name):
        raise AssertionError("mkdir should not be called")

    def instant_transfer(self, *, record, parent_id):
        self.transfer_calls += 1
        self.children[parent_id][record.file_name] = {
            "fileId": "file-id",
            "fileName": record.file_name,
            "fileSize": record.size,
            "gcid": record.gcid,
            "resType": 1,
        }
        return Decision(DecisionKind.COMPLETED)


def test_cli_import_runs_end_to_end_and_resumes(tmp_path, monkeypatch):
    source = tmp_path / "input.json"
    source.write_text(
        json.dumps({"files": [{"path": "/a.rar", "gcid": GCID, "size": "123"}]}),
        encoding="utf-8",
    )
    state = tmp_path / "state.sqlite3"
    client = FakeClient()
    monkeypatch.setattr(app, "build_client", lambda: client)
    argv = [
        "import-json",
        "--file",
        str(source),
        "--target-parent-id",
        "root",
        "--state-file",
        str(state),
        "--flush-every",
        "1",
    ]

    assert app.run_cli(argv) == 0
    assert app.run_cli(argv) == 0
    assert client.transfer_calls == 1


def test_dry_run_does_not_load_credentials_or_touch_retry_output(tmp_path, monkeypatch):
    source = tmp_path / "input.json"
    source.write_text(
        json.dumps({"files": [{"path": "/a.rar", "gcid": GCID, "size": "123"}]}),
        encoding="utf-8",
    )
    state = tmp_path / "state.sqlite3"
    retry = state.with_suffix(".retry.export.json")
    retry.write_text("keep", encoding="utf-8")
    monkeypatch.setattr(
        app,
        "build_client",
        lambda: (_ for _ in ()).throw(AssertionError("credentials loaded")),
    )

    assert (
        app.run_cli(
            [
                "import-json",
                "--file",
                str(source),
                "--target-parent-id",
                "root",
                "--state-file",
                str(state),
                "--dry-run",
            ]
        )
        == 0
    )
    assert retry.read_text(encoding="utf-8") == "keep"


def test_generate_json_cli_needs_no_credentials(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.bin").write_bytes(b"a")
    output = tmp_path / "out.json"
    monkeypatch.setattr(
        app,
        "build_client",
        lambda: (_ for _ in ()).throw(AssertionError("credentials loaded")),
    )

    assert (
        app.run_cli(
            [
                "generate-json",
                "--source-dir",
                str(source),
                "--output-file",
                str(output),
                "--workers",
                "2",
            ]
        )
        == 0
    )
    assert json.loads(output.read_text(encoding="utf-8"))["sourceTag"] == "local"


def test_compare_folder_prints_only_local_files_missing_remotely(
    tmp_path, monkeypatch, capsys
):
    local = tmp_path / "local"
    (local / "foldert").mkdir(parents=True)
    (local / "matching.txt").write_text("local", encoding="utf-8")
    (local / "missing_file1.txt").write_text("missing", encoding="utf-8")
    (local / "foldert" / "present.txt").write_text("local", encoding="utf-8")
    (local / "foldert" / "m2.txt").write_text("missing", encoding="utf-8")

    class CompareClient:
        def list_page(self, *, parent_id, page, page_size=50):
            items = {
                "remote-root": [
                    {"fileId": "matching", "fileName": "matching.txt", "resType": 1,
                     "fileSize": 5, "gcid": GCID},
                    {"fileId": "foldert", "fileName": "foldert", "resType": 2},
                    {"fileId": "remote-only", "fileName": "remote-only.txt", "resType": 1,
                     "fileSize": 5, "gcid": GCID},
                ],
                "foldert": [
                    {"fileId": "present", "fileName": "present.txt", "resType": 1,
                     "fileSize": 5, "gcid": GCID},
                ],
            }.get(parent_id, [])
            page_items = items if page == 0 else []
            return Decision(
                DecisionKind.COMPLETED,
                payload={"items": page_items, "total": len(items)},
            )

    monkeypatch.setattr(app, "build_client", CompareClient)

    argv = [
        "compare-folder",
        "--local-folder",
        str(local),
        "--remote-folder-id",
        "remote-root",
    ]
    for direction in ([], ["--local-only"]):
        assert app.run_cli([*argv, *direction]) == 0
        assert capsys.readouterr().out.splitlines() == [
            "/foldert/m2.txt",
            "/missing_file1.txt",
        ]

    assert app.run_cli([*argv, "--remote-only"]) == 0
    assert capsys.readouterr().out.splitlines() == ["/remote-only.txt"]
