from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from guangya_fastlink import app, runner
from guangya_fastlink.api import GuangyaClient, HttpResult
from guangya_fastlink.exporter import DirectoryTask, _scan_directory
from guangya_fastlink.runner import RemoteTreeCoordinator


def item(index, **overrides):
    return {
        "fileId": str(index),
        "fileName": f"{index:03d}.bin",
        "resType": 1,
        "fileSize": 1,
        "gcid": f"{index:040X}",
        **overrides,
    }


def page(indices, total):
    return HttpResult(
        200,
        {
            "msg": "success",
            "data": {
                "list": [item(i) for i in indices],
                "total": total,
            },
        },
    )


class Transport:
    def __init__(self, pages, retry_page=None):
        self.pages = pages
        self.retry_page = retry_page
        self.calls = []
        self.mutations = []

    def __call__(self, method, url, headers, body, timeout):
        query = json.loads(body)
        if not url.endswith("/userres/v1/file/get_file_list"):
            self.mutations.append(url)
            return HttpResult(400, {"msg": "unexpected mutation"})
        assert query["pageSize"] == 50
        self.calls.append(query["page"])
        if self.retry_page == query["page"]:
            self.retry_page = None
            raise TimeoutError("test timeout")
        return self.pages[query["page"]]

    def client(self):
        return GuangyaClient(
            host="https://example.test", access_token="fixture", transport=self
        )


def read_all(engine, transport):
    if engine == "coordinator":
        coordinator = RemoteTreeCoordinator(transport.client())
        return coordinator.get_child(parent_id="root", name="119.bin", max_retries=1)
    return _scan_directory(
        client=transport.client(), task=DirectoryTask("root", ""), max_retries=1
    )


@pytest.mark.parametrize("engine", ["coordinator", "scanner"])
@pytest.mark.parametrize(
    "tail",
    [
        page([], 120),
        HttpResult(200, {"msg": "success", "data": {}}),
        page(range(50, 100), 100),
        page(range(50, 100), 121),
        page(range(40, 90), 120),
        HttpResult(
            200,
            {
                "msg": "success",
                "data": {
                    "list": [
                        item(49, fileName="renamed.bin"),
                        *[item(i) for i in range(50, 99)],
                    ],
                    "total": 120,
                },
            },
        ),
        page(range(50, 121), 120),
    ],
)
def test_inconsistent_pages_fail_closed(engine, tail):
    transport = Transport([page(range(50), 120), tail, page([], 120)])
    with pytest.raises(RuntimeError):
        read_all(engine, transport)


@pytest.mark.parametrize("engine", ["coordinator", "scanner"])
@pytest.mark.parametrize("count", [0, 50, 100, 120])
def test_complete_pagination_and_retry(engine, count, monkeypatch):
    pages = [
        page(range(start, min(start + 50, count)), count)
        for start in range(0, max(1, count), 50)
    ]
    transport = Transport(pages, retry_page=1 if count > 50 else None)
    monkeypatch.setattr(runner, "compute_backoff", lambda attempt: 0)
    result = read_all(engine, transport)
    expected_calls = [0, 1, 1, *range(2, len(pages))] if count > 50 else [0]
    assert transport.calls == expected_calls
    if engine == "scanner":
        assert len(result[1]) == count
    else:
        assert (result is not None) == (count == 120)


def test_failed_listing_is_not_cached():
    transport = Transport([page(range(50), 120), page([], 120)])
    coordinator = RemoteTreeCoordinator(transport.client())
    with pytest.raises(RuntimeError):
        coordinator.classify_file(
            parent_id="root",
            record=SimpleNamespace(
                file_name="119.bin",
                gcid=item(119)["gcid"],
                size=1,
            ),
            max_retries=0,
        )
    transport.pages = [
        page(range(50), 120),
        page(range(50, 100), 120),
        page(range(100, 120), 120),
    ]
    assert coordinator.get_child(parent_id="root", name="119.bin", max_retries=0)
    assert transport.calls == [0, 1, 0, 1, 2]


@pytest.mark.parametrize("engine", ["coordinator", "scanner"])
@pytest.mark.parametrize("rename", [False, True])
def test_replayed_ids_cannot_satisfy_total(engine, rename):
    repeated = item(49, fileName="renamed.bin") if rename else item(49)
    transport = Transport(
        [
            page(range(50), 100),
            HttpResult(
                200,
                {
                    "msg": "success",
                    "data": {
                        "list": [repeated, *[item(i) for i in range(50, 99)]],
                        "total": 100,
                    },
                },
            ),
        ]
    )
    with pytest.raises(RuntimeError, match="duplicate remote fileId"):
        read_all(engine, transport)


@pytest.mark.parametrize(
    "command",
    [
        "import-json",
        "batch-import-json",
        "batch-check-json",
        "export-json",
        "compare-json",
        "compare-path",
    ],
)
def test_commands_reject_partial_lists_then_recover(
    command, tmp_path, monkeypatch, capsys
):
    source = tmp_path / "input" / "source.json"
    source.parent.mkdir()
    source.write_text(
        json.dumps(
            {"files": [{"path": "/119.bin", "size": "1", "gcid": item(119)["gcid"]}]}
        ),
        encoding="utf-8",
    )
    output = tmp_path / "out.json"
    output.write_text("previous output", encoding="utf-8")
    state = tmp_path / "state"
    if command == "import-json":
        argv = [
            command,
            "--file",
            str(source),
            "--target-parent-id",
            "root",
            "--state-file",
            str(state / "import.sqlite3"),
        ]
    elif command in {"batch-import-json", "batch-check-json"}:
        argv = [
            command,
            "--input-dir",
            str(source.parent),
            "--target-parent-id",
            "root",
            "--state-dir",
            str(state),
        ]
        if command == "batch-check-json":
            output = tmp_path / "delta" / "source.delta.export.json"
            output.parent.mkdir()
            output.write_text("previous output", encoding="utf-8")
            argv += ["--output-dir", str(output.parent)]
    elif command == "export-json":
        argv = [
            command,
            "--source-parent-id",
            "root",
            "--output-file",
            str(output),
            "--state-file",
            str(state / "export.state.json"),
        ]
    else:
        argv = ["compare-folder", "--remote-folder-id", "root"]
        if command == "compare-json":
            argv += ["--local-json", str(source), "--output-file", str(output)]
        else:
            local = tmp_path / "local"
            local.mkdir()
            (local / "119.bin").touch()
            argv += ["--local-folder", str(local)]
    argv += ["--max-retries", "0"]
    transport = Transport([page(range(50), 120), page([], 120)])
    monkeypatch.setattr(app, "build_client", transport.client)
    assert app.run_cli(argv) == 1
    assert output.read_text(encoding="utf-8") == "previous output"
    assert transport.mutations == []
    capsys.readouterr()

    transport.pages = [
        page(range(50), 120),
        page(range(50, 100), 120),
        page(range(100, 120), 120),
    ]
    assert app.run_cli(argv) == 0
    assert transport.calls[-3:] == [0, 1, 2]
    assert transport.mutations == []
    if command == "batch-check-json":
        assert not output.exists()
    elif command == "compare-json":
        assert json.loads(output.read_text(encoding="utf-8"))["files"] == []
    elif command == "export-json":
        assert json.loads(output.read_text(encoding="utf-8"))["totalFilesCount"] == 120
    elif command == "compare-path":
        assert capsys.readouterr().out == ""


@pytest.mark.parametrize("command", ["export-json", "compare-folder"])
def test_overlapping_pages_never_publish_output(command, tmp_path, monkeypatch):
    output = tmp_path / "out.json"
    output.write_text("previous output", encoding="utf-8")
    transport = Transport([page(range(50), 100), page(range(40, 90), 100)])
    monkeypatch.setattr(app, "build_client", transport.client)
    if command == "export-json":
        argv = [
            command,
            "--source-parent-id",
            "root",
            "--state-file",
            str(tmp_path / "state.json"),
        ]
    else:
        source = tmp_path / "local.json"
        source.write_text('{"files": []}', encoding="utf-8")
        argv = [
            command,
            "--remote-folder-id",
            "root",
            "--local-json",
            str(source),
            "--remote-only",
        ]
    assert app.run_cli([*argv, "--output-file", str(output)]) == 1
    assert output.read_text(encoding="utf-8") == "previous output"
