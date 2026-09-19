from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from guangya_fastlink.api import Decision, DecisionKind
from guangya_fastlink.exporter import _prepare_sidecar, run_export


GCID_A = "58A3F526EE3C569FEECCDFD66DAC9631614E7578"
GCID_B = "8A154A225F16FC3A8C6D554164BC29D3B0053490"


class FakeListClient:
    def __init__(self):
        self.items = {
            "root": [
                {"fileId": "folder", "fileName": "dir", "resType": 2},
                {
                    "fileId": "a",
                    "fileName": "a.rar",
                    "fileSize": 10,
                    "gcid": GCID_A,
                    "parentId": "root",
                    "resType": 1,
                },
            ],
            "folder": [
                {
                    "fileId": "b",
                    "fileName": "b.rar",
                    "fileSize": 20,
                    "gcid": GCID_B,
                    "parentId": "folder",
                    "resType": 1,
                }
            ],
        }

    def list_page(self, *, parent_id, page, page_size=50):
        items = self.items.get(parent_id, []) if page == 0 else []
        return Decision(
            DecisionKind.COMPLETED,
            payload={"items": items, "total": len(items)},
        )


def config(tmp_path, source="root"):
    return SimpleNamespace(
        source_parent_id=source,
        output_file=tmp_path / "out.json",
        state_file=tmp_path / "export.state.json",
        workers=2,
        max_retries=0,
    )


def test_recursive_export_writes_userscript_format_and_cleans_state(tmp_path):
    cfg = config(tmp_path)
    assert run_export(client=FakeListClient(), config=cfg) == 0

    payload = json.loads(cfg.output_file.read_text(encoding="utf-8"))
    assert payload["sourceTag"] == "guangya"
    assert payload["totalFilesCount"] == 2
    assert payload["totalSize"] == 30
    assert payload["files"] == [
        {
            "size": "10",
            "path": "/a.rar",
            "gcid": GCID_A,
            "fileId": "a",
            "parentId": "root",
            "sourceGuangya": True,
        },
        {
            "size": "20",
            "path": "/dir/b.rar",
            "gcid": GCID_B,
            "fileId": "b",
            "parentId": "folder",
            "sourceGuangya": True,
        },
    ]
    assert not cfg.state_file.exists()
    assert not cfg.state_file.with_suffix(".records.jsonl").exists()


def test_export_rejects_empty_source(tmp_path):
    cfg = config(tmp_path)
    client = FakeListClient()
    client.items = {"root": []}
    with pytest.raises(RuntimeError, match="zero files"):
        run_export(client=client, config=cfg)


def test_output_can_use_a_different_directory_from_state(tmp_path):
    cfg = config(tmp_path)
    cfg.output_file = tmp_path / "output" / "out.json"
    cfg.state_file = tmp_path / "state" / "export.state.json"
    assert run_export(client=FakeListClient(), config=cfg) == 0
    assert cfg.output_file.exists()
    assert not (cfg.output_file.parent / f".{cfg.output_file.name}.tmp").exists()


def test_resume_truncates_only_incomplete_sidecar_tail(tmp_path):
    sidecar = tmp_path / "records.jsonl"
    sidecar.write_bytes(b'{"path":"a"}\n{"path":"b"}\n{"partial"')
    _prepare_sidecar(sidecar)
    assert sidecar.read_bytes() == b'{"path":"a"}\n{"path":"b"}\n'


def test_export_rejects_empty_remote_id(tmp_path):
    cfg = config(tmp_path)
    client = FakeListClient()
    client.items = {"root": [{"fileId": "", "fileName": "dir", "resType": 2}]}
    with pytest.raises(RuntimeError, match="missing name or id"):
        run_export(client=client, config=cfg)
