from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

from guangya_fastlink.api import Decision, DecisionKind
from guangya_fastlink.batch import run_batch_check


GCID = "58A3F526EE3C569FEECCDFD66DAC9631614E7578"


class FakeClient:
    def __init__(self, *, matching: bool):
        self.matching = matching
        self.list_calls = 0

    def list_page(self, *, parent_id, page, page_size=50):
        self.list_calls += 1
        if page:
            items = []
        elif parent_id == "root":
            items = [{"fileId": "dir-id", "fileName": "dir", "resType": 2}]
        else:
            items = [
                {
                    "fileId": "file-id",
                    "fileName": "a.rar",
                    "fileSize": 123 if self.matching else 999,
                    "gcid": GCID,
                    "resType": 1,
                }
            ]
        return Decision(
            DecisionKind.COMPLETED,
            payload={"items": items, "total": len(items)},
        )


def config(tmp_path, *, mode="with_checksum"):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "one.json").write_text(
        json.dumps({"files": [{"path": "/dir/a.rar", "gcid": GCID, "size": "123"}]}),
        encoding="utf-8",
    )
    return SimpleNamespace(
        input_dir=input_dir,
        target_parent_id="root",
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output",
        workers=2,
        json_parallelism=1,
        max_retries=0,
        compare_mode=mode,
    )


def test_batch_check_aligned_writes_no_delta(tmp_path):
    cfg = config(tmp_path)
    assert run_batch_check(config=cfg, client=FakeClient(matching=True)) == 0
    assert not (cfg.output_dir / "one.delta.export.json").exists()


def test_batch_check_checksum_mismatch_writes_delta(tmp_path):
    cfg = config(tmp_path)
    assert run_batch_check(config=cfg, client=FakeClient(matching=False)) == 0
    output = cfg.output_dir / "one.delta.export.json"
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["files"][0]["path"] == "/dir/a.rar"
    assert payload["files"][0]["gcid"] == GCID


def test_switching_compare_mode_reuses_state_and_removes_stale_delta(tmp_path):
    cfg = config(tmp_path)
    client = FakeClient(matching=False)
    assert run_batch_check(config=cfg, client=client) == 0
    output = cfg.output_dir / "one.delta.export.json"
    assert output.exists()
    calls_after_scan = client.list_calls

    cfg.compare_mode = "exist_only"
    assert run_batch_check(config=cfg, client=client) == 0
    assert not output.exists()
    assert client.list_calls == calls_after_scan


def test_v2_snapshot_is_rescanned_before_reusing_missing_results(tmp_path):
    class EmptyClient(FakeClient):
        def list_page(self, **kwargs):
            return Decision(DecisionKind.COMPLETED, payload={"items": [], "total": 0})

    cfg = config(tmp_path)
    assert run_batch_check(config=cfg, client=EmptyClient(matching=True)) == 0
    output = cfg.output_dir / "one.delta.export.json"
    assert output.exists()
    # Persist the same schema-v2 cache shape produced by the old paginator.
    with sqlite3.connect(cfg.state_dir / "one.check-state.sqlite3") as connection:
        connection.execute("UPDATE job SET schema_version = 2")
    client = FakeClient(matching=True)
    assert run_batch_check(config=cfg, client=client) == 0
    assert client.list_calls == 2
    assert not output.exists()
    assert run_batch_check(config=cfg, client=client) == 0
    assert client.list_calls == 2
