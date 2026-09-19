from __future__ import annotations

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.client import IncompleteRead
from pathlib import Path

import pytest

from guangya_fastlink.api import Decision, DecisionKind, GuangyaClient, HttpResult
from guangya_fastlink.import_state import MISMATCH_ERROR, open_or_plan_import_state
from guangya_fastlink.models import inspect_export_scope
from guangya_fastlink.runner import (
    RemoteTreeCoordinator,
    create_remote_directories,
    run_import_files,
)


GCID = "58A3F526EE3C569FEECCDFD66DAC9631614E7578"


def write_export(path: Path):
    path.write_text(
        json.dumps({"files": [{"path": "/dir/a.rar", "gcid": GCID, "size": "123"}]}),
        encoding="utf-8",
    )


class FakeClient:
    def __init__(self):
        self.children = {"root": {}}
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
        file_id = f"folder-{name}"
        self.children.setdefault(parent_id, {})[name] = {
            "fileName": name,
            "fileId": file_id,
            "resType": 2,
        }
        self.children.setdefault(file_id, {})
        return Decision(DecisionKind.DIRECTORY_CREATED, file_id=file_id)

    def instant_transfer(self, *, record, parent_id):
        self.transfer_calls += 1
        self.children.setdefault(parent_id, {})[record.file_name] = {
            "fileName": record.file_name,
            "fileId": "file-a",
            "fileSize": record.size,
            "gcid": record.gcid,
            "resType": 1,
        }
        return Decision(DecisionKind.COMPLETED)


class UncertainSuccessClient(FakeClient):
    def instant_transfer(self, *, record, parent_id):
        self.transfer_calls += 1
        self.children.setdefault(parent_id, {})[record.file_name] = {
            "fileName": record.file_name,
            "fileId": "file-a",
            "fileSize": record.size,
            "gcid": record.gcid,
            "resType": 1,
        }
        return Decision(DecisionKind.RETRYABLE, error="response lost")


def test_import_plans_runs_and_resumes_without_duplicate(tmp_path):
    export = tmp_path / "source.json"
    state_path = tmp_path / "state.sqlite3"
    write_export(export)
    scope = inspect_export_scope(export)
    state = open_or_plan_import_state(
        state_path=state_path,
        source_path=export,
        source_sha256=scope.source_sha256,
        target_parent_id="root",
    )
    state.workers = 2
    client = FakeClient()
    coordinator = RemoteTreeCoordinator(client)

    create_remote_directories(state=state, coordinator=coordinator, max_retries=0)
    summary = run_import_files(
        state=state,
        client=client,
        coordinator=coordinator,
        max_retries=0,
        flush_every=1,
    )
    assert summary == {"processed": 1, "credential_fatal": False}
    assert state.stats == {"total": 1, "completed": 1, "not_reusable": 0, "failed": 0}
    assert client.transfer_calls == 1
    state.close()

    resumed = open_or_plan_import_state(
        state_path=state_path,
        source_path=export,
        source_sha256=scope.source_sha256,
        target_parent_id="root",
    )
    resumed.workers = 2
    create_remote_directories(state=resumed, coordinator=coordinator, max_retries=0)
    second = run_import_files(
        state=resumed,
        client=client,
        coordinator=coordinator,
        max_retries=0,
        flush_every=1,
    )
    assert second["processed"] == 0
    assert client.transfer_calls == 1
    resumed.close()


def test_existing_same_file_is_completed_without_transfer(tmp_path):
    export = tmp_path / "source.json"
    write_export(export)
    scope = inspect_export_scope(export)
    state = open_or_plan_import_state(
        state_path=tmp_path / "state.sqlite3",
        source_path=export,
        source_sha256=scope.source_sha256,
        target_parent_id="root",
    )
    client = FakeClient()
    client.children["root"]["dir"] = {
        "fileName": "dir",
        "fileId": "folder-dir",
        "resType": 2,
    }
    client.children["folder-dir"] = {
        "a.rar": {
            "fileName": "a.rar",
            "fileId": "file-a",
            "fileSize": 123,
            "gcid": GCID,
            "resType": 1,
        }
    }
    coordinator = RemoteTreeCoordinator(client)
    create_remote_directories(state=state, coordinator=coordinator, max_retries=0)
    run_import_files(
        state=state,
        client=client,
        coordinator=coordinator,
        max_retries=0,
        flush_every=10,
    )
    assert client.transfer_calls == 0
    assert state.stats["completed"] == 1
    state.close()


def test_existing_different_file_is_collision(tmp_path):
    export = tmp_path / "source.json"
    write_export(export)
    scope = inspect_export_scope(export)
    state = open_or_plan_import_state(
        state_path=tmp_path / "state.sqlite3",
        source_path=export,
        source_sha256=scope.source_sha256,
        target_parent_id="root",
    )
    client = FakeClient()
    client.children["root"]["dir"] = {
        "fileName": "dir",
        "fileId": "folder-dir",
        "resType": 2,
    }
    client.children["folder-dir"] = {
        "a.rar": {
            "fileName": "a.rar",
            "fileId": "other",
            "fileSize": 999,
            "gcid": GCID,
            "resType": 1,
        }
    }
    coordinator = RemoteTreeCoordinator(client)
    create_remote_directories(state=state, coordinator=coordinator, max_retries=0)
    run_import_files(
        state=state,
        client=client,
        coordinator=coordinator,
        max_retries=0,
        flush_every=10,
    )
    assert client.transfer_calls == 0
    assert state.stats["failed"] == 1
    state.close()


def test_state_scope_mismatch_is_rejected(tmp_path):
    export = tmp_path / "source.json"
    write_export(export)
    scope = inspect_export_scope(export)
    state_path = tmp_path / "state.sqlite3"
    state = open_or_plan_import_state(
        state_path=state_path,
        source_path=export,
        source_sha256=scope.source_sha256,
        target_parent_id="root",
    )
    state.close()
    with pytest.raises(ValueError, match=MISMATCH_ERROR):
        open_or_plan_import_state(
            state_path=state_path,
            source_path=export,
            source_sha256=hashlib.sha256(b"other").hexdigest(),
            target_parent_id="root",
        )


def test_uncertain_post_is_reconciled_before_retry(tmp_path):
    export = tmp_path / "source.json"
    export.write_text(
        json.dumps({"files": [{"path": "/a.rar", "gcid": GCID, "size": "123"}]}),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export)
    state = open_or_plan_import_state(
        state_path=tmp_path / "state.sqlite3",
        source_path=export,
        source_sha256=scope.source_sha256,
        target_parent_id="root",
    )
    client = UncertainSuccessClient()
    coordinator = RemoteTreeCoordinator(client)
    run_import_files(
        state=state,
        client=client,
        coordinator=coordinator,
        max_retries=2,
        flush_every=10,
    )
    assert client.transfer_calls == 1
    assert state.stats["completed"] == 1
    state.close()


def test_malformed_remote_item_fails_closed():
    class MalformedClient(FakeClient):
        def list_page(self, *, parent_id, page, page_size=50):
            return Decision(
                DecisionKind.COMPLETED,
                payload={"items": [{"fileName": "a.rar", "resType": 1}], "total": 1},
            )

    coordinator = RemoteTreeCoordinator(MalformedClient())
    with pytest.raises(RuntimeError, match="invalid remote"):
        coordinator.get_child(parent_id="root", name="a.rar", max_retries=0)


def test_empty_remote_id_fails_closed():
    class EmptyIdClient(FakeClient):
        def list_page(self, *, parent_id, page, page_size=50):
            return Decision(
                DecisionKind.COMPLETED,
                payload={
                    "items": [{"fileName": "dir", "fileId": "", "resType": 2}],
                    "total": 1,
                },
            )

    coordinator = RemoteTreeCoordinator(EmptyIdClient())
    with pytest.raises(RuntimeError, match="invalid remote"):
        coordinator.find_directory(parent_id="root", name="dir", max_retries=0)


def test_different_parent_list_requests_can_run_concurrently():
    barrier = threading.Barrier(2)

    class ConcurrentClient(FakeClient):
        def list_page(self, *, parent_id, page, page_size=50):
            barrier.wait(timeout=2)
            return Decision(DecisionKind.COMPLETED, payload={"items": [], "total": 0})

    coordinator = RemoteTreeCoordinator(ConcurrentClient())
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                coordinator.get_child,
                parent_id=parent,
                name="missing",
                max_retries=0,
            )
            for parent in ("one", "two")
        ]
        assert [future.result() for future in futures] == [None, None]


def test_incomplete_transfer_response_reconciles_without_second_post(tmp_path):
    export = tmp_path / "source.json"
    export.write_text(
        json.dumps({"files": [{"path": "/a.rar", "gcid": GCID, "size": "123"}]}),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export)
    state = open_or_plan_import_state(
        state_path=tmp_path / "state.sqlite3",
        source_path=export,
        source_sha256=scope.source_sha256,
        target_parent_id="root",
    )

    class Transport:
        file_exists = False
        transfer_calls = 0

        def __call__(self, method, url, headers, body, timeout):
            if url.endswith("/userres/v1/get_res_center_token"):
                self.transfer_calls += 1
                self.file_exists = True
                raise IncompleteRead(b"partial")
            items = []
            if self.file_exists:
                items = [
                    {
                        "fileId": "file-a",
                        "fileName": "a.rar",
                        "fileSize": 123,
                        "gcid": GCID,
                        "resType": 1,
                    }
                ]
            return HttpResult(
                200, {"msg": "success", "data": {"total": len(items), "list": items}}
            )

    transport = Transport()
    client = GuangyaClient(
        host="https://api.guangyapan.com",
        access_token="secret",
        transport=transport,
    )
    run_import_files(
        state=state,
        client=client,
        coordinator=RemoteTreeCoordinator(client),
        max_retries=2,
        flush_every=10,
    )
    assert transport.transfer_calls == 1
    assert state.stats["completed"] == 1
    state.close()


def test_eventually_consistent_reconciliation_checks_again_before_repost(
    tmp_path, monkeypatch
):
    export = tmp_path / "source.json"
    export.write_text(
        json.dumps({"files": [{"path": "/a.rar", "gcid": GCID, "size": "123"}]}),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export)
    state = open_or_plan_import_state(
        state_path=tmp_path / "state.sqlite3",
        source_path=export,
        source_sha256=scope.source_sha256,
        target_parent_id="root",
    )

    class EventuallyVisibleClient(FakeClient):
        def __init__(self):
            super().__init__()
            self.posted = False
            self.post_list_calls = 0

        def list_page(self, *, parent_id, page, page_size=50):
            if self.posted and page == 0:
                self.post_list_calls += 1
                if self.post_list_calls >= 2:
                    self.children["root"]["a.rar"] = {
                        "fileName": "a.rar",
                        "fileId": "file-a",
                        "fileSize": 123,
                        "gcid": GCID,
                        "resType": 1,
                    }
            return super().list_page(
                parent_id=parent_id, page=page, page_size=page_size
            )

        def instant_transfer(self, *, record, parent_id):
            self.transfer_calls += 1
            self.posted = True
            return Decision(DecisionKind.RETRYABLE, error="response lost")

    client = EventuallyVisibleClient()
    monkeypatch.setattr("guangya_fastlink.runner.compute_backoff", lambda attempt: 0)
    run_import_files(
        state=state,
        client=client,
        coordinator=RemoteTreeCoordinator(client),
        max_retries=2,
        flush_every=10,
    )
    assert client.transfer_calls == 1
    assert state.stats["completed"] == 1
    state.close()


def test_duplicate_paths_are_rejected_by_sqlite_planner(tmp_path):
    export = tmp_path / "duplicate.json"
    export.write_text(
        json.dumps(
            {
                "files": [
                    {"path": "/a.bin", "gcid": GCID, "size": "1"},
                    {"path": "a.bin", "gcid": GCID, "size": "1"},
                ]
            }
        ),
        encoding="utf-8",
    )
    scope = inspect_export_scope(export)
    with pytest.raises(ValueError, match="duplicate normalized path"):
        open_or_plan_import_state(
            state_path=tmp_path / "state.sqlite3",
            source_path=export,
            source_sha256=scope.source_sha256,
            target_parent_id="root",
        )
