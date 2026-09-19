from __future__ import annotations

import json
from http.client import IncompleteRead
from types import SimpleNamespace
from urllib.request import Request

import pytest

from guangya_fastlink.api import (
    DecisionKind,
    GuangyaClient,
    HttpResult,
    _RejectRedirects,
)


GCID = "58A3F526EE3C569FEECCDFD66DAC9631614E7578"


class FakeTransport:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, headers, body, timeout):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "body": json.loads(body),
                "timeout": timeout,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def client(transport):
    return GuangyaClient(
        host="https://api.guangyapan.com",
        access_token="secret",
        transport=transport,
    )


def test_list_page_accepts_empty_data_and_uses_captured_protocol():
    transport = FakeTransport(HttpResult(200, {"msg": "success", "data": {}}))
    decision = client(transport).list_page(parent_id="", page=0)

    assert decision.kind is DecisionKind.COMPLETED
    assert decision.payload == {"items": [], "total": 0}
    assert transport.calls[0]["body"] == {
        "pageSize": 50,
        "orderBy": 3,
        "sortType": 1,
        "parentId": "",
        "page": 0,
    }
    assert transport.calls[0]["headers"] == {
        "Authorization": "Bearer secret",
        "Content-Type": "application/json",
    }


def test_list_page_accepts_missing_list_on_empty_later_page():
    transport = FakeTransport(HttpResult(200, {"msg": "success", "data": {"total": 1}}))
    decision = client(transport).list_page(parent_id="1", page=1)
    assert decision.payload == {"items": [], "total": 1}


def test_list_page_rejects_falsey_non_list_and_missing_total():
    malformed_list = FakeTransport(
        HttpResult(200, {"msg": "success", "data": {"list": {}, "total": 0}})
    )
    assert (
        client(malformed_list).list_page(parent_id="", page=0).kind
        is DecisionKind.FAILED
    )

    missing_total = FakeTransport(
        HttpResult(
            200,
            {"msg": "success", "data": {"list": [{"fileId": "1"}]}},
        )
    )
    assert (
        client(missing_total).list_page(parent_id="", page=0).kind
        is DecisionKind.FAILED
    )


def test_mkdir_treats_duplicate_code_as_existing_directory():
    transport = FakeTransport(
        HttpResult(
            200,
            {
                "code": 159,
                "msg": "文件夹名称重复",
                "data": {"fileId": "123", "resType": 2},
            },
        )
    )
    decision = client(transport).mkdir(parent_id="", name="Movies")
    assert decision.kind is DecisionKind.DIRECTORY_CREATED
    assert decision.file_id == "123"


@pytest.mark.parametrize("file_id", ["", True, {}, []])
def test_mkdir_rejects_invalid_directory_id(file_id):
    transport = FakeTransport(
        HttpResult(200, {"msg": "success", "data": {"fileId": file_id}})
    )
    decision = client(transport).mkdir(parent_id="", name="Movies")
    assert decision.kind is DecisionKind.FAILED


def test_instant_transfer_uses_only_verified_candidate():
    transport = FakeTransport(
        HttpResult(200, {"code": 156, "msg": "上传已完成", "data": {"taskId": "9"}})
    )
    record = SimpleNamespace(gcid=GCID, size=123, file_name="a.rar")
    decision = client(transport).instant_transfer(record=record, parent_id="456")

    assert decision.kind is DecisionKind.COMPLETED
    assert transport.calls[0]["body"] == {
        "capacity": 2,
        "res": {"gcid": GCID.lower(), "md5": GCID.lower()[:32], "fileSize": 123},
        "name": "a.rar",
        "parentId": "456",
    }


def test_non_instant_task_is_cleaned_and_marked_not_reusable():
    transport = FakeTransport(
        HttpResult(200, {"msg": "success", "data": {"taskId": "9"}}),
        HttpResult(200, {"msg": "success"}),
    )
    record = SimpleNamespace(gcid=GCID, size=123, file_name="a.rar")
    decision = client(transport).instant_transfer(record=record, parent_id="456")

    assert decision.kind is DecisionKind.NOT_REUSABLE
    assert transport.calls[1]["url"].endswith("/userres/v1/file/delete_upload_task")
    assert transport.calls[1]["body"] == {"taskIds": ["9"]}


def test_invalid_token_is_credential_fatal():
    transport = FakeTransport(HttpResult(401, {"code": 117, "msg": "无效token"}))
    decision = client(transport).list_page(parent_id="", page=0)
    assert decision.kind is DecisionKind.CREDENTIAL_FATAL


def test_timeout_is_retryable():
    transport = FakeTransport(TimeoutError("timed out"))
    decision = client(transport).list_page(parent_id="", page=0)
    assert decision.kind is DecisionKind.RETRYABLE


def test_incomplete_response_is_retryable():
    transport = FakeTransport(IncompleteRead(b"partial"))
    decision = client(transport).list_page(parent_id="", page=0)
    assert decision.kind is DecisionKind.RETRYABLE


def test_redirects_are_rejected_before_authorization_can_be_forwarded():
    request = Request(
        "https://api.guangyapan.com/start",
        headers={"Authorization": "Bearer secret"},
    )
    redirected = _RejectRedirects().redirect_request(
        request,
        None,
        302,
        "Found",
        {"Location": "https://attacker.invalid/steal"},
        "https://attacker.invalid/steal",
    )
    assert redirected is None


def test_invalid_success_json_is_retryable_for_reconciliation():
    transport = FakeTransport(HttpResult(200, None))
    record = SimpleNamespace(gcid=GCID, size=123, file_name="a.rar")
    decision = client(transport).instant_transfer(record=record, parent_id="456")
    assert decision.kind is DecisionKind.RETRYABLE
