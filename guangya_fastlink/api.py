from __future__ import annotations

import json
import socket
import ssl
from http.client import HTTPException
from dataclasses import dataclass
from enum import Enum
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from guangya_fastlink.models import normalize_remote_id


class DecisionKind(Enum):
    RETRYABLE = "retryable"
    CREDENTIAL_FATAL = "credential_fatal"
    FAILED = "failed"
    NOT_REUSABLE = "not_reusable"
    COMPLETED = "completed"
    DIRECTORY_CREATED = "directory_created"


@dataclass(frozen=True)
class Decision:
    kind: DecisionKind
    error: str | None = None
    file_id: str | None = None
    payload: dict | None = None


@dataclass(frozen=True)
class HttpResult:
    status: int
    payload: object


Transport = Callable[[str, str, dict[str, str], bytes, float], HttpResult]


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = build_opener(_RejectRedirects())


def _default_transport(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes,
    timeout: float,
) -> HttpResult:
    request = Request(url, data=body, headers=headers, method=method)
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            status = int(response.status)
            raw = response.read()
    except HTTPError as exc:
        status = int(exc.code)
        raw = exc.read()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    return HttpResult(status=status, payload=payload)


def _transport_failure(exc: Exception) -> Decision:
    if isinstance(
        exc,
        (
            TimeoutError,
            socket.timeout,
            ssl.SSLError,
            URLError,
            ConnectionError,
            OSError,
            HTTPException,
        ),
    ):
        return Decision(DecisionKind.RETRYABLE, error=str(exc))
    return Decision(DecisionKind.FAILED, error=str(exc))


def _http_failure(result: HttpResult) -> Decision | None:
    payload = result.payload if isinstance(result.payload, dict) else {}
    code = payload.get("code")
    message = payload.get("msg")
    error = str(message or f"HTTP {result.status}")
    if result.status in {401, 403} or code == 117:
        return Decision(DecisionKind.CREDENTIAL_FATAL, error=error, payload=payload)
    if result.status == 429 or 500 <= result.status <= 599:
        return Decision(DecisionKind.RETRYABLE, error=error, payload=payload)
    if result.status < 200 or result.status >= 300:
        return Decision(DecisionKind.FAILED, error=error, payload=payload)
    if not isinstance(result.payload, dict):
        return Decision(DecisionKind.RETRYABLE, error="invalid json response")
    return None


class GuangyaClient:
    def __init__(
        self,
        *,
        host: str,
        access_token: str,
        timeout: float = 30.0,
        transport: Transport | None = None,
    ) -> None:
        self.host = host.rstrip("/")
        self.timeout = timeout
        self._transport = transport or _default_transport
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    def _post(self, path: str, payload: dict) -> HttpResult | Decision:
        try:
            result = self._transport(
                "POST",
                f"{self.host}{path}",
                dict(self._headers),
                json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                self.timeout,
            )
        except Exception as exc:
            return _transport_failure(exc)
        return _http_failure(result) or result

    def list_page(self, *, parent_id: str, page: int, page_size: int = 50) -> Decision:
        result = self._post(
            "/userres/v1/file/get_file_list",
            {
                "pageSize": page_size,
                "orderBy": 3,
                "sortType": 1,
                "parentId": str(parent_id),
                "page": int(page),
            },
        )
        if isinstance(result, Decision):
            return result
        payload = result.payload
        data = payload.get("data")
        if data is None:
            data = {}
        if payload.get("msg") != "success" or not isinstance(data, dict):
            return Decision(
                DecisionKind.FAILED,
                error=str(payload.get("msg") or "invalid list response"),
            )
        raw_items = data.get("list")
        items = [] if raw_items is None else raw_items
        raw_total = data.get("total")
        total = 0 if raw_total is None and not items else raw_total
        if (
            not isinstance(items, list)
            or isinstance(total, bool)
            or not isinstance(total, int)
            or total < len(items)
        ):
            return Decision(
                DecisionKind.FAILED, error="invalid list data", payload=payload
            )
        return Decision(
            DecisionKind.COMPLETED,
            payload={"items": items, "total": total},
        )

    def mkdir(self, *, parent_id: str, name: str) -> Decision:
        result = self._post(
            "/userres/v1/file/create_dir",
            {"dirName": name, "parentId": str(parent_id), "failIfNameExist": True},
        )
        if isinstance(result, Decision):
            return result
        payload = result.payload
        code = payload.get("code")
        if payload.get("msg") != "success" and code != 159:
            return Decision(
                DecisionKind.FAILED,
                error=str(payload.get("msg") or f"api code {code}"),
                payload=payload,
            )
        data = payload.get("data")
        raw_file_id = data.get("fileId") if isinstance(data, dict) else None
        try:
            file_id = normalize_remote_id(raw_file_id)
        except ValueError:
            return Decision(
                DecisionKind.FAILED, error="missing directory fileId", payload=payload
            )
        return Decision(
            DecisionKind.DIRECTORY_CREATED,
            file_id=file_id,
            payload=payload,
        )

    def instant_transfer(self, *, record, parent_id: str) -> Decision:
        gcid = str(record.gcid).lower()
        result = self._post(
            "/userres/v1/get_res_center_token",
            {
                "capacity": 2,
                "res": {
                    "gcid": gcid,
                    "md5": gcid[:32],
                    "fileSize": int(record.size),
                },
                "name": str(record.file_name),
                "parentId": str(parent_id),
            },
        )
        if isinstance(result, Decision):
            return result
        payload = result.payload
        if payload.get("code") == 156:
            return Decision(DecisionKind.COMPLETED, payload=payload)
        data = payload.get("data")
        task_id = data.get("taskId") if isinstance(data, dict) else None
        if task_id is not None:
            self._delete_upload_task(str(task_id))
            return Decision(
                DecisionKind.NOT_REUSABLE,
                error=str(payload.get("msg") or "strict rapid transfer missed"),
                payload=payload,
            )
        return Decision(
            DecisionKind.FAILED,
            error=str(payload.get("msg") or f"api code {payload.get('code')}"),
            payload=payload,
        )

    def _delete_upload_task(self, task_id: str) -> None:
        self._post(
            "/userres/v1/file/delete_upload_task",
            {"taskIds": [task_id]},
        )
