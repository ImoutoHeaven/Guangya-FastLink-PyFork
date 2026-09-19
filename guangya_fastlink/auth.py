from __future__ import annotations

import ipaddress
import os
import re
from dataclasses import dataclass
from urllib.parse import urlparse


HTTPS_HOST_RE = re.compile(r"https://[A-Za-z0-9.-]+(?::\d+)?")
HTTPS_IPV6_RE = re.compile(r"https://\[(?P<host>[0-9A-Fa-f:.]+)\](?::\d+)?")


@dataclass(frozen=True)
class Credentials:
    host: str
    access_token: str


def validate_https_origin(value: str) -> str:
    normalized = value.rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError("GUANGYA_HOST must be an absolute https origin")
    if (
        "@" in parsed.netloc
        or parsed.path
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("GUANGYA_HOST must be an absolute https origin")
    if HTTPS_HOST_RE.fullmatch(normalized):
        return normalized
    match = HTTPS_IPV6_RE.fullmatch(normalized)
    if not match:
        raise ValueError("GUANGYA_HOST must be an absolute https origin")
    try:
        ipaddress.IPv6Address(match.group("host"))
    except ValueError as exc:
        raise ValueError("GUANGYA_HOST must be an absolute https origin") from exc
    return normalized


def load_credentials() -> Credentials:
    host = validate_https_origin(
        os.environ.get("GUANGYA_HOST", "https://api.guangyapan.com")
    )
    token = os.environ.get("GUANGYA_ACCESS_TOKEN", "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        raise ValueError("GUANGYA_ACCESS_TOKEN is required")
    if any(not 33 <= ord(character) <= 126 for character in token):
        raise ValueError("GUANGYA_ACCESS_TOKEN contains invalid characters")
    return Credentials(host=host, access_token=token)
