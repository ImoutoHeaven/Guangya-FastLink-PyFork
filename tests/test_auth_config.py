from __future__ import annotations

import pytest

from guangya_fastlink.auth import load_credentials, validate_https_origin


def test_credentials_strip_bearer_prefix(monkeypatch):
    monkeypatch.setenv("GUANGYA_ACCESS_TOKEN", "Bearer secret")
    monkeypatch.delenv("GUANGYA_HOST", raising=False)
    credentials = load_credentials()
    assert credentials.host == "https://api.guangyapan.com"
    assert credentials.access_token == "secret"


@pytest.mark.parametrize(
    "host",
    ["http://api.guangyapan.com", "https://user@host", "https://host/path"],
)
def test_invalid_hosts_are_rejected(host):
    with pytest.raises(ValueError, match="GUANGYA_HOST"):
        validate_https_origin(host)


def test_token_control_characters_are_rejected(monkeypatch):
    monkeypatch.setenv("GUANGYA_ACCESS_TOKEN", "secret\r\nInjected: value")
    with pytest.raises(ValueError, match="invalid characters"):
        load_credentials()
