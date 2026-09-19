from __future__ import annotations

import sqlite3

import pytest

from guangya_fastlink.check_state import (
    MALFORMED_STATE_ERROR,
    open_or_plan_check_state,
)


def test_legacy_schema_version_is_rejected_immediately(tmp_path):
    state_path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(state_path)
    connection.execute(
        """
        CREATE TABLE job (
            singleton INTEGER PRIMARY KEY,
            schema_version INTEGER,
            source_sha256 TEXT,
            target_parent_id TEXT,
            compare_mode TEXT
        )
        """
    )
    connection.execute("INSERT INTO job VALUES (1, 1, 'hash', 'root', 'exist_only')")
    connection.commit()
    connection.close()

    with pytest.raises(ValueError, match=MALFORMED_STATE_ERROR):
        open_or_plan_check_state(
            state_path=state_path,
            source_path=tmp_path / "source.json",
            source_sha256="hash",
            target_parent_id="root",
            compare_mode="exist_only",
        )
