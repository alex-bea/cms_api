"""Tests for the ingestion schema preflight check."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from cms_pricing.ingestion.utils import assert_schema_is_current, SchemaOutOfDateError


@pytest.fixture
def mock_script_directory():
    with patch("cms_pricing.ingestion.utils.schema.ScriptDirectory.from_config") as factory:
        script = MagicMock()
        factory.return_value = script
        yield script


def _session_with_version(version: str):
    session = MagicMock(spec=Session)
    connection = MagicMock()
    result = MagicMock()
    result.first.return_value = (version,)
    connection.execute.return_value = result
    session.connection.return_value = connection
    return session


def test_assert_schema_is_current_passes_when_revision_matches(mock_script_directory):
    mock_script_directory.get_current_head.return_value = "1234abcd"
    session = _session_with_version("1234abcd")

    revision = assert_schema_is_current(session)

    assert revision == "1234abcd"
    session.connection.assert_called_once()
    session.connection.return_value.execute.assert_called_once()


def test_assert_schema_is_current_raises_for_stale_revision(mock_script_directory):
    mock_script_directory.get_current_head.return_value = "expected"
    session = _session_with_version("stale")

    with pytest.raises(SchemaOutOfDateError) as excinfo:
        assert_schema_is_current(session)

    assert "alembic upgrade head" in str(excinfo.value)


def test_assert_schema_is_current_handles_missing_version(mock_script_directory):
    mock_script_directory.get_current_head.return_value = "expected"
    session = _session_with_version("expected")
    session.connection.return_value.execute.return_value.first.return_value = None

    with pytest.raises(SchemaOutOfDateError):
        assert_schema_is_current(session)
