"""Schema preflight helpers used by ingestion pipelines."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Union, Iterator

import structlog
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

logger = structlog.get_logger(__name__)


class SchemaOutOfDateError(RuntimeError):
    """Raised when the database schema revision is behind the expected Alembic head."""


@contextmanager
def _managed_connection(db: Union[Session, Engine, Connection]) -> Iterator[Connection]:
    """Yield a SQLAlchemy connection, handling lifecycle for various inputs."""

    if isinstance(db, Session):
        connection = db.connection()
        close_after_use = False
    elif isinstance(db, Engine):
        connection = db.connect()
        close_after_use = True
    elif isinstance(db, Connection):
        connection = db
        close_after_use = False
    elif hasattr(db, "connection"):
        connection = db.connection()
        close_after_use = False
    else:
        raise TypeError(
            "assert_schema_is_current expects a Session, Engine, or Connection; "
            f"got {type(db)!r}"
        )

    try:
        yield connection
    finally:
        if close_after_use:
            connection.close()


def _load_expected_head(alembic_ini_path: Optional[Union[str, Path]]) -> str:
    """Return the current Alembic head revision from migration scripts."""

    if alembic_ini_path is None:
        alembic_ini_path = Path(__file__).resolve().parents[3] / "alembic.ini"

    cfg = Config(str(alembic_ini_path))
    script_directory = ScriptDirectory.from_config(cfg)
    return script_directory.get_current_head()


def _extract_version(result_row) -> Optional[str]:
    """Coerce Alembic version row into a simple string."""

    if result_row is None:
        return None

    try:
        # SQLAlchemy 1.x returns tuple-like rows
        return result_row[0]
    except (TypeError, KeyError, IndexError):
        pass

    try:
        return result_row["version_num"]
    except Exception:  # pragma: no cover - very defensive
        return getattr(result_row, "version_num", None)


def assert_schema_is_current(
    db: Union[Session, Engine, Connection],
    *,
    alembic_ini_path: Optional[Union[str, Path]] = None,
) -> str:
    """Ensure the connected database matches the latest Alembic head.

    Args:
        db: SQLAlchemy session, engine, or connection pointing at the target database.
        alembic_ini_path: Optional path to ``alembic.ini`` if a non-standard location is used.

    Returns:
        The current database revision if it matches the Alembic head.

    Raises:
        SchemaOutOfDateError: If the schema revision is missing or does not match the Alembic head.
    """

    expected_head = _load_expected_head(alembic_ini_path)

    with _managed_connection(db) as connection:
        try:
            result = connection.execute(text("SELECT version_num FROM alembic_version"))
            current_row = result.first()
        except SQLAlchemyError as exc:
            logger.error("Failed to read alembic_version", error=str(exc))
            raise SchemaOutOfDateError(
                "Unable to determine current database schema revision. "
                "Run `alembic upgrade head` before retrying ingestion."
            ) from exc

    current_revision = _extract_version(current_row)
    if not current_revision:
        raise SchemaOutOfDateError(
            "Database does not report an Alembic revision. "
            "Run `alembic upgrade head` before retrying ingestion."
        )

    if current_revision != expected_head:
        raise SchemaOutOfDateError(
            "Database schema revision '{current}' does not match Alembic head '{head}'. "
            "Run `alembic upgrade head` before retrying ingestion.".format(
                current=current_revision,
                head=expected_head,
            )
        )

    logger.debug(
        "Database schema is current",
        current_revision=current_revision,
        expected_head=expected_head,
    )

    return current_revision
