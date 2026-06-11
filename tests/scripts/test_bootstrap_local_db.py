import os

from cms_pricing import database
from cms_pricing.config import settings
from scripts.bootstrap_local_db import configure_database_url


def test_configure_database_url_overrides_env_settings_and_session_factory(
    monkeypatch,
):
    original_database_url = settings.database_url
    original_test_database_url = settings.test_database_url
    override_url = "sqlite://"

    monkeypatch.setenv(
        "TEST_DATABASE_URL",
        "postgresql://user:pass@remote.example.test:5432/remote_db",
    )

    try:
        configure_database_url(override_url)

        assert os.environ["DATABASE_URL"] == override_url
        assert os.environ["TEST_DATABASE_URL"] == override_url
        assert settings.database_url == override_url
        assert settings.test_database_url == override_url
        assert str(database.engine.url) == override_url
        assert str(database.SessionLocal.kw["bind"].url) == override_url
    finally:
        configure_database_url(original_database_url)
        settings.test_database_url = original_test_database_url
        os.environ["TEST_DATABASE_URL"] = original_test_database_url
