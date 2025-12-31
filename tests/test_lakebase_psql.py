import pytest
from sqlalchemy import create_engine

import lakebase_psql


@pytest.fixture()
def sqlite_engine(monkeypatch):
    """Provide an in-memory SQLite engine and patch lakebase_psql to use it."""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    monkeypatch.setattr(lakebase_psql, "get_engine", lambda: engine)
    return engine


def test_set_and_get_roundtrip(sqlite_engine):
    lakebase_psql.set_config("feature_flag", "on")
    assert lakebase_psql.get_config("feature_flag") == "on"


def test_set_config_overwrites_existing_value(sqlite_engine):
    lakebase_psql.set_config("feature_flag", "on")
    lakebase_psql.set_config("feature_flag", "off")
    assert lakebase_psql.get_config("feature_flag") == "off"


def test_get_config_returns_none_when_missing_key(sqlite_engine):
    # Ensure the config table exists without inserting a value.
    lakebase_psql._ensure_config_table(sqlite_engine)
    assert lakebase_psql.get_config("missing_key") is None
