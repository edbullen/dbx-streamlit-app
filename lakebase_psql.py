import os
from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from databricks.sdk import WorkspaceClient

# for local development - picks up variables from .env file
load_dotenv()

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
workspace_client: WorkspaceClient = (
    WorkspaceClient(host=f"https://{server_hostname}") if server_hostname else WorkspaceClient()
)


def provide_token(dialect, conn_rec, cargs, cparams) -> None:
    """Inject an OAuth token on every new DB connection."""
    cparams["password"] = workspace_client.config.oauth_token().access_token


def _build_engine() -> Engine:
    postgres_host = os.getenv("PGHOST")
    postgres_database = os.getenv("PGDATABASE")
    postgres_port = os.getenv("PGPORT", "5432")
    postgres_username = os.getenv("PGUSER")

    missing = [name for name, value in {"PGHOST": postgres_host, "PGDATABASE": postgres_database, "PGUSER": postgres_username}.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required environment variables for Postgres connection: {', '.join(sorted(missing))}")

    engine = create_engine(
        f"postgresql+psycopg://{postgres_username}:@{postgres_host}:{postgres_port}/{postgres_database}",
        pool_pre_ping=True,
    )
    event.listen(engine, "do_connect", provide_token)
    return engine


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Return a cached SQLAlchemy engine for the Lakebase Postgres instance."""
    return _build_engine()


def get_version() -> Optional[str]:
    """Test Postgres connectivity by returning the database version string and user for PG connect."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("select version();"))
        ver = result.scalar_one_or_none().split(" ")[:2] 
        return ' '.join(ver) + " " + os.getenv("PGUSER")
