"""Regression state uses real isolated PostgreSQL schemas; external HTTP is blocked."""
from pg_state_plugin import isolated_postgres_state  # noqa: F401 (pytest fixture registration)
