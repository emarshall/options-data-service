"""
Alembic migration environment.

Runtime code (service/db/session.py) uses an async engine (asyncpg driver)
since the ingestion pipeline and API are asyncio-based. Alembic itself runs
synchronously, so this file adapts DATABASE_URL from `+asyncpg` to a plain
sync-compatible driver (`psycopg`) rather than requiring a second,
separately-maintained connection string.
"""

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from service.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_sync_database_url() -> str:
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql+asyncpg://options_user:options_pass@localhost:5432/options_data",
    )
    # Alembic/psycopg need a sync driver; runtime code uses the async one.
    return url.replace("postgresql+asyncpg://", "postgresql+psycopg://")


def run_migrations_offline() -> None:
    url = get_sync_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Connects and runs migrations, with retries.

    Postgres (and TimescaleDB's image, built on it) goes through a two-phase
    startup on first initialization: initial setup, then a full restart to
    apply config before it's actually ready for connections. `pg_isready`
    (what docker-compose's healthcheck uses) can report success in the brief
    gap between those phases, so this container can start right as Postgres
    is mid-restart. Retrying here is more robust than trying to tune
    healthcheck timing to close that gap.
    """
    import time

    from sqlalchemy.exc import OperationalError

    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_sync_database_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    max_attempts = 15
    delay_s = 2.0
    connection = None
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            connection = connectable.connect()
            break
        except OperationalError as e:
            last_error = e
            print(
                f"[alembic] Database not ready yet (attempt {attempt}/{max_attempts}): "
                f"{e}. Retrying in {delay_s}s..."
            )
            time.sleep(delay_s)
    if connection is None:
        raise SystemExit(
            f"Could not connect to database after {max_attempts} attempts. Last error: {last_error}"
        )

    with connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
