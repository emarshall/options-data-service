"""
Conflict-tolerant inserts for the live ingestion path.

**Why this exists:** live ingestion and backfill are two independent
writers into the same tables, and they used to disagree about how to
handle a row that already exists. Backfill explicitly checks
(`session.get(...)`, then skips — see `backfill.py`), while the live
pipeline used a bare `session.add()`, which raises `IntegrityError` on a
primary-key collision. Since every bar table is keyed on
`(time, identifier)`, that collision is easy to hit in practice: a
`backfill`/`gap-reconcile` run writing the same minute the live pipeline
is flushing turns the live flush into a failed transaction. Before the
pop-on-write-failure fix in `pipeline.py`, a failed flush also *discarded*
the bars it had already popped out of memory, so the live path could lose
a minute of data outright because a backfill happened to overlap it.

`insert_ignore` makes the live path idempotent instead: a row that is
already present is silently skipped, exactly matching what backfill
already does. Re-running a flush, or overlapping with a backfill, then
becomes harmless rather than a failure.

**Dialect note:** SQLAlchemy has no backend-agnostic upsert, so this
dispatches to the Postgres or SQLite `insert()` variant. Both dialects
are supported because this project's test suite runs against in-memory
SQLite while production runs against Postgres — a Postgres-only
implementation would make the conflict handling itself untested. Any
other dialect falls back to plain adds, which is correct but not
conflict-tolerant.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

log = logging.getLogger(__name__)


async def insert_ignore(session, table: Table, rows: Sequence[dict[str, Any]]) -> None:
    """Adds rows to the session, skipping any that violate a uniqueness
    constraint. Caller is responsible for committing.

    Async because it executes against an `AsyncSession` — `execute` there
    returns a coroutine, so a sync wrapper would silently insert nothing
    (the coroutine is simply never awaited) rather than fail loudly.
    """
    if not rows:
        return

    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        await session.execute(pg_insert(table).on_conflict_do_nothing().values(list(rows)))
        return
    if dialect == "sqlite":
        await session.execute(sqlite_insert(table).on_conflict_do_nothing().values(list(rows)))
        return

    log.debug(
        "No conflict-tolerant insert for dialect %r — falling back to plain inserts, "
        "which will raise IntegrityError on a duplicate row.", dialect,
    )
    for row in rows:
        await session.execute(table.insert().values(row))
