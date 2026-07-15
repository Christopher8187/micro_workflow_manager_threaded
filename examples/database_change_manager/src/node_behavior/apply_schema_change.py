import sqlite3
from contextlib import closing

from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("apply_schema_change")
OUTPUT = OutputFileSystem("database artifacts")
NEXT = NodeInputFileSystem("verify_database")


@router.task
def apply_schema_change(ctx, table, sql):
    database = OUTPUT.file(ctx, "database.sqlite")
    with closing(sqlite3.connect(database.path)) as connection:
        with connection:
            connection.execute(sql)
            connection.execute(
                f"INSERT INTO {table}(name) VALUES (?)",
                ("demo",),
            )

    NEXT.file(ctx, "database.sqlite").copy_from(database, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="apply",
        inputs={"table": table, "sql": sql},
        decisions={"transaction": "sqlite context manager"},
        result={"database": database.relative_path},
    )
    NEXT.add_job(ctx, autostart=True, table=table)
    return database.relative_path
