import sqlite3
from contextlib import closing

from micro_workflow_manager import (
    InputFileSystem,
    NodeInputFileSystem,
    NodeRouter,
    OutputFileSystem,
)

from src.utils.provenance import record_provenance

router = NodeRouter("verify_database")
INPUT = InputFileSystem("database under test")
OUTPUT = OutputFileSystem("verification reports")
NEXT = NodeInputFileSystem("export_schema_report")


@router.task
def verify_database(ctx, table):
    database_path = INPUT.file(ctx, "database.sqlite").path
    with closing(sqlite3.connect(database_path)) as connection:
        count = connection.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        columns = [
            row[1]
            for row in connection.execute(f"PRAGMA table_info({table})")
        ]

    report = {
        "table": table,
        "rows": count,
        "columns": columns,
        "valid": count == 1 and columns == ["id", "name"],
    }
    OUTPUT.file(ctx, "verification.json").write_json(report, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="verify",
        inputs={"table": table},
        decisions={"checks": ["row count", "column order"]},
        result=report,
    )
    if not report["valid"]:
        raise ValueError("database verification failed")
    NEXT.add_job(ctx, autostart=True, report=report)
    return report
