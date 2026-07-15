from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("plan_schema_change")
router.create_job(params={"table":"items"})
OUTPUT=OutputFileSystem("migration plans")
NEXT=NodeInputFileSystem("apply_schema_change")
@router.task
def plan_schema_change(ctx,table):
    sql=f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
    OUTPUT.file(ctx,"migration.sql").write_text(sql,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="plan",inputs={"table":table},decisions={"dialect":"sqlite","destructive":False},result=sql)
    NEXT.add_job(ctx,autostart=True,table=table,sql=sql)
    return sql
