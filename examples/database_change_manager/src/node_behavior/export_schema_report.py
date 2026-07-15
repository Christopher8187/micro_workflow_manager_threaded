from micro_workflow_manager import NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("export_schema_report")
OUTPUT=OutputFileSystem("schema reports")
@router.task
def export_schema_report(ctx,report):
    text=f"Table {report['table']}: {report['rows']} row(s), columns={', '.join(report['columns'])}"
    OUTPUT.file(ctx,"schema_report.txt").write_text(text,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="report",inputs=report,decisions={"format":"plain text"},result=text)
    return text
