from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("normalize_sections", max_threads=4)
OUTPUT = OutputFileSystem("normalized sections")
NEXT = NodeInputFileSystem("attach_assets", "asset attachment queue")

@router.task
def normalize_sections(ctx, document_id, title):
    normalized = " ".join(title.split()).lower().replace(" ", "-")
    row = {"document_id": document_id, "slug": normalized}
    OUTPUT.file(ctx, f"documents/{document_id}.json").write_json(row, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="normalized", inputs={"title": title},
                      decisions={"whitespace": "collapsed", "case": "lower"}, result=row)
    NEXT.add_job(ctx, autostart=True, **row)
    return row
