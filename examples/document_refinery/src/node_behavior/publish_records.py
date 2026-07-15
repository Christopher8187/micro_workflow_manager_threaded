from micro_workflow_manager import NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("publish_records", max_threads=2)
OUTPUT = OutputFileSystem("published records")

@router.task
def publish_records(ctx, record):
    published = {**record, "status": "published"}
    OUTPUT.file(ctx, f"published/{record['document_id']}.json").write_json(published, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="published", inputs=record,
                      decisions={"validation": "required keys present"}, result=published)
    return published
