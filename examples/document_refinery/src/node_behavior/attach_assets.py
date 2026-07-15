from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("attach_assets", max_threads=4)
OUTPUT = OutputFileSystem("records with assets")
NEXT = NodeInputFileSystem("publish_records", "publication queue")

@router.task
def attach_assets(ctx, document_id, slug):
    record = {"document_id": document_id, "slug": slug, "assets": [f"{slug}.png"]}
    OUTPUT.file(ctx, f"records/{document_id}.json").write_json(record, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="assets", inputs={"slug": slug},
                      decisions={"asset_policy": "one canonical preview"}, result=record)
    NEXT.add_job(ctx, autostart=True, record=record)
    return record
