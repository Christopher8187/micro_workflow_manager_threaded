from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("discover_sources")
router.create_job(params={"documents": ["Algebra  I", "Geometry   II"]})
OUTPUT = OutputFileSystem("discovered source list")
NEXT = NodeInputFileSystem("normalize_sections", "normalization queue")

@router.task
def discover_sources(ctx, documents):
    rows = [{"document_id": i + 1, "title": title} for i, title in enumerate(documents)]
    OUTPUT.file(ctx, "sources.json").write_json(rows, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="sources", inputs=documents,
                      decisions={"ordering": "input order", "ids": "one-based"}, result=rows)
    for row in rows:
        NEXT.add_job(ctx, autostart=True, **row)
    return {"discovered": len(rows)}
