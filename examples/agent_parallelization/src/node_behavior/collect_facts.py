from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("collect_facts")
OUTPUT=OutputFileSystem("facts")
JOIN=NodeInputFileSystem("synthesize_answer")
@router.task
def collect_facts(ctx, question):
    result={"facts":["backups exist","maintenance window is available"]}
    OUTPUT.file(ctx,"facts.json").write_json(result,overwrite=True)
    JOIN.file(ctx,"facts.json").write_json(result,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="facts",inputs=question,decisions={"source":"demo inventory"},result=result)
    return result
