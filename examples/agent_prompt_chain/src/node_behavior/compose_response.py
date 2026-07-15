from micro_workflow_manager import NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("compose_response")
OUTPUT=OutputFileSystem("final responses")
@router.task
def compose_response(ctx, draft, constraints):
    result=draft
    OUTPUT.file(ctx,"response.txt").write_text(result,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="response",inputs={"draft":draft,"constraints":constraints},decisions={"constraint_check":"two sentences"},result=result)
    return result
