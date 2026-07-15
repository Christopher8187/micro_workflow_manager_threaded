from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("check_risks")
OUTPUT=OutputFileSystem("risk checks")
JOIN=NodeInputFileSystem("synthesize_answer")
@router.task
def check_risks(ctx, question):
    result={"risks":["schema lock","rollback drift"]}
    OUTPUT.file(ctx,"risks.json").write_json(result,overwrite=True)
    JOIN.file(ctx,"risks.json").write_json(result,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="risks",inputs=question,decisions={"checklist":"migration demo"},result=result)
    return result
