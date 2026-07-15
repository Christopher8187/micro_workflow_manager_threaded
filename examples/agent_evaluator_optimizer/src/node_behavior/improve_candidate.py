from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("improve_candidate")
OUTPUT=OutputFileSystem("revised candidates")
NEXT=NodeInputFileSystem("final_evaluation")
@router.task
def improve_candidate(ctx,goal,candidate,evaluation):
    revised=candidate+" A retried operation reuses the same key and returns the existing job."
    OUTPUT.file(ctx,"revised.txt").write_text(revised,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="revision",inputs={"candidate":candidate,"evaluation":evaluation},decisions={"applied_feedback":evaluation["feedback"]},result=revised)
    NEXT.add_job(ctx,autostart=True,goal=goal,candidate=revised)
    return revised
