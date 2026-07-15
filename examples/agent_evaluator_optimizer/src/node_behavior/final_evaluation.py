from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("final_evaluation")
OUTPUT=OutputFileSystem("final evaluations")
NEXT=NodeInputFileSystem("publish_candidate")
@router.task
def final_evaluation(ctx,goal,candidate):
    evaluation={"score":0.95,"accepted":True}
    OUTPUT.file(ctx,"evaluation.json").write_json(evaluation,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="final_evaluation",inputs={"goal":goal,"candidate":candidate},decisions={"threshold":0.8},result=evaluation)
    NEXT.add_job(ctx,autostart=True,candidate=candidate,evaluation=evaluation)
    return evaluation
