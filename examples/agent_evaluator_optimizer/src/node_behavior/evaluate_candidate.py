from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("evaluate_candidate")
OUTPUT=OutputFileSystem("first evaluations")
NEXT=NodeInputFileSystem("improve_candidate")
@router.task
def evaluate_candidate(ctx,goal,candidate):
    evaluation={"score":0.6,"feedback":"Explain retry behavior."}
    OUTPUT.file(ctx,"evaluation.json").write_json(evaluation,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="evaluation",inputs={"goal":goal,"candidate":candidate},decisions={"rubric":["clarity","retry semantics"]},result=evaluation)
    NEXT.add_job(ctx,autostart=True,goal=goal,candidate=candidate,evaluation=evaluation)
    return evaluation
