from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("generate_candidate")
router.create_job(params={"goal":"Describe idempotency clearly"})
OUTPUT=OutputFileSystem("candidate drafts")
NEXT=NodeInputFileSystem("evaluate_candidate")
@router.task
def generate_candidate(ctx,goal):
    candidate="Idempotency avoids duplicates."
    OUTPUT.file(ctx,"candidate.txt").write_text(candidate,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="candidate",inputs=goal,decisions={"model":"demo generator"},result=candidate)
    NEXT.add_job(ctx,autostart=True,goal=goal,candidate=candidate)
    return candidate
