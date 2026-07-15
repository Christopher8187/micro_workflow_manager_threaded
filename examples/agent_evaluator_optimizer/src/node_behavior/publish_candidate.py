from micro_workflow_manager import NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("publish_candidate")
OUTPUT=OutputFileSystem("accepted candidates")
@router.task
def publish_candidate(ctx,candidate,evaluation):
    if not evaluation["accepted"]: raise ValueError("candidate not accepted")
    OUTPUT.file(ctx,"answer.txt").write_text(candidate,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="published",inputs={"candidate":candidate,"evaluation":evaluation},decisions={"publish":"accepted only"},result=candidate)
    return candidate
