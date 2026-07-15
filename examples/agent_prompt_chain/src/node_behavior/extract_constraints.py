from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("extract_constraints")
OUTPUT=OutputFileSystem("constraint extraction")
NEXT=NodeInputFileSystem("compose_response")
@router.task
def extract_constraints(ctx, request, draft):
    constraints={"max_sentences":2,"audience":"new engineer"}
    OUTPUT.file(ctx,"constraints.json").write_json(constraints,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="constraints",inputs={"request":request,"draft":draft},decisions={"rule_source":"request text"},result=constraints)
    NEXT.add_job(ctx,autostart=True,draft=draft,constraints=constraints)
    return constraints
