from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("draft_brief")
router.create_job(params={"request":"Explain retries in two sentences for a new engineer."})
OUTPUT=OutputFileSystem("brief drafts")
NEXT=NodeInputFileSystem("extract_constraints")
@router.task
def draft_brief(ctx, request):
    draft={"request":request,"draft":"Retries rerun failed work. Limit them and preserve the last error."}
    OUTPUT.file(ctx,"draft.json").write_json(draft,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="draft",inputs=request,decisions={"model":"deterministic-demo"},result=draft)
    NEXT.add_job(ctx,autostart=True,**draft)
    return draft
