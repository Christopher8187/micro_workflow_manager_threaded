from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("seed_request")
router.create_job(params={"request_id": "demo-1", "question": "Plan a safe schema migration."})
OUTPUT = OutputFileSystem("seed requests")
NEXT = NodeInputFileSystem("plan_request")


@router.task
def seed_request(ctx, request_id: str, question: str):
    request = {"request_id": request_id, "question": question}
    OUTPUT.file(ctx, "request.json").write_json(request, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="request",
        inputs=request,
        decisions={"source": "starter job"},
        result=request,
    )
    NEXT.add_job(
        ctx,
        autostart=True,
        idempotency_key=f"plan:{request_id}",
        **request,
    )
    return request
