from micro_workflow_manager import NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("plan_request")
OUTPUT = OutputFileSystem("request plans")


@router.task
def plan_request(ctx, request_id: str, question: str):
    branches = ["research_worker", "risk_worker"]
    plan = {"request_id": request_id, "question": question, "branches": branches}
    OUTPUT.file(ctx, "plan.json").write_json(plan, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="plan",
        inputs={"question": question},
        decisions={"fan_out": branches},
        result=plan,
    )
    child_specs = [
        {
            "node": branch,
            "params": {"request_id": request_id, "question": question},
            "idempotency_key": f"{request_id}:{branch}",
        }
        for branch in branches
    ]
    for child in child_specs:
        ctx.node(child["node"]).add(
            autostart=True,
            idempotency_key=child["idempotency_key"],
            **child["params"],
        )
    return plan
