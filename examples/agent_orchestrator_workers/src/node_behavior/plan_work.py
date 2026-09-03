from micro_workflow_manager import NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("plan_work")
router.create_job(params={"topic":"workflow reliability"})
OUTPUT=OutputFileSystem("work plans")
@router.task
def plan_work(ctx, topic):
    items=["timeouts","idempotency","inspection"]
    OUTPUT.file(ctx,"plan.json").write_json(items,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="plan",inputs=topic,decisions={"decomposition":"three independent sections"},result=items)
    children=[
        ({"index":index,"item":item,"topic":topic},f"plan-work:{ctx.job_id}:{index}")
        for index,item in enumerate(items,1)
    ]
    for params,key in children:
        ctx.node("execute_work_item").add(
            **params,
            autostart=True,
            idempotency_key=key,
        )
    return items
