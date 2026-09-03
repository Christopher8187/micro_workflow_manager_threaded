from micro_workflow_manager import NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("fan_out")
router.create_job(params={"question":"How should a small team release a risky migration?"})
OUTPUT=OutputFileSystem("parallel plan")
@router.task
def fan_out(ctx, question):
    branches=["collect_facts","generate_options","check_risks"]
    OUTPUT.file(ctx,"branches.json").write_json(branches,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="fan_out",inputs=question,decisions={"branches":branches},result=branches)
    child_specs=[
        (branch,{"question":question},f"fan-out:{ctx.job_id}:{branch}")
        for branch in branches
    ]
    for branch,params,key in child_specs:
        ctx.node(branch).add(autostart=True,idempotency_key=key,**params)
    return branches
