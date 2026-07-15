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
    with ctx.transaction():
        for branch in branches:
            ctx.node(branch).add(autostart=True,question=question)
    return branches
