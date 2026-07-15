from micro_workflow_manager import InputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("synthesize_answer")
router.create_job()
INPUT=InputFileSystem("parallel branch results")
OUTPUT=OutputFileSystem("synthesized answer")
@router.task
def synthesize_answer(ctx):
    facts=INPUT.file(ctx,"facts.json").read_json()
    options=INPUT.file(ctx,"options.json").read_json()
    risks=INPUT.file(ctx,"risks.json").read_json()
    result={"recommendation":"canary with verified rollback",**facts,**options,**risks}
    OUTPUT.file(ctx,"answer.json").write_json(result,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="synthesis",inputs={"facts":facts,"options":options,"risks":risks},decisions={"priority":"rollback safety"},result=result)
    return result
