from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("generate_options")
OUTPUT=OutputFileSystem("options")
JOIN=NodeInputFileSystem("synthesize_answer")
@router.task
def generate_options(ctx, question):
    result={"options":["blue-green","canary"]}
    OUTPUT.file(ctx,"options.json").write_json(result,overwrite=True)
    JOIN.file(ctx,"options.json").write_json(result,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="options",inputs=question,decisions={"breadth":2},result=result)
    return result
