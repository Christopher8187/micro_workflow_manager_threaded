from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("execute_work_item",max_threads=3)
OUTPUT=OutputFileSystem("worker sections")
JOIN=NodeInputFileSystem("assemble_report")
@router.task(retries=1)
def execute_work_item(ctx,index,item,topic):
    section={"index":index,"heading":item.title(),"text":f"{item} improves {topic}."}
    OUTPUT.file(ctx,f"sections/{index}.json").write_json(section,overwrite=True)
    JOIN.file(ctx,f"sections/{index}.json").write_json(section,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact=f"worker_{index}",inputs={"item":item,"topic":topic},decisions={"attempt":ctx.attempt},result=section)
    return section
