from micro_workflow_manager import InputFileSystem, NodeRouter, OutputFileSystem


router = NodeRouter("review")

INPUT = InputFileSystem("review input")
OUTPUT = OutputFileSystem("review output", base="{batch}")


@router.task
def review(ctx, batch, result_file):
    result = INPUT.file(ctx, result_file).read_json()
    OUTPUT.file(ctx, "reviewed.json", batch=batch).write_json(result)
    return result
