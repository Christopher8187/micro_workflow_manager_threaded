from micro_workflow_manager import (
    InputFileSystem,
    NodeInputFileSystem,
    NodeRouter,
    OutputFileSystem,
)


router = NodeRouter("add_numbers", max_threads=2)
router.create_job(number=1, params={"batch": "example", "source_file": "numbers.json"})

INPUT = InputFileSystem("number input")
OUTPUT = OutputFileSystem("sum output", base="{batch}")
REVIEW_INPUT = NodeInputFileSystem("review", "review input", base="{batch}")


@router.task(timeout=60)
def add_numbers(ctx, batch, source_file):
    numbers = INPUT.file(ctx, source_file).read_json()

    ctx.checkpoint("numbers loaded", timeout=20, progress=0.25)
    total = sum(numbers)
    ctx.checkpoint("sum calculated", timeout=20, progress=0.75)

    result = OUTPUT.file(ctx, "sum.json", batch=batch)
    result.write_json({"total": total})

    review_copy = REVIEW_INPUT.file(ctx, "sum.json", batch=batch)
    review_copy.copy_from(result, overwrite=True)
    REVIEW_INPUT.add_job(ctx, batch=batch, result_file=review_copy.relative_path)

    return {"total": total}
