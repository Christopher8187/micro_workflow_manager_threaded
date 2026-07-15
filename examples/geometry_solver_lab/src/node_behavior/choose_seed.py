from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("choose_seed")
OUTPUT = OutputFileSystem("seed decisions")
NEXT = NodeInputFileSystem("solve_coordinates")

@router.task
def choose_seed(ctx, tokens):
    seed = sum(map(len, tokens))
    OUTPUT.file(ctx, "seed.json").write_json({"seed": seed}, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="seed", inputs=tokens,
                      decisions={"heuristic": "sum of token lengths"}, result=seed)
    NEXT.add_job(ctx, autostart=True, tokens=tokens, seed=seed)
    return seed
