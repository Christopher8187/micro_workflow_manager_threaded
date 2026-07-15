from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("solve_coordinates")
OUTPUT = OutputFileSystem("coordinate candidates")
NEXT = NodeInputFileSystem("validate_solution")

@router.task(retries=1)
def solve_coordinates(ctx, tokens, seed):
    # Deterministic stand-in for a numerical solver.
    points = {"a": [0.0, 0.0], "b": [4.0, 0.0], "c": [2.0, 0.0]}
    OUTPUT.file(ctx, "candidate.json").write_json(points, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="solve", inputs={"tokens": tokens, "seed": seed},
                      decisions={"coordinate_system": "cartesian", "attempt": ctx.attempt}, result=points)
    NEXT.add_job(ctx, autostart=True, points=points)
    return points
