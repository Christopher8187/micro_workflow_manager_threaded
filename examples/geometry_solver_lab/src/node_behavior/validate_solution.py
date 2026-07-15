from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("validate_solution")
OUTPUT = OutputFileSystem("validation reports")
NEXT = NodeInputFileSystem("format_coordinates")

@router.task
def validate_solution(ctx, points):
    residual = abs(points["c"][0] - (points["a"][0] + points["b"][0]) / 2)
    report = {"residual": residual, "valid": residual < 1e-9}
    OUTPUT.file(ctx, "validation.json").write_json(report, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="validation", inputs=points,
                      decisions={"predicate": "midpoint", "tolerance": 1e-9}, result=report)
    if not report["valid"]:
        raise ValueError("candidate failed independent validation")
    NEXT.add_job(ctx, autostart=True, points=points, report=report)
    return report
