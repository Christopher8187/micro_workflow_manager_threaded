from micro_workflow_manager import NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("format_coordinates")
OUTPUT = OutputFileSystem("formatted coordinate DSL")

@router.task
def format_coordinates(ctx, points, report):
    text = " ".join(f"{name}@{xy[0]}_{xy[1]}" for name, xy in sorted(points.items()))
    OUTPUT.file(ctx, "coordinates.dsl").write_text(text, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="format", inputs={"points": points, "report": report},
                      decisions={"ordering": "point name", "syntax": "name@x_y"}, result=text)
    return text
