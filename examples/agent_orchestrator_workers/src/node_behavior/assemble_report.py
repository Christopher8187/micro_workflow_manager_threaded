from micro_workflow_manager import InputFileSystem, NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("assemble_report")
router.create_job()

INPUT = InputFileSystem("worker sections")
OUTPUT = OutputFileSystem("assembled reports")


@router.task
def assemble_report(ctx):
    sections = [entry.read_json() for entry in INPUT.files(ctx, "sections/*.json")]
    sections.sort(key=lambda row: row["index"])
    report = "\n\n".join(
        f"## {row['heading']}\n{row['text']}" for row in sections
    )
    OUTPUT.file(ctx, "report.md").write_text(report, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="assembly",
        inputs=sections,
        decisions={"ordering": "worker index"},
        result=report,
    )
    return report
