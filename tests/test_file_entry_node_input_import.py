from pathlib import Path

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.files import NodeInputFileSystem, OutputFileSystem


def test_filesystem_entry_mkdir_binds_node_input_type_without_name_error(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])
    output = OutputFileSystem("output")
    downstream = NodeInputFileSystem("B", "B input")

    @workflow.task("A")
    def run(ctx):
        output.directory(ctx, "local").mkdir()
        downstream.directory(ctx, "routed").mkdir()
        with downstream.file(ctx, "routed", "value.txt").open("w") as handle:
            handle.write("ok")
        return "done"

    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == "done"
    assert (tmp_path / "node" / "A" / "output" / "local").is_dir()
    assert (tmp_path / "node" / "B" / "input" / "routed" / "value.txt").read_text() == "ok"
