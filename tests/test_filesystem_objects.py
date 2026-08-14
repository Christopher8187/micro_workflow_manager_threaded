from __future__ import annotations

import json
from pathlib import Path

import pytest

from micro_workflow_manager import (
    FileSystem,
    InputFileSystem,
    JobFileSystem,
    MicroWorkflow,
    NodeInputFileSystem,
    OutputFileSystem,
)


def test_filesystem_objects_read_write_copy_and_route(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("prepare", "review")])

    source_input = InputFileSystem("source input")
    prepared_output = OutputFileSystem("prepared output", base="{batch}")
    job_files = JobFileSystem("job details")
    review_input = NodeInputFileSystem("review", "review input", base="{batch}")

    (tmp_path / "node" / "prepare" / "input" / "number.txt").write_text("4")

    @workflow.task("prepare")
    def prepare(ctx):
        number = int(source_input.file(ctx, "number.txt").read_text())
        result = prepared_output.file(ctx, "result.json", batch="one")
        result.write_json({"number": number + 1})
        job_files.file(ctx, "note.txt").write_text("prepared")
        review_input.file(ctx, "result.json", batch="one").copy_from(
            result,
            overwrite=True,
        )
        review_input.add_job(ctx, result_file="one/result.json")
        return result.relative_path

    @workflow.task("review")
    def review(ctx, result_file):
        return InputFileSystem().file(ctx, result_file).read_json()

    workflow.start("prepare")
    assert workflow.run_job("prepare", 1, ignore_readiness=True) == "one/result.json"
    assert json.loads(
        (tmp_path / "node" / "review" / "input" / "one" / "result.json").read_text()
    ) == {"number": 5}
    assert (tmp_path / "node" / "prepare" / "jobs" / "1" / "files" / "note.txt").read_text() == "prepared"
    assert workflow.storage.load_job("review", 1).params == {"result_file": "one/result.json"}


def test_filesystem_objects_are_human_readable_and_template_driven(tmp_path: Path):
    filesystem = NodeInputFileSystem(
        "B",
        "B incoming records",
        base="{book}/{section}",
    )
    assert repr(filesystem) == (
        "NodeInputFileSystem(label='B incoming records', node='B', "
        "base='{book}/{section}')"
    )

    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A")
    def run(ctx):
        entry = filesystem.file(
            ctx,
            "record.json",
            book="algebra",
            section="one",
        )
        entry.write_json({"ok": True}, overwrite=True)
        return entry.relative_path

    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == "algebra/one/record.json"


def test_input_filesystem_is_read_only_and_paths_are_safe(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])

    @workflow.task("A")
    def run(ctx):
        with pytest.raises(PermissionError):
            InputFileSystem().file(ctx, "x.txt").write_text("bad")
        with pytest.raises(ValueError):
            OutputFileSystem().file(ctx, "../outside.txt")
        with pytest.raises(ValueError):
            OutputFileSystem(base="{folder}").bind(ctx)
        return "ok"

    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == "ok"


def test_output_entry_append_glob_and_delete_are_generation_guarded(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    output = OutputFileSystem("reports", base="reports")

    @workflow.task("A")
    def run(ctx):
        first = output.file(ctx, "a.txt")
        first.write_text("one")
        first.append_text(" two")
        output.file(ctx, "b.txt").write_text("three")
        names = [entry.name for entry in output.bind(ctx).glob("*.txt")]
        output.file(ctx, "b.txt").delete()
        return {"text": first.read_text(), "names": names}

    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == {
        "text": "one two",
        "names": ["a.txt", "b.txt"],
    }
    assert not (tmp_path / "node" / "A" / "output" / "reports" / "b.txt").exists()


def test_input_entry_bulk_json_read_is_sorted_scoped_and_fenced_once(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    root = tmp_path / "node" / "A" / "input" / "book"
    (root / "s1" / "items").mkdir(parents=True)
    (root / "s1" / "history").mkdir(parents=True)
    (root / "s2" / "items").mkdir(parents=True)
    (root / "s1" / "items" / "b.json").write_text('{"value":2}')
    (root / "s1" / "items" / "a.json").write_text('{"value":1}')
    (root / "s1" / "history" / "old.json").write_text('{"value":0}')
    (root / "s2" / "items" / "c.json").write_text('{"value":3}')
    checks = 0

    @workflow.task("A")
    def run(ctx):
        nonlocal checks
        original = ctx.raise_if_cancelled

        def counted_check():
            nonlocal checks
            checks += 1
            return original()

        ctx.raise_if_cancelled = counted_check
        return InputFileSystem(base="book").bind(ctx).read_jsons(
            "*/items/*.json"
        )

    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == [
        ("s1/items/a.json", {"value": 1}),
        ("s1/items/b.json", {"value": 2}),
        ("s2/items/c.json", {"value": 3}),
    ]
    assert checks == 2


def test_input_entry_bulk_json_read_overlaps_independent_file_waits(
    tmp_path: Path,
    monkeypatch,
):
    import threading
    import time

    import micro_workflow_manager.file_entry as file_entry_module

    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    root = tmp_path / "node" / "A" / "input" / "book"
    root.mkdir(parents=True)
    for index in range(8):
        (root / f"{index}.json").write_text(f'{{"value":{index}}}')

    original = file_entry_module._read_json_path
    lock = threading.Lock()
    active = 0
    peak = 0

    def delayed_read(path, encoding):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(0.02)
            return original(path, encoding)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(file_entry_module, "_read_json_path", delayed_read)

    @workflow.task("A")
    def run(ctx):
        return InputFileSystem(base="book").bind(ctx).read_jsons(
            "*.json",
            recursive=False,
        )

    workflow.start("A")
    result = workflow.run_job("A", 1, ignore_readiness=True)
    assert [relative for relative, _value in result] == [
        f"{index}.json" for index in range(8)
    ]
    assert peak > 1


def test_base_filesystem_is_exported_for_readable_subclasses():
    class ReviewInputFileSystem(NodeInputFileSystem):
        def __init__(self):
            super().__init__("review", "review input")

    assert issubclass(ReviewInputFileSystem, FileSystem)
    assert ReviewInputFileSystem().node_name == "review"


def test_filesystem_files_convenience_encoding_and_declaration(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    output = OutputFileSystem("reports", base="{batch}", encoding="utf-16")

    observed = {}

    @workflow.task("A")
    def run(ctx):
        report = output.file(ctx, "report.txt", batch="one")
        report.write_text("hello", overwrite=False)
        with pytest.raises(FileExistsError):
            report.write_text("again", overwrite=False)
        observed.update(
            text=report.read_text(),
            files=[entry.relative_path for entry in output.files(ctx, "*.txt", batch="one")],
            declaration=output.describe(),
        )
        return "ok"

    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == "ok"
    assert observed["text"] == "hello"
    assert observed["files"] == ["one/report.txt"]
    assert observed["declaration"] == {
        "kind": "OutputFileSystem",
        "label": "reports",
        "scope": "output",
        "node": None,
        "base": "{batch}",
        "encoding": "utf-16",
        "writable": True,
    }


def test_output_and_job_filesystem_copy_from_use_atomic_copy(tmp_path: Path):
    """Regression: 0.5.0 file_systems.py forgot to import _copy_file."""
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    output = OutputFileSystem("copied output")
    job_files = JobFileSystem("copied job files")
    source = tmp_path / "source.bin"
    source.write_bytes(b"post-api-payload")

    @workflow.task("organize")
    def organize(ctx):
        output.file(ctx, "images", "page.jpg").copy_from(source, overwrite=True)
        job_files.file(ctx, "debug", "response.bin").copy_from(source, overwrite=True)
        return "copied"

    workflow.start("organize")
    assert workflow.run_job("organize", 1, ignore_readiness=True) == "copied"
    assert (tmp_path / "node" / "organize" / "output" / "images" / "page.jpg").read_bytes() == b"post-api-payload"
    assert (tmp_path / "node" / "organize" / "jobs" / "1" / "files" / "debug" / "response.bin").read_bytes() == b"post-api-payload"
