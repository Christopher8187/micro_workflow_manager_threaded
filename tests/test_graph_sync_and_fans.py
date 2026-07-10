from __future__ import annotations

import json
from pathlib import Path

from micro_workflow_manager import cli


def _write_router(path: Path, node: str):
    path.write_text(
        f'''from micro_workflow_manager import NodeRouter
router = NodeRouter("{node}")
@router.task
def run(ctx):
    return "{node}"
''',
        encoding="utf-8",
    )


def test_graph_changes_require_explicit_update_and_do_not_recreate_renamed_nodes(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    graph = tmp_path / "src" / "graph.py"

    graph.write_text('EDGES = [("old_name", "sink")]\n', encoding="utf-8")
    _write_router(behavior / "old_name.py", "old_name")
    _write_router(behavior / "sink.py", "sink")

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py"]) == 0
    capsys.readouterr()

    old_folder = tmp_path / "node" / "old_name"
    new_folder = tmp_path / "node" / "new_name"
    assert old_folder.is_dir()
    assert not new_folder.exists()

    # Rename the graph node and add the new behavior file, but deliberately
    # leave the obsolete behavior file in place. Loading must not mount it.
    graph.write_text('EDGES = [("new_name", "sink")]\n', encoding="utf-8")
    _write_router(behavior / "new_name.py", "new_name")

    before = (tmp_path / ".mwf").read_bytes()
    assert cli.main(["monitor", "--once"]) == 1
    error = capsys.readouterr().err
    assert "Graph state is out of date" in error
    assert "mwf graph --update" in error
    assert old_folder.is_dir()
    assert not new_folder.exists()
    assert (tmp_path / ".mwf").read_bytes() == before

    assert cli.main(["graph", "--update"]) == 0
    output = capsys.readouterr().out
    assert "Removed stale nodes: old_name" in output
    assert "Added nodes: new_name" in output
    assert not old_folder.exists()
    assert new_folder.is_dir()

    # A later normal load imports old_name.py but filters its router because the
    # node is no longer in the graph. The deleted folder must stay deleted.
    assert cli.main(["monitor", "--once"]) == 0
    capsys.readouterr()
    assert not old_folder.exists()


def test_graph_update_detects_edge_only_changes_without_mutating_early(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    graph = tmp_path / "src" / "graph.py"
    graph.write_text('EDGES = [("a", "b"), ("b", "c")]\n', encoding="utf-8")

    for node in ["a", "b", "c"]:
        _write_router(behavior / f"{node}.py", node)

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py"]) == 0
    capsys.readouterr()

    graph.write_text('EDGES = [("a", "c"), ("b", "c")]\n', encoding="utf-8")
    before = (tmp_path / ".mwf").read_bytes()

    assert cli.main(["monitor", "--once"]) == 1
    error = capsys.readouterr().err
    assert "edges in graph.py differ" in error
    assert (tmp_path / ".mwf").read_bytes() == before

    assert cli.main(["graph", "--update"]) == 0
    capsys.readouterr()
    stored = json.loads((tmp_path / ".mwf").read_text(encoding="utf-8"))
    assert stored["edges"] == [["a", "c"], ["b", "c"]]


def test_graph_supports_a_to_B_and_A_to_b_directed_fans(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        '''A = ["left_1", "left_2"]
B = ["right_1", "right_2"]
EDGES = [
    ("source", B),  # source-B fan-out
    (A, "sink"),    # A-sink fan-in
]
''',
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py"]) == 0
    capsys.readouterr()

    stored = json.loads((tmp_path / ".mwf").read_text(encoding="utf-8"))
    assert stored["edges"] == [
        ["source", "right_1"],
        ["source", "right_2"],
        ["left_1", "sink"],
        ["left_2", "sink"],
    ]
    assert {path.name for path in (tmp_path / "node").iterdir() if path.is_dir()} == {
        "source",
        "right_1",
        "right_2",
        "left_1",
        "left_2",
        "sink",
    }


def test_fan_helper_is_supported_in_graph_files(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "src" / "node_behavior").mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        '''from micro_workflow_manager import fan
EDGES = [
    fan("root", ["a", "b"]),
    fan(["a", "b"], "end"),
]
''',
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py"]) == 0
    capsys.readouterr()

    stored = json.loads((tmp_path / ".mwf").read_text(encoding="utf-8"))
    assert stored["edges"] == [
        ["root", "a"],
        ["root", "b"],
        ["a", "end"],
        ["b", "end"],
    ]
