from __future__ import annotations

import inspect
import textwrap
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, cli
from micro_workflow_manager.cli.parser import build_parser
from micro_workflow_manager.cli.trace import trace_command
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage


def _write_project(
    tmp_path: Path,
    monkeypatch,
    *,
    edges: str,
    behaviors: dict[str, str],
    runner: str = "direct",
) -> None:
    monkeypatch.chdir(tmp_path)
    behavior_dir = tmp_path / "src" / "node_behavior"
    behavior_dir.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        textwrap.dedent(edges).strip() + "\n",
        encoding="utf-8",
    )
    for node, source in behaviors.items():
        (behavior_dir / f"{node}.py").write_text(
            textwrap.dedent(source).strip() + "\n",
            encoding="utf-8",
        )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", runner]) == 0


def _count(path: Path) -> int:
    return int(path.read_text(encoding="utf-8"))


def _trace_events(storage: FileStorage, node: str, job_id: int):
    return [
        event
        for event in storage.read_job_events(node, job_id)
        if event.get("event") == "trace"
    ]


def test_runfrom_refuseafter_freshens_full_scope_but_stops_new_admission(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'C')]",
        runner="threaded",
        behaviors={
            "A": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="threaded", max_threads=4)
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    path = Path(ctx.system.storage.project_dir) / "a-count.txt"
                    count = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(count), encoding="utf-8")
                    ctx.node("B").add(value=count)
                    return count
            """,
            "B": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="threaded", max_threads=4)
                @router.task
                def run(ctx, value):
                    path = Path(ctx.system.storage.project_dir) / "b-count.txt"
                    count = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(count), encoding="utf-8")
                    ctx.node("C").add(value=value)
                    return count
            """,
            "C": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C", runner="threaded", max_threads=4)
                @router.task
                def run(ctx, value):
                    path = Path(ctx.system.storage.project_dir) / "c-count.txt"
                    count = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(count), encoding="utf-8")
                    return count
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A"]) == 0
    capsys.readouterr()
    assert _count(tmp_path / "a-count.txt") == 1
    assert _count(tmp_path / "b-count.txt") == 1
    assert _count(tmp_path / "c-count.txt") == 1

    assert cli.main(["runfrom", "A", "refuseafter", "B"]) == 0
    output = capsys.readouterr().out
    assert "Refused further Hoeflein-component admission after {B} terminated." in output
    assert _count(tmp_path / "a-count.txt") == 2
    assert _count(tmp_path / "b-count.txt") == 2
    assert _count(tmp_path / "c-count.txt") == 1

    storage = FileStorage(tmp_path)
    assert storage.list_job_ids("C") == [1]
    assert storage.get_job_status("C", 1) == "queued"
    assert not (tmp_path / "node" / "C" / "jobs" / "1" / "output.json").exists()


def test_refuseafter_does_not_admit_work_created_by_an_already_running_branch(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('A', 'X'), ('X', 'Y')]",
        runner="threaded",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="threaded", max_threads=4)
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.node("B").add()
                    ctx.node("X").add()
            """,
            "B": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="threaded", max_threads=4)
                @router.task
                def run(ctx): return "boundary complete"
            """,
            "X": """
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("X", runner="threaded", max_threads=4)
                @router.task
                def run(ctx):
                    time.sleep(0.15)
                    ctx.node("Y").add()
            """,
            "Y": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("Y", runner="threaded", max_threads=4)
                @router.task
                def run(ctx):
                    (Path(ctx.system.storage.project_dir) / "y-ran.txt").write_text("yes")
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "refuseafter", "B"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("Y", 1) == "queued"
    assert not (tmp_path / "y-ran.txt").exists()


def test_refuseafter_failed_boundary_never_starts_its_descendant(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'C')]",
        runner="threaded",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="threaded", max_threads=4)
                router.create_job(number=1)
                @router.task
                def run(ctx): ctx.node("B").add()
            """,
            "B": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="threaded", max_threads=4)
                @router.task
                def run(ctx):
                    ctx.node("C").add()
                    raise RuntimeError("boundary failed")
            """,
            "C": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("C", runner="threaded", max_threads=4)
                @router.task
                def run(ctx):
                    (Path(ctx.system.storage.project_dir) / "c-ran.txt").write_text("yes")
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "refuseafter", "B"]) == 1
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("C", 1) == "queued"
    assert not (tmp_path / "c-ran.txt").exists()


def test_runfrom_refuseafter_parser_and_plan(tmp_path, monkeypatch, capsys):
    args = build_parser().parse_args(
        ["runfrom", "A", "refuseafter", "B", "--keeptrace"]
    )
    assert args.refuse_mode == "refuseafter"
    assert args.refuse_node == "B"
    assert args.keeptrace is True

    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B')]",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx): return None
            """,
            "B": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                @router.task
                def run(ctx): return None
            """,
        },
    )
    capsys.readouterr()
    assert cli.main(
        ["runfrom", "A", "refuseafter", "B", "--keeptrace", "--plan"]
    ) == 0
    plan = capsys.readouterr().out
    assert "refusal boundary" in plan
    assert "reset scope: unchanged" in plan
    assert "preserve all affected trace journals" in plan


def test_fresh_run_clears_trace_by_default_and_keeptrace_preserves_it(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'sink')]",
        behaviors={
            "A": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    path = Path(ctx.system.storage.project_dir) / "attempt.txt"
                    attempt = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(attempt), encoding="utf-8")
                    ctx.trace("attempt", content=attempt)
                    return attempt
            """,
            "sink": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("sink")
                @router.task
                def run(ctx): return None
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["run", "A"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    assert [event["content"] for event in _trace_events(storage, "A", 1)] == [1]

    assert cli.main(["run", "A", "--keeptrace"]) == 0
    capsys.readouterr()
    assert [event["content"] for event in _trace_events(storage, "A", 1)] == [1, 2]

    assert cli.main(["run", "A"]) == 0
    capsys.readouterr()
    assert [event["content"] for event in _trace_events(storage, "A", 1)] == [3]


def test_default_fresh_run_clears_orphan_trace_left_by_prior_keeptrace(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B')]",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx): ctx.node("B").add()
            """,
            "B": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                @router.task
                def run(ctx):
                    root = Path(ctx.system.storage.project_dir)
                    path = root / "orphan-attempt.txt"
                    attempt = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(attempt), encoding="utf-8")
                    ctx.trace("attempt", content=attempt)
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--keeptrace"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    storage.delete_node_jobs("B", remove_payload=True, preserve_events=True)
    assert not storage.job_exists("B", 1)
    assert [event["content"] for event in _trace_events(storage, "B", 1)] == [1]

    assert cli.main(["runfrom", "A"]) == 0
    capsys.readouterr()
    assert [event["content"] for event in _trace_events(storage, "B", 1)] == [2]


def test_orphan_trace_scan_leaves_live_job_journals_for_batch_cleanup(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'sink')]",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.trace("large live trace", content="x" * 100000)
            """,
            "sink": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("sink")
                @router.task
                def run(ctx): return None
            """,
        },
    )
    capsys.readouterr()
    assert cli.main(["run", "A"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)

    before = storage.read_job_events("A", 1)
    assert any(event.get("name") == "large live trace" for event in before)
    assert storage.clear_job_events_produced_by_components({("A",)}) == 0
    after = storage.read_job_events("A", 1)
    assert after == before
    source = inspect.getsource(storage.clear_job_events_produced_by_components)
    assert "INDEXED BY job_events_job_idx" in source
    assert "WITH orphan_keys" in source


def test_resumefrom_preserves_start_trace_and_clears_descendant_trace_by_default(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B')]",
        behaviors={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.trace("A trace", content="original")
                    ctx.node("B").add()
                    return None
            """,
            "B": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                @router.task
                def run(ctx):
                    root = Path(ctx.system.storage.project_dir)
                    path = root / "b-attempt.txt"
                    attempt = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(attempt), encoding="utf-8")
                    ctx.trace("B trace", content=attempt)
                    if not (root / "allow.flag").exists():
                        raise RuntimeError("blocked once")
                    return attempt
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A"]) == 1
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    assert [event["content"] for event in _trace_events(storage, "A", 1)] == ["original"]
    assert [event["content"] for event in _trace_events(storage, "B", 1)] == [1]

    (tmp_path / "allow.flag").write_text("yes", encoding="utf-8")
    assert cli.main(["resumefrom", "A"]) == 0
    capsys.readouterr()
    assert [event["content"] for event in _trace_events(storage, "A", 1)] == ["original"]
    assert [event["content"] for event in _trace_events(storage, "B", 1)] == [2]


def test_deleted_job_and_copy_paste_preserve_trace_journals(
    tmp_path, monkeypatch, capsys
):
    _write_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'sink')]",
        behaviors={
            "A": """
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    path = Path(ctx.system.storage.project_dir) / "copy-attempt.txt"
                    attempt = int(path.read_text() if path.exists() else "0") + 1
                    path.write_text(str(attempt), encoding="utf-8")
                    ctx.trace("copy trace", content=attempt)
                    return attempt
            """,
            "sink": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("sink")
                @router.task
                def run(ctx): return None
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["run", "A"]) == 0
    capsys.readouterr()
    assert cli.main(["copy", "A"]) == 0
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    storage.delete_node_jobs("A", remove_payload=True, preserve_events=True)
    assert not storage.job_exists("A", 1)
    assert [event["content"] for event in _trace_events(storage, "A", 1)] == [1]

    assert cli.main(["run", "A", "--keeptrace"]) == 0
    capsys.readouterr()
    assert [event["content"] for event in _trace_events(storage, "A", 1)] == [1, 2]

    assert cli.main(["paste", "A"]) == 0
    capsys.readouterr()
    restored = FileStorage(tmp_path)
    assert [event["content"] for event in _trace_events(restored, "A", 1)] == [1]


def test_preserved_recreated_job_records_and_renders_origin_changed(
    tmp_path, capsys
):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    workflow.graph([("P", "T"), ("Q", "T")])
    storage = workflow.storage

    first = Job(
        job_id=1,
        node_name="T",
        params={"value": 1},
        parent={"from_node": "P", "from_job_id": 4},
        producer_component=("P",),
        job_kind="component",
    )
    storage.create_job(first)
    storage.append_job_event("T", 1, "trace", name="old", content="preserved")
    assert storage.delete_job("T", 1, preserve_events=True)

    second = Job(
        job_id=1,
        node_name="T",
        params={"value": 2},
        parent={"from_node": "Q", "from_job_id": 9},
        producer_component=("Q",),
        job_kind="component",
    )
    storage.create_job(second)

    events = storage.read_job_events("T", 1)
    changed = [event for event in events if event.get("event") == "origin_changed"]
    assert len(changed) == 1
    assert changed[0]["previous_parent"] == {"from_node": "P", "from_job_id": 4}
    assert changed[0]["current_parent"] == {"from_node": "Q", "from_job_id": 9}
    assert changed[0]["previous_origin"]["producer_component"] == ["P"]
    assert changed[0]["current_origin"]["producer_component"] == ["Q"]

    assert trace_command(workflow, "T", 1) == 0
    output = capsys.readouterr().out
    assert "ORIGIN CHANGED" in output
    assert "Previous: P job 4" in output
    assert "Current: Q job 9" in output
