import json
import textwrap
import time
from pathlib import Path

import pytest

from micro_workflow_manager import cli


DURATION_SECONDS = 5.0
INITIAL_A_JOBS = 30


def _node_status(tmp_path: Path, node: str) -> str:
    return json.loads((tmp_path / "node" / node / "node_state.json").read_text(encoding="utf-8"))["status"]


def _job_count(tmp_path: Path, node: str) -> int:
    jobs = tmp_path / "node" / node / "jobs"
    if not jobs.exists():
        return 0
    return len([path for path in jobs.iterdir() if path.is_dir() and path.name.isdigit()])


def _write_node(path: Path, body: str):
    path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")


def _write_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    _write_node(
        tmp_path / "src" / "graph.py",
        """
        EDGES = [
            ("A", "B"), ("A", "C"), ("A", "D"),
            ("B", "A"), ("B", "C"),
            ("C", "A"),
            ("D", "A"),
        ]
        """,
    )

    _write_node(
        behavior / "A.py",
        f"""
        import time
        from micro_workflow_manager import NodeRouter

        DURATION_SECONDS = {DURATION_SECONDS!r}

        router = NodeRouter("A", max_threads=16)
        router.create_job(number={INITIAL_A_JOBS}, params={{"hop": 0, "deadline": 0.0, "root": 0}})

        def still_open(deadline):
            return time.monotonic() < deadline

        def next_deadline(deadline):
            if deadline <= 0:
                return time.monotonic() + DURATION_SECONDS
            return deadline

        def choose_target(job_id, hop):
            # Deterministic weighted transition table for A:
            # B = 2/4, C = 1/4, D = 1/4. Probabilities add to 1.
            slot = (job_id + hop * 17) % 4
            if slot < 2:
                return "B"
            if slot == 2:
                return "C"
            return "D"

        @router.task
        def run(ctx, hop, deadline, root):
            deadline = next_deadline(deadline)
            root = root or ctx.job_id
            ctx.write(f"A_{{ctx.job_id}}.txt", f"hop={{hop}} root={{root}}")
            if still_open(deadline):
                target = choose_target(ctx.job_id, hop)
                ctx.node(target).add(autostart=True, hop=hop + 1, deadline=deadline, root=root)
            return hop
        """,
    )

    _write_node(
        behavior / "B.py",
        """
        import time
        from micro_workflow_manager import NodeRouter

        router = NodeRouter("B", max_threads=16)

        def still_open(deadline):
            return time.monotonic() < deadline

        def choose_target(job_id, hop):
            # Deterministic weighted transition table for B:
            # A = 7/10, C = 3/10. Probabilities add to 1.
            slot = (job_id * 3 + hop * 11) % 10
            if slot < 7:
                return "A"
            return "C"

        @router.task
        def run(ctx, hop, deadline, root):
            ctx.write(f"B_{ctx.job_id}.txt", f"hop={hop} root={root}")
            if still_open(deadline):
                target = choose_target(ctx.job_id, hop)
                ctx.node(target).add(autostart=True, hop=hop + 1, deadline=deadline, root=root)
            return hop
        """,
    )

    _write_node(
        behavior / "C.py",
        """
        import time
        from micro_workflow_manager import NodeRouter

        router = NodeRouter("C", max_threads=16)

        def still_open(deadline):
            return time.monotonic() < deadline

        @router.task
        def run(ctx, hop, deadline, root):
            # Deterministic transition table for C: A = 1.0.
            ctx.write(f"C_{ctx.job_id}.txt", f"hop={hop} root={root}")
            if still_open(deadline):
                ctx.node("A").add(autostart=True, hop=hop + 1, deadline=deadline, root=root)
            return hop
        """,
    )

    _write_node(
        behavior / "D.py",
        """
        import time
        from micro_workflow_manager import NodeRouter

        router = NodeRouter("D", max_threads=16)

        def still_open(deadline):
            return time.monotonic() < deadline

        @router.task
        def run(ctx, hop, deadline, root):
            # Deterministic transition table for D: A = 1.0.
            ctx.write(f"D_{ctx.job_id}.txt", f"hop={hop} root={root}")
            if still_open(deadline):
                ctx.node("A").add(autostart=True, hop=hop + 1, deadline=deadline, root=root)
            return hop
        """,
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0


@pytest.mark.stress
def test_threaded_deterministic_markov_chain_cycle_stresses_filesystem_for_five_seconds(tmp_path, monkeypatch, capsys):
    _write_project(tmp_path, monkeypatch)
    capsys.readouterr()

    started = time.monotonic()
    assert cli.main(["runfrom", "A", "--runner", "threaded"]) == 0
    elapsed = time.monotonic() - started

    for node in ["A", "B", "C", "D"]:
        assert _node_status(tmp_path, node) == "done"

    counts = {node: _job_count(tmp_path, node) for node in ["A", "B", "C", "D"]}
    total_jobs = sum(counts.values())

    # The root schedule starts with exactly 30 A jobs, then each completed job
    # deterministically creates one next job until its chain-local deadline
    # closes.  The lower bound proves that the test did much more than just run
    # the initial seeds; the elapsed bound catches premature SCC quiescence
    # without making the test depend on machine-specific filesystem throughput.
    assert counts["A"] >= INITIAL_A_JOBS
    assert all(counts[node] > 0 for node in ["B", "C", "D"])
    assert total_jobs >= INITIAL_A_JOBS * 3
    assert elapsed >= DURATION_SECONDS * 0.80
