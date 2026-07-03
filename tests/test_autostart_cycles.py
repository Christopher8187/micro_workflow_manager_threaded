
import json
import textwrap
from pathlib import Path

from micro_workflow_manager import cli


def make_cycle_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        """
EDGES = [
    ("A", "A"),
    ("A", "B"),
    ("A", "C"),
    ("B", "A"),
    ("C", "A"),
    ("A", "D"),
]
""".strip(),
        encoding="utf-8",
    )
    (behavior / "A.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("A")
            router.create_job(params={"kind": "seed", "depth": 0, "trace": "seed"})

            @router.task
            def run(ctx, kind, depth, trace):
                ctx.write(f"A_{ctx.job_id}.txt", trace)
                ctx.node("D").add(from_a_job=ctx.job_id, kind=kind, depth=depth, trace=trace)
                if kind == "seed":
                    ctx.node("A").add(autostart=True, kind="self", depth=depth + 1, trace=f"{trace}->A")
                    ctx.node("B").add(autostart=True, depth=depth + 1, trace=f"{trace}->B")
                    ctx.node("C").add(autostart=True, depth=depth + 1, trace=f"{trace}->C")
                return trace
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("B")

            @router.task
            def run(ctx, depth, trace):
                ctx.write(f"B_{ctx.job_id}.txt", trace)
                ctx.node("A").add(autostart=True, kind="from_B", depth=depth + 1, trace=f"{trace}->A")
                return trace
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "C.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("C")

            @router.task
            def run(ctx, depth, trace):
                ctx.write(f"C_{ctx.job_id}.txt", trace)
                ctx.node("A").add(autostart=True, kind="from_C", depth=depth + 1, trace=f"{trace}->A")
                return trace
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "D.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("D")

            @router.task
            def run(ctx, from_a_job, kind, depth, trace):
                ctx.write(f"D_{ctx.job_id}.txt", trace)
                return trace
            """
        ).strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def node_status(tmp_path: Path, node: str) -> str:
    return json.loads((tmp_path / "node" / node / "node_state.json").read_text())["status"]


def test_runfrom_supports_self_and_mutual_autostart_cycles_before_downstream(tmp_path, monkeypatch, capsys):
    make_cycle_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--runner", "direct"]) == 0
    out = capsys.readouterr().out

    assert "Ran:" in out
    assert "  A" in out
    assert "  B" in out
    assert "  C" in out
    assert "  D" in out

    assert node_status(tmp_path, "A") == "done"
    assert node_status(tmp_path, "B") == "done"
    assert node_status(tmp_path, "C") == "done"
    assert node_status(tmp_path, "D") == "done"

    assert sorted(path.name for path in (tmp_path / "node" / "A" / "jobs").iterdir()) == ["1", "2", "3", "4"]
    assert sorted(path.name for path in (tmp_path / "node" / "B" / "jobs").iterdir()) == ["1"]
    assert sorted(path.name for path in (tmp_path / "node" / "C" / "jobs").iterdir()) == ["1"]
    assert sorted(path.name for path in (tmp_path / "node" / "D" / "jobs").iterdir()) == ["1", "2", "3", "4"]


def _job_count(tmp_path: Path, node: str) -> int:
    jobs = tmp_path / "node" / node / "jobs"
    if not jobs.exists():
        return 0
    return len([path for path in jobs.iterdir() if path.is_dir() and path.name.isdigit()])


def _make_project(tmp_path: Path, monkeypatch, edges: str, files: dict[str, str], runner: str = "threaded"):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(edges.strip() + "\n", encoding="utf-8")
    for name, content in files.items():
        (behavior / f"{name}.py").write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", runner]) == 0


def test_threaded_diamond_cycle_spawns_100_seed_jobs_without_deadlock(tmp_path, monkeypatch, capsys):
    _make_project(
        tmp_path,
        monkeypatch,
        """
        EDGES = [("A", "B"), ("A", "C"), ("B", "A"), ("C", "A")]
        """,
        {
            "A": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("A", max_threads=8)
                router.create_job(number=100, params={"depth": 0})

                @router.task
                def run(ctx, depth):
                    rng = random.Random(f"A-{ctx.job_id}-{depth}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"A_{ctx.job_id}.txt", str(depth))
                    if depth == 0:
                        ctx.node("B").add(autostart=True, depth=1)
                        ctx.node("C").add(autostart=True, depth=1)
                    return depth
            """,
            "B": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("B", max_threads=8)

                @router.task
                def run(ctx, depth):
                    rng = random.Random(f"B-{ctx.job_id}-{depth}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"B_{ctx.job_id}.txt", str(depth))
                    if depth == 1:
                        ctx.node("A").add(autostart=True, depth=2)
                    return depth
            """,
            "C": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("C", max_threads=8)

                @router.task
                def run(ctx, depth):
                    rng = random.Random(f"C-{ctx.job_id}-{depth}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"C_{ctx.job_id}.txt", str(depth))
                    if depth == 1:
                        ctx.node("A").add(autostart=True, depth=2)
                    return depth
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--runner", "threaded"]) == 0

    assert node_status(tmp_path, "A") == "done"
    assert node_status(tmp_path, "B") == "done"
    assert node_status(tmp_path, "C") == "done"
    assert _job_count(tmp_path, "A") == 300
    assert _job_count(tmp_path, "B") == 100
    assert _job_count(tmp_path, "C") == 100


def test_threaded_ring_cycle_spawns_100_seed_jobs_without_deadlock(tmp_path, monkeypatch, capsys):
    _make_project(
        tmp_path,
        monkeypatch,
        """
        EDGES = [("A", "B"), ("B", "C"), ("C", "A")]
        """,
        {
            "A": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("A", max_threads=8)
                router.create_job(number=100, params={"depth": 0})

                @router.task
                def run(ctx, depth):
                    rng = random.Random(f"A-{ctx.job_id}-{depth}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"A_{ctx.job_id}.txt", str(depth))
                    if depth == 0:
                        ctx.node("B").add(autostart=True, depth=1)
                    return depth
            """,
            "B": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("B", max_threads=8)

                @router.task
                def run(ctx, depth):
                    rng = random.Random(f"B-{ctx.job_id}-{depth}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"B_{ctx.job_id}.txt", str(depth))
                    if depth == 1:
                        ctx.node("C").add(autostart=True, depth=2)
                    return depth
            """,
            "C": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("C", max_threads=8)

                @router.task
                def run(ctx, depth):
                    rng = random.Random(f"C-{ctx.job_id}-{depth}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"C_{ctx.job_id}.txt", str(depth))
                    if depth == 2:
                        ctx.node("A").add(autostart=True, depth=3)
                    return depth
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--runner", "threaded"]) == 0

    assert node_status(tmp_path, "A") == "done"
    assert node_status(tmp_path, "B") == "done"
    assert node_status(tmp_path, "C") == "done"
    assert _job_count(tmp_path, "A") == 200
    assert _job_count(tmp_path, "B") == 100
    assert _job_count(tmp_path, "C") == 100


def test_threaded_stochastic_game_engine_spawn_cycle_finishes(tmp_path, monkeypatch, capsys):
    _make_project(
        tmp_path,
        monkeypatch,
        """
        EDGES = [
            ("A", "B"), ("A", "C"), ("A", "D"),
            ("B", "A"), ("C", "A"), ("D", "A"),
        ]
        """,
        {
            "A": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("A", max_threads=8)
                router.create_job(number=80, params={"depth": 0, "seed": "root"})

                @router.task
                def run(ctx, depth, seed):
                    rng = random.Random(f"A-{ctx.job_id}-{depth}-{seed}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"A_{ctx.job_id}.txt", f"{depth}:{seed}")
                    if depth < 3:
                        for target in ["B", "C", "D"]:
                            if rng.random() < 0.10:
                                ctx.node(target).add(autostart=True, depth=depth + 1, seed=f"{seed}->{target}")
                    return depth
            """,
            "B": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("B", max_threads=8)

                @router.task
                def run(ctx, depth, seed):
                    rng = random.Random(f"B-{ctx.job_id}-{depth}-{seed}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"B_{ctx.job_id}.txt", f"{depth}:{seed}")
                    if depth < 3 and rng.random() < 0.20:
                        ctx.node("A").add(autostart=True, depth=depth + 1, seed=f"{seed}->A")
                    return depth
            """,
            "C": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("C", max_threads=8)

                @router.task
                def run(ctx, depth, seed):
                    rng = random.Random(f"C-{ctx.job_id}-{depth}-{seed}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"C_{ctx.job_id}.txt", f"{depth}:{seed}")
                    if depth < 3 and rng.random() < 0.20:
                        ctx.node("A").add(autostart=True, depth=depth + 1, seed=f"{seed}->A")
                    return depth
            """,
            "D": """
                import random
                import time
                from micro_workflow_manager import NodeRouter

                router = NodeRouter("D", max_threads=8)

                @router.task
                def run(ctx, depth, seed):
                    rng = random.Random(f"D-{ctx.job_id}-{depth}-{seed}")
                    time.sleep(rng.random() * 0.002)
                    ctx.write(f"D_{ctx.job_id}.txt", f"{depth}:{seed}")
                    if depth < 3 and rng.random() < 0.20:
                        ctx.node("A").add(autostart=True, depth=depth + 1, seed=f"{seed}->A")
                    return depth
            """,
        },
    )
    capsys.readouterr()

    assert cli.main(["runfrom", "A", "--runner", "threaded"]) == 0

    for node in ["A", "B", "C", "D"]:
        assert node_status(tmp_path, node) == "done"
    total_jobs = sum(_job_count(tmp_path, node) for node in ["A", "B", "C", "D"])
    assert total_jobs >= 80
    assert total_jobs < 400
