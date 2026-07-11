import tempfile

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.models import DONE


def test_router_rejects_bad_thread_and_retry_values():
    with pytest.raises(ValueError):
        NodeRouter("bad", max_threads=0)

    router = NodeRouter("ok")
    with pytest.raises(ValueError):
        router.task(retries=-1)(lambda ctx: None)

    with pytest.raises(ValueError):
        router.task(repeats=0)(lambda ctx: None)


def test_generated_job_records_parent_node_and_job_id():
    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="direct")
        workflow.graph([("A", "B")])

        @workflow.task("A")
        def a(ctx):
            ctx.node("B").add(value=123)

        @workflow.task("B")
        def b(ctx, value):
            return value

        parent = workflow.start("A")
        workflow.run()

        child_rows = workflow.storage.list_jobs("B")
        assert child_rows[0]["parent"] == {
            "from_node": "A",
            "from_job_id": parent.job_id,
        }


def test_autostarted_downstream_job_does_not_complete_node_early():
    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="direct")
        workflow.graph([("A", "B"), ("C", "B")])

        @workflow.task("A")
        def a(ctx):
            ctx.node("B").add(value="from A", autostart=True)

        @workflow.task("C")
        def c(ctx):
            return "done"

        @workflow.task("B")
        def b(ctx, value):
            return value

        workflow.start("A")
        workflow.start("C")

        workflow.run_node("A")
        assert workflow.storage.get_node_status("B") == "queued"
        assert not workflow.node_complete("B")

        workflow.run_node("C")
        workflow.ready_nodes()
        assert workflow.node_complete("B")


def test_cancelled_jobs_do_not_count_as_successful_completion():
    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="direct")
        workflow.graph([("A", "B")])

        @workflow.task("A")
        def a(ctx):
            return "a"

        @workflow.task("B")
        def b(ctx):
            return "b"

        job = workflow.start("A")
        workflow.cancel_job("A", job.job_id)

        assert not workflow.node_complete("A")
        assert not workflow.node_ready("B")


def test_threaded_runner_runs_multiple_jobs_inside_one_node_at_once():
    import time

    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="threaded")
        workflow.graph([("A", "B")])

        @workflow.task("A", max_threads=2)
        def a(ctx):
            time.sleep(0.20)
            return ctx.job_id

        @workflow.task("B")
        def b(ctx):
            return "done"

        workflow.start("A", job_id=1)
        workflow.start("A", job_id=2)

        started = time.perf_counter()
        workflow.run_node("A")
        elapsed = time.perf_counter() - started

        assert elapsed < 0.35
        assert workflow.node_complete("A")


def test_threaded_workflow_runs_ready_nodes_at_the_same_time():
    import time

    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="threaded")
        workflow.graph([("A", "C"), ("B", "C")])

        @workflow.task("A", max_threads=1)
        def a(ctx):
            time.sleep(0.20)
            return "A"

        @workflow.task("B", max_threads=1)
        def b(ctx):
            time.sleep(0.20)
            return "B"

        @workflow.task("C")
        def c(ctx):
            return "C"

        workflow.start("A")
        workflow.start("B")

        started = time.perf_counter()
        ran = workflow.run()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.35
        assert set(ran) == {"A", "B"}
        assert workflow.node_complete("A")
        assert workflow.node_complete("B")


def test_threaded_workflow_starts_newly_ready_nodes_while_other_nodes_are_still_running():
    import time

    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="threaded")
        workflow.graph([("A", "C"), ("B", "D")])
        timeline = {}

        @workflow.task("A", max_threads=1)
        def a(ctx):
            time.sleep(0.05)
            ctx.node("C").add()
            return "A"

        @workflow.task("B", max_threads=1)
        def b(ctx):
            time.sleep(0.30)
            timeline["b_finished"] = time.perf_counter()
            return "B"

        @workflow.task("C", max_threads=1)
        def c(ctx):
            timeline["c_started"] = time.perf_counter()
            time.sleep(0.20)
            return "C"

        @workflow.task("D", max_threads=1)
        def d(ctx):
            return "D"

        workflow.start("A")
        workflow.start("B")

        ran = workflow.run()

        # Check the actual scheduling property instead of relying on a tight
        # wall-clock threshold that is flaky on busy or slow filesystems.
        assert timeline["c_started"] < timeline["b_finished"]
        assert set(ran) == {"A", "B", "C"}
        assert workflow.node_complete("C")


def test_router_can_force_one_node_to_run_jobs_sequentially_with_threaded_workflow():
    import time

    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="threaded")
        workflow.graph([("A", "B")])

        router = NodeRouter("A", max_threads=2)
        router.run_sequentially()

        @router.task
        def a(ctx):
            time.sleep(0.12)
            return ctx.job_id

        workflow.include_router(router)

        @workflow.task("B")
        def b(ctx):
            return "done"

        workflow.start("A", job_id=1)
        workflow.start("A", job_id=2)

        started = time.perf_counter()
        workflow.run_node("A")
        elapsed = time.perf_counter() - started

        assert elapsed >= 0.20
        assert workflow.node_complete("A")
        assert workflow.nodes["A"].runner_override == "direct"


def test_run_node_marks_node_running_before_streaming_queued_jobs(monkeypatch):
    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="direct")
        workflow.graph([("A", "B")])

        @workflow.task("A")
        def a(ctx):
            return "done"

        @workflow.task("B")
        def b(ctx):
            return "done"

        workflow.start("A")
        original_iter_queued_job_ids = workflow.storage.iter_queued_job_ids
        observed_status = {}

        def iter_queued_job_ids_spy(node_name):
            observed_status[node_name] = workflow.storage.get_node_status(node_name)
            yield from original_iter_queued_job_ids(node_name)

        def queued_jobs_should_not_be_used(node_name):
            raise AssertionError("run_node should stream queued job IDs instead")

        monkeypatch.setattr(workflow.storage, "iter_queued_job_ids", iter_queued_job_ids_spy)
        monkeypatch.setattr(workflow.storage, "queued_jobs", queued_jobs_should_not_be_used)
        workflow.run_node("A")

        assert observed_status["A"] == "running"


def test_threaded_runner_starts_before_lazy_source_is_exhausted():
    import threading
    import time

    from micro_workflow_manager.runners.threaded import ThreadedRunner

    first_job_started = threading.Event()
    allow_second_job = threading.Event()

    def job_source():
        yield 1
        allow_second_job.wait(timeout=1)
        yield 2

    def run_one(job_id):
        if job_id == 1:
            first_job_started.set()
        return job_id

    result_holder = {}

    def run_runner():
        result_holder["result"] = ThreadedRunner(max_threads=2).run_job_source(
            node_name="A",
            job_source=job_source(),
            run_one=run_one,
        )

    thread = threading.Thread(target=run_runner)
    thread.start()

    assert first_job_started.wait(timeout=0.5)
    assert thread.is_alive()

    allow_second_job.set()
    thread.join(timeout=1)

    assert sorted(result_holder["result"]) == [1, 2]


def test_process_runner_executes_jobs_in_child_processes_and_keeps_dynamic_job_ids_unique(tmp_path, monkeypatch):
    import json
    import os
    import textwrap

    from micro_workflow_manager import cli

    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B')]\n",
        encoding="utf-8",
    )
    (behavior / "A.py").write_text(
        textwrap.dedent(
            """
            import os
            import time
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("A", max_threads=2)
            router.create_job(number=6)

            @router.task
            def run(ctx):
                time.sleep(0.05)
                ctx.node("B").add(value=ctx.job_id)
                return os.getpid()
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("B", max_threads=2)

            @router.task
            def run(ctx, value):
                return value
            """
        ).strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "process"]) == 0
    assert cli.main(["runfrom", "A", "--runner", "process"]) == 0

    parent_pid = os.getpid()
    child_pids = set()
    for job_id in range(1, 7):
        data = json.loads((tmp_path / "node" / "A" / "jobs" / str(job_id) / "output.json").read_text())
        child_pids.add(int(data["result_repr"]))

    assert child_pids
    assert parent_pid not in child_pids
    assert sorted(path.name for path in (tmp_path / "node" / "B" / "jobs").iterdir()) == [
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
    ]
    assert json.loads((tmp_path / "node" / "A" / "node_state.json").read_text())["status"] == "done"
    assert json.loads((tmp_path / "node" / "B" / "node_state.json").read_text())["status"] == "done"


def test_process_runner_without_graph_path_explains_requirement(tmp_path):
    import pytest

    workflow = MicroWorkflow(project_dir=tmp_path, runner="process")

    @workflow.task("A")
    def a(ctx):
        return ctx.job_id

    workflow.start("A")

    with pytest.raises(RuntimeError, match="process runner needs a graph file"):
        workflow.run_node("A")


def test_dirty_job_index_does_not_fail_dynamic_spawn(monkeypatch):
    """A transient Windows-style index write failure must not kill job creation."""
    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="direct")
        workflow.graph([("A", "B")])

        @workflow.task("A")
        def a(ctx):
            ctx.node("B").add(value=123)
            return "a"

        @workflow.task("B")
        def b(ctx, value):
            return value

        original_write_job_index = workflow.storage.write_job_index
        failed_once = {"B": False}

        def flaky_write_job_index(node_name, index):
            if node_name == "B" and not failed_once["B"]:
                failed_once["B"] = True
                raise PermissionError(13, "Permission denied", str(workflow.storage.job_index_file(node_name)))
            return original_write_job_index(node_name, index)

        monkeypatch.setattr(workflow.storage, "write_job_index", flaky_write_job_index)

        workflow.start("A")
        workflow.run()

        assert failed_once["B"] is True
        assert workflow.node_complete("A")
        assert workflow.node_complete("B")
        assert workflow.storage.node_job_summary("B")["counts"][DONE] == 1


def test_threaded_high_fan_in_to_one_node_survives_index_contention(monkeypatch):
    """Many workers spawning into one convergence node should not rely on a fragile index file."""
    import time

    with tempfile.TemporaryDirectory() as project_dir:
        workflow = MicroWorkflow(project_dir=project_dir, runner="threaded")
        workflow.graph([("A", "Z")])

        @workflow.task("A", max_threads=16)
        def a(ctx):
            time.sleep(0.001)
            ctx.node("Z").add(value=ctx.job_id)
            return ctx.job_id

        @workflow.task("Z", max_threads=16)
        def z(ctx, value):
            time.sleep(0.001)
            return value

        original_write_job_index = workflow.storage.write_job_index
        failures_left = {"Z": 5}

        def flaky_write_job_index(node_name, index):
            if node_name == "Z" and failures_left["Z"] > 0:
                failures_left["Z"] -= 1
                raise PermissionError(13, "Permission denied", str(workflow.storage.job_index_file(node_name)))
            return original_write_job_index(node_name, index)

        monkeypatch.setattr(workflow.storage, "write_job_index", flaky_write_job_index)

        for job_id in range(1, 81):
            workflow.start("A", job_id=job_id)

        workflow.run()

        assert failures_left["Z"] == 0
        assert workflow.node_complete("A")
        assert workflow.node_complete("Z")
        assert workflow.storage.node_job_summary("Z")["total"] == 80
        assert workflow.storage.node_job_summary("Z")["counts"][DONE] == 80
