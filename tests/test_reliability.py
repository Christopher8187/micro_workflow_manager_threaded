import tempfile

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter


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

        @workflow.task("A", max_threads=1)
        def a(ctx):
            time.sleep(0.05)
            ctx.node("C").add()
            return "A"

        @workflow.task("B", max_threads=1)
        def b(ctx):
            time.sleep(0.30)
            return "B"

        @workflow.task("C", max_threads=1)
        def c(ctx):
            time.sleep(0.20)
            return "C"

        @workflow.task("D", max_threads=1)
        def d(ctx):
            return "D"

        workflow.start("A")
        workflow.start("B")

        started = time.perf_counter()
        ran = workflow.run()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.45
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
