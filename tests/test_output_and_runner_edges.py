import threading

from micro_workflow_manager import MicroWorkflow


def test_global_threaded_workflow_can_mix_direct_and_threaded_nodes_at_the_same_time(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="threaded")

    state = {
        "direct_active": 0,
        "threaded_active": 0,
        "max_direct_active": 0,
        "max_threaded_active": 0,
        "saw_cross_node_overlap": False,
    }
    lock = threading.Lock()
    direct_started = threading.Event()
    threaded_pair_started = threading.Event()

    @workflow.task("direct_node", runner="direct", max_threads=4)
    def direct_node(ctx):
        with lock:
            state["direct_active"] += 1
            state["max_direct_active"] = max(state["max_direct_active"], state["direct_active"])
            direct_started.set()
        threaded_pair_started.wait(timeout=2)
        with lock:
            state["direct_active"] -= 1
        return ctx.job_id

    @workflow.task("threaded_node", max_threads=2)
    def threaded_node(ctx):
        assert direct_started.wait(timeout=2)
        with lock:
            state["threaded_active"] += 1
            state["max_threaded_active"] = max(state["max_threaded_active"], state["threaded_active"])
            if state["direct_active"] > 0:
                state["saw_cross_node_overlap"] = True
            if state["threaded_active"] == 2:
                threaded_pair_started.set()
        assert threaded_pair_started.wait(timeout=2)
        with lock:
            state["threaded_active"] -= 1
        return ctx.job_id

    workflow.start("direct_node", job_id=1)
    workflow.start("direct_node", job_id=2)
    workflow.start("threaded_node", job_id=1)
    workflow.start("threaded_node", job_id=2)

    workflow.run()

    assert workflow.nodes["direct_node"].runner_override == "direct"
    assert workflow.nodes["direct_node"].max_threads == 1
    assert state["max_direct_active"] == 1
    assert state["max_threaded_active"] == 2
    assert state["saw_cross_node_overlap"] is True


def test_global_direct_workflow_can_thread_one_node_with_runner_override(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    state = {"threaded_active": 0, "max_threaded_active": 0, "direct_active": 0, "max_direct_active": 0}
    lock = threading.Lock()
    threaded_pair_started = threading.Event()

    @workflow.task("threaded_node", runner="threaded", max_threads=2)
    def threaded_node(ctx):
        with lock:
            state["threaded_active"] += 1
            state["max_threaded_active"] = max(state["max_threaded_active"], state["threaded_active"])
            if state["threaded_active"] == 2:
                threaded_pair_started.set()
        assert threaded_pair_started.wait(timeout=2)
        with lock:
            state["threaded_active"] -= 1
        return ctx.job_id

    @workflow.task("direct_node")
    def direct_node(ctx):
        with lock:
            state["direct_active"] += 1
            state["max_direct_active"] = max(state["max_direct_active"], state["direct_active"])
        with lock:
            state["direct_active"] -= 1
        return ctx.job_id

    workflow.start("threaded_node", job_id=1)
    workflow.start("threaded_node", job_id=2)
    workflow.start("direct_node", job_id=1)
    workflow.start("direct_node", job_id=2)

    workflow.run()

    assert state["max_threaded_active"] == 2
    assert state["max_direct_active"] == 1
    assert workflow.node_complete("threaded_node")
    assert workflow.node_complete("direct_node")
