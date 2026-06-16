import json
import threading
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow


def test_output_file_jobs_from_two_upstreams_wait_for_all_predecessors(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="threaded")
    workflow.graph([("source_a", "consume"), ("source_c", "consume")])

    @workflow.task("source_a", max_threads=2)
    def source_a(ctx, label):
        ctx.write_output(f"{label}.txt", label)
        ctx.node("consume").add_from_output_files(
            "*.txt",
            file_param="source_file",
            path_mode="name",
            source="A",
        )

    @workflow.task("source_c", runner="direct")
    def source_c(ctx):
        ctx.write_output("from_c.txt", "C")
        ctx.node("consume").add_from_output_files(
            "*.txt",
            file_param="source_file",
            path_mode="name",
            source="C",
        )

    @workflow.task("consume", max_threads=3)
    def consume(ctx, source, source_file):
        ctx.write(f"consumed_{ctx.job_id}.json", json.dumps({"source": source, "name": source_file}))
        return {"source": source, "name": source_file}

    workflow.start("source_a", job_id=1, label="a1")
    workflow.start("source_a", job_id=2, label="a2")
    workflow.start("source_c")

    workflow.run_node("source_a")

    assert workflow.storage.get_node_status("consume") == "queued"
    assert not workflow.node_ready("consume")
    assert not workflow.node_complete("consume")
    assert len(workflow.storage.list_jobs("consume")) == 2

    workflow.run_node("source_c")
    assert workflow.node_ready("consume")

    workflow.run()

    consumed_inputs = sorted(
        (
            workflow.storage.read_json(workflow.storage.input_file("consume", row["job_id"]))
            for row in workflow.storage.list_jobs("consume")
        ),
        key=lambda item: (item["source"], item["source_file"]),
    )
    assert consumed_inputs == [
        {"source": "A", "source_file": "a1.txt"},
        {"source": "A", "source_file": "a2.txt"},
        {"source": "C", "source_file": "from_c.txt"},
    ]
    assert workflow.node_complete("consume")


def test_output_file_autostarted_join_job_does_not_complete_downstream_node_early(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("make_file", "join"), ("other_parent", "join")])

    @workflow.task("make_file")
    def make_file(ctx):
        ctx.write_output("only_after_node_done.txt", "ready")
        ctx.node("join").add_from_output_files(
            "*.txt",
            file_param="output_file",
            autostart=True,
        )

    @workflow.task("other_parent")
    def other_parent(ctx):
        return "other parent finished"

    @workflow.task("join")
    def join(ctx, output_file):
        return Path(output_file).read_text(encoding="utf-8")

    workflow.start("make_file")
    workflow.start("other_parent")

    workflow.run_node("make_file")

    join_rows = workflow.storage.list_jobs("join")
    assert len(join_rows) == 1
    assert join_rows[0]["status"] == "done"
    assert workflow.storage.get_node_status("join") == "queued"
    assert not workflow.node_complete("join")

    workflow.run_node("other_parent")
    workflow.ready_nodes()

    assert workflow.node_complete("join")


def test_add_from_output_files_path_modes_recursive_and_files_only(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="threaded")
    workflow.graph([
        ("producer", "absolute_reader"),
        ("producer", "relative_reader"),
        ("producer", "name_reader"),
    ])

    @workflow.task("producer")
    def producer(ctx):
        ctx.write_output("root.txt", "root")
        ctx.write_output("nested/child.txt", "child")
        ctx.output_path("nested", "folder").mkdir(parents=True)

        ctx.node("absolute_reader").add_from_output_files(
            "**/*",
            recursive=True,
            files_only=True,
            path_mode="absolute",
            file_param="seen_file",
        )
        ctx.node("relative_reader").add_from_output_files(
            "**/*",
            recursive=True,
            files_only=True,
            path_mode="relative",
            file_param="seen_file",
        )
        ctx.node("name_reader").add_from_output_files(
            "**/*",
            recursive=True,
            files_only=True,
            path_mode="name",
            file_param="seen_file",
        )

    @workflow.task("absolute_reader")
    def absolute_reader(ctx, seen_file):
        path = Path(seen_file)
        assert path.is_absolute()
        assert path.is_file()
        return path.name

    @workflow.task("relative_reader")
    def relative_reader(ctx, seen_file):
        assert not Path(seen_file).is_absolute()
        assert seen_file in {"root.txt", "nested/child.txt"}
        return seen_file

    @workflow.task("name_reader")
    def name_reader(ctx, seen_file):
        assert seen_file in {"root.txt", "child.txt"}
        return seen_file

    workflow.start("producer")
    workflow.run()

    assert len(workflow.storage.list_jobs("absolute_reader")) == 2
    assert len(workflow.storage.list_jobs("relative_reader")) == 2
    assert len(workflow.storage.list_jobs("name_reader")) == 2

    relative_values = sorted(
        workflow.storage.read_json(workflow.storage.input_file("relative_reader", row["job_id"]))["seen_file"]
        for row in workflow.storage.list_jobs("relative_reader")
    )
    name_values = sorted(
        workflow.storage.read_json(workflow.storage.input_file("name_reader", row["job_id"]))["seen_file"]
        for row in workflow.storage.list_jobs("name_reader")
    )

    assert relative_values == ["nested/child.txt", "root.txt"]
    assert name_values == ["child.txt", "root.txt"]


def test_add_from_output_files_dedupe_false_scans_once_per_requesting_source_job(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("producer", "consumer")])

    @workflow.task("producer")
    def producer(ctx, name):
        ctx.write_output(f"{name}.txt", name)
        ctx.node("consumer").add_from_output_files(
            "*.txt",
            file_param="output_file",
            path_mode="name",
            dedupe=False,
        )

    @workflow.task("consumer")
    def consumer(ctx, output_file):
        return output_file

    workflow.start("producer", job_id=1, name="one")
    workflow.start("producer", job_id=2, name="two")

    workflow.run_node("producer")

    rows = workflow.storage.list_jobs("consumer")
    assert len(rows) == 4
    assert sorted(row["parent"]["from_job_id"] for row in rows) == [1, 1, 2, 2]
    assert sorted(
        workflow.storage.read_json(workflow.storage.input_file("consumer", row["job_id"]))["output_file"]
        for row in rows
    ) == ["one.txt", "one.txt", "two.txt", "two.txt"]


def test_add_from_output_files_rejects_invalid_usage_without_queuing_deferred_work(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("producer", "consumer")])

    @workflow.task("producer")
    def producer(ctx):
        return "producer"

    @workflow.task("consumer")
    def consumer(ctx, output_file, required_value):
        return output_file, required_value

    invalid_calls = [
        lambda: workflow.defer_output_file_jobs("producer", "consumer", "../*.txt", required_value="x"),
        lambda: workflow.defer_output_file_jobs("producer", "consumer", "*.txt", path_mode="bad", required_value="x"),
        lambda: workflow.defer_output_file_jobs("producer", "consumer", "*.txt", file_param="", required_value="x"),
        lambda: workflow.defer_output_file_jobs("producer", "consumer", "*.txt", output_file="reserved", required_value="x"),
        lambda: workflow.defer_output_file_jobs("producer", "consumer", "*.txt"),
        lambda: workflow.defer_output_file_jobs("producer", "consumer", "*.txt", required_value={"not json serializable"}),
    ]

    for call in invalid_calls:
        with pytest.raises((TypeError, ValueError)):
            call()

    assert workflow._deferred_output_file_jobs == []
    assert workflow.storage.list_jobs("consumer") == []


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


def test_output_file_downstream_node_can_be_forced_direct_inside_threaded_workflow(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="threaded")
    workflow.graph([("producer", "consumer")])
    state = {"active": 0, "max_active": 0}
    lock = threading.Lock()

    @workflow.task("producer")
    def producer(ctx):
        for index in range(3):
            ctx.write_output(f"item_{index}.txt", str(index))
        ctx.node("consumer").add_from_output_files("*.txt", file_param="output_file")

    @workflow.task("consumer", runner="direct", max_threads=5)
    def consumer(ctx, output_file):
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
        with lock:
            state["active"] -= 1
        return Path(output_file).read_text(encoding="utf-8")

    workflow.start("producer")
    workflow.run()

    assert len(workflow.storage.list_jobs("consumer")) == 3
    assert workflow.nodes["consumer"].runner_override == "direct"
    assert workflow.nodes["consumer"].max_threads == 1
    assert state["max_active"] == 1
    assert workflow.node_complete("consumer")


def test_output_file_downstream_node_can_be_threaded_inside_direct_workflow(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("producer", "consumer")])
    state = {"active": 0, "max_active": 0}
    lock = threading.Lock()
    all_three_started = threading.Event()

    @workflow.task("producer")
    def producer(ctx):
        for index in range(3):
            ctx.write_output(f"item_{index}.txt", str(index))
        ctx.node("consumer").add_from_output_files("*.txt", file_param="output_file")

    @workflow.task("consumer", runner="threaded", max_threads=3)
    def consumer(ctx, output_file):
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            if state["active"] == 3:
                all_three_started.set()
        assert all_three_started.wait(timeout=2)
        with lock:
            state["active"] -= 1
        return Path(output_file).name

    workflow.start("producer")
    workflow.run()

    assert len(workflow.storage.list_jobs("consumer")) == 3
    assert state["max_active"] == 3
    assert workflow.node_complete("consumer")
