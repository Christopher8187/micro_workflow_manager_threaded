from __future__ import annotations

import json
import threading
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor

from micro_workflow_manager import NodeInputFileSystem, cli
from micro_workflow_manager.cli.cleanup import prepare_fresh_components
from micro_workflow_manager.models import Job
from micro_workflow_manager.system import MicroWorkflow


def test_prepared_job_payloads_are_written_concurrently(tmp_path, monkeypatch):
    workflow = MicroWorkflow(project_dir=tmp_path)
    workflow.graph([])
    jobs = [Job(node_name="sink", job_id=index, params={"value": index}) for index in range(1, 65)]
    thread_names = set()
    guard = threading.Lock()
    original_retry = workflow.storage.retry_fs

    def observed_retry(action, *args, **kwargs):
        with guard:
            thread_names.add(threading.current_thread().name)
        time.sleep(0.005)
        return original_retry(action, *args, **kwargs)

    monkeypatch.setattr(workflow.storage, "retry_fs", observed_retry)
    workflow.storage.prepare_jobs_batch(jobs)

    publish_threads = {name for name in thread_names if name.startswith("mwf-job-publish")}
    assert len(publish_threads) > 1
    assert all(
        workflow.storage.input_file("sink", job.job_id).is_file()
        for job in jobs
    )


def test_default_job_declaration_uses_one_prepared_batch(tmp_path, monkeypatch):
    workflow = MicroWorkflow(project_dir=tmp_path)
    workflow.graph([])

    @workflow.task("seed")
    def seed(ctx, value):
        return value
    commit_sizes = []
    original_commit = workflow.storage.commit_prepared_jobs_batch

    def observed_commit(jobs, **kwargs):
        commit_sizes.append(len(jobs))
        return original_commit(jobs, **kwargs)

    monkeypatch.setattr(workflow.storage, "commit_prepared_jobs_batch", observed_commit)
    jobs = workflow.create_jobs("seed", number=64, params={"value": 1})

    assert len(jobs) == 64
    assert commit_sizes == [64]
    assert workflow.storage.job_status_counts("seed")["queued"] == 64


def test_concurrent_single_job_routes_share_publish_group_and_keep_idempotency(tmp_path, monkeypatch):
    workflow = MicroWorkflow(project_dir=tmp_path)
    workflow.graph([])

    @workflow.task("sink")
    def sink(ctx, value=None):
        return value

    @workflow.task("sink2")
    def sink2(ctx, value=None):
        return value

    applied_group_sizes = []
    applied_group_nodes = []
    original_apply = workflow.storage._apply_auto_job_publishes
    original_submit = workflow.storage.submit_grouped_db_mutation

    def apply(connection, publishes):
        applied_group_sizes.append(len(publishes))
        applied_group_nodes.append(
            {publish.provisional.node_name for publish in publishes}
        )
        return original_apply(connection, publishes)

    def submit(group_key, item, operation, **options):
        if group_key[:1] == ("auto-job-publish",):
            operation = apply
            options["collect_seconds"] = 0.020
        return original_submit(group_key, item, operation, **options)

    monkeypatch.setattr(workflow.storage, "submit_grouped_db_mutation", submit)
    gate = threading.Barrier(32)

    def add_distinct(index):
        gate.wait()
        return workflow.add_job(
            None,
            "sink" if index % 2 == 0 else "sink2",
            value=index,
        )

    with ThreadPoolExecutor(max_workers=32) as executor:
        jobs = list(executor.map(add_distinct, range(32)))

    assert sorted(job.job_id for job in jobs if job.node_name == "sink") == list(range(1, 17))
    assert sorted(job.job_id for job in jobs if job.node_name == "sink2") == list(range(1, 17))
    assert max(applied_group_sizes) > 1
    assert any(len(nodes) > 1 for nodes in applied_group_nodes)

    duplicate_gate = threading.Barrier(32)

    def add_duplicate(index):
        duplicate_gate.wait()
        return workflow.add_job(
            None,
            "sink",
            value=index,
            idempotency_key="shared-route",
        )

    with ThreadPoolExecutor(max_workers=32) as executor:
        duplicates = list(executor.map(add_duplicate, range(32)))

    assert {job.job_id for job in duplicates} == {17}
    assert workflow.storage.job_status_counts("sink")["queued"] == 17
    staged = tmp_path / ".mwf" / "staged-jobs"
    assert not staged.exists() or not any(staged.iterdir())


def test_single_route_records_parent_jobs_created_in_publish_transaction(tmp_path, monkeypatch):
    workflow = MicroWorkflow(project_dir=tmp_path)
    workflow.graph([("parent", "sink")])

    @workflow.task("parent")
    def parent(ctx):
        return ctx.node("sink").add(value=7).job_id

    @workflow.task("sink")
    def sink(ctx, value):
        return value

    direct_jobs_created_appends = 0
    original_append = workflow.storage.append_job_event

    def append(node_name, job_id, event, **data):
        nonlocal direct_jobs_created_appends
        if event == "jobs_created":
            direct_jobs_created_appends += 1
        return original_append(node_name, job_id, event, **data)

    monkeypatch.setattr(workflow.storage, "append_job_event", append)
    workflow.start("parent", job_id=1)
    workflow.run_node("parent", ignore_readiness=True)

    events = workflow.storage.read_job_events("parent", 1)
    created = [event for event in events if event["event"] == "jobs_created"]
    assert direct_jobs_created_appends == 0
    assert len(created) == 1
    assert created[0]["jobs"] == [{"node": "sink", "job_id": 1, "params": {"value": 7}}]


def test_high_fanout_batch_creates_one_job_per_object_without_autostart(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="api")
    workflow.graph([
        ("preexplode", "explode"),
        ("explode", "handler"),
        ("handler", "explode"),
    ])

    target = NodeInputFileSystem("explode", "explode records")
    object_count = 400

    @workflow.task("preexplode", runner="api", max_threads=8)
    def preexplode(ctx):
        entries = []
        params = []
        keys = []
        for index in range(1, object_count + 1):
            rel = f"book/section/items/{index:06d}.json"
            entries.append((rel, {"order": [1, index], "statement": str(index)}))
            params.append({"record_file": rel})
            keys.append(f"record:{rel}")
        target.write_jsons(ctx, entries, overwrite=True)
        jobs = target.add_jobs(
            ctx,
            params,
            autostart=False,
            idempotency_keys=keys,
        )
        return len(jobs)

    @workflow.task("explode", runner="api", max_threads=32)
    def explode(ctx, record_file):
        return record_file

    @workflow.task("handler")
    def handler(ctx):
        return None

    assert workflow.component_id("preexplode") != workflow.component_id("explode")
    workflow.start("preexplode", job_id=1, autostart=False)
    workflow.run_node("preexplode", ignore_readiness=True)

    summary = workflow.storage.node_job_summary("explode")
    assert summary["counts"]["queued"] == object_count
    assert summary["counts"]["running"] == 0
    assert summary["counts"]["done"] == 0
    assert workflow.storage.get_node_status("explode") == "queued"
    assert len(workflow.storage.list_job_ids("explode")) == object_count
    assert json.loads(
        (tmp_path / "node" / "explode" / "input" / "book" / "section" / "items" / "000400.json").read_text()
    )["statement"] == "400"


def test_batch_idempotency_reuses_existing_object_jobs(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="api")
    workflow.graph([("preexplode", "explode")])
    target = NodeInputFileSystem("explode")

    @workflow.task("preexplode")
    def preexplode(ctx, parent_number):
        rels = [f"book/items/{index}.json" for index in range(5)]
        target.write_jsons(
            ctx,
            [(rel, {"parent": parent_number, "index": index}) for index, rel in enumerate(rels)],
            overwrite=True,
        )
        return [
            job.job_id
            for job in target.add_jobs(
                ctx,
                [{"record_file": rel} for rel in rels],
                idempotency_keys=[f"record:{rel}" for rel in rels],
            )
        ]

    @workflow.task("explode")
    def explode(ctx, record_file):
        return record_file

    workflow.start("preexplode", job_id=1, parent_number=1)
    workflow.start("preexplode", job_id=2, parent_number=2)
    workflow.run_node("preexplode", ignore_readiness=True)

    assert workflow.storage.list_job_ids("explode") == [1, 2, 3, 4, 5]
    assert [
        workflow.storage.load_job("explode", job_id).params["record_file"]
        for job_id in workflow.storage.list_job_ids("explode")
    ] == [f"book/items/{index}.json" for index in range(5)]


def test_batch_registration_uses_one_job_transaction(tmp_path, monkeypatch):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="api")
    workflow.graph([("preexplode", "explode")])

    @workflow.task("preexplode")
    def preexplode(ctx):
        return None

    @workflow.task("explode")
    def explode(ctx, record_file):
        return record_file

    transactions = 0
    original = workflow.storage.commit_prepared_jobs_batch_resolving_idempotency

    def counted(jobs, **kwargs):
        nonlocal transactions
        transactions += 1
        return original(jobs, **kwargs)

    monkeypatch.setattr(workflow.storage, "commit_prepared_jobs_batch_resolving_idempotency", counted)
    jobs = workflow.add_jobs(
        from_node="preexplode",
        to_node="explode",
        params_list=[{"record_file": f"{i}.json"} for i in range(250)],
        _parent_job_id=1,
    )
    assert len(jobs) == 250
    assert transactions == 1


def test_schema_v1_upgrade_preserves_jobs_and_initializes_sequences(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path)
    workflow.graph([])

    @workflow.task("explode")
    def explode(ctx, value):
        return value

    workflow.start("explode", job_id=1, value=1)
    workflow.start("explode", job_id=7, value=7)
    workflow.storage.close_thread_connection()

    import sqlite3

    database = tmp_path / ".mwf" / "state.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute("DROP TABLE job_sequences")
        connection.execute(
            "UPDATE metadata SET value='1' WHERE key='database_schema_version'"
        )
        connection.commit()
    finally:
        connection.close()

    # Simulate opening the upgraded package in a new Python process.
    from micro_workflow_manager.storage.sqlite_state import SQLiteStateMixin

    SQLiteStateMixin._initialized_databases.clear()
    upgraded = MicroWorkflow(project_dir=tmp_path)
    assert upgraded.storage.list_job_ids("explode") == [1, 7]
    assert upgraded.storage.next_job_id("explode") == 8
    assert upgraded.storage.reserve_job_ids("explode", 3) == [8, 9, 10]
    upgraded.storage.close_thread_connection()

    connection = sqlite3.connect(database)
    try:
        assert connection.execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone()[0] == "4"
    finally:
        connection.close()



def test_cli_monitor_reports_all_batched_downstream_jobs_queued(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "from micro_workflow_manager import fan\n"
        "EDGES = [('preexplode', 'explode'), fan('explode', ['handler']), fan(['handler'], 'explode')]\n",
        encoding="utf-8",
    )
    (behavior / "preexplode.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeInputFileSystem, NodeRouter

            router = NodeRouter("preexplode", runner="api", max_threads=16)
            router.create_job(number=16)
            target = NodeInputFileSystem("explode")

            @router.task
            def run(ctx):
                entries = []
                params = []
                keys = []
                for index in range(10):
                    rel = f"book/section_{ctx.job_id}/items/{index:06d}.json"
                    entries.append((rel, {"section": ctx.job_id, "index": index}))
                    params.append({"record_file": rel})
                    keys.append(f"record:{rel}")
                target.write_jsons(ctx, entries, overwrite=True)
                target.add_jobs(ctx, params, autostart=False, idempotency_keys=keys)
                return len(params)
            """
        ).strip() + "\n",
        encoding="utf-8",
    )
    (behavior / "explode.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('explode', runner='api', max_threads=32)\n"
        "@router.task\n"
        "def run(ctx, record_file): return record_file\n",
        encoding="utf-8",
    )
    (behavior / "handler.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('handler')\n"
        "@router.task\n"
        "def run(ctx): return None\n",
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "api"]) == 0
    capsys.readouterr()
    assert cli.main(
        ["run", "preexplode", "--monitor", "--monitor-interval", "0.01"]
    ) == 0
    captured = capsys.readouterr()
    assert "last run: run preexplode | status=done" in captured.err

    storage = workflow_storage = MicroWorkflow(project_dir=tmp_path).storage
    try:
        counts = storage.node_job_summary("explode")["counts"]
        assert counts["queued"] == 160
        assert counts["running"] == 0
        assert counts["done"] == 0
    finally:
        workflow_storage.close_thread_connection()


def test_individual_job_deletion_does_not_reuse_reserved_batch_ids(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path)
    workflow.graph([])

    @workflow.task("explode")
    def explode(ctx):
        return None

    workflow.start("explode", job_id=1)
    assert workflow.storage.reserve_job_ids("explode", 3) == [2, 3, 4]
    assert workflow.storage.delete_job("explode", 1)
    assert workflow.storage.reserve_job_ids("explode", 1) == [5]


def test_fresh_component_cleanup_uses_bulk_producer_snapshot(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([
        ("preexplode", "explode"),
        ("explode", "handler"),
        ("handler", "explode"),
    ])

    @workflow.task("preexplode")
    def preexplode(ctx):
        return None

    @workflow.task("explode")
    def explode(ctx, value):
        return value

    @workflow.task("handler")
    def handler(ctx, value):
        return value

    workflow.add_jobs(
        "preexplode",
        "explode",
        [{"value": value} for value in range(100)],
        _parent_job_id=1,
    )
    workflow.add_jobs(
        "explode",
        "handler",
        [{"value": value} for value in range(200)],
        _parent_job_id=1,
    )

    monkeypatch.setattr(
        workflow.storage,
        "read_job_metadata",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("fresh cleanup must not query producer metadata per job")
        ),
    )

    removed = prepare_fresh_components(
        tmp_path,
        workflow,
        [workflow.component_for("explode")],
    )
    assert removed == {"handler": 200}
    assert workflow.storage.job_status_counts("explode")["queued"] == 100
