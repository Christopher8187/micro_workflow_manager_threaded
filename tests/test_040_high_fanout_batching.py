from __future__ import annotations

import json
import textwrap
import time

from micro_workflow_manager import NodeInputFileSystem, cli
from micro_workflow_manager.cli.cleanup import prepare_fresh_components
from micro_workflow_manager.system import MicroWorkflow


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
