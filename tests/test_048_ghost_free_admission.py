from __future__ import annotations

import asyncio
import json
import threading
import time

import httpx

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)


HANDLERS = (
    "explodeclaim",
    "explodecontext",
    "explodedefinition",
    "explodeexample",
    "explodeexercise",
    "explodeexplanation",
    "exploderemark",
    "explodetheorem",
)


def test_balanced_high_concurrency_has_no_ghost_visibility_regression(
    tmp_path,
    monkeypatch,
):
    """A bounded, non-stress regression for output/state divergence.

    The old admission loop could leave provider-completed/output-backed jobs
    without corresponding terminal state. This test creates an uneven high-limit
    Hoeflein wave, samples SQLite while it runs, and requires every durable
    output, terminal event, and final job row to identify the same execution.
    """
    monkeypatch.setenv("MWF_API_STARTUP_STRATEGY", "balanced")
    monkeypatch.setenv("MWF_API_MAX_ADMISSION_BURST", "512")
    monkeypatch.setenv("MWF_API_ADMISSION_TARGET_ROUNDS", "4")
    monkeypatch.setenv("MWF_API_EVENT_DRAIN_SECONDS", "0.010")
    monkeypatch.setenv("MWF_API_TERMINAL_MICROBATCH", "1")

    close_shared_http_transport()
    lock = threading.Lock()
    provider_completed = 0
    output_executions: dict[tuple[str, int], tuple[int, str]] = {}

    async def provider(request: httpx.Request) -> httpx.Response:
        nonlocal provider_completed
        payload = json.loads(request.content)
        await asyncio.sleep(0.001 + ((payload["value"] * 7 + payload["node"]) % 13) * 0.0007)
        with lock:
            provider_completed += 1
        return httpx.Response(200, json={"ok": True}, request=request)

    configure_shared_http_transport(
        http2=True,
        streams_per_connection=1024,
        transport=httpx.MockTransport(provider),
    )

    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.active_job_restart_enabled = True
    edges = []
    for name in HANDLERS:
        edges.extend((("explode", name), (name, "explode")))
    workflow.graph(edges)

    explode = NodeRouter(
        "explode",
        runner="threaded",
        max_threads=1,
        wait_for=list(HANDLERS),
    )

    @explode.task
    def run_explode(ctx):
        return None

    workflow.include_router(explode)
    routers = [explode]
    limits = (3000, 5000, 12000, 5000, 9000, 6000, 5000, 5000)
    counts = (40, 64, 96, 48, 128, 72, 52, 100)
    for node_index, (name, limit, count) in enumerate(zip(HANDLERS, limits, counts)):
        router = NodeRouter(name, runner="api", max_threads=limit, wait_for=["explode"])

        def make_handler(index):
            def run_handler(ctx, value):
                return shared_http_transport.post_json(
                    "https://mock.local/explode",
                    timeout=10,
                    json={"value": value, "node": index},
                )

            return run_handler

        router.task(timeout=20)(make_handler(node_index))
        workflow.include_router(router)
        routers.append(router)
        workflow.add_jobs(None, name, [{"value": value} for value in range(count)])

    original_write_output = workflow.storage.write_output

    def write_output(node_name, job_id, data):
        result = original_write_output(node_name, job_id, data)
        key = (node_name, int(job_id))
        with lock:
            assert key not in output_executions
            output_executions[key] = (
                int(data["generation"]),
                str(data["execution_id"]),
            )
        return result

    workflow.storage.write_output = write_output
    stop = threading.Event()
    max_missing_row = 0
    monitor_errors = []

    def sample():
        nonlocal max_missing_row
        try:
            while not stop.wait(0.002):
                with lock:
                    completed = provider_completed
                rows = workflow.storage.db_connection().execute(
                    "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"
                ).fetchall()
                counts_by_status = {str(row["status"]): int(row["count"]) for row in rows}
                visible = (
                    counts_by_status.get("running", 0)
                    + counts_by_status.get("done", 0)
                    + counts_by_status.get("failed", 0)
                )
                max_missing_row = max(max_missing_row, completed - visible)
        except BaseException as error:
            monitor_errors.append(error)
        finally:
            try:
                workflow.storage.close_thread_connection()
            except BaseException as error:
                monitor_errors.append(error)

    monitor = threading.Thread(target=sample, name="ghost-regression-monitor")
    monitor.start()
    errors = []

    def run():
        try:
            workflow.run_node("explode", ignore_readiness=True)
        except BaseException as error:  # pragma: no cover - surfaced below
            errors.append(error)

    started = time.perf_counter()
    worker = threading.Thread(target=run, name="ghost-regression-workflow")
    worker.start()
    body_failure = None
    cleanup_failures = []
    try:
        # Native ownership and capacity checks add work before each handler starts.
        # Keep a bounded completion wait without imposing the earlier throughput.
        worker.join(timeout=180)
        assert not worker.is_alive(), "high-concurrency ghost regression test timed out"
        assert not errors
        assert time.perf_counter() - started < 180

        stop.set()
        monitor.join(timeout=2)
        assert not monitor.is_alive(), "ghost regression monitor did not stop"
        assert not monitor_errors
        close_shared_http_transport()
        workflow.storage.flush_db_mutations()

        status_rows = workflow.storage.db_connection().execute(
            "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"
        ).fetchall()
        final = {str(row["status"]): int(row["count"]) for row in status_rows}
        assert final.get("queued", 0) == 0
        assert final.get("running", 0) == 0
        assert final.get("failed", 0) == 0
        assert final.get("done", 0) == sum(counts)
        assert max_missing_row == 0

        terminal_rows = workflow.storage.db_connection().execute(
            "SELECT node_name, job_id, data_json FROM job_events WHERE event='done'"
        ).fetchall()
        terminal_executions = {}
        for row in terminal_rows:
            key = (str(row["node_name"]), int(row["job_id"]))
            assert key not in terminal_executions
            data = json.loads(str(row["data_json"]))
            assert data["previous_status"] == "running"
            assert data["status"] == "done"
            terminal_executions[key] = (
                int(data["generation"]),
                str(data["execution_id"]),
            )

        with lock:
            durable_outputs = dict(output_executions)
        expected_jobs = {
            (node_name, job_id)
            for node_name, count in zip(HANDLERS, counts)
            for job_id in range(1, count + 1)
        }
        assert set(durable_outputs) == expected_jobs
        assert terminal_executions == durable_outputs

        execution_owner_rows = workflow.storage.db_connection().execute(
            "SELECT execution_id, node_name, job_id, generation "
            "FROM job_execution_owners"
        ).fetchall()
        execution_owners = {}
        for row in execution_owner_rows:
            key = (str(row["node_name"]), int(row["job_id"]))
            assert key not in execution_owners
            execution_owners[key] = (
                int(row["generation"]),
                str(row["execution_id"]),
            )
        assert execution_owners == durable_outputs

        owner_rows = workflow.storage.db_connection().execute(
            "SELECT node_name, job_id, status, status_json, generation, "
            "active_execution_id FROM jobs"
        ).fetchall()
        terminal_jobs = {}
        for row in owner_rows:
            key = (str(row["node_name"]), int(row["job_id"]))
            status_data = json.loads(str(row["status_json"]))
            terminal_jobs[key] = (
                str(row["status"]),
                int(row["generation"]),
                row["active_execution_id"],
                int(status_data["generation"]),
                str(status_data["execution_id"]),
            )
        assert terminal_jobs == {
            key: ("done", generation, None, generation, execution_id)
            for key, (generation, execution_id) in durable_outputs.items()
        }
    except BaseException as error:
        body_failure = (error, error.__traceback__)
    finally:
        stop.set()
        try:
            monitor.join(timeout=2)
            if monitor.is_alive():
                monitor.join(timeout=10)
            assert not monitor.is_alive(), "ghost regression monitor did not stop"
        except BaseException as error:
            cleanup_failures.append(error)
        try:
            close_shared_http_transport()
        except BaseException as error:
            cleanup_failures.append(error)
        try:
            if worker.is_alive():
                worker.join(timeout=10)
            assert not worker.is_alive(), "high-concurrency ghost regression worker did not stop"
        except BaseException as error:
            cleanup_failures.append(error)

        if not worker.is_alive() and not monitor.is_alive():
            can_close_storage = True
            try:
                workflow.storage.db_mutation_barrier()
            except BaseException as error:
                cleanup_failures.append(error)
                can_close_storage = False
            if can_close_storage:
                deadline = time.perf_counter() + 10
                try:
                    while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
                        assert time.perf_counter() < deadline, "Mutation writer did not retire"
                        time.sleep(0.01)
                except BaseException as error:
                    cleanup_failures.append(error)
                    can_close_storage = False
            if can_close_storage:
                try:
                    workflow.storage.close_database_connections()
                except BaseException as error:
                    cleanup_failures.append(error)

    if body_failure is not None:
        raise body_failure[0].with_traceback(body_failure[1])
    if cleanup_failures:
        raise cleanup_failures[0]


def test_balanced_admits_a_small_27_job_tail_without_plateau_stall(tmp_path, monkeypatch):
    monkeypatch.setenv("MWF_API_STARTUP_STRATEGY", "balanced")
    monkeypatch.setenv("MWF_API_MAX_ADMISSION_BURST", "512")
    close_shared_http_transport()

    async def provider(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.001)
        return httpx.Response(200, json={"ok": True}, request=request)

    configure_shared_http_transport(
        http2=True,
        streams_per_connection=64,
        transport=httpx.MockTransport(provider),
    )
    try:
        workflow = MicroWorkflow(tmp_path, runner="api")
        workflow.graph([])
        router = NodeRouter("explodeexercise", runner="api", max_threads=9000)

        @router.task(timeout=10)
        def run_job(ctx, value):
            return shared_http_transport.post_json(
                "https://mock.local/tail", timeout=5, json={"value": value}
            )

        workflow.include_router(router)
        workflow.add_jobs(
            None,
            "explodeexercise",
            [{"value": value} for value in range(27)],
        )
        started = time.perf_counter()
        workflow.run_node("explodeexercise", ignore_readiness=True)
        assert time.perf_counter() - started < 5
        row = workflow.storage.db_connection().execute(
            "SELECT COUNT(*) AS count FROM jobs "
            "WHERE node_name='explodeexercise' AND status='done'"
        ).fetchone()
        assert int(row["count"]) == 27
        started_events = workflow.storage.db_connection().execute(
            "SELECT COUNT(*) AS count FROM job_events "
            "WHERE node_name='explodeexercise' AND event='started'"
        ).fetchone()
        assert int(started_events["count"]) == 27
    finally:
        close_shared_http_transport()
