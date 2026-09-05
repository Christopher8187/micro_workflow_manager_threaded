from __future__ import annotations

import sqlite3
import threading
import time

from micro_workflow_manager import MicroWorkflow, NodeRouter


def test_component_consumes_publication_between_coordinator_observations(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    producer_started = threading.Event()
    allow_publication = threading.Event()
    transitioned = False
    coordinator = threading.get_ident()
    producer = NodeRouter("A", runner="threaded", max_threads=1)
    consumer = NodeRouter("B", runner="threaded", max_threads=1, wait_for=["A"])

    @producer.task
    def produce(ctx):
        producer_started.set()
        assert allow_publication.wait(10), "Coordinator never observed the active producer"
        ctx.node("B").add()
        return "published"

    @consumer.task
    def consume(ctx):
        return "consumed"

    workflow.include_routers(producer, consumer)
    workflow.add_job(None, "A")
    original_connection = workflow.storage.db_connection
    observer = sqlite3.connect(workflow.storage.state_database_path().as_uri() + "?mode=ro", uri=True)

    def after_observation():
        nonlocal transitioned
        if transitioned or not producer_started.is_set():
            return
        transitioned = True
        allow_publication.set()
        deadline = time.monotonic() + 10
        while observer.execute("SELECT status FROM jobs WHERE node_name='A' AND job_id=1").fetchone()[0] != "done":
            assert time.monotonic() < deadline, "Producer did not finish its real publication"
            time.sleep(0.001)

    class Cursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def fetchall(self):
            rows = self.cursor.fetchall()
            # Delay delivery of an actual SQLite observation. A producer may
            # publish and finish at this boundary under ordinary concurrency.
            # SQL, rows, and task execution remain real and unchanged.
            after_observation()
            return rows

        def __getattr__(self, name):
            return getattr(self.cursor, name)

    class Connection:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, *args, **kwargs):
            return Cursor(self.connection.execute(*args, **kwargs))

        def __getattr__(self, name):
            return getattr(self.connection, name)

    def connection():
        raw = original_connection()
        return Connection(raw) if threading.get_ident() == coordinator else raw

    monkeypatch.setattr(workflow.storage, "db_connection", connection)
    try:
        workflow.run_component({"A", "B"}, ignore_readiness=True)
    finally:
        allow_publication.set()
        observer.close()

    assert transitioned
    for node in ("A", "B"):
        counts = workflow.storage.job_status_counts(node)
        assert counts["done"] == 1
        assert all(counts[state] == 0 for state in ("queued", "running", "failed"))
    for node in ("A", "B"):
        assert workflow.storage.get_node_status(node) == "done"


def test_admitted_waiting_pump_finishes_between_claims_before_deadlock_decision(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    second_claim_ready = threading.Event()
    allow_second_claim = threading.Event()
    saw_between_claims = False
    producer = NodeRouter("A", runner="threaded", max_threads=1, wait_for=["B"])
    consumer = NodeRouter("B", runner="threaded", max_threads=1, wait_for=["A"])

    @producer.task
    def produce(ctx):
        ctx.node("B").add()
        return "published"

    @consumer.task
    def consume(ctx):
        return "consumed"

    workflow.include_routers(producer, consumer)
    workflow.add_jobs(None, "A", [{}, {}])
    original_status = workflow.storage.set_job_status
    original_observation = workflow.storage.nodes_by_job_status

    def mark_status(node_name, job_id, status, *args, **kwargs):
        if node_name == "A" and job_id == 2 and status == "running":
            second_claim_ready.set()
            assert allow_second_claim.wait(10), "Coordinator did not observe the gap between claims"
        return original_status(node_name, job_id, status, *args, **kwargs)

    def observe(*args, **kwargs):
        nonlocal saw_between_claims
        observed = original_observation(*args, **kwargs)
        if second_claim_ready.is_set() and observed["queued"] == {"A", "B"} and not observed["running"]:
            saw_between_claims = True
            allow_second_claim.set()
        return observed

    monkeypatch.setattr(workflow.storage, "set_job_status", mark_status)
    monkeypatch.setattr(workflow.storage, "nodes_by_job_status", observe)
    try:
        workflow.run_component({"A", "B"}, ignore_readiness=True)
    finally:
        allow_second_claim.set()

    assert saw_between_claims
    for node in ("A", "B"):
        counts = workflow.storage.job_status_counts(node)
        assert counts["done"] == 2
        assert all(counts[state] == 0 for state in ("queued", "running", "failed"))


def test_idle_resident_member_does_not_hide_a_real_waiting_deadlock(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "C"), ("C", "A")])
    for name, wait_for in (("A", ["B"]), ("B", ["A"]), ("C", None)):
        router = NodeRouter(name, runner="threaded", wait_for=wait_for)

        @router.task
        def run(ctx):
            raise AssertionError("A deadlocked component must not start its waiting jobs")

        workflow.include_router(router)
    workflow.add_job(None, "A")
    workflow.add_job(None, "B")
    original_observation = workflow.storage.nodes_by_job_status
    deadline = time.monotonic() + 10

    def observe(*args, **kwargs):
        # A diagnostic deadline also releases the resident pump through the
        # ordinary error cleanup if a regression keeps waiting indefinitely.
        assert time.monotonic() < deadline, "Idle resident pump hid the waiting deadlock"
        return original_observation(*args, **kwargs)

    monkeypatch.setattr(workflow.storage, "nodes_by_job_status", observe)
    workflow.run_component({"A", "B", "C"}, ignore_readiness=True)
    for node in ("A", "B"):
        counts = workflow.storage.job_status_counts(node)
        assert counts["queued"] == 1
        assert all(counts[state] == 0 for state in ("running", "done", "failed"))
