import asyncio
import time

import httpx

from benchmarks.local_http_delay_server import H2Session, h1_transfer
from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.models import Job
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.runners.api import ApiRunner


def test_network_manager_is_default_and_coalesces_ingress_wakeups():
    async def handler(request):
        await asyncio.sleep(0.01)
        return httpx.Response(200, content=b"ok", request=request)

    close_shared_http_transport()
    configure_shared_http_transport(
        transport=httpx.MockTransport(handler),
        streams_per_connection=16,
        architecture="manager",
    )
    try:
        results = ApiRunner(max_threads=64, poll_interval=0.001).run_jobs(
            "A",
            list(range(128)),
            lambda _index: shared_http_transport.request(
                "GET", "https://example.test/", timeout=2
            ).status_code,
        )
        snapshot = shared_http_transport.snapshot()
    finally:
        close_shared_http_transport()

    assert results == [200] * 128
    assert snapshot["architecture"] == "manager"
    assert snapshot["requests_enqueued"] == 128
    assert 0 < snapshot["ingress_wakeups"] <= 128


def test_network_manager_snapshot_is_batched_into_sqlite(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([])
    router = NodeRouter("A", runner="api", max_threads=1)

    @router.task
    def work(ctx):
        return "ok"

    workflow.include_router(router)
    workflow.storage.publish_network_manager_snapshot(
        [{
            "node_name": "A",
            "submitted": 10,
            "dispatched": 10,
            "completed": 9,
            "failed": 1,
            "bytes_received": 4096,
            "in_flight": 0,
            "peak_in_flight": 4,
            "max_ingress_delay_seconds": 0.2,
            "max_request_seconds": 1.0,
            "average_request_seconds": 0.4,
            "last_error": "boom",
        }],
        123.0,
    )
    workflow.storage.flush_db_mutations()
    row = workflow.storage.network_manager_state()["A"]
    assert row["submitted"] == 10
    assert row["completed"] == 9
    assert row["failed"] == 1
    assert row["updated_at"] == 123.0


def test_network_state_schema_migrates_to_v4(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    row = workflow.storage.db_connection().execute(
        "SELECT value FROM metadata WHERE key='database_schema_version'"
    ).fetchone()
    assert int(row["value"]) == 4
    tables = {
        str(row[0])
        for row in workflow.storage.db_connection().execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "network_state" in tables


def test_refreshable_queue_rowid_plan_avoids_temp_sort(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([])
    router = NodeRouter("A", runner="api", max_threads=1)

    @router.task
    def work(ctx):
        return "ok"

    workflow.include_router(router)
    for job_id in range(1, 5):
        workflow.storage.create_job(Job(node_name="A", job_id=job_id, params={}))
    plan = workflow.storage.db_connection().execute(
        "EXPLAIN QUERY PLAN SELECT rowid AS source_rowid, job_id FROM jobs NOT INDEXED "
        "WHERE rowid>? AND node_name=? AND status=? ORDER BY rowid LIMIT ?",
        (0, "A", "queued", 2),
    ).fetchall()
    details = " ".join(str(row["detail"]) for row in plan).upper()
    assert "TEMP B-TREE" not in details


class _Writer:
    def __init__(self):
        self.data = bytearray()
    def write(self, data):
        self.data.extend(data)
    async def drain(self):
        return None


def test_h1_only_chunk_is_bandwidth_paced():
    async def run():
        writer = _Writer()
        started = time.monotonic()
        await h1_transfer(writer, 4096, 4096, 0, 4096)
        return time.monotonic() - started
    elapsed = asyncio.run(run())
    assert 0.90 <= elapsed <= 1.50


def test_h2_only_chunk_is_bandwidth_paced():
    class FakeSession:
        def __init__(self):
            self.finished = None
        async def headers(self, stream_id, headers, end_stream=False):
            return None
        async def data(self, stream_id, payload, end_stream=False):
            if end_stream:
                self.finished = time.monotonic()

    async def run():
        session = FakeSession()
        started = time.monotonic()
        await H2Session.serve_stream(
            session,
            1,
            "/transfer?bytes=4096&bps=4096&delay_ms=0&chunk=4096",
        )
        return session.finished - started
    elapsed = asyncio.run(run())
    assert 0.90 <= elapsed <= 1.50
