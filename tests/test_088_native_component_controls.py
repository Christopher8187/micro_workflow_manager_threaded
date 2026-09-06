from __future__ import annotations

import time

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_database_connections()


@pytest.mark.parametrize('edges', [[('A', 'B')], [('A', 'B'), ('B', 'A')]])
def test_component_cannot_be_skipped_through_removed_public_operation(tmp_path, edges):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph(edges)
    router = NodeRouter('A', runner='direct')
    router.task(lambda ctx: 'finished')
    workflow.include_routers(router)
    job = workflow.add_job(None, 'A')
    try:
        before_nodes = workflow.storage.get_node_statuses(['A', 'B'])
        before_events = workflow.storage.read_job_events('A', job.job_id)
        with pytest.raises(AttributeError, match='skip_node'):
            workflow.skip_node('A')
        assert workflow.storage.get_node_statuses(['A', 'B']) == before_nodes
        assert workflow.storage.get_job_status('A', job.job_id) == 'queued'
        assert workflow.storage.read_job_events('A', job.job_id) == before_events
        assert workflow.storage.list_execution_sessions() == []
    finally:
        _close(workflow.storage)


def test_skipped_job_remains_successful_without_being_executed(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')
    executed = []

    @router.task
    def work(ctx):
        executed.append(ctx.job_id)
        return 'finished'

    workflow.include_routers(router)
    skipped = workflow.add_job(None, 'A')
    queued = workflow.add_job(None, 'A')
    workflow.storage.set_job_status('A', skipped.job_id, 'skipped')
    skipped_events = workflow.storage.read_job_events('A', skipped.job_id)
    try:
        workflow.run_node('A')
        assert executed == [queued.job_id]
        assert workflow.storage.get_job_status('A', skipped.job_id) == 'skipped'
        assert workflow.storage.get_job_status('A', queued.job_id) == 'done'
        assert workflow.node_complete('A')
        assert workflow.storage.read_job_owner_observation('A', skipped.job_id)['owner'] is None
        assert workflow.storage.read_job_events('A', skipped.job_id) == skipped_events
    finally:
        _close(workflow.storage)
