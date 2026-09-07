"""Regression for filtered sample-plan acquisition."""

from micro_workflow_manager.cli import sampling
from tests.test_090_component_session_settlement import _close
from tests.test_123_sample_history_and_filtered_coverage import _filtered_project


def test_sample_plan_ignores_excluded_status_drift_between_fresh_observations(
    tmp_path, monkeypatch,
):
    storage = _filtered_project(tmp_path, monkeypatch)
    original = sampling.load_preview
    loads = 0

    def load_after_excluded_change(root):
        nonlocal loads
        loads += 1
        if loads == 2:
            storage.set_job_status('A', 1, 'cancelled', reason='excluded acquisition change')
            storage.db_mutation_barrier()
        elif loads == 3:
            # A comparison that includes all starting jobs reaches this call and
            # fails to stabilize although the queued population never changed.
            storage.set_job_status('A', 1, 'failed', reason='excluded acquisition change')
            storage.db_mutation_barrier()
        return original(root)

    try:
        monkeypatch.setattr(sampling, 'load_preview', load_after_excluded_change)
        plan = sampling.read_sample_plan(
            tmp_path, 'A', ('100%',), seed='filtered-acquisition',
            statuses=('queued',),
        )

        assert loads == 2
        member = plan.members[0]
        assert [(job['job_id'], job['status']) for job in member.starting_jobs] == [
            (1, 'cancelled'), (2, 'queued'),
        ]
        assert [job['job_id'] for job in member.population] == [2]
        assert member.selected_job_ids == (2,)
    finally:
        _close(storage)
