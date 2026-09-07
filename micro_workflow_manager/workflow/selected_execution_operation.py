from __future__ import annotations

from ..runners.direct import DirectRunner
from ..storage.component_states import ComponentExecutionIncomplete
from ..storage.selected_execution import read_selected_execution_jobs
from .component_execution_operation import ComponentExecutionOperation


class SelectedExecutionOperation(ComponentExecutionOperation):
    """Retain ordered roots, Python values, and new causal work across repairs."""

    continues_after_nested_restarts = True
    live_admission = False
    preloaded_selection = True

    def __init__(self, workflow, jobs, context, *, scalar=False):
        jobs = tuple(jobs)
        if not jobs:
            raise ValueError('Selected execution requires at least one root job')
        addresses = tuple((job.node_name, job.job_id) for job in jobs)
        if len(addresses) != len(set(addresses)):
            raise ValueError('Selected execution requires distinct root jobs')
        component = context[1].get(jobs[0].node_name)
        if component is None or set(context[1].values()) != {component}:
            raise RuntimeError('Selected execution requires one exact admitted component')
        if any(context[1].get(job.node_name) != component for job in jobs):
            raise RuntimeError('Selected execution root is outside its admitted component')
        if scalar and len(jobs) != 1:
            raise ValueError('Scalar selected execution requires exactly one root job')

        super().__init__(workflow, component, context, None, None)
        self.roots = tuple(
            workflow.storage._read_session_job_roots(
                workflow.storage.db_connection(), context[0],
            )
        )
        if tuple((node, job_id) for node, job_id, _ in self.roots) != addresses:
            raise RuntimeError('Selected execution requires its exact ordered admitted roots')
        self.root_jobs = {
            (job.node_name, job.job_id): job
            for job in jobs
        }
        self.producing_identity = workflow.storage._read_component_producing_identity(
            workflow.storage.db_connection(), self.component,
        )
        self.scalar = scalar
        self.started = True
        self.selected_started = False
        self.frontier = ()
        self.epoch_jobs = set()

    def begin_epoch(self):
        if not self.selected_started:
            self.completion, identity = self.workflow.begin_selected_component_execution(
                self.component, self.roots, execution_context=self.execution_context,
                expected_identity=self.producing_identity,
            )
            if identity != self.producing_identity:
                raise RuntimeError('Selected execution changed its captured component alignment before start')
            self.selected_started = True
        self.completed = False
        self.frontier = self.read_frontier()
        self.epoch_jobs = (set(self.expectations) if self.stop_admission or self.replacement_epoch
                           else {(node, job_id) for node, job_id, _ in self.frontier})

    def observe_jobs(self):
        observed = self.read_frontier()
        self.frontier = tuple(row for row in observed if row[:2] in self.epoch_jobs)
        return {status: {node for node, _, observed in self.frontier if observed == status}
                for status in ('queued', 'running', 'failed')}

    def has_more_work(self):
        if self.stop_admission:
            return False
        observed = self.read_frontier()
        return any(status == 'queued' and (node, job_id) not in self.epoch_jobs
                   for node, job_id, status in observed)

    def read_frontier(self):
        return read_selected_execution_jobs(
            self.workflow.storage, self.execution_context, self.roots, self.producing_identity,
        )

    def restart_job_ids(self, node):
        replacements = super().restart_job_ids(node)
        if replacements is not None:
            return replacements
        queued = {job_id for member, job_id, status in self.frontier
                  if member == node and status == 'queued'}
        ordered = [job_id for member, job_id, _ in self.roots
                   if member == node and job_id in queued]
        return ordered + sorted(queued.difference(ordered))

    def make_runner(self, node, *, api_startup_lanes=None):
        if self.scalar and any(member == node.name and (member, job_id) not in self.values
                               for member, job_id, _ in self.roots):
            return DirectRunner()
        return super().make_runner(node, api_startup_lanes=api_startup_lanes)

    def load_source_job(self, node, job_id):
        key = (node, job_id)
        if key in self.root_jobs and key not in self.expectations and key not in self.values:
            return self.root_jobs[key]
        return self.workflow.storage.load_job(node, job_id)

    def finish_execution(self):
        self.frontier = self.read_frontier()
        unfinished = [(node, job_id) for node, job_id, status in self.frontier
                      if status not in ('done', 'skipped')]
        if unfinished:
            if self.stop_admission:
                return
            node, job_id = unfinished[0]
            raise ComponentExecutionIncomplete(
                f'Selected execution has unfinished causal job {node}/{job_id}'
            )
        self.workflow.refresh_component_status(set(self.component), allow_complete=True)
        self.workflow.storage.mark_selected_component_execution_ready(
            self.execution_context[0], self.completion,
        )
        self.completed = True


def run_selected_addresses(workflow, jobs, context, driver, *, scalar=False):
    """Run admitted roots and return their values in the admitted caller order."""
    jobs = tuple(jobs)
    operation = SelectedExecutionOperation(workflow, jobs, context, scalar=scalar)
    driver.drive(operation)
    results = [operation.values[(job.node_name, job.job_id)] for job in jobs]
    return results[0] if scalar else results


def run_selected_execution(workflow, node, jobs, context, driver, *, scalar=False):
    """Preserve the public single-node selected execution entry."""
    jobs = tuple(jobs)
    if any(job.node_name != node for job in jobs):
        raise ValueError('All selected jobs must belong to the requested node')
    return run_selected_addresses(workflow, jobs, context, driver, scalar=scalar)
