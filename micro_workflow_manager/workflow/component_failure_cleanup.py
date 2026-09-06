from ..models import FAILED, RUNNING
from ..storage.execution_terminal import TerminalOwnerExpectation


class ComponentFailureCleanupMixin:
    def _finalize_failed_component(
        self, component: set[str], error: BaseException, *, execution_context,
        publish_failure=True, operation=None,
    ) -> None:
        """Publish one failed SCC only after no local runner can still mutate it.

        A hard resource failure can occur after a handler wrote its output but
        before SQLite accepted the terminal update. Recover those output-backed
        completions first. Any remaining RUNNING rows are abandoned executions:
        every node runner has already been joined at this point, so mark them
        failed with an explicit recovery reason rather than leaving a terminal
        component that still appears to own live work.
        """
        component = set(component)
        recovery_error = repr(error)
        candidates = {} if operation is None else operation.abandoned_attempts()
        attempts, owners = {}, {}
        retained_attempts = None
        if (operation is not None and operation.task_parent is not None
                and set(operation.task_parent.component) == component):
            # A nested call has joined its own workers, while its parent,
            # ancestors, and peer tasks in this component may still be active.
            retained_attempts = set(operation.failed_attempts())

        # Normal handler failures already publish their terminal state before a
        # runner unwinds. Preserve the no-scan hot failure path in that common
        # case; recovery I/O is needed only when a joined component still has
        # stale RUNNING rows (for example after EMFILE interrupted publication).
        running_by_node = {}
        for node_name in sorted(component):
            try:
                running = self.storage.list_jobs(node_name, status=RUNNING)
            except BaseException:
                running = []
            if running:
                running_by_node[node_name] = running
            for job in running:
                try:
                    job_id = int(job['job_id'])
                    observed = self.storage.read_job_owner_observation(node_name, job_id)
                    owner = None if observed is None else observed['owner']
                    if (owner is None or owner['session_id'] != execution_context[0]
                            or owner['component'] != execution_context[1].get(node_name)
                            or set(owner['component']) != component
                            or observed['status'] != RUNNING
                            or observed['active_execution_id'] != owner['execution_id']
                            or observed['session']['status'] != 'running'):
                        continue
                    attempt = (node_name, job_id, owner['generation'], owner['execution_id'])
                    if retained_attempts is not None and attempt not in retained_attempts:
                        continue
                    candidate = candidates.get((node_name, job_id))
                    if candidate is not None and candidate != attempt:
                        continue
                    reservation = self.storage.get_component_reservation(owner['component'])
                    if reservation is None or reservation['session_id'] != owner['session_id']:
                        continue
                    attempts[(node_name, job_id)] = attempt
                    owners[(node_name, job_id)] = TerminalOwnerExpectation(
                        owner['session_id'], owner['component'], owner['job_instance_id'],
                    )
                    if operation is not None:
                        operation.record_cleanup_attempt(*attempt)
                except BaseException:
                    # Leave damaged or unreadable ownership for the session's
                    # active-claim exit check instead of guessing an execution.
                    pass

        if running_by_node:
            try:
                self.storage.reconcile_terminal_outputs(
                    component, execution_attempts=attempts, execution_owners=owners,
                )
            except BaseException:
                # Preserve the original component error. Remaining RUNNING rows
                # are handled below once descriptor/database pressure subsides.
                pass

        for node_name in sorted(component):
            try:
                running = self.storage.list_jobs(node_name, status=RUNNING)
            except BaseException:
                continue
            for job in running:
                try:
                    job_id = int(job['job_id'])
                    attempt = attempts.get((node_name, job_id))
                    if attempt is None:
                        continue
                    self.storage.finalize_job_execution(
                        node_name, job_id, attempt[2], attempt[3], FAILED,
                        expected_owner=owners[(node_name, job_id)],
                        error=recovery_error, recovered_after_component_abort=True,
                    )
                except BaseException:
                    # Failure cleanup is best effort and must never replace the
                    # original error being raised to the caller.
                    pass
        if publish_failure:
            self.mark_component_failed(component)
