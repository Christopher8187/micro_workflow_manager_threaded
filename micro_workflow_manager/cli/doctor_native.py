"""Diagnose native session and job ownership from one read-only snapshot."""

from __future__ import annotations

import json
from datetime import datetime

from micro_workflow_manager.session_liveness import execution_session_liveness
from micro_workflow_manager.storage.execution_ownership import JobExecutionOwnerStorageMixin
from micro_workflow_manager.storage.execution_sessions import read_execution_sessions_snapshot
from micro_workflow_manager.storage.thread_overrides import read_thread_override_observation
from micro_workflow_manager.storage.clipboard_recovery import observe_clipboard_cleanup


def inspect_native_state(connection, graph_nodes, checks, warnings, errors, *, root):
    try:
        sessions = read_execution_sessions_snapshot(connection, running_only=True)
    except (RuntimeError, ValueError) as error:
        errors.append(f"invalid execution sessions: {error}")
        sessions = ()
    for session in sessions:
        label = f"{session['session_kind']} session {session['session_id']}"
        liveness = execution_session_liveness(session)
        if liveness['live']:
            checks.append(label + ' is live')
        else:
            warnings.append(label + ' requires recovery: ' + liveness['reason'] + '; run mwf recover')

    abandoned = []
    overdue = []
    for row in connection.execute('SELECT node_name, job_id, status, runtime_json FROM jobs'):
        node, job_id = row['node_name'], row['job_id']
        label = f'{node}/{job_id}'
        try:
            observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(connection, node, job_id)
            if row['status'] == 'running':
                if observed['state'] != 'active' or observed['session'] is None:
                    raise RuntimeError('running job has no exact execution owner')
                if not execution_session_liveness(observed['session'])['live']:
                    abandoned.append(label)
        except (RuntimeError, ValueError) as error:
            errors.append(f'invalid job ownership for {label}: {error}')
        try:
            runtime = json.loads(row['runtime_json'] or '{}')
            if not isinstance(runtime, dict):
                raise ValueError('runtime state must be an object')
            deadline_text = runtime.get('checkpoint_deadline_at')
            if runtime.get('state') == 'running' and deadline_text is not None:
                deadline = datetime.fromisoformat(deadline_text)
                if deadline < datetime.now(deadline.tzinfo):
                    overdue.append(label)
        except (TypeError, ValueError) as error:
            errors.append(f'invalid job runtime for {label}: {error}')
    if abandoned:
        warnings.append('running jobs have no live owner: ' + ', '.join(abandoned) + '; run mwf recover')
    if overdue:
        warnings.append('checkpoint deadlines are overdue: ' + ', '.join(overdue)
                        + '; inspect the job and verify the active scheduler heartbeat')

    override_nodes = {row[0] for row in connection.execute(
        'SELECT node_name FROM pending_node_thread_overrides UNION SELECT node_name FROM node_thread_overrides',
    )}
    unknown = sorted(override_nodes - graph_nodes)
    if unknown:
        warnings.append('runtime max_threads overrides reference nodes outside the graph: ' + ', '.join(unknown))
    for node in sorted(graph_nodes | override_nodes):
        try:
            read_thread_override_observation(connection, node)
        except (RuntimeError, ValueError) as error:
            errors.append(f'invalid runtime thread ownership for {node}: {error}')
    checks.append(f'checked {len(override_nodes)} native runtime max_threads override node(s)')
    limit = connection.execute('SELECT value FROM api_thread_limit WHERE singleton=1').fetchone()
    if limit is not None:
        if type(limit[0]) is not int or limit[0] < 1:
            errors.append('invalid aggregate API limit')
        else:
            checks.append(f'aggregate API limit: {limit[0]}')

    clipboard = observe_clipboard_cleanup(connection, root)
    checks.append(
        f'checked {len(clipboard.plans)} recorded native clipboard file operation(s)'
    )
    for plan in clipboard.plans:
        warnings.append(
            f"clipboard {plan.attempt['operation']} operation {plan.operation_id} "
            "has retained file work; inspect with mwf recover --dry-run"
        )
    errors.extend('invalid clipboard recovery state: ' + error for error in clipboard.errors)
    for path in clipboard.retained_material:
        warnings.append('unrecorded private clipboard material is retained: ' + path)
