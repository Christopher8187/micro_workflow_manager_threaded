"""Print native recovery observations without loading a workflow runtime."""

from micro_workflow_manager.storage.preview_recovery import observe_abandoned_sessions
from micro_workflow_manager.storage.clipboard_recovery import observe_clipboard_cleanup


def _print_clipboard_recovery(connection, root):
    observation = observe_clipboard_cleanup(connection, root)
    for plan in observation.plans:
        state = plan.attempt['state']
        operation = plan.attempt['operation']
        if state == 'allocating':
            action = 'abort its incomplete allocation'
        elif state == 'prepared':
            action = 'restore its prior destination and record an aborted decision'
        else:
            action = 'retire its recorded private files'
        print(
            f"  clipboard {operation} operation {plan.operation_id}: would {action} "
            "after its operation lock is free"
        )
    for error in observation.errors:
        print(f'  clipboard recovery observation failed: {error}')
    for path in observation.retained_material:
        print('  unrecorded private clipboard material would be retained: ' + path)
    return observation


def print_recovery_preview(workflow, *, quiet_if_empty=True) -> int:
    clipboard = _print_clipboard_recovery(
        workflow.storage.connection, workflow.storage.project_dir,
    )
    observation = observe_abandoned_sessions(workflow.storage.connection, workflow.storage.project_dir)
    failures = len(observation['errors']) + len(clipboard.errors)
    for session in observation['sessions']:
        print(f"  {session['classification']} session {session['session_id']}: {session['liveness']['reason']}")
        if session['errors']:
            print('    recovery refused for this session; no actions can be proposed')
            for error in session['errors']:
                print(f'    recovery observation failed: {error}')
            failures += len(session['errors'])
            continue
        for job in session['jobs']:
            terminal = f" as {job['terminal_status']}" if job['terminal_status'] else ''
            print(f"    would {job['action']}{terminal}: {job['node']}/{job['job_id']} "
                  f"at generation {job['generation']}")
        for component in session['reservations']:
            print('    would release reservation: {' + ', '.join(component) + '}')
        for hold in session['holds']:
            print('    would release hold: {' + ', '.join(hold['component']) + f"}}, count={hold['count']}")
        print('    would record a failed session result during recovery')
    for error in observation['errors']:
        print(f'  recovery observation failed: {error}')
    if not quiet_if_empty:
        for session_id in observation['live_sessions']:
            print(f'  live session {session_id}: recovery leaves it active')
        if (not observation['sessions'] and not observation['errors']
                and not clipboard.plans and not clipboard.errors):
            print('No abandoned sessions need recovery.')
        print('Recovery preview only; no project state was changed.')
    return 1 if failures else 0
