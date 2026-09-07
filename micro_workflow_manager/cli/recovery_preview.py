"""Print native recovery observations without loading a workflow runtime."""

from micro_workflow_manager.storage.preview_recovery import observe_abandoned_sessions


def print_recovery_preview(workflow, *, quiet_if_empty=True) -> int:
    observation = observe_abandoned_sessions(workflow.storage.connection, workflow.storage.project_dir)
    failures = len(observation['errors'])
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
        if not observation['sessions'] and not observation['errors']:
            print('No abandoned sessions need recovery.')
        print('Recovery preview only; no project state was changed.')
    return 1 if failures else 0
