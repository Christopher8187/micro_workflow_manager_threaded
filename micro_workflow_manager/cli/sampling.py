from __future__ import annotations

import secrets
import shlex

from micro_workflow_manager.component_readiness import calculate_component_readiness
from micro_workflow_manager.storage.component_states import read_component_states_snapshot
from micro_workflow_manager.storage.membership_observation import read_membership_change, require_reusable_membership_match
from micro_workflow_manager.storage.sample_planning import parse_sample_statuses, plan_sample

from .preview import load_preview
from .recovery_preview import print_recovery_preview
from .validation import require_node


def _require_sample_ready(preview, node):
    component = preview.component_for(node)
    parents = {preview.component_key(preview.component_for(parent))
               for parent in preview.component_predecessors(component)}
    states = read_component_states_snapshot(
        preview.storage.connection, sorted(parents),
        expected_shape=preview.topology.snapshot().shape_json, allow_missing=True,
    ).values()
    if (any(state is None for state in states)
            or calculate_component_readiness(
                (state['lifecycle'], state['stability'], state['instability_origin']) for state in states
            ) is None):
        raise RuntimeError(f'Hoeflein component {sorted(component)} is not ready for sampling')


def read_sample_plan(root, node, selectors, *, seed, statuses=(), expected_population=None,
                     require_ready=False, report_recovery=False):
    """Require two fresh SQL and filesystem observations before reporting a plan."""
    previous = None
    for attempt in range(3):
        preview = load_preview(root)
        try:
            require_node(preview, node)
            require_reusable_membership_match(read_membership_change(
                preview.storage.connection, preview.topology.snapshot(), (preview.component_id(node),),
            ))
            if report_recovery and attempt == 0:
                if print_recovery_preview(preview, quiet_if_empty=True):
                    raise RuntimeError('Damaged execution state requires recovery before sampling')
            plan = plan_sample(
                preview.storage.connection, root, preview.topology.snapshot(), node, selectors,
                seed=seed, statuses=statuses, expected_population=expected_population,
            )
            if require_ready and plan.selected_jobs:
                _require_sample_ready(preview, node)
        finally:
            preview.storage.close()
        identity = plan.planning_identity
        if previous == identity:
            return plan
        previous = identity
    raise RuntimeError('Sample population or input changed during preview; try the command again')


def print_sample_selection(selection, *, executing=False):
    print(('Sample selection' if executing else 'Sample plan') + f" for: {selection['node']}")
    print(f"  algorithm: {selection['algorithm']}")
    print(f"  seed: {selection['seed']}")
    print('  status filter: ' + (', '.join(selection['status_filter']) or 'all'))
    total = 0
    for node, member in selection['members'].items():
        selected = member['selected_job_ids']
        total += len(selected)
        selector = member['selector']
        value = str(selector['value']) + ('%' if selector['kind'] == 'percentage' else '')
        print(f'  {node} selector: {value}')
        print(f"  {node}: selected {len(selected)} of {member['population_count']} eligible jobs")
        print('    job IDs: ' + ' '.join(map(str, selected)))
        print(f"    population digest: {member['population_digest']}")
        print(f"    input digest: {member['input_digest']}")
    print(f'  selected: {total} jobs')
    print(f"  combined digest: {selection['combined_digest']}")
    if selection['full_starting_coverage']:
        print('  This sample has full coverage of the starting eligible population.')
    replay = ['mwf', 'run', selection['node'], 'sample', *selection['selectors'],
              '--seed', selection['seed'], '--expect-population', selection['combined_digest']]
    if selection['status_filter']:
        replay.extend(['--status', ','.join(selection['status_filter'])])
    print('  guarded replay: ' + shlex.join(replay))
    if not executing:
        print('  Read-only plan; user code was not loaded and no work was started.')


def sample_command(root, args, *, interrupt_observation=None):
    from .files import safe_node_name
    from .layout import ensure_runtime_layout
    from .startup_recovery import recover_before_mutation
    from .project import load_workflow
    from .run_selected import run_sampled_jobs
    from .interrupt_preflight import require_interrupt_preflight_unchanged, validate_loaded_interrupt_declarations

    node = safe_node_name(args.node)
    statuses = parse_sample_statuses(args.sample_status)
    seed = args.seed if args.seed is not None else secrets.token_hex(16)
    selectors = tuple(args.job_specs)
    plan = read_sample_plan(
        root, node, selectors, seed=seed, statuses=statuses,
        expected_population=args.expect_population, require_ready=False,
        report_recovery=args.plan,
    )
    if args.plan or not plan.selected_jobs:
        print_sample_selection(plan.manifest(None))
        if not plan.selected_jobs:
            print('No jobs selected; no work was started.')
        return 0
    if interrupt_observation is not None:
        require_interrupt_preflight_unchanged(root, interrupt_observation)
    recover_before_mutation(root)
    plan = read_sample_plan(
        root, node, selectors, seed=seed, statuses=statuses,
        expected_population=args.expect_population,
        require_ready=(interrupt_observation is None
                       or interrupt_observation.preflight.explicit_start_component is None),
    )
    if not plan.selected_jobs:
        print_sample_selection(plan.manifest(None))
        print('No jobs selected; no work was started.')
        return 0
    ensure_runtime_layout(root)
    if interrupt_observation is not None:
        require_interrupt_preflight_unchanged(root, interrupt_observation)
    workflow = load_workflow(root, args.runner)
    try:
        if interrupt_observation is not None:
            validate_loaded_interrupt_declarations(workflow, interrupt_observation)
        return run_sampled_jobs(
            root, workflow, node, selectors, seed=seed, statuses=statuses,
            expected_population=args.expect_population, stats=args.stats,
            stats_interval=args.stats_interval, monitor=args.monitor,
            monitor_interval=args.monitor_interval, keep_trace=args.keeptrace,
            interrupt_preflight=None if interrupt_observation is None else interrupt_observation.preflight,
        )
    finally:
        workflow.storage.close_database_connections()
