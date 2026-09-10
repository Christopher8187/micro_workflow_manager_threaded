"""Admit and freeze the exact explicitly named interrupt component."""

from __future__ import annotations

from contextlib import ExitStack
from time import sleep

from ..component_readiness import calculate_component_readiness, calculate_interrupt_sampled_resume_readiness
from ..models import now
from .admission_wait import resolve_admission_future


def explicit_interrupt_component(preflight):
    return None if preflight is None else preflight.explicit_start_component


def interrupt_request(workflow, component, *, expected_shape):
    with workflow.lock:
        if workflow.topology.graph_shape() != expected_shape:
            raise RuntimeError('Interrupt topology changed before admission')
        parents = sorted(workflow.component_predecessor_components(set(component)))
    observations = workflow.storage.read_component_states(parents, expected_shape=expected_shape)
    readiness = calculate_component_readiness(
        (state['lifecycle'], state['stability'], state['instability_origin'])
        for state in observations.values()
    )
    return {'direct_predecessors': parents, 'readiness_overridden': readiness is None}


def admit_interrupt_session(storage, session_id, arguments):
    pending = storage.admit_interrupt_execution(session_id, **arguments, _wait=False)
    return resolve_admission_future(
        pending,
        lambda: storage._read_interrupt_execution_admission(session_id, **arguments),
    )


def freeze_interrupt_session(
    workflow, session_id, component, *, expected_shape,
    sample_request=None, sample_admission=None,
):
    storage = workflow.storage
    with workflow.lock:
        snapshot = workflow.topology.snapshot()
        if snapshot.shape_json != expected_shape:
            raise RuntimeError('Interrupt topology changed before freeze')
        parents = sorted(workflow.component_predecessor_components(set(component)))
    if (sample_request is None) != (sample_admission is None):
        raise RuntimeError('Interrupt sample freeze requires its admitted sample request')
    while True:
        observations = storage.read_component_states(parents, expected_shape=expected_shape)
        readiness = calculate_component_readiness(
            ((state['lifecycle'], state['stability'], state['instability_origin'])
             for state in observations.values()),
            interrupt_start_origin=session_id,
        )
        session = storage.get_execution_session(session_id)
        if session['command'] in ('resume', 'resumefrom', 'resumebetween'):
            from ..storage.resume_preparation import validate_resume_job_owners

            state = storage.get_component_state(component)
            retained = validate_resume_job_owners(
                storage, {component: state}, expected_shape=expected_shape,
            )[component]
            if state['misaligned']:
                raise RuntimeError('Interrupt resume target became misaligned before freeze')
            if (state['lifecycle'] in ('sampled', 'failed')
                    and retained is not None and retained.lifecycle == 'sampled'):
                readiness = calculate_interrupt_sampled_resume_readiness(
                    retained.lineage,
                    ((state['lifecycle'], state['stability'], state['instability_origin'])
                     for state in observations.values()),
                    interrupt_start_origin=session_id,
                )
                if readiness is None:
                    raise RuntimeError('Interrupt resume has incompatible retained sampled lineage')
        frozen_at = now()
        plans = []

        def build_sample(connection):
            from ..storage.sample_planning import plan_sample

            plan = plan_sample(
                connection, storage.project_dir, snapshot, sample_admission.selection['node'],
                sample_request.selectors, seed=sample_request.seed,
                statuses=sample_request.statuses,
                expected_population=sample_request.expected_population,
            )
            plans.append(plan)
            return plan

        def readback():
            admission = storage.get_interrupt_execution_admission(session_id)
            if admission is None:
                raise RuntimeError('Interrupt admission disappeared while freezing input')
            if admission['state'] == 'admitted':
                return None
            frozen = admission['frozen_identity']
            if (admission['state'] != 'frozen' or frozen is None
                    or frozen['lineage'] != readiness[:2]
                    or frozen['frozen_at'] != frozen_at
                    or admission['readiness_overridden'] != readiness[2]
                    or admission['frozen_parent_states'] != observations):
                raise RuntimeError('Interrupt freeze readback differs from its input observation')
            if sample_request is not None:
                from ..storage.sample_history import read_sample_admission_history

                history = read_sample_admission_history(
                    storage, storage.db_connection(), session_id,
                    component=component, expected_shape=expected_shape,
                )
                if (len(plans) != 1 or history.plan != plans[0]
                        or history.sample_id != sample_admission.sample_id):
                    raise RuntimeError('Interrupt freeze lost its final sample observation')
            return admission

        with ExitStack() as locks:
            for node in sorted(component):
                locks.enter_context(storage.interprocess_lock(f'node-{node}-input'))
                locks.enter_context(storage.interprocess_lock(f'node-{node}-jobs'))
            arguments = {} if sample_request is None else {
                '_reserved_sample_builder': build_sample,
                'sample_id': sample_admission.sample_id,
            }
            pending = storage.freeze_interrupt_target(
                session_id, expected_shape=expected_shape, expected_parent_states=observations,
                successful_lineage=readiness[:2], readiness_overridden=readiness[2],
                frozen_at=frozen_at, _wait=False, **arguments,
            )
            settled = resolve_admission_future(pending, readback)
            if settled.interruption is not None:
                raise settled.interruption
            if settled.value is not None:
                return settled.value
        # Parent checkpoints and publication must remain free to proceed while
        # the child awaits every captured execution or a changed parent state.
        sleep(0.05)


def frozen_sample_admission(workflow, session_id, component, *, expected_shape):
    from ..storage.sample_history import read_sample_admission_history
    from .sample_admission import SampleAdmission

    history = read_sample_admission_history(
        workflow.storage, workflow.storage.db_connection(), session_id,
        component=component, expected_shape=expected_shape,
    )
    return SampleAdmission(history.sample_id, history.roots, history.plan.manifest(history.sample_id))


def frozen_component_readiness(workflow, component, execution_context=None):
    """Read the exact starting lineage established before input preparation."""
    context = workflow.execution_session_context if execution_context is None else execution_context
    if context is None:
        return None
    admission = workflow.storage.get_interrupt_execution_admission(context[0])
    if admission is None or tuple(admission['target_component']) != tuple(sorted(component)):
        return None
    frozen = admission['frozen_identity']
    if frozen is None:
        raise RuntimeError('Interrupt target has not reached its frozen input boundary')
    return (*frozen['lineage'], admission['readiness_overridden'])


def execution_component_readiness(workflow, component, observations, execution_context=None):
    frozen = frozen_component_readiness(workflow, component, execution_context)
    if frozen is not None:
        return frozen
    return calculate_component_readiness(
        (state['lifecycle'], state['stability'], state['instability_origin'])
        for state in observations.values()
    )
