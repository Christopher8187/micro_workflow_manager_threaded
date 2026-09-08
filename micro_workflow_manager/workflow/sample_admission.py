"""Admit one native sample after freezing its component filesystem view."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from uuid import uuid4

from ..storage.sample_planning import plan_sample
from .admission_wait import resolve_admission_future


@dataclass(frozen=True)
class SampleRequest:
    selectors: tuple[str, ...]
    seed: str
    statuses: tuple[str, ...] = ()
    expected_population: str | None = None

    def __post_init__(self):
        if (not isinstance(self.selectors, tuple)
                or not self.selectors
                or not all(isinstance(token, str) for token in self.selectors)):
            raise ValueError('Sample selectors must be a nonempty tuple of text')
        if not isinstance(self.seed, str):
            raise ValueError('Sample seed must be text')
        if (not isinstance(self.statuses, tuple)
                or not all(isinstance(status, str) for status in self.statuses)):
            raise ValueError('Sample statuses must be a tuple of text')
        if self.expected_population is not None and not isinstance(self.expected_population, str):
            raise ValueError('Expected sample population must be text')


@dataclass(frozen=True)
class SampleAdmission:
    sample_id: str
    roots: tuple[tuple[str, int, str], ...]
    selection: dict
    interruption: BaseException | None = None

    @property
    def addresses(self):
        return tuple((node, job_id) for node, job_id, _ in self.roots)


def admit_sample_execution_session(
    workflow,
    *,
    session_id,
    command,
    start_node,
    snapshot,
    selected_components,
    started_at,
    hostname,
    pid,
    process_identity,
    details,
    request,
):
    """Reserve, observe, select, and persist one sample as one writer decision."""
    if not isinstance(request, SampleRequest):
        raise TypeError('sample_request must be a SampleRequest')
    components = [tuple(component) for component in selected_components]
    if len(components) != 1 or start_node not in components[0]:
        raise ValueError('Sampling admission requires exactly its starting component')
    component, = components
    storage = workflow.storage
    sample_id = uuid4().hex
    plans = []

    def build(connection):
        plan = plan_sample(
            connection, storage.project_dir, snapshot, start_node, request.selectors,
            seed=request.seed, statuses=request.statuses,
            expected_population=request.expected_population,
        )
        plans.append(plan)
        return plan

    with ExitStack() as locks:
        for node in sorted(component):
            locks.enter_context(storage.interprocess_lock(f'node-{node}-input'))
            locks.enter_context(storage.interprocess_lock(f'node-{node}-jobs'))
        pending = storage.create_execution_session(
            session_id, session_kind='main', command=command,
            start_component=component, selected_components=components, selected_jobs=(),
            started_at=started_at, hostname=hostname, pid=pid,
            process_identity=process_identity, details=details,
            sample_id=sample_id, expected_shape=snapshot.shape_json,
            _reserved_sample_builder=build,
            _wait=False,
        )

        def readback():
            if len(plans) != 1:
                if storage.get_execution_session(session_id) is None:
                    return None
                raise RuntimeError('Committed sample admission lost its captured plan')
            plan = plans[0]
            roots, selection = plan.admission_record(
                sample_id=sample_id, component=component, expected_shape=snapshot.shape_json,
            )
            return storage._read_execution_session_admission(
                session_id, session_kind='main', command=command,
                start_component=component, selected_components=components,
                selected_jobs=[(node, job_id) for node, job_id, _ in roots],
                expected_job_instances=roots, started_at=started_at,
                hostname=hostname, pid=pid, process_identity=process_identity,
                details={**details, 'selection': selection}, reserved=True,
                expected_shape=snapshot.shape_json,
            )

        settled = resolve_admission_future(pending, readback)
        plan, = plans
        return SampleAdmission(
            sample_id, plan.selected_job_instances, plan.manifest(sample_id),
            settled.interruption,
        )
