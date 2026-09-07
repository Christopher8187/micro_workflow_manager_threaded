"""Capture native sample populations and replay guards without changing storage."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

from micro_workflow_manager.models import JOB_VALID_STATUSES
from micro_workflow_manager.project_format import is_link_or_reparse_point
from micro_workflow_manager.storage.input_publication_files import check_local_path
from micro_workflow_manager.sample_selection import (
    SAMPLE_ALGORITHM, parse_sample_selectors, sample_count, select_sample_ids,
    starting_population_is_fully_selected,
)
from micro_workflow_manager.storage.execution_ownership import JobExecutionOwnerStorageMixin


def _digest(kind, value):
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    return 'sha256:' + hashlib.sha256(kind.encode('ascii') + b'\0' + encoded).hexdigest()


def parse_sample_statuses(value):
    if value is None:
        return ()
    statuses = tuple(sorted({part.strip().lower() for part in value.split(',') if part.strip()}))
    if not statuses:
        raise ValueError('--status requires one or more comma-separated job statuses')
    invalid = set(statuses) - JOB_VALID_STATUSES
    if invalid:
        raise ValueError('Unknown sample status: ' + ', '.join(sorted(invalid)))
    return statuses


def _ordinary_path(root, path):
    check_local_path(root, path)
    resolved = path.resolve()
    check_local_path(root, resolved)
    return resolved


def _read_file(root, path):
    path = _ordinary_path(root, path)
    if is_link_or_reparse_point(path) or not path.is_file():
        raise RuntimeError(f'Sample input is missing or is not an ordinary file: {path}')
    before = path.stat()
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    _ordinary_path(root, path)
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
    ):
        raise RuntimeError(f'Sample input changed while it was being read: {path}')
    return 'sha256:' + digest.hexdigest()


def _receiving_files(root, node):
    directory = _ordinary_path(root, root / 'node' / node / 'input')
    if is_link_or_reparse_point(directory):
        raise RuntimeError(f'Sample input directory is not ordinary: {directory}')
    if not directory.exists():
        return {}
    if not directory.is_dir():
        raise RuntimeError(f'Sample input directory is not a directory: {directory}')
    files = {}
    for parent, directories, names in os.walk(directory, followlinks=False):
        for name in directories:
            child = _ordinary_path(root, Path(parent) / name)
            if is_link_or_reparse_point(child):
                raise RuntimeError(f'Sample input directory is not ordinary: {child}')
        for name in names:
            child = _ordinary_path(root, Path(parent) / name)
            files[child.relative_to(directory).as_posix()] = _read_file(root, child)
    return files


@dataclass(frozen=True)
class SampleMember:
    node: str
    selector: tuple[str, int]
    starting_jobs: tuple[dict, ...]
    population: tuple[dict, ...]
    selected_job_ids: tuple[int, ...]
    starting_jobs_digest: str
    population_digest: str
    input_digest: str

    def manifest(self):
        return {
            'selector': {'kind': self.selector[0], 'value': self.selector[1]},
            'starting_jobs': [dict(candidate) for candidate in self.starting_jobs],
            'starting_jobs_digest': self.starting_jobs_digest,
            'population_count': len(self.population),
            'population': [dict(candidate) for candidate in self.population],
            'selected_job_ids': list(self.selected_job_ids),
            'population_digest': self.population_digest,
            'input_digest': self.input_digest,
        }


@dataclass(frozen=True)
class SamplePlan:
    node: str
    selectors: tuple[str, ...]
    seed: str
    statuses: tuple[str, ...]
    members: tuple[SampleMember, ...]
    shape: str
    combined_digest: str

    @property
    def planning_identity(self):
        """Compare eligible work and input while retaining a separate all-job baseline."""
        members = tuple(
            (member.node, member.selector, member.population, member.selected_job_ids,
             member.population_digest, member.input_digest)
            for member in self.members
        )
        return (self.node, self.selectors, self.seed, self.statuses, members,
                self.shape, self.combined_digest)

    @property
    def selected_jobs(self):
        return tuple((member.node, job_id) for member in self.members for job_id in member.selected_job_ids)

    @property
    def selected_job_instances(self):
        roots = []
        for member in self.members:
            instances = {
                candidate['job_id']: candidate['job_instance_id']
                for candidate in member.population
            }
            roots.extend(
                (member.node, job_id, instances[job_id])
                for job_id in member.selected_job_ids
            )
        return tuple(roots)

    @property
    def full_starting_coverage(self):
        return starting_population_is_fully_selected(
            {member.node: len(member.population) for member in self.members},
            {member.node: len(member.selected_job_ids) for member in self.members},
        )

    def manifest(self, sample_id):
        return {
            'kind': 'sample', 'algorithm': SAMPLE_ALGORITHM, 'sample_id': sample_id,
            'seed': self.seed, 'node': self.node, 'selectors': list(self.selectors),
            'status_filter': list(self.statuses), 'shape': self.shape,
            'members': {member.node: member.manifest() for member in self.members},
            'combined_digest': self.combined_digest,
            'full_starting_coverage': self.full_starting_coverage,
        }

    def admission_record(self, *, sample_id, component, expected_shape):
        """Validate and derive the one durable selection representation.

        Session admission calls this after holding the component reservation.
        Roots and JSON are returned together so a caller cannot persist one
        plan's roots beside another plan's replay metadata.
        """
        if not isinstance(sample_id, str) or not sample_id.strip():
            raise ValueError('Sample admission needs a nonempty sample ID')
        component = tuple(component)
        if (not isinstance(self.members, tuple)
                or not all(isinstance(member, SampleMember) for member in self.members)
                or not component or tuple(member.node for member in self.members) != component):
            raise ValueError('Sample plan members differ from the reserved component')
        if self.node not in component or self.shape != expected_shape:
            raise ValueError('Sample plan differs from its reserved component or graph shape')
        if (not isinstance(self.selectors, tuple)
                or not all(isinstance(token, str) for token in self.selectors)):
            raise ValueError('Sample plan selectors are invalid')
        parsed_selectors = parse_sample_selectors(self.node, component, self.selectors)
        if (not isinstance(self.statuses, tuple)
                or not all(isinstance(status, str) for status in self.statuses)
                or self.statuses != tuple(sorted(set(self.statuses)))
                or any(status not in JOB_VALID_STATUSES for status in self.statuses)):
            raise ValueError('Sample plan status filter is invalid')
        if not isinstance(self.seed, str) or not self.seed or '\0' in self.seed:
            raise ValueError('Sample plan seed is invalid')

        for member in self.members:
            if (not isinstance(member.selector, tuple) or len(member.selector) != 2
                    or not isinstance(member.selector[0], str)
                    or type(member.selector[1]) is not int
                    or member.selector != parsed_selectors[member.node]):
                raise ValueError('Sample member selector differs from the request')
            if not isinstance(member.starting_jobs, tuple) or not isinstance(member.population, tuple):
                raise ValueError('Sample member observations must be immutable')
            starting_ids = []
            starting_by_id = {}
            for candidate in member.starting_jobs:
                if not isinstance(candidate, dict) or set(candidate) != {
                    'job_id', 'job_instance_id', 'status', 'generation',
                }:
                    raise ValueError('Sample starting job observation is invalid')
                _validate_candidate_identity(candidate)
                job_id = candidate['job_id']
                starting_ids.append(job_id)
                starting_by_id[job_id] = candidate
            if starting_ids != sorted(starting_ids) or len(starting_ids) != len(set(starting_ids)):
                raise ValueError('Sample starting jobs must be distinct and ordered')
            if member.starting_jobs_digest != _digest(
                'mwf.sample.starting-jobs.v1', list(member.starting_jobs),
            ):
                raise ValueError('Sample starting job digest is invalid')
            population_ids = []
            identities = {}
            for candidate in member.population:
                if not isinstance(candidate, dict) or set(candidate) != {
                    'job_id', 'job_instance_id', 'status', 'generation', 'input_digest',
                }:
                    raise ValueError('Sample population candidate is invalid')
                _validate_candidate_identity(candidate)
                job_id = candidate['job_id']
                instance = candidate['job_instance_id']
                _require_digest(candidate['input_digest'], 'job input')
                starting = starting_by_id.get(job_id)
                if starting is None or any(
                    candidate[name] != starting[name]
                    for name in ('job_instance_id', 'status', 'generation')
                ):
                    raise ValueError('Sample population differs from its starting job observation')
                population_ids.append(job_id)
                identities[job_id] = instance.encode('ascii')
            if population_ids != sorted(population_ids) or len(population_ids) != len(set(population_ids)):
                raise ValueError('Sample population jobs must be distinct and ordered')
            expected_population_ids = [
                candidate['job_id'] for candidate in member.starting_jobs
                if not self.statuses or candidate['status'] in self.statuses
            ]
            if population_ids != expected_population_ids:
                raise ValueError('Sample population does not match its status-filtered starting jobs')
            if (not isinstance(member.selected_job_ids, tuple)
                    or any(type(job_id) is not int for job_id in member.selected_job_ids)):
                raise ValueError('Selected sample jobs must be immutable exact integers')
            count = sample_count(member.selector, len(population_ids))
            expected_selected = select_sample_ids(
                member.node, identities, count=count, seed=self.seed,
            )
            if member.selected_job_ids != expected_selected:
                raise ValueError('Selected sample jobs differ from the recorded population')
            if member.population_digest != _digest('mwf.sample.population.v1', list(member.population)):
                raise ValueError('Sample population digest is invalid')
            _require_digest(member.input_digest, 'member input')

        expected_combined = _digest('mwf.sample.drift.v1', {
            'shape': self.shape, 'status_filter': list(self.statuses),
            'members': {
                member.node: {
                    'population': member.population_digest,
                    'input': member.input_digest,
                }
                for member in self.members
            },
        })
        if self.combined_digest != expected_combined:
            raise ValueError('Combined sample drift digest is invalid')
        return self.selected_job_instances, self.manifest(sample_id)


def _require_digest(value, field):
    prefix = 'sha256:'
    if (not isinstance(value, str) or not value.startswith(prefix)
            or len(value) != len(prefix) + 64
            or any(character not in '0123456789abcdef' for character in value[len(prefix):])):
        raise ValueError(f'Sample {field} digest is invalid')


def _validate_candidate_identity(candidate):
    job_id = candidate['job_id']
    instance = candidate['job_instance_id']
    generation = candidate['generation']
    if (type(job_id) is not int or job_id < 1
            or not isinstance(instance, str) or len(instance) != 32
            or any(character not in '0123456789abcdef' for character in instance)
            or not isinstance(candidate['status'], str)
            or candidate['status'] not in JOB_VALID_STATUSES
            or type(generation) is not int or generation < 0):
        raise ValueError('Sample job observation identity is invalid')


def plan_sample(connection, root, snapshot, node, tokens, *, seed, statuses=(), expected_population=None):
    """Read one SQL snapshot; callers protect payloads or repeat the entire observation."""
    connection.execute('SAVEPOINT mwf_sample_population')
    try:
        return _plan_sample(connection, root, snapshot, node, tokens, seed=seed, statuses=statuses,
                            expected_population=expected_population)
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_sample_population')


def _plan_sample(connection, root, snapshot, node, tokens, *, seed, statuses, expected_population):
    component = next((members for members in snapshot.components if node in members), None)
    if component is None:
        raise ValueError('Sample start node is outside the graph: ' + node)
    selectors = parse_sample_selectors(node, component, tokens)
    if not seed or '\0' in seed:
        raise ValueError('Sample seed must be nonempty and contain no NUL characters')
    if set(statuses) - JOB_VALID_STATUSES:
        raise ValueError('Sample status filter contains an invalid job status')
    members = []
    for member in component:
        rows = connection.execute(
            'SELECT job_id, status, generation FROM jobs WHERE node_name=? ORDER BY job_id', (member,),
        ).fetchall()
        starting_jobs = []
        population = []
        input_files = _receiving_files(root, member)
        for row in rows:
            observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(connection, member, row['job_id'])
            if observed is None:
                raise RuntimeError(f'Sample population changed while it was read: {member}/{row["job_id"]}')
            starting = {
                'job_id': row['job_id'], 'job_instance_id': observed['job_instance_id'],
                'status': observed['status'], 'generation': observed['generation'],
            }
            starting_jobs.append(starting)
            if statuses and row['status'] not in statuses:
                continue
            path = root / 'node' / member / 'jobs' / str(row['job_id']) / 'input.json'
            population.append({**starting, 'input_digest': _read_file(root, path)})
        count = sample_count(selectors[member], len(population))
        selected = select_sample_ids(
            member, {candidate['job_id']: candidate['job_instance_id'].encode('ascii') for candidate in population},
            count=count, seed=seed,
        )
        if _receiving_files(root, member) != input_files:
            raise RuntimeError('Sample receiving input changed while it was read: ' + member)
        starting_jobs_digest = _digest('mwf.sample.starting-jobs.v1', starting_jobs)
        population_digest = _digest('mwf.sample.population.v1', population)
        input_digest = _digest('mwf.sample.inputs.v1', {
            'receiving_files': input_files,
            'jobs': {str(candidate['job_id']): candidate['input_digest'] for candidate in population},
        })
        members.append(SampleMember(
            member, selectors[member], tuple(starting_jobs), tuple(population), selected,
            starting_jobs_digest, population_digest, input_digest,
        ))
    combined = _digest('mwf.sample.drift.v1', {
        'shape': snapshot.shape_json, 'status_filter': list(statuses),
        'members': {member.node: {
            'population': member.population_digest,
            'input': member.input_digest,
        } for member in members},
    })
    if expected_population is not None:
        expected = expected_population.strip().lower()
        if not expected.startswith('sha256:'):
            expected = 'sha256:' + expected
        if combined != expected:
            raise RuntimeError(f'Sample population or input changed: expected {expected}, found {combined}. Run --plan again.')
    return SamplePlan(node, tuple(tokens), seed, tuple(statuses), tuple(members), snapshot.shape_json, combined)
