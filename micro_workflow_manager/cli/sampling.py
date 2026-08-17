from __future__ import annotations

import hashlib
import heapq
import shlex
from dataclasses import dataclass
from pathlib import Path

from micro_workflow_manager.models import JOB_VALID_STATUSES
from micro_workflow_manager.system import MicroWorkflow


SAMPLE_ALGORITHM = "mwf.sample.v1"
_POPULATION_ALGORITHM = "mwf.sample.population.v1"


@dataclass(frozen=True)
class SampleCandidate:
    job_id: int
    status: str
    generation: int
    input_digest: str


@dataclass(frozen=True)
class SamplePlan:
    node: str
    requested_count: int
    seed: str
    statuses: tuple[str, ...]
    population_count: int
    population_digest: str
    selected_job_ids: tuple[int, ...]
    selected_input_digest: str

    def manifest(self) -> dict:
        return {
            "kind": "sample",
            "algorithm": SAMPLE_ALGORITHM,
            "seed": self.seed,
            "node": self.node,
            "status_filter": list(self.statuses) if self.statuses else ["all"],
            "population_count": self.population_count,
            "population_digest": self.population_digest,
            "requested_count": self.requested_count,
            "selected_job_ids": list(self.selected_job_ids),
            "selected_input_digest": self.selected_input_digest,
        }


def parse_sample_statuses(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    statuses = tuple(dict.fromkeys(part.strip().lower() for part in value.split(",") if part.strip()))
    if not statuses:
        raise RuntimeError("--status requires one or more comma-separated job statuses")
    invalid = sorted(set(statuses) - JOB_VALID_STATUSES)
    if invalid:
        choices = ", ".join(sorted(JOB_VALID_STATUSES))
        raise RuntimeError(f"Unknown sample status {', '.join(invalid)}. Choose from: {choices}")
    return tuple(sorted(statuses))


def parse_sample_count(job_specs: list[str]) -> int:
    if len(job_specs) != 1:
        raise RuntimeError("Use: mwf run <node> sample <count> [--seed <seed>] [--status <statuses>]")
    try:
        count = int(job_specs[0])
    except ValueError as error:
        raise RuntimeError("Sample count must be a positive integer") from error
    if count <= 0:
        raise RuntimeError("Sample count must be a positive integer")
    return count


def _input_digest(workflow: MicroWorkflow, node: str, job_id: int) -> str:
    path = workflow.storage.job_base_dir(node, job_id) / "input.json"
    digest = hashlib.sha256()
    if not path.is_file():
        digest.update(b"missing")
        return f"sha256:{digest.hexdigest()}"
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def _candidate_rows(
    workflow: MicroWorkflow,
    node: str,
    statuses: tuple[str, ...],
) -> list[SampleCandidate]:
    workflow.storage.db_mutation_barrier()
    sql = "SELECT job_id, status, generation FROM jobs WHERE node_name=?"
    params: list[object] = [node]
    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        sql += f" AND status IN ({placeholders})"
        params.extend(statuses)
    sql += " ORDER BY job_id"
    rows = workflow.storage.db_connection().execute(sql, params)
    return [
        SampleCandidate(
            job_id=int(row["job_id"]),
            status=str(row["status"]),
            generation=int(row["generation"]),
            input_digest=_input_digest(workflow, node, int(row["job_id"])),
        )
        for row in rows
    ]


def _population_digest(candidates: list[SampleCandidate]) -> str:
    digest = hashlib.sha256()
    digest.update(_POPULATION_ALGORITHM.encode("ascii") + b"\0")
    for candidate in candidates:
        digest.update(str(candidate.job_id).encode("ascii") + b"\0")
        digest.update(candidate.status.encode("ascii") + b"\0")
        digest.update(str(candidate.generation).encode("ascii") + b"\0")
        digest.update(candidate.input_digest.encode("ascii") + b"\n")
    return f"sha256:{digest.hexdigest()}"


def _score(seed: str, node: str, job_id: int) -> bytes:
    digest = hashlib.sha256()
    digest.update(SAMPLE_ALGORITHM.encode("ascii"))
    digest.update(b"\0")
    digest.update(seed.encode("utf-8"))
    digest.update(b"\0")
    digest.update(node.encode("utf-8"))
    digest.update(b"\0")
    digest.update(str(job_id).encode("ascii"))
    return digest.digest()


def _selected_input_digest(candidates: list[SampleCandidate], selected: set[int]) -> str:
    digest = hashlib.sha256()
    digest.update(b"mwf.sample.inputs.v1\0")
    for candidate in candidates:
        if candidate.job_id not in selected:
            continue
        digest.update(str(candidate.job_id).encode("ascii") + b"\0")
        digest.update(candidate.input_digest.encode("ascii") + b"\n")
    return f"sha256:{digest.hexdigest()}"


def plan_sample(
    workflow: MicroWorkflow,
    node: str,
    count: int,
    *,
    seed: str,
    statuses: tuple[str, ...] = (),
    expected_population: str | None = None,
) -> SamplePlan:
    if count <= 0:
        raise RuntimeError("Sample count must be a positive integer")
    if not seed or "\0" in seed:
        raise RuntimeError("Sample seed must be a non-empty string without NUL characters")

    candidates = _candidate_rows(workflow, node, statuses)
    population_digest = _population_digest(candidates)
    if expected_population is not None:
        normalized = expected_population.strip().lower()
        if not normalized.startswith("sha256:"):
            normalized = f"sha256:{normalized}"
        if normalized != population_digest:
            raise RuntimeError(
                "Sample population changed: expected "
                f"{normalized}, found {population_digest}. Run with --plan again."
            )
    if count > len(candidates):
        scope = "matching jobs" if statuses else "jobs"
        raise RuntimeError(
            f"Cannot sample {count} {scope} from {node}; population is {len(candidates)}"
        )

    ranked = heapq.nsmallest(
        count,
        candidates,
        key=lambda candidate: (_score(seed, node, candidate.job_id), candidate.job_id),
    )
    selected_ids = tuple(sorted(candidate.job_id for candidate in ranked))
    selected_set = set(selected_ids)
    return SamplePlan(
        node=node,
        requested_count=count,
        seed=seed,
        statuses=statuses,
        population_count=len(candidates),
        population_digest=population_digest,
        selected_job_ids=selected_ids,
        selected_input_digest=_selected_input_digest(candidates, selected_set),
    )


def print_sample_plan(plan: SamplePlan, *, executing: bool = False) -> None:
    print(("Sample selection" if executing else "Sample plan") + f" for: {plan.node}")
    print(f"  population: {plan.population_count} jobs")
    print(f"  population digest: {plan.population_digest}")
    print(f"  algorithm: {SAMPLE_ALGORITHM}")
    print(f"  seed: {plan.seed}")
    print("  status filter: " + (", ".join(plan.statuses) if plan.statuses else "all"))
    print(f"  selected: {plan.requested_count} jobs")
    print("  job IDs: " + " ".join(str(job_id) for job_id in plan.selected_job_ids))
    replay = [
        "mwf", "run", plan.node, "sample", str(plan.requested_count),
        "--seed", plan.seed,
        "--expect-population", plan.population_digest,
    ]
    if plan.statuses:
        replay.extend(["--status", ",".join(plan.statuses)])
    print("  guarded replay: " + shlex.join(replay))
    if not executing:
        print("  no state, jobs, inputs, outputs, or node folders were changed")
