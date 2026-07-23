from __future__ import annotations

from typing import Any

from micro_workflow_manager.models import (
    CANCELLED,
    DONE,
    FAILED,
    QUEUED,
    RUNNING,
    SKIPPED,
)


def _one_line(value: Any, *, limit: int = 240) -> str:
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _current_execution_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return the latest execution segment for one job.

    A manual restart or a fresh reset/run appends another ``started`` event.
    Filter inspection is intended to explain the current funnel, so old
    generations must not inflate later retry/fallback counts.
    """
    start_index: int | None = None
    for index, event in enumerate(events):
        if event.get("event") == "started":
            start_index = index
    if start_index is None:
        return []
    return events[start_index:]


def _filter_stages(workflow, node: str) -> list[dict[str, Any]]:
    mounted_node = workflow.nodes[node]
    main = mounted_node.main_task
    if main is None:
        return []

    stages: list[dict[str, Any]] = []

    def add_task_stages(kind: str, name: str, retries: int, repeats: int):
        attempts = retries + 1
        for attempt in range(1, attempts + 1):
            if kind == "main":
                role = "main"
            elif kind == name:
                role = "fallback"
            else:
                role = f"fallback {kind}"
            label = f"{role}: {name} — attempt {attempt}/{attempts}"
            if repeats > 1:
                label += f" ({repeats} repeats)"
            stages.append(
                {
                    "kind": kind,
                    "task": name,
                    "attempt": attempt,
                    "label": label,
                }
            )

    add_task_stages("main", main.name, main.retries, main.repeats)
    for fallback_name in mounted_node.fallback_order:
        fallback = mounted_node.fallbacks[fallback_name]
        add_task_stages(fallback_name, fallback.name, fallback.retries, fallback.repeats)
    return stages


def _stage_indexes_for_events(
    events: list[dict[str, Any]],
    stages: list[dict[str, Any]],
) -> set[int]:
    if not events or not stages:
        return set()

    indexes = {0}
    lookup = {
        (stage["kind"], stage["attempt"]): index
        for index, stage in enumerate(stages)
    }
    current_kind = "main"

    for event in events[1:]:
        event_name = event.get("event")
        if event_name == "fallback_started":
            fallback_name = event.get("fallback")
            if isinstance(fallback_name, str):
                current_kind = fallback_name
                index = lookup.get((current_kind, 1))
                if index is not None:
                    indexes.add(index)
        elif event_name == "retry_started":
            try:
                attempt = int(event.get("attempt"))
            except (TypeError, ValueError):
                continue
            index = lookup.get((current_kind, attempt))
            if index is not None:
                indexes.add(index)
    return indexes



def _filter_analysis(workflow, node: str) -> dict[str, Any]:
    storage = workflow.storage
    stages = _filter_stages(workflow, node)
    job_ids = list(storage.iter_job_ids(node))
    entered: list[set[int]] = [set() for _ in stages]
    latest_stage: dict[int, int] = {}
    statuses: dict[int, str] = {}
    transition_errors: dict[tuple[int, int], str] = {}

    lookup = {
        (stage["kind"], stage["attempt"]): index
        for index, stage in enumerate(stages)
    }

    for job_id in job_ids:
        status = storage.get_job_status(node, job_id) or QUEUED
        statuses[job_id] = status
        events = _current_execution_events(storage.read_job_events(node, job_id))
        indexes = _stage_indexes_for_events(events, stages)
        if not indexes:
            continue
        for index in indexes:
            entered[index].add(job_id)
        latest_stage[job_id] = max(indexes)

        current_kind = "main"
        for event in events[1:]:
            event_name = event.get("event")
            target_index = None
            if event_name == "fallback_started":
                fallback_name = event.get("fallback")
                if isinstance(fallback_name, str):
                    current_kind = fallback_name
                    target_index = lookup.get((current_kind, 1))
            elif event_name == "retry_started":
                try:
                    attempt = int(event.get("attempt"))
                except (TypeError, ValueError):
                    continue
                target_index = lookup.get((current_kind, attempt))
            if target_index is not None and target_index > 0:
                previous_error = event.get("previous_error")
                if previous_error:
                    transition_errors[(job_id, target_index - 1)] = str(previous_error)

    return {
        "stages": stages,
        "job_ids": job_ids,
        "entered": entered,
        "latest_stage": latest_stage,
        "statuses": statuses,
        "transition_errors": transition_errors,
    }


def _job_terminal_error(workflow, node: str, job_id: int) -> str:
    storage = workflow.storage
    output = storage.read_json(storage.output_file(node, job_id), default={})
    error = output.get("error") if isinstance(output, dict) else None
    if not error:
        runtime = storage.read_job_runtime(node, job_id)
        error = runtime.get("timeout_message") if isinstance(runtime, dict) else None
    return _one_line(error or "(error not recorded)")


def inspect_filter(workflow, node: str, *, stage_number: int | None = None) -> int:
    """Show the retry/fallback funnel or jobs crossing one stage boundary."""
    analysis = _filter_analysis(workflow, node)
    stages = analysis["stages"]
    job_ids = analysis["job_ids"]
    entered = analysis["entered"]
    latest_stage = analysis["latest_stage"]
    statuses = analysis["statuses"]

    if stage_number is not None:
        if not stages:
            raise RuntimeError(f"Node {node} has no mounted filter stages")
        if stage_number < 1 or stage_number > len(stages):
            raise RuntimeError(
                f"Stage must be between 1 and {len(stages)} for node {node}"
            )
        index = stage_number - 1
        print(f"Filter stage {stage_number}/{len(stages)} for node {node}")
        print(f"  {stages[index]['label']}")
        print("Jobs:")
        if index == len(stages) - 1:
            selected = sorted(
                job_id
                for job_id in entered[index]
                if latest_stage.get(job_id) == index
                and statuses.get(job_id) in {FAILED, CANCELLED}
            )
            if not selected:
                print("  (none)")
                return 0
            for job_id in selected:
                print(f"  {job_id}: {_job_terminal_error(workflow, node, job_id)}")
            return 0

        selected = sorted(
            job_id
            for job_id in entered[index + 1]
            if latest_stage.get(job_id) == index + 1
            and statuses.get(job_id) in {DONE, SKIPPED}
        )
        if not selected:
            print("  (none)")
            return 0
        transition_errors = analysis["transition_errors"]
        for job_id in selected:
            error = transition_errors.get((job_id, index), "(error not recorded)")
            print(f"  {job_id}: {_one_line(error)}")
        return 0

    print(f"Filter funnel for node {node}")
    print(f"  jobs discovered: {len(job_ids)}")
    print("  scope: latest execution segment for each job")
    print("  remaining = entered - jobs that passed at that stage")

    if not stages:
        print("Stages:")
        print("  (node has no mounted task)")
    else:
        rows: list[tuple[int, str, int, int, int]] = []
        for index, stage in enumerate(stages):
            stage_entered = len(entered[index])
            passed = sum(
                1
                for job_id in entered[index]
                if latest_stage.get(job_id) == index
                and statuses.get(job_id) in {DONE, SKIPPED}
            )
            remaining = stage_entered - passed
            rows.append((index + 1, stage["label"], stage_entered, passed, remaining))

        number_width = max(1, len(str(len(rows))))
        label_width = max(5, min(72, max(len(row[1]) for row in rows)))
        count_width = max(7, len(str(max((row[2] for row in rows), default=0))))
        print("Stages:")
        print(
            f"  {'#':>{number_width}}  {'stage':<{label_width}}  "
            f"{'entered':>{count_width}}  {'passed':>{count_width}}  {'remaining':>{count_width}}"
        )
        print(
            f"  {'-' * number_width}  {'-' * label_width}  "
            f"{'-' * count_width}  {'-' * count_width}  {'-' * count_width}"
        )
        for number, label, stage_entered, passed, remaining in rows:
            print(
                f"  {number:>{number_width}}  {label:<{label_width}}  "
                f"{stage_entered:>{count_width}}  {passed:>{count_width}}  {remaining:>{count_width}}"
            )

    not_started = [job_id for job_id in job_ids if job_id not in latest_stage]
    terminal_counts = {
        status: sum(1 for value in statuses.values() if value == status)
        for status in (DONE, SKIPPED, RUNNING, QUEUED, FAILED, CANCELLED)
    }
    print("Terminal/current state:")
    print(
        "  "
        + " ".join(
            f"{status}={count}"
            for status, count in terminal_counts.items()
            if count
        )
        if any(terminal_counts.values())
        else "  (no jobs)"
    )
    print(f"  not started in latest execution: {len(not_started)}")
    return 0
