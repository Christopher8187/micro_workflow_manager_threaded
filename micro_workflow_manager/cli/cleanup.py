from __future__ import annotations

import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from micro_workflow_manager.models import QUEUED
from micro_workflow_manager.system import MicroWorkflow

from .files import remove_dir, remove_path, safe_node_dir, safe_node_name
from .graph_utils import component_topological_nodes, expand_to_components
from .validation import require_node


def is_all_nodes_request(nodes: list[str]) -> bool:
    if any(item == "*" for item in nodes):
        return True
    try:
        visible_cwd_entries = sorted(path.name for path in Path.cwd().iterdir() if not path.name.startswith("."))
    except OSError:
        return False
    return len(nodes) > 1 and sorted(nodes) == visible_cwd_entries


def resolve_node_targets(workflow: MicroWorkflow, requested: list[str]) -> list[str]:
    if not requested:
        raise RuntimeError("No node specified")
    if is_all_nodes_request(requested):
        return component_topological_nodes(workflow)
    seen: set[str] = set()
    result: list[str] = []
    for item in requested:
        node = safe_node_name(item)
        require_node(workflow, node)
        if node not in seen:
            seen.add(node)
            result.append(node)
    return result


def resolve_reset_targets(workflow: MicroWorkflow, requested: list[str]) -> list[str]:
    """Resolve the lifecycle scope used by ``mwf reset``.

    A singleton quotient-DAG vertex is reset by itself.  Naming any member of a
    nontrivial strongly connected (Hoeflein) component resets every member of
    that component.
    """
    selected_nodes = resolve_node_targets(workflow, requested)
    selected: set[str] = set()
    for node in selected_nodes:
        component = workflow.component_id(node)
        if len(component) == 1:
            selected.add(node)
        else:
            selected.update(component)
    return component_topological_nodes(workflow, selected)


def selected_reset_scope_labels(workflow: MicroWorkflow, nodes: list[str]) -> list[str]:
    seen: set[tuple[str, ...]] = set()
    labels: list[str] = []
    for node in nodes:
        component = workflow.component_id(node)
        if component in seen:
            continue
        seen.add(component)
        if len(component) == 1:
            labels.append(f"DAG node {component[0]}")
        else:
            labels.append("{" + ", ".join(component) + "}")
    return labels


def resolve_component_targets(workflow: MicroWorkflow, requested: list[str]) -> list[str]:
    """Resolve cleanup targets as whole Hoeflein components.

    Naming any member selects every vertex in that component. Repeated members
    or multiple names from the same component are deduplicated in quotient-DAG
    order.
    """
    selected_nodes = resolve_node_targets(workflow, requested)
    expanded = expand_to_components(workflow, set(selected_nodes))
    return component_topological_nodes(workflow, expanded)


def selected_component_labels(workflow: MicroWorkflow, nodes: list[str]) -> list[str]:
    seen: set[tuple[str, ...]] = set()
    labels: list[str] = []
    for node in nodes:
        component = workflow.component_id(node)
        if component in seen:
            continue
        seen.add(component)
        labels.append("{" + ", ".join(component) + "}")
    return labels


def clean_node(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    remove_input: bool = False,
    *,
    keep_trace: bool = False,
):
    node_dir = safe_node_dir(root, node)
    remove_dir(node_dir / "output")
    workflow.storage.delete_node_jobs(
        node,
        remove_payload=True,
        preserve_events=keep_trace,
    )
    if remove_input:
        remove_dir(node_dir / "input")
    workflow.storage.init_node_folders(node)
    workflow.storage.set_node_status(node, QUEUED)


def job_producer_component(workflow: MicroWorkflow, metadata: dict) -> tuple[str, ...] | None:
    stored = metadata.get("producer_component")
    if isinstance(stored, tuple) and stored:
        return stored
    parent = metadata.get("parent")
    if not isinstance(parent, dict):
        return None
    from_node = parent.get("from_node")
    if isinstance(from_node, str) and from_node in workflow.graph_obj:
        return workflow.component_id(from_node)
    return None


def delete_jobs_generated_by_components(
    workflow: MicroWorkflow,
    producer_components: set[tuple[str, ...]],
    *,
    keep_trace: bool = False,
) -> dict[str, int]:
    """Delete jobs whose immediate producer belongs to a selected component.

    The target node is irrelevant. This removes same-component spawn jobs and
    downstream DAG jobs produced by the fresh run set while preserving jobs
    produced by components outside that set.
    """
    selected_by_node: dict[str, list[int]] = {}
    # One SQLite snapshot replaces one metadata query per project job. This is
    # especially important before a large Hoeflein rerun, where cleanup happens
    # before the first component pump is allowed to start.
    for metadata in workflow.storage.list_job_parent_metadata():
        if job_producer_component(workflow, metadata) not in producer_components:
            continue
        selected_by_node.setdefault(metadata["node_name"], []).append(
            metadata["job_id"]
        )

    removed: dict[str, int] = {}
    for node_name, job_ids in selected_by_node.items():
        count = workflow.storage.delete_jobs_batch(
            node_name,
            job_ids,
            remove_payload=True,
            preserve_events=keep_trace,
        )
        if count:
            removed[node_name] = count
    # Cleanup runs before workers start and owns the active-run slot, so it is
    # safe to restore deterministic tail allocation for jobs that are about to
    # be recreated by the same selected producer components.
    for node_name in removed:
        workflow.storage.rewind_job_sequence_to_available(node_name)
    return removed


def _remove_job_artifacts_batch(
    workflow: MicroWorkflow,
    node: str,
    job_ids: list[int],
) -> None:
    """Remove independent per-job outputs concurrently on slow filesystems."""
    def remove_one(job_id: int) -> None:
        job_dir = workflow.storage.job_base_dir(node, job_id)
        remove_path(job_dir / "output.json")
        remove_path(job_dir / "files")

    if os.name != "nt" or len(job_ids) < 8:
        for job_id in job_ids:
            remove_one(job_id)
        return
    with ThreadPoolExecutor(
        max_workers=min(32, len(job_ids)),
        thread_name_prefix="mwf-fresh-cleanup",
    ) as executor:
        list(executor.map(remove_one, job_ids))


def _remove_reset_artifacts_batch(
    root: Path,
    workflow: MicroWorkflow,
    nodes: list[str],
) -> None:
    """Clear node and job result artifacts with one bounded filesystem sweep."""
    output_dirs: list[Path] = []
    job_dirs: list[Path] = []
    for node in nodes:
        node_dir = safe_node_dir(root, node)
        output_dir = node_dir / "output"
        if output_dir.exists() and not output_dir.is_dir():
            raise ValueError(f"Expected directory: {output_dir}")
        output_dirs.append(output_dir)
        jobs_dir = workflow.storage.jobs_dir(node)
        try:
            with os.scandir(jobs_dir) as entries:
                job_dirs.extend(
                    Path(entry.path)
                    for entry in entries
                    if entry.name.isdigit() and entry.is_dir(follow_symlinks=False)
                )
        except FileNotFoundError:
            pass

    def clear_output_dir(path: Path) -> None:
        shutil.rmtree(path, ignore_errors=True)

    def clear_job_dir(path: Path) -> None:
        remove_path(path / "output.json")
        remove_path(path / "files")

    tasks = [(clear_output_dir, path) for path in output_dirs]
    tasks.extend((clear_job_dir, path) for path in job_dirs)
    if len(tasks) < 8:
        for operation, path in tasks:
            operation(path)
    else:
        # Reset is filesystem-bound on Windows.  One shared executor for the
        # entire selected scope avoids repeatedly constructing pools per node.
        workers = min(64 if os.name == "nt" else 32, len(tasks))
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="mwf-reset-cleanup",
        ) as executor:
            futures = [executor.submit(operation, path) for operation, path in tasks]
            for future in futures:
                future.result()

    for node in nodes:
        workflow.storage.init_node_folders(node)


def reset_nodes_for_run(
    root: Path,
    workflow: MicroWorkflow,
    nodes: list[str],
    *,
    mark_queued: bool = True,
    keep_trace: bool = False,
) -> int:
    """Reset a complete DAG-node/component scope without per-job round trips."""
    unique_nodes = list(dict.fromkeys(nodes))
    _remove_reset_artifacts_batch(root, workflow, unique_nodes)
    return workflow.storage.reset_nodes_for_run_batch(
        unique_nodes,
        mark_nodes_queued=mark_queued,
        preserve_events=keep_trace,
    )


def reset_component_jobs_for_fresh_run(
    root: Path,
    workflow: MicroWorkflow,
    component: set[str],
    *,
    preserve_external_parent_jobs: bool,
    keep_trace: bool,
) -> None:
    """Reset the durable jobs that remain in one selected component.

    The explicitly selected start component is always a true fresh run: every
    remaining job is requeued even when it was created by an external
    predecessor. Descendant merge components in ``runfrom`` are different. They
    preserve jobs produced by unselected branches while root/default jobs are
    reset and selected-producer jobs have already been deleted for recreation.
    """
    for node in component:
        node_dir = safe_node_dir(root, node)
        has_preserved_parent_jobs = False
        reset_job_ids: list[int] = []
        for metadata in workflow.storage.list_job_parent_metadata([node]):
            job_id = metadata["job_id"]
            producer = job_producer_component(workflow, metadata)
            if preserve_external_parent_jobs and producer is not None:
                has_preserved_parent_jobs = True
                continue
            reset_job_ids.append(job_id)

        _remove_job_artifacts_batch(workflow, node, reset_job_ids)
        workflow.storage.reset_jobs_for_run_batch(
            node,
            reset_job_ids,
            preserve_events=keep_trace,
        )

        # A start component is fully reset, so its aggregate output must also be
        # cleared. A descendant merge may still contain completed jobs from an
        # unselected producer; preserve aggregate output in that case.
        if not has_preserved_parent_jobs:
            remove_dir(node_dir / "output")
        workflow.storage.init_node_folders(node)
        workflow.storage.set_node_status(node, QUEUED)


def prepare_fresh_components(
    root: Path,
    workflow: MicroWorkflow,
    components: list[set[str]],
    *,
    keep_trace: bool = False,
) -> dict[str, int]:
    """Prepare ``run``/``runfrom`` with a fully fresh start component.

    Jobs generated by selected producer components are deleted so they can be
    recreated deterministically. The first component is the user's explicit
    selection and all of its remaining jobs are reset. Later components preserve
    jobs from unselected incoming branches.
    """
    selected = {workflow.component_id(component) for component in components}
    if not keep_trace:
        # Include journals left orphaned by an earlier --keeptrace deletion.
        # They are still potentially changed when these producers recreate the
        # same node/job IDs, even though no current job row identifies them.
        workflow.storage.clear_job_events_produced_by_components(selected)
    removed = delete_jobs_generated_by_components(
        workflow,
        selected,
        keep_trace=keep_trace,
    )
    for index, component in enumerate(components):
        reset_component_jobs_for_fresh_run(
            root,
            workflow,
            component,
            preserve_external_parent_jobs=index > 0,
            keep_trace=keep_trace,
        )
    return removed


def reset_node_for_run(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    *,
    remove_parented_jobs: bool = False,
    mark_queued: bool = True,
    keep_trace: bool = False,
):
    """Reset one node, retaining the legacy parent-deletion option."""
    if not remove_parented_jobs:
        return reset_nodes_for_run(
            root,
            workflow,
            [node],
            mark_queued=mark_queued,
            keep_trace=keep_trace,
        )

    node_dir = safe_node_dir(root, node)
    remove_dir(node_dir / "output")
    retained_job_ids: list[int] = []
    for metadata in workflow.storage.list_job_parent_metadata([node]):
        job_id = metadata["job_id"]
        if metadata.get("parent") is not None:
            workflow.storage.delete_job(
                node,
                job_id,
                remove_payload=True,
                preserve_events=keep_trace,
            )
        else:
            retained_job_ids.append(job_id)
    _remove_job_artifacts_batch(workflow, node, retained_job_ids)
    workflow.storage.reset_jobs_for_run_batch(
        node,
        retained_job_ids,
        preserve_events=keep_trace,
    )
    workflow.storage.init_node_folders(node)
    if mark_queued:
        workflow.storage.set_node_status(node, QUEUED)
    return len(retained_job_ids)


def reset_job_for_run(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    job_id: int,
    *,
    mark_queued: bool = True,
    keep_trace: bool = False,
):
    if not workflow.storage.job_exists(node, job_id):
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")
    job_dir = workflow.storage.job_base_dir(node, job_id)
    remove_path(job_dir / "output.json")
    remove_path(job_dir / "files")
    if not keep_trace:
        workflow.storage.clear_job_events(node, [job_id])
    workflow.storage.set_job_status(node, job_id, QUEUED)
    if mark_queued:
        workflow.storage.set_node_status(node, QUEUED)


def clear_node(root: Path, workflow: MicroWorkflow, node: str):
    node_dir = safe_node_dir(root, node)
    remove_dir(node_dir / "output")
    workflow.storage.delete_node_jobs(node, remove_payload=True)
    workflow.storage.init_node_folders(node)
    workflow.storage.set_node_status(node, QUEUED)
