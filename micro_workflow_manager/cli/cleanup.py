from __future__ import annotations

import shutil
from pathlib import Path

from micro_workflow_manager.models import QUEUED
from micro_workflow_manager.system import MicroWorkflow

from .files import remove_dir, remove_path, safe_node_dir, safe_node_name
from .graph_utils import component_topological_nodes
from .validation import require_node


def is_all_nodes_request(nodes: list[str]) -> bool:
    if any(item == "*" for item in nodes):
        return True
    try:
        visible_cwd_entries = sorted(
            path.name for path in Path.cwd().iterdir() if not path.name.startswith(".")
        )
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


def clean_node(root: Path, workflow: MicroWorkflow, node: str, remove_input: bool = False):
    node_dir = safe_node_dir(root, node)
    remove_dir(node_dir / "output")
    workflow.storage.delete_node_jobs(node, remove_payload=True)
    if remove_input:
        remove_dir(node_dir / "input")
    workflow.storage.init_node_folders(node)
    workflow.storage.set_node_status(node, QUEUED)


def reset_node_for_run(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    *,
    remove_parented_jobs: bool = False,
    mark_queued: bool = True,
):
    node_dir = safe_node_dir(root, node)
    remove_dir(node_dir / "output")

    for job_id in list(workflow.storage.list_job_ids(node)):
        metadata = workflow.storage.read_job_metadata(node, job_id)
        if remove_parented_jobs and metadata.get("parent") is not None:
            workflow.storage.delete_job(node, job_id, remove_payload=True)
            continue
        job_dir = workflow.storage.job_base_dir(node, job_id)
        remove_path(job_dir / "output.json")
        remove_path(job_dir / "files")
        workflow.storage.set_job_status(node, job_id, QUEUED)

    workflow.storage.init_node_folders(node)
    if mark_queued:
        workflow.storage.set_node_status(node, QUEUED)


def reset_job_for_run(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    job_id: int,
    *,
    mark_queued: bool = True,
):
    if not workflow.storage.job_exists(node, job_id):
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")
    job_dir = workflow.storage.job_base_dir(node, job_id)
    remove_path(job_dir / "output.json")
    remove_path(job_dir / "files")
    workflow.storage.set_job_status(node, job_id, QUEUED)
    if mark_queued:
        workflow.storage.set_node_status(node, QUEUED)


def clear_node(root: Path, workflow: MicroWorkflow, node: str):
    node_dir = safe_node_dir(root, node)
    remove_dir(node_dir / "output")
    workflow.storage.delete_node_jobs(node, remove_payload=True)
    workflow.storage.init_node_folders(node)
    workflow.storage.set_node_status(node, QUEUED)
