from __future__ import annotations

import shutil
import sys
from pathlib import Path
from types import ModuleType

from micro_workflow_manager.graph import normalize_edges
from micro_workflow_manager.models import QUEUED
from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.system import MicroWorkflow, normalize_workflow_runner

from .constants import MWF_FILE
from .extras.scaffold import ensure_project_sidecars
from .files import find_root, read_config, safe_node_name, write_json


def init_project() -> int:
    root = Path.cwd()
    path = root / MWF_FILE
    ensure_project_sidecars(root)

    if path.exists():
        print(f"Already initialized: {path}")
        return 0

    write_json(
        path,
        {
            "version": 2,
            "schema_version": CURRENT_STATE_SCHEMA_VERSION,
            "graph_path": None,
            "runner": "threaded",
            "edges": [],
        },
    )
    print(f"Initialized {path}")
    return 0


def setup_graph(
    root: Path,
    graph_path: str | None,
    runner: str | None = None,
    *,
    update: bool = False,
    dry_run: bool = False,
) -> int:
    """Record or explicitly synchronize the graph and node folders.

    Normal workflow commands never add or remove top-level node directories.
    This command is the single explicit synchronization point.
    """

    config = read_config(root)
    path = _resolve_graph_path(root, config, graph_path, update=update)
    module = import_file(path)
    edges = read_edges(module)
    expected_nodes = _nodes_from_edges(edges)

    old_nodes = _disk_node_names(root)
    stale_nodes = sorted(old_nodes - expected_nodes)
    new_nodes = sorted(expected_nodes - old_nodes)

    if dry_run:
        stored_edges = _stored_edges(config)
        target_runner = normalize_workflow_runner(runner or config.get("runner", "threaded"))
        print("Graph synchronization dry run")
        print(f"  graph path: {path.relative_to(root).as_posix()}")
        print(f"  runner: {target_runner}")
        print(f"  edges: {len(stored_edges)} stored -> {len(edges)} defined")
        print("  nodes to add: " + (", ".join(new_nodes) if new_nodes else "(none)"))
        print("  nodes to delete: " + (", ".join(stale_nodes) if stale_nodes else "(none)"))
        print("  edge list changed: " + ("yes" if stored_edges != edges else "no"))
        print("  no configuration or node folders were changed")
        return 0

    config["version"] = 2
    config["schema_version"] = CURRENT_STATE_SCHEMA_VERSION
    config["graph_path"] = path.relative_to(root).as_posix()
    config["edges"] = edges

    if runner is not None:
        config["runner"] = normalize_workflow_runner(runner)
    else:
        config["runner"] = normalize_workflow_runner(config.get("runner", "threaded"))

    # Store the new graph state before mounting routers. Router mounting may
    # materialize schemas/default jobs, and those writes must only target nodes
    # that have already passed the explicit synchronization step.
    write_json(root / MWF_FILE, config)
    _synchronize_node_folders(root, expected_nodes, stale_nodes)

    workflow = load_workflow(root, runner, require_synced=True)

    action = "Graph updated" if update or graph_path is None else "Graph set"
    print(f"{action}: {config['graph_path']}")
    print(f"Node folder: {root / 'node'}")
    if stale_nodes:
        print(f"Removed stale nodes: {', '.join(stale_nodes)}")
    if new_nodes:
        print(f"Added nodes: {', '.join(new_nodes)}")
    if not stale_nodes and not new_nodes:
        print("Node folders already matched the graph.")
    print("Nodes:")

    for node in workflow.graph_obj.nodes:
        print(f"  {node}")

    return 0


def load_workflow(
    root: Path,
    runner: str | None = None,
    *,
    require_synced: bool = True,
) -> MicroWorkflow:
    config = read_config(root)
    config_schema = config.get("schema_version")
    if type(config_schema) is int and config_schema > CURRENT_STATE_SCHEMA_VERSION:
        raise RuntimeError(
            f"Project state schema {config_schema} is newer than this MWF supports "
            f"({CURRENT_STATE_SCHEMA_VERSION}). Install a compatible newer version."
        )
    graph_path = config.get("graph_path")

    if not graph_path:
        raise RuntimeError("No graph set. Run: mwf graph src/graph.py")

    graph_file = resolve_stored_graph_path(root, graph_path)
    module = import_file(graph_file)
    edges = read_edges(module)

    if require_synced:
        require_graph_synced(root, config, edges)

    graph_nodes = _nodes_from_edges(edges)
    workflow = MicroWorkflow(
        project_dir=root,
        runner=runner or config.get("runner", "threaded"),
        process_graph_path=graph_file,
        persist_graph=False,
        initialize_node_folders=False,
    )
    workflow.graph(edges)
    workflow.include_node_dir(
        graph_file.parent / "node_behavior",
        allowed_node_names=graph_nodes,
    )
    return workflow


def require_graph_synced(root: Path, config: dict, edges: list[tuple[str, str]]):
    """Refuse implicit graph/node-folder changes during ordinary commands."""

    expected_nodes = _nodes_from_edges(edges)
    disk_nodes = _disk_node_names(root)
    stored_edges = _stored_edges(config)

    missing = sorted(expected_nodes - disk_nodes)
    stale = sorted(disk_nodes - expected_nodes)
    edges_changed = stored_edges != edges

    if not missing and not stale and not edges_changed:
        return

    details = ["Graph state is out of date; no node folders were changed automatically."]
    if missing:
        details.append(f"New graph nodes not on disk: {', '.join(missing)}")
    if stale:
        details.append(f"Stale or renamed node folders: {', '.join(stale)}")
    if edges_changed:
        details.append("The edges in graph.py differ from the last synchronized graph state.")
    details.append("Run: mwf graph --update")
    raise RuntimeError("\n".join(details))


def import_file(path: Path):
    root = find_root()

    for item in [root, path.parent]:
        text = str(item)
        if text not in sys.path:
            sys.path.insert(0, text)

    # Execute the current source directly instead of trusting timestamp-based
    # bytecode caches. Graph files are often renamed/edited and reloaded within
    # the same second by tests, editors, or automation.
    module = ModuleType("mwf_user_graph")
    module.__file__ = str(path)
    source = path.read_text(encoding="utf-8")
    exec(compile(source, str(path), "exec"), module.__dict__)
    return module


def read_edges(module) -> list[tuple[str, str]]:
    raw_edges = getattr(module, "EDGES", None)
    if raw_edges is None:
        raw_edges = getattr(module, "edges", None)
    if callable(raw_edges):
        raw_edges = raw_edges()

    result = []
    for start, end in normalize_edges(raw_edges):
        result.append((safe_node_name(start), safe_node_name(end)))
    return result



def resolve_stored_graph_path(root: Path, stored: str) -> Path:
    """Resolve a graph path written with either POSIX or Windows separators."""
    if not isinstance(stored, str) or not stored.strip():
        raise RuntimeError("No graph set. Run: mwf graph src/graph.py")
    normalized = stored.replace("\\", "/")
    parts = [part for part in normalized.split("/") if part not in {"", "."}]
    if any(part == ".." for part in parts):
        raise ValueError(f"Unsafe stored graph path: {stored}")
    path = root.joinpath(*parts).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as error:
        raise ValueError(f"Graph file must be inside the mwf project: {path}") from error
    return path


def resolve_configured_graph_path(root: Path, config: dict) -> Path:
    return resolve_stored_graph_path(root, config.get("graph_path"))

def _resolve_graph_path(
    root: Path,
    config: dict,
    graph_path: str | None,
    *,
    update: bool,
) -> Path:
    if graph_path is None:
        stored = config.get("graph_path")
        if not update:
            raise RuntimeError("Provide a graph path, or use: mwf graph --update")
        if not stored:
            raise RuntimeError("No graph set. Run: mwf graph src/graph.py")
        path = resolve_stored_graph_path(root, stored)
    else:
        # Accept either separator on either host. This matters when a command
        # copied from PowerShell is run under Linux/WSL, or vice versa.
        portable_input = graph_path.replace("\\", "/")
        candidate = Path(portable_input)
        path = (candidate if candidate.is_absolute() else Path.cwd() / candidate).resolve()

    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_file():
        raise ValueError(f"Graph path is not a file: {path}")

    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError(f"Graph file must be inside the mwf project: {path}") from error

    return path


def _stored_edges(config: dict) -> list[tuple[str, str]]:
    stored = config.get("edges", [])
    result: list[tuple[str, str]] = []

    if not isinstance(stored, list):
        return result

    for edge in stored:
        if not isinstance(edge, (list, tuple)) or len(edge) != 2:
            return []
        start, end = edge
        if not isinstance(start, str) or not isinstance(end, str):
            return []
        result.append((start, end))

    return result


def _nodes_from_edges(edges: list[tuple[str, str]]) -> set[str]:
    return {node for edge in edges for node in edge}


def _disk_node_names(root: Path) -> set[str]:
    node_root = root / "node"
    if not node_root.exists():
        return set()
    if not node_root.is_dir():
        raise ValueError(f"Expected node directory: {node_root}")
    return {path.name for path in node_root.iterdir() if path.is_dir()}


def _synchronize_node_folders(root: Path, expected_nodes: set[str], stale_nodes: list[str]):
    node_root = root / "node"
    node_root.mkdir(parents=True, exist_ok=True)

    for node in stale_nodes:
        path = node_root / safe_node_name(node)
        if path.exists():
            shutil.rmtree(path)

    storage = FileStorage(root)
    for node in sorted(expected_nodes):
        storage.init_node_folders(node)
        if storage.get_node_status(node) is None:
            storage.set_node_status(node, QUEUED)
