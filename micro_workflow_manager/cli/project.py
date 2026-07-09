from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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
            "version": 1,
            "graph_path": None,
            "runner": "threaded",
            "edges": [],
        },
    )
    print(f"Initialized {path}")
    return 0

def setup_graph(root: Path, graph_path: str, runner: str | None = None) -> int:
    path = (Path.cwd() / graph_path).resolve()

    if not path.exists():
        raise FileNotFoundError(path)

    config = read_config(root)
    config["graph_path"] = str(path.relative_to(root))

    if runner is not None:
        config["runner"] = normalize_workflow_runner(runner)

    write_json(root / MWF_FILE, config)
    workflow = load_workflow(root, runner)

    print(f"Graph set: {config['graph_path']}")
    print(f"Node folder: {root / 'node'}")
    print("Nodes:")

    for node in workflow.graph_obj.nodes:
        print(f"  {node}")

    return 0

def load_workflow(root: Path, runner: str | None = None) -> MicroWorkflow:
    config = read_config(root)
    graph_path = config.get("graph_path")

    if not graph_path:
        raise RuntimeError("No graph set. Run: mwf graph src/graph.py")

    graph_file = (root / graph_path).resolve()
    module = import_file(graph_file)
    edges = read_edges(module)

    workflow = MicroWorkflow(
        project_dir=root,
        runner=runner or config.get("runner", "threaded"),
        process_graph_path=graph_file,
    )
    workflow.graph(edges)
    workflow.include_node_dir(graph_file.parent / "node_behavior")
    return workflow

def import_file(path: Path):
    root = find_root()

    for item in [root, path.parent]:
        text = str(item)
        if text not in sys.path:
            sys.path.insert(0, text)

    spec = importlib.util.spec_from_file_location("mwf_user_graph", path)

    if spec is None or spec.loader is None:
        raise ImportError(path)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def read_edges(module) -> list[tuple[str, str]]:
    edges = getattr(module, "EDGES", None) or getattr(module, "edges", None)

    if not edges:
        raise RuntimeError("graph.py must define EDGES")

    result = []
    for edge in edges:
        if len(edge) != 2:
            raise RuntimeError(f"Invalid edge: {edge}")
        result.append((safe_node_name(edge[0]), safe_node_name(edge[1])))

    return result
