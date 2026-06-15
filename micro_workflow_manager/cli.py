from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import shutil
import sys
from pathlib import Path

import networkx as nx

from .models import QUEUED
from .system import MicroWorkflow

MWF_FILE = ".mwf"
RUNNER_CHOICES = ["threaded", "direct", "prefect"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mwf")
    parser.add_argument("--runner", choices=RUNNER_CHOICES)

    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")

    graph_cmd = commands.add_parser("graph")
    graph_cmd.add_argument("path")
    graph_cmd.add_argument("--runner", choices=RUNNER_CHOICES)

    for name in ["clean", "wipe", "run", "runfrom"]:
        cmd = commands.add_parser(name)
        cmd.add_argument("node")
        if name in {"run", "runfrom"}:
            cmd.add_argument("--runner", choices=RUNNER_CHOICES)

    args = parser.parse_args(argv)

    try:
        if args.command == "init":
            return init_project()

        root = find_root()

        if args.command == "graph":
            return setup_graph(root, args.path, args.runner)

        workflow = load_workflow(root, args.runner)
        node = safe_node_name(args.node)
        require_node(workflow, node)

        if args.command == "clean":
            clean_node(root, workflow, node)
            print(f"Cleaned {node}")
            return 0

        if args.command == "wipe":
            clean_node(root, workflow, node, remove_input=True)
            print(f"Wiped {node}")
            return 0

        if args.command == "run":
            return run_node(root, workflow, node)

        if args.command == "runfrom":
            return run_from(root, workflow, node)

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    return 0


def init_project() -> int:
    path = Path.cwd() / MWF_FILE

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
        config["runner"] = runner

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

    workflow = MicroWorkflow(project_dir=root, runner=runner or config.get("runner", "threaded"))
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


def run_node(root: Path, workflow: MicroWorkflow, node: str) -> int:
    if not is_ready(workflow, node):
        print_not_ready(workflow, node)
        return 1

    graph_file = (root / read_config(root)["graph_path"]).resolve()
    autostart_nodes = autostart_closure(workflow, graph_file, [node])
    nodes = [node]

    if autostart_nodes:
        print("Detected autostarts to:", ", ".join(autostart_nodes))
        if not ask("Run all detected nodes sequentially?"):
            print("Stopped without running.")
            return 1
        nodes = topo_subset(workflow, {node, *autostart_nodes})

    blockers = direct_incomplete_inputs(workflow, set(nodes)) - set(workflow.graph_obj.predecessors(node))
    ignore_external = False

    if blockers:
        print("Detected incomplete nodes:", ", ".join(sorted(blockers)))
        print("These nodes directly lead into the requested run set.")
        if not ask("Run anyway?"):
            print("Stopped without running.")
            return 1
        ignore_external = True

    clean_node(root, workflow, node)
    for item in nodes:
        if item != node:
            clear_node(root, workflow, item)

    return run_nodes(workflow, nodes, node, ignore_external=ignore_external)


def run_from(root: Path, workflow: MicroWorkflow, node: str) -> int:
    nodes = [node] + descendants_in_order(workflow, node)
    graph_file = (root / read_config(root)["graph_path"]).resolve()
    autostart_nodes = autostart_closure(workflow, graph_file, nodes)
    extra_autostart_nodes = [item for item in autostart_nodes if item not in nodes]

    if extra_autostart_nodes:
        print("Detected autostarts outside the runfrom set:", ", ".join(extra_autostart_nodes))
        if not ask("Include these nodes and run all selected nodes sequentially?"):
            print("Stopped without running.")
            return 1
        nodes = topo_subset(workflow, {node, *nodes, *extra_autostart_nodes})

    blockers = direct_incomplete_inputs(workflow, set(nodes))
    ignore_external = False

    if blockers:
        print("Detected incomplete nodes:", ", ".join(sorted(blockers)))
        print("These nodes directly lead into the runfrom node set.")
        if not ask("Run anyway?"):
            print("Stopped without running.")
            return 1
        ignore_external = True

    clean_node(root, workflow, node)
    for child in nodes:
        if child != node:
            clear_node(root, workflow, child)

    return run_nodes(workflow, nodes, node, ignore_external=ignore_external)


def run_nodes(
    workflow: MicroWorkflow,
    nodes: list[str],
    start_node: str,
    ignore_external: bool = False,
) -> int:
    run_set = set(nodes)
    previous_allowed_run_nodes = workflow.allowed_run_nodes
    previous_autostart_mode = workflow.autostart_mode

    workflow.allowed_run_nodes = run_set
    workflow.autostart_mode = "queue"

    try:
        ensure_auto_start_job(workflow, start_node)

        if not workflow.storage.queued_jobs(start_node):
            print(f"No queued jobs for {start_node}")
            return 0

        if workflow.runner == "threaded":
            ran = workflow.run_concurrently(
                nodes=nodes,
                ready_check=lambda item: ready_for_run_set(
                    workflow,
                    item,
                    run_set,
                    ignore_external,
                ),
            )
        else:
            ran = []

            while True:
                ready = [
                    node
                    for node in nodes
                    if workflow.storage.queued_jobs(node)
                    and ready_for_run_set(workflow, node, run_set, ignore_external)
                ]

                if not ready:
                    break

                for node in ready:
                    workflow.run_node(node, ignore_readiness=True)
                    ran.append(node)

        workflow.finalize_ready_nodes()

        blocked = [node for node in nodes if workflow.storage.queued_jobs(node)]

        if blocked:
            print("Stopped before these queued nodes became ready:")
            for node in blocked:
                status = workflow.storage.get_node_status(node) or "missing"
                print(f"  {node}: {status}")
            return 1

        unfinished = [node for node in nodes if not workflow.node_complete(node)]

        if unfinished:
            print("These nodes did not complete:")
            for node in unfinished:
                status = workflow.storage.get_node_status(node) or "missing"
                job_count = len(workflow.storage.list_jobs(node))
                queued_count = len(workflow.storage.queued_jobs(node))
                print(f"  {node}: {status}, jobs={job_count}, queued={queued_count}")
            print("This usually means an upstream task did not create the expected downstream jobs.")
            return 1

        print("Ran:")
        for node in ran:
            print(f"  {node}")

        return 0

    finally:
        workflow.allowed_run_nodes = previous_allowed_run_nodes
        workflow.autostart_mode = previous_autostart_mode

def clean_node(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    remove_input: bool = False,
):
    node_dir = safe_node_dir(root, node)

    remove_dir(node_dir / "output")

    jobs_dir = node_dir / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)

    for job_dir in jobs_dir.iterdir():
        if not job_dir.is_dir() or not job_dir.name.isdigit():
            continue

        for item in job_dir.iterdir():
            if item.name in {"job.json", "input.json"}:
                continue
            remove_path(item)

        workflow.storage.set_job_status(node, int(job_dir.name), QUEUED)

    if remove_input:
        remove_dir(node_dir / "input")

    workflow.storage.init_node_folders(node)
    workflow.storage.set_node_status(node, QUEUED)


def clear_node(root: Path, workflow: MicroWorkflow, node: str):
    node_dir = safe_node_dir(root, node)
    remove_dir(node_dir / "output")
    remove_dir(node_dir / "jobs")
    workflow.storage.init_node_folders(node)
    workflow.storage.set_node_status(node, QUEUED)


def ensure_auto_start_job(workflow: MicroWorkflow, node: str):
    if workflow.storage.list_jobs(node):
        return

    if list(workflow.graph_obj.predecessors(node)):
        return

    task = workflow.nodes[node].main_task

    if task is not None and not task.required_params:
        workflow.start(node)


def ready_for_run_set(
    workflow: MicroWorkflow,
    node: str,
    run_set: set[str],
    ignore_external: bool,
) -> bool:
    for previous in workflow.graph_obj.predecessors(node):
        if previous not in run_set and ignore_external:
            continue

        if not workflow.node_complete(previous):
            return False

    return True


def direct_incomplete_inputs(workflow: MicroWorkflow, nodes: set[str]) -> set[str]:
    blockers = set()

    for node in nodes:
        for previous in workflow.graph_obj.predecessors(node):
            if previous not in nodes and not workflow.node_complete(previous):
                blockers.add(previous)

    return blockers


def descendants_in_order(workflow: MicroWorkflow, node: str) -> list[str]:
    descendants = nx.descendants(workflow.graph_obj, node)
    return [item for item in nx.topological_sort(workflow.graph_obj) if item in descendants]


def topo_subset(workflow: MicroWorkflow, nodes: set[str]) -> list[str]:
    return [node for node in nx.topological_sort(workflow.graph_obj) if node in nodes]


def autostart_closure(
    workflow: MicroWorkflow,
    graph_file: Path,
    start_nodes: list[str],
) -> list[str]:
    edges = scan_autostarts(graph_file.parent / "node_behavior")
    seen = set(start_nodes)
    found = set()
    queue = list(start_nodes)

    while queue:
        current = queue.pop(0)

        for target in sorted(edges.get(current, set())):
            if not workflow.graph_obj.has_edge(current, target):
                continue

            if target in seen:
                continue

            seen.add(target)
            found.add(target)
            queue.append(target)

    return topo_subset(workflow, found)


def scan_autostarts(directory: Path) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}

    for path in directory.glob("*.py"):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue

        tree = ast.parse(path.read_text(encoding="utf-8"))
        from_node = router_name(tree) or path.stem
        node_handles = simple_node_handle_assignments(tree)

        for node in ast.walk(tree):
            target = autostart_target(node, node_handles)
            if target is not None:
                result.setdefault(from_node, set()).add(target)

    return result


def router_name(tree: ast.AST) -> str | None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        func = node.func
        if isinstance(func, ast.Name) and func.id == "NodeRouter":
            pass
        elif isinstance(func, ast.Attribute) and func.attr == "NodeRouter":
            pass
        else:
            continue

        if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            return safe_node_name(node.args[0].value)

    return None


def simple_node_handle_assignments(tree: ast.AST) -> dict[str, str]:
    """Detect simple aliases like: target = ctx.node("next_node").

    This intentionally stays conservative. Dynamic node names are still checked
    at runtime by MicroWorkflow.allowed_run_nodes.
    """
    result: dict[str, str] = {}

    for node in ast.walk(tree):
        target_name = assigned_name(node)
        value = assigned_value(node)

        if target_name is None or value is None:
            continue

        target_node = node_call_target(value)
        if target_node is not None:
            result[target_name] = target_node

    return result


def assigned_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Assign) and len(node.targets) == 1:
        target = node.targets[0]
        if isinstance(target, ast.Name):
            return target.id

    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return node.target.id

    return None


def assigned_value(node: ast.AST) -> ast.AST | None:
    if isinstance(node, ast.Assign):
        return node.value

    if isinstance(node, ast.AnnAssign):
        return node.value

    return None


def node_call_target(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Call):
        return None

    if not isinstance(node.func, ast.Attribute) or node.func.attr != "node":
        return None

    if not node.args:
        return None

    target = node.args[0]
    if isinstance(target, ast.Constant) and isinstance(target.value, str):
        return safe_node_name(target.value)

    return None


def autostart_target(node: ast.AST, node_handles: dict[str, str]) -> str | None:
    if not isinstance(node, ast.Call):
        return None

    if not isinstance(node.func, ast.Attribute) or node.func.attr != "add":
        return None

    if not any(keyword.arg == "autostart" and is_true(keyword.value) for keyword in node.keywords):
        return None

    source = node.func.value

    direct_target = node_call_target(source)
    if direct_target is not None:
        return direct_target

    if isinstance(source, ast.Name):
        return node_handles.get(source.id)

    return None


def is_true(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is True


def ask(question: str) -> bool:
    try:
        answer = input(f"{question} [y/N] ").strip().lower()
    except EOFError:
        print("n")
        return False

    return answer in {"y", "yes"}


def print_not_ready(workflow: MicroWorkflow, node: str):
    print(f"{node} is not ready yet.")
    print("Previous nodes not finished:")

    for previous in workflow.graph_obj.predecessors(node):
        status = workflow.storage.get_node_status(previous) or "missing"
        if not workflow.node_complete(previous):
            print(f"  {previous}: {status}")


def is_ready(workflow: MicroWorkflow, node: str) -> bool:
    return workflow.node_ready(node)


def require_node(workflow: MicroWorkflow, node: str):
    if node not in workflow.graph_obj.nodes:
        raise RuntimeError(f"Unknown node: {node}")


def safe_node_name(name: str) -> str:
    if not name or name in {".", ".."}:
        raise ValueError("Invalid node name")

    if any(part in name for part in ["/", "\\", ".."]):
        raise ValueError(f"Unsafe node name: {name}")

    return name


def safe_node_dir(root: Path, node: str) -> Path:
    safe_node_name(node)
    base = (root / "node").resolve()
    path = (base / node).resolve()

    try:
        path.relative_to(base)
    except ValueError as error:
        raise ValueError(f"Unsafe node path: {path}") from error

    return path


def remove_dir(path: Path):
    if path.exists():
        if not path.is_dir():
            raise ValueError(f"Expected directory: {path}")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def remove_path(path: Path):
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def find_root(start: Path | None = None) -> Path:
    path = (start or Path.cwd()).resolve()

    for folder in [path, *path.parents]:
        if (folder / MWF_FILE).exists():
            return folder

    raise RuntimeError("Not an mwf project. Run: mwf init")


def read_config(root: Path) -> dict:
    path = root / MWF_FILE

    if not path.exists():
        raise RuntimeError("Not an mwf project. Run: mwf init")

    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
