from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import shutil
import sys
import textwrap
from pathlib import Path

import networkx as nx

from .models import QUEUED, RUNNING
from .system import MicroWorkflow, normalize_workflow_runner

MWF_FILE = ".mwf"
RUNNER_CHOICES = ["threaded", "process", "direct", "thread", "processes", "process_pool", "processpool"]
COMMAND_NAMES = ["init", "graph", "clean", "reset", "wipe", "run", "runfrom"]

HELP_EPILOG = """
Common help commands:
  mwf --help
  mwf init --help
  mwf graph --help
  mwf clean --help
  mwf reset --help
  mwf wipe --help
  mwf run --help
  mwf runfrom --help

Command descriptions:
  mwf --describe init
  mwf --describe graph
  mwf --describe clean
  mwf --describe reset
  mwf --describe wipe
  mwf --describe run
  mwf --describe runfrom

Typical flow:
  mwf init
  mwf graph src/graph.py
  mwf run start_node
  mwf run start_node job 1 3 8-10
  mwf runfrom start_node

Cleaning:
  mwf clean node_name   # clear output/files/jobs for one node, keep input files
  mwf clean *           # clean every node in the graph
  mwf reset node_name   # clear outputs and rerun existing jobs, keep job definitions
  mwf reset *           # reset every node in the graph
  mwf wipe node_name    # clean one node and remove its input folder too
  mwf wipe *            # wipe every node in the graph
"""

COMMAND_DESCRIPTIONS = {
    "init": """
init creates the project marker file .mwf in the current directory.

Code context:
  init does not import your graph.py or node_behavior files.

File-system context:
  writes .mwf with the default runner set to threaded
  stores graph_path as null until you run mwf graph <path>
  does not create node folders until a graph is loaded

Use when:
  starting a new micro-workflow project folder
""",
    "graph": """
graph records the Python file that defines your DAG edges.

Code context:
  imports the supplied graph file
  reads EDGES or edges from that module
  imports sibling node_behavior/*.py files when loading the workflow
  mounts NodeRouter objects found in those behavior files

File-system context:
  updates .mwf with graph_path, runner, and edges
  creates node/<node-name>/ folders for graph nodes
  each node folder contains input/, output/, jobs/, node_state.json, and schema.json when applicable

Use when:
  you add or change graph edges, or you want to change the stored default runner:
    mwf graph src/graph.py --runner threaded
    mwf graph src/graph.py --runner process
    mwf graph src/graph.py --runner direct
""",
    "clean": """
clean resets runnable state for one or more nodes while preserving node input files.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py so it can validate node names
  does not run your node task code

File-system context for each selected node:
  deletes node/<node>/output/
  keeps node/<node>/input/
  deletes node/<node>/jobs/ entirely
  resets node_state.json to queued
  the next CLI load will recreate router.create_job(...) defaults if declared
  use reset instead when you want to keep existing job definitions and inputs

Targets:
  mwf clean node_name   cleans one node
  mwf clean a b c       cleans several named nodes
  mwf clean *           cleans every node in the graph

Shell note:
  in shells that expand *, quote it as mwf clean "*" if needed.
""",
    "reset": """
reset reruns existing jobs for one or more nodes while preserving node input files and job definitions.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py so it can validate node names
  does not run your node task code

File-system context for each selected node:
  deletes node/<node>/output/
  keeps node/<node>/input/
  keeps node/<node>/jobs/<id>/job.json and input.json
  removes per-job status/output/files so those jobs are queued again
  resets node_state.json to queued
  unlike clean, it does not delete the jobs folder or erase existing job inputs

Targets:
  mwf reset node_name   resets one node
  mwf reset a b c       resets several named nodes
  mwf reset *           resets every node in the graph

Shell note:
  in shells that expand *, quote it as mwf reset "*" if needed.
""",
    "wipe": """
wipe is clean plus input removal.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py so it can validate node names
  does not run your node task code

File-system context for each selected node:
  does everything clean does
  also deletes and recreates node/<node>/input/

Targets:
  mwf wipe node_name   wipes one node
  mwf wipe a b c       wipes several named nodes
  mwf wipe *           wipes every node in the graph

Shell note:
  in shells that expand *, quote it as mwf wipe "*" if needed.
""",
    "run": """
run executes one ready node, or selected jobs inside that node.

Job-selection syntax:
  mwf run node_name job 1
  mwf run node_name job 1 3 8-10

The job form reruns only the selected job IDs. Other jobs on the same node are
left untouched. Selected jobs are reset before execution, so previously done or
failed jobs can be rerun.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py
  validates the requested node exists in the graph
  may inspect node_behavior source code for simple autostart calls
  uses the stored runner from .mwf unless --runner is provided
  honors per-node runner overrides such as runner="direct" or router.run_sequentially()

File-system context:
  does not invent starter jobs; declare them with router.create_job(...) in node_behavior/<node>.py
  resets the requested node's existing job artifacts before running it
  if detected autostart nodes are included, resets those downstream nodes before running
  reads jobs from node/<node>/jobs/
  writes task artifacts under node/<node>/jobs/<job-id>/files/ and node/<node>/output/
  in job-selection mode, clears only the selected job folders' status/output/files artifacts

Use when:
  you want to rerun a single ready node, or a node plus detected autostart chain
  you want to rerun exact jobs without disturbing the rest of the node:
    mwf run tagify job 1 3 8-10
""",
    "runfrom": """
runfrom executes a node and its descendants in graph order, with threaded execution where possible.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py
  computes all descendants of the requested start node
  may inspect node_behavior source code for simple autostart calls outside that descendant set
  uses the stored runner from .mwf unless --runner is provided
  honors per-node runner overrides such as runner="direct", runner="threaded", or runner="process"

File-system context:
  resets the starting node's existing job artifacts before running it
  resets descendant nodes selected for this run while preserving router.create_job(...) jobs
  removes stale parent-generated descendant jobs so upstream nodes can regenerate them
  queues autostart-created jobs instead of letting them escape the selected run set
  does not invent starter jobs; declare them with router.create_job(...) in node_behavior/<node>.py
  reads and writes each selected node's node/<node>/jobs/, node/<node>/output/, and node_state.json
  refuses to continue if direct upstream nodes outside the run set are incomplete, unless you confirm

Use when:
  you want a safe partial workflow rerun from one node onward.
""",
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.describe is not None:
            return describe_command(args.describe)

        if args.command is None:
            parser.print_help()
            return 0

        if args.command == "init":
            return init_project()

        root = find_root()

        if args.command == "graph":
            return setup_graph(root, args.path, args.runner)

        workflow = load_workflow(root, args.runner)

        if args.command in {"clean", "reset", "wipe"}:
            nodes = resolve_node_targets(workflow, args.nodes)

            if args.command == "reset":
                for node in nodes:
                    reset_node_for_run(root, workflow, node)
                verb = "Reset"
            else:
                remove_input = args.command == "wipe"
                for node in nodes:
                    clean_node(root, workflow, node, remove_input=remove_input)
                verb = "Wiped" if remove_input else "Cleaned"

            if is_all_nodes_request(args.nodes):
                print(f"{verb} all nodes: {', '.join(nodes)}")
            else:
                print(f"{verb}: {', '.join(nodes)}")
            return 0

        node = safe_node_name(args.node)
        require_node(workflow, node)

        if args.command == "run":
            job_ids = selected_job_ids_from_args(args.job_mode, args.job_specs)
            if job_ids is not None:
                return run_selected_jobs(root, workflow, node, job_ids)
            return run_node(root, workflow, node)

        if args.command == "runfrom":
            return run_from(root, workflow, node)

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mwf",
        description=(
            "A small file-backed DAG workflow manager. Use 'mwf <command> --help' "
            "for command-specific help, or 'mwf --describe <command>' for the "
            "code and file-system context behind a command."
        ),
        epilog=textwrap.dedent(HELP_EPILOG).strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--runner",
        choices=RUNNER_CHOICES,
        help="Temporarily override the stored runner for commands that load the workflow.",
    )
    parser.add_argument(
        "--describe",
        metavar="COMMAND",
        help="Describe the code and file-system context for a command.",
    )

    commands = parser.add_subparsers(dest="command", metavar="command")

    commands.add_parser(
        "init",
        help="Create a .mwf project marker in the current directory.",
        description=COMMAND_DESCRIPTIONS["init"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    graph_cmd = commands.add_parser(
        "graph",
        help="Set the graph.py file and initialize node folders.",
        description=COMMAND_DESCRIPTIONS["graph"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    graph_cmd.add_argument("path", help="Path to the Python file defining EDGES or edges.")
    graph_cmd.add_argument(
        "--runner",
        choices=RUNNER_CHOICES,
        help="Store a default runner for future workflow commands.",
    )

    clean_cmd = commands.add_parser(
        "clean",
        help="Reset node output/job artifacts while keeping input files. Use '*' for all nodes.",
        description=COMMAND_DESCRIPTIONS["clean"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    clean_cmd.add_argument(
        "nodes",
        nargs="+",
        metavar="node",
        help="One or more node names, or '*' to clean every graph node.",
    )

    reset_cmd = commands.add_parser(
        "reset",
        help="Reset node output/status artifacts while keeping input files and jobs. Use '*' for all nodes.",
        description=COMMAND_DESCRIPTIONS["reset"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    reset_cmd.add_argument(
        "nodes",
        nargs="+",
        metavar="node",
        help="One or more node names, or '*' to reset every graph node.",
    )

    wipe_cmd = commands.add_parser(
        "wipe",
        help="Like clean, but remove input files too. Use '*' for all nodes.",
        description=COMMAND_DESCRIPTIONS["wipe"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    wipe_cmd.add_argument(
        "nodes",
        nargs="+",
        metavar="node",
        help="One or more node names, or '*' to wipe every graph node.",
    )

    run_cmd = commands.add_parser(
        "run",
        help="Run one ready node, or selected jobs in that node.",
        description=COMMAND_DESCRIPTIONS["run"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    run_cmd.add_argument("node", help="Node name to run.")
    run_cmd.add_argument(
        "job_mode",
        nargs="?",
        metavar="job",
        help="Optional literal 'job' or 'jobs' to run selected job IDs only.",
    )
    run_cmd.add_argument(
        "job_specs",
        nargs="*",
        metavar="id|start-end",
        help="Job IDs and ranges, for example: 1 3 8-10.",
    )
    run_cmd.add_argument(
        "--runner",
        choices=RUNNER_CHOICES,
        help="Temporarily override the workflow runner for this run.",
    )

    runfrom_cmd = commands.add_parser(
        "runfrom",
        help="Run a node and its descendants safely.",
        description=COMMAND_DESCRIPTIONS["runfrom"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    runfrom_cmd.add_argument("node", help="Start node for the partial workflow run.")
    runfrom_cmd.add_argument(
        "--runner",
        choices=RUNNER_CHOICES,
        help="Temporarily override the workflow runner for this runfrom.",
    )

    return parser


def describe_command(command: str) -> int:
    command = command.strip().lower()

    if command not in COMMAND_DESCRIPTIONS:
        valid = ", ".join(COMMAND_NAMES)
        raise RuntimeError(f"Unknown command for --describe: {command}. Choose one of: {valid}")

    print(f"mwf {command}")
    print("=" * (4 + len(command)))
    print(textwrap.dedent(COMMAND_DESCRIPTIONS[command]).strip())

    context = current_project_context()
    if context:
        print("\nCurrent directory context:")
        print(context)

    print(f"\nMore syntax help: mwf {command} --help")
    return 0


def current_project_context() -> str:
    try:
        root = find_root()
    except RuntimeError:
        return "  No .mwf project found from the current directory."

    lines = [f"  project root: {root}"]

    try:
        config = read_config(root)
    except Exception as error:
        lines.append(f"  could not read .mwf: {error}")
        return "\n".join(lines)

    graph_path = config.get("graph_path")
    runner = config.get("runner", "threaded")
    lines.append(f"  stored runner: {runner}")
    lines.append(f"  graph path: {graph_path or 'not set'}")
    lines.append(f"  node folder: {root / 'node'}")

    node_root = root / "node"
    if node_root.exists():
        nodes = sorted(path.name for path in node_root.iterdir() if path.is_dir())
        lines.append(f"  nodes on disk: {', '.join(nodes) if nodes else '(none)'}")
    else:
        lines.append("  nodes on disk: (node folder does not exist yet)")

    return "\n".join(lines)


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



def selected_job_ids_from_args(job_mode: str | None, job_specs: list[str]) -> list[int] | None:
    if job_mode is None:
        if job_specs:
            raise RuntimeError("Job selectors require the 'job' keyword, for example: mwf run node job 1 3 8-10")
        return None

    if job_mode not in {"job", "jobs"}:
        raise RuntimeError(
            f"Unexpected run argument: {job_mode}. Use: mwf run <node> job 1 3 8-10"
        )

    if not job_specs:
        raise RuntimeError("No jobs selected. Use: mwf run <node> job 1 3 8-10")

    return parse_job_selectors(job_specs)


def parse_job_selectors(selectors: list[str]) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()

    for selector in selectors:
        if "-" in selector:
            parts = selector.split("-")
            if len(parts) != 2 or not parts[0] or not parts[1]:
                raise RuntimeError(f"Invalid job range: {selector}")

            start = parse_positive_job_id(parts[0])
            end = parse_positive_job_id(parts[1])

            if end < start:
                raise RuntimeError(f"Invalid job range: {selector}; end must be >= start")

            values = range(start, end + 1)
        else:
            values = [parse_positive_job_id(selector)]

        for job_id in values:
            if job_id not in seen:
                seen.add(job_id)
                ids.append(job_id)

    return ids


def parse_positive_job_id(text: str) -> int:
    if not text.isdigit():
        raise RuntimeError(f"Invalid job id: {text}")

    job_id = int(text)
    if job_id < 1:
        raise RuntimeError(f"Invalid job id: {text}; job IDs start at 1")

    return job_id


def run_selected_jobs(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    job_ids: list[int],
) -> int:
    if not is_ready(workflow, node):
        print_not_ready(workflow, node)
        return 1

    for job_id in job_ids:
        if not workflow.storage.job_exists(node, job_id):
            raise RuntimeError(f"Job does not exist: {node}/{job_id}")

    # Flip the visible node status before reset bookkeeping, so a large selected
    # run does not appear stuck at queued while stale artifacts are removed.
    workflow.storage.set_node_status(node, RUNNING)

    for job_id in job_ids:
        reset_job_for_run(root, workflow, node, job_id, mark_queued=False)

    previous_allowed_run_nodes = workflow.allowed_run_nodes
    previous_autostart_mode = workflow.autostart_mode
    workflow.allowed_run_nodes = {node}
    workflow.autostart_mode = "queue"

    try:
        jobs = [workflow.storage.load_job(node, job_id) for job_id in job_ids]
        workflow.run_node_jobs(node, jobs, ignore_readiness=True)
    finally:
        workflow.allowed_run_nodes = previous_allowed_run_nodes
        workflow.autostart_mode = previous_autostart_mode

    print(f"Ran jobs for {node}:")
    for job_id in job_ids:
        print(f"  {job_id}")

    return 0

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

    # The requested node is now actively being prepared to run. Keep it marked
    # running during reset/bookkeeping instead of flipping back to queued.
    workflow.storage.set_node_status(node, RUNNING)

    reset_node_for_run(root, workflow, node, mark_queued=False)
    for item in nodes:
        if item != node:
            reset_node_for_run(root, workflow, item, remove_parented_jobs=True)

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

    workflow.storage.set_node_status(node, RUNNING)

    reset_node_for_run(root, workflow, node, mark_queued=False)
    for child in nodes:
        if child != node:
            reset_node_for_run(root, workflow, child, remove_parented_jobs=True)

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
        if not workflow.storage.has_queued_jobs(start_node):
            workflow.storage.set_node_status(start_node, QUEUED)
            print(
                f"No queued jobs for {start_node}. "
                f"Create default jobs in node_behavior/{start_node}.py with "
                "router.create_job(number=..., params={...})."
            )
            return 0

        if workflow.runner in {"threaded", "process"}:
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
                    if workflow.storage.has_queued_jobs(node)
                    and ready_for_run_set(workflow, node, run_set, ignore_external)
                ]

                if not ready:
                    break

                for node in ready:
                    workflow.run_node(node, ignore_readiness=True)
                    ran.append(node)

        workflow.finalize_ready_nodes()

        blocked = [node for node in nodes if workflow.storage.has_queued_jobs(node)]

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
                queued_count = len(workflow.storage.queued_job_ids(node))
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


def is_all_nodes_request(nodes: list[str]) -> bool:
    if any(item == "*" for item in nodes):
        return True

    # On shells that expand '*' before Python sees it, mwf clean * may arrive
    # as the visible names in the current directory. Treat that exact expansion
    # as the same all-nodes request.
    try:
        visible_cwd_entries = sorted(
            path.name for path in Path.cwd().iterdir()
            if not path.name.startswith(".")
        )
    except OSError:
        return False

    return len(nodes) > 1 and sorted(nodes) == visible_cwd_entries


def resolve_node_targets(workflow: MicroWorkflow, requested: list[str]) -> list[str]:
    if not requested:
        raise RuntimeError("No node specified")

    if is_all_nodes_request(requested):
        return list(nx.topological_sort(workflow.graph_obj))

    seen: set[str] = set()
    nodes: list[str] = []

    for item in requested:
        node = safe_node_name(item)
        require_node(workflow, node)
        if node not in seen:
            seen.add(node)
            nodes.append(node)

    return nodes

def clean_node(
    root: Path,
    workflow: MicroWorkflow,
    node: str,
    remove_input: bool = False,
):
    """User-facing clean/wipe.

    This is intentionally destructive for jobs: `mwf clean node` means the node
    is no longer finished and has no queued/done jobs on disk. Router-declared
    default jobs are recreated the next time the workflow is loaded. Dynamic
    jobs must be regenerated by rerunning their upstream nodes.
    """
    node_dir = safe_node_dir(root, node)

    remove_dir(node_dir / "output")
    remove_dir(node_dir / "jobs")

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
    """Reset a node while preserving its job definitions.

    This keeps each job's job.json and input.json, but removes the generated
    run artifacts so the job is queued again. When remove_parented_jobs=True,
    jobs created by upstream runs are deleted instead of preserved; this lets
    runfrom keep router.create_job(...) jobs on descendants while avoiding stale
    dynamic jobs from earlier runs.
    """
    node_dir = safe_node_dir(root, node)

    remove_dir(node_dir / "output")

    jobs_dir = node_dir / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)

    for job_dir in list(jobs_dir.iterdir()):
        if not job_dir.is_dir() or not job_dir.name.isdigit():
            continue

        if remove_parented_jobs:
            job_data = workflow.storage.read_json(job_dir / "job.json", default={})
            if job_data.get("parent") is not None:
                remove_path(job_dir)
                continue

        # Remove the known generated artifacts directly instead of scanning every
        # child path. On nodes with thousands of jobs this removes a large amount
        # of startup bookkeeping before the actual run begins. A missing
        # status.json is now interpreted as queued by FileStorage.
        remove_path(job_dir / "status.json")
        remove_path(job_dir / "output.json")
        remove_path(job_dir / "files")

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
    """Reset one job while preserving job.json and input.json."""
    node_dir = safe_node_dir(root, node)
    job_dir = node_dir / "jobs" / str(job_id)

    if not job_dir.is_dir():
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")

    remove_path(job_dir / "status.json")
    remove_path(job_dir / "output.json")
    remove_path(job_dir / "files")

    if mark_queued:
        workflow.storage.set_node_status(node, QUEUED)

def clear_node(root: Path, workflow: MicroWorkflow, node: str):
    node_dir = safe_node_dir(root, node)
    remove_dir(node_dir / "output")
    remove_dir(node_dir / "jobs")
    workflow.storage.init_node_folders(node)
    workflow.storage.set_node_status(node, QUEUED)


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
