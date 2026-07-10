from __future__ import annotations

import sys

from .cleanup import clean_node, is_all_nodes_request, reset_node_for_run, resolve_node_targets
from .describe import describe_command
from .files import find_root, safe_node_name
from .graph_utils import component_topological_nodes
from .jobs import selected_job_ids_from_args
from .monitoring import monitor_command
from .parser import build_parser
from .project import init_project, load_workflow, setup_graph
from .run import run_from, run_node, run_selected_jobs
from .validation import require_node


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
            return setup_graph(root, args.path, args.runner, update=args.update)

        workflow = load_workflow(root, args.runner)

        if args.command == "monitor":
            nodes = resolve_node_targets(workflow, args.nodes) if args.nodes else component_topological_nodes(workflow)
            return monitor_command(
                workflow,
                nodes,
                interval=args.interval,
                once=args.once,
                json_output=args.json,
                no_clear=args.no_clear,
            )

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
                return run_selected_jobs(
                    root,
                    workflow,
                    node,
                    job_ids,
                    stats=args.stats,
                    stats_interval=args.stats_interval,
                )
            return run_node(root, workflow, node, stats=args.stats, stats_interval=args.stats_interval)

        if args.command == "runfrom":
            return run_from(root, workflow, node, stats=args.stats, stats_interval=args.stats_interval)

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
