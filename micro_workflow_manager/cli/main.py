from __future__ import annotations

import sys

from .cleanup import clean_node, is_all_nodes_request, reset_node_for_run, resolve_node_targets
from .describe import describe_command
from .files import find_root, safe_node_name
from .doctor import doctor_command
from .inspect import inspect_command
from .migration import migrate_command
from .recovery import recover_command
from .graph_utils import component_topological_nodes
from .jobs import selected_job_ids_from_args
from .monitoring import monitor_command
from .planning import print_run_plan
from .parser import build_parser
from .project import init_project, load_workflow, setup_graph
from .restart import restart_active_jobs
from .run import resume_from, resume_node, run_from, run_node, run_selected_jobs
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
            return setup_graph(root, args.path, args.runner, update=args.update, dry_run=args.dry_run)

        if args.command == "migrate":
            return migrate_command(root, dry_run=args.dry_run)

        # Restart is intentionally handled before graph/router loading. The
        # generation fence reaches the running job as early as possible and the
        # command never starts or replaces a workflow scheduler.
        if args.command == "restart":
            node = safe_node_name(args.node)
            job_ids = selected_job_ids_from_args(
                args.job_mode,
                args.job_specs,
                command="restart",
            )
            assert job_ids is not None
            return restart_active_jobs(root, node, job_ids, dry_run=args.dry_run)

        if args.command == "doctor":
            return doctor_command(root)

        workflow = load_workflow(root, args.runner)

        if args.command == "recover":
            return recover_command(root, workflow, dry_run=args.dry_run)

        if args.command == "inspect":
            node = safe_node_name(args.node)
            require_node(workflow, node)
            if args.job_mode is None and args.job_id is None:
                return inspect_command(workflow, node)
            if args.job_mode != "job" or args.job_id is None or args.job_id < 1:
                raise RuntimeError("Use: mwf inspect <node> job <id>")
            return inspect_command(workflow, node, args.job_id)

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

            if args.dry_run:
                action = {
                    "clean": "delete jobs and output; preserve input",
                    "reset": "preserve jobs/input; requeue all jobs and clear generated output",
                    "wipe": "delete jobs, output, and input",
                }[args.command]
                print(f"Dry run for mwf {args.command}:")
                for item in nodes:
                    print(f"  {item}: would {action}")
                print("  no files or statuses were changed")
                return 0

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
            if args.plan:
                return print_run_plan(root, workflow, command="run", node=node, selected_jobs=job_ids)
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

        if args.command == "resume":
            if args.plan:
                return print_run_plan(root, workflow, command="resume", node=node)
            return resume_node(root, workflow, node, stats=args.stats, stats_interval=args.stats_interval)

        if args.command == "runfrom":
            if args.plan:
                return print_run_plan(root, workflow, command="runfrom", node=node)
            return run_from(root, workflow, node, stats=args.stats, stats_interval=args.stats_interval)

        if args.command == "resumefrom":
            if args.plan:
                return print_run_plan(root, workflow, command="resumefrom", node=node)
            return resume_from(root, workflow, node, stats=args.stats, stats_interval=args.stats_interval)

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
