from __future__ import annotations

import sys

from .cleanup import (
    clean_node,
    is_all_nodes_request,
    reset_nodes_for_run,
    resolve_node_targets,
    resolve_component_targets,
    resolve_reset_targets,
    selected_component_labels,
    selected_reset_scope_labels,
)
from .describe import describe_command
from .files import find_root, safe_node_name
from .doctor import doctor_command
from .filter import inspect_filter
from .inspect import inspect_command
from .trace import trace_command
from .layout import ensure_runtime_layout
from .migration import migrate_command
from .recovery import recover_command
from .graph_utils import component_topological_nodes
from .jobs import selected_job_ids_from_args
from .monitoring import monitor_command
from .top import top_command
from .planning import print_run_plan
from .parser import build_parser
from .project import init_project, load_workflow, setup_graph
from .restart import restart_active_jobs, restart_active_scope
from .threads import threads_command, update_declared_threads
from .deploy import deploy_command
from .run import resume_from, resume_node, run_from, run_node, run_selected_jobs
from .validation import require_node
from .node_clipboard import copy_node_to_clipboard, paste_node_from_clipboard


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
            return init_project(args.archive)

        root = find_root()
        # Migration previews must be strictly read-only, including for projects
        # that still contain legacy runtime layout or lock directories.
        if args.command == "migrate" and args.dry_run:
            return migrate_command(root, dry_run=True)
        ensure_runtime_layout(root)

        if args.command == "copy":
            return copy_node_to_clipboard(root, safe_node_name(args.node))

        if args.command == "paste":
            return paste_node_from_clipboard(root, safe_node_name(args.node))

        if args.command == "deploy":
            return deploy_command(root, args)

        if args.command == "graph":
            return setup_graph(root, args.path, args.runner, update=args.update, dry_run=args.dry_run)

        if args.command == "migrate":
            return migrate_command(root, dry_run=args.dry_run)

        if args.command == "threads":
            if args.update:
                if args.node is not None or args.value is not None:
                    raise RuntimeError("mwf threads --update does not accept a node or runtime value")
                return update_declared_threads(root)
            return threads_command(root, args.node, args.value)

        # Restart is intentionally handled before graph/router loading. The
        # generation fence reaches the running job as early as possible and the
        # command never starts or replaces a workflow scheduler.
        if args.command == "restart":
            node = safe_node_name(args.node)
            if args.job_mode in {"job", "jobs"}:
                job_ids = selected_job_ids_from_args(
                    args.job_mode,
                    args.job_specs,
                    command="restart",
                )
                assert job_ids is not None
                return restart_active_jobs(root, node, job_ids, dry_run=args.dry_run)
            if args.job_specs:
                raise RuntimeError("Job IDs require the literal job or jobs mode.")
            return restart_active_scope(
                root,
                node,
                failed_only=args.job_mode == "failed",
                dry_run=args.dry_run,
            )

        if args.command == "doctor":
            return doctor_command(root)

        workflow = load_workflow(root, args.runner)

        if args.command == "recover":
            return recover_command(root, workflow, dry_run=args.dry_run)

        if args.command == "inspect":
            node = safe_node_name(args.node)
            require_node(workflow, node)
            if args.mode is None and args.job_id is None:
                return inspect_command(workflow, node)
            if args.mode == "debug" and args.job_id is None:
                return inspect_command(workflow, node, debug=True)
            if args.mode == "failed" and args.job_id is None:
                return inspect_command(workflow, node, failed=True)
            if args.mode != "job" or args.job_id is None or args.job_id < 1:
                raise RuntimeError(
                    "Use: mwf inspect <node> [failed | debug | job <id>]"
                )
            return inspect_command(workflow, node, args.job_id)


        if args.command == "trace":
            node = safe_node_name(args.node)
            require_node(workflow, node)
            if args.job_mode != "job" or args.job_id < 1:
                raise RuntimeError("Use: mwf trace <node> job <id>")
            return trace_command(workflow, node, args.job_id)


        if args.command == "filter":
            node = safe_node_name(args.node)
            require_node(workflow, node)
            if args.stage_mode is None and args.stage is None:
                return inspect_filter(workflow, node)
            if args.stage_mode != "stage" or args.stage is None:
                raise RuntimeError("Use: mwf filter <node> [stage <x>]")
            return inspect_filter(workflow, node, stage_number=args.stage)

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

        if args.command == "top":
            if args.events < 0:
                raise RuntimeError("--events must be an integer >= 0")
            nodes = resolve_node_targets(workflow, args.nodes) if args.nodes else component_topological_nodes(workflow)
            return top_command(
                workflow,
                nodes,
                interval=args.interval,
                once=args.once,
                json_output=args.json,
                no_clear=args.no_clear,
                window_seconds=args.window,
                recent_events=args.events,
            )

        if args.command in {"clean", "reset", "wipe"}:
            if args.command == "reset":
                nodes = resolve_reset_targets(workflow, args.nodes)
                scopes = selected_reset_scope_labels(workflow, nodes)
            else:
                nodes = resolve_component_targets(workflow, args.nodes)
                scopes = selected_component_labels(workflow, nodes)

            if args.dry_run:
                action = {
                    "clean": "delete jobs and output; preserve input",
                    "reset": "preserve jobs/input; requeue all jobs and clear generated output",
                    "wipe": "delete jobs, output, and input",
                }[args.command]
                print(f"Dry run for mwf {args.command}:")
                if args.command == "reset" and all(scope.startswith("{") for scope in scopes):
                    label = "selected Hoeflein components"
                elif args.command == "reset":
                    label = "selected reset scopes"
                else:
                    label = "selected Hoeflein components"
                print(f"  {label}: " + ", ".join(scopes))
                for item in nodes:
                    print(f"  {item}: would {action}")
                print(
                    "  trace journals: "
                    + ("would be preserved" if args.keeptrace else "would be cleared")
                )
                print("  no files or statuses were changed")
                return 0

            if args.command == "reset":
                reset_nodes_for_run(
                    root,
                    workflow,
                    nodes,
                    keep_trace=args.keeptrace,
                )
                verb = "Reset"
            else:
                remove_input = args.command == "wipe"
                for node in nodes:
                    clean_node(
                        root,
                        workflow,
                        node,
                        remove_input=remove_input,
                        keep_trace=args.keeptrace,
                    )
                verb = "Wiped" if remove_input else "Cleaned"

            if is_all_nodes_request(args.nodes):
                print(f"{verb} all nodes: {', '.join(nodes)}")
            elif args.command == "reset":
                if all(scope.startswith("{") for scope in scopes):
                    print(f"Reset Hoeflein component(s) {', '.join(scopes)}: {', '.join(nodes)}")
                else:
                    print(f"Reset {', '.join(scopes)}: {', '.join(nodes)}")
            else:
                print(f"{verb} Hoeflein component(s) {', '.join(scopes)}: {', '.join(nodes)}")
            return 0

        node = safe_node_name(args.node)
        require_node(workflow, node)

        if args.command == "run":
            job_ids = selected_job_ids_from_args(args.job_mode, args.job_specs)
            if args.plan:
                return print_run_plan(
                    root,
                    workflow,
                    command="run",
                    node=node,
                    selected_jobs=job_ids,
                    keep_trace=args.keeptrace,
                )
            if job_ids is not None:
                return run_selected_jobs(
                    root,
                    workflow,
                    node,
                    job_ids,
                    stats=args.stats,
                    stats_interval=args.stats_interval,
                    monitor=args.monitor,
                    monitor_interval=args.monitor_interval,
                    keep_trace=args.keeptrace,
                )
            return run_node(
                root,
                workflow,
                node,
                stats=args.stats,
                stats_interval=args.stats_interval,
                monitor=args.monitor,
                monitor_interval=args.monitor_interval,
                keep_trace=args.keeptrace,
            )

        if args.command == "resume":
            if args.plan:
                return print_run_plan(
                    root,
                    workflow,
                    command="resume",
                    node=node,
                    keep_trace=args.keeptrace,
                )
            return resume_node(
                root,
                workflow,
                node,
                stats=args.stats,
                stats_interval=args.stats_interval,
                monitor=args.monitor,
                monitor_interval=args.monitor_interval,
                keep_trace=args.keeptrace,
            )

        if args.command == "runfrom":
            if (args.refuse_mode is None) != (args.refuse_node is None):
                raise RuntimeError(
                    "Use: mwf runfrom <node> [refuseafter <node>] [--keeptrace]"
                )
            refuse_after_node = None
            if args.refuse_node is not None:
                refuse_after_node = safe_node_name(args.refuse_node)
                require_node(workflow, refuse_after_node)
            if args.plan:
                return print_run_plan(
                    root,
                    workflow,
                    command="runfrom",
                    node=node,
                    keep_trace=args.keeptrace,
                    refuse_after_node=refuse_after_node,
                )
            return run_from(
                root,
                workflow,
                node,
                stats=args.stats,
                stats_interval=args.stats_interval,
                monitor=args.monitor,
                monitor_interval=args.monitor_interval,
                keep_trace=args.keeptrace,
                refuse_after_node=refuse_after_node,
            )

        if args.command == "resumefrom":
            if args.plan:
                return print_run_plan(
                    root,
                    workflow,
                    command="resumefrom",
                    node=node,
                    keep_trace=args.keeptrace,
                )
            return resume_from(
                root,
                workflow,
                node,
                stats=args.stats,
                stats_interval=args.stats_interval,
                monitor=args.monitor,
                monitor_interval=args.monitor_interval,
                keep_trace=args.keeptrace,
            )

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
