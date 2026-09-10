from __future__ import annotations

import sys

from micro_workflow_manager.project_format import read_native_project_config

from .cleanup import resolve_node_targets
from .destructive import execute_destructive_command
from .describe import describe_command
from .files import find_root, safe_node_name
from .doctor import doctor_command
from .engine import engine_command
from .filter import inspect_filter
from .inspect import inspect_command
from .trace import trace_command
from .lineage import lineage_command
from .layout import ensure_runtime_layout
from .recovery import recover_command
from .startup_recovery import command_needs_recovery, recover_before_mutation
from .recovery_preview import print_recovery_preview
from .graph_utils import component_topological_nodes
from .jobs import selected_job_ids_from_args
from .monitoring import monitor_command
from .top import top_command
from .planning import print_run_plan
from .preview import load_preview
from .membership_preflight import validate_command_membership
from .graph_command_dispatch import graph_preview_requested, print_graph_preview, refuse_reset_running_sessions
from .parser import build_parser
from .project import init_project, load_workflow, setup_graph
from .restart import restart_active_jobs, restart_active_scope
from .threads import api_total_command, threads_command, update_declared_threads
from .deploy import deploy_command
from .run import resume_from, resume_node, run_from, run_node, run_selected_jobs, run_between, resume_between
from .sampling import sample_command
from .validation import require_node
from .node_clipboard import copy_node_to_clipboard, paste_node_from_clipboard, validate_clipboard_request
from .resource_limits import raise_open_file_limit
from .interrupt_command import EXECUTING_GRAPH_COMMANDS, prepare_interrupt_command
from .interrupt_preflight import require_interrupt_preflight_unchanged, validate_loaded_interrupt_declarations


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    workflow = None
    interrupt_observation = None

    try:
        if args.describe is not None:
            return describe_command(args.describe)

        if args.command is None:
            parser.print_help()
            return 0

        if args.command in {"run", "runfrom", "runbetween", "resume", "resumefrom", "resumebetween"} and not getattr(args, "plan", False):
            raise_open_file_limit()

        if args.command == "init":
            return init_project(args.archive)

        root = find_root()
        read_native_project_config(root)
        if args.command == "trace":
            if args.json and not args.lineage:
                raise RuntimeError("--json requires: mwf trace <node> job <id> --lineage")
            if args.lineage:
                if args.job_id < 1:
                    raise RuntimeError("Use: mwf trace <node> job <id> --lineage")
                return lineage_command(
                    root, safe_node_name(args.node), args.job_id, json_output=args.json,
                )
        if args.command == "threads":
            if args.api_total is not None:
                print(
                    "Deprecation warning: mwf threads --api-total is deprecated and remains functional.",
                    file=sys.stderr,
                )
            if args.update and (
                args.node is not None or args.value is not None or args.api_total is not None
            ):
                raise RuntimeError("mwf threads --update does not accept a node or runtime value")
            if args.api_total is not None and (
                args.node is not None or args.value is not None
            ):
                raise RuntimeError("mwf threads --api-total does not accept a node or node value")
            if args.api_total is not None:
                recover_before_mutation(root)
                return api_total_command(root, args.api_total)
            if args.update:
                recover_before_mutation(root)
                return update_declared_threads(root)
            # Per-node mutation performs a read-only exact-owner observation
            # before startup recovery, then rechecks that observation in its
            # SQLite writer. Inspection is read-only throughout.
            return threads_command(root, args.node, args.value)
        if args.command in EXECUTING_GRAPH_COMMANDS:
            interrupt_observation = prepare_interrupt_command(root, args)
        # Engine is a strictly read-only visualization path. Dispatch it before
        # layout migration, SQLite initialization, or user graph imports.
        if args.command == "engine":
            return engine_command(root)
        if args.command == "run" and args.job_mode == "sample":
            return sample_command(root, args, interrupt_observation=interrupt_observation)
        if args.command == "run" and any(value is not None for value in (args.seed, args.sample_status, args.expect_population)):
            raise RuntimeError("--seed, --status, and --expect-population require: mwf run <node> sample <count>")
        read_only_plan = (
            args.command in {"run", "runfrom", "runbetween", "resume", "resumefrom", "resumebetween"}
            and args.plan
        ) or (
            args.command in {"reset", "resetfrom", "resetbetween", "recover"} and args.dry_run
        )
        if args.command == 'recover' and not read_only_plan:
            return recover_command(root)
        if interrupt_observation is not None:
            require_interrupt_preflight_unchanged(root, interrupt_observation)
        if args.command in {"copy", "paste"}:
            validate_clipboard_request(root, args.command, safe_node_name(args.node))
        if command_needs_recovery(args):
            recover_before_mutation(root)
        if args.command in {"reset", "resetfrom", "resetbetween"} and not read_only_plan:
            refuse_reset_running_sessions(root)
        if not read_only_plan:
            validate_command_membership(root, args)
            ensure_runtime_layout(root)

        if args.command == "copy":
            return copy_node_to_clipboard(root, safe_node_name(args.node))

        if args.command == "paste":
            return paste_node_from_clipboard(root, safe_node_name(args.node))

        if args.command == "deploy":
            return deploy_command(root, args)

        if args.command == "graph":
            return setup_graph(root, args.path, args.runner, update=args.update, dry_run=args.dry_run)

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

        if interrupt_observation is not None:
            require_interrupt_preflight_unchanged(root, interrupt_observation)
        workflow = load_preview(root) if read_only_plan else load_workflow(root, args.runner)
        if interrupt_observation is not None and not read_only_plan:
            validate_loaded_interrupt_declarations(workflow, interrupt_observation)
        if read_only_plan:
            recovery_result = print_recovery_preview(workflow, quiet_if_empty=args.command != "recover")
            if args.command == "recover":
                return recovery_result
            if graph_preview_requested(args):
                return print_graph_preview(
                    root, workflow, args,
                    interrupt_preflight=None if interrupt_observation is None else interrupt_observation.preflight,
                )

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
            return trace_command(
                workflow,
                node,
                args.job_id,
                errors_only=args.errors,
            )


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

        if args.command in {"reset", "resetfrom", "resetbetween"}:
            if args.command == "resetfrom" and (
                (args.refuse_mode is None) != (args.refuse_node is None)
            ):
                raise RuntimeError(
                    "Use: mwf resetfrom <node> [refuseafter <node>] [--yes]"
                )
            return execute_destructive_command(root, workflow, args)

        node = safe_node_name(args.node)
        require_node(workflow, node)

        if args.command in {"runbetween", "resumebetween"}:
            end_node = safe_node_name(args.end_node)
            handler = run_between if args.command == "runbetween" else resume_between
            return handler(
                root, workflow, node, end_node, stats=args.stats,
                stats_interval=args.stats_interval, monitor=args.monitor,
                monitor_interval=args.monitor_interval, keep_trace=args.keeptrace,
                interrupt_preflight=interrupt_observation.preflight,
            )

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
                    interrupt_preflight=interrupt_observation.preflight,
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
                    interrupt_preflight=interrupt_observation.preflight,
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
                interrupt_preflight=interrupt_observation.preflight,
            )

        if args.command == "resume":
            return resume_node(
                root,
                workflow,
                node,
                stats=args.stats,
                stats_interval=args.stats_interval,
                monitor=args.monitor,
                monitor_interval=args.monitor_interval,
                keep_trace=args.keeptrace,
                interrupt_preflight=interrupt_observation.preflight,
            )

        if args.command == "runfrom":
            if (args.refuse_mode is None) != (args.refuse_node is None):
                raise RuntimeError(
                    "Use: mwf runfrom <node> [(refuse | refuseafter) <node>] [--keeptrace]"
                )
            refuse_after_node = None
            refuse_before_node = None
            if args.refuse_node is not None:
                refusal_node = safe_node_name(args.refuse_node)
                require_node(workflow, refusal_node)
                if args.refuse_mode == "refuse":
                    refuse_before_node = refusal_node
                else:
                    refuse_after_node = refusal_node
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
                refuse_before_node=refuse_before_node,
                interrupt_preflight=interrupt_observation.preflight,
            )

        if args.command == "resumefrom":
            if (args.refuse_mode is None) != (args.refuse_node is None):
                raise RuntimeError(
                    "Use: mwf resumefrom <node> [(refuse | refuseafter) <node>] [--keeptrace]"
                )
            refuse_after_node = None
            refuse_before_node = None
            if args.refuse_node is not None:
                refusal_node = safe_node_name(args.refuse_node)
                require_node(workflow, refusal_node)
                if args.refuse_mode == "refuse":
                    refuse_before_node = refusal_node
                else:
                    refuse_after_node = refusal_node
            return resume_from(
                root,
                workflow,
                node,
                stats=args.stats,
                stats_interval=args.stats_interval,
                monitor=args.monitor,
                monitor_interval=args.monitor_interval,
                keep_trace=args.keeptrace,
                refuse_after_node=refuse_after_node,
                refuse_before_node=refuse_before_node,
                interrupt_preflight=interrupt_observation.preflight,
            )

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    finally:
        if workflow is not None:
            if getattr(workflow, "read_only", False):
                workflow.storage.close()
            else:
                workflow.storage.close_database_connections()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
