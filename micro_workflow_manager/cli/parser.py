from __future__ import annotations

import argparse
import textwrap

from .constants import RUNNER_CHOICES
from .descriptions import COMMAND_HELP_DESCRIPTIONS, HELP_EPILOG

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

    init_cmd = commands.add_parser(
        "init",
        help="Initialize a project, optionally unpacking an MWF deployment archive first.",
        description=COMMAND_HELP_DESCRIPTIONS["init"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    init_cmd.add_argument(
        "archive",
        nargs="?",
        help="Optional deployment.zip path. If omitted, mwf checks common local deployment archive locations.",
    )


    copy_cmd = commands.add_parser(
        "copy",
        help="Save one node folder into the sibling clipboard folder.",
    )
    copy_cmd.add_argument("node", help="Node folder name to copy into clipboard/<node>.")

    paste_cmd = commands.add_parser(
        "paste",
        help="Replace one node folder with its saved clipboard copy.",
    )
    paste_cmd.add_argument("node", help="Node folder name to restore from clipboard/<node>.")

    graph_cmd = commands.add_parser(
        "graph",
        help="Set or explicitly synchronize graph.py and node folders.",
        description=COMMAND_HELP_DESCRIPTIONS["graph"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    graph_cmd.add_argument("path", nargs="?", help="Path to the Python file defining EDGES or edges. Omit when using --update.")
    graph_cmd.add_argument(
        "--update",
        action="store_true",
        help="Synchronize edges and node folders using the configured graph file; stale node folders are deleted.",
    )
    graph_cmd.add_argument(
        "--dry-run",
        action="store_true",
        help="Show graph/node-folder changes without writing .mwf or changing folders.",
    )
    graph_cmd.add_argument(
        "--runner",
        choices=RUNNER_CHOICES,
        help="Store a default runner for future workflow commands.",
    )

    commands.add_parser(
        "doctor",
        help="Run read-only project health checks.",
        description=COMMAND_HELP_DESCRIPTIONS["doctor"],
    )

    migrate_cmd = commands.add_parser(
        "migrate",
        help="Upgrade MWF-owned metadata to the current state schema.",
        description=COMMAND_HELP_DESCRIPTIONS["migrate"],
    )
    migrate_cmd.add_argument("--dry-run", action="store_true", help="List metadata that would change without writing it.")

    inspect_cmd = commands.add_parser(
        "inspect",
        help="Explain a node/job, list failed job IDs, or show debug output.",
        description=COMMAND_HELP_DESCRIPTIONS["inspect"],
    )
    inspect_cmd.add_argument("node", help="Node name to inspect.")
    inspect_cmd.add_argument(
        "mode",
        nargs="?",
        metavar="job|failed|debug",
        help="Optional literal job, failed, or debug.",
    )
    inspect_cmd.add_argument("job_id", nargs="?", type=int, metavar="id", help="Job ID when mode is job.")


    filter_cmd = commands.add_parser(
        "filter",
        help="Show a node's retry/fallback funnel or inspect one stage boundary.",
        description=COMMAND_HELP_DESCRIPTIONS["filter"],
    )
    filter_cmd.add_argument("node", help="Node name whose retry/fallback funnel should be shown.")
    filter_cmd.add_argument(
        "stage_mode",
        nargs="?",
        choices=("stage",),
        metavar="stage",
        help="Optional literal stage followed by a one-based stage number.",
    )
    filter_cmd.add_argument(
        "stage",
        nargs="?",
        type=int,
        metavar="x",
        help="One-based stage number when stage mode is selected.",
    )

    recover_cmd = commands.add_parser(
        "recover",
        help="Recover running jobs abandoned by a dead CLI process.",
        description=COMMAND_HELP_DESCRIPTIONS["recover"],
    )
    recover_cmd.add_argument("--dry-run", action="store_true", help="Show jobs that would be recovered without changing them.")

    clean_cmd = commands.add_parser(
        "clean",
        help="Reset whole Hoeflein-component output/job artifacts while keeping input files. Use '*' for all nodes.",
        description=COMMAND_HELP_DESCRIPTIONS["clean"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    clean_cmd.add_argument(
        "nodes",
        nargs="+",
        metavar="node",
        help="One or more node names; each selects its entire Hoeflein component, or '*' for all nodes.",
    )
    clean_cmd.add_argument("--dry-run", action="store_true", help="Describe the cleanup without changing files or statuses.")

    reset_cmd = commands.add_parser(
        "reset",
        help="Reset one DAG node or the whole Hoeflein component containing the named node, while keeping inputs/jobs. Use '*' for all nodes.",
        description=COMMAND_HELP_DESCRIPTIONS["reset"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    reset_cmd.add_argument(
        "nodes",
        nargs="+",
        metavar="node",
        help="Each DAG node selects itself; a Hoeflein member selects its whole component; use '*' for all nodes.",
    )
    reset_cmd.add_argument("--dry-run", action="store_true", help="Describe the cleanup without changing files or statuses.")

    wipe_cmd = commands.add_parser(
        "wipe",
        help="Like component-level clean, but remove input files too. Use '*' for all nodes.",
        description=COMMAND_HELP_DESCRIPTIONS["wipe"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    wipe_cmd.add_argument(
        "nodes",
        nargs="+",
        metavar="node",
        help="One or more node names; each selects its entire Hoeflein component, or '*' for all nodes.",
    )
    wipe_cmd.add_argument("--dry-run", action="store_true", help="Describe the cleanup without changing files or statuses.")

    run_cmd = commands.add_parser(
        "run",
        help="Run one ready node, or selected jobs in that node.",
        description=COMMAND_HELP_DESCRIPTIONS["run"].strip(),
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


    resume_cmd = commands.add_parser(
        "resume",
        help="Continue one node without resetting done jobs.",
        description=COMMAND_HELP_DESCRIPTIONS["resume"],
    )
    resume_cmd.add_argument("node", help="Node name to resume.")
    resume_cmd.add_argument("--runner", choices=RUNNER_CHOICES, help="Temporarily override the workflow runner.")
    resume_cmd.add_argument("--plan", action="store_true", help="Show the resume selection without changing or running anything.")
    add_stats_arguments(resume_cmd)


    restart_cmd = commands.add_parser(
        "restart",
        help="From a second terminal, restart running or failed jobs inside the active sequence.",
        description=COMMAND_HELP_DESCRIPTIONS["restart"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    restart_cmd.add_argument("node", help="Node selecting the active Hoeflein component.")
    restart_cmd.add_argument(
        "job_mode",
        nargs="?",
        choices=("failed", "job", "jobs"),
        metavar="failed|job|jobs",
        help=(
            "Omit to restart running plus failed/cancelled jobs in the active "
            "component; use failed for failed/cancelled jobs only, or job/jobs "
            "to select explicit IDs."
        ),
    )
    restart_cmd.add_argument(
        "job_specs",
        nargs="*",
        metavar="id|start-end",
        help="Explicit job IDs and ranges after job/jobs, for example: 1 3 8-10.",
    )
    restart_cmd.add_argument("--dry-run", action="store_true", help="Validate and show restart targets without fencing them.")

    threads_cmd = commands.add_parser(
        "threads",
        help="View or change per-node runtime limits.",
        description=COMMAND_HELP_DESCRIPTIONS["threads"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    threads_cmd.add_argument("node", nargs="?", help="Node name. Omit to list every mounted node.")
    threads_cmd.add_argument("value", nargs="?", help="Absolute integer, +N, -N, or reset/default/clear.")
    threads_cmd.add_argument(
        "--update",
        action="store_true",
        help="Reload node behavior files and refresh declared max_threads/runner values in mounted schemas.",
    )

    deploy_cmd = commands.add_parser(
        "deploy",
        help="Build filtered local deployments and copy them to a configured server.",
        description=COMMAND_HELP_DESCRIPTIONS["deploy"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    deploy_actions = deploy_cmd.add_subparsers(dest="deploy_command", metavar="action", required=True)

    deploy_setup = deploy_actions.add_parser(
        "setup",
        help="Store server connection settings and create .mwfignore.",
    )
    deploy_setup.add_argument("--host", help="Server host or IP. Omit to be prompted.")
    deploy_setup.add_argument("--user", help="SSH user. Omit to be prompted.")
    deploy_setup.add_argument("--port", type=int, help="SSH port. Default: 22.")
    deploy_setup.add_argument("--auth", choices=["password", "key"], help="Authentication mode.")
    deploy_setup.add_argument("--tool", choices=["putty", "openssh"], help="Transfer client. Password mode requires putty.")
    deploy_setup.add_argument("--key", help="Private key path. .ppk keys use PuTTY.")
    deploy_setup.add_argument("--pscp", help="Path to PuTTY pscp.exe.")
    deploy_setup.add_argument("--plink", help="Path to PuTTY plink.exe.")
    deploy_setup.add_argument("--python-command", help="Remote Python command used for extraction. Default: python3.")

    deploy_actions.add_parser(
        "local",
        help="Rebuild the filtered local deployment archive, replacing the previous one.",
    )

    deploy_remote = deploy_actions.add_parser(
        "remote",
        help="Upload the local deployment and extract it into a server path.",
    )
    deploy_remote.add_argument("--path", help="Destination directory on the server. Omit to be prompted.")
    deploy_remote.add_argument("--yes", action="store_true", help="Deploy the existing local archive without the first confirmation prompt.")

    runfrom_cmd = commands.add_parser(
        "runfrom",
        help="Run a node and its descendants safely.",
        description=COMMAND_HELP_DESCRIPTIONS["runfrom"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    runfrom_cmd.add_argument("node", help="Start node for the partial workflow run.")
    runfrom_cmd.add_argument(
        "--runner",
        choices=RUNNER_CHOICES,
        help="Temporarily override the workflow runner for this runfrom.",
    )
    resumefrom_cmd = commands.add_parser(
        "resumefrom",
        help="Continue a node and descendants without resetting done jobs.",
        description=COMMAND_HELP_DESCRIPTIONS["resumefrom"],
    )
    resumefrom_cmd.add_argument("node", help="Start node for the resumed partial workflow.")
    resumefrom_cmd.add_argument("--runner", choices=RUNNER_CHOICES, help="Temporarily override the workflow runner.")

    run_cmd.add_argument("--plan", action="store_true", help="Show the run selection and reset effects without changing or running anything.")
    runfrom_cmd.add_argument("--plan", action="store_true", help="Show the descendant run selection without changing or running anything.")
    resumefrom_cmd.add_argument("--plan", action="store_true", help="Show the resumed descendant selection without changing or running anything.")
    add_stats_arguments(run_cmd)
    add_stats_arguments(runfrom_cmd)
    add_stats_arguments(resumefrom_cmd)

    monitor_cmd = commands.add_parser(
        "monitor",
        help="Show live workflow/node/job statistics. Use from a second terminal during run/runfrom.",
        description=COMMAND_HELP_DESCRIPTIONS["monitor"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    monitor_cmd.add_argument(
        "nodes",
        nargs="*",
        metavar="node",
        help="Optional nodes to monitor. Omit to monitor every graph node.",
    )
    monitor_cmd.add_argument(
        "--interval",
        type=positive_float,
        default=2.0,
        help="Seconds between refreshes in watch mode. Default: 2.",
    )
    monitor_cmd.add_argument(
        "--once",
        action="store_true",
        help="Print one snapshot and exit instead of watching continuously.",
    )
    monitor_cmd.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of a table.",
    )
    monitor_cmd.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear the terminal between watch snapshots.",
    )

    top_cmd = commands.add_parser(
        "top",
        help="Event-driven htop-style workflow diagnostics and startup/terminal rates.",
        description=COMMAND_HELP_DESCRIPTIONS["top"],
    )
    top_cmd.add_argument(
        "nodes",
        nargs="*",
        metavar="node",
        help="Optional nodes to display. Omit to display every graph node.",
    )
    top_cmd.add_argument("--interval", type=positive_float, default=0.5, help="Maximum redraw/fallback interval. Default: 0.5.")
    top_cmd.add_argument("--window", type=positive_float, default=5.0, help="Rate and latency window in seconds. Default: 5.")
    top_cmd.add_argument("--events", type=int, default=8, help="Number of recent lifecycle events to display. Default: 8.")
    top_cmd.add_argument("--once", action="store_true", help="Print one snapshot and exit.")
    top_cmd.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    top_cmd.add_argument("--no-clear", action="store_true", help="Do not clear the terminal between redraws.")

    return parser

def positive_float(text: str) -> float:
    try:
        value = float(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected a positive number, got {text!r}") from error

    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")

    return value

def add_stats_arguments(command: argparse.ArgumentParser):
    command.add_argument(
        "--stats",
        action="store_true",
        help="Print compact timestamped statistics while this command runs.",
    )
    command.add_argument(
        "--stats-interval",
        type=positive_float,
        default=5.0,
        help="Seconds between --stats lines. Default: 5.",
    )
    command.add_argument(
        "--monitor",
        action="store_true",
        help="Print the full timestamped monitor dashboard in this terminal while the command runs.",
    )
    command.add_argument(
        "--monitor-interval",
        type=positive_float,
        default=2.0,
        help="Seconds between inline --monitor snapshots. Default: 2.",
    )
