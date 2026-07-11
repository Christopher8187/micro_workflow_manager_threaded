from __future__ import annotations

import argparse
import textwrap

from .constants import RUNNER_CHOICES
from .descriptions import COMMAND_DESCRIPTIONS, HELP_EPILOG

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
        help="Set or explicitly synchronize graph.py and node folders.",
        description=COMMAND_DESCRIPTIONS["graph"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    graph_cmd.add_argument("path", nargs="?", help="Path to the Python file defining EDGES or edges. Omit when using --update.")
    graph_cmd.add_argument(
        "--update",
        action="store_true",
        help="Synchronize edges and node folders using the configured graph file; stale node folders are deleted.",
    )
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


    restart_cmd = commands.add_parser(
        "restart",
        help="Safely restart running jobs inside an active run/runfrom sequence.",
        description=COMMAND_DESCRIPTIONS["restart"].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    restart_cmd.add_argument("node", help="Node containing the currently running job.")
    restart_cmd.add_argument(
        "job_mode",
        metavar="job",
        help="Literal 'job' or 'jobs'.",
    )
    restart_cmd.add_argument(
        "job_specs",
        nargs="+",
        metavar="id|start-end",
        help="Running job IDs and ranges, for example: 1 3 8-10.",
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
    add_stats_arguments(run_cmd)
    add_stats_arguments(runfrom_cmd)

    monitor_cmd = commands.add_parser(
        "monitor",
        help="Show live workflow/node/job statistics. Use from a second terminal during run/runfrom.",
        description=COMMAND_DESCRIPTIONS["monitor"].strip(),
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
        help="Print compact live statistics while this command runs. For a cleaner dashboard, use mwf monitor in another terminal.",
    )
    command.add_argument(
        "--stats-interval",
        type=positive_float,
        default=5.0,
        help="Seconds between --stats lines. Default: 5.",
    )
