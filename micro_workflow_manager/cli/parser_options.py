import argparse


def positive_float(text: str) -> float:
    try:
        value = float(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected a positive number, got {text!r}") from error

    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")

    return value


def add_destructive_arguments(command) -> None:
    command.add_argument("--dry-run", action="store_true", help="Describe the requested operation without applying it.")
    command.add_argument("--yes", action="store_true", help="Acknowledge the danger and skip the interactive typed confirmation.")
    add_keeptrace_argument(command)


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


def add_keeptrace_argument(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--keeptrace",
        action="store_true",
        help="Preserve existing job trace journals that this command would otherwise clear.",
    )
