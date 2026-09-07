"""Register half-open graph interval commands with their normal operation options."""

import argparse

from .constants import RUNNER_CHOICES
from .descriptions import COMMAND_HELP_DESCRIPTIONS
from .parser_options import add_destructive_arguments, add_keeptrace_argument, add_stats_arguments


def add_between_commands(commands):
    for name in ('runbetween', 'resumebetween', 'resetbetween'):
        command = commands.add_parser(
            name, help=COMMAND_HELP_DESCRIPTIONS[name],
            description=COMMAND_HELP_DESCRIPTIONS[name],
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        command.add_argument('node', help='Node selecting the included start component.')
        command.add_argument('end_node', help='Node selecting the excluded end component.')
        if name == 'resetbetween':
            add_destructive_arguments(command)
        else:
            command.add_argument('--runner', choices=RUNNER_CHOICES, help='Temporarily override the runner.')
            command.add_argument('--plan', action='store_true', help='Show the interval without changing project state.')
            add_keeptrace_argument(command)
            add_stats_arguments(command)
