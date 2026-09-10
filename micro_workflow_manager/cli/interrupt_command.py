"""Resolve terminal interrupt choices before any executing command mutates state."""

from __future__ import annotations

import sys

from micro_workflow_manager.workflow.interrupt_preflight import (
    InterruptChoiceRequired,
    InterruptPolicyRequired,
)
from .interrupt_preflight import (
    read_interrupt_command_preflight,
    render_interrupt_preflight,
    require_interrupt_preflight_unchanged,
)


EXECUTING_GRAPH_COMMANDS = frozenset({
    "run", "runfrom", "runbetween", "resume", "resumefrom", "resumebetween",
})


def _terminal_choice(prompt, choices):
    while True:
        try:
            answer = input(prompt).strip().lower()
        except (EOFError, OSError) as error:
            raise RuntimeError("Interrupt choices were not completed; no work was started") from error
        if answer in choices:
            return choices[answer]
        print("Choose " + ", ".join(dict.fromkeys(choices.values())) + ".")


def prepare_interrupt_command(root, args):
    while True:
        try:
            observed = read_interrupt_command_preflight(root, args)
            break
        except InterruptPolicyRequired:
            if not sys.stdin.isatty():
                raise
            args.interrupt_policy = _terminal_choice(
                "Interrupt components: 1 run all normally, 2 stop before all, 3 decide individually: ",
                {"1": "run-all", "run-all": "run-all", "2": "stop-all", "stop-all": "stop-all",
                 "3": "individual", "individual": "individual"},
            )
        except InterruptChoiceRequired as error:
            if not sys.stdin.isatty():
                raise
            canonical = error.component[0]
            action = _terminal_choice(
                f"Interrupt component {{{', '.join(error.component)}}} [run/stop]: ",
                {"run": "run", "stop": "stop"},
            )
            args.interrupt_choice = [*args.interrupt_choice, f"{canonical}={action}"]
    if (observed.preflight.decisions or observed.preflight.unused_choices
            or args.interrupt_policy is not None or args.interrupt):
        print(render_interrupt_preflight(observed.preflight))
    require_interrupt_preflight_unchanged(root, observed)
    return observed
