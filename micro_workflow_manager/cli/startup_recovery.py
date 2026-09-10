"""Run native recovery at the boundary before an applied CLI mutation."""

from .recovery import recover_native_project


def recover_before_mutation(root):
    result = recover_native_project(root, quiet=True)
    if result['errors']:
        raise RuntimeError('Native recovery must resolve the reported state before this command can continue')


def command_needs_recovery(args):
    if getattr(args, 'plan', False) or getattr(args, 'dry_run', False):
        return False
    if args.command == 'threads':
        return args.update or args.value is not None or getattr(args, 'api_total', None) is not None
    return args.command in {
        'run', 'runfrom', 'runbetween', 'resume', 'resumefrom', 'resumebetween',
        'reset', 'resetfrom', 'resetbetween', 'restart', 'copy', 'paste', 'graph',
    }
