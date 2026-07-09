from __future__ import annotations

import textwrap

from .constants import COMMAND_NAMES
from .descriptions import COMMAND_DESCRIPTIONS
from .files import find_root, read_config

def describe_command(command: str) -> int:
    command = command.strip().lower()

    if command not in COMMAND_DESCRIPTIONS:
        valid = ", ".join(COMMAND_NAMES)
        raise RuntimeError(f"Unknown command for --describe: {command}. Choose one of: {valid}")

    print(f"mwf {command}")
    print("=" * (4 + len(command)))
    print(textwrap.dedent(COMMAND_DESCRIPTIONS[command]).strip())

    context = current_project_context()
    if context:
        print("\nCurrent directory context:")
        print(context)

    print(f"\nMore syntax help: mwf {command} --help")
    return 0

def current_project_context() -> str:
    try:
        root = find_root()
    except RuntimeError:
        return "  No .mwf project found from the current directory."

    lines = [f"  project root: {root}"]

    try:
        config = read_config(root)
    except Exception as error:
        lines.append(f"  could not read .mwf: {error}")
        return "\n".join(lines)

    graph_path = config.get("graph_path")
    runner = config.get("runner", "threaded")
    lines.append(f"  stored runner: {runner}")
    lines.append(f"  graph path: {graph_path or 'not set'}")
    lines.append(f"  node folder: {root / 'node'}")

    node_root = root / "node"
    if node_root.exists():
        nodes = sorted(path.name for path in node_root.iterdir() if path.is_dir())
        lines.append(f"  nodes on disk: {', '.join(nodes) if nodes else '(none)'}")
    else:
        lines.append("  nodes on disk: (node folder does not exist yet)")

    return "\n".join(lines)
