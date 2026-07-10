from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypeAlias

NodeGroup: TypeAlias = str | Iterable[str]


@dataclass(frozen=True)
class DirectedFan:
    """A compact directed fan used inside ``EDGES``.

    ``fan("a", ["b", "c"])`` expands to ``a -> b`` and ``a -> c``.
    ``fan(["a", "b"], "c")`` expands to ``a -> c`` and ``b -> c``.
    """

    start: NodeGroup
    end: NodeGroup


def fan(start: NodeGroup, end: NodeGroup) -> DirectedFan:
    """Return a compact directed fan specification for a graph file."""

    return DirectedFan(start=start, end=end)


def normalize_edges(items) -> list[tuple[str, str]]:
    """Expand ordinary edges and compact directed fans.

    Supported entries include all of the following::

        ("a", "b")
        ("a", ["b", "c"])       # a-B fan-out
        (["a", "b"], "c")       # A-c fan-in
        fan("a", ["b", "c"])
        fan(["a", "b"], "c")

    A collection on both sides is rejected because it describes a complete
    bipartite graph rather than a directed fan and is easy to write by mistake.
    """

    if items is None:
        raise RuntimeError("graph.py must define EDGES or edges")

    result: list[tuple[str, str]] = []

    for item in items:
        if isinstance(item, DirectedFan):
            start, end = item.start, item.end
        else:
            try:
                if len(item) != 2:
                    raise RuntimeError(f"Invalid edge or fan: {item!r}")
                start, end = item
            except TypeError as error:
                raise RuntimeError(f"Invalid edge or fan: {item!r}") from error

        starts = _node_group(start)
        ends = _node_group(end)

        if len(starts) > 1 and len(ends) > 1:
            raise RuntimeError(
                "A directed fan may have a node collection on only one side. "
                f"Split complete bipartite specification {item!r} into separate fans."
            )

        for source in starts:
            for target in ends:
                result.append((source, target))

    if not result:
        raise RuntimeError("graph.py must define at least one edge")

    return result


def _node_group(value: NodeGroup) -> list[str]:
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError as error:
            raise RuntimeError(f"Expected a node name or node collection, got {value!r}") from error

    if not values:
        raise RuntimeError("A directed fan node collection cannot be empty")

    for node in values:
        if not isinstance(node, str):
            raise RuntimeError(f"Node names must be strings, got {node!r}")

    return values
