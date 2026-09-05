from __future__ import annotations

import json


def component_key(members) -> tuple[str, ...]:
    """Identify a component by its exact sorted, distinct raw-node names."""
    return tuple(sorted(set(members)))


def encode_component_key(members) -> str:
    """Keep the established SQLite spelling, including ASCII escapes and spaces."""
    return json.dumps(list(component_key(members)))


def decode_component_key(value: str) -> tuple[str, ...]:
    return tuple(json.loads(value))
