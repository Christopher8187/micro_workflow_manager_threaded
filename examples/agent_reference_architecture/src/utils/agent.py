from __future__ import annotations

from typing import Any, Callable

from src.config import AGENT_URL
from src.utils.http_client import post_agent_json


def run_json_agent(
    *,
    system_prompt: str,
    payload: dict[str, Any],
    offline: Callable[[dict[str, Any]], dict[str, Any]],
    validator: Callable[[dict[str, Any]], dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    raw = post_agent_json(system_prompt, payload) if AGENT_URL else offline(payload)
    return validator(raw), ("http" if AGENT_URL else "offline")
