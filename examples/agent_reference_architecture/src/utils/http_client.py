from __future__ import annotations

from typing import Any

from micro_workflow_manager import shared_http_transport

from src.config import (
    AGENT_TOKEN,
    AGENT_URL,
    CONNECT_TIMEOUT_SECONDS,
    READ_TIMEOUT_SECONDS,
)


def post_agent_json(system_prompt: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not AGENT_URL:
        raise RuntimeError("MWF_EXAMPLE_AGENT_URL is not configured")
    headers = {"content-type": "application/json"}
    if AGENT_TOKEN:
        headers["authorization"] = f"Bearer {AGENT_TOKEN}"
    result = shared_http_transport.post_json(
        AGENT_URL,
        json={"system_prompt": system_prompt, "payload": payload},
        headers=headers,
        timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        wait_name="reference example agent request",
    )
    if not isinstance(result, dict):
        raise ValueError("agent endpoint must return one JSON object")
    return result
