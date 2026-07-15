from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def record_provenance(ctx, output, *, artifact: str, inputs: Any, decisions: Any, result: Any = None):
    """Write user-owned debugging provenance beside durable node output."""
    payload = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "node": ctx.current_node,
        "job_id": ctx.job_id,
        "task": ctx.current_task,
        "attempt": ctx.attempt,
        "repeat_index": ctx.repeat_index,
        "artifact": artifact,
        "inputs": inputs,
        "decisions": decisions,
        "result": result,
    }
    path = output.file(
        ctx,
        f"provenance/job_{ctx.job_id}_{artifact}.json",
    )
    path.write_json(payload, overwrite=True)
    return path
