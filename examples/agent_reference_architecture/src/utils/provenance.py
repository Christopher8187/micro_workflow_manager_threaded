from __future__ import annotations

from datetime import datetime, timezone


def record_provenance(ctx, output, *, artifact, inputs, decisions, result):
    payload = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "node": ctx.current_node,
        "job_id": ctx.job_id,
        "attempt": ctx.attempt,
        "artifact": artifact,
        "inputs": inputs,
        "decisions": decisions,
        "result": result,
    }
    output.file(ctx, "provenance", f"{artifact}.json").write_json(
        payload,
        overwrite=True,
    )
    ctx.trace("project provenance", content=payload)
