from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("revise_candidate", runner="api", max_threads=8)
OUTPUT = OutputFileSystem("candidate revisions")
REVIEW = NodeInputFileSystem("review_candidate")


@router.task(retries=1)
def revise_candidate(ctx, candidate: dict, feedback: str):
    revised = dict(candidate)
    revised["iteration"] += 1
    revised["text"] = candidate["text"] + " Verify the rollback in staging before release."
    OUTPUT.file(ctx, "revisions", f"{revised['iteration']}.json").write_json(
        revised,
        overwrite=True,
    )
    record_provenance(
        ctx,
        OUTPUT,
        artifact=f"revision_{revised['iteration']}",
        inputs={"candidate": candidate, "feedback": feedback},
        decisions={"bounded_iteration": revised["iteration"]},
        result=revised,
    )
    REVIEW.add_job(
        ctx,
        autostart=True,
        idempotency_key=f"review:{revised['request_id']}:{revised['iteration']}",
        candidate=revised,
    )
    return revised
