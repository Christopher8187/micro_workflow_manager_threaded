from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem

from src.config import MAX_REVISIONS
from src.utils.provenance import record_provenance

router = NodeRouter("review_candidate")
OUTPUT = OutputFileSystem("candidate reviews")
REVISE = NodeInputFileSystem("revise_candidate")
PUBLISH = NodeInputFileSystem("publish_candidate")


@router.task
def review_candidate(ctx, candidate: dict):
    accepted = "rollback" in candidate["text"].lower() and candidate["iteration"] >= 1
    review = {
        "accepted": accepted,
        "feedback": "Name the rollback verification explicitly.",
        "iteration": candidate["iteration"],
    }
    OUTPUT.file(ctx, "reviews", f"{candidate['iteration']}.json").write_json(
        review,
        overwrite=True,
    )
    record_provenance(
        ctx,
        OUTPUT,
        artifact=f"review_{candidate['iteration']}",
        inputs=candidate,
        decisions={"rubric": ["rollback", "verification"], "max_revisions": MAX_REVISIONS},
        result=review,
    )
    if accepted:
        PUBLISH.add_job(
            ctx,
            autostart=True,
            idempotency_key=f"publish:{candidate['request_id']}",
            candidate=candidate,
            review=review,
        )
    elif candidate["iteration"] < MAX_REVISIONS:
        REVISE.add_job(
            ctx,
            autostart=True,
            idempotency_key=f"revise:{candidate['request_id']}:{candidate['iteration'] + 1}",
            candidate=candidate,
            feedback=review["feedback"],
        )
    else:
        raise ValueError("candidate exhausted the bounded revision budget")
    return review
