from micro_workflow_manager import NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("publish_candidate")
OUTPUT = OutputFileSystem("published candidates")


@router.task
def publish_candidate(ctx, candidate: dict, review: dict):
    if not review.get("accepted"):
        raise ValueError("only accepted candidates may be published")
    OUTPUT.file(ctx, "answer.json").write_json(candidate, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="published",
        inputs={"candidate": candidate, "review": review},
        decisions={"publication_gate": "accepted review"},
        result=candidate,
    )
    return candidate
