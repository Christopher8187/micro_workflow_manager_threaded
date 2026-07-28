from micro_workflow_manager import InputFileSystem, NodeInputFileSystem, NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("assemble_candidate")
router.create_job(params={"request_id": "demo-1"})
INPUT = InputFileSystem("worker fan-in")
OUTPUT = OutputFileSystem("assembled candidates")
NEXT = NodeInputFileSystem("review_candidate")


@router.task
def assemble_candidate(ctx, request_id: str):
    research = INPUT.file(ctx, "parts", request_id, "research.json").read_json()
    risks = INPUT.file(ctx, "parts", request_id, "risks.json").read_json()
    candidate = {
        "request_id": request_id,
        "iteration": 0,
        "text": "Use a canary migration with a measured, tested rollback.",
        **research,
        **risks,
    }
    OUTPUT.file(ctx, "candidate.json").write_json(candidate, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="assembly",
        inputs={"research": research, "risks": risks},
        decisions={"join": "two required deterministic files"},
        result=candidate,
    )
    NEXT.add_job(
        ctx,
        autostart=True,
        idempotency_key=f"review:{request_id}:0",
        candidate=candidate,
    )
    return candidate
