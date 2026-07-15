from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("classify_request")
router.create_job(params={"request": "What is 7 * 6?"})

OUTPUT = OutputFileSystem("routing decisions")
SPECIALIST_INPUT = NodeInputFileSystem(
    "answer_with_specialist",
    "selected specialist requests",
)


@router.task
def classify_request(ctx, request):
    lowered = request.lower()
    if any(character.isdigit() for character in request):
        route = "math"
    elif "write" in lowered or "draft" in lowered:
        route = "writing"
    else:
        route = "general"

    decision = {
        "route": route,
        "request": request,
        "reason": "deterministic lexical classifier",
    }
    OUTPUT.file(ctx, "route.json").write_json(decision, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="route",
        inputs={"request": request},
        decisions={"classifier": "deterministic lexical", "selected_route": route},
        result=decision,
    )
    SPECIALIST_INPUT.add_job(
        ctx,
        autostart=True,
        request=request,
        route=route,
        route_reason=decision["reason"],
    )
    return decision
