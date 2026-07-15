from micro_workflow_manager import NodeRouter, OutputFileSystem

from src.utils.provenance import record_provenance

router = NodeRouter("answer_with_specialist")
OUTPUT = OutputFileSystem("specialist answers")


def solve_math(request: str) -> str:
    if "7" in request and "6" in request:
        return "42"
    return "The deterministic demo only evaluates its seeded example."


def draft_writing(_: str) -> str:
    return "A concise, audience-aware draft."


def answer_general(_: str) -> str:
    return "A concise general response."


SPECIALISTS = {
    "math": solve_math,
    "writing": draft_writing,
    "general": answer_general,
}


@router.task(retries=1)
def answer_with_specialist(ctx, request, route, route_reason):
    specialist = SPECIALISTS[route]
    answer = specialist(request)
    OUTPUT.file(ctx, "answer.txt").write_text(answer, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="answer",
        inputs={"request": request, "route": route},
        decisions={
            "route_reason": route_reason,
            "specialist_function": specialist.__name__,
        },
        result=answer,
    )
    return answer


@router.fallback(name="safe_general_answer", retries=1)
def safe_general_answer(ctx, request, route, route_reason, error):
    answer = "The selected specialist failed; this is the safe general response."
    OUTPUT.file(ctx, "answer.txt").write_text(answer, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="fallback_answer",
        inputs={"request": request, "route": route},
        decisions={
            "route_reason": route_reason,
            "fallback": "safe_general_answer",
            "previous_error": repr(error),
        },
        result=answer,
    )
    return answer
