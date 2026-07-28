from micro_workflow_manager import InputFileSystem, NodeInputFileSystem, NodeRouter, OutputFileSystem

from src.utils.agent import run_json_agent
from src.utils.provenance import record_provenance

router = NodeRouter("risk_worker", runner="api", max_threads=16)
INPUT = InputFileSystem("risk prompts")
OUTPUT = OutputFileSystem("risk results")
JOIN = NodeInputFileSystem("assemble_candidate")


def _validate(value):
    risks = value.get("risks")
    if not isinstance(risks, list) or not all(isinstance(item, str) for item in risks):
        raise ValueError("risk result requires a string list named risks")
    return {"risks": risks}


@router.task(retries=1, timeout=70, checkpoint_timeout=55)
def risk_worker(ctx, request_id: str, question: str):
    prompt = INPUT.file(ctx, "system_prompt.md").read_text()
    result, source = run_json_agent(
        system_prompt=prompt,
        payload={"question": question},
        offline=lambda _: {"risks": ["long lock", "rollback drift"]},
        validator=_validate,
    )
    OUTPUT.file(ctx, "result.json").write_json(result, overwrite=True)
    JOIN.file(ctx, "parts", request_id, "risks.json").write_json(result, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="risks",
        inputs={"request_id": request_id, "question": question},
        decisions={"source": source},
        result=result,
    )
    return result


@router.fallback(name="local_risk_check", retries=1)
def local_risk_check(ctx, request_id: str, question: str, error):
    result = {"risks": ["long lock", "unverified rollback"]}
    JOIN.file(ctx, "parts", request_id, "risks.json").write_json(result, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="risks_fallback",
        inputs={"question": question},
        decisions={"fallback": "local_risk_check", "previous_error": repr(error)},
        result=result,
    )
    return result
