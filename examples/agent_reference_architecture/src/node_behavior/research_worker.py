from micro_workflow_manager import InputFileSystem, NodeInputFileSystem, NodeRouter, OutputFileSystem

from src.utils.agent import run_json_agent
from src.utils.provenance import record_provenance

router = NodeRouter("research_worker", runner="api", max_threads=16)
INPUT = InputFileSystem("research prompts")
OUTPUT = OutputFileSystem("research results")
JOIN = NodeInputFileSystem("assemble_candidate")


def _validate(value):
    facts = value.get("facts")
    if not isinstance(facts, list) or not all(isinstance(item, str) for item in facts):
        raise ValueError("research result requires a string list named facts")
    return {"facts": facts}


@router.task(retries=1, timeout=70, checkpoint_timeout=55)
def research_worker(ctx, request_id: str, question: str):
    prompt = INPUT.file(ctx, "system_prompt.md").read_text()
    result, source = run_json_agent(
        system_prompt=prompt,
        payload={"question": question},
        offline=lambda _: {"facts": ["back up first", "measure rollback time"]},
        validator=_validate,
    )
    OUTPUT.file(ctx, "result.json").write_json(result, overwrite=True)
    JOIN.file(ctx, "parts", request_id, "research.json").write_json(result, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="research",
        inputs={"request_id": request_id, "question": question},
        decisions={"source": source},
        result=result,
    )
    return result


@router.fallback(name="local_research", retries=1)
def local_research(ctx, request_id: str, question: str, error):
    result = {"facts": ["back up first", "test the rollback"]}
    JOIN.file(ctx, "parts", request_id, "research.json").write_json(result, overwrite=True)
    record_provenance(
        ctx,
        OUTPUT,
        artifact="research_fallback",
        inputs={"question": question},
        decisions={"fallback": "local_research", "previous_error": repr(error)},
        result=result,
    )
    return result
