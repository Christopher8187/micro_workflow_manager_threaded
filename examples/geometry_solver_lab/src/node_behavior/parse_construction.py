from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance

router = NodeRouter("parse_construction")
router.create_job(params={"statement": "a=(0,0); b=(4,0); c midpoint a b"})
OUTPUT = OutputFileSystem("parsed constructions")
NEXT = NodeInputFileSystem("choose_seed")

@router.task
def parse_construction(ctx, statement):
    tokens = [part.strip() for part in statement.split(";")]
    OUTPUT.file(ctx, "construction.json").write_json(tokens, overwrite=True)
    record_provenance(ctx, OUTPUT, artifact="parse", inputs=statement,
                      decisions={"separator": ";", "empty_tokens": "discarded"}, result=tokens)
    NEXT.add_job(ctx, autostart=True, tokens=tokens)
    return tokens
