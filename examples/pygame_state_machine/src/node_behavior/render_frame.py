from micro_workflow_manager import InputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("render_frame")
router.create_job()
INPUT=InputFileSystem("final game state")
OUTPUT=OutputFileSystem("rendered frame descriptions")
@router.task
def render_frame(ctx):
    state=INPUT.file(ctx,"final_state.json").read_json()
    frame=f"mode={state['mode']} x={state['x']} frame={state['frame']}"
    OUTPUT.file(ctx,"frame.txt").write_text(frame,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="frame",inputs=state,decisions={"renderer":"text stand-in for pygame Surface"},result=frame)
    return frame
