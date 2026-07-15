from micro_workflow_manager import NodeInputFileSystem, NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("apply_game_event",sequential=True)
OUTPUT=OutputFileSystem("state transitions")
RENDER=NodeInputFileSystem("render_frame")
@router.task
def apply_game_event(ctx,order,event):
    state_path=OUTPUT.file(ctx,"state.json")
    state=state_path.read_json() if state_path.exists() else {"mode":"menu","x":0,"frame":0}
    before=dict(state)
    if event=="start": state["mode"]="playing"
    elif event=="move_right" and state["mode"]=="playing": state["x"]+=1
    elif event=="pause": state["mode"]="paused"
    elif event=="resume": state["mode"]="playing"
    elif event=="quit": state["mode"]="quit"
    state["frame"]+=1
    state_path.write_json(state,overwrite=True)
    OUTPUT.file(ctx,f"transitions/{order}.json").write_json({"before":before,"event":event,"after":state},overwrite=True)
    RENDER.file(ctx,"final_state.json").write_json(state,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact=f"transition_{order}",inputs={"event":event,"before":before},decisions={"transition_table":"pygame demo"},result=state)
    return state
