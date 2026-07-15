from micro_workflow_manager import NodeRouter, OutputFileSystem
from src.utils.provenance import record_provenance
router=NodeRouter("load_game_session")
router.create_job(params={"events":["start","move_right","pause","resume","quit"]})
OUTPUT=OutputFileSystem("session plans")
@router.task
def load_game_session(ctx,events):
    initial={"mode":"menu","x":0,"frame":0}
    OUTPUT.file(ctx,"initial_state.json").write_json(initial,overwrite=True)
    record_provenance(ctx,OUTPUT,artifact="session",inputs=events,decisions={"initial_state":initial},result=initial)
    for order,event in enumerate(events,1):
        ctx.node("apply_game_event").add(autostart=True,order=order,event=event)
    return {"events":len(events)}
