# Pygame state machine

`apply_game_event` is sequential so state transitions are ordered. A real Pygame
project can replace the textual renderer with Surface creation while retaining the
same event/state provenance.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom load_game_session
mwf inspect apply_game_event job 3
mwf inspect render_frame job 1
```
