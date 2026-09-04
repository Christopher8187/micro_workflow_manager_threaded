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

## Architecture conventions

This example uses the standard 0.5.2 source layout: the graph is declarative,
node modules are thin, reusable logic/provenance belongs in `src/utils`, and
workflow-owned data is written through MWF filesystem objects. For a
production-shaped HTTP/API, fallback, fan-out/fan-in, and Hoeflein-component
reference, compare this focused pattern with `../agent_reference_architecture/`.

Before destructive work, preview the matching non-running command:

```bash
mwf resetfrom <start-node> --dry-run
```
