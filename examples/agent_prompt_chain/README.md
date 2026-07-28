# Prompt chaining

Each node narrows the problem and passes a compact result forward.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom draft_brief
mwf inspect compose_response job 1
```

## Architecture conventions

This example uses the standard 0.5.1 source layout: the graph is declarative,
node modules are thin, reusable logic/provenance belongs in `src/utils`, and
workflow-owned data is written through MWF filesystem objects. For a
production-shaped HTTP/API, fallback, fan-out/fan-in, and Hoeflein-component
reference, compare this focused pattern with `../agent_reference_architecture/`.

Before destructive work, preview the matching non-running command:

```bash
mwf resetfrom <start-node> --dry-run
mwf cleanfrom <start-node> --dry-run
mwf wipefrom <start-node> --dry-run
```
