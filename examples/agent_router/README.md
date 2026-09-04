# Agent routing

A classifier selects a specialist strategy and sends one downstream job to a
single execution node. The route, reason, selected specialist function, and any
fallback are retained as output provenance.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom classify_request
mwf inspect classify_request job 1
mwf inspect answer_with_specialist filter
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
