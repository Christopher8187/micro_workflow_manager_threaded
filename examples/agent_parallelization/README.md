# Agent parallelization

Three independent workers run in parallel and write into a join node's input.
The join has one explicit job and waits for all three predecessors.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom fan_out
mwf monitor --once
mwf inspect synthesize_answer job 1
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
