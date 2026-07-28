# Database change manager

Plans, applies, verifies, and reports a SQLite schema change. The database artifact
and every decision are inspectable.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom plan_schema_change
mwf inspect apply_schema_change job 1
mwf inspect verify_database failed
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
