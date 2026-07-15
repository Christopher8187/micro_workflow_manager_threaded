# Orchestrator-workers

The orchestrator decomposes work; workers run concurrently; a single join job
assembles deterministic ordered output.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom plan_work
mwf inspect execute_work_item filter
mwf inspect assemble_report job 1
```
