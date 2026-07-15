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
