# Evaluator-optimizer

Generation, evaluation, revision, and re-evaluation are separate inspectable nodes.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom generate_candidate
mwf inspect evaluate_candidate job 1
mwf inspect final_evaluation job 1
```
