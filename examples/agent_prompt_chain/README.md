# Prompt chaining

Each node narrows the problem and passes a compact result forward.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom draft_brief
mwf inspect compose_response job 1
```
