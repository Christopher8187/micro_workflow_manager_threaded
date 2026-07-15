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
