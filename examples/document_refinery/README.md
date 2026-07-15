# Document refinery

A renamed, simplified source-and-utilities pipeline: discovery, normalization,
asset attachment, and publication. Every node writes its durable artifact and
a provenance record under `output/provenance/`.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom discover_sources
mwf inspect normalize_sections filter
mwf monitor --once
```
