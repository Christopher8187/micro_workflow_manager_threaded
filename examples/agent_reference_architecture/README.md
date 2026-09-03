# Reference agent architecture

This is the first example to copy for a new MWF 0.5.2 project. It demonstrates:

- `runner="api"` nodes using the framework-owned pooled HTTP transport;
- finite connect/read timeouts and deterministic offline behavior;
- local JSON validation, retries, and named fallbacks;
- precomputed cross-node fan-out with stable idempotency keys;
- a durable file-based fan-in;
- a bounded `{review_candidate, revise_candidate}` Hoeflein component; and
- user-owned provenance beside every material result.

## Layout

```text
src/graph.py
src/config.py
src/node_behavior/*.py
src/utils/{agent,http_client,provenance}.py
node/{research_worker,risk_worker}/input/system_prompt.md
tests/test_smoke.py
```

## Run

```bash
mwf init
mwf graph src/graph.py --runner api
mwf runfrom seed_request --monitor
mwf inspect review_candidate
mwf trace review_candidate job 1
```

The example is offline by default. To call a compatible JSON agent endpoint, set
`MWF_EXAMPLE_AGENT_URL` and optionally `MWF_EXAMPLE_AGENT_TOKEN`. The endpoint
receives `{system_prompt, payload}` and should return one JSON object.

Preparation without execution:

```bash
mwf resetfrom seed_request --dry-run
mwf resetfrom seed_request
```
