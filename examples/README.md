# MWF example index

Start with `agent_reference_architecture/` when building a new project. It shows
the recommended 0.5.2 layout and combines pooled HTTP/API processing, validated
agent responses, retries and named fallbacks, transactional fan-out, durable
fan-in, provenance, and a bounded review/revise Hoeflein component.

| Example | Primary pattern |
|---|---|
| `agent_reference_architecture` | Complete production-shaped reference |
| `agent_prompt_chain` | Narrow typed stages and compact payloads |
| `agent_router` | Route decision plus specialist fallback |
| `agent_parallelization` | Transactional fan-out and explicit join |
| `agent_orchestrator_workers` | Dynamic high fan-out and ordered fan-in |
| `agent_evaluator_optimizer` | Bounded evaluator/optimizer protocol |
| `document_refinery` | Durable file-oriented transformation |
| `database_change_manager` | Plan/apply/verify with recovery evidence |
| `geometry_solver_lab` | Deterministic validation pipeline |
| `pygame_state_machine` | Event-driven cyclic state transitions |

Every example follows the same source layout:

```text
src/graph.py
src/node_behavior/<node>.py
src/utils/provenance.py
node/<node>/input/          # prompts/static resources when needed
```

Use `mwf init`, `mwf graph src/graph.py`, and the example README's run command.
Before deleting state, use `mwf resetfrom ... --dry-run`,
`mwf cleanfrom ... --dry-run`, or `mwf wipefrom ... --dry-run`.
