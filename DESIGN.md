# MWF design recommendations and runnable patterns

## Advice first

1. **Design outputs for both reuse and diagnosis.** Every node should write the
   durable artifact needed by downstream work and enough user-owned provenance
   to explain how that artifact was produced. A result that can be reformatted
   but cannot be traced is incomplete for an iterative workflow.
2. **Give nodes domain names, not implementation names.** Prefer
   `validate_solution`, `classify_request`, or `apply_schema_change` over
   `step_3`, `worker`, or `process_data`.
3. **Keep one reason to retry per node.** Split model calls, validation, database
   mutation, and publication when they have different failure modes or fallback
   policies.
4. **Pass compact jobs; pass large data as files.** Job parameters should identify
   the work. Use `InputFileSystem`, `OutputFileSystem`, and
   `NodeInputFileSystem` for substantial artifacts.
5. **Use autostart for work created during execution.** Use explicit
   `router.create_job(...)` for known starter or join jobs. This makes the graph
   readable while preserving dynamic fan-out.
6. **Use fallbacks for alternate implementations, not hidden business stages.**
   A fallback should satisfy the same output contract as the main task and write
   which implementation ran to provenance.
7. **Make joins deterministic.** Name branch files by stable IDs, sort them before
   assembly, and record the expected set. Do not depend on thread completion
   order.
8. **Use idempotency at side-effect boundaries.** Downstream job creation,
   database mutations, uploads, and publication need stable keys or transactions.
9. **Validate independently from generation.** A generator should not be the only
   authority deciding whether its result is correct.
10. **Inspect the funnel before adding capacity.** `mwf inspect NODE filter`
    distinguishes a slow stage from a low-quality main attempt or fallback.

## The output contract: artifact plus provenance

The `output/` folder is a deliberate debugging surface. A production node should
normally contain both:

- the **durable result**, in a stable machine-readable form that can be copied,
  indexed, reformatted, or passed downstream; and
- **provenance**, written by project code, that records the inputs, decisions,
  implementation or model, attempt/fallback, validation evidence, and relevant
  parameters that produced that result.

A practical layout is:

```text
node/validate_solution/output/
  validated_solution.json
  metrics.json
  provenance/
    job_17_validation.json
```

A useful provenance record often includes:

```json
{
  "node": "validate_solution",
  "job_id": 17,
  "task": "validate_solution",
  "attempt": 1,
  "inputs": {"solution_file": "candidate_17.json"},
  "decisions": {
    "validator": "independent residual suite",
    "tolerance": 1e-9,
    "coordinate_system": "cartesian"
  },
  "result": {"accepted": true, "max_residual": 2.1e-12}
}
```

MWF's `events.jsonl`, `runtime.json`, status files, and checkpoint data explain
scheduler behavior. They do not replace domain provenance. Project provenance
should explain *why the result is defensible*; scheduler diagnostics explain
*what the framework did while running it*.

For an agent node, retain the model/provider/version, prompt or prompt hash,
structured response, tool calls, validation failures, chosen fallback, and token
or latency measurements when available. Avoid storing secrets or unnecessary
personal data.

All runnable examples in `examples/` use `src/utils/provenance.py` to create a
small JSON record under each node's `output/provenance/` folder. Replace that
helper with a domain-specific schema in a real project.

## Recommended project architecture

```text
project/
  src/
    graph.py
    node_behavior/
      discover_sources.py
      normalize_sections.py
      publish_records.py
    utils/
      provenance.py
      schemas.py
      domain_helpers.py
  node/                  # inspectable runtime/data folders
  clipboard/             # saved node snapshots
  examples/ or fixtures/
  tests/
  README.md
```

A node behavior file should read in this order:

1. imports;
2. `NodeRouter` declaration and explicit starter jobs;
3. filesystem declarations;
4. small pure helper functions;
5. main task;
6. fallbacks that preserve the main output contract.

Use pure functions in `src/utils/` for parsing, scoring, geometry, SQL planning,
or state transitions. Keep MWF context calls at the node boundary. This makes
unit testing possible without creating a workflow project for every calculation.

## Commands used across designs

```bash
mwf init
mwf graph src/graph.py
mwf graph --update --dry-run
mwf graph --update
mwf doctor
mwf run START --plan
mwf runfrom START
mwf monitor --once
mwf inspect NODE
mwf inspect NODE filter
mwf inspect NODE failed
mwf inspect NODE job 1
mwf restart NODE job 1
mwf resume NODE
mwf resumefrom NODE
```

Use `run` for one selected node and `runfrom` for a complete descendant sequence.
Use `resume`/`resumefrom` after a partial failure so successful jobs and their
outputs are preserved. Use `restart` to replace a specific live attempt or
requeue a failed job.

---

# Adapted source-and-utilities designs

These examples are renamed, self-contained adaptations of the kinds of parsing,
solver, validation, and content-processing pipelines that naturally live in a
`src/` plus `utils/` project. They are not copies of application-specific code;
they demonstrate cleaner node boundaries and stronger output provenance.

## 1. Document refinery

**Location:** `examples/document_refinery`

```text
discover_sources
  -> normalize_sections
  -> attach_assets
  -> publish_records
```

- `discover_sources` creates one job for each source record.
- `normalize_sections` converts source-specific content to a stable schema.
- `attach_assets` keeps image/asset relationships explicit.
- `publish_records` writes the final reusable record.
- Every node records its source, transformation decision, and result.

This design is preferable to one large “process document” node because discovery,
normalization, asset association, and publication have different retry and
validation policies.

```bash
cd examples/document_refinery
mwf init
mwf graph src/graph.py
mwf runfrom discover_sources
mwf monitor --once
mwf inspect normalize_sections filter
mwf inspect publish_records job 1
```

## 2. Geometry solver lab

**Location:** `examples/geometry_solver_lab`

```text
parse_construction
  -> choose_seed
  -> solve_coordinates
  -> validate_solution
  -> format_coordinates
```

This separates parser, seed/autofix policy, numerical solving, independent
validation, and output formatting. The solver can retry without changing the
parser. Validation does not merely repeat the equations supplied to the solver.
The formatter retains the seed, coordinate system, tolerance, and accepted
residuals so a compact coordinate string remains debuggable.

```bash
cd examples/geometry_solver_lab
mwf init
mwf graph src/graph.py
mwf runfrom parse_construction
mwf inspect solve_coordinates filter
mwf inspect validate_solution job 1
mwf inspect format_coordinates job 1
```

For a larger geometry system, split validators by invariant family only when they
need different concurrency or failure handling. Otherwise keep one validator and
return a structured list of all residual checks.

---

# Five common agentic workflow patterns

The following five examples follow the workflow taxonomy described in
Anthropic's *Building effective agents*: prompt chaining, routing,
parallelization, orchestrator-workers, and evaluator-optimizer. The included
projects are deterministic and offline so their architecture can be tested
without API credentials. Replace the demo functions with model/tool clients while
preserving the same node, filesystem, validation, and provenance contracts.

Reference: <https://www.anthropic.com/engineering/building-effective-agents>

## 3. Prompt chaining

**Location:** `examples/agent_prompt_chain`

```text
draft_brief -> extract_constraints -> compose_response
```

Use prompt chaining when one call can make the next call smaller and more
reliable. Each node emits a compact intermediate result rather than forwarding a
large conversational transcript. Add a validator or fallback at the stage whose
contract can fail.

```bash
cd examples/agent_prompt_chain
mwf init
mwf graph src/graph.py
mwf runfrom draft_brief
mwf inspect draft_brief job 1
mwf inspect extract_constraints job 1
mwf inspect compose_response job 1
```

**Production provenance:** prompt template/version, model, structured response,
constraint checks, and final formatting decisions.

## 4. Routing

**Location:** `examples/agent_router`

```text
classify_request -> answer_with_specialist
```

The classifier chooses a named specialist strategy and passes one downstream
job. The execution node maps that route to the selected implementation and has a
safe fallback satisfying the same answer contract. This avoids creating fake
jobs for unselected branches and allows `runfrom` to finish cleanly.

```bash
cd examples/agent_router
mwf init
mwf graph src/graph.py
mwf runfrom classify_request
mwf inspect classify_request job 1
mwf inspect answer_with_specialist filter
mwf inspect answer_with_specialist job 1
```

**Production provenance:** route label, confidence, classifier/model version,
selected specialist, fallback, and the reason for overriding a low-confidence
route.

## 5. Parallelization

**Location:** `examples/agent_parallelization`

```text
                    -> collect_facts ----\
fan_out             -> generate_options ---> synthesize_answer
                    -> check_risks -----/
```

The fan-out node creates three jobs in one transaction. Branches run
independently and write stable named files into the join node's input. The join
has one explicit starter job and becomes ready only after its predecessors
complete. This uses concurrency without making the synthesis depend on completion
order.

```bash
cd examples/agent_parallelization
mwf init
mwf graph src/graph.py
mwf runfrom fan_out
mwf monitor --once
mwf inspect collect_facts filter
mwf inspect synthesize_answer job 1
```

**Production provenance:** branch list, source/tool used by each branch, expected
join inputs, missing/late branch policy, and synthesis rubric.

## 6. Orchestrator-workers

**Location:** `examples/agent_orchestrator_workers`

```text
plan_work -> execute_work_item (many jobs) -> assemble_report
```

The orchestrator produces an explicit plan and dynamically creates worker jobs.
`execute_work_item` uses `max_threads=3`; each worker writes a section named by a
stable index. The assembly job sorts by that index, not completion time. Worker
retries are visible in `mwf inspect execute_work_item filter`.

```bash
cd examples/agent_orchestrator_workers
mwf init
mwf graph src/graph.py
mwf runfrom plan_work
mwf inspect plan_work job 1
mwf inspect execute_work_item filter
mwf inspect assemble_report job 1
```

**Production provenance:** decomposition prompt/version, work-item IDs,
dependencies, worker model/tool choice, attempt count, and assembly ordering.

## 7. Evaluator-optimizer

**Location:** `examples/agent_evaluator_optimizer`

```text
generate_candidate
  -> evaluate_candidate
  -> improve_candidate
  -> final_evaluation
  -> publish_candidate
```

Generation and evaluation are separate authorities. The evaluator returns a
structured score and actionable feedback; the optimizer applies that feedback;
a final evaluator checks the revised result before publication. For an open-ended
loop, use a bounded cycle and a stop condition, but retain every candidate and
score rather than overwriting history.

```bash
cd examples/agent_evaluator_optimizer
mwf init
mwf graph src/graph.py
mwf runfrom generate_candidate
mwf inspect evaluate_candidate job 1
mwf inspect final_evaluation job 1
mwf inspect publish_candidate job 1
```

**Production provenance:** candidate lineage, rubric version, evaluator model,
score components, applied feedback, stop reason, and acceptance threshold.

---

# Application designs

## 8. Database change manager

**Location:** `examples/database_change_manager`

```text
plan_schema_change
  -> apply_schema_change
  -> verify_database
  -> export_schema_report
```

The example manages a SQLite database:

- planning writes the intended DDL and safety assumptions without mutating data;
- applying creates a backup and performs the change in a transaction;
- verification independently inspects schema and expected rows;
- reporting emits a portable schema/change report.

The database file is a durable artifact, while checksums, SQL, backup location,
row counts, schema observations, and verification results are provenance. In a
real service, use a migration ID as the idempotency key and keep credentials out
of outputs.

```bash
cd examples/database_change_manager
mwf init
mwf graph src/graph.py
mwf runfrom plan_schema_change
mwf inspect plan_schema_change job 1
mwf inspect apply_schema_change job 1
mwf inspect verify_database failed
mwf inspect export_schema_report job 1
```

Useful extensions:

- a `request_approval` node between plan and apply;
- separate `backup_database` and `restore_database` nodes;
- one verification job per table for large databases;
- a fallback that restores only when the apply contract proves rollback-safe;
- a publication node that stores signed migration evidence.

## 9. Pygame state machine

**Location:** `examples/pygame_state_machine`

```text
load_game_session
  -> apply_game_event (ordered jobs)
  -> render_frame
```

The loader writes an initial state and creates an ordered event stream. The state
reducer is sequential so two transitions cannot race. Each transition records
previous state, event, guard result, and next state. The final renderer reads the
state and emits a textual frame; a real Pygame project can replace it with
`pygame.Surface` rendering while leaving state transitions testable as pure
functions.

```bash
cd examples/pygame_state_machine
mwf init
mwf graph src/graph.py
mwf runfrom load_game_session
mwf inspect apply_game_event job 1
mwf inspect apply_game_event job 3
mwf inspect render_frame job 1
```

Recommended production split:

```text
load_assets -> initialize_session -> reduce_events -> simulate_world -> render_frame
                                      \-> persist_checkpoint
```

Keep rendering separate from the authoritative game-state transition. Store RNG
seed, delta time, input event, previous state hash, next state hash, and guard
outcome so a bad frame can be replayed deterministically.

---

# Choosing retries, fallbacks, and nodes

Use a **retry** when the same implementation may succeed after a transient error:
network interruption, rate limit, temporary file lock, or nondeterministic model
formatting failure.

Use a **fallback** when an alternate implementation can satisfy the same contract:
a second model, a simpler parser, a cached source, a lower-resolution renderer,
or a safe read-only database report. Record the fallback name and previous error
in output provenance.

Use a **new node** when the stage has a different contract, owner, concurrency
limit, validation criterion, or side effect. Do not hide approval, validation, or
publication inside a fallback.

A useful failure workflow is:

```bash
mwf inspect NODE filter
mwf inspect NODE failed
mwf inspect NODE job 42
mwf restart NODE job 42
mwf resume NODE
```

For a descendant sequence:

```bash
mwf restart NODE jobs 42 57 80-82
mwf resumefrom NODE
```

# Testing recommendations

Test at three levels:

1. **Pure utility tests** for parsing, state transitions, SQL planning, scoring,
   and validation.
2. **Node contract tests** that create a temporary MWF project, run one node, and
   assert durable output plus provenance.
3. **Workflow tests** that run from the starter node and assert every expected
   node completes, joins receive all files, and retries/fallbacks produce the
   expected filter funnel.

The repository test suite runs every project under `examples/` using the direct
runner and verifies that provenance JSON is produced. To exercise them manually:

```bash
python -m pytest -q tests/test_033_filter_icons_design.py
```

For large real projects, add a small deterministic fixture mode so architecture
and provenance tests do not depend on external APIs.
