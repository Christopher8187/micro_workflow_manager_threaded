# MWF instructions for coding agents

## Product Workspace definitions

Read `C:\Business\product\docs\definitions.md` before planning Product Workspace
directory or multi-repository work, referring to another Product Workspace
repository, changing Agentic File System Routing, or interpreting an unfamiliar
Product Workspace term.

A narrow MWF implementation, diagnosis, or bug fix may stay inside this
repository without loading the shared definitions when its source changes and
verification remain here and no shared term is unclear. When uncertain, read
the definitions.

If MWF work requires a change in another Product Workspace repository, stop
before editing that repository and ask Christopher. Name this repository, the
additional repository, why it is needed, and the exact action proposed.

These instructions govern the whole `micro-workflow-manager` repository. Read
this file before designing an MWF workflow or changing framework behavior. Read
`HOW_TO_TEST.md` completely before running tests. Use `README.md` for exact user
semantics, `DESIGN.md` for framework rationale, and
`examples/agent_reference_architecture/` as the production-shaped example.

Do not infer MWF behavior from folder names alone. The synchronized graph,
statically declared autostarts, Hoeflein components, mounted routers, and durable
SQLite state together determine what can run.

## Start every task by classifying it

Decide which kind of work the user requested:

- **Inspect or diagnose:** use read-only commands and report evidence. Do not
  reset, run, restart, clean, wipe, deploy, commit, or publish unless requested.
- **Design a workflow:** define graph shape, artifacts, validation, fallbacks,
  idempotency, and acceptance tests before implementing node bodies.
- **Repair a workflow:** reproduce the smallest failing node/job, inspect its
  trace and fallback funnel, then change the narrowest faulty layer.
- **Change MWF:** state the scheduler/filesystem/durability invariant affected,
  add a focused regression, and follow the complete test protocol.
- **Execute a workflow:** restate the run boundary and stop conditions. Use a
  plan or deterministic sample first when the user requested a partial test.

Preserve user data and dirty worktrees. A working node directory is durable test
state, not disposable build output. Never silently translate a request to
`reset`, `clean`, `wipe`, `paste`, or a fresh `runfrom`.

## The mental model

An MWF project has four related layers:

1. `src/graph.py` declares ordinary directed edges.
2. `src/node_behavior/<node>.py` mounts each `NodeRouter`, tasks, retries,
   fallbacks, concurrency, waiting rules, and dynamic `ctx.node(...)` routing.
3. `node/<node>/` holds durable inputs, per-job payloads/results, node output,
   prompts, and project-owned diagnostics.
4. `.mwf/` holds synchronized project metadata, SQLite scheduler state, the
   active/last run receipt, and temporary runtime overrides.

Ordinary edges express dependency and allowed routing. An edge used with
`autostart=True` also contributes its reverse arc when MWF computes strongly
connected components. Those augmented strongly connected components are
**Hoeflein components**. A nontrivial component is one scheduling unit; its
quotient graph is a DAG.

Reason at both levels:

```text
raw graph:       router -> handler
                         <- autostart circulation

quotient graph:  previous -> {router, handler} -> next
```

Naming one member in a normal `run`, `resume`, cleanup, or descendant selection
usually selects its complete Hoeflein component. The deliberate exception is an
explicit selected-job or sample run: it is an isolated node test, disables
component circulation and descendants, and is not component acceptance.

SQLite is authoritative for scheduler state. User payloads and provenance stay
inspectable as files. Generation fencing prevents a timed-out, restarted, or
stale handler from publishing MWF-managed status, files, or child jobs.

## Command reference

Use `mwf <command> --help` for syntax and `mwf --describe <command>` for a
longer explanation. The commands below are the operating vocabulary an agent
should know.

| Command | What it does | Mutation boundary |
|---|---|---|
| `mwf init [archive.zip]` | Initializes a project and optionally unpacks a deployment archive. | Creates project/runtime sidecars and SQLite. Does not run nodes. |
| `mwf graph src/graph.py` | Registers the graph and synchronizes node folders for the first time. | Writes synchronized edges and creates/removes node folders. |
| `mwf graph --update [--dry-run]` | Explicitly resynchronizes after graph edits. | `--dry-run` is read-only; apply may permanently remove stale node folders. |
| `mwf engine` | Opens the graph-only local browser canvas. | Strictly read-only; no project code import or runtime initialization. |
| `mwf doctor` | Checks graph/router/folder agreement, state integrity, stale runs, and undeclared literal routing. | Read-only. Run after structural edits and before blaming execution. |
| `mwf migrate [--dry-run]` | Upgrades MWF-owned JSON/SQLite schemas. | Preview is read-only; apply changes framework metadata, never user outputs. |
| `mwf copy NODE` | Saves a node folder under the sibling clipboard. | Writes/replaces the saved clipboard copy. |
| `mwf paste NODE` | Replaces a node folder with its saved clipboard copy. | Destructive to the current node folder; use only when explicitly requested. |
| `mwf inspect NODE` | Explains node topology, component, runner, status, readiness, and blockers. | Read-only. |
| `mwf inspect NODE job ID` | Shows one job's input/output, task, checkpoint, generation, error, and events. | Read-only. |
| `mwf inspect NODE failed` | Lists failed IDs/errors and appropriate retry commands. | Read-only. |
| `mwf inspect NODE debug` | Shows node debug output. | Read-only. |
| `mwf trace NODE job ID` | Renders the ordered task/fallback/custom-trace/file/routing/terminal transcript. | Read-only; primary evidence for prompt, parser, and validation repair. |
| `mwf filter NODE` | Shows the main-retry/fallback funnel and terminal counts. | Read-only. |
| `mwf filter NODE stage N` | Shows jobs crossing or failing one retry/fallback boundary. | Read-only. |
| `mwf monitor [NODES] [--once] [--json]` | Shows durable node/job summaries and run ownership. | Read-only; suitable for a second terminal. |
| `mwf top [NODES] [--once] [--json]` | Shows event-driven rates, latency, waits, CPU/RSS, SQLite/WAL, and writer backlog. | Read-only; use for stalls, resource slopes, and admission/terminal imbalance. |
| `mwf threads [NODE [VALUE]]` | Reads or changes run-scoped effective concurrency; `reset` removes an override. | Listing is read-only; a value writes a temporary override. |
| `mwf threads --update` | Reloads router declarations such as `max_threads` without changing graph edges. | Updates mounted schema metadata; does not execute jobs. |
| `mwf run NODE [--plan]` | Freshens and executes one ready Hoeflein component. | `--plan` is read-only; execution resets affected work before running. |
| `mwf run NODE job ID...` / `jobs ...` | Resets and executes exactly the named jobs. | Other jobs remain untouched; this is isolated node execution. |
| `mwf run NODE sample COUNT` | Deterministically selects, resets, and executes a partial node population. | Other jobs remain untouched; descendants/component circulation are disabled. |
| `mwf runfrom START [refuse B | refuseafter B]` | Freshens START and quotient-DAG descendants, then runs the branch. | Producer-aware fresh execution; reset scope is the full selected descendants. |
| `mwf resume NODE [--plan]` | Preserves successful work and continues queued/failed/abandoned work in one component. | Does not perform fresh producer cleanup. |
| `mwf resumefrom START [refuse B | refuseafter B]` | Resume semantics over START and descendants. | Preserves completed descendant work. |
| `mwf restart NODE [failed | job/jobs ...]` | From a second terminal, generation-fences and replaces eligible work in the active component. | Does not launch a scheduler; valid only while a run sequence is active. |
| `mwf recover [--dry-run]` | Fences/requeues running jobs abandoned by a dead CLI owner. | Never use against a live owner; done and failed jobs are not reset. |
| `mwf reset NODE [job/jobs ...] [--dry-run]` | Performs `run` preparation without executing. | Requeues/clears selected generated output; confirmation required to apply. |
| `mwf resetfrom START [--dry-run]` | Performs producer-aware `runfrom` preparation without executing. | Confirmation required to apply. |
| `mwf clean NODE... [--dry-run]` | Deletes every job/output in selected components but preserves inputs. | Destructive; confirmation required. |
| `mwf cleanfrom START [--dry-run]` | Applies clean to all quotient-DAG descendants. | Destructive; confirmation required. |
| `mwf wipe NODE... [--dry-run]` | Deletes selected jobs, outputs, and inputs. | Most destructive local node operation; confirmation required. |
| `mwf wipefrom START [--dry-run]` | Applies wipe to all descendants. | Most destructive descendant operation; confirmation required. |
| `mwf deploy setup` | Stores non-secret server metadata and creates `.mwfignore`. | Changes deployment configuration only. |
| `mwf deploy local` | Rebuilds the filtered local deployment archive. | Replaces the previous local deployment copy/archive. |
| `mwf deploy remote` | Uploads and extracts the existing deployment on the configured server. | External write; requires explicit deployment authorization. |

`refuse B` and `refuseafter B` are not synonyms:

- `refuse B` triggers when B's component is ready and stops before it starts.
- `refuseafter B` allows B's component to terminate, then stops later admission.
- Both preserve queued later work for a future resume.
- Neither shrinks the fresh-preparation scope of `runfrom`.

Use `--keeptrace` only when previous transcripts must survive a fresh rerun.
Use `--stats` for compact periodic metrics and `--monitor` for the full inline
dashboard. Prefer separate `mwf monitor` and `mwf top` terminals during a serious
performance or timing investigation.

## Deterministic partial node runs

Use samples for prompt, model, parser, validator, and provider experiments where
a full node would be expensive:

```bash
mwf run refhook sample 50 --seed repair-v2 --plan
mwf run refhook sample 50 --seed repair-v2
mwf run refhook sample 20 --seed failures-v1 --status failed
```

The default population is all existing jobs, including terminal jobs. Selection
uses portable SHA-256 ranking (`mwf.sample.v1`), not SQL `RANDOM()` or Python's
version-dependent pseudo-random sequence. A plan prints the population digest,
IDs, seed, and a guarded replay command. Preserve the printed seed in benchmark
or acceptance evidence.

The population digest covers candidate job ID, status, execution generation,
and input content. Use `--expect-population` when the plan and execution are
separate steps:

```bash
mwf run refhook sample 50 \
  --seed repair-v2 \
  --expect-population sha256:<digest>
```

If the guard detects drift, plan again. Do not remove the guard merely to make a
stale experiment run. A sample bypasses predecessor readiness intentionally but
allows only the named node to execute; a handler that tries to circulate through
a Hoeflein component or create descendants will not turn the sample into a
hidden end-to-end run.

## Design workflows from contracts, not just node names

For every node, write down:

- one purpose;
- required job parameters and durable file inputs;
- returned result and durable file outputs;
- predecessor and successor artifact mapping;
- cardinality: one-to-one, fan-out, fan-in, or bounded iteration;
- runner and evidence-based concurrency;
- task retry versus named fallback policy;
- hard validation invariants and optional enrichment;
- side effects, idempotency key, and replay behavior;
- provenance and acceptance checks.

Keep `src/graph.py` structural:

```python
WORKERS = ["extract_text", "extract_images"]

EDGES = [
    ("ingest", WORKERS),
    (WORKERS, "assemble"),
]
```

Put one thin router per node in `src/node_behavior/`. Put reusable provider,
parsing, validation, schema, and provenance code in `src/utils/`. Prompts and
static resources belong in `node/<node>/input/`; they should not be giant Python
string literals in `graph.py`.

### Fan-out and fan-in

Publish related child jobs atomically and idempotently:

```python
with ctx.transaction():
    ctx.node("worker").add_many(
        [{"record_id": item["id"]} for item in records],
        idempotency_keys=[f"record:{item['id']}" for item in records],
    )
```

For very large homogeneous batches, prefer `add_many`/`add_jobs` to thousands
of independent SQLite round trips. Each child should still be a separate job.

Fan-in is a real node, not a shared in-memory list. Workers write deterministic
contributions such as `parts/<request-id>/<index>.json`; the join sorts,
validates completeness/duplicates, and writes one assembled result. Graph
predecessors provide the barrier.

### Hoeflein components

Use a cycle only for a real bounded protocol, such as review/revise. Every cycle
needs a monotone termination value, an explicit maximum, idempotent child
creation, and success/failure/restart tests. Do not hide polling or an unbounded
retry loop inside a task.

### Runners

- `direct`: deterministic single-process debugging and tiny work.
- `threaded`: local blocking/file/SDK work with bounded threads.
- `api`: high logical concurrency for external latency using cooperative jobs
  and the shared HTTP transport.
- `process`: CPU work with importable code and pickleable parameters/results.

For API work, use `micro_workflow_manager.shared_http_transport`; do not create a
client per job. Declare finite connect/read timeouts and a meaningful
`wait_name`. `max_threads` is logical node concurrency, not a promise that the
provider, database, network, or CPU can sustain that many active operations.

## Model work: separate transport, parsing, validation, and quality

A reliable model node has four distinguishable layers:

1. **Transport:** request dispatch, response receipt, timeouts, and provider
   errors.
2. **Parsing:** can the response become the expected local data structure?
3. **Semantic validation:** does it preserve non-negotiable source/domain
   invariants?
4. **Evaluation:** is the structurally valid result useful and high quality?

Do not turn the parser into a creativity grader. Do not label a repeated local
validation error as networking merely because the model call preceded it.

### Build a strict-to-loose fallback slope

The slope loosens representational and optional requirements while keeping hard
truth/safety invariants fixed:

```text
main task
  full enrichment request + ordinary schema
        ↓ validation feedback
fallback 1
  same semantic goal; clearer repair prompt; normalize equivalent forms
        ↓ validation feedback
fallback 2
  preserve required core; accept valid optional candidates independently;
  use the more reliable/high-reasoning route only when evidence warrants it
```

Start with one main task and one or two named repair fallbacks. Add another stage
only when `mwf trace` proves a distinct recoverable failure class. Repeating the
same prompt/model/validator under different names is not a useful fallback.

Hard constraints that must not slope:

- source facts, quotes, labels, IDs, and required references must be grounded;
- required core records cannot be silently dropped;
- dimensional/schema agreements needed by downstream consumers stay exact;
- safety, privacy, egress, idempotency, and publication gates stay exact;
- a workflow that requires model enrichment cannot finish with an empty copy,
  no-op placeholder, or deterministic bypass merely to become green.

Checks that can often slope safely:

- whitespace, punctuation, label wrappers, and equivalent JSON surface forms;
- optional candidate ordering;
- an invalid optional enrichment candidate when valid siblings can survive;
- redundant restatement fields that code can recover from the grounded source;
- nullable optional flags that can be normalized without inventing meaning.

Prefer deterministic canonicalization after a model has made the semantic
choice. For example, let the model identify which label is a reference, then
recover the exact verbatim label span from the known source in code. Do not ask
the model to reproduce brittle whitespace merely to pass a validator.

Validate optional candidates independently. One bad optional guess should not
erase seven valid enrichments unless the response contract explicitly makes the
set atomic. Record normalization/drop counts and reasons in project provenance.
Conversely, do not drop a bad required mapping to manufacture success.

### When `mwf trace` shows validator absurdity

Inspect the raw response, parsed value, validation error, fallback prompt, and
persisted output together:

```bash
mwf inspect NODE job ID
mwf trace NODE job ID
mwf filter NODE
mwf filter NODE stage N
```

Treat these patterns as likely prompt/parser/validator design debt:

- several different model routes produce plausible content but hit the same
  terminal local error;
- a valid grounded value is rejected only because it is not copied verbatim in
  a redundant response field;
- one invalid optional list member rejects the whole response;
- the trace contains valid candidates but the persisted enrichment is empty;
- a validator requires the model to violate the task's creative objective;
- the fallback prompt repeats an impossible constraint without the prior error
  and source evidence needed to repair it.

Before weakening anything, state the intended invariant in plain language and
write fixtures for:

- valid ordinary output;
- valid equivalent formatting;
- a recoverable optional-candidate defect;
- an invalid required/core value;
- mixed valid/invalid optional candidates;
- empty output when enrichment is mandatory.

Then change the narrowest layer. Normalize syntax in the parser, make optional
validation candidate-scoped, clarify the repair prompt, or correct an actually
over-strict semantic rule. Do not globally disable validation or extend
transport leases to conceal a local rejection.

## Debug one node methodically

Use this sequence unless evidence justifies skipping a step:

1. Check structural health.

   ```bash
   mwf doctor
   mwf inspect NODE
   ```

2. Establish the failure population and exact jobs.

   ```bash
   mwf inspect NODE failed
   mwf filter NODE
   ```

3. Read at least one success and each distinct failure class.

   ```bash
   mwf inspect NODE job ID
   mwf trace NODE job ID
   mwf filter NODE stage N
   ```

4. Classify the failing layer: readiness/component, task code, external
   transport, response parsing, semantic validation, terminal publication,
   SQLite durability, or filesystem fencing.

5. Reproduce with an isolated deterministic sample.

   ```bash
   mwf run NODE sample 20 --seed diagnosis-1 --plan
   mwf run NODE sample 20 --seed diagnosis-1
   ```

6. Make one narrow change and rerun the same seed/population. Compare fallback
   stage counts and retained enrichment, not only the final success count.

7. Test low, moderate, and declared concurrency if the failure is load-shaped.
   Use a temporary `mwf threads NODE VALUE` override; restore it with
   `mwf threads NODE reset`.

8. Run integrated acceptance only after the isolated node contract is sound.

During a live run, observe from separate terminals:

```bash
mwf monitor
mwf top
```

`monitor` answers what is queued/running/done/failed. `top` answers whether work
is being admitted/completed, where time is spent, whether memory or SQLite/WAL
grows, and whether the mutation writer is behind. A quiet terminal is not proof
of a frozen scheduler.

Use `mwf restart NODE job ID` only while the original scheduler is active. After
the sequence ends, use `mwf resume NODE` or `mwf resumefrom START`. Use
`mwf recover` only when the recorded CLI owner is demonstrably dead.

## Performance and networking diagnosis

Do not start by lowering provider concurrency, extending transport leases, or
calling the provider at fault. Compare layers:

- transport-only control;
- MWF runner plus shared transport;
- full durable workflow;
- one small node versus a large node under the same provider/model;
- early steady state versus late tail;
- low, moderate, and declared concurrency.

Gather admission rate, completion rate, queue wait, external wait, request
latency, failure class, effective pumps, RSS slope, CPU, thread/client count,
SQLite/WAL size, mutation backlog, and oldest running-job age. If provider logs
show normal routing/TTFT/speed while MWF admission or terminal publication
stalls, inspect framework storage/resource overhead. If transport and runner
controls also stall, the provider/network/host stack remains in scope.

Priority of admission, successful completion, and failure publication must stay
balanced. Starving failure or completion persistence can create an impossible
backlog even when request dispatch is fast.

Never create one network client per job or retry. Reuse the shared manager,
retire poisoned shards, and bound replacement capacity by current demand rather
than historical failures. Memory should track live work, not cumulative retries
or completed jobs.

## Framework change procedure

1. Read the closest implementation and tests.
2. State the invariant and exact user-visible behavior.
3. Add a focused regression that fails for the old behavior.
4. Make the smallest coherent implementation change.
5. Run the focused regression and adjacent lifecycle/CLI tests.
6. Follow every compile, ordinary-batch, separate-cycle, and stress instruction
   in `HOW_TO_TEST.md` before claiming full verification.
7. Update help, `README.md`, examples, and this file when command behavior
   changes.
8. Report exact commands, pass counts, and any test not run.

Do not combine the timing-sensitive autostart-cycle tests into one Python
process. Do not replace a failing batch with individually passing tests and call
the suite green. A timeout is evidence to investigate, not a scheduler diagnosis.

## Framework invariants to preserve

- Graph synchronization is explicit; ordinary commands do not add/remove node
  folders based on changed source.
- Hoeflein components are the unit of ordinary scheduling and lifecycle state.
- Selected-job/sample runs touch only selected jobs and do not start descendants.
- Fresh `run`/`runfrom` cleanup is producer-component aware and preserves work
  from unselected merge branches.
- Resume preserves done/skipped work and does not become a hidden fresh run.
- Active restart advances generations before stale work can publish.
- Component failure stops admission, joins started work, and reaches a durable
  terminal boundary.
- SQLite mutation priority must not starve admission, completion, or failure.
- MWF file APIs retain path containment and Windows extended-path support.
- External calls have finite timeouts; retry/fallback loops are bounded.
- Side-effecting nodes are idempotent and publication emits inspectable receipts.
- Validation preserves mandatory model enrichment and source truth.
- `mwf engine`, `doctor`, `inspect`, `trace`, `filter`, `monitor`, `top`, plans,
  and dry-run previews remain non-executing diagnostics.
