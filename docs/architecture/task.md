# Task architecture

Task architecture joins one job's routing, transformation, and validation into
one task interface. Read the authoritative
[task terms](../../CONTEXT.md#task-architecture) before documenting or changing
a task.

## Start with Job Scope

State the Job Scope in domain language before choosing parameters or filenames.
Then write down:

- the job function;
- required parameters and file inputs;
- the durable result and output-provenance layout;
- the inbound and outbound paths;
- deterministic validation rules;
- side effects and idempotency behavior;
- timeout, checkpoint, retry, and fallback boundaries.

Job metadata may describe scope, but it is not the scope itself. Keep job
parameters compact. Put substantial data in files and pass stable identifiers
or relative paths.

## Three layers

Inbound routing resolves the job's files, data, parameters, and state. The
functional layer performs the transformation. Validation applies deterministic
checks to the result. Outbound routing publishes only the work that the task is
allowed to carry forward.

Inbound and outbound work remain one routing layer even though they occur on
opposite sides of the transformation. The node and framework own retries,
fallback selection, and escalation. A task should not hide a second retry or
fallback system inside its function.

Separate provider transport, response parsing, semantic validation, and quality
evaluation when they fail for different reasons. A parser establishes shape. A
validator enforces intended meaning. An evaluator judges quality. Combining
them can turn a recoverable formatting difference into a false rejection or let
an unsuitable result pass because it parsed.

## Parameters and reserved names

The first handler parameter is `ctx`. Later parameters form the task's accepted
job parameters. Required Python parameters are required job inputs. Defaults
make parameters optional.

Current routing consumes `job_id`, `autostart`, and `idempotency_key` as
framework controls. The executor reserves `error` for the previous failure. The
current branch also reserves `errors` for ordered live failure history.
Do not use these names for unrelated project data.

The full compatibility policy for the public Python API remains undecided. Check
current source and tests before relying on an undocumented alias or signature.
The checked-in source currently exports 16 names lazily and aliases
`GraphJobSystem` to `MicroWorkflow`; those are implementation facts, not a
settled long-term compatibility promise.

## Filesystem boundaries

Declare filesystem objects beside the NodeRouter and bind them inside a task:

- `InputFileSystem` reads the current node's input directory;
- `OutputFileSystem` reads and writes the current node's single output prefix;
- `NodeInputFileSystem` writes a connected node's input and routes jobs there.

In MWF 0.6.2, forwarding `evidence/source.json` from raw node A to B writes
`node/B/input/A/evidence/source.json`. The visible prefix is the actual producing
raw node, including inside a Hoeflein component. It contains no producer job
number. `NodeInputFileSystem` applies its bound `base` below that producer
prefix, including for `write_jsons()` batches.

Managed forwarding records the exact producing execution independently of
optional trace. Each batch waits for a durable ownership and trace decision.
A synchronous file or metadata failure restores the prior files. If restoration
cannot complete, MWF retains recovery material and refuses further managed
publication and ownership reads for that receiver. Native recovery uses the
saved decision to restore the prior files of an uncommitted publication or
preserve a committed publication, then removes its known recovery files. It refuses when ownership,
visible files, or saved recovery material no longer match that decision.

Managed input changes and newly created jobs mark done, sampled, or failed receivers
misaligned while preserving their established results. The publication decision records one first
cause per receiving raw node and alignment generation, using the actual changed
path or created job. An arrival does not set misalignment while a receiver is
queued or running. Repeated arrivals retain the first cause; manual filesystem
edits trigger no detection. Full fresh preparation
advances the generation and removes old causes from the current view. Idempotent
job reuse creates no arrival. Job causes retain the receiving instance even after
trace clearing, deletion, or numeric ID reuse. See
[receiver input progress](../plans/0.6.2/receiver-input-misalignment-progress.md) and
[managed job progress](../plans/0.6.2/managed-job-arrivals-progress.md).

Full component preparation removes the selected producers' managed jobs and input
files at excluded receivers, preserves other producers' material, and records a
preparation-removal cause for an established receiver result. It checks the whole
mutation footprint and holds short-lived receiver guards before moving files.
Durable receipts keep filesystem restoration consistent with the SQLite decision,
including failures reported after commit. Selected-job preparation and all
interval commands use the same producer identities and receiver protections.
Native recovery resolves unfinished preparation from its durable decision.

An overwritten or appended path can retain several producing executions.
Overwriting an existing unowned file also preserves that predecessor in the
ownership record. Ambiguous ownership refuses automatic owner selection;
current byte-writing behavior is retained. Explicit unique-name copies record
ownership against the actual path returned to the task.

The receiver names the producer explicitly, for example
`ctx.input_path("A", "evidence", "source.json")`. Receiving-input listings use
fixed-depth patterns such as `ctx.input_files("A/*.json")`. Recursive input
listing and `**` patterns are rejected. Input `read_jsons()` defaults to a
fixed-depth read; output `read_jsons()` retains its recursive default. Output
tree traversal remains available.

These objects keep path containment, generation fencing, Windows extended-path
support, and trace events inside the framework boundary. Use raw paths for
source or other files outside MWF-managed data only when the project explicitly
owns that boundary.

Generation fencing applies to managed write and copy methods. A filesystem
entry's `.path`, path-like conversion, `ctx.output_path()`, and a writable
`.open()` handle are escape hatches for third-party libraries. MWF checks a
writable handle before opening it, but cannot fence or roll back later writes
made through a retained raw path or handle.

Connected tasks can use different relative paths. Carried-forward input path
consistency requires the sender to publish the exact path the receiver expects.
Test both ends together. A file existing somewhere under `node/` is not enough.

`NodeInputFileSystem` is a normal MWF feature for workflow data when a declared
edge permits the route. It is not a workaround for missing framework failure
history. Failure lineage stays inside the executing job.

## Durable result and output provenance

Every node has one framework output prefix:

```text
node/<node-name>/output/
```

Output provenance is the navigable user-owned tree below that prefix. It can
hold stage results, diagnostics, intermediate representations, and final
results in any useful formats, including directories, JSON, text, PDFs, images,
video, office documents, and archives. It is a design principle for useful and
navigable output, not a required file or schema.

An `OutputFileSystem(base="...")` selects a subtree below the same prefix. It
does not establish another framework output prefix. A task may partition its
tree by stable document, section, or job identity when concurrency requires it.
Prefer a relative organization similar to the receiving input tree so users do
not have to learn two unrelated navigation systems.

Keep large content in files and route relative locators in job parameters. Node
output is not carried forward automatically. The sender must use a connected
`NodeInputFileSystem` or another explicitly project-owned route to place the
exact path the receiver expects. Avoid duplicating one large object in every
job payload.

Per-job storage contains only `input.json` and `output.json`. MWF 0.6.1 has no
`JobFileSystem`, `ctx.write()`, `ctx.write_bytes()`, `ctx.files_dir`, or
automatic copying of a returned `Path`, `file`, or `files` value. Such values
remain ordinary return data. Use `OutputFileSystem`, `ctx.write_output()`, or
`ctx.write_output_bytes()` for managed output.

Framework events explain scheduling and execution. They complement the output
tree without prescribing its shape. Keep secrets and unnecessary personal data
out of both.

For fan-out, give every child deterministic identity and stable input paths.
Use `ctx.node(TARGET).add_many(params, idempotency_keys=keys)` for a same-node
batch. For cross-node fan-out, precompute child specifications and call `add()`
with one explicit idempotency key per child. MWF 0.6.1 has no public staging
helper around several `add()` calls.

MWF 0.6.2 records the executing task as the creator of a new job made through
`add()`, `add_many()`, `add_job()`, or `add_jobs()`. Logical parent metadata is
separate. Clearing optional trace preserves this operational ancestry, and
idempotent reuse keeps the reused job's original creator. A retained handle
cannot publish after its producer finishes or while another task is active.
Default job declarations and calls outside a task have no task creator.

Selected runs admit exact selected roots and same-component jobs newly created
by that invocation, including further causal circulation. They preserve
unrelated preexisting jobs. Jobs published to another quotient component remain
unexecuted until a command selects that component and readiness permits it.

For fan-in, sort inputs by stable identity, check the expected set, reject
duplicates or missing required inputs, and write one assembled result. Thread
completion order must not decide output order.

## Validation and filter behavior

Write deterministic checks for source truth, required identifiers, schemas,
path agreements, safety rules, and publication gates. Optional candidates can
often be checked independently. One bad optional item should not erase valid
siblings unless the task interface makes the set atomic.

Observed rejection does not establish semantic correctness. Classify outcomes
with the acceptance and rejection terms in `CONTEXT.md`. A validation failure
can be intended, incidental, or wrong. A validation ghost needs human review,
external ground truth, or a later correction to expose it.

Before changing a validator, use fixtures for ordinary valid output, equivalent
formatting, recoverable optional defects, invalid required values, mixed
optional candidates, and empty output when enrichment is mandatory. Change the
narrowest responsible layer.

## Side effects and cancellation

Use stable idempotency keys at downstream job creation, database mutation,
upload, or publication boundaries. `JobContext.side_effects()` can group a
short sequence of local fenced mutations. Keep network waits and long
computation outside that scope so restart is not delayed.

Tasks with external calls need finite client timeouts even when MWF task and
checkpoint deadlines exist. Check cancellation before publishing. A stale task
must not write output, forward input, or create child jobs after its generation
has been replaced.

Every feature and bug fix affecting task behavior needs regression coverage.
Use [the testing model](../testing.md) and the `mwf-test` skill.
