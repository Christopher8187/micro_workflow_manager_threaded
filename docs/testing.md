# Testing model

MWF testing separates correctness, failure and recovery behavior, state reuse,
and measured performance. The `mwf-test` skill owns the detailed command
sequence. This document owns the model and the conditions that select each kind
of check.

## Regression requirement

Every feature change requires regression coverage for its observable behavior.
Every bug fix requires a regression that fails without the fix. Cover failure
and recovery as well as the successful path when the change affects retries,
fallbacks, lifecycle state, routing, storage, or restart fencing.

A regression should state the user-visible behavior, create the smallest
representative state, and assert durable results. Prefer direct state or event
observations to wall-clock guesses. A timeout is evidence to investigate, not a
scheduler diagnosis.

Tests and benchmark programs are executable changes. Obtain the approval
required by the active task before adding or changing them.

## Documentation-only changes

When every task-owned change is documentation or an instruction-only
`SKILL.md`, do not run pytest or another executable suite. Check local links and
heading fragments, fenced blocks, referenced paths, terminology ownership, and
documented file lists. Compare claims about current behavior with the relevant
source and tests. When delegation is available and authorized, ask an
independent reviewer to check that the changed documentation matches the
implemented behavior.

If the change also touches source, tests, benchmark programs, examples, runtime
configuration, or another executable file, use the isolated sequence below.

## Isolated sibling Test Area

Derive the Test Area from the exact MWF source tree under test:

```text
<mwf-source-parent>/test_area
```

The source may be the normal working directory or a linked worktree. Resolve
that exact directory first, then use its parent. Do not redirect testing to a
fixed machine path or another checkout.

Create a unique run directory inside the Test Area. Keep the source working
directory untouched while tests run.

When testing another project built with MWF, use that project's exact source
tree to derive its sibling Test Area. Copy the project, install its declared
dependencies and recorded MWF wheel or version, and run from the copy. Do not
reuse the framework source tree as an editable installation unless the test
explicitly targets that combination.

For framework testing, copy the exact source tree into the run directory and
run pytest against the copy. Exclude only repository metadata, local virtual
environments, interpreter caches, and build output that will be recreated. The
copy must preserve tracked changes and relevant untracked source files from the
selected tree.

For example testing, build or select the intended wheel, copy the wheel and the
example into the run directory, create an isolated environment, initialize MWF
there, and run from the copied example. Do not import the editable framework
source by accident. A failing example remains evidence. Repairing it is a
separate change.

Failed run directories and short failure records may remain only while the
failure is active. Mark their source path, commit, changed-file state,
interpreter, command, and failure, and never add those records to the repository.
After recovery succeeds and the result is recorded, remove the temporary record
and copy. Before cleanup, resolve the exact path and confirm that it is inside
the intended Test Area.

## Test categories

### Focused regression

Run the new or closest regression first. It provides the shortest feedback loop
and confirms that the intended behavior has a direct check.

### Adjacent behavior

Run modules that share the changed scheduler, storage, CLI, filesystem, runner,
or lifecycle boundary. This catches a narrow fix that changes a neighboring
invariant.

### Ordinary suite

Run the ordinary pytest suite as a batch when practical. Dividing a large run is
acceptable when the division has a clear category or dependency boundary. A
fixed harness timeout does not define the batches, and individually passing
files do not replace a failing combined run.

The repository's pytest settings exclude the `stress` marker by default and
treat warnings as errors.

### Cyclic and timing-sensitive cases

`tests/test_autostart_cycles.py` exercises process-global scheduler and worker
lifecycle under cyclic load. Run each test in a fresh pytest process. Combining
them changes the state being tested.

### Stress cases

Run marked stress tests explicitly when the change affects their path or when
release verification requires them. Record the selected marker and module.

### CLI and repeated-use cases

Exercise relevant commands more than once. Include dry-run or plan modes,
confirmation boundaries, reuse after failure, and the second invocation of
stateful operations. Read-only claims need checks that observe the filesystem
and database before and after the command.

### Example acceptance

An example file-existence smoke test establishes layout only. Running the graph,
tasks, fallbacks, routing, validation, and recovery establishes different
behavior. State exactly which level ran.

### Release artifacts

Release verification tests the built wheel and source archive, not only the
working tree. Inspect artifact metadata and file lists, extract the source
archive into a fresh Test Area run, and repeat the ordinary, cyclic, and
release-selected stress checks there. Install the copied wheel in a separate
fresh environment and verify its version, import location, CLI, and installed
package files. A syntax-compilation pass does not establish import-time or
runtime behavior. The `mwf-test` skill owns the detailed sequence.

## Benchmarks and diagnostics

Run a relevant benchmark when an approved change can affect an existing
measured path or when Christopher requests it. Measured paths include:

- scheduling and Hoeflein-component behavior;
- concurrency, admission, and runner control;
- networking and transport recovery;
- SQLite and filesystem persistence;
- event recording and terminal publication;
- high fan-out or fan-in behavior;
- another path already measured by a program in `benchmarks/`.

An ordinary change with no plausible performance effect does not run every
benchmark.

Benchmark results need the MWF version and commit, source dirty state, machine
and interpreter details, command and parameters, repetitions, raw measurements,
and an explicit comparison threshold. A program that prints an error or failed
job count but exits successfully is diagnostic output, not a passing gate.

The three saved result files predate this evidence standard. Treat them as
historical observations. [benchmarks/README.md](../benchmarks/README.md) records
their exact limits.

## Failure diagnosis

When a test stalls or times out, inspect:

- active run and Hoeflein-component state;
- queued, running, done, and failed job counts;
- job events and checkpoint deadlines;
- provider or external waits;
- live threads, fibers, and child processes;
- SQLite writer backlog and WAL growth;
- file-descriptor or Windows handle pressure.

Compare the same workload at low, moderate, and declared concurrency before
changing scheduler behavior. Separate harness failure, task failure, transport
failure, validation failure, and terminal-publication delay.

## Reporting

Report the exact source tree and commit, dirty files copied, environment, wheel
when applicable, commands, selected tests, pass and failure counts, retained
failure directories, benchmark parameters, and every check not run.

Keep evidence categories precise. A demonstrated contradiction has a direct
failing behavior or broken instruction. A framework risk has a plausible path
without a confirming regression. Missing evidence means the current checks do
not establish the claim. Undecided behavior needs a design decision before a
regression can define correctness.
