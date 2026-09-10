# Document an MWF workflow

Use this guide when creating or updating a project's documentation. Inspect its
graph, task source, tests, and installed MWF version first. Describe current
behavior and distinguish known defects from the intended design.

## Project entry point

Begin the project-root `README.md` with a human-readable workflow summary.
Explain its purpose, setup, graph idea, and important operating boundaries.
Link to every node README and the relevant RUN files. Add `src/README.md` only
when source organization needs its own explanation.

## Node README

Each graph node has `node/<node-name>/README.md` and
`node/<node-name>/RUN.md`. Begin the README with a short explanation of what
happens to work at that node. Put node and task architecture below it:

- role, Job Scope, parameters, file inputs, output provenance, and routes;
- main task, retries, fallback order, models and configuration, validation,
  validator-fallback balancing, and use of earlier failure context;
- runner, concurrency, timeouts, checkpoints, idempotency, and replay behavior;
- waiting or interrupt boundaries that affect this node's design.

Explain only details that apply. Link the node's RUN and short, relevant tests.
Record a known current defect in the affected node README with a link to its
planned repair. Keep the intended behavior distinct from the observed failure.
Full test explanations belong in linked documentation; each node document does
not need an exhaustive claim-to-evidence table.

## Component and shared architecture

Choose one central node README for each multi-node Hoeflein component. Keep its
graph explanation brief and focused on the workflow idea, including meaningful
circulation and any known component vortex. Explain overlapping handler
architecture and shared fallback models, stages, and order there. Separate this
shared behavior from the central node's own tasks, and link to every member's
README for differences and node-specific detail. Use the stages and models the
project actually implements; no fixed count applies to all MWF projects.

## Node RUN

RUN holds concise current operating knowledge for work that begins at its node.
Useful material includes API costs, filter statistics, brief job problems,
findings that affect present operation, and relevant sampling or reset commands.
Record the scope and evidence for reported costs or results. Give commands for
the installed version, including the relevant component and job selection, and
explain where prepared jobs reside when that matters to using them.

A handler RUN may be very short and link to the component's central RUN. Keep
shared commands there rather than repeating them in every handler. Include
current testing information and useful results; link fuller explanations and
framework command semantics in [operations.md](operations.md). A RUN does not
need exhaustive procedures or speculative analysis of every pre-run risk.

Keep README and RUN current. Remove obsolete guidance when the replacement
model is settled. Put historical runs and superseded designs under the
project's `docs/`, retaining only currently useful observations in RUN. If a
measurement or operating choice has not been established, say so without
inventing a result or command.

## Input paths and framework version

MWF 0.6.2 distinguishes carried-forward input from project-owned direct input:

- `node/A/input/B/...` is input received at A from the actual producing raw
  node B, including when both nodes share a component.
- `node/A/input/prompt.md` is an example of a project-owned file used directly
  by A. It does not acquire a producer prefix merely because A reads it.

Document the actual filenames and producer/receiver pair for every route.
Check exact-path and fixed-depth reads against the published tree. The
[task filesystem guide](architecture/task.md#filesystem-boundaries) owns the
framework behavior. For a project pinned to an earlier MWF version, describe
that implementation until migration changes it.

## Verify the documentation

Check every graph node has a README/RUN pair, central component and handler
links resolve, commands and paths agree with source, current defects have
repair pointers, and results have evidence. Review changed claims against
source and relevant tests, then check local links and heading fragments.

README and RUN are documentation requirements. MWF does not generate these
files or require them for runtime validity. Existing example completion and
repair remain in the MWF 0.6.3 work.
