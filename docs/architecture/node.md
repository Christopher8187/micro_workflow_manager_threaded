# Node architecture

An MWF node manages one Job Scope through a main task, optional fallback tasks,
validation, routing, and one configured runner. Read the
[node glossary](../../CONTEXT.md#node-architecture) for the authoritative terms.

## Design one node

Record these decisions before writing or changing node code:

- the node's role in the graph and the Job Scope it receives;
- required parameters and durable file inputs;
- durable result, output-provenance layout, and downstream routes;
- main task and fallback implementations;
- retry and repetition counts;
- validator policies and their strictness by filter stage;
- runner, concurrency, timeouts, and checkpoints;
- idempotency and replay behavior;
- replay and idempotency behavior after a task failure or fresh execution.

A [routing node](../../CONTEXT.md#routing-node) may have little functional or
validation work. A [fan-out node](../../CONTEXT.md#fan-out-node) is the narrower
role that creates or routes multiple children. Neither term means
[MWF NodeRouter](../../CONTEXT.md#mwf-noderouter), which is the Python declaration.

## Node README

Each project node should have `node/<node-name>/README.md`. Describe the node's
role and Job Scope, main and fallback tasks, parameters, input paths, output
layout, downstream routes, functional hierarchy, validation hierarchy,
validator-fallback balancing, fallback context control, runner, concurrency,
timeouts, checkpoints, idempotency, and replay behavior.

Do not fill every node README with routine command instructions for restart,
recovery, or cleanup. Put project-wide operating procedures in the root README
and keep framework command detail in `docs/operations.md`. A node README should
mention an operating exception only when that node has a special boundary that
changes how an operator must act.

The README is a documentation standard, not a runtime validity condition. MWF
0.6.1 does not generate it automatically.

## Functional and validation hierarchies

The functional hierarchy orders the main task and fallbacks. Retries stay at
the same functional point. Escalation moves to a later fallback. Give each
fallback a distinct response to evidence already gathered. Repeating the same
implementation, configuration, inputs, and context under a different name adds
no useful recovery behavior.

The functional gradient tracks compute or cost. The validation gradient tracks
strictness. They may both rise or fall, but they measure different things.
Balance them so suitable work passes, unsuitable work escalates, and the final
stages do not manufacture success by dropping mandatory output or source truth.

A node filter contains every main attempt, retry, fallback attempt, and fallback
retry. Each attempt is one filter stage. Repetition counts are annotations on
the applicable stage rather than separate stages. Filter counts
describe observed passage and rejection. They do not determine whether an
acceptance or rejection is true, false, incidental, or designed.

An intentionally dirty early stage can be useful when designed rejections route
uncertain work to later confirmation. Designed rejections should approach zero
in the final stages because no later confirmation stage remains.

## Fallback context and failure lineage

Fallback context control lets a later stage choose evidence from earlier
attempts. In the settled MWF 0.6.1 behavior, one execution sequence maintains an
ordered history of the original Python exception objects raised by:

- failed main attempts and retries;
- failed repeated attempts;
- failed fallback attempts and fallback retries.

The history belongs to one job's current execution sequence. It never imports
errors from upstream nodes. The first main attempt receives an empty history.
Each later attempt receives every earlier failure in order.

Task code reads a fresh list through `ctx.errors` or an optional reserved
`errors` parameter. It reads the immediately preceding exception through
`ctx.error` or an optional `error` parameter. For any later attempt,
`ctx.error == ctx.errors[-1]`. Local list mutation does not change the
framework's internal history.

The last failed task becomes the terminal job error. A fresh run, resume, or
restarted execution starts with an empty live history. Stored text is durable
diagnostic data, not a reconstructed Python exception.

Use compatible exception shapes through one recovery chain when that makes
fallback code simpler. MWF does not require one exception class or attribute
set because different recovery strategies may need different failures.

### Current implementation

The checked-in executor creates a new internal history for each call to
`execute_with_fallbacks()`. It appends each ordinary failed task attempt before
retrying or escalating, uses the latest entry as the next attempt's previous
error, and passes fresh list copies to task code. A `JobRestartedError` leaves
the current execution without entering ordinary failure history. It also records
each ordinary failed attempt as a durable `task_failed` event. Trace renders
those events normally and provides the failure-focused view described below.
Filter uses those same events for stage errors.

## Durable failure events and inspection

The settled 0.6.1 journal records one `task_failed` event for every failed task
attempt. Each event identifies the task, main or fallback role, attempt,
repetition, safely rendered error, and journal time.

Normal `mwf trace NODE job ID` places those failures in chronological context.
`mwf trace NODE job ID --errors` limits the display to job identity and origin,
ordered task failures, task and attempt details, terminal state, and terminal
error. The option filters display; recording is always on. `mwf filter` consumes
the same failure events rather than inferring one stage's error from the next
transition.

## Runners and concurrency

Choose a runner by the task's dominant cost:

- `direct` for deterministic single-process work and debugging;
- `threaded` for local blocking I/O and bounded parallel work;
- `api` for high-latency external calls through cooperative jobs and the shared
  HTTP transport;
- `process` for CPU work whose parameters, result, and imported code can cross
  a process boundary.

`max_threads` is node job concurrency. It does not promise that a provider,
database, host, or network can sustain the same number of active operations.
External calls need finite timeouts. Use the shared HTTP transport instead of
constructing one client for every job or retry.

Waiting declarations are intra-component admission gates. A target outside the
node's Hoeflein component is invalid. A waiting singleton adds no DAG behavior.

## State, restart, and recovery

SQLite is authoritative for scheduler state. User payloads and output
provenance remain inspectable files. Generation and execution fencing prevent a
timed-out, restarted, or stale handler from publishing MWF-managed files, state,
or child jobs.

Resume preserves successful work and continues eligible unfinished work.
Restart operates inside an active run session and advances the execution fence
before replacement work publishes. Recovery is for work abandoned by a dead CLI
owner. None of these operations should be treated as synonyms for a fresh run.

Use `mwf inspect`, `mwf trace`, `mwf filter`, `mwf monitor`, and `mwf top` to
separate a node job backlog from running work that has stopped making progress.
Read [testing.md](../testing.md) before changing scheduler or recovery behavior.
