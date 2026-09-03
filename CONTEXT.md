# MWF language and boundaries

This file is the authoritative glossary for Micro Workflow Manager. Shared
Product Workspace terms and relationships live in
`C:\Business\product\CONTEXT-MAP.md`.

Use standard filesystem, Git, graph, and Python terms when they fit. Add an MWF
term only when the framework needs a distinction that standard language does not
carry. Architecture documents link here instead of redefining these terms.

## Framework and project

### MWF

Micro Workflow Manager, the hybrid file and SQLite manager for directed graphs
of work.

### MWF project

A directory containing an MWF graph, node behavior modules, node input and
output directories, and MWF-managed runtime state.

### Output provenance

The user-owned, navigable filesystem tree rooted at one node's output prefix.
It retains the node's results and enough useful intermediate information to
inspect how those results developed. It may contain any file types and combine
work from many jobs. Output provenance is not a required file, record, manifest,
schema, or filename.

Every node has one framework output prefix: `node/<node-name>/output/`.
Project code may organize subtrees beneath it. A debug file is one possible
diagnostic artifact, not output provenance itself. Framework runtime events
describe execution and complement this tree; they do not define its layout.

## Architecture scales

MWF has three architecture scales, from largest to smallest:

1. graph architecture;
2. node architecture;
3. task architecture.

Graph architecture concerns connectivity and collective behavior across nodes.
Node architecture manages one node's main task, retries, fallback tasks,
validators, node filter, and escalation. Task architecture defines one task's
transformation, routing, validation, parameters, and filesystem behavior.

The raw graph is part of graph architecture. It is not a fourth scale.

## Graph architecture

### Raw graph

The complete directed node-and-edge graph before component contraction. A raw
graph may contain directed cycles.

### Hoeflein component

An MWF scheduling and lifecycle group whose nodes use Hoeflein-component
queueing and may circulate jobs through declared internal routes. MWF builds
components from the raw graph and autostarting relationships. The raw subgraph
does not need to be strongly connected without those relationships.

### Quotient DAG

The acyclic graph produced by contracting each Hoeflein component into one
vertex. Its edges retain the direction of raw graph edges between components.

### Cycle

A directed topological cycle in the raw graph. A cycle does not by itself imply
a runtime failure.

### DAG queueing system

The system that schedules work along the quotient DAG.

### Hoeflein queueing system

The system that governs work inside a Hoeflein component. Component members
share lifecycle behavior and are ordinarily available concurrently.

### Autostarting

The architecture decision that a node relationship overrides ordinary DAG
sequencing and uses Hoeflein-component queueing. Separate names such as
`component autostart` and `autostart node` add no shared distinction.

### Internal waiting relationship

An exceptional admission gate between members of one Hoeflein component.
Ordinary component design makes members available together. Heavy waiting can
indicate that the gated work belongs in separate quotient-DAG vertices.

### Normal job circulation

Collective movement of jobs through a Hoeflein component when that movement
contributes to the component's intended design. Returns, waves, retries,
fan-out, and temporary growth can all be normal circulation.

### Meaningful contribution

A step serves the intended design of its Hoeflein component. Bytes changed,
elapsed time, queue count, or job count cannot establish this alone.

### Component vortex

Jobs circulate inside a Hoeflein component without meaningfully contributing
to its intended design. Vortex classification depends on meaning, not scale,
duration, growth, or job count. A quotient DAG cannot vortex because it is
acyclic, though it can carry a chain reaction.

Use `cycle` for topology and `component vortex` for this semantic failure.

### Confusion vortex

A component vortex in which substantially the same unresolved Job Scope keeps
circulating without serving the component's design. Relabeling or redirecting a
job is not progress when its underlying scope stays unresolved.

### Context vortex

A component vortex sustained by information overload. Context grows, spreads,
or becomes entangled until the accumulated information prevents the component
from settling meaningfully. A context vortex may overlap a confusion vortex.

### Death vortex

The provisional residual class of component vortex that is neither a confusion
vortex nor a context vortex. Its sustaining mechanism lies in orchestration such
as routing, fan-out, waiting, queue interaction, or connectivity. No observed
MWF case yet supports a mature definition.

### Refinement measure

Provisional evidence that continued circulation moves a component toward its
intended settled state instead of reproducing the same architecture failure. It
need not be numeric or monotonic.

### Chain reaction

An earlier error or unsuitable result persists through job transfers and causes
erroneous jobs or durable results downstream. A chain reaction needs neither a
cycle nor a component vortex. Tracing and recovery for chain reactions belong
to the MWF 0.6.2 workflow-management discussion.

### Semantic oasis

A rare quotient-DAG vertex at which data arrives in a coherent and precisely
recognizable semantic form. The vertex may represent one raw node or one whole
Hoeflein component.

### Semantic pathing

The top-level graph-design practice of organizing a workflow around its start
state, end state, and rare intermediate semantic oases. A semantic path may
branch and merge. The lower raw nodes and components perform the transformation
between oases.

## Node architecture

### MWF node

A named graph unit that manages jobs through one main task, optional fallback
tasks, routing, and validation.

### MWF NodeRouter

The Python router declared for one MWF node. It is not the same concept as a
routing node.

### Main task and fallback task

The main task is the first implementation attempted by a node. A fallback task
is a later implementation that can satisfy the same task output requirements.
Use these names instead of `node task` and `node fallback` when the longer names
add no distinction.

### Retry

Another attempt of the same task implementation at the same functional level.

### Escalation

Movement from the current task stage to a later fallback stage. An escalation
may change configuration, model, process, or implementation. Adjacent stages
with identical implementation, inputs, configuration, and context add no useful
response even though they remain structurally separate stages.

### Functional hierarchy

The ordered main task and fallback tasks.

### Functional gradient

The compute or cost curve across the functional hierarchy. Reliability may
justify the curve but is not part of its definition. The curve may rise, fall,
or vary between stages.

### Validation hierarchy

The ordered validator policies across the filter stages.

### Validation gradient

The strictness curve across the validation hierarchy. Greater strictness accepts
a smaller subset of possible outputs. The curve describes programmed
strictness, not compute or observed pass rate.

### Validator-fallback balancing

The node-architecture practice of tuning validator strictness, retries,
fallback ordering, functional cost, and escalation together. It relates the
functional and validation gradients without merging them.

### Fallback context control

The design principle that a later fallback may deliberately use earlier outputs,
validation results, exceptions, or other failure information. Under the settled
0.6.1 behavior, the framework makes prior evidence available and the active
fallback chooses what to interpret. See the
[node architecture document](docs/architecture/node.md#fallback-context-and-failure-lineage)
for release-specific behavior.

### Node filter

The complete node-scoped attempt sequence for a job. It includes the main
attempt, main retries, fallback attempts, and fallback retries.

### Filter stage

One task attempt. Every initial attempt, retry, fallback attempt, and fallback
retry is a separate filter stage.

### Clean and dirty filter stages

A clean filter stage accepts most jobs reaching it. A dirty filter stage rejects
many. Cleanliness describes observed rejection frequency, not objective
correctness.

### True acceptance

Suitable work is accepted.

### False acceptance

Unsuitable work is accepted.

### True rejection

Unsuitable work is rejected for an intended reason.

### Incidental true rejection

Unsuitable work is rejected for an unrelated or unplanned reason.

### Designed rejection

A task performs its semantic job correctly, but the architecture intentionally
rejects the result for later confirmation, review, or stabilization.

### False rejection

Suitable work is rejected unintentionally because the task or validator policy
is wrong.

### Validation failure

The observable event in which the validation layer rejects an attempt. The
event alone does not determine which acceptance or rejection category applies.

### Filter rejection

Any attempt rejected by the node filter. This includes validation failures,
timeouts, exceptions, parser failures, transport failures, and other task
failures.

### Validation ghost

A false acceptance or false rejection that ordinary filter observations cannot
identify without human review, external ground truth, or a later correction to
the intended policy.

### Routing node

A node whose main role is transferring jobs, data, parameters, and state between
graph regions. Its functional and validation layers may be trivial or absent.

### Fan-out node

A routing node that creates or routes work to multiple children.

### Node job backlog

Queued, unclaimed jobs awaiting execution in one node. Running jobs are
excluded. A running job that makes no progress is a runtime-health or
state-transparency problem, not backlog work.

### MWF run session

One framework-owned execution record or sequence.

### Full workflow run

Execution of the complete intended end-to-end workflow region. Not every MWF
run session is a full workflow run.

### Node clipboard snapshot and restore

The operations exposed as `mwf copy` and `mwf paste`. They copy stored node
state, not Python behavior or graph edges, so they are not full node duplication.

## Task architecture

### Job function

The transformation a job performs from input to output.

### Job Scope

The loose semantic portion of a workflow's total work assigned to one job. It
may be a subtask or a subset of a larger input. Metadata may describe Job Scope,
but metadata does not constitute it.

### Routing layer

The task layer that resolves inbound files, data, parameters, and state, then
publishes validated work to downstream inputs. Inbound and outbound routing
happen at different times but remain one layer.

### Functional layer

The task layer that performs the job's transformation.

### Validation layer

The task layer that applies deterministic checks to the functional result.

### Task interface

The task's accepted inputs, produced outputs, parameters, filesystem locations,
routing behavior, functional transformation, and validation behavior.

### Carried-forward input path consistency

Filesystem correctness between connected tasks. Outbound routing places or
references data at the path expected by the receiving task's function.
Connected tasks may use different relative paths as long as their task
interfaces agree.

## Release boundaries

MWF 0.6.1 owns this documentation hierarchy, AFSR instructions, isolated
testing model, failure lineage, durable per-attempt failure events, trace error
views, removal of per-job file storage and `ctx.transaction()`, narrow CLI and
benchmark corrections, and their approved regression coverage.

Issue 44 owns the contested MWF 0.6.2 workflow-management changes. Proper
example implementation, including correction of the mistaken output-history
framing in examples, remains MWF 0.6.3 work.

The provisional future items currently collected as 0.6.4 still need a later
grilling session to settle the exact 0.6.3 versus 0.6.4 boundary and Wayfinder
shape. The long-term compatibility boundary for the public Python API remains
undecided.
