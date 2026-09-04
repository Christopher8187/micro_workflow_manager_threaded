# Graph architecture

MWF graph architecture connects the framework's semantic design, raw directed
graph, scheduling components, and acyclic dependency view. Read the relevant
[glossary terms](../../CONTEXT.md#graph-architecture) before changing a graph or
describing its behavior.

## Design from semantic states

Start with [semantic pathing](../../CONTEXT.md#semantic-pathing). Name the
workflow's recognizable start state, durable end state, and any rare
[semantic oases](../../CONTEXT.md#semantic-oasis) between them. Then design the
raw nodes and Hoeflein components that perform each transformation.

This order prevents a graph from becoming a list of implementation steps with
no visible semantic purpose. A semantic path may branch or merge. It remains
acyclic after Hoeflein components are contracted.

For each path, record:

- the Job Scope that enters and leaves it;
- the component or node that owns each durable transition;
- fan-out and fan-in cardinality;
- where retries or circulation are bounded;
- which outputs can trigger downstream chain reactions;
- the acceptance evidence for the settled result.

## Declare the raw graph

`src/graph.py` declares ordinary directed edges. Keep node behavior in
`src/node_behavior/` rather than importing providers, prompts, validation, or
other task code into the graph file.

MWF accepts ordinary edges and one-sided directed fans:

```python
from micro_workflow_manager import fan

EXTRACTORS = ["extract_text", "extract_images"]

EDGES = [
    ("ingest", EXTRACTORS),
    fan(EXTRACTORS, "assemble"),
]
```

A collection on both sides is rejected because it would silently describe a
complete bipartite graph. Split that shape into explicit fans.

Graph synchronization is explicit. `mwf graph src/graph.py` sets the first
graph. After an edit, `mwf graph --update --dry-run` previews folder and edge
changes, and `mwf graph --update` applies them. Removing a node during graph
synchronization can remove its node directory, so inspect and preserve needed
data first.

## Component construction

MWF keeps ordinary edge direction in the raw graph. An edge declared for
autostarting also contributes reverse reachability while components are built.
If `G = (V, E)` and `A` is the set of declared autostart edges, current source
uses this model:

```text
G_H = (V, E union {(v, u) : (u, v) in A})
Hoeflein(G) = strongly connected components of G_H
quotient DAG = G / Hoeflein(G)
```

Only original raw graph edges produce edges between quotient vertices. The
synthetic reverse arcs affect membership, not dependency direction.

Design each [Hoeflein component](../../CONTEXT.md#hoeflein-component) as one
communicating subsystem. Naming one member for an ordinary run or lifecycle
operation normally selects the whole component. Keep independent phases in
separate quotient vertices.

Use autostarting when newly routed work must become available inside the same
communicating subsystem. A normal edge between separate components retains DAG
readiness. Use an internal waiting relationship only for a narrow gate. If most
work waits, separate components will usually express the design more clearly.

## Quotient intervals

`workflow.component_interval(A, B)` returns the half-open quotient interval
`[C_A, C_B)`, where each endpoint names a raw node and selects its whole
Hoeflein component. B must be a strict directed descendant of A. Endpoints in
one component, reversed endpoints, and undirected-only connections are invalid.

The calculation intersects descendants-or-self of A with ancestors-or-self of
B, then excludes B's entire component. It includes every directed route between
the endpoints without enumerating paths. Components contain sorted raw-node
names and appear in deterministic topological order. Calculating the interval
does not prepare or execute work or change stored state.

For `A -> B -> D` and `A -> C -> D`, the interval from A to D contains A, B,
and C. A separate branch from A that never reaches D is excluded. This graph
calculation is the basis for the MWF 0.6.2 between-command work.

## Circulation and termination

A cycle is topology. Decide whether its circulation makes a meaningful
contribution before treating it as healthy or faulty.

A bounded cyclic design should identify:

- the state that changes during each return;
- the condition that settles the component;
- a finite attempt, iteration, or work bound;
- idempotent downstream publication;
- the behavior after success, terminal failure, resume, and fresh execution.

Use the glossary's component-vortex terms only after examining the intended
design and Job Scope. Repeated unresolved scope points to a confusion vortex.
Information overload can sustain a context vortex. A residual orchestration
failure may fit the provisional death-vortex term. Local counters alone cannot
make that judgment.

## Fan-out, fan-in, and producer identity

Give fan-out a stable parent identity and deterministic child keys. Large data
belongs in files. Job parameters identify the work.

Fan-in is an explicit graph node with a durable input boundary. The joining
task sorts contributions, checks missing or duplicate inputs, and publishes one
settled result. It does not depend on worker completion order.

MWF records producer-component identity for generated jobs. In a merge such as
`A -> C` and `B -> C`, a fresh `runfrom B` can rebuild B-produced jobs while
preserving A-produced jobs in C. Node-level output may also need preservation
when retained jobs still depend on it.

## Scheduling and lifecycle

The quotient DAG controls readiness between components. A starting component
requires its external predecessors to be complete. A selected branch may reach
a merge component before an unselected branch does, and that merge component
may reactivate when later work arrives.

Within a running Hoeflein component, ordinary members remain available while
peer work can still arrive. A component failure stops new admission and joins
already-started work before the component reaches a durable failed boundary.
Waiting nodes gate admission based on the configured peer queues; they do not
introduce a new job status.

Fresh execution, resume, restart, reset, clean, and wipe have different data
boundaries. Consult [README.md](../../README.md) for command semantics and
[testing.md](../testing.md) before exercising them.

## Current evidence boundaries

The framework accepts `ctx.node(...).add`, `add_many`, `add_job`, and `add_jobs`
as routing forms. The static autostart scanner currently recognizes only the
literal `add` form. This is a framework risk, not a demonstrated component
failure. Inspect synchronized autostart edges and current source before relying
on another form for component construction.

Engine tests establish graph-only loading, component collapse, loopback access,
token handling, and absence of runtime-layout initialization. They do not show
that a human can read every rendered graph layout. That remains missing visual
evidence, not a demonstrated engine defect.

Sampling tests establish deterministic selection, preservation of unselected
jobs, and planning that does not apply the sample run. They do not exercise a sampled task that tries to
route descendants or circulate through a Hoeflein component. Treat those
stronger isolation claims as missing evidence until a focused regression exists.

The exact semantic contraction algorithm, treatment of noncontractible
subgraphs, fan-out expansion, and graph-viewer behavior remain deferred design
work. The glossary names the views without settling those implementations.
