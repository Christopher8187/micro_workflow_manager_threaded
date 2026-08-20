# MWF context

Shared Product Workspace relationships and terms live in
`C:\Business\product\CONTEXT-MAP.md`.

## MWF terms

### MWF

Micro Workflow Manager, a hybrid file and SQLite workflow manager for directed
graphs of work.

### MWF project

A directory containing an MWF graph, node behavior modules, node input and
output directories, and MWF-managed runtime state.

### MWF node

A named unit of work with one main task, optional fallbacks, jobs, inputs,
outputs, and routing behavior.

### MWF NodeRouter

The Python router declared for one MWF node. It defines that node's tasks,
fallbacks, concurrency, waiting rules, and dynamic routing behavior.

### Hoeflein component

An MWF scheduling group computed from the graph after autostart relationships
add their reverse communication direction. MWF schedules every member of a
nontrivial Hoeflein component as one communicating unit.

### Project provenance

User-owned files that record the inputs, decisions, tools, models, parameters,
validation, or attempts used to produce a durable result. Project provenance is
separate from scheduler diagnostics in `.mwf`.
