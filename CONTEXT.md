# MWF

This context defines Micro Workflow Manager language. Shared Product Workspace
relationships and terms live in `C:\Business\product\CONTEXT-MAP.md`.

## Language

**MWF**:
Micro Workflow Manager, a hybrid file and SQLite workflow manager for directed
graphs of work.
_Avoid_: Workflow engine, micro-workflow-manager

**MWF project**:
A directory containing an MWF graph, node behavior modules, node input and
output directories, and MWF-managed runtime state.
_Avoid_: Scenario, workflow copy

**MWF node**:
A named unit of work with one main task, optional fallbacks, jobs, inputs,
outputs, and routing behavior.
_Avoid_: Agent, step

**MWF NodeRouter**:
The Python router declared for one MWF node.
_Avoid_: Agent router, Agentic File System Router

**Hoeflein component**:
An MWF scheduling group whose nodes communicate through ordinary and autostart
graph relationships.
_Avoid_: Cycle, branch

**Project provenance**:
User-owned files that record how an MWF project produced a durable result.
_Avoid_: Artifact, returned evidence
