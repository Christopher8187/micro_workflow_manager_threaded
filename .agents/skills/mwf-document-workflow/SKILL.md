---
name: mwf-document-workflow
description: Create or update documentation for a project built with MWF, including the project README, every node README, and an optional source-organization README.
---

# Document an MWF project

1. Read the framework `README.md`, relevant glossary and architecture pages,
   then inspect the project's graph, node behavior, tests, commands, and durable
   data layout. Finish when documentation claims can be tied to current source
   or an explicit intended design.
2. Make the project-root `README.md` the project entry point. Explain its purpose,
   graph and semantic paths, Hoeflein components, design philosophy, setup, run,
   inspection, and important operating and recovery boundaries, the
   output-provenance layout, and links to every node README.
3. Create or update `node/<node-name>/README.md` for every graph node. Explain
   the node's role and Job Scope, jobs, main and fallback tasks, parameters,
   inputs, outputs, routing, functional and validation hierarchies,
   validator-fallback balancing, fallback context control, runner, concurrency,
   timeouts, idempotency, and output-provenance layout. Keep routine restart, recovery,
   and cleanup instructions in the root README unless the node has a special
   operating exception.
4. Add `src/README.md` only when source organization needs explanation beyond
   the root and node documents. Keep framework term definitions in MWF
   `CONTEXT.md`; project documents use those terms and describe this project.
5. Check every graph node has one README, every link resolves, paths agree across
   connected tasks, and disagreements are visible under the `AGENTS.md` process.
   Return changed documents, consulted sources, assumptions, and unresolved
   inconsistencies.
