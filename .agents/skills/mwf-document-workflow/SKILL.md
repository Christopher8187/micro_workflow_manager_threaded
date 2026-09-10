---
name: mwf-document-workflow
description: Create or update an MWF project's root README, node README and RUN files, component guidance, and optional source-organization README.
---

# Document an MWF project

1. Read the framework [README](../../../README.md), relevant glossary and
   architecture pages, and the [workflow documentation guide](../../../docs/workflow-documentation.md).
   Inspect the project's graph, node behavior, tests, commands, durable data,
   and installed MWF version. Finish when each current-behavior claim has source
   or run evidence and intended design is identified separately.
2. Update the project entry point and every affected node's README/RUN pair
   using that guide. Keep shared component architecture and operating knowledge
   in their central README and RUN, linked from member documents. Preserve
   node-specific detail, current defects and repair pointers, and short test
   references. Put historical material under the project's `docs/`.
3. Check producer-qualified carried-forward paths against direct project input
   and the installed version. Verify command selections against current source
   and actual job locations. Record only supported measurements and run findings.
4. Add `src/README.md` only when source organization needs a separate explanation.
   Keep framework term definitions in MWF `CONTEXT.md`; project documents use
   those terms and explain this project.
5. Finish when every graph node has a README/RUN pair, links and heading
   fragments resolve, shared explanations have one owner, and commands and paths
   agree with source. Handle disagreements through `AGENTS.md`. Return changed
   documents, consulted sources, assumptions, and unresolved inconsistencies.
