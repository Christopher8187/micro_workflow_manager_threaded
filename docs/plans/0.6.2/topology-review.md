# Shared-topology correction review

## Disposition

**Reviewer:** independent `gpt-5.6-sol`, `xhigh` reasoning
**Selected tree:** `C:\Business\product\test_area\mwf-062-issue45-20260904\topology-only`
**Accepted staged-tree object:** `3c00f0a46ccd44b5b9f026eb343591fe27ec3325`
**Preparation base:** command-retirement tree `fa1ee43d5716f1777410fc078961317d752dcee6`

I found no actionable code or documentation defect in the selected shared-topology correction. I recommend accepting staged tree `3c00f0a46ccd44b5b9f026eb343591fe27ec3325` for this narrow scope. The tree passes the complete valid performance gate and `git diff --check`. This acceptance must not carry acceptance for the unfinished SQLite preview behavior, command retirement, component-state migration, the separate scheduler early-stop finding, or the full nine-command integration.

One independent Sol reviewer is sufficient for this correction under `45-REV-002`. The change has many callers, so I expanded the review beyond its five selected files. Its semantic content is still narrow and mechanically checkable: thirteen existing calculation bodies move unchanged into a dependency-light module; every existing workflow method retains its signature; runtime and graph-only callers delegate to the same implementation; dynamic autostart freshness is exercised; and the adjacent, ordinary, cyclic, stress, randomized, and performance checks cover the affected paths. A second reviewer becomes necessary if the accepted scope grows to caching, storage ownership, migration, autostart discovery policy, lifecycle changes, or benchmark repair. Those areas have the delicate interactions described by `45-REV-003`.

## Exact scope reviewed

The isolated tree contains the retirement baseline plus these five selected files:

- `micro_workflow_manager/topology.py`
- `micro_workflow_manager/workflow/component_state.py`
- `micro_workflow_manager/workflow/component_scheduler.py`
- `micro_workflow_manager/cli/engine.py`
- `tests/test_066_shared_topology.py`

The first file differs from the direct MWF working file only by one extra blank line between methods. The other four selected files are byte-identical to their direct MWF counterparts. I also inspected the new pure-topology paragraph in `docs/architecture/graph.md`, the `test_066_shared_topology.py` entry in `tests/README.md`, all callers and overrides of the moved methods, the runtime autostart mutation paths, the graph-only engine loader, and the corrected composition seam in the direct working `cli/preview.py`.

I reread `C:\Business\product\CONTEXT-MAP.md`, `mwf/AGENTS.md`, the complete final Issue #44 resolution in `requirements.md`, `issue-45.json`, `selection-review-preparation.md`, and both preparation transcripts before making the architectural assessment. I also checked the direct MWF architecture, testing, benchmark, and 0.6.2 question documents. AQ1, AQ2, and AQ3 remain recorded, unaccepted questions. This correction does not silently answer them.

## Applicable requirements

The correction supports these settled parts of the final resolution:

| Requirement area | Review result |
| --- | --- |
| `44-SCP-001`, `44-SCP-002`, `44-SCP-009`, `44-SCP-020`, `44-SCP-021`, `44-SCP-023` | MWF retains ownership of selection and quotient-DAG boundaries. Both the runtime and graph-only engine consume the workflow's raw graph through one MWF calculation module. The module does not execute work. |
| `44-CMP-001`, `44-CMP-018` | Hoeflein components remain execution units, and autostart relationships contribute topology only. The extracted class contains no lifecycle or storage operations. |
| `44-CMD-014`, `44-CMD-016` through `44-CMD-022` | Descendant and half-open interval calculations retain their previously reviewed algorithms, endpoint rejection, all-route inclusion, whole-component behavior, and deterministic ordering. |
| `44-CMD-023` | The shared calculation is a sound base for eventual consistency across all nine commands. This release-wide requirement remains pending because this correction does not implement or accept all command surfaces. |
| `44-MEM-001` | `component_key()` still derives identity from exact sorted raw-node membership. Persistence and historical-membership questions remain outside this correction. |
| `44-DOC-019`, `44-DOC-022` | The graph architecture document explains quotient selection and the shared pure module. The test-area README names the focused regression coverage. |
| `44-REC-014`, `44-REC-015`, `44-REC-021` | The selected verification exercises the real scheduler, interval base, runtime topology refresh after relationship changes, and graph-only engine. Wider release reconciliation remains pending. |
| `45-REV-001` through `45-REV-017` as applicable | The reviewer model and reasoning level match the approved procedure. Review covered source, specifications, checks, callers, and effects beyond the selected diff. |

The graph documentation accurately describes the implemented model: synthetic reverse arcs affect strongly connected membership; only raw edges create quotient edges; calculations read no storage and mutate neither supplied input; and lifecycle updates stay separate. I found no wording that overstates acceptance of migration or dynamic discovery.

## Source and behavior analysis

### Pure calculation boundary

`ComponentTopology` depends only on NetworkX and `InvalidGraphError`. Its constructor retains references to the supplied raw graph and autostart relationship set. Query methods build new augmented or quotient graphs and new collections; they do not mutate either supplied object. No storage, runtime workflow, task, router, or project graph module is imported.

The thirteen moved calculation bodies are AST-identical to the immediate base implementations:

- Hoeflein graph and strongly connected component construction
- deterministic component key, component lookup, ID, and raw-node map
- quotient DAG and descendants
- half-open quotient interval
- cyclic-component recognition
- raw and component predecessor queries
- execution-component ordering

An independent signature comparison found all 37 pre-existing methods across `ComponentStateMixin` and `ComponentSchedulerMixin` unchanged. `ComponentStateMixin.topology` is the only added workflow-facing member. Lifecycle, readiness, waiting, job-state, storage, and component-run coordination remain in their existing mixins.

### Runtime freshness

The runtime `topology` member is a property that constructs a lightweight `ComponentTopology(self.graph_obj, self.autostart_edges)` view on every access. This preserves current observations when `register_autostart_edge()` mutates the set in place and when `set_autostart_edges()` replaces the entire set. There is no cached component map that can become stale. The existing workflow method names remain adapters, so downstream callers do not need to depend on the new class directly.

The focused test first observes singleton components, registers `A -> B` as autostart, observes the merged `{'A', 'B'}` component and its selections, then replaces the set with `B -> C` and observes the new shape. The expected values are stated independently, so the test can fail even if two adapters make the same mistake.

### Graph-only engine

`build_engine_snapshot()` still reads synchronized configuration and edges, scans behavior files through the AST-only scanner, and never constructs `MicroWorkflow`, opens storage, or imports user graph or task modules. It now supplies the graph and filtered autostart relationships to `ComponentTopology.component_dag()`.

Component tuples are sorted before component IDs are assigned, and every topological generation is sorted before layout. This preserves deterministic member order, IDs, and positions across reversed edge declaration order. Quotient edges still come only from raw graph edges. The two focused parameter cases reverse the source edge list and assert exact runtime and engine memberships plus exact quotient-edge sets. File bytes are compared before and after the engine call, which detects graph-only loading side effects.

The test would not detect a future engine that duplicated the right algorithm instead of reusing the module. Direct source inspection supplies that check: the engine imports `ComponentTopology` and calls `component_dag()`.

### Corrected preview seam

The direct working `PreviewWorkflow` composes one `ComponentTopology` after reading its fixed synchronized graph and AST-discovered relationships. It exposes explicit `node_complete()` and `node_ready()` persisted observations, and explicit delegation methods for component key, ID, lookup, descendants, predecessors, intervals, and execution ordering. Current planning, destructive-planning, and graph utility callers have the methods they use.

This is a sound ownership correction. The preview no longer inherits the runtime lifecycle mixin or borrows an unbound scheduler method. Holding one topology object is appropriate for a preview instance because that instance does not mutate its graph or autostart set.

This assessment covers only the composition and method surface. The current preview storage path opens SQLite with `mode=ro`; AQ1 and the WAL-sidecar regression show that path does not yet meet the no-visible-mutation requirement. Nothing in this review accepts the preview's SQLite behavior or the plans that depend on it.

## Verification and sensitivity

| Check | Result | What it detects |
| --- | --- | --- |
| Preservation run before the source refactor | 14 passed in 15.01 s | The new focused expectations and existing quotient-selection checks pass against the unchanged `7a2bb8e` behavior. |
| Focused and adjacent selection | 26 passed in 32.46 s | `test_066_shared_topology.py`, `test_063_quotient_selection.py`, `test_036_hoeflein_scheduling.py`, `test_062_engine_and_sampling.py`, and `test_046_module_boundaries.py`. This covers calculation, scheduler use, graph-only loading, and dependency boundaries. |
| Ordinary suite | 390 passed, 1 deselected in 430.98 s | Broad public workflow and CLI behavior after the extraction. |
| Four fresh-process cyclic runs | All passed in 3.50, 9.22, 7.20, and 3.67 s | Repeated cyclic scheduling and dynamic component behavior without process-state reuse. |
| Deterministic Markov-chain stress | 1 passed in 7.18 s | Live cyclic scheduler behavior, where topology and execution-component calculation are reused. |
| Independent randomized comparison | 300 generated directed graphs passed | Compared membership, quotient nodes and edges, descendants, predecessor nodes and components, cyclic recognition, execution order, interval acceptance/rejection/order, and nonmutation of the supplied graph. |
| AST and interface comparison | Passed | Thirteen calculation bodies match the immediate base structurally, and all 37 pre-existing mixin method signatures are preserved. |

The first attempted cyclic run could not create pytest's default directory under the user temporary directory. Repeating it with an isolated `--basetemp` succeeded, and the four recorded fresh-process runs all passed. This was a test-environment permission issue rather than a behavior failure.

## Performance measurement

The predeclared default workload was three alternating baseline/candidate runs of:

```text
benchmark_hoeflein_wait.py --seeds 200 --rounds 3 --threads 100 --delay 0.001
```

The unchanged baseline failed the correctness precondition before a candidate comparison: it reported no error and exited zero with only A=306 and B=200 done, leaving A=94 and B=106 queued. A candidate diagnostic likewise left queued work. The benchmark currently treats `error is None` as success and does not enforce the README requirement that all expected rounds finish with no queued, running, or failed residue. Timing from that workload is invalid and must not be used as evidence for or against this correction.

The minimized baseline diagnostic also exposes a separate scheduler concern. With two seeds and one thread it ended with A=4 done, B=2 done, and B=2 queued, while A had no queued or running jobs. That terminal state is not explained by both members having pending work in an ordinary mutual wait. In the unchanged scheduler, queued nodes, running-job nodes, and wait blockers are read in three separate calls before the deadlock branch. A transition between those observations is one credible explanation for a false deadlock decision, but it has not been established. Deterministic instrumentation or a focused reproducer is required before choosing a fix. This unresolved implementation finding exists on the immediate base and is not caused by the topology extraction. It does not reject the selected calculation refactor, but it must stay visible and must not be described as resolved by the replacement timing run.

A bounded replacement workload, `--seeds 1 --rounds 200 --threads 1 --delay 0.001`, completed the exact expected A=200 and B=199 jobs with zero queued, running, or failed residue and no reported error in all six runs. The three alternating results were:

| Repetition | Baseline seconds | Candidate seconds |
| --- | ---: | ---: |
| 1 | 90.8388028 | 91.5898530 |
| 2 | 89.3441165 | 87.9124478 |
| 3 | 87.2029430 | 88.5576565 |

The baseline median is 89.3441165 s. The candidate median is 88.5576565 s. The candidate/baseline median ratio is 0.9911974, which passes the predeclared `<= 1.20` gate. Raw output, durable database counts, commands, and environment manifests are recorded in `topology-benchmark-single-*.json`, `.jsonl`, and `.log` under `testing_ground/issue-45`.

The benchmark's false-success behavior is an existing verification-harness defect, demonstrated on the immediate base. It is not caused by the topology extraction. The scheduler observation, raw cases, ranked explanations, and required controlled diagnostic are preserved separately in `testing_ground/issue-45/scheduler-early-stop.md`. Changing either the scheduler or benchmark in this correction would expand the selected scope and require renewed review.

## Remaining boundaries

AQ3 remains material. The graph-only scanner recognizes literal `.add(..., autostart=True)`, while the runtime supports `add_many`, `add_job`, `add_jobs`, and computed routing that may register relationships later. Sharing the calculation after relationships are supplied does not make those two sources universally equivalent, reconstruct historical component membership, or decide persisted ownership. The current architecture document states this boundary, and the open question correctly reserves the policy decision.

AQ1 and AQ2 also remain outside this acceptance. Command retirement and all working preview, planning, destructive, and main-module edits remain outside the selected tree. `44-CMD-023` and full release acceptance therefore remain pending even if this shared calculation correction is accepted.
