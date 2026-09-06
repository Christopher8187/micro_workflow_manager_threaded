# Test modules

This file explains the test modules and the shared helper. Update it whenever a test module is added,
renamed, removed, or changes purpose. Read [the testing model](../docs/testing.md)
and use the `mwf-test` skill for execution order and isolation.

| Module | Scope |
| --- | --- |
| `test_030_runtime_updates.py` | Run-scoped thread overrides, restart refusal outside an active run, and deployment-port prompting. |
| `test_031_inspect_failed_and_examples.py` | Failed-job inspection, empty failure output, help text, and neutral names in command descriptions. |
| `test_033_filter_icons_design.py` | Filter-funnel rendering, generated ignore and icon settings, documentation routing, and example output provenance. |
| `test_034_sqlite_api_runner.py` | SQLite initialization and migration, file payload boundaries, API concurrency, schema refresh, paste recovery, and orphan diagnostics. |
| `test_036_hoeflein_scheduling.py` | External predecessor readiness, producer-aware merge preservation, and component-wide failure state. |
| `test_037_advisory_lock_recovery.py` | Advisory-lock ownership, dead-process reclamation, and run-state cleanup when override binding or cleanup fails. |
| `test_038_fresh_resume_restart_semantics.py` | Fresh run preparation, merge-branch preservation, descendant resume, active restart, and inline monitoring. |
| `test_039_sqlite_contention_recovery.py` | Checkpoint writes, execution fences, asynchronous runtime observations, unassisted worker-connection cleanup, exact retained outputs after repeated API rounds, and repeated CLI merge runs. |
| `test_040_high_fanout_batching.py` | Prepared payload batches, idempotent fan-out, job-ID reservation, schema upgrade, grouped publication, monitoring, and bulk fresh cleanup. |
| `test_041_live_component_pumping.py` | Live member refills, component status, lazy-source failure, late job IDs, Windows descendant paths, and monitoring during routing. |
| `test_042_cooperative_api_scaling.py` | Cooperative API jobs, high logical concurrency, and pending aggregate API budgets. |
| `test_043_waiting_nodes.py` | Intra-component waiting gates, monitor display, valid and invalid wait targets, and singleton behavior. |
| `test_043_watchdog_networking.py` | Checkpoint and total deadlines; initial persistence and heap-maintenance delays; checkpoint/network rearming; physical retry entry and heartbeat progress; caller time, exact expiry and renewed leases; 100-request isolation; immutable timeout observations, event ownership across terminalization/restart, and runtime ordering across retries, fallbacks, queued writes, and write failures. |
| `test_044_queue_transport_scaling.py` | Batch queue loading, event-driven fiber completion, queue wakeups, HTTP sharding, admission fairness, commit priority, and terminal flushing. |
| `test_045_terminal_recovery.py` | Native output recovery, idempotent late finalization, duplicate or conflicting grouped submissions, unchanged output and events, session release, and joining started work before component failure. |
| `test_046_module_boundaries.py` | The repository's source-module size boundary and approved cohesive exceptions. |
| `test_046_resume_restart_wait.py` | Resume reconciliation, component restart selections, and the queued/running/failed conditions for waiting gates. |
| `test_047_event_state_top.py` | Event cursors, local and cross-process wakeups, `mwf top`, writer lifecycle, network-state coalescing, ordered asynchronous journal appends, and timeout-only terminal ownership with stale-execution and damaged-owner rejection. |
| `test_048_ghost_free_admission.py` | Monitor visibility under balanced high-concurrency admission and small-tail draining. |
| `test_049_job_trace.py` | Chronological job trace rendering and trace command parsing. |
| `test_050_windows_process_signal_safety.py` | Platform-safe process liveness checks, subscriber and `top` behavior, and recycled Windows PID rejection. |
| `test_051_refuseafter_trace_retention.py` | Inclusive admission boundaries, trace clearing and retention, orphan journals, copy/paste, and changed job origin. |
| `test_052_sqlite_finalizer_reentrancy.py` | Same-thread connection-registry behavior during storage finalization. |
| `test_053_windows_extended_paths.py` | Windows extended-length aliases, sibling-path rejection, and context output recording. |
| `test_054_destructive_preparation_commands.py` | Confirmation and scope for reset and resetfrom without execution. |
| `test_055_threaded_prefetch_and_nofile.py` | Threaded payload prefetch, bounded source reservations, file-descriptor limit handling, and CLI setup order. |
| `test_056_resumefrom_refuseafter_052.py` | Inclusive `resumefrom` boundaries, component expansion, planning, and invalid-boundary behavior. |
| `test_057_hoeflein_live_sync_053.py` | Resident component members, late feedback, source loading, startup subscription, failure joining, native claim cleanup after output failure, wakeup scope, and mutation-lane use. |
| `test_058_http_fanout_scaling_054.py` | Batched task-start events, API pump allocation across overlapping workflow waves, sparse refill, wide fan-out, HTTP/1.1 sharding, router identity, and execution priority. |
| `test_060_network_manager_056.py` | Shared network-manager dispatch, HTTP/2 terminal recovery, shard retirement and reuse, quiet-tail evidence, diagnostics, cancellation, persistence, and local pacing. |
| `test_061_refuse_before_0510.py` | Exclusive refusal boundaries for run and resume, global admission stops, component naming, planning, and invalid selections. |
| `test_062_engine_and_sampling.py` | Graph-only engine boundaries and deterministic sampled runs. It does not establish human layout readability or descendant and component-circulation isolation. |
| `test_063_quotient_selection.py` | Half-open quotient intervals, endpoint rejection, whole-component expansion, deterministic ordering, unchanged stored state, and overlapping directed routes. |
| `test_065_removed_commands.py` | Rejection of retired commands before project bootstrap, with no description, help example, or filesystem mutation. |
| `test_066_shared_topology.py` | Runtime and engine agreement on autostart components and quotient edges, plus updated selections after autostart registration or replacement. |
| `test_067_live_legacy_migration.py` | Applied migration, automatic layout conversion, and initialization before archive extraction refuse observed live legacy owners or non-object run state before filesystem changes; finished or recycled process owners permit migration. |
| `test_068_component_snapshot.py` | Coherent completion observations, waiting-pump progress between claims, and a real waiting deadlock with an idle resident member. |
| `test_069_execution_sessions.py` | Internal fresh SQLite session storage, exact session history and live readers, process-safe main cardinality, conditional updates, validation, rollback, and preservation of existing project initialization. |
| `test_070_legacy_run_preflight.py` | Both legacy run-file locations, structural diagnostics and link refusal, preservation during layout conversion, direct creation, graph and process-worker loading, archive initialization, clipboard ordering, and retained established live-project opens. |
| `test_071_component_session_ownership.py` | Exact component identity and producing shape, canonical persistence, registration races, full-scope reservations including historical membership overlap, counted holds, rollback, validation, and reopening. |
| `test_072_job_claim_ownership.py` | Retained version-4 claim bytes, exact session/component ownership in single and batch claims, refusal before mutation, lease/owner/event rollback, historical ownership, bounded owner lookups, concurrent caller separation, and transaction-time validation. |
| `test_073_sample_calculations.py` | Private sampling selectors, exact component membership, explicit zero, status filtering, exact percentage/count calculations, seeded selection from caller identity bytes, and starting-population coverage. Public command integration remains pending. |
| `test_074_component_readiness.py` | Private readiness and compatible lineage from supplied direct-parent results, incomplete and conflicting parents, authorized starting overrides, exact origins, and invalid observations. Runtime integration remains pending. |
| `test_075_component_state_storage.py` | Private fresh queued state, atomic registration, preserved results and producing shape, approved lineage, damaged-row refusal, SQLite constraints, fresh-process schema refusal, and version-4 preservation. Lifecycle transitions have separate coverage; runtime integration remains pending. |
| `test_076_component_state_transitions.py` | Private sampled resume, retained exact lineage and producing shape, transaction-time ownership and generation checks, damaged-state refusal, rollback and retry, concurrent repeats, and version-4 preservation. Public execution integration remains pending. |
| `test_077_component_resume_completion.py` | Private resumed-sample completion, retained lineage and ownership, producing-shape and generation checks in the writer transaction, damaged-state refusal, rollback and retry, concurrent attempts, and version-4 preservation. Coverage calculation and public execution remain pending. |
| `test_078_component_running_failure.py` | Private running-component failure, cleared lineage with preserved ownership and other stored data, transaction-time guards, damaged-state refusal, rollback and retry, competing failure and completion attempts, and version-4 preservation. Public execution remains pending. |
| `test_079_job_instance_identity.py` | Native job identity across creation paths, input refresh, deletion and recreation, reopening, damaged-state refusal, rollback, native clipboard transfer, and execution-state preservation. Runtime ownership integration remains separate. |
| `test_080_native_project_bootstrap.py` | Ordinary native creation/reopening, unchanged old-format refusal, schema validation, failure cleanup, configuration publication, clipboard admission and state rollback, and refusal before graph or process-worker imports. Full clipboard and read-only preview verification remain pending. |
| `test_081_native_main_execution.py` | Ordinary native main-session ownership through real execution, reservation lifetime, and retained claim events. This group is under implementation. |
| `test_082_native_programmatic_execution.py` | Shared native programmatic ownership, scheduler and process dispatch, known-component child creation, guarded task reuse, selected jobs, batch autostart, replacement execution, competing-call refusal, and cleanup after admission failure. Wider lifecycle integration remains pending. |
| `test_083_native_session_monitoring.py` | Exact native session lists and text, local and foreign process attribution, timestamped writer diagnostics, timezone-aware times, and recorded API settings. Wider component and control integration remains pending. |
| `test_084_native_current_job_owner.py` | Current and last committed job ownership, claim-time incarnation, exclusive claims, trace clearing, reopen, released claims, fresh reset, numeric-ID recreation, generation restart, transactional rollback, and damaged-reference refusal. Command integration remains pending. |
| `test_085_native_job_inspection.py` | Exact native owner display, independent liveness observations, damaged ownership, replacement during inspection, real CLI preservation, and nullable process metadata. Wider inspection and restart integration remain pending. |
| `test_086_native_owned_restart.py` | Native owner and component restart, atomic refusal and output compensation, running and finite failed-job replacement, retained results and selection, session-exit ordering, accepted restarts after finite output failure, cleanup error precedence, competing node-state publication, and process submission/result failures. |
| `test_087_native_component_continuation.py` | Native component and DAG continuation, retained results, bounded setup retries, mixed runners, two API lanes, abandoned claims, and CLI restart ordering. Output failure cases cover original-error precedence, saved-output recovery, and owner or reservation damage during cleanup. Wider integration remains under verification. |
| `test_active_job_restart.py` | Generation-fenced restart during active threaded, direct, and process runs, refusal cases, fast-path imports, and replacement-runtime isolation after the old checkpoint deadline and stale-write attempt. |
| `test_autostart_cycles.py` | Self, mutual, diamond, ring, and stochastic cyclic scheduling. Run each test in a fresh process. |
| `test_benchmark_exit_codes.py` | Nonzero benchmark status for workflow exceptions, failed jobs, unfinished work, missing expected rounds, and repeated-API slowdown beyond the fixed allowance; successful completion and exact allowance equality. |
| `test_checkpoint_keyword_api.py` | Keyword checkpoint fields, persistence, validation, dynamic deadlines, fallbacks, and router schema. |
| `test_cli_help_and_reset.py` | Help and describe output, reset semantics, selected-job runs, initialization sidecars, and component-expanded preparation. |
| `test_cli_monitor.py` | Bulk summaries, inline and standalone monitoring, timing metadata, reuse, combined statistics, and diagnostic monitor failure. |
| `test_deploy.py` | Legacy state consolidation, deployment setup, ignore rules, local archives, remote extraction, PuTTY use, and clean SQLite reinitialization. |
| `test_file_entry_node_input_import.py` | Binding a node-input filesystem entry during directory creation. |
| `test_filesystem_objects.py` | Node input and output filesystem declarations, safe reads and writes, templates, generation fencing, bulk JSON reads, encoding, atomic copy, and absence of per-job file APIs or automatic returned-file copying. |
| `test_framework_improvements.py` | Graph paths, doctor and inspect, recovery, timeout fencing, failure-history exposure, current fallback terminal error, removed staging-helper behavior, explicit idempotency keys, resume, plans, dry runs, help, and checkpoint supervision. |
| `test_graph_sync_and_fans.py` | Explicit graph updates, edge-only changes, compact directed fan forms, and the `fan` helper. |
| `test_init_clipboard_debug_028.py` | Deployment archive initialization, node clipboard copy/paste, and debug output. |
| `test_markov_chain_stress.py` | Marked long-running deterministic cyclic filesystem stress. |
| `test_output_and_runner_edges.py` | Mixed direct and threaded node execution under global runner choices. |
| `test_reliability.py` | Router validation, job parent identity, component readiness, cancellation, observed overlap of independent ready-node handlers with a direct-runner serial control, process execution, dynamic spawn, and high fan-in. |
| `test_release_packaging.py` | Builds a source archive and verifies inclusion of project guidance, all five skills, documentation, examples, benchmarks, and tests while excluding injected HTML and cache sentinels. |
| `test_runtime_thread_overrides.py` | Runtime thread changes, adaptive worker scaling, aggregate API allocation and redistribution, set/reset deprecation warnings, rejected API-budget values without stored changes, invalid nodes, high limits, warnings, and concurrent run claiming. |

## Shared helper

`state_helpers.py` constructs `FileStorage` instances and seeds jobs and status
for tests that need SQLite-backed state without repeating setup code.

`tests/__init__.py` marks the directory as a Python package and contains no test
behavior.
