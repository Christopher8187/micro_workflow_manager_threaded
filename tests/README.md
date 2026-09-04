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
| `test_039_sqlite_contention_recovery.py` | Checkpoint writes, execution fences, asynchronous runtime observations, connection cleanup, and repeated merge runs. |
| `test_040_high_fanout_batching.py` | Prepared payload batches, idempotent fan-out, job-ID reservation, schema upgrade, grouped publication, monitoring, and bulk fresh cleanup. |
| `test_041_live_component_pumping.py` | Live member refills, component status, lazy-source failure, late job IDs, Windows descendant paths, and monitoring during routing. |
| `test_042_cooperative_api_scaling.py` | Cooperative API jobs, high logical concurrency, and pending aggregate API budgets. |
| `test_043_waiting_nodes.py` | Intra-component waiting gates, monitor display, valid and invalid wait targets, and singleton behavior. |
| `test_043_watchdog_networking.py` | Checkpoint and total deadlines during framework network waits, physical dispatch timing, replay leases, and cancellation isolation. |
| `test_044_queue_transport_scaling.py` | Batch queue loading, event-driven fiber completion, queue wakeups, HTTP sharding, admission fairness, commit priority, and terminal flushing. |
| `test_045_terminal_recovery.py` | Idempotent output-backed terminal reconciliation and joining started work before component failure. |
| `test_046_module_boundaries.py` | The repository's source-module size boundary and approved cohesive exceptions. |
| `test_046_resume_restart_wait.py` | Resume reconciliation, component restart selections, and the queued/running/failed conditions for waiting gates. |
| `test_047_event_state_top.py` | Event cursors, local and cross-process wakeups, `mwf top`, writer lifecycle, network-state coalescing, and ordered asynchronous journal appends. |
| `test_048_ghost_free_admission.py` | Monitor visibility under balanced high-concurrency admission and small-tail draining. |
| `test_049_job_trace.py` | Chronological job trace rendering and trace command parsing. |
| `test_050_windows_process_signal_safety.py` | Platform-safe process liveness checks, subscriber and `top` behavior, and recycled Windows PID rejection. |
| `test_051_refuseafter_trace_retention.py` | Inclusive admission boundaries, trace clearing and retention, orphan journals, copy/paste, and changed job origin. |
| `test_052_sqlite_finalizer_reentrancy.py` | Same-thread connection-registry behavior during storage finalization. |
| `test_053_windows_extended_paths.py` | Windows extended-length aliases, sibling-path rejection, and context output recording. |
| `test_054_destructive_preparation_commands.py` | Confirmation and scope for reset, resetfrom, cleanfrom, and wipefrom without execution. |
| `test_055_threaded_prefetch_and_nofile.py` | Threaded payload prefetch, bounded source reservations, file-descriptor limit handling, and CLI setup order. |
| `test_056_resumefrom_refuseafter_052.py` | Inclusive `resumefrom` boundaries, component expansion, planning, and invalid-boundary behavior. |
| `test_057_hoeflein_live_sync_053.py` | Resident component members, late feedback, source loading, startup subscription, failure joining, cleanup, wakeup scope, and mutation-lane use. |
| `test_058_http_fanout_scaling_054.py` | Batched task-start events, API pump allocation, sparse refill, wide fan-out, HTTP/1.1 sharding, router identity, and execution priority. |
| `test_060_network_manager_056.py` | Shared network-manager dispatch, HTTP/2 terminal recovery, shard retirement and reuse, quiet-tail evidence, diagnostics, cancellation, persistence, and local pacing. |
| `test_061_refuse_before_0510.py` | Exclusive refusal boundaries for run and resume, global admission stops, component naming, planning, and invalid selections. |
| `test_062_engine_and_sampling.py` | Graph-only engine boundaries and deterministic sampled runs. It does not establish human layout readability or descendant and component-circulation isolation. |
| `test_063_quotient_selection.py` | Half-open quotient intervals, endpoint rejection, whole-component expansion, deterministic ordering, unchanged stored state, and overlapping directed routes. |
| `test_active_job_restart.py` | Generation-fenced restart during active threaded, direct, and process runs, refusal cases, fast-path imports, and checkpoint replacement. |
| `test_autostart_cycles.py` | Self, mutual, diamond, ring, and stochastic cyclic scheduling. Run each test in a fresh process. |
| `test_benchmark_exit_codes.py` | Nonzero benchmark status when Hoeflein synchronization raises or an explode-pump sample reports failed jobs. |
| `test_checkpoint_keyword_api.py` | Keyword checkpoint fields, persistence, validation, dynamic deadlines, fallbacks, and router schema. |
| `test_cli_help_and_clean_wipe.py` | Help and describe output, reset/clean/wipe semantics, selected-job runs, initialization sidecars, and component-expanded cleanup. |
| `test_cli_monitor.py` | Bulk summaries, inline and standalone monitoring, timing metadata, reuse, combined statistics, and diagnostic monitor failure. |
| `test_deploy.py` | Legacy state consolidation, deployment setup, ignore rules, local archives, remote extraction, PuTTY use, and clean SQLite reinitialization. |
| `test_file_entry_node_input_import.py` | Binding a node-input filesystem entry during directory creation. |
| `test_filesystem_objects.py` | Node input and output filesystem declarations, safe reads and writes, templates, generation fencing, bulk JSON reads, encoding, atomic copy, and absence of per-job file APIs or automatic returned-file copying. |
| `test_framework_improvements.py` | Graph paths, doctor and inspect, recovery, timeout fencing, failure-history exposure, current fallback terminal error, removed staging-helper behavior, explicit idempotency keys, resume, plans, dry runs, help, and checkpoint supervision. |
| `test_graph_sync_and_fans.py` | Explicit graph updates, edge-only changes, compact directed fan forms, and the `fan` helper. |
| `test_init_clipboard_debug_028.py` | Deployment archive initialization, node clipboard copy/paste, and debug output. |
| `test_markov_chain_stress.py` | Marked long-running deterministic cyclic filesystem stress. |
| `test_output_and_runner_edges.py` | Mixed direct and threaded node execution under global runner choices. |
| `test_reliability.py` | Router validation, job parent identity, component readiness, cancellation, runner concurrency, process execution, dynamic spawn, and high fan-in. |
| `test_release_packaging.py` | Builds a source archive and verifies inclusion of project guidance, all five skills, documentation, examples, benchmarks, and tests while excluding injected HTML and cache sentinels. |
| `test_runtime_thread_overrides.py` | Runtime thread changes, adaptive worker scaling, aggregate API allocation and redistribution, invalid nodes, high limits, warnings, and concurrent run claiming. |

## Shared helper

`state_helpers.py` constructs `FileStorage` instances and seeds jobs and status
for tests that need SQLite-backed state without repeating setup code.

`tests/__init__.py` marks the directory as a Python package and contains no test
behavior.
