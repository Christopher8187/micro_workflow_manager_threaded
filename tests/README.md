# Test modules

This file explains the test modules and the shared helper. Update it whenever a test module is added,
renamed, removed, or changes purpose. Read [the testing model](../docs/testing.md)
and use the `mwf-test` skill for execution order and isolation.

| Module | Scope |
| --- | --- |
| `test_030_runtime_updates.py` | Native session thread overrides, restart refusal without active work, and deployment-port prompting. |
| `test_031_inspect_failed_and_examples.py` | Failed-job inspection, empty failure output, help text, and neutral names in command descriptions. |
| `test_033_filter_icons_design.py` | Filter-funnel rendering, generated ignore and icon settings, documentation routing, and example output provenance. |
| `test_034_sqlite_api_runner.py` | Native SQLite initialization, file payload boundaries, API concurrency, schema validation, clipboard recovery, and orphan diagnostics. |
| `test_036_hoeflein_scheduling.py` | External predecessor readiness, producer-aware merge preservation, and component-wide failure state. |
| `test_037_advisory_lock_recovery.py` | Native advisory-lock ownership, unknown-owner lease preservation, dead-process reclamation, and run-state cleanup when override binding or cleanup fails. |
| `test_038_fresh_resume_restart_semantics.py` | Fresh run preparation, merge-branch preservation, descendant resume, active restart, and inline monitoring. |
| `test_039_sqlite_contention_recovery.py` | Checkpoint writes, execution fences, asynchronous runtime observations, unassisted worker-connection cleanup, exact retained outputs after repeated API rounds, and repeated CLI merge runs. |
| `test_040_high_fanout_batching.py` | Prepared payload batches, idempotent fan-out, job-ID reservation, native reopen and damaged-schema refusal, grouped publication, monitoring, and bulk fresh cleanup. |
| `test_041_live_component_pumping.py` | Live member refills, component status, shutdown after a lazy-source failure, late job IDs, Windows descendant paths, and monitoring during live handler execution. |
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
| `test_051_refuseafter_trace_retention.py` | Inclusive admission boundaries, trace clearing and retention, orphan journals, copy/paste, complete native origin changes, and malformed-origin rendering. |
| `test_052_sqlite_finalizer_reentrancy.py` | Same-thread connection-registry behavior during storage finalization. |
| `test_053_windows_extended_paths.py` | Windows extended-length aliases, sibling-path rejection, and context output recording. |
| `test_054_destructive_preparation_commands.py` | Confirmation and scope for reset and resetfrom without execution. |
| `test_055_threaded_prefetch_and_nofile.py` | Threaded payload prefetch, bounded source reservations, file-descriptor limit handling, and CLI setup order. |
| `test_056_resumefrom_refuseafter_052.py` | Inclusive `resumefrom` boundaries, component expansion, planning, and invalid-boundary behavior. |
| `test_057_hoeflein_live_sync_053.py` | Resident component members, late feedback, source loading, startup subscription, failure joining, native claim cleanup after output failure, wakeup scope, and mutation-lane use. |
| `test_058_http_fanout_scaling_054.py` | Batched task-start events, API pump allocation across overlapping workflow waves, sparse refill, wide fan-out, HTTP/1.1 sharding, router identity, and execution priority. |
| `test_060_network_manager_056.py` | Shared network-manager dispatch, HTTP/2 terminal recovery, shard retirement and reuse, quiet-tail evidence, diagnostics, cancellation, persistence, and local pacing. |
| `test_061_refuse_before_0510.py` | Exclusive refusal boundaries for run and resume, global admission stops, component naming, planning, and invalid selections. |
| `test_062_engine_and_sampling.py` | Graph-only engine boundaries, native sample plans, parent readiness, and preservation of unselected work. |
| `test_063_quotient_selection.py` | Half-open quotient intervals, endpoint rejection, whole-component expansion, deterministic ordering, unchanged stored state, and overlapping directed routes. |
| `test_064_read_only_previews.py` | Native plan and dry-run snapshots, source-import refusal, closed-database sidecar preservation, and committed WAL reads. |
| `test_065_removed_commands.py` | Rejection of retired commands before project bootstrap, with no description, help example, or filesystem mutation. |
| `test_066_shared_topology.py` | Runtime and engine agreement on autostart components and quotient edges, exact-import unshadowed `NodeInputFileSystem` single/batch discovery without imports, false or rebound binding preservation, and updated selections after autostart registration or replacement. |
| `test_068_component_snapshot.py` | Coherent completion observations, waiting-pump progress between claims, and a real waiting deadlock with an idle resident member. |
| `test_069_execution_sessions.py` | Native SQLite session storage, exact instance selection, session history and live readers, process-safe main cardinality, conditional updates, validation, rollback, native initialization races, and unchanged old-format refusal. |
| `test_071_component_session_ownership.py` | Exact component identity and producing shape, canonical persistence, registration races, full-scope reservations including historical membership overlap, counted holds, rollback, validation, and reopening. |
| `test_072_job_claim_ownership.py` | Native owned lease and event behavior, unchanged old-format refusal, exact session/component ownership in single and batch claims, refusal before mutation, lease/owner/event rollback, historical ownership, bounded owner lookups, concurrent caller separation, and transaction-time validation. |
| `test_073_sample_calculations.py` | Private sampling selectors, exact component membership, explicit zero, status filtering, exact percentage/count calculations, seeded selection from caller identity bytes, and starting-population coverage. Public commands are covered by test117. |
| `test_074_component_readiness.py` | Readiness and compatible lineage from supplied direct-parent results, incomplete and conflicting parents, authorized starting overrides, exact origins, and invalid observations. |
| `test_075_component_state_storage.py` | Fresh queued state, atomic registration, preserved results and producing shape, approved lineage, damaged-row refusal, SQLite constraints, fresh-process schema refusal, and missing-session refusal with preserved data. |
| `test_076_component_state_transitions.py` | Private sampled resume, retained exact lineage and producing shape, transaction-time ownership and generation checks, damaged-state refusal, rollback and retry, concurrent repeats, and native missing-session refusal with preserved data. Public sampled resume is covered by tests125/129/130. |
| `test_077_component_resume_completion.py` | Private resumed-sample completion, retained lineage and ownership, producing-shape and generation checks in the writer transaction, damaged-state refusal, rollback and retry, concurrent attempts, and native missing-session refusal with preserved data. Coverage and public execution are covered by tests123/125/126/129/130. |
| `test_078_component_running_failure.py` | Private running-component failure, cleared lineage with preserved ownership and other stored data, transaction-time guards, damaged-state refusal, rollback and retry, competing failure and completion attempts, and native missing-session refusal with preserved data. Public sampled failure and repair are covered by tests125/129/130. |
| `test_079_job_instance_identity.py` | Native job identity across creation paths, input refresh, deletion and recreation, reopening, damaged-state refusal, rollback, native clipboard transfer, and execution-state preservation. Runtime ownership integration remains separate. |
| `test_080_native_project_bootstrap.py` | Native creation and reopening, unchanged old-format refusal, schema validation, failure cleanup, configuration publication, clipboard admission and rollback, and refusal before graph or process-worker imports. |
| `test_081_native_main_execution.py` | Native main-session ownership through execution, reservation lifetime, retained claim events, and refusal when admitted reservations disappear before cleanup. |
| `test_082_native_programmatic_execution.py` | Native programmatic ownership, scheduler and process dispatch, known-component child creation, guarded task reuse, selected jobs, batch autostart, replacement execution, competing-call refusal, and cleanup after admission failure. |
| `test_083_native_session_monitoring.py` | Exact native session lists and text, local and foreign process attribution, timestamped writer diagnostics, timezone-aware times, and recorded API settings. |
| `test_084_native_current_job_owner.py` | Current and last committed job ownership, claim-time incarnation, exclusive claims, trace clearing, reopen, released claims, fresh reset, numeric-ID recreation, generation restart, rollback, and damaged-reference refusal. |
| `test_085_native_job_inspection.py` | Exact native owner display, independent liveness observations, damaged ownership, replacement during inspection, real CLI preservation, and nullable process metadata. |
| `test_086_native_owned_restart.py` | Native owner and component restart, atomic refusal and output compensation, running and finite failed-job replacement, retained results and selection, session-exit ordering, accepted restarts after finite output failure, cleanup error precedence, competing node-state publication, and process submission/result failures. |
| `test_087_native_component_continuation.py` | Native component and DAG continuation, retained results, bounded setup retries, mixed runners, two API lanes, abandoned claims, and CLI restart ordering. Output failure cases cover original-error precedence, saved-output recovery, and owner or reservation damage during cleanup. Wider integration remains under verification. |
| `test_088_native_component_controls.py` | Removed component skip operation and preservation of successful skipped jobs during ordinary execution. |
| `test_089_ordinary_component_start.py` | Guarded queued component start, exact ownership, transactional refusal and rollback, concurrent starts, and native reopen. |
| `test_090_component_session_settlement.py` | Atomic component results and session exit, success within a live session, accepted restart ordering, lineage, stale ownership, unfinished-job refusal, and batch rollback. |
| `test_091_component_state_observations.py` | Consistent component snapshots during concurrent writes, exact ordering, optional absent-definition observations, damaged-state refusal, and caller transaction preservation. |
| `test_092_native_component_lifecycle.py` | Public native component start and completion, nested calls, retained task results, exact accepted restarts before ordinary queued work, readiness, and terminal session cleanup. |
| `test_093_native_cli_readiness.py` | Native parent readiness with raw-status disagreement, mutation-free refusal for absent parents, empty selected components, and native CLI stop boundaries. |
| `test_094_native_fresh_preparation.py` | Full component preparation epochs, exact run and reset authority, preserved unrelated state, active-work refusal, and failed preparation recovery. |
| `test_095_native_preparation_files.py` | Exact job-file removal with unattributed files preserved, staging and restoration, collision preservation, original-error retention, committed preparation with retained temporary files, refusal of links into unselected nodes, and changed job or orphan-journal refusal. |
| `test_096_managed_input_publications.py` | Producer-qualified forwarded paths, cyclic raw producers and plural copies, other-owner preservation during deletion, static link refusal, fixed-depth receiving reads, recursive output reads, bound JSON batch bases, invalid-name refusal before batch writes, stale handles, and real sender/receiver runs on every runner. |
| `test_097_execution_producing_identity.py` | Captured producing graph and alignment, actual recursive job creator history, exact selected roots, idempotent reuse, historical reads after trace removal and deletion, and damaged ownership or selection refusal. |
| `test_098_causal_execution_boundaries.py` | Child restart continuation, process creator transport, retained-handle refusal, actual task attribution, grouped claims, preparation creator checks, immutable shapes, damaged scope and ancestry, and nested pending-result settlement. |
| `test_099_managed_input_ownership.py` | Historical producing execution ownership, ambiguity, batch restoration, atomic forwarding events, unfinished receipt refusal, exact producer checks, read-only copies, repeated paths, case aliases, and raw-node storage collisions. |
| `test_100_native_advisory_ownership.py` | Exact greenlet lock ownership, nesting across storage instances, independent API locks, copied contexts, helper threads, and protection during commit notification. |
| `test_101_programmatic_fresh_preparation.py` | Repeated independent full preparation, retained root inputs, queue-only and selected-job preservation, preparation failure restoration, parent preflight, and admission selection checks. |
| `test_102_component_input_arrivals.py` | Managed-input receiver misalignment, first causes, generation repair, rollback, process contention, local result caching, and damaged-history refusal. |
| `test_103_component_job_arrivals.py` | Managed-job receiver causes, creator identity, shape refusal, publication rollback, directory ownership, process and grouped races, damaged history, and interrupted waits. |
| `test_104_producer_footprint_preparation.py` | Producer-owned cleanup across excluded receivers, durable ancestry, retained results, preservation of other producers and their orphan traces, and notification failure after commit. |
| `test_105_preparation_receiver_guards.py` | Complete preparation preflight, excluded receiver ownership and guards, publication refusal, and preservation after a later component fails. |
| `test_106_preparation_receipt_failures.py` | Ambiguous input refusal, SQL rollback, repeated interrupted waits, retained recovery material, and original errors after committed preparation. |
| `test_107_selected_producer_preparation.py` | Exact selected-root ancestry, historical generations, prior descendant cleanup, excluded receiver causes, and preservation of unrelated work and user files. |
| `test_108_native_resume_preparation.py` | Whole-selection misalignment and parent refusal before mutation; atomic failed-job requeue and component transition with file restoration. |
| `test_109_native_resume_recovery.py` | Exact output recovery, execution lock order, timing and restart history, real terminal output identity, complete active metadata, incompatible prior owners, and retained failures from older alignments. |
| `test_110_native_resume_guidance.py` | Command-specific repair guidance, separate misaligned branches, and refusal of missing failed-job ownership. |
| `test_111_selected_preparation_readiness.py` | Mandatory parent readiness for independent selected calls, exact admitted roots, and refusal before fresh preparation. |
| `test_112_selected_causal_execution.py` | Recursive same-component children, causal failures, retained root values, and unrelated-work preservation. |
| `test_113_selected_causal_runners.py` | CLI, direct, threaded, API, and process execution; waiting, supplied payloads, child restarts, and alignment drift. |
| `test_114_selected_component_lifecycle.py` | Running, sampled, done, and failed transitions; cumulative coverage, retained lineage, misalignment, repair, and zero-work behavior. |
| `test_115_selected_adopted_restart_continuation.py` | Repeated child repairs and new causal work after a retained nested component repair, preserving the original root result. |
| `test_116_selected_ready_session_failure.py` | Ready results after caller failure, transactional rollback, damaged pending history, and producing-identity drift before the first claim. |
| `test_117_execution_sampling.py` | Public count and percentage sampling, named members, causal circulation, zero work, guarded replay, and partial or full component results. |
| `test_119_native_preview_storage.py` | Read-only SQLite snapshots with active writers, file-identity checks, Windows handle pinning, bounded capture, and original-file preservation. |
| `test_120_native_sample_admission.py` | Atomic session, reservation, sampled population, ordered roots, and manifest retention; rollback after refused or suppressed writes. |
| `test_121_native_preview_recovery.py` | Read-only abandoned-session observations, exact output recovery candidates, ownership validation, and mutation-free diagnostics. |
| `test_122_native_preview_session_identity.py` | Damaged session identity, retained component scope and lineage, active metadata, and recovery-action refusal. |
| `test_123_sample_history_and_filtered_coverage.py` | Strict persisted sampling history, filtered coverage, newer work, Boolean field refusal, and replay guards after excluded-status changes. |
| `test_124_admission_interruption.py` | Interrupted admission delivery and in-flight writes, session or reservation rollback, exact retained roots, and cleanup after notification failure following COMMIT. |
| `test_125_sampled_resume.py` | Resume remaining sampled work, promote completed samples, retain running lineage, refuse incompatible inputs, and settle failures through native sessions. |
| `test_126_sampling_lifecycle_writer_paths.py` | Filtered completion after excluded work changes, repair of failed or cancelled sampled jobs, and retained unstable origins. |
| `test_127_between_command_previews.py` | Half-open interval previews, component expansion, excluded endpoints, crossing edges, invalid directed endpoints, and unchanged project files without user imports. |
| `test_128_sample_plan_filtered_acquisition.py` | Fresh read-only sampling observations accept excluded-status changes while retaining the later complete job baseline. |
| `test_129_sampled_resume_history.py` | Refuse conflicting history before admission, preserve sampled lineage after failed repair, and check selected parent lineage before preparation. |
| `test_130_sampled_resume_history_writers.py` | Missing-history refusal, retained results through failed resume, atomic pending-write rollback, and file restoration after history changes during preparation. |
| `test_131_native_command_planning.py` | All nine previews use native prerequisite state despite raw-status disagreement and include retained publication receivers outside graph edges. |
| `test_132_between_command_execution.py` | Fresh and resumed interval execution, excluded operator work, producer cleanup, native alignment, and publication without receiver execution. |
| `test_133_readonly_reset_live_refusal.py` | Applied reset forms refuse a live interrupt before imports or mutation, preserving all database rows and project bytes. |
| `test_134_between_external_prerequisites.py` | Wide interval preparation with a blocked interior merge across all four runners, preserving excluded component state, jobs, and node trees. |
| `test_135_graph_preview_busy_boundaries.py` | Valid reservations, holds, and pending work render as busy; damaged links fail; excluded receiver guards and active jobs appear without mutation. |
| `test_136_nine_command_wiring_regressions.py` | Stale-session reset refusal, parent drift after reservation, consistent interval order, unknown refusal-boundary validation, and exact interval repair guidance. |
| `test_137_between_run_membership.py` | Public split and merge repair through fresh run and reset, exact preparation scope, preserved execution selection, and unchanged unrelated data. |
| `test_138_membership_reconciliation_boundaries.py` | Automatic reconciliation without reusable history, retained unowned jobs and inputs, unchanged components, and refusal of non-fresh reuse or damaged state. |
| `test_139_membership_commit_boundaries.py` | Regional mapping changes after all preparation units commit, failure and retry behavior, historical producer cleanup, and removed members. |
| `test_140_unchanged_membership_execution_shapes.py` | Resume and selected execution across unrelated graph changes, exact new producing identity, and preserved earlier ownership and successful results. |
| `test_141_membership_alignment_generation.py` | Repaired membership starts above the alignment generations of all overlapping historical components. |
| `test_142_regional_membership_session_continuity.py` | Disjoint fresh repair preserves a live interrupt's admitted membership, while overlapping repair refuses before session, database, or filesystem mutation. |
| `test_143_unproduced_membership_reconciliation.py` | Read-only and applied no-history reconciliation agree for resume and selected execution; returning components start queued above their overlapping history. |
| `test_144_deletion_only_membership_reconciliation.py` | Entirely removed no-history components leave active membership while preserving stored data; live overlap refuses atomically and reusable history remains active. |
| `test_145_state_listener_retirement.py` | Transient subscriber-record deletion failures and delayed old listener startup cannot leak records or retire a replacement listener. |
| `test_146_native_applied_recovery.py` | Applied recovery for every abandoned session, exact ownership, preserved unrelated work, stale holds, and automatic recovery before mutation. |
| `test_147_native_recovery_atomicity.py` | Atomic session recovery, output restoration, writer failures, and recovery refusal after observed state changes. |
| `test_148_native_cleanup_recovery.py` | Recovery of interrupted preparation and managed-input publications, exact retained files, retry, and read-only cleanup previews. |
| [test_149_interrupt_declarations.py](test_149_interrupt_declarations.py) | Exact Boolean interrupt declarations, component classification, raw waiting preservation, and source validation before import. |
| [test_150_interrupt_policy_preflight.py](test_150_interrupt_policy_preflight.py) | Reachable interrupt choices and read-only plans for every executing graph command, selected jobs, and sampling. |
| [test_151_ordinary_interrupt_stopping.py](test_151_ordinary_interrupt_stopping.py) | Ordinary interrupt stops, independent branches, selected-job preservation, and explicit session bridges. |
| `test_152_native_recovery_terminal_identity.py` | Terminal session identity, immutable producing ownership, and refusal of damaged recovery state. |
| `test_153_native_recovery_unstarted_components.py` | Abandoned admitted components with no started jobs, exact session settlement, and unchanged unrelated work. |
| [test_154_interrupt_scope_transfers.py](test_154_interrupt_scope_transfers.py) | Atomic interrupt admission, actual parent ownership, scope transfer, conflicting claims, and scope return. |
| [test_155_explicit_interrupt_execution.py](test_155_explicit_interrupt_execution.py) | Main and nested predecessor pauses, frozen target input, hold release, and later input without automatic reexecution. |
| `test_156_native_session_recovery_receipts.py` | Durable recovery receipts, interruption before and after commit, exact intended effects, and repeated recovery. |
| `test_157_preparation_recovery_boundaries.py` | Partial staging allocation, changed replacement directories, retained private material, and restoration after interrupted cleanup. |
| `test_158_preparation_input_receipt_integrity.py` | Preparation and input receipt intent, terminal decisions, changed metadata, and refusal before recovery mutations. |
| `test_159_native_recovery_saved_cleanup.py` | Replaced saved files during cleanup, exact file detachment, interrupted deletion, and safe retry. |
| `test_160_preparation_attempt_ownership.py` | Complete preparation admission, suppressed guard insertion, and changed attempt or guard ownership before release. |
| `test_161_suppressed_file_receipt_insert.py` | Suppressed preparation and input receipt inserts refuse before visible file changes. |
| `test_162_terminal_receipt_readback.py` | Reverted terminal receipt writes roll back database changes and restore files; suppressed native recovery intent refuses before staging. |
| `test_163_native_cleanup_locks.py` | Bounded refusal while required recovery locks are held, exact retained state, and progress for compatible unrelated cleanup. |
| `test_164_membership_attempt_finalization.py` | Membership attempts require the final regional update, preserve completed unit cleanup on failure, and remain retryable. |
| `test_165_input_decision_observation.py` | Original notification errors survive failed receipt reads, with exact durable database state and recovery files preserved. |
| [test_166_native_restart_recovery.py](test_166_native_restart_recovery.py) | Durable restart intent, interrupted output moves, atomic writer decisions, exact restoration, and cleanup after a later execution. |
| [test_167_native_readonly_cleanup.py](test_167_native_readonly_cleanup.py) | Read-only native output cleanup and retry after interruption before deletion, with exact durable state preservation. |
| [test_168_interrupt_preflight_integration.py](test_168_interrupt_preflight_integration.py) | Static declaration forms, interactive policy order, and source/topology revalidation before recovery. |
| [test_169_native_thread_ownership.py](test_169_native_thread_ownership.py) | Exact live or pending thread ownership, independent node values, read-only inspection, and damaged ownership refusal. |
| [test_170_interrupt_preparation_preservation.py](test_170_interrupt_preparation_preservation.py) | Stopped failures retain their job owner, generation, output, and trace across all six graph commands. |
| [test_171_interrupt_static_exports.py](test_171_interrupt_static_exports.py) | Indeterminate router exports refuse before abandoned-session recovery or user-code import. |
| [test_172_interrupt_plan_effects.py](test_172_interrupt_plan_effects.py) | Plans retain stopped selection and its preserved outputs, jobs, and traces. Explicit interrupt plans override readiness only at the start component and preserve all project state. |
| [test_173_interrupt_cooperation.py](test_173_interrupt_cooperation.py) | Cooperative checkpoints outside publication locks across all runners, retained NodeHandle publication, and retained API jobs when interruption precedes their claim. |
| [test_174_native_api_total_atomicity.py](test_174_native_api_total_atomicity.py) | Concurrent relative aggregate API-limit updates both apply in one serialized writer decision. |
| [test_175_interrupt_fence_semantics.py](test_175_interrupt_fence_semantics.py) | Fences follow actual partial results or readiness overrides; a main session continues through an ordinarily ready child result without rerunning it. |
| [test_176_interrupt_sample_freeze.py](test_176_interrupt_sample_freeze.py) | Final sample selection includes input published before predecessor acknowledgement, while an earlier replay refuses changed input before preparation. |
| [test_177_interrupt_resume_authority.py](test_177_interrupt_resume_authority.py) | Aligned sampled interrupt resume preserves completed work and its origin, including retained failed work. Completed incompatible parents refuse; later ordinary commands cross only boundaries already present at their admission. |
| [test_178_sqlite_storage_owner_close.py](test_178_sqlite_storage_owner_close.py) | Closing one storage owner preserves peer writers; a closed owner can reopen, and graph setup releases owned connections on success or failure. |
| [test_179_project_api_admission.py](test_179_project_api_admission.py) | A paused API parent yields capacity to its interrupt child, including a claimed parent whose handler has not started, and reacquires capacity before continuing. |
| [test_180_cli_workflow_owner_cleanup.py](test_180_cli_workflow_owner_cleanup.py) | CLI inspection, sampling, and import failures release their storage owners without retained SQLite sidecars. |
| [test_181_native_diagnostics.py](test_181_native_diagnostics.py) | Doctor preserves closed project bytes without importing user code, reports exact live and stale sessions, and failed-job guidance uses each job's native owner. |
| [test_182_native_clipboard_history.py](test_182_native_clipboard_history.py) | Copied sampled jobs retain successful history, exact job identities, and creating owners; missing retained history refuses restoration. |
| [test_183_native_clipboard_scope.py](test_183_native_clipboard_scope.py) | Node-scoped same-project restoration preserves component peers, rejects changed peer work, and recovers interrupted file publication. |
| [test_184_sqlite_failed_reopen_owner_cleanup.py](test_184_sqlite_failed_reopen_owner_cleanup.py) | Failed native database reopening releases its storage ownership without relying on cyclic garbage collection. |
| [test_185_project_api_permit_faults.py](test_185_project_api_permit_faults.py) | API admission failures retain exact claims for recovery and bypass retries, fallbacks, timeout masking, and concurrent restart masking. |
| [test_186_project_api_capacity.py](test_186_project_api_capacity.py) | Cross-process capacity includes pending provider calls when the limit changes. Native recovery releases abandoned capacity while preserving a continuing session. |
| [test_187_interrupt_membership.py](test_187_interrupt_membership.py) | Fresh upstream membership repair preserves the jobs, producing owners, outputs, and state of an unchanged stopped branch. |
| [test_188_lineage_trace.py](test_188_lineage_trace.py) | Read-only compact lineage shows direct job creation, exact sample and interrupt identities, retained origins, and current first misalignment causes. |
| [test_189_native_component_display.py](test_189_native_component_display.py) | Monitor and inspect distinguish component state from raw job counts and show stability, interrupt origin, and late-arrival misalignment. |
| [test_190_native_reservation_admission_marker.py](test_190_native_reservation_admission_marker.py) | Recovery distinguishes interrupted reservation admission from lost admitted scope, and terminal validation follows nested interrupt transfers. |
| [test_191_native_clipboard_recovery.py](test_191_native_clipboard_recovery.py) | Clipboard rename and cleanup interruptions remain recoverable; changed terminal receipts retain recovery material, and committed preparation units remain usable after a later unit aborts. |
| [test_192_native_api_limit_display.py](test_192_native_api_limit_display.py) | Live disjoint API sessions report their local requests and one shared project capacity consistently in monitor, inspect, top, and threads. |
| [test_193_native_node_lifecycle_observation.py](test_193_native_node_lifecycle_observation.py) | Public node lifecycle readers and top use native component results. Readiness observes uninitialized parents without mutation and refuses damaged established state. |
| [test_194_unadmitted_instability_origin.py](test_194_unadmitted_instability_origin.py) | Writers and read-only lineage reject an interrupt origin that never acquired execution scope. Terminal admitted origins remain valid after reservations end. |
| [test_195_grouped_api_permits.py](test_195_grouped_api_permits.py) | Mixed permit decisions preserve order, exact ownership, capacity, and unrelated state when a peer request is invalid or its write is suppressed. |
| [test_196_sqlite_connection_registry_scaling.py](test_196_sqlite_connection_registry_scaling.py) | Repeated database reads avoid scanning historical project paths while connection admission still reclaims dead owners, deleted projects, and reused thread identities. |
| [test_197_interrupt_pause_candidate.py](test_197_interrupt_pause_candidate.py) | Empty pause polling avoids full owner reads; every raw candidate retains strict validation, and stale executions refuse before the probe. |
| [test_198_thread_owner_observation.py](test_198_thread_owner_observation.py) | Live thread limits preserve distinct component-member values, reread changed limits, and refuse damaged later active claims without changing ownership. Interrupt child jobs retain their exact parent sessions. |
| [test_199_native_only_runtime_surface.py](test_199_native_only_runtime_surface.py) | Obsolete API and network options refuse; supported strategies retain job results, native session settings, and network request behavior. |
| `test_active_job_restart.py` | Generation-fenced restart during active threaded, direct, and process runs, refusal cases, fast-path imports, and replacement-runtime isolation after the old checkpoint deadline and stale-write attempt. |
| `test_autostart_cycles.py` | Self, mutual, diamond, ring, and stochastic cyclic scheduling. Run each test in a fresh process. |
| `test_benchmark_exit_codes.py` | Nonzero benchmark status for workflow exceptions, failed jobs, unfinished work, missing expected rounds, and repeated-API slowdown beyond the fixed allowance; successful completion and exact allowance equality. |
| `test_checkpoint_keyword_api.py` | Keyword checkpoint fields, persistence, validation, dynamic deadlines, fallbacks, and router schema. |
| `test_cli_help_and_reset.py` | Help and describe output, reset semantics, selected-job runs, initialization sidecars, and component-expanded preparation. |
| `test_cli_monitor.py` | Bulk summaries, inline and standalone monitoring, timing metadata, reuse, combined statistics, and diagnostic monitor failure. |
| `test_deploy.py` | Deployment setup, ignore rules, local archives, remote extraction, PuTTY use, and fresh SQLite initialization. |
| `test_file_entry_node_input_import.py` | Binding a node-input filesystem entry during directory creation. |
| `test_filesystem_objects.py` | Node input and output filesystem declarations, safe reads and writes, templates, generation fencing, bulk JSON reads, encoding, atomic copy, and absence of per-job file APIs or automatic returned-file copying. |
| `test_framework_improvements.py` | Graph paths, doctor and inspect, recovery, timeout fencing, failure-history exposure, current fallback terminal error, removed staging-helper behavior, explicit idempotency keys, resume, plans, dry runs, help, and checkpoint supervision. |
| `test_graph_sync_and_fans.py` | Explicit graph updates, edge-only changes, compact directed fan forms, and the `fan` helper. |
| `test_init_clipboard_debug_028.py` | Deployment archive initialization, node clipboard copy/paste, and debug output. |
| `test_markov_chain_stress.py` | Marked long-running deterministic cyclic filesystem stress. |
| `test_output_and_runner_edges.py` | Mixed direct and threaded node execution under global runner choices. |
| `test_reliability.py` | Router validation, job parent identity, component readiness, cancellation, observed overlap of independent ready-node handlers with a direct-runner serial control, process execution, dynamic spawn, and high fan-in. |
| `test_release_packaging.py` | Builds a source archive and verifies inclusion of project guidance, all seven skills, documentation, examples, benchmarks, and tests while excluding injected HTML and cache sentinels. |
| `test_runtime_thread_overrides.py` | Runtime thread changes, adaptive worker scaling, aggregate API allocation and redistribution, set/reset deprecation warnings, rejected API-budget values without stored changes, invalid nodes, high limits, warnings, and concurrent run claiming. |

## Shared helper

`state_helpers.py` constructs `FileStorage` instances and seeds jobs and status
for tests that need SQLite-backed state without repeating setup code.

`tests/__init__.py` marks the directory as a Python package and contains no test
behavior.
