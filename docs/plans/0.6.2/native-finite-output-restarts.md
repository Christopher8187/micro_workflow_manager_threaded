# Finite calls after output failure

Status: focused and adjacent checks pass; [bounded review](../../../../testing_ground/issue-45/native-finite-output-restart-correction-review.md) passed.
This follows [finite admission](stage-native-finite-restart-admission.md) and
[component output cleanup](native-output-publication-cleanup.md).

Publication errors already carried the exact attempt, but the finite operation
discarded it for infrastructure errors. A CLI restart could therefore succeed
before the session exited without its successor running.

The finite operation now retains any attached attempt while the slot has an
error. The outer driver admits only accepted successors, keeps ordinary
admission stopped, then raises the original infrastructure exception.
The eager retry path still handles ordinary job failures separately.

The public regression uses `run_jobs` and `run_node_jobs`, each with direct and
threaded runners. One worker executes the selection `[2, 1, 3]`. Job 2 finishes;
job 1 loses its output write; job 3 stays unstarted. A real CLI restart commits
before the terminal decision. Assertions require one successor, unchanged peer
output and events, the same incarnation and session, and reservation release.

All four cases failed before correction in 5.14 seconds. Corrected freeze
`03C56A964803E8F77BE6372A2DA8447D141834CCB241F9454E6F104AEB7E97EC`
passed all four plus the native idempotence cleanup control in 5.69 seconds.
Its 168 surrounding checks passed in 177.44 seconds.
The latter now registers cleanup before any assertion. API and process
publication-error transport receive no additional acceptance from these cases.
