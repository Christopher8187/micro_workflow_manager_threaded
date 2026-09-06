# Component results and session exit

This private increment joins calculated component results to
`decide_execution_session_exit()`. The writer checks selection, ownership,
reservation, producing shape, alignment generation, and running state. Success
preserves retained lineage and requires every job to be done or skipped.
Failure clears lineage. Active work prevents terminal publication.

An accepted restart returns its successor while preserving the running
component, session, and reservation. Otherwise component results, session exit,
and reservation release commit together. A suppressed write rolls back the
whole batch, including injected trigger changes. Nonempty terminal batches must
include every running component currently reserved by the settling session.
Queued components may remain omitted. Work transferred to another valid owner
keeps its active claim, state, reservation, and history.

The initial missing-operation check failed before implementation. Two later
checks exposed successful returns after suppressed session or reservation
writes; row-count guards corrected both. Independent review then found the
omitted-running-component case. Its regression failed before the completeness
guard and passed afterward. The final 33 focused checks and one native reopening
check passed in 11.33 seconds.

The earlier adjacent run recorded 458 passes, 17 failures, and two deselections
in 382.97 seconds. It receives no whole-selection pass credit. Fifteen failures
expect cancelled old-model behavior; one expects the native project marker to
be absent. One native case received subprocess-pipe warnings and passed in the
final run. A preceding fixture error also receives no behavioral failure credit.

Final freeze `sample-calculations-component-session-settlement-omitted-green-01`
has SHA-256 `7A89670C69489D7C10AA83F24CCA929842F3B22EFC82877392325EB0D9C02F62`.
The independent Sol xhigh [lifecycle review](../../../../testing_ground/issue-45/component-session-settlement-lifecycle-review.md)
and [restart review](../../../../testing_ground/issue-45/component-session-settlement-restart-review.md)
record correction and acceptance limits.

This private increment completes no whole requirement. Parent observations,
result calculation, and shared public lifecycle activation remain unfinished.
