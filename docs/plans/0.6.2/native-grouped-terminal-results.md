# One terminal result within a writer group

Status: focused checks, 330 surrounding checks, and [correction review](../../../../testing_ground/issue-45/native-grouped-terminal-correction-review.md) passed.

The [complementary review](../../../../testing_ground/issue-45/native-terminal-owner-complementary-review.md)
found that two submissions for one job used the same initial row. Both callers
received success and both events were inserted, although only the first update
changed the job. Different terminal statuses produced contradictory history.

A real writer gate now tests duplicate and conflicting submissions through
`finalize_job_execution`. Either submission may carry the native owner
expectation. Same-status duplicates must produce one event. A conflicting
second status must refuse while preserving the first result and output.
All four cases failed before correction in 2.43 seconds.

The writer now records each accepted result in its per-job batch state.
Later submissions see that result before SQL publication. Corrected freeze
`266A2C406886BE873781122884297838D07BC2F2C3A3B3669EBCF3DB51F10697`
passed all 15 grouped-result, saved-output, finite-restart, and cleanup checks
in 9.74 seconds. This correction does not expand generic recovery or native
damage repair.
