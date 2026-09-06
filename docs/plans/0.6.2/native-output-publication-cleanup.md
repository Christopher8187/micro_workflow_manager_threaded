# Output publication failure and restart

Status: corrections under verification. This follows [mixed runner cleanup](native-mixed-runner-continuation.md).

An output write failure leaves a started native claim running. Cleanup formerly
used an unconditional status update for that claim. A real CLI restart between
cleanup's read and write therefore lost its queued successor.

Cleanup now captures validated session and component ownership before output
reconciliation. It records started cleanup attempts separately from unstarted
preclaims and uses the generation-checked terminal writer. The component retains
accepted successors while preserving the original infrastructure exception and
keeping ordinary admission stopped.

Restart before the first cleanup scan exposed another gap. Publication errors
now carry the actual execution attempt as they leave the job controller.
The component retains that attempt immediately, so continuation does not depend
on finding the old generation in a later running-row scan.

## Verification

Parent Repo names start with `sample-calculations-native-output-cleanup-`.

| Run suffix | Result |
| --- | --- |
| `restart-red-01` | All 3 runner cases failed; hardened two-lane control passed, 5.40 s |
| `preservation-01` | Native public cleanup without restart passed, 0.89 s |
| `restart-green-01` | All 27 cleanup and abandonment cases passed, 42.65 s |
| `earlier-restart-red-01` | Before-scan restart failed for 3 runners; 6 controls passed, 8.88 s |
| `earlier-restart-green-01` | All 75 component and native cleanup checks passed, 140.58 s |

The latest `earlier-restart-green-01` freeze is
`EB251E54C3DAFB2A815C4EE4D2A2E55F518A998AAC8F8711408BC17C8F982594`.
The [independent review](../../../../testing_ground/issue-45/native-output-cleanup-earlier-restart-review.md)
accepts that bounded correction. The next broader run passed 306 checks and
exposed one obsolete anonymous-claim setup. Its native replacement passed.
[Atomic cleanup ownership](native-terminal-cleanup-ownership.md) records the
subsequent damaged-state correction. Lost processes and standalone finite-call
publication failures remain open.
