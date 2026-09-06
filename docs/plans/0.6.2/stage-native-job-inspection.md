# Native job ownership in inspection

Status: bounded source review passed, with 18 focused and 389 adjacent passes.
This supports AQ6, Q8, Q9, `44-SES-044`, and `44-DOC-023`;
no whole requirement is complete.

`mwf inspect NODE job ID` now displays the stable job instance, active or last
committed owner, execution ID, exact session ID, kind, persisted status, parent,
outcome, component, and claim generation. Unclaimed jobs are explicit. Trace
clearing preserves ownership; another main session never replaces the recorded owner.

`read_job_owner_observation` reads job state, identity, owner, immutable selected
scope, and session metadata with one joined SQLite statement. It shares owner
validation with `read_job_current_owner` and preserves that method's return
shape. Missing or conflicting ownership refuses before printing. Inspection
also refuses if deletion and recreation replace the observed instance before
the job payload is loaded.

Process liveness is observed after the durable snapshot and printed separately
from persisted session status. The existing liveness decisions are unchanged.
Two reason strings no longer call valid native sessions legacy. Nullable
job-local process fields display as not recorded and do not erase a valid owner.
The reader emits no mutation or queue notification.

## Verification

Root used immutable Test Area copies and the sole executable lane. Records in
Parent Repo `testing_ground/issue-45/` use the
`sample-calculations-native-job-inspection-` prefix.

| Run | Result | Observation |
| --- | --- | --- |
| `red-01` | 3 failed, 2.85 s | Ownership display absent; an unexercised status expectation needed correction. |
| `red-02` | 3 failed, 2.36 s | Correct native terminal-status expectation; display remained absent. |
| `green-01` | 39 passed, 23.45 s | Three inspection cases and 36 ownership cases. |
| `liveness-red-01` | 5 failed, 3.21 s | Separate live/stale observations absent. |
| `green-02` | 8 passed, 7.44 s | Exact owner and liveness display. |
| `preservation-01` | 14 passed, 10.43 s | Damaged joins, replacement race, actual CLI preservation. |
| `owner-scope-red-01` | 1 failed, 1.05 s | A valid foreign key named a session whose scope excluded the owner component. |
| `green-03` | 51 passed, 33.59 s | Requires the immutable session/component pair. |
| `native-wording-red-01` | 3 failed, 2.62 s | Nullable-identity reasons said legacy; missing process fields printed None. |
| `green-04` | 18 passed, 12.26 s | Native wording and missing metadata. |
| `adjacent-01` | 389 passed, 9 deselected, 182.24 s | Native modules, CLI inspection, and live checkpoint display. |

Green 04 contains 315 Python files. Its freeze SHA-256 is
`ABE9973DC97C22F8FAAC12CCCD5D6542079E79131622A227B5C6486C3F20BB47`.
Freezer revision 16 has SHA-256
`1426EF77A5B1E13646CB5D9BD630C0C20E75AED950C19B100D2B3959FA372966`.

## Review and remaining work

Independent GPT-5.6 Sol xhigh review identified the missing immutable
session/component validation and misleading nullable-identity wording. Both
received sensitive failing checks and corrections. The independent
[final source review](../../../../testing_ground/issue-45/native-job-inspection-source-review.md)
passes green 04 within this boundary. Its SHA-256 is
`7F6F18C5BF7A3B9EC5D1004858B032610C260FE52EA049C1956AE58EBE61651C`.
Root read the full report.

The [restart and inspection preparation](../../../../testing_ground/issue-45/native-restart-inspection-preparation.md)
has SHA-256 `3E7F442509A8A279C98C0A327166D533FB13D6AA36A2A6B555A5A5899DD3F216`.
Recovery, full lineage, component-state display,
`inspect_failed` singleton removal, graph-free inspection, and complete
read-only filesystem guarantees remain unfinished. These tests establish
unchanged stored rows, with runtime-file consistency still pending.
