# Private job identity storage

Status: implementation in progress. No stage acceptance or complete requirement
credit. The preceding [private terminal storage](stage-component-terminal-storage.md)
stage is accepted and committed locally as
`ecfad3781bd55bdcbd1a390e7c8312874a02d613`.

The later [one-model scope change](single-model-scope.md) cancels the legacy
import and backfill portions below. They remain historical results only.
Their implementation and dedicated tests have been removed from the active
identity section. Native identity and rollback work continues.

This section supplies a durable identity for each job incarnation in private
version-5 storage. Updates retain it; deletion removes it; recreation at the
same node and numeric job ID assigns a new value. The current input remains
the input authority. An identity alone supplies neither input drift detection
nor sampling behavior.

The [preparation](../../../../testing_ground/issue-45/job-instance-identity-preparation.md),
[schema preparation](../../../../testing_ground/issue-45/job-instance-identity-schema-preparation.md),
and [failure fixtures](../../../../testing_ground/issue-45/job-instance-identity-failure-fixtures.md)
define the private boundary. Potential prerequisite evidence concerns
44-SMP-020, 028, 029, and 032, and 44-REC-024. All remain incomplete.

## Test-first progression

The [first candidate review](../../../../testing_ground/issue-45/job-instance-identity-first-red-review.md)
accepted the unrun design. Root copied it unchanged into test 079 after the
preceding local commit. Every run below uses a separate verified 304-file copy
made by `freeze_job_instance_revision01.py`. Names have the
`sample-calculations-job-instance-` prefix and `-run` suffix. Each freeze
finished before its dependent test started.

| Run suffix | Result | Meaning |
| --- | --- | --- |
| `red-01` | 1 failed, 0.68 seconds | Missing reader after valid private job creation. |
| `green-01` | 1 passed, 0.42 seconds | Stored identity survives input refresh and reopen, but changes after deletion and recreation. |
| `legacy-red-01` | 2 failed, 1 deselected, 0.81 seconds | Outer `OR IGNORE` accepted an identity insertion conflict and an orphan address. |
| `legacy-green-01` | 3 passed, 0.61 seconds | Explicit trigger aborts preserve logical state and every legacy file. |
| `reader-red-01` | 9 failed, 3 deselected, 1.48 seconds | Missing or malformed identities were returned without refusal. |
| `reader-green-01` | 12 passed, 2.09 seconds | The reader validates exact lowercase hexadecimal text without repair. |
| `value-red-01` | 8 failed, 1 passed, 12 deselected, 1.44 seconds | Invalid stored values were accepted; the existing null refusal already passed. |
| `value-green-01` | 21 passed, 2.66 seconds | The database rejects invalid identity values. |
| `nul-red-01` | 1 failed, 1 passed, 21 deselected, 0.51 seconds | SQLite text checks accepted a NUL suffix; the reader already refused it. |
| `nul-green-01` | 23 passed, 2.91 seconds | Byte-length validation closes that database gap. |
| `trigger-red-01` | 7 failed, 23 deselected, 2.98 seconds | Fresh processes accepted missing or changed triggers and a private trigger in version 4. |
| `trigger-green-01` | 30 passed, 5.63 seconds | Initialization validates exact trigger declarations and detects partial private state. |
| `reopen-red-01` | 11 failed, 30 deselected, 4.74 seconds | Fresh processes accepted missing, orphaned, or malformed identities. |
| `reopen-green-01` | 41 passed, 11.44 seconds | Initialization refuses those rows without repairing logical state. |
| `backfill-red-01` | 1 failed, 41 deselected, 0.44 seconds | The private schema helper left preexisting jobs without identities. |
| `backfill-green-01` | 42 passed, 10.21 seconds | An explicit private schema transaction assigns first known identities and preserves existing jobs and input. |
| `schema-preservation-01` | 53 passed, 15.55 seconds | Unchanged-source checks cover rollback after an earlier identity inserted, damaged table declarations, version-4 refusal, and healthy fresh-process reopen. |
| `paths-preservation-01` | 66 passed, 18.45 seconds | Creation, reuse, import, reconciliation, and deletion checks before the scope change. Legacy portions are now historical only. |
| `failure-preservation-01` | Not run | The completed freeze was preserved when Christopher cancelled legacy compatibility. |
| `native-preservation-01` | 65 passed, 1 fixture failure, 22.16 seconds | The added claim fixture omitted required session ownership. This is not an implementation regression or new RED credit. |
| `native-preservation-02` | 66 passed, 21.68 seconds | The corrected fixture uses a real owning session and reservation. Native identity, creation failure, grouped rollback, reset, and restart preservation pass after legacy-only retirement. |

The current freeze is `sample-calculations-job-instance-native-preservation-02`,
SHA-256 `FE67A0DBF9E9C39FB56DAE91582254152B2D4ED744F6AC890EBD8F39A934065B`.
It contains schema source
`CB53C0CA08DF59CA4A7CE53F0B09F9CED24F02B901895D4D8F76FBDBB89001A0`,
reader source
`423EC0DA457CCD87F5E1E9F9D84F80036F68732358E1AFAE07710077E961191C`,
and test 079
`E72B2BB8D371CFF5D9AA791DF380D2C22064A8B303E227F376F85B1AB96D1F47`.

The retired backfill fixture called the private helper inside an explicit disposable
transaction. Its failure control delegates real SQLite statements and installs
a test trigger after table creation. That trigger refuses only once an earlier
identity exists. The failure rolls back the new schema and identity rows while
preserving the version-4 rows and payloads. Its code and checks are cancelled.
The [retirement record](../../../../testing_ground/issue-45/job-instance-legacy-retirement.json)
preserves the exact removed functions and source hashes. The runtime backfill
statement and its legacy-only trigger precondition were removed. Native
insertion still requires a new identity, and the table rejects an occupied
identity address. A separate native suppression fixture checks the retained
postcondition without invoking a legacy importer.

## Remaining work

Broader native verification and independent stage reviews remain pending.
The pending acceptance group now includes [native project bootstrap](stage-native-project-bootstrap.md).
Ordinary creation uses the new model, and its latest combined focused selection
passes all 66 identity checks and 28 bootstrap checks. The old version-4
initialization path has been removed. Broader native integration remains pending.
Population and input snapshots, manifests, seed replay, causal execution, and
final Astra review remain later required work. Embedded legacy migration and
backfill are cancelled and are not readiness blockers.
