# Native current job owner

Status: in progress. The incarnation and duplicate-claim corrections have
independent source approval. Their combined native selection passes 367 checks.
This work implements the persistence portion of
approved AQ6, Q8, and Q9 and supports `44-SES-027`, `44-SES-042` through
`44-SES-044`, and `44-REC-005`. No command or complete requirement is accepted.

The native job-instance row now retains an explicit `last_execution_id` foreign
key. Job claim publication updates that reference in the same transaction as
the active execution, owner row, and started events. Completion, unstarted
release, generation restart, and diagnostic trace clearing preserve it. The
most recent committed claim replaces it even when both claims have the same
generation and neither handler started.

`read_job_current_owner` reads the job, its instance, and its referenced owner
in one SQLite snapshot. The active claim is authoritative while present and
must agree with the stored last reference. Missing or malformed instances,
dangling owners, conflicting active references, and ownership naming another
job refuse. Each owner now records the claim-time job instance. A pointer to
an owner from a deleted incarnation refuses even when the numeric address and
generation match. The lookup returns the exact stored execution and session.

Claims also refuse an existing active lease. Separate requests collected by
the mutation writer cannot both claim one job. The later overlapping request
fails as a whole, including any other jobs in that request. The final lease
update requires the observed generation and an absent active execution, and
its changed count must match before owner and event insertion. Terminal status
alone does not prohibit direct execution after its prior lease has ended.

Both fresh-reset batch methods clear only selected references within their
existing transaction. Single-job CLI fresh reset now uses that same batch
operation. Deleting a job removes its instance and reference by cascade.
Recreating the numeric ID receives a new instance with no owner. Historical
owner rows remain available for retained operational and lineage needs.
This does not implement later history pruning or coherent clipboard transfer.

## Verification

Records under Parent Repo `testing_ground/issue-45/` use the
`sample-calculations-native-current-owner-` prefix. Root used the sole
sequential executable lane, immutable Test Area copies, and runner revision 04.
Freezer revision 13 adds the owner test, claim, cleanup, and CLI cleanup paths
to the permitted overlays. Its SHA-256 is
`96113D37AD526B9F334A8FF72F5B2EB61519CFA4851D6540F05FD91AC4FDF6B3`.

| Run | Result | Observation |
| --- | --- | --- |
| `red-01` | 15 failed in 11.93 seconds | The new job-scoped owner reader was absent. |
| `green-01` | 15 passed in 11.64 seconds | Four terminal states, trace clearing, reopen, latest released claim, six fresh-reset cases, and four deletion/recreation paths. |
| `failures-red-01` | 1 failed, 8 passed in 5.61 seconds | Malformed instance identity was accepted; generation restart, three injected rollbacks, and four other damaged-reference checks passed. |
| `green-02` | 24 passed in 15.37 seconds | Malformed instance refusal corrected and all focused checks pass. |
| `adjacent-01` | 241 passed, 7 failed in 124.22 seconds | Existing identity-damage fixtures supplied three positional values after a fourth column was added. These are fixture errors, not behavioral RED results. |
| `adjacent-02` | 248 passed in 126.21 seconds | Explicit fixture column names corrected; native modules 079 through 084 pass on green 03. |
| `incarnation-red-01` | 1 failed in 1.36 seconds | A replacement job accepted its deleted predecessor's retained owner. |
| `incarnation-green-01` | 25 passed in 19.45 seconds | Claim-time incarnation storage and current-instance equality correct that defect. |
| `incarnation-adjacent-01` | 249 passed in 150.99 seconds | Green 02 strengthens refusal preservation and verifies a subsequent replacement claim; modules 079 through 084 pass. |
| `adjacent-fixtures-01` | 76 passed, 8 deselected in 49.05 seconds | Native owner tests, component schema damage, six owner-instance declaration cases, and two claim-time instance damage cases. Cancelled version-four-only cases were excluded. |

The separate `sample-calculations-native-claim-exclusivity-` records show three
behavioral RED failures in 2.10 seconds. Both single and batch APIs overwrote an
active execution, and two real batch requests behind a writer barrier both
succeeded. The correction passes 70 native owner checks with eight cancelled
cases deselected in 42.18 seconds. The wider run includes native tests from
modules 071, 072, and 079 through 084. It passes 367 checks with nine cancelled
version-four-only cases deselected in 162.16 seconds. No timing threshold changed.

The current green-02 freeze has 314 Python files and SHA-256
`4234D80BCEA9BC930D51B1FA2198991CCA2DD1BD7792EA42418EB93735910957`.
Freezer revision 14 additionally permits the two adjacent ownership test files;
its SHA-256 is `BF29457829A419E3144FBCC5FC235E14212A78D63506E8C692ABC7F14F0C8ACE`.

## Independent review

The [initial owner review](../../../../testing_ground/issue-45/native-current-job-owner-source-review.md)
found NJO-001, the missing relationship between a retained owner and its job
incarnation. Its [correction review](../../../../testing_ground/issue-45/native-current-job-owner-incarnation-correction-review.md)
passes that correction within its private boundary. The correction report has
SHA-256 `245AD8BE0D1513D7DB4AA821016F1D7D904C16A971383E5662687BC2C18E049D`.

The [complementary review](../../../../testing_ground/issue-45/native-current-job-owner-complementary-review.md)
found NJO-C01, duplicate active claims, plus the two adjacent fixture defects.
Its SHA-256 is `A019CC8C3BD84D521164AEA3DFA9F6FD4F3BD5F66D4C23C70DB5AD0BB796FA76`.
Both independent reviewers used GPT-5.6 Sol with xhigh reasoning. Root inspected
their source findings and full reports. The
[exclusivity correction review](../../../../testing_ground/issue-45/native-current-job-owner-claim-exclusivity-correction-review.md)
passes its bounded source assignment. Its corrected SHA-256 is
`1F34E503A119F25E15D7530404AB8534D90EE132628CBBD9809FF519219334AF`.
The correction identifies the six declaration checks as same-process reopening
and records the 367-check result. No whole requirement receives acceptance here.

## Remaining work

Restart, recovery, and ownership-dependent controls still need this lookup
integrated with their exact session and reservation checks. The separate
[native job-inspection section](stage-native-job-inspection.md) now consumes
the shared owner validation. Public
fresh-preparation scope and filesystem rollback remain unfinished. These tests
establish SQLite rollback at owner-reference failure; they do not establish
rollback of earlier file removal. Native clipboard state, producing lineage,
component lifecycle, history retention cleanup, old reader removal, and Q10's
dynamic membership decision remain outside this result.
