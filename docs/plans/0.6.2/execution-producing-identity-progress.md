# Producing execution identity progress

This continues [Implement and verify the agreed MWF 0.6.2 workflow-management
changes](https://github.com/Christopher8187/product/issues/45) from
`4039154a362edf7e4053b3ce1eb24771fb0f7404`. The current portion records producing
execution history and selected-job claim boundaries. Integrated and additional affected-path checks passed, and both independent
Sol reviews returned PASS for this bounded portion. It completes no whole requirement.

## Implemented behavior

Every accepted job claim captures its producing graph shape, exact component,
and alignment generation in the same writer transaction as its execution
owner. Historical reads use that retained shape. They do not reconstruct it
from current component definitions or require a pending full-component result.
Stored graph shapes reject updates. Their decoder validates canonical graph
structure and every raw node name.

Job creation records the actual active task execution independently of the
logical parent and optional trace. This applies to explicit IDs, allocated IDs,
and grouped publications through task handles and public `add_job`/`add_jobs`
calls. A task cannot use another active task's retained handle. Finished or
superseded producers refuse before publication. Default declarations and
creation outside a task remain unowned. Idempotent reuse preserves the original
creator. Reset preserves creator ancestry while clearing the current/last
claim; deletion preserves historical execution records needed by descendants.

Selected sessions retain exact root instances and an explicit selection kind.
Claims accept those instances or descendants linked through actual creator
executions in the same session, component, shape, and alignment generation.
Recursive ancestry detects missing links and cycles. Recreated numeric IDs and
unrelated jobs cannot enter through an old root address. Grouped writer refusal
still permits an independent valid claim batch. Damaged selected declarations
refuse before the exit decision changes state.

Accepted child restarts can continue after a selected root returns or propagates
a failure. The root's Python value and completed work are retained. A valid
pending full-component result follows the replacement so that component can
settle. Damaged pending completion metadata cannot cancel an already accepted
repair; the later exit still refuses the damaged result. Proposal-free
continuation admits only exact accepted replacements and refreshes raw node
status without publishing a full-component result.

Current and historical owner readers validate the producing identity, creator,
and owning session's declared component. Restart predecessor classification
uses that same historical reader. Preparation compares creator identity in its
final writer decision and restores staged files when that observation changes.

## Verification history

All MWF execution uses immutable Test Area copies. Records are under
`C:/Business/product/testing_ground/issue-45`. The names below begin with
`sample-calculations-producing-identity-`. Source records end in `-freeze.json`;
executed checks retain `-run.log`, `-run.xml`, and `-run-manifest.json`.
The unused freezes remain recorded. No check was skipped or deselected.

| Source suffix | Result and scope | Freeze SHA-256 |
| --- | --- | --- |
| `red-01` | Not run. Unused freeze, corrected before execution. | `62264876D74EF1DF36717DB78502178659FAC4EE57836FAA96C466F9CF4858BE` |
| `red-02` | 0 passed, 15 failed, 0 errors, 0 skipped; 10.83 seconds. Nine fixture failures and six missing-behavior failures. | `9F7F88FE94C1A6BDB34D9D88FFB197CDDD43E3078903C28BF909D69674DDDC50` |
| `red-03` | 2 passed, 13 failed, 0 errors, 0 skipped; 15.53 seconds. Producing identity and selected claim behavior missing. | `DDE7F799006EDBBBDFDD76DC7F191902DE6D94CF39B6F0EFBF20D8F12FF9731A` |
| `green-01` | 26 passed, 0 failed, 0 errors, 0 skipped; 44.34 seconds. Initial identity and ancestry checks. | `BB6F3642F4DC454C515A152AB99BE54D4DD6AD07D6FE4367806A4552233E03E0` |
| `integrity-red-01` | 37 passed, 9 failed, 0 errors, 0 skipped; 59.41 seconds. Current creator and historical shape damage were accepted. | `BE9788C0AE49B3FFA11382AFD430CA88E91AC006C124AF6BF81372E6763047EE` |
| `integrity-green-01` | 50 passed, 0 failed, 0 errors, 0 skipped; 77.92 seconds. Shared owner and shape validation. | `398A6DEDF01126530A45FFBABC892E42C7674A4D79DA89E398F4E2EAE879D435` |
| `restart-red-01` | Not run. Unused freeze, expanded before execution. | `31C8C2DFC4B2835C1D6375453A5DC4AE16F53D2E8EF10E014E617AA0348D6D9A` |
| `boundaries-red-01` | 4 passed, 5 failed, 0 errors, 0 skipped; 23.23 seconds. Two restart failures and three fixture failures. | `A57D0B603CE83E2EB25A61CB01A945176801D7A5EBBE6C2D7BE93C3FB97D27DE` |
| `boundaries-red-02` | 6 passed, 3 failed, 0 errors, 0 skipped; 21.72 seconds. Two restart failures and one preparation failure. | `EF33236AEE249E66CBBE9C94DBE538E5D4D35FAA6E4C75C27CD8C84C734975D8` |
| `boundaries-green-01` | 59 passed, 0 failed, 0 errors, 0 skipped; 72.18 seconds. Child restart continuation and preparation creator checks. | `091C17D4BA87C8749B31973C33669BFA70FBE54E0CF23229739E8D728FC77785` |
| `shape-red-01` | 9 passed, 1 failed, 0 errors, 0 skipped; 10.29 seconds. Canonical producing graph update was accepted. | `5CDEA63637E85741FC5D5B095ADBE10DC4069067E497098F59BCAAD3A5B0EB5C` |
| `integrated-01` | 724 passed, 8 failed, 0 errors, 0 skipped; 496.46 seconds. Six obsolete initialization fixtures, one related pipe warning, one obsolete reclaim expectation. | `4F51FD9397AB181F664253C7820C91420357F1324E918D47E62F91D551B7835E` |
| `refinement-red-01` | 215 passed, 13 failed, 0 errors, 0 skipped; 119.88 seconds. Actual task creator, retained handle, raw node status and session declaration failures. | `A46C4CBCB95C4B50F608366ED99712F8DC41FC8B93DDC2DF07E510FBF6C1159F` |
| `refinement-green-01` | 323 passed, 3 failed, 0 errors, 0 skipped; 269.60 seconds. Only three selected-parent nested component settlement cases failed. | `C55F0A90431A2854A1AE4A5C16505420CD8FBD0BE3DAF409B6513869DC2F6ED8` |
| `settlement-red-01` | 31 passed, 6 failed, 0 errors, 0 skipped; 27.60 seconds. Five behavior failures and one foreign-key damage fixture failure. | `E9D864F4609FA9790B7D539187055CAF47CC9F0E523BD47B0BF0FF63E84B030C` |
| `settlement-red-02` | 31 passed, 6 failed, 0 errors, 0 skipped; 27.04 seconds. Six intended damaged-scope, pending-proposal and predecessor-reader failures. | `E53D04BF2099A9B4836B2E0E20F1FD1510F42437EF31C12E61709A56230EBF5E` |
| `settlement-green-01` | 860 passed, 0 failed, 0 errors, 0 skipped; 692.29 seconds. All 16 complete modules passed. | `B42555900D0575EB1C13B8EFF5868EE8C1EC24EE957E8474FBB2F2E3E453A197` |

The table reports elapsed time from JUnit XML. The final integrated pytest
summary reports 860 passes in 692.49 seconds across complete modules 046, 069,
072, 079, 080, 081, 082, 084, 086, 090, 092, 094, 095, 096, 097, and 098.
The source contains 340 Python files. A final whitespace check removed one
trailing blank line in each of `storage/job_creation.py` and
`storage/session_selection.py`; the executable syntax is unchanged across all
340 files. The unused `final-01` freeze predates the second newline cleanup.
The `final-02` freeze is
`F2A35A8892A8B875110254B0CB3005567BEA8CB5FD85BE49F1AFB0CBCBDE7872`.
Additional full-module checks 040, 046, 087, 097, and 098 passed all 181
cases in 287.93 seconds on that final copy. Neither final run had failures,
errors, skips, or deselections. The two counts overlap; they are not a count
of distinct tests.

The final source audit is
`execution-producing-identity-final-source-check-revision02.json`, SHA-256
`0169FD31F200578D20E5C47BE180692DEAF4801413B5ED471BE15E3148517D06`. It verifies every frozen hash,
all Direct source content after newline normalization, all 26 overlaid files
byte for byte, the two exact newline removals, and no executable syntax change.

Final execution records:

| Record | SHA-256 |
| --- | --- |
| `sample-calculations-producing-identity-settlement-green-01-run-manifest.json` | `4832B5316E9C72066768EB94B9E8EE2C86B2F01D6400B5728489989A6E3352B8` |
| `sample-calculations-producing-identity-settlement-green-01-run.xml` | `E54893575C442EFA0E68B547B3A2373523505A86F960DDE09B919B222B22BBA0` |
| `sample-calculations-producing-identity-settlement-green-01-run.log` | `7FEB0E6BDB963A0BD81953BA5A64EE91104CFB8E0F23A7C2B9942B8990AC6B17` |
| `sample-calculations-producing-identity-final-02-run-manifest.json` | `03B0AC710C786B4BA9B61AF9CBA02E7D34F431FE3A656FE889D488E650924C1A` |
| `sample-calculations-producing-identity-final-02-run.xml` | `91CB7AE0F22BE08867D0640D6B698C21A5E52B69558F2E151F96C497074E760F` |
| `sample-calculations-producing-identity-final-02-run.log` | `7E5FA3D99B0A7119356EB054818CC610CC1707E9F8C6DD68D3EBC2D9570B106E` |

Obsolete version-4 expectations in the existing session and claim tests were
replaced with native creation, unchanged old-format refusal, writer rollback,
reopening, and concurrent native initialization controls. No case was excluded.
The current-owner recreation fixture now requires a damaged retained pointer
to refuse reclamation before explicitly repairing that fixture. A corruption
fixture drops the immutable-shape trigger before testing reader validation.
The concurrent opener fixture always closes both subprocess pipes.

The first boundaries fixtures gave B an unsupported parameter and supplied an
incomplete full-component preparation plan. Those fixture errors were corrected
and rerun. The initial predecessor-corruption fixture hit its foreign key;
the corrected case deliberately disables that guard only for damage injection.
An attempted freeze with an incomplete allowed-file list failed before copying;
later immutable freezer revisions include the new cohesive modules.

The independent final Sol reviews are recorded under the same external evidence
directory. Both returned PASS only for producing identity, selected-root claim
admission, and accepted restart continuation:

| Review | SHA-256 |
| --- | --- |
| `execution-producing-identity-spec-review.md` | `437EDC802A15F00067D10EB58DAC521C2F036C64A4D19EB52A9255605F2A2A60` |
| `execution-producing-identity-standards-review.md` | `08E0BE03559626ACA7D5516837A237253A5C51E3751C3A45934A99FF3B95ED59` |

The reviewed root-restart question required no additional change. A successful
nested full-component call retains its completion-ready result; a failed child
with an accepted restart carries its own pending proposal. Unrepaired nested
failure must not be converted into successful component completion.

## Remaining work

Automatic selected-job causal circulation is still pending. The tests explicitly
run ordinary children; only accepted restart continuation is automatic here.
A selected root that merely creates a child can leave it queued. A root that
catches a failed child without an accepted restart can still complete its
selected session. Full selected preparation, failure accounting, sampled state,
input digests, and unstable lineage remain required. Requirements SMP-038,
SMP-039, SMP-057, SMP-058, and REC-007 therefore remain incomplete.

Durable managed-file ownership is the next publication dependency. Captured
execution identity alone does not satisfy PRP-006. File and ownership atomicity,
producer cleanup across receivers, receiver guards, causes, misalignment, and
recovery are still pending. The same-producer visible-path collision policy
remains deferred. Explicit creation events and prepared ID reservations also
remain separate from their final job publication transaction.

This record earns no full-suite, cyclic-suite, stage, release, or final Astra
acceptance. The external migration guide will be written after the entire
implementation is complete. No push or release is authorized.
