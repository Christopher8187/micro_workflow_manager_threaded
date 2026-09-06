# Managed input ownership progress

This continues [Implement and verify the agreed MWF 0.6.2 workflow-management
changes](https://github.com/Christopher8187/product/issues/45) from
`bc9bd0b386342823bf43450076206c650a1c258c`. It implements durable producing
execution records and synchronous file-publication restoration. The final
source passed all 774 affected checks in 1045.53 seconds. Both independent
reviews returned bounded PASS. No whole requirement receives completion credit
from this portion.

## Implemented behavior

Task-facing text, bytes, text and JSON batches, copies, plural copies, append,
and delete use one execution-scoped storage operation. The storage operation
validates the exact active job execution, incarnation, running session,
component reservation, retained graph edge, producing shape, and alignment
generation before intent and final publication. Selected jobs do not require
a pending whole-component result. A live handle remains usable in its task's
helper thread; another active task cannot claim that handle's authority.

Each managed path records all immutable producing execution claimants.
Historical reads resolve the captured component, raw node, job, incarnation,
shape, and alignment independently of trace and the current job owner.
Same-execution rewrites retain sole ownership. A distinct execution or an
unowned predecessor makes ownership ambiguous. The reader refuses ambiguity
rather than selecting a latest claimant. This preserves the deferred
same-producer collision decision. Project-created paths remain unowned until
a managed write explicitly touches them.

The producer execution fence and receiver input lock cover target allocation,
staging, visible file changes, the SQLite decision, and restoration. Ordered
plural copies preserve existing same-basename behavior. Their ownership uses
the actual returned paths. Read-only copy sources retain their file mode. Failed restoration preserves
the exact replacement's previous mode. Existing filename case aliases use the
same ownership path, while the raw producer keeps its exact graph identity.
Deletion removes its corresponding ownership with the file operation.

A prepared receipt precedes the first visible replacement. The final writer
transaction records ownership, forwarding events, and the committed receipt.
Writer callbacks perform only SQLite work. Ownership readers take the receiver
lock and reject unfinished publication receipts before returning a result.
They therefore cannot return an old owner for a replacement awaiting its
metadata decision.

A synchronous file, event, or transaction failure restores every changed target
from its prior bytes and leaves prior ownership and trace intact. Repeated
targets restore once, accepting any replacement staged by that batch. A failed
restoration, receipt read, or abort decision preserves the original exception,
adds diagnostic notes compatible with Python 3.10, and retains recovery
material. Post-commit notification failures preserve the committed result.
The writer request is drained before an interrupted waiter can compensate.

## Verification history

All execution uses immutable Test Area copies. Records live under
`C:/Business/product/testing_ground/issue-45`. Names below begin with
`sample-calculations-input-ownership-`. Every executed source has a
`-freeze.json`, `-run-manifest.json`, `-run.xml`, and `-run.log`. Times below
come from JUnit XML. No check in these runs was skipped or deselected; all
reported runs have zero collection errors.

| Source suffix | Result | Freeze SHA-256 |
| --- | --- | --- |
| `red-01` | 0 passed, 16 failed; 12.19 seconds | `84073109F13CF084491DA78337C1BECFFD2B6D99E122BDB70DE34B5F68EFDC2F` |
| `red-02` | 0 passed, 20 failed; 21.88 seconds | `B78708DDC4FD04A57F1F8EFB01AFDF31E0AD2E7C9FD074A1F41EEE359C31274D` |
| `green-01` | 62 passed, 46 failed; 72.09 seconds | `6D30036E2D8D1C21BC3454BBD9DC977DECB8F0FE41DD810206F1BFD8DBA41ECB` |
| `green-02` | 108 passed, 0 failed; 79.66 seconds | `938B2D6179DB5AB3C870B8A385757849FA5634B45D2EE4FFACA600B85C005140` |
| `decisions-red-01` | 25 passed, 1 failed; 19.64 seconds | `F92183BC2E27D334CA685D4313F60DB632CE1D97150AF6203ED7BC57EDB1E38B` |
| `decisions-red-02` | 25 passed, 6 failed; 21.49 seconds | `FA2735AA2DF1B8B59F2E9B98C50E50A23A02B2CFD086AA78D520A891B3F5ADD4` |
| `decisions-green-01` | 119 passed, 0 failed; 98.42 seconds | `28A10C0432FA7EA6275BF9ADA30C5276ED7D09646729B6068B9CC1446FB25773` |
| `preservation-red-01` | 33 passed, 3 failed; 41.88 seconds | `9CC104E6349CA87627F92C874945B11A45DA698095210D850DB0319B1E8DCA5C` |
| `preservation-red-02` | 33 passed, 8 failed; 41.71 seconds | `52243106D0DAF0556E346378975336A088B74300533C58B1726EAE9991842C58` |
| `preservation-red-03` | 33 passed, 10 failed; 44.57 seconds | `A6E1514DCC828758337A32AEF7BB5CBAF3A478DD8D5A6A55C55163AC35E91051` |
| `preservation-green-01` | 131 passed, 0 failed; 111.49 seconds | `916FDFF9FDB7280601E218C4E67EBA76D2920C4A0FC33BAE13DA22946F718327` |
| `repeated-target-red-01` | 43 passed, 2 failed; 48.53 seconds | `AEE2C9B8DBC8A98BA5D3CA4AA4B34C69C97A108DACD09D59B570EAE7226AF7B2` |
| `integrated-01` | 691 passed, 0 failed; 522.76 seconds | `0BC99D8A43F3A1EE4F99F565DA2883AA1C144D445ABC2CABE398A2297DD1B68D` |
| `final-boundaries-red-01` | 45 passed, 2 failed; 61.62 seconds | `F6E852C4357D2D9E27CB6C9857D2498990125FE16D4037998C74C48C5A51E3FB` |
| `final-01` | 372 passed, 2 failed; 441.37 seconds | `92FBC0697D511B402C7231AF5B0BE01D3E32F33399D391607DB38A1FEC39CD74` |
| `lock-alias-red-01` | 50 passed, 4 failed; 40.71 seconds | `4246FCB95963B6CE0EA07E844D17805E47B2F6049B80B97A04850EB5DBCE8B2C` |
| `lock-red-02` | 2 passed, 4 failed; 2.95 seconds | `68E5C9A75C5A44D0C1746BC9544D7A2DEB44BE32D2343A250C83E1CFFF24FA3F` |
| `lock-alias-green-01, corrected run` | 68 passed, 0 failed; 97.41 seconds | `DEE1FBBE4D7544631EFD5162387270944B2894A2BDAC34501EFE74037C48F04E` |
| `lock-surrounding-01, same source` | 563 passed, 11 failed; 738.66 seconds | `DEE1FBBE4D7544631EFD5162387270944B2894A2BDAC34501EFE74037C48F04E` |
| `native-controls-01` | 30 passed, 0 failed; 59.08 seconds | `7E067162926BF41F6049536BF522DDB4882A31274B1683F530C2C482AAD53B8A` |
| `final-02` | 772 passed, 2 failed; 1048.39 seconds | `95DBDC270D0DECF98487032FAE84B89731DF01B99C6D6AF4481F314132110363` |
| `final-03, boundary run` | 2 passed, 0 failed; 4.33 seconds | `57AAE1DDD674EF7F50E921AE50F486621AF3E8F9367B1636E70A848D190015BD` |
| `final-03, combined run` | 774 passed, 0 failed; 1045.53 seconds | `57AAE1DDD674EF7F50E921AE50F486621AF3E8F9367B1636E70A848D190015BD` |

The initial RED failures all reached the missing ownership reader. The first
implementation run found a Windows writable-descriptor requirement for file
flushes. Subsequent RED runs demonstrated the separate event transaction,
reader visibility race, unfinished-receipt acceptance, lost original errors,
read-only copy handling, repeated copy basenames, invalid generations, and
repeated-target restoration failures. Each failure remains recorded.

The attempted first invocation reused the frozen source name as a run name.
The runner refused before starting pytest or creating a run. The corrected
invocation used the distinct `-run` name.

The latest focused complete-module result before integration is 131 passes in
111.49 seconds, covering ownership, forwarding, filesystem objects, file-entry
imports, and module-size boundaries. This overlaps earlier passing runs.
Integrated source `integrated-01` contains 344 Python files and has freeze
SHA-256 `0BC99D8A43F3A1EE4F99F565DA2883AA1C144D445ABC2CABE398A2297DD1B68D`.
Its complete session, claim, bootstrap, restart, preparation, and filesystem
selection passed 691 checks in 522.76 seconds. The final source adds read-only
mode restoration and existing filename-case normalization, both demonstrated
by failing regressions. An additional preservation case checks the raw producer
identity when its directory already has a case variant.

Source `restore-mode-red-01` was frozen but not run. The expanded
`final-boundaries-red-01` supersedes it. Final source `final-01` contains 344
Python files and has freeze SHA-256
`92FBC0697D511B402C7231AF5B0BE01D3E32F33399D391607DB38A1FEC39CD74`.
Its ten complete modules add high-fanout, main and programmatic execution,
component settlement, and component lifecycle coverage to the ownership and
filesystem selection. It passed 372 checks and failed two native API producer
checks. Both failures entered another fiber's unfinished receiver publication.
The advisory lock used a thread-local name as reentrance authority, which API
fibers shared. The correction binds reentrance to the actual greenlet, process,
canonical database path, and lock name, including across storage instances.
Owners register before their advisory row can commit and notify a contender.

The lock controls also demonstrated false reentrance through a copied context,
failed legitimate nesting across storage instances, and premature reclamation
during commit notification. Separate lock names remain independent. Captured
graphs with distinct raw node names that share filesystem storage now refuse
managed publication before staging; historical ownership readers use the same
validation. Producer and receiver case-collision checks both demonstrated RED.

The first `lock-alias-green-01-run` named a nonexistent module and ran no tests.
The corrected invocation retains a distinct `-corrected-run` record. That corrected source passed 68 checks. The surrounding run passed 563 and
failed 11 in the older API, advisory recovery, and checkpoint modules. Native
fixtures now supply exact sessions and claims, old formats refuse without
import, and monitor checks use native session fields. No test was skipped.

The 30-pass control run used explicit fresh preparation for repeated API
rounds. Review identified that the original direct repeated `run_node` failure
is a real missing integration. The explicit setup was removed and the original
regression restored. [Automatic programmatic fresh preparation](programmatic-fresh-preparation.md)
is included in this change. Its 285-case native-control run passed. The next
combined run passed 772 and failed two older partial-restart trace assertions.
Those cases now require the exact fresh-preparation event and still verify
unchanged job control, no owner or output, the accepted successor, and released
reservations. Both corrected cases passed. The same 774-case combined selection
then passed on `final-03` in 1045.53 seconds. No case was excluded.

## Final source and review

Final source `sample-calculations-input-ownership-final-03` contains 347 Python
files. Its freeze SHA-256 is
`57AAE1DDD674EF7F50E921AE50F486621AF3E8F9367B1636E70A848D190015BD`.
The 22-module combined run has zero failures, errors, or skips. It includes all
previously failed native controls and the new ownership, lock, and fresh-entry
checks. These results overlap earlier runs and are not additive coverage totals.

The independent [Standards review](../../../../testing_ground/issue-45/managed-input-ownership-standards-review.md)
and [Spec review](../../../../testing_ground/issue-45/managed-input-ownership-spec-review.md)
returned bounded PASS. [Source and staged-file verification](../../../../testing_ground/issue-45/managed-input-ownership-source-verification.json)
compares the tested source with the local commit inputs. Existing line endings
are normalized explicitly. The only later Python edit removes one empty line
at the end of `workflow/preparation.py`; its parsed syntax is unchanged. Every
other Python file matches the tested content exactly after newline normalization.
The [commit verification](../../../../testing_ground/issue-45/managed-input-ownership-commit-verification.json)
records the resulting local commit and its exact files.

## Remaining work

The receipt and backups are not a complete crash-recovery implementation.
Persisted artifact identities, directory durability, a validated recovery
consumer, and terminal-receipt retirement remain required. Receipts currently
retain their producing execution rows even after staging cleanup. Unfinished
receipts deliberately block managed ownership reads and further publication.
Raw paths and writable raw handles remain outside managed publication tracking.

Receiver misalignment, first-arrival causes, preparation removal/change causes,
complete producer-footprint cleanup, selected-job cleanup, and excluded-receiver
guards remain pending. Ambiguous paths must refuse ownership-based cleanup
until the deferred collision policy is settled. Whole selected-job lifecycle,
ordinary causal child circulation, native recovery, previews, interrupts, and
remaining command integration also remain unfinished.

This record grants no ordinary-suite, cyclic-suite, stress, package, stage,
release, or final Astra acceptance. The external migration guide will be
written from the completed implementation at the end. No push or release is
authorized.
