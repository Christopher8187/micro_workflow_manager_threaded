# Managed input publication progress

This record continues [Implement and verify the agreed MWF 0.6.2
workflow-management changes](https://github.com/Christopher8187/product/issues/45)
from commit `c5fbee0fa0bff9e4a96fa0061bcedfe45261b1d7`. It records a bounded
implementation increment. It does not accept the full publication, preparation,
misalignment, ordinary-command, or release requirements.

## Implemented path behavior

Forwarded text, bytes, batches, copies, append, deletion, and filesystem entries
use the actual producing raw node below the receiver's input directory.
Deletion preserves unqualified project input and other producers' files.
Receiving listings reject recursion and `**`; bulk input JSON reads default to
fixed depth, while output recursion remains available. Static links and Windows
junctions along a managed producer path refuse before publication or deletion.
The check is not protection against an actor concurrently replacing directories
outside the managed APIs.

JSON batches now bind the filesystem base and template values through the same
path handling used by individual entries. The receiver tests read the exact
producer-qualified path. Four adjacent test workflows were adapted from their
old unqualified input spelling; none was removed or skipped.

File writers now reject empty normalized names, `.` and `./` before modifying
any batch member. Directory lookup remains valid with no filename. Copy
operations refuse an explicitly empty destination; omission uses the source
basename. Cyclic producers and stale-context controls pass.

## Verification history

Every run used an immutable Test Area copy. The complete source maps and run
records live under `C:/Business/product/testing_ground/issue-45`. Names below
have the prefix `sample-calculations-managed-input-`; freeze records end in
`-freeze.json`, and run records use the corresponding `-run` name.

| Source suffix | Result | Freeze SHA-256 |
| --- | --- | --- |
| `prefix-red-01` | 8 failed, missing producer prefix. | `BE3CC887989D1158BE9550102638B321ED93AB2F3974BD4C5A5A6B98BA859DBF` |
| `prefix-green-01` | 8 passed in 10.54 seconds. | `45E34C12E3131B2962268194D6C198D21A5D658EA4034F1542E230EBF8B19DE5` |
| `delete-red-01` | 8 passed, 2 failed, deletion selected the unqualified file. | `AAAA66934704FE7EA031F4453E0EDE07EB35CDE76BDD87F0C9CFB2183BE16CF8` |
| `delete-green-01` | 10 passed in 16.65 seconds. | `B10046662DA7FA4724ACA6E7E7AD03B55EA1DF59EB16B5C4F53DAB11C0A243FD` |
| `depth-red-02` | 10 passed, 13 failed, recursive discovery still allowed. | `21DB3769A5752E721A8DFF7C6F8F00A2496950B0755FF784B8387C94DCDF512B` |
| `depth-green-01` | 62 passed, 4 adjacent old-path fixture failures. All 23 new cases passed. | `912ECFE2C6175772E4C518903140C4F0FCC9F8C04338D4CCD9C7D349574A64D0` |
| `callers-green-01` | All 66 cases passed in 93.11 seconds across six full modules. | `2F3BFF0F25F9A28F22A5DE748247561F036A2153D4E30DFA4F6D08351C62EB40` |
| `links-red-01` | 23 passed, 7 failed, actual junctions redirected every tested route. | `B14C8121FAF3E09812A7E51E09A5FDC864A20C474EACD55ED466258DD6700D1E` |
| `links-green-01` | 30 passed in 26.59 seconds. | `7489D070A0ED9EF70A13C164F0C0F569C7B2B40E9DE48CC82A5A5EC1BD41474C` |
| `batch-base-red-01` | 30 passed, 2 failed, ignored static base and unsupported template values. | `4B5D5901BC146B5AFA202D92ED11C6FCCEF56356C13FC58CF15C0A8A34FCF68C` |
| `batch-base-green-01` | 32 passed in 48.26 seconds. All 335 frozen Python hashes rechecked. | `C8B1D3DDA84826E5619B78E6E7881A0C329460D37692D21047EE31F1143851B3` |
| `invalid-red-01` | 42 passed, 8 failed, empty normalized names and partial batch writes. | `10C8CC5209831C4DF82E311A9C40319C95A59F3550CC1D37FAD5CDC0F2759A5C` |
| `invalid-red-02` | 53 passed, 11 failed in 275.80 seconds. JSON bases also masked empty filenames; cyclic and stale-context controls passed. | `C5937680AE5BA6530991868BCFAB882B418AFF9B7C4482975E5605DA83A61F9F` |
| `invalid-green-01` | All 64 cases passed in 108.90 seconds. | `8A43672BDFE96DE8331F75523754A6569EFCA003E5C43E24AC20A50C800097D6` |
| `copy-controls-red-01` | 73 passed, 1 failed in 139.39 seconds. Only the explicit-empty copy case failed; retained handles and all four runners passed. | `53C3E5BB53E5E8D0275B63C08A87A07ACD6EAA203705A37E6D48418B7B22F510` |
| `final-01` | All 117 cases passed in 308.69 seconds across six full modules. | `5B12F243E9FF69DBF0858AE99BE799F76FE005FC596AE869753C877B4D8C69D7` |

The unused `depth-red-01` freeze was corrected before execution because its
forwarding fixture lacked a declared edge. The final run is named
`sample-calculations-managed-input-final-integrated-01-run`. It includes all 74
managed-input cases and the complete filesystem, input-import, high-fanout,
job-trace, and CLI help/reset modules. There were no failures, errors, skips, or
deselections. This selection does not earn full-suite, separate cyclic-suite,
stage, or release acceptance.

The final source audit checked all 335 frozen Python files against their map
and Direct after newline normalization. All ten changed Python files are
byte-identical to Direct. The audit found no mismatches. Evidence hashes:

| Record | SHA-256 |
| --- | --- |
| `managed-input-paths-final-source-check.json` | `27BDB1EEF9F2E01189DFB8B2C67122AAE5CB6BDB3263518E35B046E9BB356A86` |
| Final run manifest | `E3AD21888AD928B5EFEFBA379B5DB159455C767F61B167BBFB143C6F7D1B8D1D` |
| Final run XML | `A74C6B4C34C3F78E1D3CB3215E65134CE5A4B60FA86154865779EC600E296F69` |
| Final run log | `7EEDF8326C27FFD8AA36A5132EDE3155B0AA349CCA08FD7CFDE249152E4B81AA` |

The bounded reviews are recorded separately in
`managed-input-paths-spec-review.md` and
`managed-input-paths-standards-review.md` under the same external record
directory. Their final hashes belong in the commit verification record.

## Required continuation

Durable file ownership remains unimplemented. The reviewed next step is to
capture producing shape and alignment generation on the immutable job execution
owner when the claim is accepted. A file publication can then retain its exact
execution owner independently of optional trace history. This must cover both
whole-component and selected-job execution. Selected-job execution legitimately
has no pending whole-component result.

The remaining publication work also includes atomic file/ownership changes,
complete downstream preparation footprints, excluded-receiver guards,
preparation causes and misalignment, start-input preservation, and recovery.
Receiver alignment generation is separate from the producer's captured
generation. The same-producer visible-path collision policy remains deferred.
Integrated verification is complete for this increment. No push or release is
authorized or claimed.
