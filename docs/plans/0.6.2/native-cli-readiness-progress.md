# Native CLI readiness progress

Work continues from `0a80688fbfcc3743b9d6121b511903a33050dfbe` toward
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
That commit was an intermediate result. The remaining applicable requirements
still need implementation and verification under the approved single project
model. Christopher authorized coherent local commits after correctness checks
pass. This record grants no release or whole-stage completion.

## Current behavior

Ordinary `run` and `runfrom` preflight their named start's exact direct parents
using native component lifecycle and compatible lineage. Missing parents
refuse before execution registration or preparation. The CLI uses native
selected-component results for completion and blocked outcomes. Empty full
components execute and settle. Stop boundaries consult native lifecycle.
The outer scheduler no longer rewrites an unselected parent's raw status.

The latest reader change calculates preflight from one ordered database
snapshot. Optional absent definitions remain `None`; damaged records and
wrong producing shapes still raise. Strict callers retain missing-state
refusal. Writer-side admission continues to compare parent observations.

## Verification history

All runs use immutable copies in Test Area. Run names below share the prefix
`sample-calculations-native-cli-` and suffix `-run`. Each has a log, XML result,
and manifest in `testing_ground/issue-45`. Every source has a freeze record
containing its complete Python file map.

| Run name between prefix and suffix | Result | Meaning |
| --- | --- | --- |
| `parent-readiness-controls-01` | 4 passed, 4 deselected | Raw and native state agreement controls. |
| `parent-readiness-red-01` | 4 failed, 4 deselected | Initial absent-parent and conflicting-state cases. |
| `parent-readiness-red-02` | 4 passed, 4 failed | Explicit native queued/done state matrix. |
| `parent-readiness-green-01` | 6 passed, 2 failed | Global finalization still changed unselected raw status. |
| `parent-readiness-green-02` | 8 passed | Parent state, ownership, events, and files preserved. |
| `empty-components-red-01` | 2 passed, 6 failed, 8 deselected | Empty components were bypassed or reported incomplete. |
| `empty-components-green-01` | 16 passed | Native empty-component execution and CLI postflight. |
| `empty-boundaries-red-01` | 4 passed, 16 deselected | Preservation controls, despite the source name. |
| `stop-readiness-red-01` | 4 passed, 2 failed, 16 deselected | Raw completion stopped before the native boundary ran. |
| `stop-readiness-green-01` | 22 passed in 19.77 seconds | All readiness and stop-boundary cases pass. |
| `parent-snapshot-red-01` | 12 passed, 11 failed | Optional missing observations are not yet supported. |
| `parent-snapshot-green-01` | 45 passed in 20.76 seconds | Strict and optional snapshots, concurrent writes, damage refusal, and all CLI cases. |
| `distant-merge-controls-01` | 26 passed in 28.74 seconds | Full CLI module with both unselected-parent merge directions. |
| `fresh-merge-red-01` | 1 failed | Repeated B branch cannot start after cleanup leaves native components done. |

The 22-case passing source freeze is
`10F715C35A0AA0864025D097F3A9870F881495790BBF71828EFAFF378242D68A`.
The later snapshot candidate freeze is
`3687DA08FE58D31041DA5B57086B6F2A194105BEEDB1DCEA60CDB81B1295281F`.
Its combined observation and CLI verification passed. Earlier failures remain
in the history and retained test cases.

## Fresh preparation correction

The adapted producer-preservation case now expects the first branch to leave
the merge blocked. The second branch completes it. Repeating that second
branch must retain the first producer's original job, events, owner, and output.
The native state transition fixes the repeated-run refusal and advances each
prepared component's alignment generation.

Full runs require an exact running owner and reservation. Resets require no
live execution session. Both validate shape, lifecycle, holds, pending work,
and active jobs before cleanup. The final writer compares the full captured
state before queuing and realigning the component.

Additional runs share the prefix `sample-calculations-native-fresh-preparation-`
and suffix `-run`.

| Run name between prefix and suffix | Result | Meaning |
| --- | --- | --- |
| `red-01` | 17 failed | Missing transition methods; one filesystem fixture stopped during bootstrap. |
| `red-02` | 17 failed | Refined observation API and sensitive later-component state check. |
| `green-01` | 18 passed in 8.68 seconds | Native transition cases and repeated producer preservation. |
| `controls-01` | 32 passed, 1 failed | Active-job fixture omitted required job parameters. |
| `rollback-red-01` | 32 passed, 1 failed | Stronger assertions find requeued B jobs beneath unchanged B component state. |
| `rollback-red-02` | 32 passed, 6 failed | Invalid output and failed or suppressed database updates, with and without generated jobs. |
| `rollback-green-01` | 39 passed in 23.74 seconds | Per-component job and filesystem rollback, including generated work in later receivers. |
| `adjacent-01` | 120 passed, 6 failed | Broader scheduling, bulk cleanup, native session, and CLI checks. |
| `runner-controls-01` | 94 passed in 143 seconds | Expanded direct, threaded, API, and process controls, strict observations, and competing completions. |
| `adjacent-native-02` | 34 passed in 53 seconds | Both modules containing all six earlier adjacent failures. |

The next candidate groups job cleanup by receiving component. It stages
filesystem paths reversibly and applies job deletion/reset, owner clearing,
events, allocation, raw display, and the component transition in one writer
transaction. Queue notifications follow commit. Its 333-file freeze is
`0044162C32911F943BC7BD412025AEB95DCEC0B77ADBD276E1319D72965EFB5F`.
Its 39 focused checks passed. The earlier native transition passes did not
establish job or filesystem rollback; the expanded failures remain recorded.

The adjacent failures retained their substantive checks under the agreed
native behavior. Damaged storage now refuses without repair, monitor assertions
use native session fields, heartbeat reuse exercises the supported selected-job
entry, failed publication preserves the original error until an explicit retry,
and reservation loss is injected at the terminal boundary being tested.

The new filesystem module separately ran as
`sample-calculations-native-preparation-files-red-01-run`, with two passes and
three failures. Those failures demonstrate original-error masking on supported
Python versions, a post-commit temporary-file deletion error, and a Windows
junction leading into an unselected node. The corrected source preserves
`__notes__`, reports retained temporary files after commit while continuing,
and rejects parent links or reparse points. Its freeze is
`C264AF1387BF3FBEC470DCF388DA4CCCA3E98562334E9A064848E23A17ED3CF2`.
The combined preparation and filesystem run passed all 53 cases in 25.04
seconds.

Additional controls preserve newly replaced jobs, new jobs, and a newer orphan
creation event when they appear after planning. A restoration collision retains
the newer original path and the older staged data without overwriting either.
The expanded run `sample-calculations-native-preparation-job-race-red-01-run`
passed 57 cases and failed three: isolated active process, thread, and start-time
metadata incorrectly permitted preparation. The corrected observation and
writer refuse those partial active records.

`sample-calculations-native-preparation-job-race-green-01-run` passed all 60
preparation and filesystem cases in 26.74 seconds. Its 334-file source freeze is
`895F6A01794A4D7713A96ED5BC0473D7D0A8FC4DBAE181A8793707B9DE9205CE`.
`sample-calculations-native-preparation-integrated-01-run` passed all 168 cases
in 136.84 seconds on that exact source. It includes the complete scheduling,
bulk preparation, module-boundary, main-session, state-observation, CLI,
fresh-preparation, and filesystem modules. It has no failures, errors, skips,
or deselections. All six previously failing adjacent cases are included.

At that point all 16 changed or new Python files matched the tested copy byte
for byte, and `git diff --check` passed.

The shared lifecycle run, `sample-calculations-native-preparation-lifecycle-preservation-01-run`,
passed 399 cases and failed one in 234.06 seconds. The failing fixture assumed
that constructing a new project created schema 4. It now explicitly constructs
old SQLite state and verifies refusal preserves its raw status, output, complete
database contents, and file bytes.

`sample-calculations-native-preparation-public-reset-01-run` passed 17 cases
and failed two in 18.74 seconds. One expected obsolete preview wording. It now
checks native preview wording and unchanged database rows and node files. The
other inspected a retired per-node preparation call. Its replacement requires
a second-node database failure to roll back both nodes' jobs and files, followed
by a successful full-component retry. The original prohibition on per-job status
calls remains.

Both reviewers found that the whole-jobs-directory staging shortcut could
delete unattributed filesystem entries. The public `reset` and `resetfrom`
regressions reproduced this: `sample-calculations-native-preparation-exact-files-red-01-run`
had nine passes and two failures. Staging now removes only the exact named job
paths. The corrected run passed all 11 filesystem cases in 6.36 seconds.

The corrected fixture run, `sample-calculations-native-preparation-public-controls-02-run`,
passed 148 cases and failed one in 58.45 seconds. Its injected trigger was
correctly rejected by schema validation before preparation began. The fault now
enters through the public preparation method after storage validation. The full
CLI help and reset module then passed all 16 cases in 12.04 seconds under
`sample-calculations-native-preparation-public-controls-03-run`.

The latest source freeze is
`1CE3CB7904A627E81582EA781B78176ADCE7599EE4E9B1E6F7FAA33EE9478306`,
named `sample-calculations-native-preparation-public-controls-03`.
`native-preparation-final-source-check.json` records all 334 Python files
matching the tested source after line-ending normalization. All 19 changed or
new Python files match byte for byte. `git diff --check` passes.

The combined 17-module run,
`sample-calculations-native-preparation-final-integrated-01-run`, passed all 589
tests in 388.81 seconds with zero failures, errors, skips, or deselections. Its
manifest SHA-256 is
`82FAE21BA48F07B07B36FA97B5FB7BC41D7264CCA763EE12D4C8A7642B0C720F`.
The XML SHA-256 is
`C147FA26A87DDDB5B225804329139AB0E020671390A39FC923C021CA5558BE57`.
The source comparison record SHA-256 is
`E021709AF8450417A3F49770E8E0FF6122833D9952D7A068C50FBD6256D81368`.
Earlier failures remain recorded and their corrected cases remain included.

## Remaining verification and implementation

Both independent `gpt-5.6-sol`, `xhigh` reviews pass for this bounded progress:

- [Spec review](../../../../testing_ground/issue-45/native-preparation-integrated-spec-review.md),
  SHA-256 `CA78440D5DA893E3068D3CB7FB734A046EE401C2E963989DB9A664CA55E1C7AB`.
- [Standards review](../../../../testing_ground/issue-45/native-preparation-integrated-standards-review.md),
  SHA-256 `9BC7BE62DE0917BC619CB5C503B61E5219ED80A976EAE53767163B1E194DC7C3`.

Both bind the final source and 589-case result. Root read both reports and
resolved the filesystem targeting finding before acceptance of this local
progress. This does not complete a whole preparation stage or the broader task.

The ordinary entire suite, separate cyclic checks, and release checks were not
run for this progress portion and receive no completion credit.
Managed file publication ownership, excluded-receiver guards and misalignment
causes are still required before full preparation acceptance. Staging crash
recovery also remains pending. Resume, the remaining command forms, recovery,
sampling, interruption, previews, clipboard,
dynamic membership, and final documentation remain unfinished. The external
migration guide will be written from the completed implementation at the end.
