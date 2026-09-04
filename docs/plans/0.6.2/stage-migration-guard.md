# Live legacy migration guard

This accepted preflight section of [Implement and verify the agreed MWF 0.6.2
workflow-management changes](https://github.com/Christopher8187/product/issues/45)
implements the applied-migration and older-layout portions of 44-SES-034.
It does not accept session import, general component conversion, or future
SQLite upgrades during ordinary startup.

The worked source is Q112 and Christopher's explicit agreement in the complete
local task **Review GitHub issue #44**. The final resolution retains the rule
that a live legacy run must finish or become stale before migration. Root read
the full discussion, including original messages omitted by two paginated tool
turns, before starting this section. Test boundaries are the approved CLI,
managed filesystem, and migration/liveness invariants.

## Behavior and boundaries

Applied `mwf migrate` now enters its own liveness check before ordinary CLI
bootstrap. The shared check reads both supported legacy run locations directly,
using the existing host, PID, process-instance, and heartbeat rules. It opens
no storage and changes no project file. Direct applied migration calls use the
same path. Automatic conversion of old layout or legacy locks checks liveness
before its first write.
Initialization checks before extracting a deployment archive. The helper refuses
unreadable JSON and non-object run records before layout conversion.

Finished or recycled process records still permit migration. A live matching
process or a fresh other-host heartbeat refuses it. The refusal identifies the
run and advises waiting for completion or staleness. Existing migration
preserves payloads and rejects newer schemas as before.

This is a partial foundation. Current session state is still JSON. The SQLite
registry, safe import, ordinary-startup schema conversion, recovery ordering,
and exact job ownership remain unfinished. The guard does not decide AQ1
through AQ4, and does not accept SQLite dry-run behavior.
The check does not exclude an older process starting immediately afterward.
The safety reviewer reproduced that race. Concurrent admission and validated
session import remain required; this section cannot satisfy the whole live
legacy migration requirement. Existing layout conversion also discards the old
root run file when both locations exist, so future import must inspect both
before invoking that conversion.

## Test-first record

The selected source is based on accepted topology commit
`24f584413619d1bbe94da2264032600b9b401105`; the later
`696c61bfe383f66ed80be263953ae595475acc5c` adds architectural-question records
only. The migration-only source copy excludes unfinished preview changes.

- Before implementation, eight existing migration and process-liveness checks
  passed in 5.00 seconds.
- `migration-red-01.log` shows applied migration returned success, created
  SQLite, and rewrote the live run's metadata. The first guard made that case
  pass in `migration-green-01.log`.
- The automatic-layout case initially lacked graph folders; that setup failure
  is excluded. After completing the fixture, `migration-red-02c.log` directly
  shows changed layout, moved run state, removed locks, and new database files
  before the old competing-run refusal.
- The layout guard made all five then-current cases and eight retained checks
  pass in `migration-green-02.log`.
- Nine initial migration cases plus adjacent SQLite, process, framework CLI, and
  deployment checks passed: 74 tests in 30.29 seconds.
- Review found that legacy lock fixtures masked the unconditional applied
  migration check. Two added CLI/direct cases omit all legacy layout artifacts.
  Removing only that check in the isolated source produced two expected
  failures, including actual SQLite and JSON changes. Restoring it made all
  eleven migration cases pass in 0.92 seconds. No runtime correction was needed.
- The ordinary suite passed 399 tests with one stress case deselected in
  528.51 seconds. The two review-added cases then passed in the complete
  eleven-case focused run; they did not change runtime source.
- Four cyclic tests passed in separate processes in 3.44, 9.26, 6.41, and
  3.20 seconds.

- Safety review exposed deployment extraction before the initialization guard
  and acceptance of a non-object run record. Three added cases failed with
  actual filesystem changes or an unexpected successful command before the
  fixes. `migration-safety-red-01.log` records those failures.
- Checking initialization before extraction and rejecting non-object run state
  made all fourteen migration cases and seven deployment cases pass in 2.63
  seconds, recorded in `migration-safety-green-01.log`.
- The next ordinary run passed 402 tests but failed one retained restart test
  in 454.81 seconds. The replacement generation hit its own 0.4-second
  checkpoint timeout. It was not a migration refusal or an old-generation
  timeout. Three sequential repeats passed on both baseline and candidate;
  the precise transient cause remains unconfirmed. No test or runtime change
  was made to suppress it.
- The complete ordinary suite then passed 403 tests with one stress case
  deselected in 439.62 seconds, with test workloads run sequentially.
- All four cyclic cases passed again in fresh processes in 5.07, 15.15,
  9.03, and 5.82 seconds.

Both independent Sol xhigh reviews passed the corrected preflight:
[migration safety](migration-safety-review.md) and
[compatibility, test sensitivity, and documentation](migration-compatibility-review.md).
The section is accepted only within the partial boundary above. No scheduler
loop, job admission, or persistence hot path changes, so an existing performance
workload has not been selected for this guard. Stress testing is not selected
for the same reason.

Logs live under `C:/Business/product/testing_ground/issue-45/`. The isolated
source is `C:/Business/product/test_area/mwf-062-issue45-20260904/migration-guard`.
Native Python 3.12.14 uses the declared test dependencies, exact-source
`PYTHONPATH`, disabled pytest cache, and a unique `--basetemp` per process.
