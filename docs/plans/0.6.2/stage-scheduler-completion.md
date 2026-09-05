# Component completion and waiting progress

This section of [Implement and verify the agreed MWF 0.6.2 workflow-management
changes](https://github.com/Christopher8187/product/issues/45) is accepted within
the boundaries below.
It repairs retained scheduler behavior exposed by the topology comparison.
It does not implement the new component lifecycle or session ownership model.

## Failure and cause

The existing waiting benchmark returned success with queued jobs in the accepted
baseline. A two-seed case left only one waiting member queued after its peer
finished. A final mutual-wait state could not explain that outcome.

Two controlled regressions distinguish the causes:

- The coordinator read queued, running, and blocking state separately. A real
  producer could publish and finish between those reads. Combining the results
  could report no work or a waiting deadlock that never existed in one snapshot.
- Even with one coherent snapshot, a finite waiting pump can be between claims.
  Both members can have queued jobs and neither have a running job, while the
  already admitted pump can still progress. The coordinator declared deadlock,
  stopped admission, and joined that pump, leaving its peer's jobs queued.

One storage query now observes all required job states together. Deadlock
detection waits until finite pumps finish. Ordinary resident pumps remain
excluded from that check because they stay alive while idle. The regression
for a real waiting deadlock with an idle resident member verifies that the
coordinator still returns without executing the blocked jobs.

The benchmark now derives completed counts from SQLite and fails if expected
rounds are missing or any non-done jobs remain. Both compared framework trees
use this same benchmark program; its durable checks run after the timed interval.

## Test-first and compatibility record

The isolated `scheduler-fix` source is based on accepted topology commit
`24f584413619d1bbe94da2264032600b9b401105`. It excludes unfinished previews and
the separate migration preflight. Python 3.12.14 uses declared dependencies,
the selected source on `PYTHONPATH`, disabled pytest cache, and fresh test roots.

- Three real scheduler probes reproduced the inconsistent-read failure. The
  permanent SQLite-observation regression failed before the first fix, while
  retained alternation and late-feedback cases passed. All three passed after it.
- Adjacent testing exposed an existing fault injection attached to the replaced
  query. Moving the injected I/O failure to the new observation call preserved
  its check that live workers join before component failure. Thirty-nine
  adjacent tests passed in 43.12 seconds.
- The original multi-seed workload still failed after the first fix. A targeted
  trace observed the deadlock decision while a finite B pump was running.
- The first new regression used a fenced claim hook that the programmatic
  workload does not call, and an over-specific return-list assertion. Those
  setup failures are excluded. The corrected test pauses the actual second
  RUNNING transition and fails on missing completed consumer jobs. The coherent
  snapshot and real-deadlock cases pass alongside that RED.
- Deferring deadlock while a finite pump remains active made seven focused
  scheduler and benchmark cases pass in 3.62 seconds.
- Original workload checks at two seeds/one thread, ten seeds/ten threads, and
  200 seeds/100 threads completed exact A/B counts of 6/4, 30/20, and 600/400.
  All had zero remaining jobs and returned success. These are correctness
  observations, not comparative timing results.

The benchmark's unfinished-job RED is recorded. Review added a separate case
with all existing rows done but missing expected rounds. Removing only the
expected-count condition in memory made that case fail because the benchmark
returned success. A fresh process with the unmodified source passed all eight
focused scheduler and benchmark cases in 5.44 seconds.

Final adjacent checks passed 42 tests in 43.82 seconds. The ordinary suite
passed 395 tests with one stress case deselected in 433.10 seconds. Four cyclic
cases passed separately in 6.32, 11.92, 9.06, and 4.00 seconds. The selected
Markov-chain stress case passed in 7.62 seconds.

The measured comparison passed without other test workloads. It used one
circulating seed, 50 rounds, one thread, and a 0.001-second task delay. Three
alternating baseline/candidate pairs each finished exact A50/B49 counts with
zero non-done jobs. The predeclared maximum candidate/baseline median elapsed
ratio was 1.20. The observed ratio was 0.832573. The same benchmark program ran
against each verified framework source, with durable result checks after timing.

| Pair | Baseline seconds | Candidate seconds |
| --- | ---: | ---: |
| 1 | 24.8388470 | 20.0397600 |
| 2 | 24.2505025 | 24.3308996 |
| 3 | 23.2504955 | 20.1903154 |
| Median | 24.2505025 | 20.1903154 |

The host was Windows 11 build 26200, AMD64, Intel Family 6 Model 186 Stepping 2,
with 16 logical processors. Python was 3.12.14 and SQLite was 3.53.1. The local
`scheduler-benchmark-manifest.json` records complete source hashes, dependency
versions, executable, parameters, ordering, and thresholds. The improvement
applies to this workload; it does not establish a general speedup.

An additional source copy combines this section with the accepted migration
preflight at `8bde626ff083840bf002cfe685d79bd8d9aa674d`. Focused checks of that
combination passed 38 tests in 25.08 seconds and another 20 in 15.57 seconds.
They cover initialization, SQLite, restart, waiting, terminal behavior,
benchmark failures, and module boundaries. All six scheduler source and test
files match the independently reviewed copy exactly. Migration source differs
from the accepted Git content only in line-ending representation.

Two independent Sol xhigh reviews passed their assigned areas. The
[safety review](scheduler-safety-review.md) covers coherent observation,
finite and resident pumps, failure cleanup, and the benchmark completion gate.
The [compatibility review](scheduler-compatibility-review.md) covers retained
behavior, specification, test sensitivity, documentation, and the combined
source checks. These reviews accept this repair only; the new component
lifecycle, waiting display, session ownership, and full release remain pending.

Logs and workload manifests are under `testing_ground/issue-45` in the Parent
Repo. Temporary source and test trees are under
`C:/Business/product/test_area/mwf-062-issue45-20260904/`.
