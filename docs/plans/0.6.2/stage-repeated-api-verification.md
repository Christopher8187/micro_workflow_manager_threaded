# Repeated API verification

Status: **accepted within the repeated-API functional verification boundary**.
Focused checks, the reusable measurement, and the final 298-file combined
ordinary, cyclic, and selected-stress checks passed within their recorded
source boundaries. This test and benchmark section adds no runtime, sampling,
release, or whole-issue acceptance.

This retained-behavior section belongs to
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
It changes tests and a benchmark, with no runtime or sampling requirement credit.

## Existing failure and measurement

The earlier ordinary test timed three 96-job rounds and compared the largest
later duration with the first duration times three plus one second. Its
1.3956, 14.1946, and 6.9109-second failure did not establish progressive slowdown.
It also counted connections only after a defensive prune.

The independently reviewed Parent measurement uses three fresh processes, each
with one excluded warmup and four measured rounds on the same project. It
retains the workload and allowance while comparing early and late medians.
All fifteen correctness checks passed. Early and late medians were 1.7537655
and 1.9061631 seconds. All per-project comparisons passed. The exact records
and hashes are in [retained deadlines](stage-retained-deadlines.md).

## Durable checks

The ordinary test now requires zero abandoned worker connections before its
durable reads, zero mutation backlog, exact cumulative completion, SQLite
integrity, and exact output files and result records. It clears an inherited
startup strategy to exercise the normal two-worker path and rereads all 288
outputs after reuse. Cleanup drains mutations, waits for writer retirement,
and closes the caller's connection.

The first six-case module check passed in 36.09 seconds. Disabling sharded
worker cleanup in a separate source copy failed with two abandoned connections
in 12.21 seconds. The final environment-controlled check passed all 24 repeated
API and benchmark cases in 50.68 seconds. With an inherited `single` startup
setting, the corrected test still detected the two abandoned workers in
11.08 seconds. Independent test review passed at SHA-256
`E245D614C5AACE038E70B8D33D7C5253D4BE7420F1F6428B4FE97523B7801B81`.

## Reusable benchmark

[`benchmark_repeated_api_rounds.py`](../../../benchmarks/benchmark_repeated_api_rounds.py)
preserves the reviewed measurement and records source, environment, raw results,
and exact correctness. Its summary binds the plan and samples by SHA-256.
It requires source-state metadata and unoptimized Python.

Test-first checks rejected clear slowdown and then exercised the command's
nonzero result. Review-driven regressions exposed missing sample binding,
optional source state, and optimization bypass. After correction, all eighteen
benchmark checks passed in 11.09 seconds. Removing only source validation made
both wrong-source controls fail. Initial fixture errors remain excluded.

The reusable source-07 measurement passed all fifteen correctness checks. Its
early median was 1.48304335 seconds, late median 1.56411965 seconds, and fixed
allowance 5.44913005 seconds. All three per-project comparisons passed.
The Test Area `repeated-api-benchmark-07/result.json` has SHA-256
`FBE33A2A6072961E5E48A58B42B662874D971B8C49F0B67DDC650CD15D3687A5`.

A later source inspection found that importing `tomllib` excluded supported
Python 3.10 installations. A fresh-process regression denied that import and
failed as expected in 0.75 seconds. The version reader now reads the selected
package's literal `__version__` through `ast`, without importing that package.
All nineteen benchmark checks passed in 13.04 seconds. This simulates the
missing library on Python 3.12; it is not a Python 3.10 interpreter run.

Parent reviews are `reusable-api-benchmark-review.md` and
`repeated-api-test-review.md`. The completed benchmark review has SHA-256
`2AB98ACD4C5E6650C17494DC156E6B89BCA969EF8DFFAD6D3F5F5ADA789CC58E`.
It independently checked all raw results and the source-08 freeze. The five
measured, validation, and controller functions have byte-identical function
source before and after the version-reader correction. All 132 runtime files
are unchanged. The measurement remains an exact source-07 result carried
forward across that metadata-only correction.

The final source-08 ordinary run passed 660 tests in 440.41 seconds, with one
stress case deselected. The separately selected stress case passed in
7.04 seconds. Both manifests match all 204 mapped package, test, and benchmark
Python files. Four separate
cyclic checks passed on source 07 in 3.81, 6.98, 6.75, and 3.10 seconds. Their
runtime and cyclic-test bytes are unchanged in source 08. At that point final
stage review remained outstanding. This section introduces no runtime behavior
and receives no sampling or release acceptance.

The final deliberately broken worker-cleanup control produced its expected
pytest failure at two abandoned connections. Its outer PowerShell cleanup
wrapper incorrectly returned zero after restoring the environment; the saved
log and JUnit retain the failure. This is a sensitivity failure, not a passing
test. Later launch wrappers preserve and return the child process status.

## Combined functional acceptance

`sample-calculations-combined-functionality-01` has 298 frozen Python files;
its freeze SHA-256 is
`F688AC7E0B395AAC607D17B411145C543E63852D7E5ADD6DE6198E12E30412FF`.
The ordinary suite passed 884 cases with one marked stress case deselected in
581.88 seconds. It includes all six SQLite contention and repeated-use cases;
the three-round 288-job reuse case passed in 35.157 seconds with its exact
output, result, completion, mutation-drain, connection-cleanup, and integrity
checks. Four fresh-process cyclic cases and the separately selected stress case
also passed. All six manifests match the same 298 source hashes, and all native
child processes exited zero and drained.

The independent
[combined functionality acceptance review](../../../../testing_ground/issue-45/combined-functionality-acceptance-review.md)
has SHA-256
`1BD24E4345935161A89D9BCAB55974CB08E317F6B815E55D397632FF56367C2F`.
It accepts this functional verification boundary. It creates no new timing
gate and does not change any historical measurement outcome. The newer
300-file component-state work is excluded, as are sampling integration,
release, the whole issue, and final Astra review.
