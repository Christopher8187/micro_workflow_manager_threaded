# Aggregate API-limit deprecation

Status: **accepted within the aggregate API-limit deprecation boundary**.
Source, focused, adjacent, public wording, and final 298-file combined
functional checks passed review. Project-wide sharing across several active
sessions remains pending under 44-SES-056.

This section implements the narrow deprecation requirement from
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
The base commit is `4ea961b8a894df2adfaba5fbf747a2962d4662de`.

## Behavior

`mwf threads --api-total VALUE` keeps its existing parser and set/reset behavior.
Help labels the option deprecated. Each successfully parsed use prints one
deprecation warning to standard error before project, combination, or value
checks. This includes an invalid value. Help and parse-level errors leave
argument parsing before the custom warning. Ordinary per-node use stays silent.

The change adds no session-specific form, removal date, or replacement advice.
Current storage, admission, run binding, allocation, and clearing are unchanged.
The project-wide cap shared by all active execution sessions remains unfinished
under 44-SES-056 and receives no credit from this CLI change.

## Before-and-after checks

Existing preservation cases covered pending budget storage, proportional
allocation, live-node redistribution, a budget smaller than the live-node count,
run scoping, reset deletion, and aggregate-budget help before implementation.

| Record | Observed result |
| --- | --- |
| `api-total-help-red-01` | One expected failure on the missing deprecated help label, 1.00 s. |
| `api-total-help-green-01` | One pass, including unchanged aggregate meaning and silent help, 0.85 s. |
| `api-total-warning-red-01` | Two expected missing-warning failures; retained storage and refusal behavior reached their expected paths, 1.87 s. |
| `api-total-warning-green-01` | Two passes, ten deselected, 3.55 s. |
| `api-total-adjacent-01` | 57 passes across runtime overrides, framework behavior, and cooperative API scaling, 41.48 s. |

The warning checks retain successful set/reset status, standard output, pending
run binding, exact stored value, and reset deletion. A rejected value must leave
existing override bytes unchanged and print its warning before the diagnostic.
Per-node use produces no deprecation warning.

The adjacent manifest has SHA-256
`D3278359C230192F8B1B2A535FD9E7B2E2D638F512E3581F40B22BA5F6098B07`.
The isolated runner verifies 290 baseline Python files before copying, overlays
only the two CLI modules and two test modules, and records the resulting file
hashes, command, environment, log, and JUnit. Its baseline contains the separately
reviewed initial-deadline corrections; this deprecation section does not assign
their acceptance.

## Review and remaining gates

An independent GPT-5.6 Sol reviewer at xhigh reasoning accepted the source and
focused evidence. Its Parent Repo report is `api-total-deprecation-review.md`,
SHA-256 `685E746F001A99A83216A9D86D83E11C780775283BB4E940FAB422B3F4EAA87C`.
The reviewer inspected dispatch, refusal order, parsing, persisted state,
allocation, run scoping, and test sensitivity. It found no source defect or
unresolved architectural choice.

README, operations, release-history development notes, and the test-module
description now explain the deprecation. The independent review revision
`api-total-deprecation-review-02.md` accepts their wording and the 57-case
adjacent evidence, SHA-256
`318E61C3C7D329A9C589503ABB277C01EB995AE786502ACBC9676D86D35163E7`.
At that point final ordinary and fresh-process cyclic checks remained pending.
They are completed below. No performance comparison is selected for the
conditional CLI notice.

44-SES-054, 44-SES-055, and 44-SES-057 receive bounded completion credit.
44-SES-056 receives no credit because supported storage remains scoped to one
active or next run rather than several active sessions. 44-SES-058 records the
retained policy. 44-REC-037 and 44-DOC-021 remain partial because they include
other work.

## Combined functional acceptance

`sample-calculations-combined-functionality-01` has 298 frozen Python files;
its freeze SHA-256 is
`F688AC7E0B395AAC607D17B411145C543E63852D7E5ADD6DE6198E12E30412FF`.
The ordinary suite passed 884 cases with one marked stress case deselected in
581.88 seconds. It includes all twelve runtime-thread override cases and all
forty-one framework-improvement cases. Four fresh-process cyclic cases passed
in 3.24, 7.33, 7.60, and 3.22 seconds, and the separately selected stress case
passed in 6.84 seconds. All six manifests contain the same 298 source hashes;
all JUnit files report zero failures, errors, and skips, and every native child
process exited zero and drained.

The independent
[combined functionality acceptance review](../../../../testing_ground/issue-45/combined-functionality-acceptance-review.md),
SHA-256
`1BD24E4345935161A89D9BCAB55974CB08E317F6B815E55D397632FF56367C2F`,
confirmed that the command body and run-scoped behavior remain unchanged. Help
labels the option deprecated without warning, and each parsed use warns once
before project, combination, and value checks. No replacement, removal date,
session-specific form, or project-wide multi-session implementation is added.

The newer 300-file component-state work is excluded. This acceptance does not
cover 44-SES-056, other session integration, release, the whole issue, or final
Astra review.
