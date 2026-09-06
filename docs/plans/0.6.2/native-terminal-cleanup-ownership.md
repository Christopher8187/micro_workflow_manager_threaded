# Native ownership during terminal cleanup

Status: bounded [correction review](../../../../testing_ground/issue-45/native-terminal-owner-validation-correction-review.md) passed. This follows
[output publication cleanup](native-output-publication-cleanup.md).

The first output filter passed 34 focused checks in 52.31 seconds. Its
[review](../../../../testing_ground/issue-45/native-output-damaged-owner-source-review.md)
accepted missing-owner refusal but identified changes after the scheduler read.
The broader run passed 306 checks in 388.20 seconds and failed one anonymous
claim setup in test 045. Native session setup preserved those assertions,
passed in 0.91 seconds, and received
[independent acceptance](../../../../testing_ground/issue-45/native-output-idempotence-control-review.md).

The new public component test saves output, fails terminal publication, then
changes the owner incarnation before grouped terminal submission. The first
injection attempted deletion and hit a foreign-key restriction, so it gives no
behavioral evidence. The corrected RED produced one failure and two controls
passing in 2.77 seconds. Invalid terminalization replaced the original error.

Cleanup now freshly validates running candidates and carries an immutable
session, component, and incarnation expectation into both terminal paths.
The mutation writer checks ownership, the running session, selected component,
and reservation inside the terminal transaction. These expectations do not
enter job output or event data. Unscoped recovery remains separately unfinished.

The corrected freeze is
`3F6E075AB9CC06B84C2334EE7E3DF96948B799ECF6A278638B94B325FE74E9ED`.
It passed 35 selected saved-output, restart-order, abandoned-claim, and two-lane
checks in 50.04 seconds. The global selection excluded test 057. A later
production-identical freeze added reservation loss after capture and passed
all seven saved-output, test 045, and native test 057 controls in 5.84 seconds.
The broader freeze
`438A8E5501A1979A94FF69591C2DE92586A34DF3726E5E195D7EFCCFC7F5E22D`
passed 310 checks in 385.29 seconds. A [complementary review](../../../../testing_ground/issue-45/native-terminal-owner-complementary-review.md)
found duplicate or conflicting terminal events when one writer group contains
two updates for the same job. The [correction](native-grouped-terminal-results.md)
passed its bounded review. Unscoped recovery
and other native damaged-state handling remain unfinished.
