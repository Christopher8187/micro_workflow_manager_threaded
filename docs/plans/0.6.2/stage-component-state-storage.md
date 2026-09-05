# Private component-state storage

Status: accepted within the private creation and reading boundary. Three
safety-review findings have sensitive failing regressions and passing
corrections. All 68 current component-state checks, all 312 adjacent checks,
and the corrected-source ordinary, separate cyclic, and selected stress checks
pass. The independent bounded acceptance review passed on the exact frozen
source. Work remains within [Implement and verify the agreed MWF 0.6.2
workflow-management changes](https://github.com/Christopher8187/product/issues/45).

The accepted private ownership foundation supplies exact component definitions
and their producing graph. This section adds one persisted lifecycle record at
that existing storage boundary. Fresh topology registration must create the
queued state and definition atomically. Reading after close and reopen must
preserve exact members, producing shape, lifecycle, stability, instability
origin, misalignment, and alignment generation.

## Implemented boundary

`register_component_topology()` creates the definition and fresh queued state
in the same writer transaction. Re-registering an existing definition preserves
its stored result. A definition whose state is missing causes refusal before
any registration write. The existing same-members/different-shape refusal
continues to preserve the producing graph and result.

`get_component_state()` reads exact members, producing shape, lifecycle,
stability, instability origin, misalignment, and alignment generation in one
joined observation. It returns `None` for an unknown definition and refuses a
missing state, missing producing shape, or invalid field combination. Unstable
lineage must name an existing interrupt session, including a finished one.
Running state may retain approved sampled lineage and remains incomplete.

SQLite checks reject impossible lifecycle/lineage combinations, non-integer
alignment values, and negative alignment generations. The lifecycle check
requires an explicitly true SQL result, so SQL null does not bypass it. There
is no generic state setter or production caller of this private reader.

## Test-first evidence

All commands use the isolated Test Area runner
`testing_ground/issue-45/run_sample_calculations_check_revision04.py` with unique
source, temporary, log, JUnit, and manifest paths. The records below are in
Parent Repo `testing_ground/issue-45`; each run name starts with
`sample-calculations-component-state-` and ends with `-run`.

| Run suffix | Result | Behavior established |
| --- | --- | --- |
| `red-01` | 1 failed | Fresh queued state could not be read before implementation. |
| `green-01` | 1 passed | Exact members and producing shape survive close and reopen. |
| `red-02` | 1 failed, 3 passed | Registration failed to refuse an existing definition with missing state. |
| `green-02` | 4 passed | Missing-state refusal, transaction rollback, and unchanged re-registration pass. |
| `red-03a` | 27 failed, 16 passed | Damaged lifecycle/lineage combinations were accepted by the reader. |
| `green-03` | 43 passed | The reader accepts approved combinations and refuses damaged rows. |
| `red-04` | 11 failed, 43 deselected | Normal SQLite writes accepted impossible cross-field combinations. |
| `green-04` | 54 passed | SQLite rejects invalid combinations and accepts approved states. |
| `preservation-01` | 1 failed, 4 passed, 126 deselected | The constructor test incorrectly expected same-process schema revalidation after external damage. |
| `preservation-02` | 5 passed, 126 deselected | Correct fresh-process refusal and shape/version-4 preservation pass. |
| `preservation-03` | 9 passed, 126 deselected | Fresh-process refusal also covers altered foreign-key and check declarations. |
| `adjacent-03` | 305 passed, 57.51 seconds | All state, ownership, claims, sessions, topology, and readiness cases pass together. |

The prepared `red-03` source never ran. Its null fixture was corrected before
the actual `red-03a` command. Approved matrix cases use normal SQLite checks
and foreign keys. Invalid reader cases disable constraints only in a disposable
seeding connection. This separates valid storage writes from refusal of already
damaged records.

The pre-correction adjacent source is Test Area
`mwf-062-issue45-20260904/sample-calculations-component-state-preservation-03`.
Its 300-file freeze record has SHA-256
`BD86C29A98BEAE7107124D3E6AEF837CF6168D427041790FCBDC08277F2E2543`.
The state-test file has SHA-256
`93C4BBBF0B74928E9375AE24A126C797DEDD8BF4095DABA5237C3E2FFF4D8105`.
The adjacent command selects tests 075, 071, 072, 069, 066, and 074. It includes
all 61 component-state cases and the existing 1,024-component memory-bound
case, extended to read every fresh state without relaxing its bound.

## Preservation and schema validation

Changed producing shape refuses reuse without modifying existing state.
Historical split and merged component identities remain separate and receive
their own fresh queued rows. No result transfers between those identities.
Ordinary version-4 state and outputs remain unchanged, and its schema gains no
component-state table.

A fresh MWF process refuses private version-5 state with a missing table or
changed required declaration. Tests compare the complete SQLite declarations
and version marker before and after refusal. No repair or migration occurs.
The checks cover a removed origin foreign key and weakened boolean,
generation, and lifecycle constraints.

The failed `preservation-01` constructor expectation remains recorded. MWF
caches successful initialization by resolved path and process ID. Constructing
another storage object in that same process does not revalidate a table
externally dropped after initialization. Independent source review confirmed
that the corrected fresh-process check matches the supported opening scenario.
No connection-cache behavior changed, and this section does not claim detection
of external schema mutation after a cached open.

## Safety findings and corrections

The independent [safety review](../../../../testing_ground/issue-45/component-state-storage-safety-review.md)
has SHA-256
`8490C420B45A89139579FDDF94585EA59C6743FCFAE6BC69F16813A886F9EEA0`.
It required two corrections before acceptance:

- A surviving canonical graph shape with a missing component definition and
  state permitted registration to recreate queued work. The missing
  history must cause refusal before mutation. The added regression failed
  before the correction and passes afterward.
- A damaged interrupt-session ID containing only whitespace qualified
  as an instability origin. The reader must apply the existing session API's
  nonblank-text rule while preserving valid nonblank origin strings exactly.

The earlier [compatibility review](../../../../testing_ground/issue-45/component-state-storage-compatibility-review.md)
passed the recorded source and completed narrower evidence at SHA-256
`42F3C0FD8D294C9B208E860392EFC371720EC62FC5173AA91763C557027A190D`.
This corrected report distinguishes the preservation JUnit duration of 2.664
seconds from the pytest log's 2.72-second summary.
That result does not resolve these subsequently identified omissions. The
300-file preservation source and its broad result receive no stage acceptance.
Its ordinary command completed with 945 passed and one marked stress case
deselected in 594.41 seconds. The native process exited zero and drained.
That run omits the two newly identified damage cases; no cyclic or stress
acceptance run is needed on this superseded source.

The following runs use the same standard prefix and suffix as the table above:

| Run suffix | Result | Interpretation |
| --- | --- | --- |
| `missing-definition-red-01` | 1 failed, 0.73 seconds | Registration recreated a lost component instead of refusing. |
| `missing-definition-green-01` | 6 passed, 56 deselected, 1.31 seconds | Refusal and existing registration/shape preservation checks pass. |
| `blank-origin-red-01` | 3 failed, 40 passed, 23 deselected, 13.40 seconds | Spaces, control whitespace, and Unicode whitespace incorrectly qualified as origins. |
| `blank-origin-green-01` | 66 passed, 19.43 seconds | All component-state checks pass with both corrections. |
| `adjacent-04` | 310 passed, 71.12 seconds | The six adjacent modules pass with those two corrections. |
| `ordinary-04` | Cancelled, partial log only | A further source finding superseded this run before completion. It earns no acceptance credit. |
| `partition-red-01` | 2 failed, 66 deselected, 0.81 seconds | Registration accepted an extra definition under the producing shape, with or without a state row. |
| `partition-green-01` | 68 passed, 20.85 seconds | Complete stored membership is required before any registration write. |
| `adjacent-05` | 312 passed, 74.16 seconds | All six adjacent modules pass on the final corrected source. |

The first registration correction found missing expected definitions but
queried only expected keys. A further safety review identified extra
definitions attached to the same producing shape. Both added regression
variants failed before the full-set correction. They preserve established
results and compare all shape, definition, and state rows after refusal.

Registration now reads every definition attached to an existing canonical
shape and requires that stored set to equal the expected component-key set.
The earlier scan still detects conflicting shapes and missing state rows.
All checks run before inserts in the existing writer transaction. Valid empty
shapes and concurrent idempotent registration remain supported.

Root cancelled the superseded `ordinary-04` test process after this finding.
The exact isolated pytest process and its child were identified before
termination. The runner exited with status 1 and drained. Its partial log has
no completed pytest summary or JUnit result. This was an intentional
cancellation, not a product failure or a passing check.

The reader now requires a nonblank origin using the existing session API's
stripped-nonempty rule. It returns the original valid string unchanged, as the
` int-spaced ` case verifies. SQLite continues to enforce origin existence;
the reader enforces semantic validity, including interrupt kind and nonblank
text. No duplicate Unicode whitespace rule was added to SQLite.

The corrected 300-file source is Test Area
`mwf-062-issue45-20260904/sample-calculations-component-state-partition-green-01`.
Its freeze SHA-256 is
`7174FD4976ADBB3820E638AB6FA98F3F0832DA5E0E74F5921C18DE824CBFFA90`.
The updated state test has SHA-256
`1842158CD22A6E510EB7B86C7E53EA756B9D702E8D6B53F5FEA19AAA8EB1508C`.
The [safety correction review](../../../../testing_ground/issue-45/component-state-storage-safety-correction-review.md)
has SHA-256 `5643B6CF5E49014F8E80907685ACAE68BC6893D78306005EF7B134FDF1B3C5E9`.
It accepts the source, focused checks, and adjacent checks within this private
boundary. The separate [partition correction review](../../../../testing_ground/issue-45/component-state-partition-correction-review.md)
has SHA-256 `BB8920442CDC49FB41435DF76262FF6CA3670FD3FA259CB2530A416D0C14B340`.
Root read both full reports and inspected the corrected query and regression.
Neither reviewer found a remaining source blocker.

## Final corrected-source verification

Every final run uses the 300-file `partition-green-01` freeze named above.
The ordinary run excludes `tests/test_autostart_cycles.py`. Each cyclic case
runs in its own fresh process. The stress command explicitly selects
`-m stress tests/test_markov_chain_stress.py`.

| Run suffix | Pytest result | Run-manifest SHA-256 |
| --- | --- | --- |
| `ordinary-05` | 952 passed, 1 deselected, 740.21 seconds | `402C1BB7A6B94E31AEE2914E139173BD358123845A2CFCA458F15D326AE65FD2` |
| `cycle-05-1` | 1 passed, 4.68 seconds | `2D46437B19375C4D0438CDF182FE2E205DFCE1CF8A54FF1ACA726304CDEDC22A` |
| `cycle-05-2` | 1 passed, 25.23 seconds | `16DDCFD586333E6DBB30909AC2488DF3B15847BAD8A23F5B1B70FD67E2ABFBBE` |
| `cycle-05-3` | 1 passed, 14.97 seconds | `906D56134868F784CA0E6E786E94B63E6A2D801C608BA89B359A38D73B3DEDF0` |
| `cycle-05-4` | 1 passed, 8.49 seconds | `ECF1392C2D10CCF888998DCA42455BEDFE7F04C245859E7FC6839B1396B2DC96` |
| `stress-05` | 1 passed, 8.55 seconds | `955ACC75D9C17DEE0714483AC7C8B0ED3306A5446F6166A04F706C2D46399F3C` |

The cycle cases cover self and mutual autostart, the 100-job diamond, the
100-job ring, and stochastic circulation, in that order. Every command exited
zero, and every returned native session drained before the next workload.
Every JUnit report has zero failures, errors, and skips. The ordinary command's
one deselected case is the marked stress test that passed separately.

Lifecycle transitions are outside this frozen selection and remain
unimplemented and unaccepted. The independent
[bounded acceptance review](../../../../testing_ground/issue-45/component-state-storage-acceptance-review.md),
SHA-256 `D2116CAF62CF417934ADCBC16A567849B17D796B11E7257820C9AE371470550E`,
reconciles all 300 frozen source hashes, the six current Direct MWF files, all
eight final source maps, and the JUnit results. The separate read-only
`component-state-storage-final-source-verification.json` record corroborates
those comparisons at SHA-256
`0418B719DD533F2CFE3C7F2E57A6C1C90017E33DE39F74B5E8CDC27A86403ABA`.

The [source-only preparation](../../../../testing_ground/issue-45/component-state-persistence-preparation.md)
has SHA-256 `14EEA2B991297302E18D2FA69650526C9A751346B94A30A46F18C355D6AC3F80`.
It specifies atomicity, damage-refusal, shape-preservation, and version-4
checks now covered by the focused and adjacent results. Approved AQ5 permits running state to retain established
sampled lineage; later transitions must copy that stored lineage themselves.
A structurally valid row alone does not establish a successful result or
authorize a transition.

This section does not activate public version-5 creation, migrate existing
projects, infer component results from raw-node state, or change scheduler,
command, preview, monitor, or recovery callers. No requirement is complete.
