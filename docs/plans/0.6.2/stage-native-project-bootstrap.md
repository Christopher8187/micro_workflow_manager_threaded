# Native project creation and reopening

Status: in progress. No stage acceptance or requirement completion is claimed.

Christopher's [single-model scope change](./single-model-scope.md) cancels the
old-state execution and conversion paths. This stage combines the pending
[job-instance identity work](./stage-job-instance-identity.md) with activation
of the sole native project format. The preceding accepted commit remains
`ecfad3781bd55bdcbd1a390e7c8312874a02d613`.

The dependency change makes ordinary creation, current-version reopening,
unchanged old-project refusal, and fresh-state rollback part of the same
native acceptance boundary. Earlier old-format results remain historical.
No test cycle will be run to complete cancelled import or conversion work.

The approved test boundaries are managed filesystem effects and required
persistence, session ownership, and recovery invariants. The first behavioral
check constructs storage through the ordinary public constructor, persists
a native session and job identity, and reopens them in a separate process.
It uses disposable Test Area project data and the sequential executable lane.

The format check must happen before writable SQLite access or user-code
loading. The existing project configuration can identify the one new format;
the database validates native session declarations and stored job identities.
Core-table declarations are now validated on reopening. Creation must publish
the configuration only after its state is ready. Old projects must be refused
with a pointer to the external migration guide and left untouched.

## Focused verification

Every run uses an immutable Test Area source copy and runner revision 04. The
prefix below is `sample-calculations-native-project-`; each executable result
has the suffix `-run`. Source freezes, logs, JUnit results, and per-run source
hashes are retained in Parent Repo `testing_ground/issue-45/`.

| Run | Result | Observation |
| --- | --- | --- |
| `red-01` | 1 failed, 0.23 seconds | Ordinary creation still selected the old database model. |
| `green-01` | 1 passed, 0.70 seconds | Ordinary native creation persists sessions and job identity across a new process. |
| `refusal-red-01` | 1 failed, 1 deselected, 0.27 seconds | Rejection changed an old database's journal mode first. |
| `refusal-green-01` | 68 passed, 23.38 seconds | Pure configuration admission protects old files; native identity checks also pass. |
| `cli-red-01` | 1 failed, 2 deselected, 0.81 seconds | CLI initialization still wrote old configuration. |
| `cli-green-01` | 3 passed, 1.25 seconds | Native CLI initialization and repeated initialization preserve data. |
| `commands-red-01` | 13 failed, 3 passed, 3 deselected, 1.16 seconds | Old CLI roots reached task loading or filesystem changes. |
| `commands-green-01` | 16 failed, 3 passed, 1.48 seconds | Fifteen failures were an incomplete source-copy dependency; the lightweight thread path also still changed an old root. No new RED credit. |
| `commands-green-02` | 19 passed, 1.71 seconds | Complete source dependencies and pure layout validation protect both CLI entry paths. |
| `graph-red-01` | 1 failed, 19 deselected, 0.71 seconds | Graph setup overwrote the new project format with version 4. |
| `graph-green-01` | 20 passed, 2.60 seconds | Graph set/update retain the native format and job identity. |
| `reopen-red-01` | 1 failed, 20 deselected, 0.28 seconds | The same-process path cache admitted a damaged native trigger. |
| `reopen-green-01` | 21 passed, 1.85 seconds | Each new storage owner validates the schema. |
| `rollback-red-01` | 6 failed, 21 deselected, 0.30 seconds | Early connection/schema failures stranded owned state. |
| `rollback-green-01` | 27 passed, 2.00 seconds | Exclusive database ownership and connection cleanup allow a clean retry. |
| `missing-db-red-01` | 1 failed, 27 deselected, 0.28 seconds | SQLite recreated a database deleted immediately before connection. |
| `missing-db-green-01` | 94 passed, 19.59 seconds | Existing-state connections require the database to exist; all 66 identity checks pass alongside 28 bootstrap checks. |
| `retirement-red-01` / `retirement-green-01` | 1 failed, 28 deselected / 95 passed | The embedded migration command was still registered; registration and old filesystem import methods are now removed. |
| `core-schema-red-01` / `core-schema-green-01` | 4 failed, 29 deselected / 99 passed | Missing or changed core SQL declarations now cause refusal without repair. |
| `config-write-red-01` / `config-write-green-01` | 1 failed, 33 deselected / 34 passed | An interrupted graph configuration write now preserves the prior configuration and permits retry. |
| `journal-red-01` / `journal-green-01` | 1 failed, 34 deselected / 35 passed | Refusing a damaged native database now preserves its existing journal setting. |
| `readers-red-01` / `readers-green-01` | 6 failed, 35 deselected / 41 passed | Direct engine and preview readers now refuse old configuration before opening state. |
| `preview-missing-red-01` / `preview-missing-green-01` | 2 failed, 41 deselected / 43 passed | Preview now refuses a missing native database; old JSON fallback readers are removed. |
| `clipboard-admission-red-01` / `clipboard-admission-green-01` | 2 failed, 43 deselected / 45 passed | Direct clipboard commands now validate storage before copying or replacing files. |
| `clipboard-snapshot-red-01` / `clipboard-snapshot-green-01` | 2 failed, 45 deselected / 47 passed | A missing snapshot now leaves destination jobs and payloads intact. |
| `reconciliation-preservation-01` | 1 passed, 65 deselected | The mixed identity check was narrowed to retained native requeue and deletion behavior before removing reconstruction. |
| `reconciliation-red-01` / `reconciliation-green-01` | 1 failed, 47 deselected / 114 passed | Payload-only reconstruction is removed. Native reconciliation refuses missing job metadata before changing other rows. All 66 identity and 48 bootstrap checks pass together. |
| `import-rollback-red-01` / `import-rollback-green-01` | 1 failed, 48 deselected / 49 passed | A failed snapshot import now rolls back destination deletion and inserted rows together. |
| `node-path-red-01` / `node-path-green-01` | 1 failed, 49 deselected / 50 passed | Creation refuses an ordinary user file occupying the required node directory. |
| `graph-admission-red-01` | 2 failed, 50 deselected | Graph setup and ordinary graph loading imported user code before native schema admission. |
| `graph-admission-green-01` | 2 failed, 50 passed | Both paths now refused before import. An extra assertion still required unchanged directory timestamps after writable SQLite validation. |
| `graph-admission-green-02` | 52 passed | The assertion now checks unchanged paths and exact file identities, modification times, and bytes; it permits SQLite coordination to change directory timestamps. No additional RED credit. |
| `worker-admission-red-01` / `worker-admission-green-01` | 2 failed, 52 deselected / 54 passed | Fresh process workers now refuse old or damaged state before importing project code. |
| `marker-red-01` / `marker-green-01` | 3 failed, 54 deselected / 57 passed | Native admission now requires canonical text `5`; padded, zero-prefixed, and binary markers refuse without conversion. |
| `preview-schema-red-01` / `preview-schema-green-01` | 7 failed, 2 passed, 57 deselected / 66 passed | Preview now uses the shared native database validator. Old, damaged, and noncanonical databases refuse; a valid preview remains readable. CLI configuration returns its one validated read. |
| `owned-paths-red-01` | 4 failed, 66 deselected | Empty and native database replacements exposed missing success checks. The directory case already refused because the donor contained configuration; Windows prevented replacing an open configuration file. Those two fixture issues receive no RED credit. |
| `owned-paths-red-02` / `owned-paths-green-01` | 4 failed, 66 deselected / 70 passed | The donor directory now has no configuration, and configuration replacement occurs after close. Every replacement is refused and preserved. Creation checks path identities before SQLite setup and around configuration publication. |
| `publication-red-01` / `publication-green-01` | 2 failed, 5 passed, 70 deselected / 77 passed | Open, write, flush, sync, and close failures already permitted clean retry. Cleanup deleted unexpected WAL/SHM files; it now removes only its verified main file and preserves unexpected companions. |
| `creator-preservation-01` | 1 passed, 77 deselected, 1.07 seconds | A gated creator in one process prevents a second process from adopting unpublished state. The first completes and the result reopens successfully. |
| `native-adjacent-02` | 167 passed, 52.71 seconds | All 66 identity, 78 bootstrap, and 23 ordinary-main cases pass together on the creator-preservation source. |

The last complete bootstrap run passed 77 checks in 7.40 seconds. Its freeze
contains 309 Python files, SHA-256
`4932A5B590C8629EBF5F5411E4216C10779D075DCD4FFC0556EE51F2450BEFB8`.
The later focused creator check raises the current bootstrap count to 78.
Its freeze is
`137DE2C518D29D7D0B258D92C39C117ECB1031E42642E6AA3AD3B9C1D20E696F`.
Freezer revision 09 preserves baseline handling while explicitly permitting
the native ordinary-main source additions. Complete run records retain the
exact commands and hashes. Combined current-source verification passed as
recorded above; independent acceptance and wider runtime work remain pending.
The current source uses project configuration format 5 with JSON metadata
shape 2, and database schema 5. It has no automatic version-4 initialization.
The fresh creator claims its database file before any SQLite setup can fail
and publishes project configuration only after the schema commits.

The pending preview, planning, destructive-command, and preview-test files were
added unchanged to the source copy in `commands-green-02`. The copied CLI entry
already depended on them. Their inclusion corrects the incomplete earlier
freeze and does not accept preview safety or the unfinished preview tests.

The independent [bootstrap source preparation](../../../../testing_ground/issue-45/native-bootstrap-source-preparation.md)
was read in full. It found no new architecture question. Its graph-version,
cached-open, early-ownership, create-if-missing, core declaration, configuration
write, and direct admission findings now have focused corrections. Review of
those corrections remains required. The completed source-only removal review
also identified payload-only reconstruction in the earlier mixed identity
check. Its native assertions remain; the cancelled reconstruction assertion
and runtime branch have been removed.

The [bootstrap follow-up review](../../../../testing_ground/issue-45/native-bootstrap-followup-source-review.md)
was read in full, SHA-256
`067C854FA4E3EC04EFDEC61622CB58B7997B6AD0F5F487F0474D55329A663085`.
Its schema-marker, success-path ownership, preview database validation,
configuration read consistency, and companion cleanup findings now have the
observations above. Independent correction review remains pending. File identity
checks detect the tested replacements at creation boundaries; they do not claim
an atomic defense against arbitrary replacement at every machine instruction.
The implementation also makes no host power-loss durability claim.

The [independent correction review](../../../../testing_ground/issue-45/native-bootstrap-corrections-source-review.md)
was read in full, SHA-256
`66354B5AFFC0955E7282BC8FE9A51AA4660CF8D9BD444375D454EBD6205E8A6C`.
It found the reviewed corrections coherent. It requested explicit assertions
that foreign-companion cleanup also removes the owned main database and leaves
exactly the injected companion. Those assertions passed in the 214-case
[combined native run](stage-native-programmatic-execution.md#focused-observations).
A simultaneous pre-directory-claim creator case
remains an evidence gap, distinct from the passing unpublished-creator case.
The review does not accept strict preview behavior or full clipboard rollback.

## Remaining work

Native bootstrap still needs the remaining link/marker coverage and full
clipboard filesystem rollback and snapshot validation.
Existing native tests need their obsolete
creation assumptions updated. Cancelled import and command code must be removed,
and its tests retired or narrowed to native behavior. Surrounding and broader
native checks and independent stage acceptance remain pending.

Read-only preview safety, complete runtime integration, and the guide written
at the end remain required. No stage acceptance follows from these results.
Ordinary main-session wiring continues in the separate
[native execution record](stage-native-main-execution.md).
