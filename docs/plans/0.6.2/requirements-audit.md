# MWF 0.6.2 requirement audit

Status: implementation in progress. The interval-calculation section S1a is accepted; full stages and final review remain pending. See [stage-s1a.md](stage-s1a.md).

Authoritative behavior source: [Settle the MWF workflow-management model for 0.6.2, final consolidated resolution](https://github.com/Christopher8187/product/issues/44#issuecomment-5539997969), abbreviated `44-FCR` below. The still-active architecture gate is [Settle the MWF workflow-management model for 0.6.2, cross-session architecture clarification](https://github.com/Christopher8187/product/issues/44#issuecomment-5463708247), abbreviated `44-ARCH`. The authoritative work procedure is [Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45), abbreviated `45-PROC`. Source applicability and supersession are recorded in [source-applicability.md](./source-applicability.md).

The proposed stages are:

- `S1`: selection and read-only paths.
- `S2`: component and session storage, migration, reservations, and ownership.
- `S3`: publication ownership, fresh preparation, guards, misalignment, and membership repair.
- `S4`: ordinary commands, readiness, sampling, and causal selected work.
- `S5`: interrupt boundaries, explicit interruption, transfers, holds, and ownership-dependent controls.
- `S6`: lineage, diagnostics, CLI and documentation reconciliation, skills, and integrated verification.
- `ALL`: applies to every relevant stage.
- `GATE`: must hold before an affected stage can be accepted.
- `FINAL`: applies only after every readiness condition for the final review is met.

Every implementation, verification, stage-review, and disposition field began as `pending`. Updating a field requires a link or exact local reference to the corresponding change, command result, review, or decision.

## Retained MWF 0.6.1 requirements

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 17-SUP-001 | Withdraw the older term "Project provenance." | the shared-vocabulary decision, `Supersession note` | current MWF documentation | S6 | pending | pending | pending | pending |
| 17-SUP-002 | Preserve output provenance under the single node prefix `node/<node-name>/output/`. | the shared-vocabulary decision, `Supersession note` | the 0.6.1 implementation task published behavior | ALL | pending | pending | pending | pending |
| 17-SUP-003 | Preserve per-job storage as only `input.json` and `output.json`. | the shared-vocabulary decision, `Supersession note` | the 0.6.1 implementation task published behavior | ALL | pending | pending | pending | pending |
| 17-SUP-004 | Do not reintroduce `JobFileSystem`. | the shared-vocabulary decision, `Supersession note` | the 0.6.1 implementation task published behavior | ALL | pending | pending | pending | pending |
| 17-SUP-005 | Do not reintroduce `ctx.write()`. | the shared-vocabulary decision, `Supersession note` | the 0.6.1 implementation task published behavior | ALL | pending | pending | pending | pending |
| 17-SUP-006 | Do not reintroduce `ctx.write_bytes()`. | the shared-vocabulary decision, `Supersession note` | the 0.6.1 implementation task published behavior | ALL | pending | pending | pending | pending |
| 17-SUP-007 | Do not reintroduce `ctx.files_dir`. | the shared-vocabulary decision, `Supersession note` | the 0.6.1 implementation task published behavior | ALL | pending | pending | pending | pending |
| 17-SUP-008 | Do not reintroduce automatic copying of files returned from tasks. | the shared-vocabulary decision, `Supersession note` | the 0.6.1 implementation task published behavior | ALL | pending | pending | pending | pending |
| 17-SUP-009 | Do not reintroduce `ctx.transaction()`. | the shared-vocabulary decision, `Supersession note` | the 0.6.1 implementation task published behavior | ALL | pending | pending | pending | pending |
| 17-SUP-010 | Follow current MWF documentation wherever the older the shared-vocabulary decision resolution differs. | the shared-vocabulary decision, `Supersession note` | 17-SUP-001 through 17-SUP-009 | ALL | pending | pending | pending | pending |

## Behavioral requirements

### Responsibility and scope

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-SCP-001 | MWF owns command selection. | 44-FCR, `Responsibility and scope` | none | S1 | pending | pending | pending | pending |
| 44-SCP-002 | MWF owns quotient-DAG execution boundaries. | 44-FCR, `Responsibility and scope` | 44-SCP-001 | S1 | pending | pending | pending | pending |
| 44-SCP-003 | MWF owns component lifecycle and stability. | 44-FCR, `Responsibility and scope` | none | S2 | pending | pending | pending | pending |
| 44-SCP-004 | MWF owns execution sampling. | 44-FCR, `Responsibility and scope` | 44-SCP-003 | S4 | pending | pending | pending | pending |
| 44-SCP-005 | MWF owns interrupt sessions. | 44-FCR, `Responsibility and scope` | 44-SCP-003 | S5 | pending | pending | pending | pending |
| 44-SCP-006 | MWF owns misalignment detection. | 44-FCR, `Responsibility and scope` | 44-SCP-003 | S3 | pending | pending | pending | pending |
| 44-SCP-007 | MWF owns persisted session state. | 44-FCR, `Responsibility and scope` | none | S2 | pending | pending | pending | pending |
| 44-SCP-008 | MWF owns tracing and management diagnostics. | 44-FCR, `Responsibility and scope` | 44-SCP-003, 44-SCP-007 | S6 | pending | pending | pending | pending |
| 44-SCP-009 | A workflow declares its raw graph. | 44-FCR, `Responsibility and scope` | the shared-vocabulary decision terminology | ALL | pending | pending | pending | pending |
| 44-SCP-010 | A workflow declares waiting raw nodes. | 44-FCR, `Responsibility and scope` | 44-SCP-009 | S5 | pending | pending | pending | pending |
| 44-SCP-011 | A workflow declares interrupt components through raw-node configuration. | 44-FCR, `Responsibility and scope` | 44-SCP-009 | S5 | pending | pending | pending | pending |
| 44-SCP-012 | A workflow declares report branches. | 44-FCR, `Responsibility and scope` | 44-SCP-009 | S6 | pending | pending | pending | pending |
| 44-SCP-013 | The management layer translates vague actor instructions into exact MWF operations. | 44-FCR, `Responsibility and scope` | 44-SCP-001 | S6 | pending | pending | pending | pending |
| 44-SCP-014 | Management behavior and instructions must remain actor-neutral. | 44-FCR, `Responsibility and scope` | 44-SCP-013 | S6 | pending | pending | pending | pending |
| 44-SCP-015 | AI validation-ghost discovery remains outside MWF 0.6.2. | 44-FCR, `Responsibility and scope` | the shared-vocabulary decision terminology | ALL | pending | pending | pending | pending |
| 44-SCP-016 | Inspection sampling is a named review practice, not executable MWF behavior. | 44-FCR, `Responsibility and scope` | none | S6 | pending | pending | pending | pending |
| 44-SCP-017 | Inspection sampling reproducibly selects framework-visible poor-performing nodes and jobs. | 44-FCR, `Responsibility and scope` | 44-SCP-016 | S6 | pending | pending | pending | pending |
| 44-SCP-018 | Inspection sampling emphasizes failures, unusually dirty filter stages, and expensive late fallback work. | 44-FCR, `Responsibility and scope` | 44-SCP-017 | S6 | pending | pending | pending | pending |
| 44-SCP-019 | The actor fixes clear deterministic problems first and leaves remaining semantic judgment to human review. | 44-FCR, `Responsibility and scope` | 44-SCP-016 | S6 | pending | pending | pending | pending |
| 44-SCP-020 | Subgraph management supports selecting, planning, running, resuming, resetting, and debugging an isolated quotient-DAG part. | 44-FCR, `Responsibility and scope` | 44-SCP-001, 44-SCP-002 | ALL | pending | pending | pending | pending |
| 44-SCP-021 | Normal subgraph selections are a component, a descendant selection, or a quotient interval. | 44-FCR, `Responsibility and scope` | 44-SCP-020 | S1 | pending | pending | pending | pending |
| 44-SCP-022 | Management may inspect the matching raw subgraph, exact input files, jobs, and traces. | 44-FCR, `Responsibility and scope` | 44-SCP-020 | S6 | pending | pending | pending | pending |
| 44-SCP-023 | Subgraph inspection and execution must not execute a component outside the selected execution boundary. | 44-FCR, `Responsibility and scope` | 44-SCP-002, 44-SCP-022 | S1 | pending | pending | pending | pending |
| 44-SCP-024 | Managed publication to an unselected receiver follows the command and preparation rules rather than expanding execution. | 44-FCR, `Responsibility and scope` | 44-SCP-023 | S3 | pending | pending | pending | pending |
| 44-SCP-025 | Kaicenat remains read-only during the implementation task. | 44-FCR, `Responsibility and scope` | the implementation task boundary | ALL | pending | pending | pending | pending |
| 44-SCP-026 | MWF 0.6.2 adds no Kaicenat compatibility layer. | 44-FCR, `Responsibility and scope` | 44-SCP-025 | ALL | pending | pending | pending | pending |
| 44-SCP-027 | the implementation task does not refactor Kaicenat examples or workflows. | 44-FCR, `Responsibility and scope` | 44-SCP-025 | ALL | pending | pending | pending | pending |

### Component model

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-CMP-001 | The exact Hoeflein component is the quotient-DAG execution unit. | 44-FCR, `Component model` | the shared-vocabulary decision component terminology | S2 | pending | pending | pending | pending |
| 44-CMP-002 | Every raw node in one component derives one authoritative lifecycle value from the component record. | 44-FCR, `Component model` | 44-CMP-001 | S2 | pending | pending | pending | pending |
| 44-CMP-003 | Component lifecycle values are exactly `queued`, `running`, `sampled`, `done`, and `failed`. | 44-FCR, `Component model` | 44-CMP-002 | S2 | pending | pending | pending | pending |
| 44-CMP-004 | `stable` and `unstable` are successful forms of a done result, not lifecycle values. | 44-FCR, `Component model` | 44-CMP-003 | S2 | pending | pending | pending | pending |
| 44-CMP-005 | A successful unstable result stores one exact interrupt-session ID as its instability origin. | 44-FCR, `Component model` | 44-CMP-004, 44-SES-030 | S2 | pending | pending | pending | pending |
| 44-CMP-006 | Sampled work is not done. | 44-FCR, `Component model` | 44-CMP-003 | S2 | pending | pending | pending | pending |
| 44-CMP-007 | Sampled work retains the compatible stability lineage and origin established by completed work so far. | 44-FCR, `Component model` | 44-CMP-005, 44-CMP-006 | S2 | pending | pending | pending | pending |
| 44-CMP-008 | Queued and failed components have no successful stability result. | 44-FCR, `Component model` | 44-CMP-003, 44-CMP-004 | S2 | pending | pending | pending | pending |
| 44-CMP-009 | A component never automatically carries more than one instability origin. | 44-FCR, `Component model` | 44-CMP-005 | S2 | pending | pending | pending | pending |
| 44-CMP-010 | `misaligned` is a component Boolean separate from lifecycle and stability. | 44-FCR, `Component model` | 44-CMP-002 | S2 | pending | pending | pending | pending |
| 44-CMP-011 | Misalignment means managed input or jobs changed after the current done, sampled, or failed result was established. | 44-FCR, `Component model` | 44-CMP-010 | S3 | pending | pending | pending | pending |
| 44-CMP-012 | A normal late arrival can cause misalignment. | 44-FCR, `Component model` | 44-CMP-011 | S3 | pending | pending | pending | pending |
| 44-CMP-013 | Reset-like preparation can cause misalignment when it changes producer-owned downstream input or jobs while leaving the receiver unselected. | 44-FCR, `Component model` | 44-CMP-011, 44-PRP-022, 44-PRP-026 | S3 | pending | pending | pending | pending |
| 44-CMP-014 | Diagnostics display misalignment together with lifecycle and stability, including exact instability origin when present. | 44-FCR, `Component model` | 44-CMP-010 | S6 | pending | pending | pending | pending |
| 44-CMP-015 | Waiting remains a raw-node declaration and active display condition, not a lifecycle value. | 44-FCR, `Component model` | 44-CMP-002 | S2 | pending | pending | pending | pending |
| 44-CMP-016 | A queued component never displays one of its members as waiting. | 44-FCR, `Component model` | 44-CMP-003, 44-CMP-015 | S2 | pending | pending | pending | pending |
| 44-CMP-017 | While the component is running, a configured raw node with an active internal wait may display `waiting` instead of `running`. | 44-FCR, `Component model` | 44-CMP-015, 44-CMP-016 | S2 | pending | pending | pending | pending |
| 44-CMP-018 | Autostart remains graph routing and is not component state. | 44-FCR, `Component model` | the shared-vocabulary decision autostart terminology | S2 | pending | pending | pending | pending |
| 44-CMP-019 | Jobs retain `queued`, `running`, `done`, `failed`, `cancelled`, and `skipped` outcomes. | 44-FCR, `Component model` | 0.6.1 job behavior | S2 | pending | pending | pending | pending |
| 44-CMP-020 | A cancelled job prevents successful component completion and leaves the component failed until repaired. | 44-FCR, `Component model` | 44-CMP-003, 44-CMP-019 | S4 | pending | pending | pending | pending |
| 44-CMP-021 | A skipped job may count as successful under existing job rules. | 44-FCR, `Component model` | 44-CMP-019 | S4 | pending | pending | pending | pending |
| 44-CMP-022 | Components never use cancelled or skipped lifecycle values. | 44-FCR, `Component model` | 44-CMP-003, 44-CMP-019 | S2 | pending | pending | pending | pending |
| 44-CMP-023 | Remove public `skip_node()` behavior. | 44-FCR, `Component model` | 44-CMP-022 | S4 | pending | pending | pending | pending |
| 44-CMP-024 | Remove cancelled and skipped from raw-node lifecycle language. | 44-FCR, `Component model` | 44-CMP-022 | S6 | pending | pending | pending | pending |
| 44-CMP-025 | Persist lifecycle, stability, instability origin, misalignment, and alignment generation once for each exact Hoeflein component. | 44-FCR, `Component model` | 44-CMP-001 | S2 | pending | pending | pending | pending |
| 44-CMP-026 | Do not keep independent authoritative copies of component result state on raw-node rows. | 44-FCR, `Component model` | 44-CMP-002, 44-CMP-025 | S2 | pending | pending | pending | pending |

### Graph commands and selections

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-CMD-001 | The graph-management family contains exactly three operations across exactly three selection shapes. | 44-FCR, `The nine graph commands` | 44-SCP-001 | S4 | pending | pending | pending | pending |
| 44-CMD-002 | `run` freshly prepares and executes one component. | 44-FCR, `The nine graph commands` | 44-CMD-011 | S4 | pending | pending | pending | pending |
| 44-CMD-003 | `runfrom` freshly prepares and executes a component and its quotient descendants. | 44-FCR, `The nine graph commands` | 44-CMD-002, 44-CMD-014 | S4 | pending | pending | pending | pending |
| 44-CMD-004 | `runbetween` freshly prepares and executes a half-open quotient interval. | 44-FCR, `The nine graph commands` | 44-CMD-002, 44-CMD-016 | S4 | pending | pending | pending | pending |
| 44-CMD-005 | `resume` preserves successful work, requeues repairable unsuccessful work under existing rules, and continues one component. | 44-FCR, `The nine graph commands` | 44-CMP-025 | S4 | pending | pending | pending | pending |
| 44-CMD-006 | `resumefrom` applies resume behavior to a component and its quotient descendants. | 44-FCR, `The nine graph commands` | 44-CMD-005, 44-CMD-014 | S4 | pending | pending | pending | pending |
| 44-CMD-007 | `resumebetween` applies resume behavior to a half-open quotient interval. | 44-FCR, `The nine graph commands` | 44-CMD-005, 44-CMD-016 | S4 | pending | pending | pending | pending |
| 44-CMD-008 | `reset` performs appropriate fresh preparation for one component without running task code. | 44-FCR, `The nine graph commands` | 44-CMD-011 | S3 | pending | pending | pending | pending |
| 44-CMD-009 | `resetfrom` performs fresh preparation for a component and its quotient descendants without running task code. | 44-FCR, `The nine graph commands` | 44-CMD-008, 44-CMD-014 | S3 | pending | pending | pending | pending |
| 44-CMD-010 | `resetbetween` performs fresh preparation for a half-open quotient interval without running task code. | 44-FCR, `The nine graph commands` | 44-CMD-008, 44-CMD-016 | S3 | pending | pending | pending | pending |
| 44-CMD-011 | Full run and reset forms call one shared semantic fresh-component preparation function. | 44-FCR, `The nine graph commands` | 44-PRP-016 through 44-PRP-021 | S3 | pending | pending | pending | pending |
| 44-CMD-012 | Keep job-scoped `restart` and `recover`. | 44-FCR, `The nine graph commands` | 0.6.1 behavior | S5 | pending | pending | pending | pending |
| 44-CMD-013 | Keep `refuse` and `refuseafter` as modifiers where they do not conflict with the final resolution; they are not added family commands, and `between` is not their alias. | 44-FCR, `The nine graph commands` | 44-CMD-001 | S4 | pending | pending | pending | pending |
| 44-CMD-014 | A `from` selection contains the starting component and all its quotient-DAG descendants. | 44-FCR, `The nine graph commands` | 44-CMP-001 | S1 | pending | pending | pending | pending |
| 44-CMD-015 | A `from` execution reruns the starting component without resetting that component's incoming input. | 44-FCR, `The nine graph commands` | 44-CMD-014, 44-PRP-016 | S4 | pending | pending | pending | pending |
| 44-CMD-016 | Compute the closed quotient interval as `descendants-or-self(C_A) intersect ancestors-or-self(C_B)` without enumerating paths. | 44-FCR, `The nine graph commands` | 44-CMP-001 | S1 | [calculation](stage-s1a.md) | [checks passed](stage-s1a.md#broader-verification) | [Sol xhigh PASS](selection-review.md#s1a-review-record) | calculation accepted; CLI integration pending |
| 44-CMD-017 | `between A B` uses the half-open interval `[C_A, C_B)`. | 44-FCR, `The nine graph commands` | 44-CMD-016 | S1 | [calculation](stage-s1a.md) | [checks passed](stage-s1a.md#broader-verification) | [Sol xhigh PASS](selection-review.md#s1a-review-record) | calculation accepted; CLI integration pending |
| 44-CMD-018 | A between selection includes A's component and excludes B's component. | 44-FCR, `The nine graph commands` | 44-CMD-017 | S1 | [calculation](stage-s1a.md) | [checks passed](stage-s1a.md#broader-verification) | [Sol xhigh PASS](selection-review.md#s1a-review-record) | calculation accepted; CLI integration pending |
| 44-CMD-019 | A between selection contains the union of all directed quotient paths from A to B. | 44-FCR, `The nine graph commands` | 44-CMD-016 | S1 | [calculation](stage-s1a.md) | [checks passed](stage-s1a.md#broader-verification) | [Sol xhigh PASS](selection-review.md#s1a-review-record) | calculation accepted; CLI integration pending |
| 44-CMD-020 | B's component must be a strict directed descendant of A's component. | 44-FCR, `The nine graph commands` | 44-CMD-016 | S1 | [calculation](stage-s1a.md) | [checks passed](stage-s1a.md#broader-verification) | [Sol xhigh PASS](selection-review.md#s1a-review-record) | calculation accepted; CLI integration pending |
| 44-CMD-021 | Reject between endpoints in the same component. | 44-FCR, `The nine graph commands` | 44-CMD-020 | S1 | [calculation](stage-s1a.md) | [checks passed](stage-s1a.md#broader-verification) | [Sol xhigh PASS](selection-review.md#s1a-review-record) | calculation accepted; CLI integration pending |
| 44-CMD-022 | Reject every between request where B is not reachable from A, including undirected-only connectivity. | 44-FCR, `The nine graph commands` | 44-CMD-020 | S1 | [calculation](stage-s1a.md) | [checks passed](stage-s1a.md#broader-verification) | [Sol xhigh PASS](selection-review.md#s1a-review-record) | calculation accepted; CLI integration pending |
| 44-CMD-023 | Apply the selection model consistently in parsing, planning, execution, monitoring, help, descriptions, and tests for all nine commands. | 44-FCR, `The nine graph commands` | 44-CMD-001, 44-CMD-014, 44-CMD-016 | S6 | pending | pending | pending | pending |
| 44-CMD-024 | Publication from a selected component may reach an unselected component without executing the receiver. | 44-FCR, `The nine graph commands` | 44-SCP-024 | S3 | pending | pending | pending | pending |
| 44-CMD-025 | Remove `clean`, `cleanfrom`, `wipe`, and `wipefrom` from parser registration. | 44-FCR, `The nine graph commands` | none | S4 | pending | pending | pending | pending |
| 44-CMD-026 | Remove those four commands from command-name collections, `--describe`, help, documentation, generated command examples, and tests. | 44-FCR, `The nine graph commands` | 44-CMD-025 | S6 | pending | pending | pending | pending |
| 44-CMD-027 | Do not leave redirect stubs or a compatibility layer for the four removed commands. | 44-FCR, `The nine graph commands` | 44-CMD-025 | S4 | pending | pending | pending | pending |
| 44-CMD-028 | Example READMEs may receive only the narrow documentation change needed to stop teaching a removed command. | 44-FCR, `The nine graph commands` | 44-CMD-026 | S6 | pending | pending | pending | pending |
| 44-CMD-029 | Do not change example graphs, tasks, or behavior, and do not perform the 0.6.3 example repair. | 44-FCR, `The nine graph commands` | the context-loop decision boundary | ALL | pending | pending | pending | pending |
| 44-CMD-030 | Preserve shared fresh-preparation functions used by reset and run. | 44-FCR, `The nine graph commands` | 44-CMD-011 | S3 | pending | pending | pending | pending |
| 44-CMD-031 | `runbetween` and `resumebetween` use `--plan` for their read-only previews. | 44-FCR, `The nine graph commands` | 44-CMD-004, 44-CMD-007 | S1 | pending | pending | pending | pending |
| 44-CMD-032 | `resetbetween` uses `--dry-run` for its read-only preview. | 44-FCR, `The nine graph commands` | 44-CMD-010 | S1 | pending | pending | pending | pending |
| 44-CMD-033 | Do not add a second reset-preview spelling for symmetry. | 44-FCR, `The nine graph commands` | 44-CMD-032 | S1 | pending | pending | pending | pending |
| 44-CMD-034 | Every between preview shows selected quotient components and expanded raw nodes. | 44-FCR, `The nine graph commands` | 44-CMD-017 | S1 | pending | pending | pending | pending |
| 44-CMD-035 | Every between preview shows the excluded B component. | 44-FCR, `The nine graph commands` | 44-CMD-018 | S1 | pending | pending | pending | pending |
| 44-CMD-036 | Every between preview shows entering and leaving edges and prerequisite state. | 44-FCR, `The nine graph commands` | 44-CMD-019 | S1 | pending | pending | pending | pending |
| 44-CMD-037 | Every between preview shows unselected receivers that may receive publications. | 44-FCR, `The nine graph commands` | 44-CMD-024 | S3 | pending | pending | pending | pending |
| 44-CMD-038 | Read-only paths may load source and graph information. | 44-FCR, `The nine graph commands` | none | S1 | pending | pending | pending | pending |
| 44-CMD-039 | Read-only paths cause no durable or externally visible mutation. | 44-FCR, `The nine graph commands` | 44-CMD-038 | S1 | pending | pending | pending | pending |
| 44-CMD-040 | A plan or dry-run must not migrate storage. | 44-FCR, `The nine graph commands` | 44-CMD-039 | S1 | pending | pending | pending | pending |
| 44-CMD-041 | A plan or dry-run must not mount or refresh runtime state. | 44-FCR, `The nine graph commands` | 44-CMD-039 | S1 | pending | pending | pending | pending |
| 44-CMD-042 | A plan or dry-run must not create starter jobs. | 44-FCR, `The nine graph commands` | 44-CMD-039 | S1 | pending | pending | pending | pending |
| 44-CMD-043 | A plan or dry-run must not reserve sessions. | 44-FCR, `The nine graph commands` | 44-CMD-039 | S1 | pending | pending | pending | pending |
| 44-CMD-044 | A plan or dry-run must not prepare components. | 44-FCR, `The nine graph commands` | 44-CMD-039 | S1 | pending | pending | pending | pending |
| 44-CMD-045 | A plan or dry-run must not run task code. | 44-FCR, `The nine graph commands` | 44-CMD-039 | S1 | pending | pending | pending | pending |
| 44-CMD-046 | Separate current mutating bootstrap behavior from every advertised read-only path. | 44-FCR, `The nine graph commands` | 44-CMD-040 through 44-CMD-045 | S1 | pending | pending | pending | pending |
| 44-CMD-047 | `reset`, `resetfrom`, and `resetbetween` refuse before mutation while any main or interrupt execution session is live. | 44-FCR, `The nine graph commands` | 44-SES-001, 44-SES-002 | S3 | pending | pending | pending | pending |
| 44-CMD-048 | Reset preparation must not freshen shared component or publication state underneath a live main or interrupt session. | 44-FCR, `The nine graph commands` | 44-CMD-047 | S3 | pending | pending | pending | pending |
| 44-CMD-049 | Reset has no interrupt form. | 44-FCR, `The nine graph commands` | 44-CMD-008 through 44-CMD-010, 44-INT-001 | S3, S5 | pending | pending | pending | pending |

### Producer-qualified input and fresh preparation

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-PRP-001 | A managed file forwarded from producer raw node A to receiver raw node B uses `node/B/input/A/<producer-relative-path>`. | 44-FCR, `Producer-qualified input and fresh preparation` | 0.6.1 filesystem behavior | S3 | pending | pending | pending | pending |
| 44-PRP-002 | Forwarding A job 7 output `evidence/source.json` to B produces visible path `node/B/input/A/evidence/source.json`. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-001 | S3 | pending | pending | pending | pending |
| 44-PRP-003 | The visible receiving path does not expose the producer job ID. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-001 | S3 | pending | pending | pending | pending |
| 44-PRP-004 | The visible receiving path does not invent a Hoeflein-component name. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-001 | S3 | pending | pending | pending | pending |
| 44-PRP-005 | Publications inside a component use the actual producing raw-node name in the visible path. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-001 | S3 | pending | pending | pending | pending |
| 44-PRP-006 | Private publication ownership records the producing component execution. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-SES-030, 44-SES-031 | S3 | pending | pending | pending | pending |
| 44-PRP-007 | Private publication ownership records the producing raw node and job. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-006 | S3 | pending | pending | pending | pending |
| 44-PRP-008 | Publication ownership supports selected preparation, trace lineage, and misalignment causes. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-006, 44-PRP-007 | S3 | pending | pending | pending | pending |
| 44-PRP-009 | Receiving-input access is exact or fixed-depth, including `ctx.input_path("A", "evidence", "source.json")` and `ctx.input_files("A/*.md")`. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-001 | S3 | pending | pending | pending | pending |
| 44-PRP-010 | Receiving input is not discovered recursively. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-009 | S3 | pending | pending | pending | pending |
| 44-PRP-011 | MWF adds no source-discovery API. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-010 | S3 | pending | pending | pending | pending |
| 44-PRP-012 | Projects name the expected producers and paths. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-009 | S6 | pending | pending | pending | pending |
| 44-PRP-013 | Narrow receiving-input access does not remove output-tree traversal. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-010 | S3 | pending | pending | pending | pending |
| 44-PRP-014 | Narrow receiving-input access does not remove unrelated recursive source-module loading. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-010 | S3 | pending | pending | pending | pending |
| 44-PRP-015 | Semantics for two writes from the same producer to the same visible path are deferred. | 44-FCR, `Producer-qualified input and fresh preparation` | none | ALL | pending | pending | pending | pending |
| 44-PRP-016 | Fresh preparation preserves the established incoming input of the starting component. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-006 through 44-PRP-008 | S3 | pending | pending | pending | pending |
| 44-PRP-017 | Fresh preparation removes the freshly prepared producers' MWF-managed jobs wherever those producers published them. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-006 | S3 | pending | pending | pending | pending |
| 44-PRP-018 | Fresh preparation removes the freshly prepared producers' producer-qualified files wherever those producers published them. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-006 | S3 | pending | pending | pending | pending |
| 44-PRP-019 | Producer-owned cleanup includes receivers outside the selected quotient interval. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-017, 44-PRP-018 | S3 | pending | pending | pending | pending |
| 44-PRP-020 | Fresh preparation preserves project-owned input. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-017, 44-PRP-018 | S3 | pending | pending | pending | pending |
| 44-PRP-021 | Fresh preparation preserves material owned by unselected producers. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-017, 44-PRP-018 | S3 | pending | pending | pending | pending |
| 44-PRP-022 | Centralize downstream-change detection in the shared reset and fresh-preparation behavior. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-CMD-011 | S3 | pending | pending | pending | pending |
| 44-PRP-023 | Downstream-change detection applies to `reset`, `resetfrom`, and `resetbetween`. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-022 | S3 | pending | pending | pending | pending |
| 44-PRP-024 | Downstream-change detection applies to selected-job reset. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-022 | S3 | pending | pending | pending | pending |
| 44-PRP-025 | Downstream-change detection applies to every fresh-run form. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-022 | S3 | pending | pending | pending | pending |
| 44-PRP-026 | Removing, requeuing, replacing, or otherwise changing producer-owned input or jobs in an unselected done, sampled, or failed receiver marks that receiver misaligned once. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-CMP-011, 44-PRP-022 | S3 | pending | pending | pending | pending |
| 44-PRP-027 | Use cause `preparation-removal` for preparation-driven removal. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-026 | S3 | pending | pending | pending | pending |
| 44-PRP-028 | Use `preparation-change` with the exact action for every other preparation-driven change. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-026 | S3 | pending | pending | pending | pending |
| 44-PRP-029 | A preparation-driven cause records the initiating reset or fresh-run operation. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-027, 44-PRP-028 | S3 | pending | pending | pending | pending |
| 44-PRP-030 | A preparation-driven cause records the producer and receiving raw node. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-027, 44-PRP-028 | S3 | pending | pending | pending | pending |
| 44-PRP-031 | A preparation-driven cause records the affected kind and one representative job or path. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-027, 44-PRP-028 | S3 | pending | pending | pending | pending |
| 44-PRP-032 | A selected receiver becomes aligned only when its whole component receives full fresh preparation. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-MIS-020 | S3 | pending | pending | pending | pending |
| 44-PRP-033 | Selected-job preparation never clears component-wide misalignment. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-032 | S3 | pending | pending | pending | pending |
| 44-PRP-034 | Do not record preparation-driven causes as ordinary late arrivals. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-027, 44-PRP-028 | S3 | pending | pending | pending | pending |
| 44-PRP-035 | Batch a preparation-driven cause once rather than writing once per affected item. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-026 | S3 | pending | pending | pending | pending |
| 44-PRP-036 | Preflight includes the complete downstream mutation footprint, not only selected execution components. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-019, 44-PRP-022 | S3 | pending | pending | pending | pending |
| 44-PRP-037 | Protect every excluded affected receiver with a short-lived atomic mutation guard. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-036, 44-SES-004 | S3 | pending | pending | pending | pending |
| 44-PRP-038 | Hold the downstream mutation guard only through fresh preparation and then release it. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-037 | S3 | pending | pending | pending | pending |
| 44-PRP-039 | The downstream mutation guard is not session-lived execution ownership. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-037 | S3 | pending | pending | pending | pending |
| 44-PRP-040 | The downstream mutation guard does not change `mwf threads NODE` ownership. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-039, 44-SES-049 through 44-SES-053 | S3, S5 | pending | pending | pending | pending |
| 44-PRP-041 | Refuse before mutation if an excluded affected receiver is running. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-036 | S3 | pending | pending | pending | pending |
| 44-PRP-042 | Refuse before mutation if an excluded affected receiver is owned by a live session. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-036, 44-SES-004 | S3 | pending | pending | pending | pending |
| 44-PRP-043 | Refuse before mutation if another live session already guards an excluded affected receiver. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-036, 44-PRP-037 | S3 | pending | pending | pending | pending |
| 44-PRP-044 | A downstream-footprint refusal identifies the receiver and owner. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-041 through 44-PRP-043 | S3 | pending | pending | pending | pending |
| 44-PRP-045 | An interrupt intended to pause direct predecessors must not pause or reset an out-neighbor merely because preparation would affect it. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-041, 44-INT-011 | S5 | pending | pending | pending | pending |
| 44-PRP-046 | Plans show every excluded downstream mutation, affected receiver, and required guard. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-036 through 44-PRP-043 | S3 | pending | pending | pending | pending |
| 44-PRP-047 | `resetbetween A B` may remove stale B material published by a selected prepared producer even though B is excluded, while preserving B material from unselected producers. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-CMD-018, 44-PRP-019, 44-PRP-021 | S3 | pending | pending | pending | pending |
| 44-PRP-048 | Reset restores rerunnable MWF state but cannot undo project-owned external side effects. | 44-FCR, `Producer-qualified input and fresh preparation` | none | S6 | pending | pending | pending | pending |
| 44-PRP-049 | Plans and documentation state the external-side-effect limit plainly. | 44-FCR, `Producer-qualified input and fresh preparation` | 44-PRP-048 | S6 | pending | pending | pending | pending |

### Execution sampling and exact selected jobs

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-SMP-001 | Execution sampling is valid only on plain `run`, including plain `run ... --interrupt`. | 44-FCR, `Execution sampling` | 44-CMD-002 | S4 | pending | pending | pending | pending |
| 44-SMP-002 | Sampling is invalid on `resume`, every `from` or `between` command, and every reset command. | 44-FCR, `Execution sampling` | 44-SMP-001 | S4 | pending | pending | pending | pending |
| 44-SMP-003 | Support singleton count syntax such as `mwf run A sample 30`. | 44-FCR, `Execution sampling` | 44-SMP-001 | S4 | pending | pending | pending | pending |
| 44-SMP-004 | Support singleton percentage syntax such as `mwf run A sample 10%`. | 44-FCR, `Execution sampling` | 44-SMP-001 | S4 | pending | pending | pending | pending |
| 44-SMP-005 | Support per-raw-node count assignments such as `mwf run X sample X=30 Y=10`. | 44-FCR, `Execution sampling` | 44-SMP-001 | S4 | pending | pending | pending | pending |
| 44-SMP-006 | Support per-raw-node percentage assignments such as `mwf run X sample X=10% Y=25%`. | 44-FCR, `Execution sampling` | 44-SMP-001 | S4 | pending | pending | pending | pending |
| 44-SMP-007 | Every named assignment must name a raw node in the starting component. | 44-FCR, `Execution sampling` | 44-SMP-005, 44-SMP-006 | S4 | pending | pending | pending | pending |
| 44-SMP-008 | Reject a sampling assignment that names a raw node outside the starting component. | 44-FCR, `Execution sampling` | 44-SMP-007 | S4 | pending | pending | pending | pending |
| 44-SMP-009 | An omitted member of the starting component selects zero starting jobs. | 44-FCR, `Execution sampling` | 44-SMP-005, 44-SMP-006 | S4 | pending | pending | pending | pending |
| 44-SMP-010 | With no positive selector, report a clear no-work result and change no component state. | 44-FCR, `Execution sampling` | 44-SMP-009 | S4 | pending | pending | pending | pending |
| 44-SMP-011 | Singleton shorthand applies only to the named raw node, even in a multi-node component. | 44-FCR, `Execution sampling` | 44-SMP-003, 44-SMP-004 | S4 | pending | pending | pending | pending |
| 44-SMP-012 | Singleton shorthand selects zero starting jobs from every other component member. | 44-FCR, `Execution sampling` | 44-SMP-011 | S4 | pending | pending | pending | pending |
| 44-SMP-013 | Same-component causal circulation still applies after shorthand selects starting jobs. | 44-FCR, `Execution sampling` | 44-SMP-011, 44-SMP-038 | S4 | pending | pending | pending | pending |
| 44-SMP-014 | Use named assignments to select starting jobs from several raw members. | 44-FCR, `Execution sampling` | 44-SMP-005, 44-SMP-006 | S6 | pending | pending | pending | pending |
| 44-SMP-015 | Without `--status`, every existing job in the addressed raw node is in the starting population. | 44-FCR, `Execution sampling` | 0.6.1 count-sampling behavior | S4 | pending | pending | pending | pending |
| 44-SMP-016 | With `--status`, filter the population by status before applying a count or percentage. | 44-FCR, `Execution sampling` | 44-SMP-015 | S4 | pending | pending | pending | pending |
| 44-SMP-017 | A percentage of zero selects zero jobs. | 44-FCR, `Execution sampling` | 44-SMP-004 | S4 | pending | pending | pending | pending |
| 44-SMP-018 | For positive percentage `p` and population `N`, select `ceil(p * N / 100)` jobs. | 44-FCR, `Execution sampling` | 44-SMP-004 | S4 | pending | pending | pending | pending |
| 44-SMP-019 | A positive percentage selects at least one job when the population is nonempty. | 44-FCR, `Execution sampling` | 44-SMP-018 | S4 | pending | pending | pending | pending |
| 44-SMP-020 | Rank jobs deterministically with SHA-256 keyed by the recorded seed and stable job identity. | 44-FCR, `Execution sampling` | 44-SMP-015 | S4 | pending | pending | pending | pending |
| 44-SMP-021 | Select the required number of lowest-ranked jobs. | 44-FCR, `Execution sampling` | 44-SMP-020 | S4 | pending | pending | pending | pending |
| 44-SMP-022 | Selection is uniform without replacement within each raw node. | 44-FCR, `Execution sampling` | 44-SMP-020, 44-SMP-021 | S4 | pending | pending | pending | pending |
| 44-SMP-023 | Persist a sampling algorithm-version identifier. | 44-FCR, `Execution sampling` | 44-SMP-020 | S4 | pending | pending | pending | pending |
| 44-SMP-024 | Independent samples may overlap. | 44-FCR, `Execution sampling` | 44-SMP-022 | S4 | pending | pending | pending | pending |
| 44-SMP-025 | Generate and print a fresh pseudorandom seed by default for every invocation. | 44-FCR, `Execution sampling` | 44-SMP-020 | S4 | pending | pending | pending | pending |
| 44-SMP-026 | Persist the seed only when execution begins. | 44-FCR, `Execution sampling` | 44-SMP-025 | S4 | pending | pending | pending | pending |
| 44-SMP-027 | A read-only plan prints its generated seed without persisting it. | 44-FCR, `Execution sampling` | 44-SMP-025, 44-CMD-039 | S4 | pending | pending | pending | pending |
| 44-SMP-028 | `--seed` reproduces a selection only while the population and relevant input remain unchanged. | 44-FCR, `Execution sampling` | 44-SMP-020 | S4 | pending | pending | pending | pending |
| 44-SMP-029 | Persist algorithm, per-node population identity, selectors, selected IDs, population and input digests, combined drift digest, seed, and sample ID. | 44-FCR, `Execution sampling` | 44-SMP-023, 44-SMP-028, 44-SES-030, 44-SES-031 | S4 | pending | pending | pending | pending |
| 44-SMP-030 | Sampling plan output shows each raw node's population, selector, selected count, selected job IDs, generated seed, and digests. | 44-FCR, `Execution sampling` | 44-SMP-027, 44-SMP-029 | S4 | pending | pending | pending | pending |
| 44-SMP-031 | Sampling plan output prints an exact replay command using `--seed` and `--expect-population <combined-digest>`. | 44-FCR, `Execution sampling` | 44-SMP-030 | S4 | pending | pending | pending | pending |
| 44-SMP-032 | Replay with `--expect-population` refuses before fresh preparation if any per-node population or relevant input changed. | 44-FCR, `Execution sampling` | 44-SMP-028, 44-SMP-031 | S4 | pending | pending | pending | pending |
| 44-SMP-033 | Execution without `--expect-population` reserves its session, freezes its own current population and input, and then generates or accepts its seed. | 44-FCR, `Execution sampling` | 44-SMP-025, 44-SES-001, 44-SES-030 | S4 | pending | pending | pending | pending |
| 44-SMP-034 | Execution without `--expect-population` does not claim to execute an earlier plan. | 44-FCR, `Execution sampling` | 44-SMP-033 | S4 | pending | pending | pending | pending |
| 44-SMP-035 | Selected existing jobs receive selected-job fresh preparation. | 44-FCR, `Execution sampling` | 44-PRP-024 | S4 | pending | pending | pending | pending |
| 44-SMP-036 | Selected-job preparation follows recorded same-component causal descendants from earlier executions of the selected roots and clears their stale MWF-owned work. | 44-FCR, `Execution sampling` | 44-SMP-035, 44-PRP-006 | S4 | pending | pending | pending | pending |
| 44-SMP-037 | Selected-job preparation does not execute previously recorded descendants merely because they existed. | 44-FCR, `Execution sampling` | 44-SMP-036 | S4 | pending | pending | pending | pending |
| 44-SMP-038 | Selected execution admits each selected root and same-component jobs newly created by this invocation, recursively as causal jobs appear. | 44-FCR, `Execution sampling` | 44-SMP-036 | S4 | pending | pending | pending | pending |
| 44-SMP-039 | Selected execution leaves unrelated preexisting jobs untouched and unexecuted. | 44-FCR, `Execution sampling` | 44-SMP-038 | S4 | pending | pending | pending | pending |
| 44-SMP-040 | Selected execution does not execute quotient descendants, although publication outside the component is allowed. | 44-FCR, `Execution sampling` | 44-SMP-038, 44-CMD-024 | S4 | pending | pending | pending | pending |
| 44-SMP-041 | A successful partial sample sets the entire component to `sampled`. | 44-FCR, `Execution sampling` | 44-CMP-006 | S4 | pending | pending | pending | pending |
| 44-SMP-042 | Earlier wording "initially empty" means eligible queued jobs exist but none has previously been processed; it does not mean a zero-job population. | 44-FCR, `Execution sampling` | 44-SMP-015 | S4 | pending | pending | pending | pending |
| 44-SMP-043 | Selecting no jobs never changes component state. | 44-FCR, `Execution sampling` | 44-SMP-010 | S4 | pending | pending | pending | pending |
| 44-SMP-044 | A partial sample uses ordinary run readiness. | 44-FCR, `Execution sampling` | 44-SMP-041 | S4 | pending | pending | pending | pending |
| 44-SMP-045 | All direct parents successfully stable permits ordinary sampling. | 44-FCR, `Execution sampling` | 44-SMP-044 | S4 | pending | pending | pending | pending |
| 44-SMP-046 | All direct parents successfully unstable with the same exact origin permits sampling with that origin. | 44-FCR, `Execution sampling` | 44-SMP-044, 44-CMP-005 | S4 | pending | pending | pending | pending |
| 44-SMP-047 | Incomplete parents, stable and unstable parents together, or different instability origins refuse ordinary sampling. | 44-FCR, `Execution sampling` | 44-SMP-044 | S4 | pending | pending | pending | pending |
| 44-SMP-048 | A sampled component blocks quotient descendants. | 44-FCR, `Execution sampling` | 44-SMP-041 | S4 | pending | pending | pending | pending |
| 44-SMP-049 | An explicit interrupt may override predecessor readiness only for the interrupt-classified starting component. | 44-FCR, `Execution sampling` | 44-SMP-047, 44-INT-001 | S5 | pending | pending | pending | pending |
| 44-SMP-050 | Treat a sample as a full ordinary run when at least one job is selected, every member with nonzero eligible work selects its full population, all causal work succeeds, and no newer unprocessed work exists. | 44-FCR, `Execution sampling` | 44-SMP-038, 44-SMP-041 | S4 | pending | pending | pending | pending |
| 44-SMP-051 | Zero-job component members are vacuously covered for the full-coverage calculation. | 44-FCR, `Execution sampling` | 44-SMP-050 | S4 | pending | pending | pending | pending |
| 44-SMP-052 | Tell the actor when a sample achieves full coverage. | 44-FCR, `Execution sampling` | 44-SMP-050 | S4 | pending | pending | pending | pending |
| 44-SMP-053 | Calculate full-coverage sample stability from direct parents rather than declaring it stable solely because coverage reached 100 percent. | 44-FCR, `Execution sampling` | 44-SMP-045 through 44-SMP-047, 44-SMP-050 | S4 | pending | pending | pending | pending |
| 44-SMP-054 | An aligned sampled component may resume. | 44-FCR, `Execution sampling` | 44-SMP-041, 44-MIS-020, 44-MIS-022 | S4 | pending | pending | pending | pending |
| 44-SMP-055 | When all remaining eligible work succeeds and no unprocessed work remains, resume promotes sampled to done while preserving compatible stability or the exact origin. | 44-FCR, `Execution sampling` | 44-SMP-054 | S4 | pending | pending | pending | pending |
| 44-SMP-056 | Misalignment blocks resume of a sampled component. | 44-FCR, `Execution sampling` | 44-SMP-054, 44-MIS-020, 44-MIS-022 | S4 | pending | pending | pending | pending |
| 44-SMP-057 | Exact selected-job execution uses the same prior-descendant preparation and new causal-circulation rules as random sampling. | 44-FCR, `Execution sampling` | 44-SMP-035 through 44-SMP-040 | S4 | pending | pending | pending | pending |
| 44-SMP-058 | `mwf run A job 17` executes job 17 and same-component jobs newly created by that invocation recursively, but no unrelated preexisting job. | 44-FCR, `Execution sampling` | 44-SMP-057 | S4 | pending | pending | pending | pending |
| 44-SMP-059 | In a queued component with eligible but never-processed jobs, successful proper-subset selected execution sets the component to sampled. | 44-FCR, `Execution sampling` | 44-SMP-057 | S4 | pending | pending | pending | pending |
| 44-SMP-060 | In that queued component, successful selected execution that covers all eligible work and leaves none unprocessed promotes directly to done. | 44-FCR, `Execution sampling` | 44-SMP-059 | S4 | pending | pending | pending | pending |
| 44-SMP-061 | Successful selected execution in an already sampled component remains sampled until cumulative successful coverage is complete with no unprocessed work, then promotes to done under ordinary stability rules. | 44-FCR, `Execution sampling` | 44-SMP-055, 44-SMP-057 | S4 | pending | pending | pending | pending |
| 44-SMP-062 | Successful selected execution in a done component preserves done. | 44-FCR, `Execution sampling` | 44-SMP-057 | S4 | pending | pending | pending | pending |
| 44-SMP-063 | A failed or cancelled selected rerun in a done component makes the component failed; later successful repair recalculates done. | 44-FCR, `Execution sampling` | 44-CMP-020, 44-SMP-062 | S4 | pending | pending | pending | pending |
| 44-SMP-064 | Failure during a sampled component makes it failed. | 44-FCR, `Execution sampling` | 44-SMP-041 | S4 | pending | pending | pending | pending |
| 44-SMP-065 | Repair after sampled failure returns to sampled unless complete eligible coverage has succeeded, in which case it becomes done. | 44-FCR, `Execution sampling` | 44-SMP-064 | S4 | pending | pending | pending | pending |
| 44-SMP-066 | Misalignment stays independent of selected-job lifecycle transitions, and selected-job operations never clear it. | 44-FCR, `Execution sampling` | 44-PRP-033, 44-MIS-020, 44-MIS-029 | S4 | pending | pending | pending | pending |

### Ordinary interrupt boundaries

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-BND-001 | A raw node may declare `interrupt=True` alongside router configuration. | 44-FCR, `Ordinary interrupt boundaries` | 0.6.1 router configuration | S5 | pending | pending | pending | pending |
| 44-BND-002 | One `interrupt=True` declaration classifies the entire Hoeflein component as interrupt-capable. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-001, 44-CMP-001 | S5 | pending | pending | pending | pending |
| 44-BND-003 | Interrupt classification does not spread to ancestor or descendant components. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-002 | S5 | pending | pending | pending | pending |
| 44-BND-004 | Waiting stays raw-node scoped while interrupt classification is component scoped. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-002, 44-CMP-015 | S5 | pending | pending | pending | pending |
| 44-BND-005 | Without `--interrupt`, an interrupt component is ordinary work subject to normal readiness. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-002 | S5 | pending | pending | pending | pending |
| 44-BND-006 | Before any of the six executing graph commands mutates state, preflight every reachable interrupt component in its selection. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-005, 44-CMD-002 through 44-CMD-007 | S5 | pending | pending | pending | pending |
| 44-BND-007 | Interactive preflight offers run all normally, stop before all, or decide individually. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-006 | S5 | pending | pending | pending | pending |
| 44-BND-008 | Individual interactive decisions occur in reachability order. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-007 | S5 | pending | pending | pending | pending |
| 44-BND-009 | Do not ask about a later interrupt component made unreachable by an earlier stop decision. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-008 | S5 | pending | pending | pending | pending |
| 44-BND-010 | The noninteractive policy interface is exactly `--interrupt-policy run-all\|stop-all\|individual`. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-006 | S5 | pending | pending | pending | pending |
| 44-BND-011 | The noninteractive individual-choice interface is repeatable `--interrupt-choice <raw-node>=run\|stop`. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-010 | S5 | pending | pending | pending | pending |
| 44-BND-012 | Only reachable interrupt components require choices. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-006 | S5 | pending | pending | pending | pending |
| 44-BND-013 | Ignore supplied choices for unreachable interrupt components with a clear notice. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-012 | S5 | pending | pending | pending | pending |
| 44-BND-014 | Under `individual`, a missing reachable choice is an error. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-010 through 44-BND-012 | S5 | pending | pending | pending | pending |
| 44-BND-015 | Resolve every required interrupt boundary decision before mutation. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-006 through 44-BND-014 | S5 | pending | pending | pending | pending |
| 44-BND-016 | Each choice key is a raw-node name resolved to its Hoeflein component. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-011 | S5 | pending | pending | pending | pending |
| 44-BND-017 | Plans and prompts display an interrupt component as its sorted raw-node member set. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-016 | S5 | pending | pending | pending | pending |
| 44-BND-018 | Plans and prompts use the lexicographically first raw-node name as the canonical replay key. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-017 | S5 | pending | pending | pending | pending |
| 44-BND-019 | Several member-name choices that resolve to one component and agree collapse to one decision. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-016 | S5 | pending | pending | pending | pending |
| 44-BND-020 | Conflicting member-name decisions for one component are an error. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-016 | S5 | pending | pending | pending | pending |
| 44-BND-021 | With explicit `--interrupt`, exclude only the explicitly named starting component from ordinary interrupt preflight. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-006, 44-INT-001 | S5 | pending | pending | pending | pending |
| 44-BND-022 | Apply the same run-all, stop-all, or individual policy to every later reachable interrupt component. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-021 | S5 | pending | pending | pending | pending |
| 44-BND-023 | A later interrupt-classified component never starts another interrupt session merely because the starting component used `--interrupt`. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-022 | S5 | pending | pending | pending | pending |
| 44-BND-024 | Stopping before an interrupt component creates no skipped state. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-007 | S5 | pending | pending | pending | pending |
| 44-BND-025 | Unrelated runnable selected work may finish after a stop decision. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-024 | S5 | pending | pending | pending | pending |
| 44-BND-026 | When no selected work remains runnable, the main session ends with non-failure outcome `stopped`. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-024, 44-SES-001 | S5 | pending | pending | pending | pending |
| 44-BND-027 | The stopped main session records every interrupt boundary that left selected work unreachable or unrunnable. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-026 | S5 | pending | pending | pending | pending |
| 44-BND-028 | The stopped main session releases the one-main-session slot. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-026, 44-SES-001 | S5 | pending | pending | pending | pending |
| 44-BND-029 | A later explicit interruption is standalone unless it pauses a session that remains active. | 44-FCR, `Ordinary interrupt boundaries` | 44-BND-028, 44-SES-006 | S5 | pending | pending | pending | pending |

### Explicit interruption

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-INT-001 | `run`, `runfrom`, `runbetween`, `resume`, `resumefrom`, and `resumebetween` accept `--interrupt`. | 44-FCR, `Explicit interruption` | 44-CMD-002 through 44-CMD-007 | S5 | pending | pending | pending | pending |
| 44-INT-002 | The explicitly named starting component must be interrupt-classified. | 44-FCR, `Explicit interruption` | 44-BND-002 | S5 | pending | pending | pending | pending |
| 44-INT-003 | `--interrupt` applies only to the explicitly named starting component. | 44-FCR, `Explicit interruption` | 44-INT-002 | S5 | pending | pending | pending | pending |
| 44-INT-004 | `--interrupt` does not override readiness or conflicts at descendant components. | 44-FCR, `Explicit interruption` | 44-INT-003 | S5 | pending | pending | pending | pending |
| 44-INT-005 | A misaligned starting component rejects every interrupt resume form. | 44-FCR, `Explicit interruption` | 44-INT-001, 44-MIS-020, 44-MIS-022 | S5 | pending | pending | pending | pending |
| 44-INT-006 | An explicit interrupt may start when no main session is active. | 44-FCR, `Explicit interruption` | 44-SES-001 | S5 | pending | pending | pending | pending |
| 44-INT-007 | An explicit interrupt may start alongside the one permitted main session. | 44-FCR, `Explicit interruption` | 44-SES-001, 44-SES-002 | S5 | pending | pending | pending | pending |
| 44-INT-008 | An explicit interrupt ignores ordinary predecessor completion only for its starting component. | 44-FCR, `Explicit interruption` | 44-INT-003 | S5 | pending | pending | pending | pending |
| 44-INT-009 | When direct predecessors have active MWF-managed work, interrupt those direct predecessors and no more distant work. | 44-FCR, `Explicit interruption` | 44-INT-008 | S5 | pending | pending | pending | pending |
| 44-INT-010 | Stop admitting new jobs in the active direct predecessors. | 44-FCR, `Explicit interruption` | 44-INT-009 | S5 | pending | pending | pending | pending |
| 44-INT-011 | Pause predecessor MWF-managed work at safe cooperative points. | 44-FCR, `Explicit interruption` | 44-INT-009 | S5 | pending | pending | pending | pending |
| 44-INT-012 | Barricade managed files and jobs from entering the interrupt target. | 44-FCR, `Explicit interruption` | 44-INT-009 | S5 | pending | pending | pending | pending |
| 44-INT-013 | Run the target against the frozen input snapshot. | 44-FCR, `Explicit interruption` | 44-INT-010 through 44-INT-012 | S5 | pending | pending | pending | pending |
| 44-INT-014 | Release predecessor holds as soon as the target finishes or fails, even if interrupt-session descendants are still running. | 44-FCR, `Explicit interruption` | 44-INT-013 | S5 | pending | pending | pending | pending |
| 44-INT-015 | MWF does not claim to suspend an arbitrary Python thread or external request halfway through an instruction. | 44-FCR, `Explicit interruption` | none | S5 | pending | pending | pending | pending |
| 44-INT-016 | Immediate framework interruption means safe-point pausing plus publication barricading. | 44-FCR, `Explicit interruption` | 44-INT-011, 44-INT-012, 44-INT-015 | S5 | pending | pending | pending | pending |
| 44-INT-017 | After holds release, predecessor work continues and the target accepts new input. | 44-FCR, `Explicit interruption` | 44-INT-014 | S5 | pending | pending | pending | pending |
| 44-INT-018 | The interrupt target never reruns automatically after predecessor work resumes. | 44-FCR, `Explicit interruption` | 44-INT-017 | S5 | pending | pending | pending | pending |
| 44-INT-019 | The first new managed arrival after release makes the target result misaligned. | 44-FCR, `Explicit interruption` | 44-INT-017, 44-MIS-004 | S5 | pending | pending | pending | pending |
| 44-INT-020 | A later explicit interruption may rerun the target under a fresh interrupt-session ID. | 44-FCR, `Explicit interruption` | 44-INT-018, 44-INT-031 | S5 | pending | pending | pending | pending |
| 44-INT-021 | A successful explicit interrupt that used the readiness override or produced a sampled result creates a post-interrupt execution fence. | 44-FCR, `Explicit interruption` | 44-INT-008, 44-SMP-041 | S5 | pending | pending | pending | pending |
| 44-INT-022 | `mwf run I --interrupt` authorizes only I and does not let an older main session cross the partial result or rerun I after arrivals. | 44-FCR, `Explicit interruption` | 44-INT-021 | S5 | pending | pending | pending | pending |
| 44-INT-023 | `runfrom I --interrupt` and `runbetween I B --interrupt` authorize only their selected descendants within that interrupt session. | 44-FCR, `Explicit interruption` | 44-INT-021, 44-CMD-014, 44-CMD-017 | S5 | pending | pending | pending | pending |
| 44-INT-024 | Later progression through a fenced result requires another explicit command. | 44-FCR, `Explicit interruption` | 44-INT-021 | S5 | pending | pending | pending | pending |
| 44-INT-025 | If the starting component already meets ordinary readiness, `--interrupt` adds no data instability or partial-result fence. | 44-FCR, `Explicit interruption` | 44-INT-021 | S5 | pending | pending | pending | pending |
| 44-INT-026 | With ordinary readiness, all stable direct predecessors produce a stable result. | 44-FCR, `Explicit interruption` | 44-INT-025 | S5 | pending | pending | pending | pending |
| 44-INT-027 | With ordinary readiness, complete unstable direct predecessors sharing one exact origin preserve that origin. | 44-FCR, `Explicit interruption` | 44-INT-025, 44-CMP-005 | S5 | pending | pending | pending | pending |
| 44-INT-028 | Apart from its separate interrupt session, an ordinarily ready interrupt follows the corresponding command's downstream behavior. | 44-FCR, `Explicit interruption` | 44-INT-025 | S5 | pending | pending | pending | pending |
| 44-INT-029 | Every explicit interruption receives a fresh opaque ID, preferably a UUID, even when command and input match an earlier run. | 44-FCR, `Explicit interruption` | 44-SES-030, 44-SES-031 | S5 | pending | pending | pending | pending |
| 44-INT-030 | Record input snapshot hashes or revisions separately from interrupt-session identity. | 44-FCR, `Explicit interruption` | 44-INT-013, 44-INT-029 | S5 | pending | pending | pending | pending |
| 44-INT-031 | Never derive interrupt-session identity from content. | 44-FCR, `Explicit interruption` | 44-INT-029, 44-INT-030 | S5 | pending | pending | pending | pending |
| 44-INT-032 | When readiness does not hold and the override is used, a successful full target result is unstable with the new session ID. | 44-FCR, `Explicit interruption` | 44-INT-008, 44-INT-029 | S5 | pending | pending | pending | pending |
| 44-INT-033 | A partial interrupted execution remains sampled and carries the new interrupt-session origin. | 44-FCR, `Explicit interruption` | 44-SMP-041, 44-INT-029 | S5 | pending | pending | pending | pending |
| 44-INT-034 | All stable direct-parent results permit stable ordinary propagation. | 44-FCR, `Explicit interruption` | 44-CMP-004 | S4 | pending | pending | pending | pending |
| 44-INT-035 | All unstable direct-parent results with the same exact origin permit unstable propagation with that origin. | 44-FCR, `Explicit interruption` | 44-CMP-005 | S4 | pending | pending | pending | pending |
| 44-INT-036 | Mixed stable and unstable direct-parent results create an instability conflict. | 44-FCR, `Explicit interruption` | 44-CMP-004 | S4 | pending | pending | pending | pending |
| 44-INT-037 | Unstable direct-parent results with different origins create an instability conflict. | 44-FCR, `Explicit interruption` | 44-CMP-005 | S4 | pending | pending | pending | pending |
| 44-INT-038 | An instability conflict blocks ordinary execution, including at a distant merge. | 44-FCR, `Explicit interruption` | 44-INT-036, 44-INT-037 | S4 | pending | pending | pending | pending |
| 44-INT-039 | At an interrupt-classified blocked start, explicit interruption may replace conflicting input origins with one new session origin. | 44-FCR, `Explicit interruption` | 44-INT-002, 44-INT-038 | S5 | pending | pending | pending | pending |
| 44-INT-040 | Trace data retains the input origins replaced by an explicit interrupt. | 44-FCR, `Explicit interruption` | 44-INT-039 | S6 | pending | pending | pending | pending |
| 44-INT-041 | A component with no direct predecessor components has no stability conflict and remains ordinarily runnable. | 44-FCR, `Explicit interruption` | 44-INT-036, 44-INT-037 | S4 | pending | pending | pending | pending |
| 44-INT-042 | If an interrupt target fails, mark its component failed. | 44-FCR, `Explicit interruption` | 44-CMP-003 | S5 | pending | pending | pending | pending |
| 44-INT-043 | Run no descendants from a failed interrupt target. | 44-FCR, `Explicit interruption` | 44-INT-042 | S5 | pending | pending | pending | pending |
| 44-INT-044 | Release all target holds after interrupt failure. | 44-FCR, `Explicit interruption` | 44-INT-042 | S5 | pending | pending | pending | pending |
| 44-INT-045 | Retain the failed interrupt session for tracing without assigning a successful instability origin. | 44-FCR, `Explicit interruption` | 44-INT-042, 44-INT-029 | S5 | pending | pending | pending | pending |
| 44-INT-046 | Later predecessor publications make the failed interrupt target misaligned. | 44-FCR, `Explicit interruption` | 44-INT-042, 44-MIS-004 | S5 | pending | pending | pending | pending |
| 44-INT-047 | A retry after interrupt failure starts a fresh interrupt session with a fresh ID. | 44-FCR, `Explicit interruption` | 44-INT-042, 44-INT-029 | S5 | pending | pending | pending | pending |
| 44-INT-048 | Persist holds with heartbeats or leases. | 44-FCR, `Explicit interruption` | 44-SES-030, 44-SES-031 | S2 | pending | pending | pending | pending |
| 44-INT-049 | On the next mutating non-preview MWF startup or an applied `recover`, automatically identify abandoned sessions. | 44-FCR, `Explicit interruption` | 44-INT-048, 44-SES-030, 44-SES-045 | S5 | pending | pending | pending | pending |
| 44-INT-050 | Automatic abandoned-session handling releases stale holds, records abandoned or failed outcome, and recovers affected jobs under recovery rules. | 44-FCR, `Explicit interruption` | 44-INT-049 | S5 | pending | pending | pending | pending |
| 44-INT-051 | A read-only command reports stale sessions and proposed recovery without changing anything. | 44-FCR, `Explicit interruption` | 44-INT-049, 44-CMD-039 | S6 | pending | pending | pending | pending |
| 44-INT-052 | Stale interrupt state does not require manual restoration. | 44-FCR, `Explicit interruption` | 44-INT-049, 44-INT-050 | S5 | pending | pending | pending | pending |

### Several execution sessions and ownership

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-SES-001 | Allow at most one live main session. | 44-FCR, `Several execution sessions and ownership` | none | S2 | pending | pending | pending | pending |
| 44-SES-002 | Allow several live interrupt sessions. | 44-FCR, `Several execution sessions and ownership` | 44-SES-001 | S2 | pending | pending | pending | pending |
| 44-SES-003 | An interrupt session reserves its full selected component execution scope before mutation. | 44-FCR, `Several execution sessions and ownership` | 44-SES-002, 44-CMD-039 | S2 | pending | pending | pending | pending |
| 44-SES-004 | Two independent live interrupt sessions may not reserve overlapping component scopes. | 44-FCR, `Several execution sessions and ownership` | 44-SES-003 | S2 | pending | pending | pending | pending |
| 44-SES-005 | Refuse the second overlapping independent interrupt before mutation and list overlapping components and owning sessions. | 44-FCR, `Several execution sessions and ownership` | 44-SES-004 | S5 | pending | pending | pending | pending |
| 44-SES-006 | Defined parent-child interruption is the only overlap exception. | 44-FCR, `Several execution sessions and ownership` | 44-SES-004 | S2 | pending | pending | pending | pending |
| 44-SES-007 | A child may interrupt direct-predecessor work owned by the main session or another interrupt session. | 44-FCR, `Several execution sessions and ownership` | 44-SES-006, 44-INT-009 | S5 | pending | pending | pending | pending |
| 44-SES-008 | Before child mutation, the parent yields the child's transferred component scope and cannot claim work there. | 44-FCR, `Several execution sessions and ownership` | 44-SES-007 | S5 | pending | pending | pending | pending |
| 44-SES-009 | The child temporarily owns the transferred component scope. | 44-FCR, `Several execution sessions and ownership` | 44-SES-008 | S5 | pending | pending | pending | pending |
| 44-SES-010 | When the child ends, remaining scope returns to the parent. | 44-FCR, `Several execution sessions and ownership` | 44-SES-009 | S5 | pending | pending | pending | pending |
| 44-SES-011 | The parent observes child-completed work and does not rerun it automatically. | 44-FCR, `Several execution sessions and ownership` | 44-SES-010 | S5 | pending | pending | pending | pending |
| 44-SES-012 | Overlapping holds use reference counts. | 44-FCR, `Several execution sessions and ownership` | 44-SES-006, 44-INT-048 | S2 | pending | pending | pending | pending |
| 44-SES-013 | A main session may yield future admission over an overlapping interrupt selection when no direct predecessor is actively paused. | 44-FCR, `Several execution sessions and ownership` | 44-SES-003 | S5 | pending | pending | pending | pending |
| 44-SES-014 | Refuse a future-admission transfer before mutation when the main session has an active claim inside the transferred scope. | 44-FCR, `Several execution sessions and ownership` | 44-SES-013 | S5 | pending | pending | pending | pending |
| 44-SES-015 | Safe-point interruption of direct predecessors does not authorize resetting an active target or descendant. | 44-FCR, `Several execution sessions and ownership` | 44-SES-014, 44-INT-011 | S5 | pending | pending | pending | pending |
| 44-SES-016 | A future-admission transfer does not create a parent-session ID by itself. | 44-FCR, `Several execution sessions and ownership` | 44-SES-013 | S5 | pending | pending | pending | pending |
| 44-SES-017 | A future-admission transfer prevents joint ownership. | 44-FCR, `Several execution sessions and ownership` | 44-SES-013 | S5 | pending | pending | pending | pending |
| 44-SES-018 | After an ordinarily ready interrupt without a partial-result fence, the main session may continue from its result. | 44-FCR, `Several execution sessions and ownership` | 44-INT-025, 44-SES-013 | S5 | pending | pending | pending | pending |
| 44-SES-019 | When a fence exists, the main session records the boundary and does not cross it automatically. | 44-FCR, `Several execution sessions and ownership` | 44-INT-021 | S5 | pending | pending | pending | pending |
| 44-SES-020 | A second independent request targeting a component reserved by a live interrupt session is refused before mutation. | 44-FCR, `Several execution sessions and ownership` | 44-SES-003, 44-SES-006 | S5 | pending | pending | pending | pending |
| 44-SES-021 | A refused independent request is neither queued nor serialized. | 44-FCR, `Several execution sessions and ownership` | 44-SES-020 | S5 | pending | pending | pending | pending |
| 44-SES-022 | Session scheduling and parent-child transfer do not change the quotient DAG or create an interrupt subgraph. | 44-FCR, `Several execution sessions and ownership` | 44-SES-006 | S5 | pending | pending | pending | pending |
| 44-SES-023 | A new main command compares its selected execution scope and complete reset-like downstream mutation footprint with every live interrupt reservation and guard. | 44-FCR, `Several execution sessions and ownership` | 44-SES-003, 44-PRP-036, 44-PRP-037 | S3 | pending | pending | pending | pending |
| 44-SES-024 | Disjoint main and interrupt work may coexist. | 44-FCR, `Several execution sessions and ownership` | 44-SES-023 | S5 | pending | pending | pending | pending |
| 44-SES-025 | On overlap, a new main command refuses before mutation and names the conflicting session and components. | 44-FCR, `Several execution sessions and ownership` | 44-SES-023 | S5 | pending | pending | pending | pending |
| 44-SES-026 | An ordinary main command never preempts a live interrupt or defers fresh preparation underneath it. | 44-FCR, `Several execution sessions and ownership` | 44-SES-025 | S5 | pending | pending | pending | pending |
| 44-SES-027 | Every claimed job records exactly one owning execution-session ID. | 44-FCR, `Several execution sessions and ownership` | 44-SES-001, 44-SES-002 | S2 | pending | pending | pending | pending |
| 44-SES-028 | Schedulers claim only work inside their reserved scope and never guess ownership. | 44-FCR, `Several execution sessions and ownership` | 44-SES-003, 44-SES-027 | S2 | pending | pending | pending | pending |
| 44-SES-029 | A job passing through a nested transfer records the exact session that claimed that execution. | 44-FCR, `Several execution sessions and ownership` | 44-SES-009, 44-SES-027 | S5 | pending | pending | pending | pending |
| 44-SES-030 | Replace authoritative `.mwf/run.json` state with one SQLite execution-session registry. | 44-FCR, `Several execution sessions and ownership` | published 0.6.1 storage | S2 | pending | pending | pending | pending |
| 44-SES-031 | The registry represents the main session, every interrupt session, actual parent relationships, IDs, commands, starting and selected components, selected jobs, reservations, holds, heartbeats, terminal outcomes, and failures. | 44-FCR, `Several execution sessions and ownership` | 44-SES-030 | S2 | pending | pending | pending | pending |
| 44-SES-032 | SQLite is the only writable session-state source. | 44-FCR, `Several execution sessions and ownership` | 44-SES-030 | S2 | pending | pending | pending | pending |
| 44-SES-033 | Import safe legacy run information once during migration. | 44-FCR, `Several execution sessions and ownership` | 44-SES-030 | S2 | pending | pending | pending | pending |
| 44-SES-034 | Never migrate session state underneath a live legacy process. | 44-FCR, `Several execution sessions and ownership` | 44-SES-033 | S2 | pending | pending | pending | pending |
| 44-SES-035 | An API defined to read the main session returns the main session or no result. | 44-FCR, `Several execution sessions and ownership` | 44-SES-030 | S2 | pending | pending | pending | pending |
| 44-SES-036 | A legacy generic single-session reader returns a live main session when one exists. | 44-FCR, `Several execution sessions and ownership` | 44-SES-035 | S2 | pending | pending | pending | pending |
| 44-SES-037 | With no main and exactly one live interrupt, the legacy generic reader returns that interrupt. | 44-FCR, `Several execution sessions and ownership` | 44-SES-036 | S2 | pending | pending | pending | pending |
| 44-SES-038 | With no live session, the legacy generic reader returns no result. | 44-FCR, `Several execution sessions and ownership` | 44-SES-036 | S2 | pending | pending | pending | pending |
| 44-SES-039 | With several live interrupts and no main, the generic reader reports ambiguity and requires an explicit session ID. | 44-FCR, `Several execution sessions and ownership` | 44-SES-036 | S2 | pending | pending | pending | pending |
| 44-SES-040 | The generic reader never chooses a session heuristically. | 44-FCR, `Several execution sessions and ownership` | 44-SES-036 through 44-SES-039 | S2 | pending | pending | pending | pending |
| 44-SES-041 | Monitoring and multi-session APIs list exact sessions. | 44-FCR, `Several execution sessions and ownership` | 44-SES-030 | S6 | pending | pending | pending | pending |
| 44-SES-042 | `restart NODE job ID` uses the job's recorded owning session. | 44-FCR, `Several execution sessions and ownership` | 44-SES-027 | S5 | pending | pending | pending | pending |
| 44-SES-043 | `restart NODE` works when all affected jobs resolve to one owner. | 44-FCR, `Several execution sessions and ownership` | 44-SES-042 | S5 | pending | pending | pending | pending |
| 44-SES-044 | If damaged or legacy restart data yields several possible owners, refuse and report ambiguity rather than guessing. | 44-FCR, `Several execution sessions and ownership` | 44-SES-042, 44-SES-043 | S5 | pending | pending | pending | pending |
| 44-SES-045 | One applied `recover` invocation processes every stale session. | 44-FCR, `Several execution sessions and ownership` | 44-SES-030 | S5 | pending | pending | pending | pending |
| 44-SES-046 | Recovery releases each stale session's holds, records its outcome, and performs applicable job recovery. | 44-FCR, `Several execution sessions and ownership` | 44-SES-045 | S5 | pending | pending | pending | pending |
| 44-SES-047 | `recover --dry-run` previews the same complete stale-session set without mutation. | 44-FCR, `Several execution sessions and ownership` | 44-SES-045, 44-CMD-039 | S5 | pending | pending | pending | pending |
| 44-SES-048 | A failure recovering one stale session is reported without hiding the others. | 44-FCR, `Several execution sessions and ownership` | 44-SES-045 | S5 | pending | pending | pending | pending |
| 44-SES-049 | `mwf threads NODE VALUE` updates the live session that currently owns NODE. | 44-FCR, `Several execution sessions and ownership` | 44-SES-027 | S5 | pending | pending | pending | pending |
| 44-SES-050 | If no live session owns NODE, store a pending node override for the next session that claims it. | 44-FCR, `Several execution sessions and ownership` | 44-SES-049 | S5 | pending | pending | pending | pending |
| 44-SES-051 | If damaged state reports several owners for NODE, the threads command refuses. | 44-FCR, `Several execution sessions and ownership` | 44-SES-049 | S5 | pending | pending | pending | pending |
| 44-SES-052 | `mwf threads NODE reset` and read-only `mwf threads NODE` resolve the same exact live owner or pending override. | 44-FCR, `Several execution sessions and ownership` | 44-SES-049, 44-SES-050 | S5 | pending | pending | pending | pending |
| 44-SES-053 | Do not add a new thread-command form for ownership behavior. | 44-FCR, `Several execution sessions and ownership` | 44-SES-049 through 44-SES-052 | S5 | pending | pending | pending | pending |
| 44-SES-054 | Keep `mwf threads --api-total VALUE` parser-compatible and functional. | 44-FCR, `Several execution sessions and ownership` | 0.6.1 behavior | S5 | pending | pending | pending | pending |
| 44-SES-055 | Mark `--api-total` deprecated in help and print a deprecation warning when used. | 44-FCR, `Several execution sessions and ownership` | 44-SES-054 | S6 | pending | pending | pending | pending |
| 44-SES-056 | `--api-total` remains one project-wide aggregate API admission cap shared by all active sessions. | 44-FCR, `Several execution sessions and ownership` | 44-SES-054 | S5 | pending | pending | pending | pending |
| 44-SES-057 | Do not add a session-specific aggregate API-limit form or a scheduled removal date in 0.6.2. | 44-FCR, `Several execution sessions and ownership` | 44-SES-054 | ALL | pending | pending | pending | pending |
| 44-SES-058 | Revisit the aggregate API limit only if real use later demands a replacement. | 44-FCR, `Several execution sessions and ownership` | 44-SES-057 | ALL | pending | pending | pending | pending |

### Misalignment

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-MIS-001 | New MWF-managed input or jobs set the component misalignment Boolean when the component is done. | 44-FCR, `Misalignment` | 44-CMP-010 | S3 | pending | pending | pending | pending |
| 44-MIS-002 | New MWF-managed input or jobs set misalignment when the component is sampled. | 44-FCR, `Misalignment` | 44-CMP-010 | S3 | pending | pending | pending | pending |
| 44-MIS-003 | New MWF-managed input or jobs set misalignment when the component is failed. | 44-FCR, `Misalignment` | 44-CMP-010 | S3 | pending | pending | pending | pending |
| 44-MIS-004 | A reset or fresh-preparation path sets misalignment when it changes managed producer-owned input or jobs in an unselected done, sampled, or failed component. | 44-FCR, `Misalignment` | 44-PRP-022, 44-PRP-026 | S3 | pending | pending | pending | pending |
| 44-MIS-005 | Queued and running components do not become misaligned from arrivals. | 44-FCR, `Misalignment` | 44-CMP-003 | S3 | pending | pending | pending | pending |
| 44-MIS-006 | Refusal and guard rules protect active receivers from preparation-driven changes. | 44-FCR, `Misalignment` | 44-PRP-041 through 44-PRP-044 | S3 | pending | pending | pending | pending |
| 44-MIS-007 | Manual filesystem edits remain the actor's responsibility and trigger no scan or hash detection. | 44-FCR, `Misalignment` | none | S3 | pending | pending | pending | pending |
| 44-MIS-008 | Misalignment does not propagate merely through graph position. | 44-FCR, `Misalignment` | 44-CMP-010 | S3 | pending | pending | pending | pending |
| 44-MIS-009 | A descendant becomes misaligned only when its own managed input or job set changes. | 44-FCR, `Misalignment` | 44-MIS-008 | S3 | pending | pending | pending | pending |
| 44-MIS-010 | One atomic component compare-and-set performs the false-to-true misalignment transition. | 44-FCR, `Misalignment` | 44-CMP-025 | S3 | pending | pending | pending | pending |
| 44-MIS-011 | A uniqueness rule on receiving raw node plus alignment generation permits one first-cause record per receiving raw node without rewriting component state. | 44-FCR, `Misalignment` | 44-MIS-010, 44-CMP-025 | S3 | pending | pending | pending | pending |
| 44-MIS-012 | An arrival first-cause records producer node, producer job when known, arrival kind, and first path or job information. | 44-FCR, `Misalignment` | 44-MIS-011 | S3 | pending | pending | pending | pending |
| 44-MIS-013 | A preparation-driven first-cause records `preparation-removal` or `preparation-change`, operation, producer, exact action, affected kind, and one representative path or job. | 44-FCR, `Misalignment` | 44-MIS-011, 44-PRP-027 through 44-PRP-031 | S3 | pending | pending | pending | pending |
| 44-MIS-014 | Batch arrival and preparation-driven causes once per receiver rather than once per affected item. | 44-FCR, `Misalignment` | 44-MIS-011 | S3 | pending | pending | pending | pending |
| 44-MIS-015 | Cache a receiver check only against the exact established-result generation or equivalent terminal-result identity. | 44-FCR, `Misalignment` | 44-MIS-011 | S3 | pending | pending | pending | pending |
| 44-MIS-016 | A queued or running no-op check must not suppress the first eligible check after the receiver later becomes sampled, done, or failed in the same session. | 44-FCR, `Misalignment` | 44-MIS-005, 44-MIS-015 | S3 | pending | pending | pending | pending |
| 44-MIS-017 | Batch APIs signal once per receiver batch. | 44-FCR, `Misalignment` | 44-MIS-014 | S3 | pending | pending | pending | pending |
| 44-MIS-018 | Cross-process races produce one durable winner and then latch locally. | 44-FCR, `Misalignment` | 44-MIS-010, 44-MIS-011 | S3 | pending | pending | pending | pending |
| 44-MIS-019 | Persistent misalignment work scales with sessions, receiving raw nodes, and established results, not with every file or job. | 44-FCR, `Misalignment` | 44-MIS-014 through 44-MIS-018 | S3 | pending | pending | pending | pending |
| 44-MIS-020 | A misalignment conflict means a resume selection encountered a misaligned component. | 44-FCR, `Misalignment` | 44-CMP-010 | S3 | pending | pending | pending | pending |
| 44-MIS-021 | A misalignment conflict is distinct from an instability conflict. | 44-FCR, `Misalignment` | 44-MIS-020, 44-INT-036, 44-INT-037 | S3 | pending | pending | pending | pending |
| 44-MIS-022 | Preflight blocks `resume`, `resumefrom`, and `resumebetween`, including their interrupt forms, before mutation when the selection has a misalignment conflict. | 44-FCR, `Misalignment` | 44-MIS-020, 44-CMD-005 through 44-CMD-007 | S3 | pending | pending | pending | pending |
| 44-MIS-023 | A full component run or reset can repair alignment. | 44-FCR, `Misalignment` | 44-CMD-002, 44-CMD-008 | S3 | pending | pending | pending | pending |
| 44-MIS-024 | Clear misalignment once in shared fresh preparation only after that component's preparation succeeds, then advance alignment generation. | 44-FCR, `Misalignment` | 44-MIS-023, 44-CMD-011 | S3 | pending | pending | pending | pending |
| 44-MIS-025 | During a wide selection, clear each component immediately after its own preparation succeeds. | 44-FCR, `Misalignment` | 44-MIS-024 | S3 | pending | pending | pending | pending |
| 44-MIS-026 | If later preparation fails, already prepared components remain aligned and queued while unprepared components retain earlier state. | 44-FCR, `Misalignment` | 44-MIS-025 | S3 | pending | pending | pending | pending |
| 44-MIS-027 | Shared repair behavior covers all full run and reset selections without separate command implementations. | 44-FCR, `Misalignment` | 44-CMD-011, 44-MIS-024 | S3 | pending | pending | pending | pending |
| 44-MIS-028 | Resetting a done, sampled, or failed component leaves it queued. | 44-FCR, `Misalignment` | 44-MIS-023 | S3 | pending | pending | pending | pending |
| 44-MIS-029 | Selected-job run or reset never clears component-wide misalignment. | 44-FCR, `Misalignment` | 44-PRP-033 | S3 | pending | pending | pending | pending |
| 44-MIS-030 | For `resume C` blocked by misaligned C, suggest `mwf run C`. | 44-FCR, `Misalignment` | 44-MIS-022 | S6 | pending | pending | pending | pending |
| 44-MIS-031 | For `resumefrom B` blocked by misaligned C, suggest `mwf resetfrom C`, then retry `mwf resumefrom B`. | 44-FCR, `Misalignment` | 44-MIS-022 | S6 | pending | pending | pending | pending |
| 44-MIS-032 | For `resumebetween B E` blocked by misaligned C, suggest `mwf resetbetween C E`, then retry. | 44-FCR, `Misalignment` | 44-MIS-022 | S6 | pending | pending | pending | pending |
| 44-MIS-033 | Recommend several commands for separate misaligned branches rather than adding a multi-start command. | 44-FCR, `Misalignment` | 44-MIS-030 through 44-MIS-032 | S6 | pending | pending | pending | pending |

### Component membership changes

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-MEM-001 | Use a deterministic component key based on exact sorted raw-node membership. | 44-FCR, `Component membership changes` | 44-CMP-001 | S2 | pending | pending | pending | pending |
| 44-MEM-002 | Associate persisted component state with the graph shape that produced it. | 44-FCR, `Component membership changes` | 44-MEM-001, 44-CMP-025 | S2 | pending | pending | pending | pending |
| 44-MEM-003 | If membership changes while reusable history exists, execution touching the affected area refuses until fresh preparation resolves the ambiguity. | 44-FCR, `Component membership changes` | 44-MEM-002 | S3 | pending | pending | pending | pending |
| 44-MEM-004 | A targeted reset is sufficient to repair a membership change. | 44-FCR, `Component membership changes` | 44-MEM-003 | S3 | pending | pending | pending | pending |
| 44-MEM-005 | Expand membership repair through the transitive overlap of stored and current component memberships. | 44-FCR, `Component membership changes` | 44-MEM-004 | S3 | pending | pending | pending | pending |
| 44-MEM-006 | If stored `{A, B}` becomes current `{A}` and `{B}`, `mwf reset A` also prepares `{B}`. | 44-FCR, `Component membership changes` | 44-MEM-005 | S3 | pending | pending | pending | pending |
| 44-MEM-007 | If stored `{A}` and `{B}` becomes current `{A, B}`, resetting A prepares the new combined component. | 44-FCR, `Component membership changes` | 44-MEM-005 | S3 | pending | pending | pending | pending |
| 44-MEM-008 | Show expanded repair scope before applying it. | 44-FCR, `Component membership changes` | 44-MEM-005 | S3 | pending | pending | pending | pending |
| 44-MEM-009 | Membership repair does not reset unrelated components. | 44-FCR, `Component membership changes` | 44-MEM-005 | S3 | pending | pending | pending | pending |
| 44-MEM-010 | If no reusable work exists, reconcile membership records automatically. | 44-FCR, `Component membership changes` | 44-MEM-002 | S3 | pending | pending | pending | pending |

### Tracing

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-TRC-001 | Keep the existing detailed job trace. | 44-FCR, `Tracing` | the 0.6.1 implementation task trace behavior | S6 | pending | pending | pending | pending |
| 44-TRC-002 | Add compact nonrecursive text lineage command `mwf trace <node> job <id> --lineage`. | 44-FCR, `Tracing` | 44-TRC-001 | S6 | pending | pending | pending | pending |
| 44-TRC-003 | Add compact nonrecursive JSON lineage command `mwf trace <node> job <id> --lineage --json`. | 44-FCR, `Tracing` | 44-TRC-002 | S6 | pending | pending | pending | pending |
| 44-TRC-004 | Both lineage forms report named node and job, job status, component state, stability, exact origin, sample or interrupt identity when relevant, misalignment and first cause, creating job, and directly created jobs. | 44-FCR, `Tracing` | 44-CMP-025, 44-SES-027, 44-MIS-011, 44-SMP-029 | S6 | pending | pending | pending | pending |
| 44-TRC-005 | The JSON minimum shape is versioned. | 44-FCR, `Tracing` | 44-TRC-003 | S6 | pending | pending | pending | pending |
| 44-TRC-006 | JSON includes `schema_version`, `node`, `job_id`, and `job_status`. | 44-FCR, `Tracing` | 44-TRC-005 | S6 | pending | pending | pending | pending |
| 44-TRC-007 | JSON includes `component.members`, `component.state`, `component.stability`, and `component.instability_origin`. | 44-FCR, `Tracing` | 44-TRC-005 | S6 | pending | pending | pending | pending |
| 44-TRC-008 | JSON includes `component.misaligned` and `component.misalignment_causes`. | 44-FCR, `Tracing` | 44-TRC-005, 44-MIS-011 | S6 | pending | pending | pending | pending |
| 44-TRC-009 | JSON includes `sample_id`, `interrupt_session_id`, `created_by`, and `created_jobs`. | 44-FCR, `Tracing` | 44-TRC-005 | S6 | pending | pending | pending | pending |
| 44-TRC-010 | `component.misalignment_causes` contains every receiving-raw-node first cause for the component's current alignment generation. | 44-FCR, `Tracing` | 44-MIS-011, 44-MIS-024 | S6 | pending | pending | pending | pending |
| 44-TRC-011 | Order misalignment causes by receiver name. | 44-FCR, `Tracing` | 44-TRC-010 | S6 | pending | pending | pending | pending |
| 44-TRC-012 | Order component members and created jobs deterministically by node and job ID. | 44-FCR, `Tracing` | 44-TRC-004 | S6 | pending | pending | pending | pending |
| 44-TRC-013 | Keep sample and interrupt identities separate because both may apply. | 44-FCR, `Tracing` | 44-SMP-029, 44-INT-029 | S6 | pending | pending | pending | pending |
| 44-TRC-014 | The JSON lineage view does not silently add recursive ancestry. | 44-FCR, `Tracing` | 44-TRC-003 | S6 | pending | pending | pending | pending |
| 44-TRC-015 | Omit full context-trace objects, parameters, output bodies, retry bodies, error bodies, and timing bodies from compact lineage. | 44-FCR, `Tracing` | 44-TRC-002 | S6 | pending | pending | pending | pending |
| 44-TRC-016 | Cross-job lineage traversal uses several explicit lineage commands rather than implicit recursion. | 44-FCR, `Tracing` | 44-TRC-014 | S6 | pending | pending | pending | pending |
| 44-TRC-017 | Use `state` for quotient-component lifecycle and keep it distinct from job status. | 44-FCR, `Tracing` | 44-CMP-003 | S6 | pending | pending | pending | pending |
| 44-TRC-018 | Do not introduce `quotient-state`. | 44-FCR, `Tracing` | 44-TRC-017 | S6 | pending | pending | pending | pending |

### Report interrupt architecture

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-RPT-001 | Report interrupt components are recommended workflow architecture rather than a new special runtime type. | 44-FCR, `Report interrupt architecture` | 44-BND-002 | S6 | pending | pending | pending | pending |
| 44-RPT-002 | Give a report interrupt component the same direct predecessor components as the related semantic oasis. | 44-FCR, `Report interrupt architecture` | the shared-vocabulary decision semantic-oasis terminology | S6 | pending | pending | pending | pending |
| 44-RPT-003 | A report output may feed a short analysis branch. | 44-FCR, `Report interrupt architecture` | 44-RPT-001 | S6 | pending | pending | pending | pending |
| 44-RPT-004 | Keep the report analysis branch separate from the oasis branch, with `Desc(Report) intersect Desc(Oasis) = empty`. | 44-FCR, `Report interrupt architecture` | 44-RPT-002, 44-RPT-003 | S6 | pending | pending | pending | pending |
| 44-RPT-005 | A report component may have no descendants, one or two analysis nodes, or a small Hoeflein component where useful. | 44-FCR, `Report interrupt architecture` | 44-RPT-003 | S6 | pending | pending | pending | pending |
| 44-RPT-006 | The framework does not reject a longer report branch. | 44-FCR, `Report interrupt architecture` | 44-RPT-005 | S6 | pending | pending | pending | pending |
| 44-RPT-007 | Report-branch separation is guidance to prevent partial report data merging into the main result path. | 44-FCR, `Report interrupt architecture` | 44-RPT-004 | S6 | pending | pending | pending | pending |
| 44-RPT-008 | An actor may interrupt a report component repeatedly, and each explicit run may create a new report artifact. | 44-FCR, `Report interrupt architecture` | 44-INT-020 | S6 | pending | pending | pending | pending |
| 44-RPT-009 | A report component never reruns automatically after predecessor work resumes. | 44-FCR, `Report interrupt architecture` | 44-INT-018 | S6 | pending | pending | pending | pending |
| 44-RPT-010 | A report may resume only while aligned for the same established input, including unfinished or failed work from that input. | 44-FCR, `Report interrupt architecture` | 44-MIS-022 | S6 | pending | pending | pending | pending |
| 44-RPT-011 | Late managed input makes the report misaligned, after which every resume form refuses and a full fresh report run is required. | 44-FCR, `Report interrupt architecture` | 44-RPT-010, 44-MIS-022 | S6 | pending | pending | pending | pending |

### AFSR instructions, documentation, and skills

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-DOC-001 | MWF 0.6.2 is incomplete until runtime, AFSR instructions, documentation, CLI text, and tests agree. | 44-FCR, `AFSR instructions and documentation` | all behavioral requirements | S6 | pending | pending | pending | pending |
| 44-DOC-002 | `AGENTS.md` routes run, resume, reset, interrupt, sample, or isolate requests to `.agents/skills/mwf-run-workflow/SKILL.md`. | 44-FCR, `AFSR instructions and documentation` | 44-SCP-013 | S6 | pending | pending | pending | pending |
| 44-DOC-003 | `AGENTS.md` routes debug, diagnose, inspect-input, trace-lineage, or investigate component/session-state requests to `.agents/skills/mwf-debug-workflow/SKILL.md`. | 44-FCR, `AFSR instructions and documentation` | 44-SCP-022 | S6 | pending | pending | pending | pending |
| 44-DOC-004 | `README.md` remains the documentation entry point and routes to relevant architecture, operations, and testing documents. | 44-FCR, `AFSR instructions and documentation` | the 0.6.1 implementation task hierarchy | S6 | pending | pending | pending | pending |
| 44-DOC-005 | `CONTEXT.md` defines workflow management and subgraph management. | 44-FCR, `AFSR instructions and documentation` | 44-SCP-020 | S6 | pending | pending | pending | pending |
| 44-DOC-006 | `CONTEXT.md` defines quotient interval, execution sampling, inspection sampling, and sampled. | 44-FCR, `AFSR instructions and documentation` | 44-CMD-016, 44-SMP-041, 44-SCP-016 | S6 | pending | pending | pending | pending |
| 44-DOC-007 | `CONTEXT.md` defines tracing, full job trace, and lineage trace. | 44-FCR, `AFSR instructions and documentation` | 44-TRC-001 through 44-TRC-003 | S6 | pending | pending | pending | pending |
| 44-DOC-008 | `CONTEXT.md` defines producer-qualified input. | 44-FCR, `AFSR instructions and documentation` | 44-PRP-001 | S6 | pending | pending | pending | pending |
| 44-DOC-009 | `CONTEXT.md` defines execution session, main session, and interrupt session. | 44-FCR, `AFSR instructions and documentation` | 44-SES-001, 44-SES-002 | S6 | pending | pending | pending | pending |
| 44-DOC-010 | `CONTEXT.md` defines interrupt component, interrupt barricade, and post-interrupt fence. | 44-FCR, `AFSR instructions and documentation` | 44-BND-002, 44-INT-012, 44-INT-021 | S6 | pending | pending | pending | pending |
| 44-DOC-011 | `CONTEXT.md` defines waiting configuration, active waiting display, and autostart routing. | 44-FCR, `AFSR instructions and documentation` | 44-CMP-015 through 44-CMP-018 | S6 | pending | pending | pending | pending |
| 44-DOC-012 | `CONTEXT.md` defines stability, instability origin, and instability conflict. | 44-FCR, `AFSR instructions and documentation` | 44-CMP-004, 44-CMP-005, 44-INT-036, 44-INT-037 | S6 | pending | pending | pending | pending |
| 44-DOC-013 | `CONTEXT.md` defines misaligned, misalignment conflict, and alignment generation. | 44-FCR, `AFSR instructions and documentation` | 44-CMP-010, 44-MIS-020, 44-MIS-024 | S6 | pending | pending | pending | pending |
| 44-DOC-014 | `CONTEXT.md` defines report interrupt architecture. | 44-FCR, `AFSR instructions and documentation` | 44-RPT-001 | S6 | pending | pending | pending | pending |
| 44-DOC-015 | Revise the existing MWF run-session definition to agree with several sessions. | 44-FCR, `AFSR instructions and documentation` | 44-SES-001, 44-SES-002 | S6 | pending | pending | pending | pending |
| 44-DOC-016 | Glossary definitions remain actor-neutral and omit implementation detail. | 44-FCR, `AFSR instructions and documentation` | 44-SCP-014, the 0.6.1 implementation task hierarchy | S6 | pending | pending | pending | pending |
| 44-DOC-017 | `docs/architecture/node.md` owns producer and receiver node scope, waiting scope, and interrupt declaration scope. | 44-FCR, `AFSR instructions and documentation` | 44-PRP-001, 44-CMP-015, 44-BND-001 | S6 | pending | pending | pending | pending |
| 44-DOC-018 | `docs/architecture/task.md` owns task-facing filesystem objects and APIs, producer-qualified writes, and exact or fixed-depth input reads. | 44-FCR, `AFSR instructions and documentation` | 44-PRP-001, 44-PRP-009 | S6 | pending | pending | pending | pending |
| 44-DOC-019 | `docs/architecture/graph.md` owns quotient selections, component state, stability propagation, producer-aware preparation, interrupt scheduling, sessions, and the report-branch recommendation. | 44-FCR, `AFSR instructions and documentation` | 44-CMD-014 through 44-CMD-019, 44-CMP-025, 44-PRP-022, 44-INT-034 through 44-INT-039, 44-SES-001, 44-RPT-001 | S6 | pending | pending | pending | pending |
| 44-DOC-020 | `docs/operations.md` owns effects and plans for all nine commands and refusal modifiers. | 44-FCR, `AFSR instructions and documentation` | 44-CMD-001 through 44-CMD-048 | S6 | pending | pending | pending | pending |
| 44-DOC-021 | `docs/operations.md` owns interrupt policy and choices, session ownership and recovery, sampling and seeds, selected-job behavior, resets, monitoring, threads behavior and API-limit deprecation, and full and lineage traces. | 44-FCR, `AFSR instructions and documentation` | 44-BND-005 through 44-BND-029, 44-SES-027 through 44-SES-057, 44-SMP-001 through 44-SMP-066, 44-TRC-001 through 44-TRC-018 | S6 | pending | pending | pending | pending |
| 44-DOC-022 | `docs/testing.md`, `tests/README.md`, and affected benchmark guidance explain isolated verification and relevant regression areas. | 44-FCR, `AFSR instructions and documentation` | the 0.6.1 implementation task testing model | S6 | pending | pending | pending | pending |
| 44-DOC-023 | CLI help, descriptions, plans, monitor, inspect, top-level diagnostics, errors, and release notes use the settled terms and examples. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-005 through 44-DOC-016 | S6 | pending | pending | pending | pending |
| 44-DOC-024 | The workflow-running skill reads the AFSR and translates vague actor instructions into exact safe MWF commands. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-002, 44-SCP-013 | S6 | pending | pending | pending | pending |
| 44-DOC-025 | The workflow-running skill previews consequential selections and follows settled management behavior. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-024, 44-CMD-034 through 44-CMD-046 | S6 | pending | pending | pending | pending |
| 44-DOC-026 | The debugging skill inspects exact input paths, component and session state, filters, detailed traces, compact lineage, and related jobs through explicit repeated commands. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-003, 44-TRC-001 through 44-TRC-018 | S6 | pending | pending | pending | pending |
| 44-DOC-027 | Neither new skill searches for validation ghosts. | 44-FCR, `AFSR instructions and documentation` | 44-SCP-015 | S6 | pending | pending | pending | pending |
| 44-DOC-028 | Update existing skill `mwf-design-new-architecture` where 0.6.2 behavior changes its procedure. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-001 | S6 | pending | pending | pending | pending |
| 44-DOC-029 | Update existing skill `mwf-modify-architecture` where 0.6.2 behavior changes its procedure. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-001 | S6 | pending | pending | pending | pending |
| 44-DOC-030 | Update existing skill `mwf-analyze-architecture` where 0.6.2 behavior changes its procedure. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-001 | S6 | pending | pending | pending | pending |
| 44-DOC-031 | Update existing skill `mwf-test` where 0.6.2 behavior changes its procedure. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-001 | S6 | pending | pending | pending | pending |
| 44-DOC-032 | Update existing skill `mwf-document-workflow` where 0.6.2 behavior changes its procedure. | 44-FCR, `AFSR instructions and documentation` | 44-DOC-001 | S6 | pending | pending | pending | pending |
| 44-DOC-033 | Skills own procedures and route to the authoritative glossary and architecture documents rather than duplicating them. | 44-FCR, `AFSR instructions and documentation` | the 0.6.1 implementation task document ownership | S6 | pending | pending | pending | pending |

### Required reconciliation with current MWF

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-REC-001 | Change current sampling that bypasses ordinary predecessor readiness to the settled ordinary readiness rules. | 44-FCR, `Required reconciliation with current MWF` | 44-SMP-044 through 44-SMP-048 | S4 | pending | pending | pending | pending |
| 44-REC-002 | Change current waiting display so queued nodes are not waiting and only active waiting members of a running component display waiting. | 44-FCR, `Required reconciliation with current MWF` | 44-CMP-015 through 44-CMP-017 | S2 | pending | pending | pending | pending |
| 44-REC-003 | Eliminate divergent raw-node lifecycle authority inside one component. | 44-FCR, `Required reconciliation with current MWF` | 44-CMP-002, 44-CMP-026 | S2 | pending | pending | pending | pending |
| 44-REC-004 | Replace `.mwf/run.json` and competing-run singleton assumptions with the several-session foundation. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-001 through 44-SES-004, 44-SES-006, 44-SES-012, 44-SES-027, 44-SES-028, 44-SES-030 through 44-SES-040 | S2 | pending | pending | pending | pending |
| 44-REC-005 | Add exact execution-session ownership to job claims. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-027, 44-SES-028 | S2 | pending | pending | pending | pending |
| 44-REC-006 | Reconcile restart, recover, and runtime thread overrides with exact session ownership. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-042 through 44-SES-053 | S5 | pending | pending | pending | pending |
| 44-REC-007 | Change selected execution to run newly created same-component causal work. | 44-FCR, `Required reconciliation with current MWF` | 44-SMP-035 through 44-SMP-040 | S4 | pending | pending | pending | pending |
| 44-REC-008 | Extend input preparation to remove all producer-owned forwarded files as well as producer jobs. | 44-FCR, `Required reconciliation with current MWF` | 44-PRP-017 through 44-PRP-019 | S3 | pending | pending | pending | pending |
| 44-REC-009 | Remove planning and dry-run bootstrap mutation. | 44-FCR, `Required reconciliation with current MWF` | 44-CMD-039 through 44-CMD-046 | S1 | pending | pending | pending | pending |
| 44-REC-010 | Replace destructive reset's single-run exclusion with refusal during every live session kind. | 44-FCR, `Required reconciliation with current MWF` | 44-CMD-047, 44-CMD-048 | S3 | pending | pending | pending | pending |
| 44-REC-011 | Narrow receiving-input recursion without breaking output traversal that currently shares machinery. | 44-FCR, `Required reconciliation with current MWF` | 44-PRP-010, 44-PRP-013 | S3 | pending | pending | pending | pending |
| 44-REC-012 | Remove four obsolete commands from parser and documentation while adding between and interrupt behavior. | 44-FCR, `Required reconciliation with current MWF` | 44-CMD-025 through 44-CMD-029, 44-INT-001 | S6 | pending | pending | pending | pending |
| 44-REC-013 | Recheck available Parent Repo repositories for external executable callers of the four removed commands before implementation. | 44-FCR, `Required reconciliation with current MWF` | 44-CMD-025 | S4 | pending | pending | pending | pending |
| 44-REC-014 | Exercise real scheduler, storage, parser, monitoring, input, and trace paths. | 44-FCR, `Required reconciliation with current MWF` | all runtime requirements | ALL | pending | pending | pending | pending |
| 44-REC-015 | Regression coverage includes interval selection and the S1 read-only preview base. | 44-FCR, `Required reconciliation with current MWF` | 44-CMD-014 through 44-CMD-022, 44-CMD-031 through 44-CMD-036, 44-CMD-038 through 44-CMD-046 | S1 | pending | pending | pending | pending |
| 44-REC-016 | Regression coverage includes reset refusal during every live session kind. | 44-FCR, `Required reconciliation with current MWF` | 44-CMD-047 | S3 | pending | pending | pending | pending |
| 44-REC-017 | Regression coverage includes producer-aware reset mutations inside and outside selections. | 44-FCR, `Required reconciliation with current MWF` | 44-PRP-017 through 44-PRP-021 | S3 | pending | pending | pending | pending |
| 44-REC-018 | Regression coverage includes refusal for active excluded receivers affected by any downstream change. | 44-FCR, `Required reconciliation with current MWF` | 44-PRP-041 through 44-PRP-044 | S3 | pending | pending | pending | pending |
| 44-REC-019 | Regression coverage includes downstream file and job detection for every reset and fresh-run form, including selected-job reset. | 44-FCR, `Required reconciliation with current MWF` | 44-PRP-022 through 44-PRP-026 | S3 | pending | pending | pending | pending |
| 44-REC-020 | Regression coverage includes preservation of starting-component incoming input. | 44-FCR, `Required reconciliation with current MWF` | 44-PRP-016 | S3 | pending | pending | pending | pending |
| 44-REC-021 | Regression coverage includes component state and membership changes. | 44-FCR, `Required reconciliation with current MWF` | 44-CMP-001 through 44-CMP-026, 44-MEM-001 through 44-MEM-010 | S2, S3 | pending | pending | pending | pending |
| 44-REC-022 | Regression coverage includes waiting display. | 44-FCR, `Required reconciliation with current MWF` | 44-CMP-015 through 44-CMP-017 | S2 | pending | pending | pending | pending |
| 44-REC-023 | Regression coverage includes sample parsing, status filtering, and percentage rounding. | 44-FCR, `Required reconciliation with current MWF` | 44-SMP-003 through 44-SMP-019 | S4 | pending | pending | pending | pending |
| 44-REC-024 | Regression coverage includes randomness, seed replay, and drift guards. | 44-FCR, `Required reconciliation with current MWF` | 44-SMP-020 through 44-SMP-034 | S4 | pending | pending | pending | pending |
| 44-REC-025 | Regression coverage includes zero selection and full coverage. | 44-FCR, `Required reconciliation with current MWF` | 44-SMP-010, 44-SMP-043, 44-SMP-050 through 44-SMP-053 | S4 | pending | pending | pending | pending |
| 44-REC-026 | Regression coverage includes ordinary sample readiness, descendant blocking, and resume. | 44-FCR, `Required reconciliation with current MWF` | 44-SMP-044 through 44-SMP-048, 44-SMP-050 through 44-SMP-056 | S4 | pending | pending | pending | pending |
| 44-REC-027 | Regression coverage includes prior causal-descendant preparation. | 44-FCR, `Required reconciliation with current MWF` | 44-SMP-035 through 44-SMP-037 | S4 | pending | pending | pending | pending |
| 44-REC-028 | Regression coverage includes newly created selected-job circulation and failure transitions. | 44-FCR, `Required reconciliation with current MWF` | 44-SMP-038, 44-SMP-057 through 44-SMP-066 | S4 | pending | pending | pending | pending |
| 44-REC-029 | Regression coverage includes ordinary interrupt preflight and stopping boundaries. | 44-FCR, `Required reconciliation with current MWF` | 44-BND-005 through 44-BND-029 | S5 | pending | pending | pending | pending |
| 44-REC-030 | Regression coverage includes fences, stable and unstable propagation, conflicts, and explicit origin replacement. | 44-FCR, `Required reconciliation with current MWF` | 44-INT-021 through 44-INT-041 | S5 | pending | pending | pending | pending |
| 44-REC-031 | Regression coverage includes disjoint session scopes and overlapping-session refusal. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-003 through 44-SES-005, 44-SES-020 through 44-SES-026 | S5 | pending | pending | pending | pending |
| 44-REC-032 | Regression coverage includes main-to-interrupt transfer with active-claim refusal. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-013 through 44-SES-019 | S5 | pending | pending | pending | pending |
| 44-REC-033 | Regression coverage includes nested transfer and reference-counted holds. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-006 through 44-SES-012 | S5 | pending | pending | pending | pending |
| 44-REC-034 | Regression coverage includes exact job ownership. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-027 through 44-SES-029 | S2, S5 | pending | pending | pending | pending |
| 44-REC-035 | Regression coverage includes crash recovery and stale-hold release. | 44-FCR, `Required reconciliation with current MWF` | 44-INT-048 through 44-INT-052, 44-SES-045 through 44-SES-048 | S5 | pending | pending | pending | pending |
| 44-REC-036 | Regression coverage includes restart and thread ownership. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-042 through 44-SES-053 | S5 | pending | pending | pending | pending |
| 44-REC-037 | Regression coverage includes the project-wide aggregate API limit and its deprecation behavior. | 44-FCR, `Required reconciliation with current MWF` | 44-SES-054 through 44-SES-057 | S5, S6 | pending | pending | pending | pending |
| 44-REC-038 | Regression coverage includes misalignment batching and repair. | 44-FCR, `Required reconciliation with current MWF` | 44-MIS-010 through 44-MIS-029 | S3 | pending | pending | pending | pending |
| 44-REC-039 | Regression coverage includes full trace retention and both compact lineage forms. | 44-FCR, `Required reconciliation with current MWF` | 44-TRC-001 through 44-TRC-018 | S6 | pending | pending | pending | pending |
| 44-REC-040 | Regression coverage confirms recursive output traversal remains available. | 44-FCR, `Required reconciliation with current MWF` | 44-PRP-013 | S3 | pending | pending | pending | pending |
| 44-REC-041 | Regression coverage confirms complete removal of all four obsolete commands from every public entry point. | 44-FCR, `Required reconciliation with current MWF` | 44-CMD-025 through 44-CMD-029 | S6 | pending | pending | pending | pending |
| 44-REC-042 | Reconcile and test the explicit-interrupt sampling readiness override only at the interrupt-classified starting component. | 44-FCR, `Execution sampling` and `Required reconciliation with current MWF` | 44-SMP-049, 44-INT-002 through 44-INT-004 | S5 | pending | pending | pending | pending |
| 44-REC-043 | Regression coverage completes preview behavior for excluded downstream mutations, affected receivers, and required guards after S3 establishes ownership data. | 44-FCR, `Producer-qualified input and fresh preparation` and `Required reconciliation with current MWF` | 44-CMD-037, 44-PRP-036 through 44-PRP-046 | S3 | pending | pending | pending | pending |

### Deferred reminders and exclusions

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-DEF-001 | Defer same-producer collisions at one visible input path. | 44-FCR, `Deferred reminders` | 44-PRP-015 | ALL | pending | pending | pending | pending |
| 44-DEF-002 | Defer more than one ordinary main session. | 44-FCR, `Deferred reminders` | 44-SES-001 | ALL | pending | pending | pending | pending |
| 44-DEF-003 | Defer circular or unusually elaborate report layouts and report-specific session interactions beyond settled direct parent-child interruption. | 44-FCR, `Deferred reminders` | 44-RPT-001 through 44-RPT-011 | ALL | pending | pending | pending | pending |
| 44-DEF-004 | Defer Kaicenat compatibility and workflow refactoring. | 44-FCR, `Deferred reminders` | 44-SCP-025 through 44-SCP-027 | ALL | pending | pending | pending | pending |
| 44-DEF-005 | Defer AI validation-ghost discovery. | 44-FCR, `Deferred reminders` | 44-SCP-015 | ALL | pending | pending | pending | pending |
| 44-DEF-006 | Defer MWF 0.6.3 example repair and finalization. | 44-FCR, `Deferred reminders` | the context-loop decision boundary | ALL | pending | pending | pending | pending |
| 44-DEF-007 | Defer broader Wayfinder and Parent Repo restructuring. | 44-FCR, `Deferred reminders` | Issue #49 | ALL | pending | pending | pending | pending |
| 44-DEF-008 | the workflow-management resolution's decision itself made no MWF, Kaicenat, or Parent Repo source change. | 44-FCR, `Deferred reminders` | none | ALL | pending | pending | pending | pending |
| 44-DEF-009 | Keep every deferred reminder outside MWF 0.6.2 and carry it to its later owner. | 44-FCR, `Deferred reminders` | 44-DEF-001 through 44-DEF-007 | ALL | pending | pending | pending | pending |
| 44-DEF-010 | The 0.6.3 example work includes correction of the mistaken output-history framing; the implementation task does not make that correction. | MWF `CONTEXT.md`, `Release boundaries` | the context-loop decision boundary | ALL | pending | pending | pending | pending |

## Architecture gate carried from the workflow-management resolution

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 44-ARCH-001 | Check the 0.6.2 management model against MWF's internal architecture. | 44-ARCH, complete comment | current MWF source and docs | ALL | pending | pending | pending | pending |
| 44-ARCH-002 | Resolve code compatibility with the approved management behavior. | 44-ARCH, complete comment | 44-ARCH-001 | ALL | pending | pending | pending | pending |
| 44-ARCH-003 | Resolve behavior compatibility with retained MWF behavior. | 44-ARCH, complete comment | 44-ARCH-001 | ALL | pending | pending | pending | pending |
| 44-ARCH-004 | Treat any inconsistency in framework responsibility, state ownership, scheduling, or internal architecture as an explicit HITL question for Christopher. | 44-ARCH, complete comment | 44-ARCH-001 | GATE | pending | pending | pending | pending |
| 44-ARCH-005 | Do not silently choose one side of an internal-architecture inconsistency. | 44-ARCH, complete comment | 44-ARCH-004 | GATE | pending | pending | pending | pending |
| 44-ARCH-006 | Do not pull project-authored graph, node, or task design into the implementation task. | 44-ARCH, complete comment | 44-SCP-009 through 44-SCP-012 | ALL | pending | pending | pending | pending |

## Execution requirements

### Objective, inputs, and prerequisites

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-SRC-001 | Start from the published MWF 0.6.1 AFSR tree and implement the settled 0.6.2 workflow-management changes. | 45-PROC, `Objective` | the 0.6.1 release task closed | ALL | pending | pending | pending | pending |
| 45-SRC-002 | Verify code compatibility, retained behavior, and agreement with internal MWF architecture. | 45-PROC, `Objective` | 44-ARCH-001 through 44-ARCH-003 | ALL | pending | pending | pending | pending |
| 45-SRC-003 | Resolve internal architectural inconsistency with Christopher before release work may proceed. | 45-PROC, `Objective` | 44-ARCH-004, 44-ARCH-005 | GATE | pending | pending | pending | pending |
| 45-SRC-004 | MWF 0.6.2 may extend the 0.6.1 architecture skill with the settled management model. | 45-PROC, `Objective` | the 0.6.1 implementation task skills | S6 | pending | pending | pending | pending |
| 45-SRC-005 | the implementation task does not repair or finalize examples. | 45-PROC, `Objective` | the context-loop decision boundary | ALL | pending | pending | pending | pending |
| 45-SRC-006 | Read the implementation task and all its comments. | 45-PROC, `Authoritative requirements` | none | GATE | pending | pending | pending | pending |
| 45-SRC-007 | Read the the workflow-management resolution final consolidated resolution in full. | 45-PROC, `Authoritative requirements` | none | GATE | pending | pending | pending | pending |
| 45-SRC-008 | Treat every runtime, AFSR, documentation, CLI, planning, session, storage, testing, skill, migration, compatibility, and removal clause in the final resolution as required. | 45-PROC, `Authoritative requirements` | 45-SRC-007 | ALL | pending | pending | pending | pending |
| 45-SRC-009 | Keep the workflow-management resolution's cross-session architecture clarification in force. | 45-PROC, `Authoritative requirements` | 44-ARCH-001 through 44-ARCH-006 | GATE | pending | pending | pending | pending |
| 45-SRC-010 | Read current repository instructions and testing guidance. | 45-PROC, `Authoritative requirements` | none | GATE | pending | pending | pending | pending |
| 45-SRC-011 | Read the main Wayfinder, the shared-vocabulary decision's original resolution and supersession note, and apply current MWF documentation where older vocabulary differs. | 45-PROC, `Authoritative requirements` | source-applicability.md | GATE | pending | pending | pending | pending |
| 45-SRC-012 | Confirm the 0.6.1 release task and the workflow-management resolution are closed native prerequisites. | 45-PROC, `Authoritative requirements` | none | GATE | pending | pending | pending | pending |
| 45-SRC-013 | Search related issues and comments, follow relevant references, and record applicability and supersession. | 45-PROC, `Authoritative requirements` | 45-SRC-006 through 45-SRC-012 | ALL | pending | pending | pending | pending |
| 45-SRC-014 | Do not let an old proposal or unrelated later issue change 0.6.2 scope. | 45-PROC, `Authoritative requirements` | 45-SRC-013 | ALL | pending | pending | pending | pending |
| 45-SRC-015 | Send a material unresolved source conflict to grilling. | 45-PROC, `Authoritative requirements` | 45-SRC-013, 44-ARCH-004 | GATE | pending | pending | pending | pending |
| 45-SRC-016 | Read the complete local preparation task `Find objective for issue #45`; summaries do not satisfy this requirement. | 45-PROC, `Approved execution procedure` opening | local task `01a06e03-9d84-72f0-a292-863980e3b51d` | GATE | pending | pending | pending | pending |
| 45-SRC-017 | A fresh or resumed implementation session follows the saved procedure and can begin from the implementation task alone. | 45-PROC, `Approved execution procedure` opening | 45-SRC-006 through 45-SRC-016 | GATE | pending | pending | pending | pending |
| 45-SRC-018 | Read the preparation task with `read_thread` on host `local`, locating its exact title through `list_threads` if needed. | 45-PROC, `Approved execution procedure` opening | local task `01a06e03-9d84-72f0-a292-863980e3b51d` | GATE | pending | pending | pending | pending |
| 45-SRC-019 | Follow every returned `page.nextCursor` through `cursor` until all preparation turns, including later corrections, have been read. | 45-PROC, `Approved execution procedure` opening | 45-SRC-018 | GATE | pending | pending | pending | pending |

### Advance approval and work boundary

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-AUT-001 | the implementation task has advance approval for MWF source and engine changes necessary to satisfy approved 0.6.2 requirements. | 45-PROC, `Advance approval and scope` | 45-SRC-008 | ALL | pending | pending | pending | pending |
| 45-AUT-002 | Advance approval covers necessary tests, benchmarks, documentation, AFSR instructions, skills, and review corrections. | 45-PROC, `Advance approval and scope` | 45-SRC-008 | ALL | pending | pending | pending | pending |
| 45-AUT-003 | Later architectural answers Christopher approves during grilling enter the same advance-approved scope. | 45-PROC, `Advance approval and scope` | 45-BLK-014 | ALL | pending | pending | pending | pending |
| 45-AUT-004 | The the implementation task advance approval supersedes the earlier unchanged-approval comment and the main Wayfinder's default rule only for this task. | 45-PROC, `Advance approval and scope` | the main Wayfinder exception | ALL | pending | pending | pending | pending |
| 45-AUT-005 | Proceed without per-change or per-test permission inside the approved boundary. | 45-PROC, `Advance approval and scope` | 45-AUT-001 through 45-AUT-004 | ALL | pending | pending | pending | pending |
| 45-AUT-006 | Routine implementation choices are delegated, while architectural and domain decisions remain Christopher's. | 45-PROC, `Advance approval and scope` | 44-ARCH-004 | GATE | pending | pending | pending | pending |
| 45-AUT-007 | Work on a dedicated implementation branch in `C:\Business\product\mwf`. | 45-PROC, `Advance approval and scope` | 45-SRC-001 | ALL | pending | pending | pending | pending |
| 45-AUT-008 | Start from the recorded published 0.6.1 baseline or resume the recorded implementation branch. | 45-PROC, `Advance approval and scope` | 45-AUT-007, the 0.6.1 release task | ALL | pending | pending | pending | pending |
| 45-AUT-009 | Inspect and preserve existing MWF work before editing. | 45-PROC, `Advance approval and scope` | 45-AUT-007 | ALL | pending | pending | pending | pending |
| 45-AUT-010 | Record the branch and baseline before editing. | 45-PROC, `Advance approval and scope` | 45-AUT-008 | GATE | pending | pending | pending | pending |
| 45-AUT-011 | Keep Kaicenat read-only, including while checking external callers. | 45-PROC, `Advance approval and scope` | 44-SCP-025 | ALL | pending | pending | pending | pending |
| 45-AUT-012 | Allow only the narrow example documentation edits specified by the workflow-management resolution. | 45-PROC, `Advance approval and scope` | 44-CMD-028, 44-CMD-029 | S6 | pending | pending | pending | pending |
| 45-AUT-013 | Leave packaging and publication to the packaging task. | 45-PROC, `Advance approval and scope` | the packaging task | ALL | pending | pending | pending | pending |
| 45-AUT-014 | Production changes remain outside the implementation task. | 45-PROC, `Advance approval and scope` | Parent Repo boundary | ALL | pending | pending | pending | pending |

### Establish requirements and stages

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-PLN-001 | Claim the implementation task before implementation while respecting active claims. | 45-PROC, `1. Establish requirements and stages` | none | GATE | pending | pending | pending | pending |
| 45-PLN-002 | Follow Wayfinder operations and refresh the main graph when relationships or claim state change. | 45-PROC, `1. Establish requirements and stages` | 45-PLN-001 | ALL | pending | pending | pending | pending |
| 45-PLN-003 | Link a durable requirement-by-requirement record from the implementation task. | 45-PROC, `1. Establish requirements and stages` | this file | GATE | pending | pending | pending | pending |
| 45-PLN-004 | For each requirement, record source and section, dependencies, stage, changes, verification, findings, and disposition. | 45-PROC, `1. Establish requirements and stages` | 45-PLN-003 | ALL | pending | pending | pending | pending |
| 45-PLN-005 | Split separate clauses within specification paragraphs into separate tracked requirements. | 45-PROC, `1. Establish requirements and stages` | 45-PLN-004 | GATE | pending | pending | pending | pending |
| 45-PLN-006 | Track retained behavior and intentional compatibility changes. | 45-PROC, `1. Establish requirements and stages` | 45-PLN-004 | ALL | pending | pending | pending | pending |
| 45-PLN-007 | Choose manageable stages according to implementation dependencies. | 45-PROC, `1. Establish requirements and stages` | 45-PLN-004 | ALL | pending | pending | pending | pending |
| 45-PLN-008 | Use specification order as guidance rather than a fixed implementation order. | 45-PROC, `1. Establish requirements and stages` | 45-PLN-007 | ALL | pending | pending | pending | pending |
| 45-PLN-009 | Split, combine, and reorder stages as needed while recording material plan changes and keeping every requirement accounted for. | 45-PROC, `1. Establish requirements and stages` | 45-PLN-007 | ALL | pending | pending | pending | pending |
| 45-PLN-010 | Assess selection calculations and the read-only preview base as the first dependency group. | 45-PROC, `1. Establish requirements and stages` | 44-CMD-014 through 44-CMD-022, 44-CMD-031 through 44-CMD-036, 44-CMD-038 through 44-CMD-046 | S1 | pending | pending | pending | pending |
| 45-PLN-011 | Assess component identity and lifecycle, durable sessions, ownership, migration, and recovery foundations as the second dependency group. | 45-PROC, `1. Establish requirements and stages` | 44-CMP-001 through 44-CMP-026, 44-SES-001 through 44-SES-004, 44-SES-006, 44-SES-012, 44-SES-027, 44-SES-028, 44-SES-030 through 44-SES-040, 44-INT-048 | S2 | pending | pending | pending | pending |
| 45-PLN-012 | Assess publication ownership, shared fresh preparation, downstream guards, misalignment, and membership repair as the third dependency group. | 45-PROC, `1. Establish requirements and stages` | S2 foundations | S3 | pending | pending | pending | pending |
| 45-PLN-013 | Assess ordinary execution, nine commands, readiness, sampling, and selected-job causal execution as the fourth dependency group. | 45-PROC, `1. Establish requirements and stages` | S1 through S3 foundations | S4 | pending | pending | pending | pending |
| 45-PLN-014 | Assess interrupt execution, transfers, holds, fences, and ownership-dependent recovery, restart, and thread controls as the fifth dependency group. | 45-PROC, `1. Establish requirements and stages` | S2 through S4 foundations | S5 | pending | pending | pending | pending |
| 45-PLN-015 | Complete diagnostics and reconcile runtime, CLI, documentation, AFSR instructions, skills, and integrated behavior as the sixth dependency group. | 45-PROC, `1. Establish requirements and stages` | S1 through S5 | S6 | pending | pending | pending | pending |
| 45-PLN-016 | Tests and documentation accompany every relevant stage. | 45-PROC, `1. Establish requirements and stages` | 45-TDD-001 through 45-TDD-026 | ALL | pending | pending | pending | pending |
| 45-PLN-017 | Unresolved session ownership blocks dependent interrupts and reset guards. | 45-PROC, `1. Establish requirements and stages` | 44-SES-027, 44-SES-028 | S2, GATE | pending | pending | pending | pending |
| 45-PLN-018 | Continue only work whose prerequisites are settled. | 45-PROC, `1. Establish requirements and stages` | 45-PLN-007, 45-BLK-001 | GATE | pending | pending | pending | pending |
| 45-PLN-019 | Complete the downstream-mutation part of previews in S3 after publication ownership and guard data exist; this is a recorded dependency split from the initial S1 grouping. | 45-PROC, `1. Establish requirements and stages` | 44-CMD-037, 44-PRP-036 through 44-PRP-046 | S3 | pending | pending | pending | pending |

### Test-first implementation

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-TDD-001 | Use the TDD skill and repeat its sequence for small behavioral sections within every stage. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-PLN-007 | ALL | pending | pending | pending | pending |
| 45-TDD-002 | Read the full local task `Review GitHub issue #44` by paging through every older turn; its latest summary is insufficient. | 45-PROC, `2. Implement each small behavioral section test-first` | local task `01a0669c-232f-7443-a06b-e832063ff0ca` | ALL | pending | pending | pending | pending |
| 45-TDD-003 | Use that task's worked examples to design stage-relevant tests. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-002 | ALL | pending | pending | pending | pending |
| 45-TDD-004 | Record the source question or turn and the applicable approved requirement for each reused worked example. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-003 | ALL | pending | pending | pending | pending |
| 45-TDD-005 | Check every worked example's expected result against the final the workflow-management resolution resolution because earlier proposals may have changed. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-003, 45-SRC-007 | ALL | pending | pending | pending | pending |
| 45-TDD-006 | Do not require a public link or transcript export for the local example source. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-002 | ALL | pending | pending | pending | pending |
| 45-TDD-007 | Advance-approved test boundaries include CLI commands and outputs. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-AUT-002 | ALL | pending | pending | pending | pending |
| 45-TDD-008 | Advance-approved test boundaries include task-facing APIs. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-AUT-002 | ALL | pending | pending | pending | pending |
| 45-TDD-009 | Advance-approved test boundaries include workflow execution and scheduling outcomes. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-AUT-002 | ALL | pending | pending | pending | pending |
| 45-TDD-010 | Advance-approved test boundaries include managed filesystem effects. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-AUT-002 | ALL | pending | pending | pending | pending |
| 45-TDD-011 | Advance-approved test boundaries include required persistence, session ownership, migration, and recovery invariants. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-AUT-002 | ALL | pending | pending | pending | pending |
| 45-TDD-012 | Choose individual cases and existing test tools inside the approved test boundaries without further confirmation. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-007 through 45-TDD-011 | ALL | pending | pending | pending | pending |
| 45-TDD-013 | Avoid assertions tied only to incidental private implementation detail. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-012 | ALL | pending | pending | pending | pending |
| 45-TDD-014 | Send an unresolved interface or architectural requirement to grilling. | 45-PROC, `2. Implement each small behavioral section test-first` | 44-ARCH-004 | GATE | pending | pending | pending | pending |
| 45-TDD-015 | For each small section, identify the exact requirement and behavior to test. | 45-PROC, `2. Implement each small behavioral section test-first` | requirement row | ALL | pending | pending | pending | pending |
| 45-TDD-016 | Before implementation, add any missing regression checks for behavior that must remain and confirm they pass on existing code. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-015 | ALL | pending | pending | pending | pending |
| 45-TDD-017 | For new or corrected behavior, write and run the test before changing implementation. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-015 | ALL | pending | pending | pending | pending |
| 45-TDD-018 | The pre-implementation test must fail for the expected behavior, not because of unrelated setup, syntax, or test failure. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-017 | ALL | pending | pending | pending | pending |
| 45-TDD-019 | Derive expected results from the approved specification or an independent example. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-015 | ALL | pending | pending | pending | pending |
| 45-TDD-020 | Implement the small change, make its test pass, and repeat for the next behavior rather than implementing the whole stage first. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-016 through 45-TDD-019 | ALL | pending | pending | pending | pending |
| 45-TDD-021 | Run relevant surrounding checks and record before-and-after commands and results. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-020 | ALL | pending | pending | pending | pending |
| 45-TDD-022 | Preservation tests may already pass, and already-satisfied requirements still need verification and a record. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-016 | ALL | pending | pending | pending | pending |
| 45-TDD-023 | A regression found during review follows the same test-first correction sequence. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-017 through 45-TDD-021 | ALL | pending | pending | pending | pending |
| 45-TDD-024 | Reviewers inspect test relevance, sensitivity to incorrect behavior, and before-and-after results. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-015 through 45-TDD-023 | ALL | pending | pending | pending | pending |
| 45-TDD-025 | Follow MWF's isolated testing guidance and exercise real scheduler, storage, parser, input, monitoring, and trace paths. | 45-PROC, `2. Implement each small behavioral section test-first` | the 0.6.1 implementation task testing model, 44-REC-014 | ALL | pending | pending | pending | pending |
| 45-TDD-026 | Run focused and adjacent regressions, then required broader checks. | 45-PROC, `2. Implement each small behavioral section test-first` | 45-TDD-025 | ALL | pending | pending | pending | pending |
| 45-TDD-027 | Documentation-only edits receive documentation verification rather than artificial executable tests. | 45-PROC, `2. Implement each small behavioral section test-first` | 44-DOC-001 through 44-DOC-033 | S6 | pending | pending | pending | pending |

### Stage adversarial reviews

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-REV-001 | Stage adversarial reviewers use `gpt-5.6-sol` with `xhigh` reasoning. | 45-PROC, `3. Review each stage according to its impact` | stage implementation and checks | ALL | pending | pending | pending | pending |
| 45-REV-002 | One independent Sol reviewer is sufficient for a small, narrow change with limited behavioral impact. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-001 | ALL | pending | pending | pending | pending |
| 45-REV-003 | Use multiple independent Sol reviewers for delicate interactions or potentially broad effects. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-001 | ALL | pending | pending | pending | pending |
| 45-REV-004 | Diff size alone does not determine reviewer count. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-002, 45-REV-003 | ALL | pending | pending | pending | pending |
| 45-REV-005 | Give multiple reviewers different affected areas and overlapping coverage of shared boundaries. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-003 | ALL | pending | pending | pending | pending |
| 45-REV-006 | Record each stage's review coverage. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-002 or 45-REV-003 | ALL | pending | pending | pending | pending |
| 45-REV-007 | Supply reviewers changed code, documented changes, applicable requirements, check results, and earlier findings. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-006 | ALL | pending | pending | pending | pending |
| 45-REV-008 | Each reviewer finds all applicable specifications for its assigned area, including requirements in other sections and approved issue decisions. | 45-PROC, `3. Review each stage according to its impact` | 45-SRC-013 | ALL | pending | pending | pending | pending |
| 45-REV-009 | Each reviewer maps the requirements to documented changes and inspects the implementation and checks for omission, drift, ambiguity, and incorrect behavior. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-007, 45-REV-008 | ALL | pending | pending | pending | pending |
| 45-REV-010 | Each reviewer examines affected behavior beyond the diff. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-009 | ALL | pending | pending | pending | pending |
| 45-REV-011 | Preserve existing behavior except where an approved requirement changes it. | 45-PROC, `3. Review each stage according to its impact` | 45-PLN-006 | ALL | pending | pending | pending | pending |
| 45-REV-012 | Reviewers look for missed affected areas and expand review coverage when needed. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-010 | ALL | pending | pending | pending | pending |
| 45-REV-013 | A Sol reviewer encountering an architectural problem reads the full local preparation history before calling it unresolved. | 45-PROC, `3. Review each stage according to its impact` | 45-SRC-016 | GATE | pending | pending | pending | pending |
| 45-REV-014 | Reviewers report precise findings, supporting observations, and each architectural question. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-009 | ALL | pending | pending | pending | pending |
| 45-REV-015 | Fix actionable findings and obtain review of the corrections before accepting the stage. | 45-PROC, `3. Review each stage according to its impact` | 45-TDD-023, 45-REV-014 | GATE | pending | pending | pending | pending |
| 45-REV-016 | Stage review inspects code as well as summaries and test results. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-009 | ALL | pending | pending | pending | pending |
| 45-REV-017 | Use blast-radius guidance to inspect effects elsewhere. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-010 | ALL | pending | pending | pending | pending |
| 45-REV-018 | Resolve factual reviewer disagreements through source inspection and focused checks rather than voting. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-014 | ALL | pending | pending | pending | pending |
| 45-REV-019 | Architectural reviewer disagreements return to Christopher. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-018, 44-ARCH-004 | GATE | pending | pending | pending | pending |
| 45-REV-020 | After three attempts without meaningful progress, record the failure and continue independent work. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-018 | ALL | pending | pending | pending | pending |
| 45-REV-021 | Keep implementation failures distinct from architectural ambiguity. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-019, 45-REV-020 | ALL | pending | pending | pending | pending |
| 45-REV-022 | Neither an implementation failure nor an architectural ambiguity counts as completed work. | 45-PROC, `3. Review each stage according to its impact` | 45-REV-021 | GATE | pending | pending | pending | pending |

### Blockers, grilling, and resumed implementation

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-BLK-001 | Before deferring an architectural question, check the specification and current code. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-SRC-007, current MWF source | GATE | pending | pending | pending | pending |
| 45-BLK-002 | Read the full local preparation history as the final check for an answer before deferral. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-001, 45-SRC-016 | GATE | pending | pending | pending | pending |
| 45-BLK-003 | Apply an explicit approved answer found in the preparation history. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-002 | ALL | pending | pending | pending | pending |
| 45-BLK-004 | If still unresolved, record the relevant turns and explain exactly what remains unsettled. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-002 | GATE | pending | pending | pending | pending |
| 45-BLK-005 | Record the exact unresolved architectural question. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-004 | GATE | pending | pending | pending | pending |
| 45-BLK-006 | Record conflicting requirements or code for the question. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-005 | GATE | pending | pending | pending | pending |
| 45-BLK-007 | Record available choices and their consequences. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-005 | GATE | pending | pending | pending | pending |
| 45-BLK-008 | Record every dependent piece of work. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-005 | GATE | pending | pending | pending | pending |
| 45-BLK-009 | Mark affected work and all dependencies blocked while continuing independent stages. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-005 through 45-BLK-008 | ALL | pending | pending | pending | pending |
| 45-BLK-010 | Do not add assumed semantics, temporary behavior, or an incomplete substitute to bypass the decision. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-009 | GATE | pending | pending | pending | pending |
| 45-BLK-011 | After available independent work is exhausted, grill accumulated architectural questions with Christopher using Grilling and Domain Modeling. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-009 | GATE | pending | pending | pending | pending |
| 45-BLK-012 | If Christopher is unavailable, preserve progress and questions for his return. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-011 | ALL | pending | pending | pending | pending |
| 45-BLK-013 | Never answer Christopher's side of a grilling. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-011 | GATE | pending | pending | pending | pending |
| 45-BLK-014 | Record Christopher's approved answers on the implementation task and link them into this requirement record. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-011 | ALL | pending | pending | pending | pending |
| 45-BLK-015 | Proceed directly to test-first implementation of newly unblocked work. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-014, 45-TDD-001 | ALL | pending | pending | pending | pending |
| 45-BLK-016 | Repeat review, grilling, and implementation as many times as needed. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-BLK-011 through 45-BLK-015 | ALL | pending | pending | pending | pending |
| 45-BLK-017 | Unresolved implementation failures remain unfinished and must be repaired before final review. | 45-PROC, `4. Defer blockers and repeat grilling and implementation` | 45-REV-020, 45-REV-021 | FINAL gate | pending | pending | pending | pending |

### Preserve reviewed progress and support resumption

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-CHK-001 | Commit and push each stage only after its required checks and reviews pass. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-REV-015 | GATE | pending | pending | pending | pending |
| 45-CHK-002 | Identify the reviewed commit for every accepted stage. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-CHK-001 | GATE | pending | pending | pending | pending |
| 45-CHK-003 | Isolate unfinished changes from accepted work. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-CHK-001 | ALL | pending | pending | pending | pending |
| 45-CHK-004 | Preserve blocked attempts, findings, and unrelated work. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-BLK-009, 45-CHK-003 | ALL | pending | pending | pending | pending |
| 45-CHK-005 | Keep accepted work coherent. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-CHK-001, 45-CHK-003 | GATE | pending | pending | pending | pending |
| 45-CHK-006 | Record branch, baseline, accepted commit, requirement progress, and check results on the implementation task. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-AUT-010, 45-CHK-002 | ALL | pending | pending | pending | pending |
| 45-CHK-007 | Record reviewer models, findings, resolutions, blockers, and next work on the implementation task. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-REV-001 through 45-REV-021 | ALL | pending | pending | pending | pending |
| 45-CHK-008 | Link larger records and artifacts so another session can resume without hidden context. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-CHK-006, 45-CHK-007 | ALL | pending | pending | pending | pending |
| 45-CHK-009 | Accept a stage only after required checks pass and findings are resolved. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-TDD-026, 45-REV-015 | GATE | pending | pending | pending | pending |
| 45-CHK-010 | If a required reviewer cannot run, record the review as outstanding and continue independent work. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-REV-001 | ALL | pending | pending | pending | pending |
| 45-CHK-011 | Do not silently substitute another reviewer model or mark the outstanding review passed. | 45-PROC, `5. Preserve reviewed progress and make resumption explicit` | 45-CHK-010 | GATE | pending | pending | pending | pending |

### Final exhaustive review

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-FIN-001 | The final reviewer uses `gpt-6-astra` with `xhigh` reasoning. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | all accepted stages | FINAL | pending | pending | pending | pending |
| 45-FIN-002 | Astra reads the full local preparation history, including Christopher's corrections and approvals. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-SRC-016 | FINAL | pending | pending | pending | pending |
| 45-FIN-003 | The final review record states that Astra read the complete preparation history. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-002 | FINAL | pending | pending | pending | pending |
| 45-FIN-004 | Do not start Astra review until every known architectural question is resolved. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-BLK-014 | FINAL gate | pending | pending | pending | pending |
| 45-FIN-005 | Do not start Astra review until all required implementation, testing, and documentation work is complete. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | all requirement rows | FINAL gate | pending | pending | pending | pending |
| 45-FIN-006 | Do not start Astra review until required checks pass. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-TDD-026 | FINAL gate | pending | pending | pending | pending |
| 45-FIN-007 | Do not start Astra review until all stage-review findings are resolved. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-REV-015 | FINAL gate | pending | pending | pending | pending |
| 45-FIN-008 | Do not run Astra while another known architectural grilling or implementation cycle is needed. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-004 through 45-FIN-007 | FINAL gate | pending | pending | pending | pending |
| 45-FIN-009 | Astra independently establishes the complete applicable specification. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | source-applicability.md | FINAL | pending | pending | pending | pending |
| 45-FIN-010 | Astra reads the implementation task step by step, including every requirement and comment. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-009 | FINAL | pending | pending | pending | pending |
| 45-FIN-011 | Astra reads the authoritative the workflow-management resolution final resolution in full. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-009 | FINAL | pending | pending | pending | pending |
| 45-FIN-012 | Astra searches relevant open and closed issues and approved decisions for additional applicable requirements. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-009 | FINAL | pending | pending | pending | pending |
| 45-FIN-013 | Astra follows relevant references until the applicable requirement set is fully accounted for. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-010 through 45-FIN-012 | FINAL | pending | pending | pending | pending |
| 45-FIN-014 | Astra actively finds missed specifications across issues rather than merely collecting links or reading only the workflow-management resolution. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-012, 45-FIN-013 | FINAL | pending | pending | pending | pending |
| 45-FIN-015 | Give Astra all stage reviews, coverage, findings, fixes, architectural questions, and approved answers. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-CHK-007 | FINAL | pending | pending | pending | pending |
| 45-FIN-016 | Astra independently checks every applicable requirement against final code and verification. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-009, 45-FIN-015 | FINAL | pending | pending | pending | pending |
| 45-FIN-017 | Astra challenges the completeness of the requirement record. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-PLN-003 through 45-PLN-006 | FINAL | pending | pending | pending | pending |
| 45-FIN-018 | Astra examines coherence and interactions across the complete implementation. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-016 | FINAL | pending | pending | pending | pending |
| 45-FIN-019 | Earlier stage acceptance does not replace Astra's independent examination. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-016 | FINAL | pending | pending | pending | pending |
| 45-FIN-020 | Record reviewed commit, issues and decisions examined, requirement coverage, verification assessed or performed, findings, and disposition. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-010 through 45-FIN-019 | FINAL | pending | pending | pending | pending |
| 45-FIN-021 | New final-review findings return to test-first correction or architectural grilling as appropriate. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-020, 45-TDD-023, 45-BLK-011 | FINAL | pending | pending | pending | pending |
| 45-FIN-022 | After correcting final findings, re-establish every readiness condition before final acceptance on the resulting code. | 45-PROC, `6. Run the final exhaustive review only after the work is ready` | 45-FIN-004 through 45-FIN-008, 45-FIN-021 | FINAL gate | pending | pending | pending | pending |

### Completion and handoff

| ID | Requirement | Exact source section | Dependencies | Proposed stage | Implementation | Verification | Stage review | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 45-DON-001 | Keep the implementation task open throughout implementation and grilling. | 45-PROC, `7. Complete this issue` | none | ALL | pending | pending | pending | pending |
| 45-DON-002 | Close the implementation task only after every applicable requirement is satisfied. | 45-PROC, `7. Complete this issue` | all requirement rows | FINAL gate | pending | pending | pending | pending |
| 45-DON-003 | Close only after all necessary checks pass and every finding is resolved. | 45-PROC, `7. Complete this issue` | 45-TDD-026, 45-REV-015, 45-FIN-021 | FINAL gate | pending | pending | pending | pending |
| 45-DON-004 | Close only after the final Astra review accepts the resulting code. | 45-PROC, `7. Complete this issue` | 45-FIN-022 | FINAL gate | pending | pending | pending | pending |
| 45-DON-005 | Close only after accepted changes are committed and pushed. | 45-PROC, `7. Complete this issue` | 45-CHK-001 | FINAL gate | pending | pending | pending | pending |
| 45-DON-006 | Record the final commit, requirement record, verification and review results, and handoff to the packaging task. | 45-PROC, `7. Complete this issue` | 45-DON-002 through 45-DON-005 | FINAL | pending | pending | pending | pending |
| 45-DON-007 | Follow normal Wayfinder resolution and graph-update steps. | 45-PROC, `7. Complete this issue` | 45-DON-006 | FINAL | pending | pending | pending | pending |
| 45-DON-008 | Closing the implementation task does not publish MWF 0.6.2. | 45-PROC, `7. Complete this issue` | the packaging task | FINAL | pending | pending | pending | pending |

## Current audit state

S1a records the accepted interval calculation and its checks. Between-command integration remains pending. All other rows remain pending until their implementation, checks, review, and disposition are recorded. This record does not accept the full S1 stage or start final review.
