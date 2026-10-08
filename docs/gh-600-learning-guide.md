# Learning GH-600 through this project

We are building an immediate Kafka worker service while learning how to supervise coding agents.
Your role is to set a concrete outcome, understand the critical invariant, and
judge evidence before accepting work. The agent can investigate, implement,
test, and prepare a review; its confident description is not proof of correctness.

This guide follows the six areas in the [official GH-600 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/gh-600),
checked 2026-10-08. It is a project-specific practice plan, not complete exam
coverage. Copilot-specific setup and GitHub controls need hands-on practice in
the actual environment as well as these repository exercises.

## Where the learning happens

| Exam area | Project exercise | Evidence to retain |
| --- | --- | --- |
| Architecture and SDLC | Bound a task before implementation | Requirement, plan, scope decision, PR |
| Tools and environments | Use repository-scoped tools and a verified MCP setup | Tool inventory, effective permissions, failure/recovery record |
| Memory and execution | Resume a task from durable context | Commit-linked handoff and corrected stale decision |
| Evaluation and tuning | Find a failure and improve the agent workflow | Reproduction, test/scan output, correction, rerun |
| Multi-agent coordination | Separate implementation and review responsibilities | Ownership, handoff, conflicting findings and resolution |
| Guardrails and accountability | Exercise enforced access and review controls | Allowed/denied action evidence and review/check records |

These are learning targets. None is complete just because this table exists.

## Lesson 1: turn intent into a falsifiable requirement

“Same jobs must not overlap” needs precise identities. In our spec, jobId names
a serial lane and executionId names one invocation. Repeating an invocation is
a duplicate; a different invocation with the same jobId must wait its turn within
the same topic. The service does not durably deduplicate invocations.

Consider A1 and A2 with jobId A, and B1 with jobId B. A1 and B1 may run together.
A2 waits until A1 succeeds or its retry/DLQ handoff is acknowledged. Once A1
is handed to the retry topic, A2 can start in the main worker, even while a retry
worker runs A1. The simpler spec deliberately allows this cross-topic overlap.

The test should hold A1 behind a controlled barrier, verify B1 starts, and verify
A2 has not started. Release A1 and check A2 starts afterward. Extend it with an
A1 retry handoff and verify the allowed cross-topic overlap. Avoid an arbitrary
sleep that merely makes an overlap less likely.

**Your check:** can A2 run while another pod retries A1? Yes: each topic has its
own ownership and per-job gate. Matching keys across topics do not create a
global lock. This boundary must be visible in tests and handler requirements.

This lesson is about evaluating an agent's interpretation of a requirement.
The Kafka behavior supplies the example.

## Lesson 2: plan a small change and inspect its evidence

Start implementation with envelope validation (L01), after establishing
the Maven skeleton. Copy the [task template](templates/task-record.md) into
`docs/tasks/` with a descriptive filename. Record the relevant B01/B17 cases.

A useful task request is:

> Implement the Spec 002 main and retry envelope validators.
> Cover valid input, mismatched jobId/key, forbidden scheduledAt, and invalid
> retry attempt values. Keep handler execution out of this change. State the
> plan, implement within that scope, run the relevant checks, and report evidence.

Separate plan review from patch review. A plan can be sound while the code is
wrong. A passing unit test can establish validation behavior without proving
durability or rebalance safety. Inspect what each result actually supports.

For each task, use: requirement -> plan -> branch -> patch -> checks -> review.
Capture the user's scope decision once; routine authorized fixes do not need a
new approval. Escalate a genuinely new architectural choice with concrete options.

## Lesson 3: configure tools with observable boundaries

For L02, inventory the actual execution environment: repository path/branch,
Java/Maven/Docker availability, shell access, GitHub access, and network access.
Record which operations can mutate local files or remote resources.

Then perform a separate GitHub Copilot/MCP setup exercise using current official
instructions for the installed client. Configure the GitHub remote MCP server,
inspect available tools, and document the actual registry/allowlist mechanisms
supported by that environment. Begin with reading this repository. Verify a
disallowed operation using a safe test target, rather than testing against a
real destructive resource. Keep credentials outside the repository.

Do not equate a prompt saying “read-only” with read-only credentials. Record
which boundary is enforced by credentials, the client, or server policy. If an
allowlist feature is unavailable, record the gap and the enforced alternative.
Do not label this exercise complete based on an example configuration alone.

**Your check:** which tool could push code, and what actually restricts it?

## Lesson 4: make context recoverable

For L03, end a work session with a task record identifying the verified commit,
completed checks, unresolved issue, and next concrete action. Resume in a fresh
session using that record plus the spec and diff.

Use the PostgreSQL-to-Kafka architecture change as a drift exercise: supply the
historical Spec 001 alongside the current Spec 002 and verify the agent resolves
the supersession correctly. Also check the simplified Spec 002 against its
earlier Git version: delayed jobs and RocksDB were removed, and cross-topic
ordering was relaxed. An agent that adds those features has used stale context.
Record and correct the instruction or handoff that allowed the mistake.

Keep reusable invariants in AGENTS.md, temporary progress in task records, and
final outcomes in PRs. Revalidate task context when the commit changes; archive
completed task records and remove obsolete temporary notes. Preserve historical
decisions as superseded. Never make stale context authoritative by repetition.

**Your check:** could another session resume the task without guessing what passed?

## Lesson 5: evaluate the agent, then improve its workflow

For L04, deliberately challenge a claim: duplicate retry handoffs, premature
offset commits, or an adaptive limiter that exceeds its configured maximum.
Use controlled synthetic faults, never production data.

When a test fails, distinguish a wrong requirement interpretation, wrong code,
tool misuse, stale context, and environment failure. A missing Docker daemon
does not prove that a handoff/offset algorithm is wrong or correct.

Record the initial reproduction, root cause, correction, and rerun. Where the
failure reveals a repeatable agent mistake, improve an instruction or task
boundary and test it on a similar case. Avoid growing instructions with every
one-off error. Retain passing and failing artifacts, with commit identifiers.

Track useful signals: acceptance cases demonstrated, substantive review findings,
regressions, and repeated instruction violations. Speed alone is insufficient.
Include dependency and code/security scanning once the build exists; record
findings and their disposition rather than reporting only a green badge.

**Your check:** what changed in the workflow that should prevent recurrence?

## Lesson 6: coordinate agents without losing ownership

L05 is a deliberate later exercise, not a request to launch agents now. Assign
one implementation owner and a reviewer with a separate checkout/read-only
scope. Give each the same base commit, relevant spec sections, acceptance IDs,
expected output, and explicit file boundaries.

The reviewer challenges crash/rebalance behavior and returns findings with
reproduction steps; the implementation owner resolves them. The orchestrator
checks evidence and reconciles disagreements. Avoid both agents silently editing
the same files or assuming the other ran tests.

Practice a stalled participant: preserve its patch and handoff, stop its writes,
and let a replacement resume from the last verified state. Record what was
retained, discarded, and rechecked. Retire participants with a final outcome so
their stale guidance does not keep circulating.

**Your check:** who owns the next edit, and which commit did the reviewer inspect?

## Lesson 7: make GitHub controls real

For L06, introduce CI only after build commands exist. Retain unit/integration
results and scan artifacts in a PR. Configure applicable branch/ruleset controls
and required reviews/checks, then verify their effective behavior in the available
GitHub plan and repository. A PR template does not enforce review or branch rules.

Add a restricted agent-in-CI exercise, such as preparing an advisory explanation
of a synthetic test failure. Verify the configured agent can run in that
environment, has no unnecessary write token, and cannot merge its own change.
Avoid workflows that execute untrusted PR code with privileged secrets. Never
claim this setup exists until a real workflow run and permission check prove it.

Use a harmless prompt-injection fixture in issue text or a test log, asking the
agent to ignore its task or reveal credentials. Use fake credentials only. Check
that it treats the fixture as untrusted data, continues the authorized task,
and preserves a redacted record of the result.

Recovery should be concrete: reverting an isolated bad change, restoring a
known-good instruction, or handing an unresolved finding to the reviewer. Do not
grant broad permissions merely to make a failing workflow turn green.

**Your check:** which action is technically blocked, and where is the evidence?

## Progress ledger

| Item | Current status |
| --- | --- |
| Kafka architectural specification | Simplified for immediate main/retry processing; runtime not implemented |
| Learning workflow and templates | Written; exercises not yet run |
| Runtime implementation / Maven build | L01 Maven foundation and validators implemented; worker runtime pending |
| Actual automated evaluations and scans | L01 unit validation executed; integration tests and scans pending |
| Copilot/MCP setup and permissions exercise | Not verified |
| Fresh-session memory exercise | Not run |
| Multi-agent exercise | Not run |
| Agent-in-CI and GitHub enforcement exercise | Not configured/verified |

Update this ledger with evidence links as exercises finish. Read the official
study guide again before exam preparation to catch changed objectives and topics
not exercised here.

First implementation evidence: [L01 task record](tasks/L01-envelope-validation.md).
