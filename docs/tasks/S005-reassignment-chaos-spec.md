# Task S005: Specify partition reassignment and chaos acceptance

Status: ready for review

## Scope and plan

- Requirement: write a new spec following the pod reassignment review.
- Spec 002 acceptance IDs: B03, B05, B09, B10, B11, B14, B15, B16, B18, B19.
- Base commit: `f4588e6b08ed287cff9da83d9e03ade7938617d2`; existing branch
  `docs/h03-h06-arch-hardening`. Documentation only; no implementation branch switch.
- Authorized scope: new spec and this task record; implementation and fault runs
  are not part of this writing task.
- Expected behavior: specify generation-safe reassignment, bounded retirement,
  safe commits, and independently observable fault recovery without strengthening
  Spec 002 into global exclusion or exactly-once effects.
- Files: `specs/005-partition-reassignment-chaos.md` and this record.
- Validation: reconcile Specs 002/003, inspect local links and whitespace, review
  acceptance traceability and explicit test/evidence status.
- Non-goals: runtime edits, topic expansion, cluster operations, performance tests.

## Tools and boundaries

- Local shell and patch tools for repository reads and two new documentation files.
- Official Kafka/Kubernetes documentation read using web tools; sources linked
  directly in the spec. No remote mutations or cluster access.
- Existing H03–H06 source/test/task modifications and untracked files preserved.
- Environment enforcement beyond the provided filesystem sandbox is unverified.

## Progress and decisions

- 2026-10-27: read Specs 002/003, task template, repository instructions, and
  existing assignment-lifecycle task evidence.
- Used Spec 005 because Spec 003 reserves that number for chaos campaigns.
- Interpreted repartitioning as consumer reassignment for pod membership changes;
  topic-count migration remains excluded under Spec 002/H05.
- Defined C01–C15, deterministic and seeded tests, independent offset/resource
  assertions, separate Kafka/Kubernetes evidence, and bounded shutdown/recovery.
- Review findings are motivations for reproduction, not claims of executed failures.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Spec consistency | base above + new docs | Read Specs 002/003 and map C01–C15 | passed: preserves existing guarantees and H01–H04 traceability | Spec sections 1–2, 9, 12 |
| Markdown/link/whitespace checks | base above + new docs | Python local-link/whitespace/C-ID assertions; `git diff --check` | passed | Both new files validated; C01–C15 unique and complete; commands exited 0 |
| Runtime / Kafka / chaos tests | base above | Not run for documentation-only task | not run | All acceptance cases explicitly planned |

## Failure analysis and correction

- No runtime failure reproduced in this task. Prior review findings require the
  deterministic reproductions specified here.
- Existing repository instructions describe an older test baseline; current
  `pom.xml` has Failsafe and Kafka integration sources. The spec names planned
  commands without claiming their execution or results.

## Handoff and review

Update 2026-10-27: the original draft-only handoff below is historical. Subsequent
user direction selected cooperative sticky assignment; the
[C12 task](C12-cooperative-sticky.md) records implementation and passing Kafka
tests. The current [chaos ledger](../CHAOS.md) tracks remaining scenarios. This
supersedes the original assumption that all C01–C15 cases are only planned;
the original drafting task's “not run” result remains accurate for that task.

- Current deliverables: new Spec 005 and this task record; no existing files edited.
- Next step: review spec, then implement the first lifecycle reproduction slice
  on a feature branch with the current working diff reconciled.
- Open implementation work: verify exact pinned Vert.x revoke/lost callback and
  commit behavior, implement isolated fault runners, and execute required tiers.
- No commit, push, PR, deployment, or chaos execution performed.

## Learning check

- Invariant: partition reassignment never releases a still-running handler permit.
- Disproof: C10 observes replacement starts while old-generation usage is at cap.
- Evidence required: process-level start/settlement ledger across generations;
  assignment ownership alone cannot prove handler termination.
