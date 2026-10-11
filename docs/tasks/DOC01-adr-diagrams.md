# Task DOC01: Illustrate all architecture decisions

Status: complete

## Scope and plan

- Requirement: add explanatory diagrams under every ADR in `arch.md`, extending
  the previously authorized ADR 9 description and SVG.
- Spec 002 acceptance IDs: B01–B20 as relevant to each ADR; captions identify the
  specific invariants illustrated. Diagrams are documentation, not new evidence
  that an acceptance case passes.
- Base commit and branch: `f5ccec71dceb7ef16f0521b1b3c974dbdc40a74b` on
  `feat/spec03-worker-runtime-correctness`; prior uncommitted ADR 9 work retained.
  Publication supersedes that branch choice: `docs/adr-diagrams` is based on
  refreshed `origin/main` at `c437786`, excluding the unrelated Spec 003 commit.
- Authorized scope: `arch.md`, nine additional SVGs in `docs/diagrams/`, and this
  task record. No unresolved architectural choices; preserve Spec 002.
- Expected behavior: all ten ADRs have a local, accessible diagram and a short
  explanation. Option C remains an unimplemented alternative; offsets remain
  scoped by group/topic/partition; cancellation requests never imply stopped work.
- Validation: parse all SVGs, verify image links and one image per ADR, render
  and inspect a contact sheet, check wording against Spec 002 and source, and
  run `git diff --check`.
- Non-goals: runtime changes, merge, new acceptance tests, or a general rewrite
  of the older architecture risk list. The initial no-commit/no-push scope was
  superseded by the user's explicit request to raise a PR.

## Tools and boundaries

- Local Git reads, filesystem edits, Python standard library, and installed
  librsvg/Cairo rendering. No external service or dependency installation needed.
- SVGs are editable vector source. Temporary authoring/rendering files and the
  contact sheet are in `/tmp`; diagrams contain no external assets or scripts.
- Existing `.vscode/` and ADR 9 work are preserved. No claims about GitHub merge
  enforcement or remote permissions are introduced.

## Progress and decisions

- 2026-10-11: read ADRs, Spec 002, and relevant window/encoder source; authored
  nine diagrams with the same palette and typography as ADR 9.
- Rendered all ten SVGs. Initial inspection identified two labels needing
  clarification: replicas share a group per role, and retry visibility need not
  wait for the main worker's acknowledgment callback. Corrected both.
- Added captions with acceptance IDs. Corrected ADR 8's local pause scope and
  distinguished existing tests from the unresolved reservation-cleanup finding.
- 2026-10-11: user requested publication as a PR. Refreshed `origin/main` and
  moved only the documentation changes onto `docs/adr-diagrams`; the existing
  runtime-correctness branch remains intact. Publication is authorized; merging
  is not part of this task.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| SVG structure and links | `f5ccec7` + documentation diff | Python XML and Markdown checks | passed: all 10 ADRs have one matching image; all 10 SVGs have accessible labels and no scripts/external assets | `arch.md`, `docs/diagrams/` |
| Render and visual inspection | same | librsvg/Cairo contact sheet | passed: all 10 render; final contact sheet inspected after label corrections | `/tmp/adr-diagrams-preview.png` (temporary) |
| Invariant wording | same | compare Spec 002 §§1–9 and implementation | passed: group scope, safe prefixes, handoff ACK, intended window thresholds, and alternative status checked | ADR captions and SVG labels |
| Whitespace | same | `git diff --check` | passed | command output |
| Java unit/integration tests | same | not run | documentation-only; no new runtime result claimed | no runtime/build changes |

## Failure analysis and correction

- No technical check failure observed. Initial visual review prompted the group
  ownership and acknowledgment-label corrections above.
- Negative review criterion: any diagram that connects Option C as implemented,
  releases a permit on a cancellation request, advances past incomplete work,
  or treats each replica as a separate consumer group would invalidate it.
- Remaining uncertainty: diagrams simplify flows and do not resolve previously
  reported runtime bugs or prove behavior outside existing test assertions.

## Handoff and review

- Publication baseline: `c437786` on `docs/adr-diagrams`, targeting `main`.
- Completed: nine new illustrations and explanatory captions; prior ADR 9 kept.
- Validation complete: structural, link, rendering, visual, and whitespace checks
  passed. No runtime test execution is claimed.
- Remaining gaps: older sections outside the edited ADRs still contain stale
  coverage statements; broader reconciliation is outside this task.
- Outcome: all ten ADRs illustrated and validated; publish as a documentation-only
  PR for review. Track publication and CI results in the PR; the earlier
  no-publication instruction is superseded by the user's PR request.

## Learning check

- Invariant: an offset may advance only through completed delivered records.
- Failure: finishing offsets 11 and 12 cannot justify skipping unfinished 10.
- Evidence: ADR 4 illustrates the invariant; B05/B15 tests supply execution
  evidence separately.
- Check: after all three complete, what next offset is safe to propose? **13**;
  retained entries release only after the broker acknowledges the commit.
