# GH-600 Lesson 7: Verify GitHub controls and review evidence

Status: in progress

## Scope and plan

- Requirement: exercise the GH-600 guardrails/accountability objective using the
  L06 pull requests; distinguish a successful CI run from an enforced merge gate.
- Guide area: Lesson 7, GitHub controls, CI permissions, review, and recovery.
- Base commit and branch: `1f2fb29` (`origin/main`); documentation closeout is on
  `docs/l06-gh600-review-closeout`.
- Authorized scope: inspect merged PR metadata, check results, branch protection,
  repository rulesets, and the checked-in workflow; record evidence in docs.
- Expected invariant: a green check is advisory unless repository rules require
  it; do not claim that a reviewer approved work when GitHub records no review.
- Files: this task record, [learning guide](../gh-600-learning-guide.md), and
  [L06 task record](L06-backpressure-recovery.md).
- Next steps: obtain repository-owner direction on whether to configure required
  reviews/checks; if authorized, apply a minimal ruleset and demonstrate both a
  blocked failing check and an allowed passing merge. Separately, design a
  restricted-agent-in-CI exercise with no unnecessary write credentials.
- Non-goals: changing branch settings, rulesets, Actions permissions, secrets,
  or workflows without explicit authorization; claiming the agent-in-CI exercise
  is complete.

## Tools and boundaries

- Tool, purpose, scope, permissions: `gh` CLI read-only PR/API queries for
  `spakai/agentic-job-scheduler`; local file inspection of the Actions workflow.
- Reads: merged PR status/check/review metadata, `main` protection endpoint,
  repository rulesets, and workflow permissions.
- Writes: only this repository's documentation closeout. No GitHub settings,
  workflow, or secret changes were made.
- Environment constraints: GitHub reports `main` is not protected; required
  reviews/checks cannot be claimed. No restricted agent is configured in CI.
- Effective boundary: findings describe observed GitHub API responses and the
  workflow file; no broader administrative permissions are inferred.

## Progress and decisions

- PRs #9, #10, and #11 are merged. Each has a successful `Java 21 test and
  package` check. PR #11 merged at `1f2fb29`.
- The GitHub API reports `reviews: []` for PRs #9–#11. No human review approval
  or independent reviewer disposition is evidenced by those PR records.
- `GET /repos/spakai/agentic-job-scheduler/branches/main/protection` returned
  HTTP 404 with `Branch not protected`.
- `GET /repos/spakai/agentic-job-scheduler/rulesets` returned an empty list.
- `.github/workflows/maven.yml` declares `contents: read`, disables persisted
  checkout credentials, pins Actions, and uploads test reports/JAR. It does not
  run an agent or establish a merge restriction.
- Decision: record the lack of enforcement as a verified gap. Do not change
  repository controls as part of this documentation task.

## Evaluation

| Check | Revision | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| L06 PR CI | PRs #9–#11 | GitHub Actions `Java 21 test and package` | passed for all three PRs | [PR #9](https://github.com/spakai/agentic-job-scheduler/pull/9), [PR #10](https://github.com/spakai/agentic-job-scheduler/pull/10), [PR #11](https://github.com/spakai/agentic-job-scheduler/pull/11) |
| PR review records | PRs #9–#11 | `gh pr view --json reviews,comments` | no review entries/comments returned; no independent review claim | GitHub CLI output, 2026-10-11 |
| `main` branch protection | `1f2fb29` | `gh api repos/spakai/agentic-job-scheduler/branches/main/protection` | HTTP 404 `Branch not protected` | GitHub API response, 2026-10-11 |
| Repository rulesets | `1f2fb29` | `gh api repos/spakai/agentic-job-scheduler/rulesets` | empty list | GitHub API response, 2026-10-11 |
| Workflow permissions | `1f2fb29` | inspect `.github/workflows/maven.yml` | configured `contents: read`; checkout credentials not persisted; no agent step | checked-in workflow |
| Required check/review enforcement | n/a | merge-blocking test | not exercised; no protection/rulesets exist | no claim |
| Restricted agent in CI | n/a | synthetic failure / prompt-injection exercise | not configured or run | no claim |

## Failure analysis and correction

- Observed failure: no technical failure; the inspection found missing enforcement.
- Classification: repository configuration gap.
- Root cause supported by evidence: the branch protection endpoint returns 404,
  the rulesets endpoint returns no rules, and the merged PRs have no review records.
- Corrective action: document this gap instead of treating passing CI or merged
  PRs as proof of enforced review/check policy.
- Rerun and remaining uncertainty: none needed for the read-only inspection;
  branch rules and agent-in-CI remain open pending repository-owner direction.

## Handoff and review

- Latest verified baseline: `1f2fb29`; no GitHub configuration changes made.
- Completed: inspected PR/CI records, branch protection, rulesets, and workflow
  permission declarations; updated the learning ledger.
- Next concrete step: repository owner decides whether to enable required reviews
  and checks; then run safe negative/positive merge-gate tests. Separately plan a
  restricted-agent workflow with fake prompt-injection input and no write token.
- Open risks/decisions and owner: repository owner must decide whether to enable
  branch/ruleset enforcement and authorize any settings changes.
- Reviewer findings: no GitHub PR review records found for PRs #9–#11.
- Outcome: GH-600 Lesson 7 is partially exercised; enforcement and agent-in-CI
  are not complete.

## Learning check

- Invariant: a passing CI result is not a merge gate unless repository policy
  technically requires it.
- Failure that invalidates the claim: a change merges while a required check is
  failing, or without required approval.
- Evidence: branch protection 404, empty rulesets, and the successful merged PR
  checks above.
- Check of understanding: what additional evidence would prove that CI blocks a
  merge rather than merely reporting a result?
