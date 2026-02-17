# AGENTS.md

These rules are mandatory for any coding agent working in this repo.

## Timer Safety
- MUST NOT call the live DataSync API until the user types exactly: START TIMER.
- Before START TIMER, use mocks/stubs only.

## No AI References
- MUST NOT include any references to AI tools, Codex, or development assistance in:
  - source code
  - comments
  - README
  - commit messages
- Disclosure will be handled manually at the end.

## Git Workflow
- MUST NOT commit directly to `main`.
- MUST use feature branches only: `feature/<short-kebab-name>`

### End-of-Milestone Checklist (Required)
At the end of each milestone, the agent MUST:
1) Run tests and show output (`npm test`)
2) Show `git status`
3) Create ONE commit (Conventional Commits: feat/fix/test/docs/refactor/chore)
   - No micro-commits
   - If minor fixes are needed, amend the previous commit
4) STOP and ask exactly: `APPROVE PUSH?`
5) Wait for exact user reply: `APPROVED TO PUSH`
6) Push the branch: `git push -u origin <current-branch>`
7) Print exactly: `PUSH COMPLETE`

## PR Creation (GitHub CLI)
After pushing:
- Create/overwrite `PR_DESCRIPTION.md` with PR body content
- Create PR using:
  `gh pr create --title "<commit-title>" --body-file PR_DESCRIPTION.md --base main`
- If PR creation succeeds, print: `PR CREATED: <url>`
- If PR creation fails, print: `PR READY (MANUAL)` and stop
- MUST NOT proceed until user says: `OK NEXT`

## Commit Pacing
- Commits must represent meaningful milestones.
- MUST NOT create rapid consecutive commits.
- MUST batch related edits into a single milestone commit.