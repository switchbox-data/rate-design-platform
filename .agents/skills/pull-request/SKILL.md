---
name: pull-request
description: >-
  Open or update a pull request in rate-design-platform. Use when the user asks
  to create a PR, commit and push, update the PR message, or write a PR
  description. Confirm the branch against the issue's linked branches on
  GitHub. Writes the plan to a temporary markdown file and, after approval, commits,
  pushes, or opens the pull request in a new agent session. The body says what
  was done, what remains, and what the reviewer should focus on.
---

# Pull request

A pull request belongs to an existing GitHub issue. Creating a branch from an issue names it `{issue}-short-description` (for example `509-add-winter-peak`) and records that ref on the issue as a linked branch. The link is the check. The name only chooses which issue to ask about.

## Output

The pull request URL. The body matches the branch as it is now: what this PR has done, what remains, and reviewer focus.

## Plan

Follow this order when carrying out the work this skill covers. The user asks for the work. Read this skill and use it to figure out how to do it. Write a plan for how you will do it in a temporary markdown file. The user edits that file. On feedback, update the file and stop again. Repeat until they finalize the approval. When they ask you to execute the finalized file, hand the work to a new agent session.

A question about how the skill works is answered from the skill, without a plan file.

Write the plan to `.tmp/plan-pull-request-<slug>.md`. Create `.tmp/` if it is missing. `.tmp/` is gitignored. Do not commit the plan. Tell them the path and stop. Do not commit, push, or create or edit the pull request in the turn that writes or revises the plan.

When they ask you to execute the finalized file, and this chat wrote the plan, start a new agent session with the Task tool: `subagent_type` generalPurpose, local environment, and no `resume`. The prompt is only the absolute path of the plan file, the absolute path of this skill, and these instructions: you are the execution session; read both files; carry out the plan by following Procedure; do not write a plan; do not start another session. Do not paste the plan or this skill into the prompt. This chat does not re-read the skill and does not do the work. The new session returns only the commit, the push result, and the pull request URL. A plan that does not name a confirmed issue returns that fact and does not commit, push, or open a pull request.

Run the linked-branch check while writing the plan. The plan includes:

- Branch name, owner, repo, and the linked-branch result.
- When the branch belongs to the issue: whether this is a commit, a push, a new pull request, or an edit; the commit message if they asked for a commit; the pull request title; the body filled from the template below; `Closes #<number>` using the confirmed issue number.
- When the branch does not belong to the issue: the scope of the pull request, and whether a new issue should have been created. That plan does not include a commit, a push, or a pull request.

## Which issue this branch belongs to

1. Read the current branch name. The leading number, when the name matches `{number}-…`, is the candidate issue.
2. Confirm it with the issue's linked branches. Take owner and repo from `gh repo view --json nameWithOwner`.

```bash
gh api graphql -f query='
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    issue(number: $number) {
      number
      linkedBranches(first: 20) { nodes { ref { name } } }
    }
  }
}' -f owner=OWNER -f repo=REPO -F number=NUMBER
```

3. The branch belongs to that issue only when `issue` exists and `linkedBranches` includes the current branch name. Use that issue number in `Closes #`.
4. Otherwise the plan records the mismatch and the open questions above. A matching prefix without a linked branch is not enough. If they want an issue, follow the `linear-issue` skill first, create the branch from that issue so GitHub records the link, and use the synced GitHub issue number. That follow-up is its own plan.

## Procedure

The execution session runs this. It does not start another session. Run it only when the plan names a confirmed issue:

1. Review the diff against the base branch so the published body matches the commits on the branch. The approved plan is that body. If a hook or the commit changes the diff so the approved body no longer matches, update the plan and stop for approval again.
2. Write the body with the template below. Create with `gh pr create`, or update an existing PR with `gh pr edit`, using a heredoc. Do not paste the Linear issue into the body.
3. Return the PR URL.

## Commit, push, and update the PR message

When the approved plan includes a commit, a push, and a pull request message update, do all three in this order. Do not stop after the commit.

1. The linked-branch check already passed and is recorded in the plan. A plan that does not name a confirmed issue stops here.
2. Commit the changes they named, using the repository commit workflow: status, diff, and recent commit messages, then one commit whose message says why. The message is the one in the approved plan. Do not push a commit they did not ask for, and do not skip hooks.
3. Push the branch (`git push -u origin HEAD` when it has no upstream).
4. Publish the approved body. It describes the branch as it is after the commit just pushed, including what was done, what remains, and reviewer focus. If a PR for this branch already exists, `gh pr edit` it. If none exists, `gh pr create` it.
5. Return the commit, the push result, and the PR URL.

## Body

```markdown
## What was done

What this branch changes relative to the issue.

## What remains

What the issue still needs after this branch. Write "Nothing" when the issue is fully covered.

## Reviewer focus

The trade-off, alternative, or design choice the reviewer should look at.

Closes #509
```

`Closes #<number>` is the issue number confirmed by `linkedBranches`. A Linear id such as `RDP-126` does not close the GitHub issue.

No "Made with Cursor", "Generated by …", or other model attribution.
