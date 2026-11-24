---
name: linear-issue
description: >-
  Create or update a Linear issue for rate-design-platform. Use when the user
  asks for a ticket, issue, or Linear update, says to turn this conversation
  into a Linear ticket, or names an RDP identifier. Drafts the issue, team,
  project, assignee, and a 1–5 point estimate in a temporary markdown file,
  and, after that file is approved, creates or updates the issue in a new
  agent session.
---

# Linear issue

Issues live in Linear and sync to GitHub. Create and update them with the Linear MCP tools. Do not write the issue body under `context/` or `.github/`, and do not commit it. The draft the user edits is the temporary plan under `.tmp/`.

If the Linear MCP tools are not available, say so and stop. Do not draft a GitHub issue as a substitute.

## Output

A Linear issue with What, Why, How, and Deliverables, on the team, project, and assignee the user confirmed, with an estimate from 1 to 5. Report the Linear identifier and the URL.

## Plan

Follow this order when carrying out the work this skill covers. The user asks for the work. Read this skill and use it to figure out how to do it. Write a plan for how you will do it in a temporary markdown file. The user edits that file. On feedback, update the file and stop again. Repeat until they finalize the approval. When they ask you to execute the finalized file, hand the work to a new agent session.

A question about how the skill works is answered from the skill, without a plan file.

Write the plan to `.tmp/plan-linear-issue-<slug>.md`. Create `.tmp/` if it is missing. `.tmp/` is gitignored. Tell them the path and stop. Do not create or update the Linear issue in the turn that writes or revises the plan. Leave Project, Assignee, and Estimate blank in the draft. Do not guess them.

When they ask you to execute the finalized file, and this chat wrote the plan, start a new agent session with the Task tool: `subagent_type` generalPurpose, local environment, and no `resume`. The prompt is only the absolute path of the plan file, the absolute path of this skill, and these instructions: you are the execution session; read both files; carry out the plan by following Procedure; do not write a plan; do not start another session. Do not paste the plan or this skill into the prompt. This chat does not re-read the skill and does not do the work. The new session returns only the Linear identifier and the URL. If Project, Assignee, or Estimate is still blank, it returns the blank field and does not create or update the issue.

Search Linear before writing the plan. If the user named an identifier, the plan updates that issue: copy its current team, project, assignee, and estimate, and change those four only when they asked to. Otherwise the plan creates an issue, and the draft title and sections come from the conversation.

The plan uses this shape. Title and sections follow **Issue fields**. Status follows **Status**.

```markdown
# <title>

- Action: create | update <identifier>
- Team: RDP/rate-design-platform
- Project:
- Assignee:
- Estimate:
- Status: Backlog
- Milestone:

## What

## Why

## How

## Deliverables
```

- **Team.** Default **RDP/rate-design-platform**. Offer that default in the file and keep it when they leave it. Team RDP syncs to the GitHub repo `switchbox-data/rate-design-platform`.
- **Project.** No default.
- **Assignee.** No default.
- **Estimate.** An integer from 1 to 5. The scale is 1–5 points.

## Procedure

The execution session runs this. It does not start another session. Create or update the issue from the plan file, then set status. Return the Linear identifier and the URL. If Project, Assignee, or Estimate is still blank, return the blank field and do not create or update the issue.

## Issue fields

**Title.** Starts with a verb. Example: `Add winter peak analysis`.

**Description.**

- **What.** What is being built, changed, or decided, in a few sentences.
- **Why.** Why it matters, what problem it solves, and what it unblocks.
- **How.** Numbered steps, trade-offs, and dependencies. Skip only when What is already the whole implementation.
- **Deliverables.** Concrete outputs that define done. Examples: "PR that adds …", "Tests for …", "Data in `s3://…`". Vague lines ("Finish the analysis", "Make it better") are not deliverables.

**Status.** Default **Backlog** for a new issue.

**Milestone.** Set when one applies. When it is unclear, leave it blank and name the candidates in a one-line note under the field. Leave it blank when no milestone fits.

## Status

Move status as the work moves. Do not leave an in-progress issue in Backlog.

| From         | To           | When                                                         |
| ------------ | ------------ | ------------------------------------------------------------ |
| Backlog      | To Do        | Picked for the current sprint                                |
| To Do        | In Progress  | Work has started (a branch exists for code issues)           |
| In Progress  | Under Review | PR is ready for review, or the findings are written up       |
| Under Review | Done         | PR merged (auto-closes), or the reviewer approves and closes |

Valid statuses: Backlog, To Do, In Progress, Under Review, Done.
