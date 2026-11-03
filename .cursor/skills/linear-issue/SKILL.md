---
name: linear-issue
description: >-
  Create or update a Linear issue for rate-design-platform. Use when the user
  asks for a ticket, issue, or Linear update, says to turn this conversation
  into a Linear ticket, or names an RDP identifier. Asks for team, project,
  assignee, and a 1–5 point estimate before creating.
---

# Linear issue

Issues live in Linear and sync to GitHub. Create and update them with the Linear MCP tools. Do not write the issue body into the repo (`context/`, `.github/`, or a markdown file).

If the Linear MCP tools are not available, say so and stop. Do not draft a GitHub issue as a substitute.

## Output

A Linear issue with What, Why, How, and Deliverables, on the team, project, and assignee the user confirmed, with an estimate from 1 to 5. Report the Linear identifier and the URL.

## Procedure

1. Search Linear for an existing issue before creating one. If the user named an identifier, update that issue and skip the questions below unless they asked to change team, project, assignee, or estimate.
2. When they ask to turn this conversation into a ticket, draft the title and the What, Why, How, and Deliverables from the conversation. Show that draft in the same message as the questions below. Do not create the issue in that message.
3. Before creating, ask the user for all four of these. Do not create the issue until they answer. Do not guess.
   - **Team.** Default **RDP/rate-design-platform**. Offer that default and use it when they accept. Team RDP syncs to the GitHub repo `switchbox-data/rate-design-platform`.
   - **Project.** No default.
   - **Assignee.** No default.
   - **Estimate.** An integer from 1 to 5. The scale is 1–5 points.
4. After they answer, create the issue with the draft and their answers, then set status. Report the Linear identifier and the URL.

## Issue fields

**Title.** Starts with a verb. Example: `Add winter peak analysis`.

**Description.**

- **What.** What is being built, changed, or decided, in a few sentences.
- **Why.** Why it matters, what problem it solves, and what it unblocks.
- **How.** Numbered steps, trade-offs, and dependencies. Skip only when What is already the whole implementation.
- **Deliverables.** Concrete outputs that define done. Examples: "PR that adds …", "Tests for …", "Data in `s3://…`". Vague lines ("Finish the analysis", "Make it better") are not deliverables.

**Status.** Default **Backlog** for a new issue.

**Milestone.** Set when one applies. Ask if it is unclear. Skip only when no milestone fits.

## Status

Move status as the work moves. Do not leave an in-progress issue in Backlog.

| From         | To           | When                                                         |
| ------------ | ------------ | ------------------------------------------------------------ |
| Backlog      | To Do        | Picked for the current sprint                                |
| To Do        | In Progress  | Work has started (a branch exists for code issues)           |
| In Progress  | Under Review | PR is ready for review, or the findings are written up       |
| Under Review | Done         | PR merged (auto-closes), or the reviewer approves and closes |

Valid statuses: Backlog, To Do, In Progress, Under Review, Done.
