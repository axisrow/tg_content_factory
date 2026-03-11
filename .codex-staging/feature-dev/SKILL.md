---
name: feature-dev
description: Structured feature development workflow with codebase exploration, architecture tradeoffs, implementation gating, and review
version: 1.0.0
metadata:
  source: claude-plugins-official/feature-dev
---

# Feature Dev Skill

Use this skill when the user wants to build a new feature and would benefit from a deliberate workflow instead of immediate implementation.

## Goal

Drive feature work through seven phases:

1. Discovery
2. Codebase exploration
3. Clarifying questions
4. Architecture design
5. Implementation
6. Quality review
7. Summary

## Operating Rules

- Do not jump straight into code for ambiguous feature work.
- Read the codebase before proposing implementation details.
- Ask clarifying questions after exploration and before architecture selection.
- Present concrete tradeoffs, not abstract options.
- Do not start implementation until the user approves an approach.
- After implementation, run a review pass focused on bugs, regressions, and convention mismatches.

## Workflow

### 1. Discovery

- Restate the feature request in concrete terms.
- If the request is underspecified, ask what problem is being solved, required behavior, constraints, and non-goals.
- Create a task plan that tracks all seven phases.

### 2. Codebase Exploration

Investigate the codebase from multiple angles. Parallelize local reads and searches when possible.

Cover at least these perspectives:
- Similar or adjacent features
- Relevant architecture layers and extension points
- Existing tests, conventions, and user flows

For each perspective, produce:
- Key files with path references
- Relevant control flow and abstractions
- Constraints that affect implementation

After exploration, read the most important files and summarize patterns that the new feature should follow.

### 3. Clarifying Questions

Before designing the solution, identify ambiguities such as:
- Edge cases
- Error handling
- Scope boundaries
- Integration points
- Backward compatibility
- Performance or security requirements

Ask the user a concise grouped list of questions and wait for answers. If the user delegates the choice, give a recommendation and get explicit confirmation.

### 4. Architecture Design

Develop 2-3 implementation approaches with distinct tradeoffs:
- Minimal changes
- Clean architecture
- Pragmatic balance

For each approach, include:
- Files to change or create
- Main abstractions
- Benefits
- Costs and risks

Recommend one approach and explain why it fits this codebase and request. Ask the user which approach to use.

### 5. Implementation

Do not begin until the user explicitly approves an approach.

When implementing:
- Follow existing project patterns
- Keep changes scoped to the agreed design
- Update the task plan as work progresses
- Add tests when the codebase supports them

### 6. Quality Review

Review the resulting changes from three lenses:
- Simplicity, DRY, and maintainability
- Bugs, correctness, and regressions
- Project conventions and abstraction fit

Report the highest-signal findings first. If issues exist, ask whether to fix now, defer, or proceed as-is.

### 7. Summary

Close with:
- What was built
- Key decisions
- Main files changed
- Remaining risks or next steps

## Specialist Lenses

Use these role prompts internally when useful:

- [code-explorer](references/code-explorer.md): trace feature flows and architecture
- [code-architect](references/code-architect.md): produce decisive implementation blueprints
- [code-reviewer](references/code-reviewer.md): review for real bugs and convention drift

You do not need separate agents to use this skill. Reuse the lenses as structured thinking modes during exploration, design, and review.
