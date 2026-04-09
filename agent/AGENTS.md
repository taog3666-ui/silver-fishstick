# Scoring-Optimized Solver Instructions

Your output is scored by **positional line-level exact matching** against a reference solution.
Each changed line (added or removed) is compared position-by-position in the unified diff.
Score = matched_lines / max(your_lines, reference_lines).
**Example: 10 matches in 50 changed lines = 0.20. Same 10 matches in 15 changed lines = 0.67. Fewer lines = higher score.**

**One extra or missing line at the top of a file shifts ALL subsequent lines — zeroing the entire file's score.**

## Workflow

### Step 1: Read

Read the task carefully. Read each file you will edit **in full** before making any changes.
While reading, note the file's style: indentation (tabs vs spaces, width), quote style, semicolons, trailing commas, naming conventions, spacing.

### Step 2: Edit

Implement each acceptance criterion with the minimum necessary edits. Match surrounding style exactly.

### Step 3: Stop

Stop immediately after editing. Do not summarize, explain, or verify.

## Scoring Rules (memorize these)

1. **Positional alignment is everything.** Your 1st changed line is compared to reference's 1st, your 2nd to reference's 2nd. One shift = catastrophic.
2. **Extra lines inflate the denominator.** Adding an unnecessary line costs more than missing one. When in doubt, leave it out.
3. **Exact string match.** `"  foo()" != "    foo()"`. Whitespace, quotes, spacing — character for character.
4. **Touching an extra file = pure penalty.** If the reference didn't change a file and you did, those lines are all denominator, zero numerator.
5. **Deletions and additions never match each other.** Lines are tagged `-:` or `+:` — they only match their own type.

## Rules

- **Minimal diff.** Change only what the task requires. Every extra changed line hurts your score. Do not touch formatting, imports, comments, or anything the task does not explicitly ask for.
- **Exact style match.** Use the same indentation, quote style, semicolons, trailing commas, naming conventions, and spacing as the surrounding code. Match character-for-character.
- **Direct implementation.** Use the simplest, most straightforward approach. Follow patterns already present in the codebase. Do not introduce abstractions or helpers beyond what the task specifies.
- **No commits.** The evaluation framework captures your diff automatically.
- **Import placement.** When adding imports, place them next to related existing imports. Never reorder or alphabetize. Append new specifiers after existing: `{A}` → `{A, B}` not `{B, A}`.
- **Match existing test patterns.** When adding tests, mirror the file's existing structure, nesting, assertion style, and test-per-feature count.
- **Hardcode feature flags.** When enabling features, use hardcoded sensible defaults. Only parameterize when the task gives exact parameter names to accept.
- **Completeness on renames.** If the task renames X to Y, grep for ALL occurrences: code, comments, strings, docs. Missing one at the top of a file zeros that entire file.
- **Group additions at bottom.** When adding new functions or blocks, place them at the bottom of the relevant section. Changes at the bottom only dilute the denominator slightly; changes at the top zero the numerator.
- **Conservative scope.** When the task describes a class of changes, apply it to the fewest clear-cut instances.
