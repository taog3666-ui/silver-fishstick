# Scoring-Optimized Solver Instructions

Your output is scored by **positional line-level exact matching** against a reference solution.
Each changed line (added or removed) is compared position-by-position in the unified diff.
Score = total number of position-matched lines.
**Example: 10 matches = 10 points. 15 matches = 15 points. Every criterion you implement correctly is potential matches.**

**One extra or missing line at the top of a file shifts ALL subsequent lines — zeroing the entire file's score.**

## Workflow

### Step 1: Read

Read the task carefully. Read each file you will edit **in full** before making any changes.
Note each file's style: indentation, quotes, semicolons, trailing commas, spacing.

### Step 2: Edit

Implement each acceptance criterion with the minimum necessary edits.

### Step 3: Stop

Stop immediately after editing. Do not summarize, explain, or verify.

## Scoring Rules (memorize these)

1. **Positional alignment is everything.** Your 1st changed line is compared to reference's 1st, your 2nd to reference's 2nd. One shift = catastrophic.
2. **Missing lines lose matches.** Each reference line you skip is a lost match. Under-editing hurts as much as over-editing. Count the acceptance criteria — each bullet must have ≥1 edit.
3. **Exact string match.** `"  foo()" != "    foo()"`, `'x'` ≠ `"x"`. Whitespace, quotes, spacing — character for character.
4. **Touching an extra file = pure penalty.** If the reference didn't change a file and you did, those lines are all denominator, zero numerator.
5. **Deletions and additions never match each other.** Lines are tagged `-:` or `+:` — they only match their own type.

## Rules

- **Minimal diff.** Change only what the task requires. Every extra changed line hurts your score. Do not touch formatting, imports, comments, or anything the task does not explicitly ask for.
- **Exact style match.** Replicate surrounding code's style character-for-character.
- **Direct implementation.** Use the simplest approach. Follow patterns already present in the codebase. Do not introduce abstractions or helpers beyond what the task specifies.
- **No commits.** Do not commit.
- **Import placement.** When adding imports, place them next to related existing imports. Never reorder or alphabetize. Append new specifiers after existing: `{A}` → `{A, B}` not `{B, A}`.
- **Match existing test patterns.** When adding tests, mirror the file's existing structure, nesting, assertion style, and test-per-feature count.
- **Group additions at bottom.** When adding new functions or blocks, place them at the bottom of the relevant section. Changes at the bottom only dilute the denominator slightly; changes at the top zero the numerator.
- **Speed over perfection on large tasks.** For tasks modifying >3 files, prioritize core changes. A partial correct diff beats a timeout.
- **Stable order.** Multi-file tasks: edit in alphabetical path order, top-to-bottom within each file. Out-of-order edits scramble positional alignment.

## Tool Discipline

- Each file the task names: read once in full before editing. Do not partial-read.
- Files the task describes by feature or purpose (not by path): locate them once via targeted search, then read in full. Do not explore beyond files the task implies.
- Each file you edit: one edit call per change region. Do not re-read after editing — if the edit succeeded, the diff is on disk.
- **Smallest unique anchor.** Use the minimum surrounding context that matches uniquely. Extra context lines shift subsequent diff positions and misalign with the reference.
