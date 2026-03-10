"""Observe-batch prompt builders for triage runner."""

from __future__ import annotations

from pathlib import Path


def _observe_batch_instructions(issue_count: int, total_batches: int) -> str:
    return f"""\
## OBSERVE Batch Instructions

You are one of {total_batches} parallel observe batches. Your task: verify every issue
assigned to you against the actual source code.

**The review system has a high false-positive rate.** Issues frequently:
- Claim "12 unsafe casts" when there are actually 2
- Describe code that was already refactored
- Propose over-engineering that would make things worse
- Count props/returns/args wrong

Your job is to catch these. A report that just restates issue titles is **worthless**.
The value you add is reading the actual code and forming an independent judgment.

Do NOT analyze themes, strategy, or relationships between issues. Just verify: is each issue real?

**For EVERY issue you must:**
- Open and read the actual source file
- Verify specific claims: count the actual casts, props, returns, line count
- Check if the suggested fix already exists (common false positive)
- Report a clear verdict: genuine / false positive / exaggerated / over-engineering

**What a GOOD report looks like:**
- "[34580232] taskType is plain string — FALSE POSITIVE. Uses branded string union KnownTaskType
  with ~25 literals in src/types/database.ts line 50. The issue describes code that doesn't exist."
- "[b634fc71] useGenerationsPaneController returns 60+ values — GENUINE. Confirmed 65 properties
  at lines 217-282. Mixes pane lifecycle, filters, gallery data, interaction, and navigation."

**What a LAZY report looks like (will be rejected):**
- "There are several convention issues that should be addressed"
- "The type safety dimension has some genuine concerns"
- Listing issue titles without any verification or independent analysis

**Your report must include for EVERY issue ({issue_count} total):**
1. The issue hash
2. Your verdict (genuine / false positive / exaggerated / over-engineering)
3. Your verdict reasoning (what you found when you read the code)
4. The file paths you actually read
5. Your recommendation

## IMPORTANT: Output Rules

**Do NOT run any `desloppify` commands.** Do NOT run `desloppify plan triage --stage observe`.
You are a parallel batch — the orchestrator will merge all batch outputs and record the stage.

**Write your analysis as plain text only.**
**Do NOT use the old one-line `[hash] VERDICT — evidence` format.**
Use this structured template for EVERY issue:
```
- hash: <issue hash>
  verdict: genuine | false-positive | exaggerated | over-engineering
  verdict_reasoning: <what you verified in the code and why that leads to this verdict>
  files_read: [<file paths you opened>]
  recommendation: <what to do next>
```

Before finishing, do a self-check:
- Every issue in the batch has one entry
- Every entry has a non-empty `files_read` list
- Every entry has a concrete `recommendation`
"""


def build_observe_batch_prompt(
    batch_index: int,
    total_batches: int,
    dimension_group: list[str],
    issues_subset: dict[str, dict],
    *,
    repo_root: Path,
) -> str:
    """Build a scoped observe prompt for a single dimension-group batch.

    Unlike build_stage_prompt(), this produces a prompt for observe only,
    scoped to a subset of issues. The batch subprocess writes analysis to
    stdout — it does NOT run ``desloppify plan triage --stage observe``.
    The orchestrator merges batch outputs and records observe once.
    """
    parts: list[str] = []

    # Batch context header
    parts.append(
        f"You are observe batch {batch_index}/{total_batches}.\n"
        f"Dimensions assigned to you: {', '.join(dimension_group)}\n"
        f"Total issues in this batch: {len(issues_subset)}\n\n"
        f"Repo root: {repo_root}"
    )

    # Issue data — inline the subset directly
    parts.append("## Issues to Verify\n")
    for fid, f in sorted(issues_subset.items()):
        detail = f.get("detail", {}) if isinstance(f.get("detail"), dict) else {}
        dim = detail.get("dimension", "unknown")
        title = f.get("title", fid)
        file_path = detail.get("file_path", "")
        description = detail.get("description", f.get("description", ""))
        line = f"- [{fid[:8]}] ({dim}) **{title}**"
        if file_path:
            line += f" — `{file_path}`"
        if description:
            line += f"\n  {description[:300]}"
        parts.append(line)

    # Batch-specific observe instructions (no subagent/CLI references)
    parts.append(_observe_batch_instructions(len(issues_subset), total_batches))

    return "\n\n".join(parts)


__all__ = ["_observe_batch_instructions", "build_observe_batch_prompt"]
