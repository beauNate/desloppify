"""Helper logic for plan resolve command workflow/triage gating."""

from __future__ import annotations

from desloppify.base.output.terminal import colorize
from desloppify.engine.plan import (
    TRIAGE_IDS,
    TRIAGE_STAGE_DEPENDENCIES,
    TRIAGE_STAGE_IDS,
    confirmed_triage_stage_names,
    recorded_unconfirmed_triage_stage_names,
)

_CLUSTER_INDIVIDUAL_THRESHOLD = 10


def check_cluster_guard(patterns: list[str], plan: dict, state: dict) -> bool:
    """Return True if blocked by cluster guard, False if OK to proceed."""
    clusters = plan.get("clusters", {})
    issues = state.get("issues", {})
    for pattern in patterns:
        if pattern in clusters:
            cluster = clusters[pattern]
            issue_ids = [
                issue_id
                for issue_id in cluster.get("issue_ids", [])
                if issue_id in issues and issues[issue_id].get("status") == "open"
            ]
            if len(issue_ids) == 0:
                print(
                    colorize(
                        f"\n  Cluster '{pattern}' is empty — add items before marking it done.\n",
                        "yellow",
                    )
                )
                print(colorize(f"  Use: desloppify plan cluster add {pattern} <issue-id>", "dim"))
                return True
            if len(issue_ids) <= _CLUSTER_INDIVIDUAL_THRESHOLD:
                print_cluster_guard(pattern, issue_ids, state)
                return True
    return False


def print_cluster_guard(cluster_name: str, issue_ids: list[str], state: dict) -> None:
    issues = state.get("issues", {})
    print(
        colorize(
            f"\n  Cluster '{cluster_name}' has {len(issue_ids)} item(s) — mark them done individually first:\n",
            "yellow",
        )
    )
    for issue_id in issue_ids:
        issue = issues.get(issue_id, {})
        summary = issue.get("summary", "(no summary)")[:80]
        detector = issue.get("detector", "?")
        print(f"    {issue_id}  [{detector}]  {summary}")
    print(
        colorize(
            "\n  Use: desloppify resolve <id> --status fixed --note '...' --attest '...'",
            "dim",
        )
    )
    print(
        colorize(
            "  Or mark each resolved: desloppify plan resolve <id> --note '...' --confirm\n",
            "dim",
        )
    )


def is_synthetic_id(issue_id: str) -> bool:
    """Return True if the ID is a synthetic workflow/triage item."""
    return (
        issue_id.startswith("triage::")
        or issue_id.startswith("workflow::")
        or issue_id.startswith("subjective::")
    )


def resolve_synthetic_ids(patterns: list[str]) -> tuple[list[str], list[str]]:
    """Separate synthetic IDs from real issue patterns."""
    synthetic = [pattern for pattern in patterns if is_synthetic_id(pattern)]
    remaining = [pattern for pattern in patterns if not is_synthetic_id(pattern)]
    return synthetic, remaining


def blocked_triage_stages(plan: dict) -> dict[str, list[str]]:
    """Return triage stages that are blocked by unmet dependencies."""
    order_set = set(plan.get("queue_order", []))
    stage_names = ("observe", "reflect", "organize", "enrich", "sense-check", "commit")
    present_names = {
        name
        for stage_id, name in zip(TRIAGE_STAGE_IDS, stage_names, strict=False)
        if stage_id in (order_set & TRIAGE_IDS)
    }
    present_names.update(recorded_unconfirmed_triage_stage_names(plan.get("epic_triage_meta", {})))
    if not present_names:
        return {}

    confirmed = confirmed_triage_stage_names(plan.get("epic_triage_meta", {}))

    blocked: dict[str, list[str]] = {}
    for stage_id, name in zip(TRIAGE_STAGE_IDS, stage_names, strict=False):
        if name not in present_names or name in confirmed:
            continue
        deps = TRIAGE_STAGE_DEPENDENCIES.get(name, set())
        unmet = sorted(
            f"triage::{dep}" for dep in deps if dep in present_names and dep not in confirmed
        )
        if unmet:
            blocked[stage_id] = unmet
    return blocked


__all__ = [
    "blocked_triage_stages",
    "check_cluster_guard",
    "resolve_synthetic_ids",
]
