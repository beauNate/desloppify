"""Sense-check stage command flow."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path

from desloppify.base.output.terminal import colorize

from .helpers import print_cascade_clear_feedback
from .services import TriageServices, default_triage_services
from .stage_flow_enrich import ColorizeFn


def record_sense_check_stage(
    stages: dict,
    *,
    report: str,
    existing_stage: dict | None,
    is_reuse: bool,
    utc_now_fn: Callable[[], str],
    cascade_clear_later_confirmations_fn: Callable[[dict, str], list[str]],
) -> list[str]:
    """Persist the sense-check stage and clear later confirmations as needed."""
    stages["sense-check"] = {
        "stage": "sense-check",
        "report": report,
        "timestamp": utc_now_fn(),
    }
    if is_reuse and existing_stage and existing_stage.get("confirmed_at"):
        stages["sense-check"]["confirmed_at"] = existing_stage["confirmed_at"]
        stages["sense-check"]["confirmed_text"] = existing_stage.get("confirmed_text", "")
    return cascade_clear_later_confirmations_fn(stages, "sense-check")


def run_stage_sense_check(
    args: argparse.Namespace,
    *,
    services: TriageServices | None,
    has_triage_in_queue_fn: Callable[[dict], bool],
    resolve_reusable_report_fn: Callable[[str | None, dict | None], tuple[str | None, bool]],
    record_sense_check_stage_fn: Callable[..., list[str]],
    colorize_fn: ColorizeFn = colorize,
    default_triage_services_fn: Callable[[], TriageServices] = default_triage_services,
    print_cascade_clear_feedback_fn: Callable[[list[str], dict], None] = print_cascade_clear_feedback,
    get_project_root_fn: Callable[[], Path] | None = None,
    underspecified_steps_fn: Callable[[dict], list[tuple[str, int, int]]] | None = None,
    steps_missing_issue_refs_fn: Callable[[dict], list[tuple[str, int, int]]] | None = None,
    steps_with_bad_paths_fn: Callable[[dict, Path], list[tuple[str, int, list[str]]]] | None = None,
    steps_with_vague_detail_fn: Callable[[dict, Path], list[tuple[str, int, str]]] | None = None,
    steps_without_effort_fn: Callable[[dict], list[tuple[str, int, int]]] | None = None,
) -> None:
    """Record the SENSE-CHECK stage after rerunning enrich-level validations."""
    report: str | None = getattr(args, "report", None)

    resolved_services = services or default_triage_services_fn()
    plan = resolved_services.load_plan()

    if not has_triage_in_queue_fn(plan):
        print(colorize_fn("  No planning stages in the queue — nothing to sense-check.", "yellow"))
        return

    meta = plan.get("epic_triage_meta", {})
    stages = meta.get("triage_stages", {})

    existing_stage = stages.get("sense-check")
    report, is_reuse = resolve_reusable_report_fn(report, existing_stage)

    if not stages.get("enrich", {}).get("confirmed_at"):
        print(colorize_fn("  Cannot sense-check: enrich stage not confirmed.", "red"))
        print(colorize_fn("  Run: desloppify plan triage --confirm enrich", "dim"))
        return

    if get_project_root_fn is None:
        from desloppify.base.discovery.paths import get_project_root

        get_project_root_fn = get_project_root

    if (
        underspecified_steps_fn is None
        or steps_missing_issue_refs_fn is None
        or steps_with_bad_paths_fn is None
        or steps_with_vague_detail_fn is None
        or steps_without_effort_fn is None
    ):
        from ._stage_validation import (
            _steps_missing_issue_refs,
            _steps_with_bad_paths,
            _steps_with_vague_detail,
            _steps_without_effort,
            _underspecified_steps,
        )

        underspecified_steps_fn = _underspecified_steps
        steps_missing_issue_refs_fn = _steps_missing_issue_refs
        steps_with_bad_paths_fn = _steps_with_bad_paths
        steps_with_vague_detail_fn = _steps_with_vague_detail
        steps_without_effort_fn = _steps_without_effort

    repo_root = get_project_root_fn()
    problems: list[str] = []

    underspec = underspecified_steps_fn(plan)
    if underspec:
        total_bare = sum(n for _, n, _ in underspec)
        problems.append(f"{total_bare} step(s) lack detail or issue_refs")

    bad_paths = steps_with_bad_paths_fn(plan, repo_root)
    if bad_paths:
        total_bad = sum(len(bp) for _, _, bp in bad_paths)
        problems.append(f"{total_bad} file path(s) don't exist on disk")

    untagged = steps_without_effort_fn(plan)
    if untagged:
        total_missing = sum(n for _, n, _ in untagged)
        problems.append(f"{total_missing} step(s) have no effort tag")

    no_refs = steps_missing_issue_refs_fn(plan)
    if no_refs:
        total_missing = sum(n for _, n, _ in no_refs)
        problems.append(f"{total_missing} step(s) have no issue_refs")

    vague = steps_with_vague_detail_fn(plan, repo_root)
    if vague:
        problems.append(f"{len(vague)} step(s) have vague detail")

    if problems:
        print(colorize_fn("  Cannot record sense-check — plan still has issues:", "red"))
        for problem in problems:
            print(colorize_fn(f"    • {problem}", "yellow"))
        print(colorize_fn("  Fix these before recording the sense-check stage.", "dim"))
        return

    print(colorize_fn("  All enrich-level checks pass after sense-check.", "green"))

    if not report:
        print(colorize_fn("  --report is required for --stage sense-check.", "red"))
        print(
            colorize_fn(
                "  Describe what the content and structure subagents found and fixed.",
                "dim",
            )
        )
        return

    if len(report) < 100:
        print(colorize_fn(f"  Report too short: {len(report)} chars (minimum 100).", "red"))
        return

    # --- Sense-check evidence: file paths + cluster names ---
    from ._stage_evidence_parsing import (
        EvidenceFailure,
        format_evidence_failures,
        validate_report_has_file_paths,
        validate_report_references_clusters,
    )
    from .helpers import manual_clusters_with_issues

    sc_evidence_failures: list[EvidenceFailure] = []
    path_failures = validate_report_has_file_paths(report)
    if path_failures:
        sc_evidence_failures.extend(path_failures)

    cluster_names = manual_clusters_with_issues(plan)
    cluster_failures = validate_report_references_clusters(report, cluster_names)
    if cluster_failures:
        sc_evidence_failures.extend(cluster_failures)

    blocking_ev = [f for f in sc_evidence_failures if f.blocking]
    advisory_ev = [f for f in sc_evidence_failures if not f.blocking]
    if blocking_ev:
        msg = format_evidence_failures(blocking_ev, stage_label="sense-check")
        print(colorize_fn(msg, "red"))
        return
    if advisory_ev:
        msg = format_evidence_failures(advisory_ev, stage_label="sense-check")
        print(colorize_fn(msg, "yellow"))

    stages = meta.setdefault("triage_stages", {})
    cleared = record_sense_check_stage_fn(
        stages,
        report=report,
        existing_stage=existing_stage,
        is_reuse=is_reuse,
    )

    resolved_services.save_plan(plan)

    resolved_services.append_log_entry(
        plan,
        "triage_sense_check",
        actor="user",
        detail={"reuse": is_reuse},
    )
    resolved_services.save_plan(plan)

    print(colorize_fn("  Sense-check stage recorded.", "green"))
    if is_reuse:
        print(colorize_fn("  Sense-check data preserved (no changes).", "dim"))
        if cleared:
            print_cascade_clear_feedback_fn(cleared, stages)
    else:
        print(colorize_fn("  Now confirm the sense-check.", "yellow"))
        print(colorize_fn("    desloppify plan triage --confirm sense-check", "dim"))


__all__ = ["record_sense_check_stage", "run_stage_sense_check"]
