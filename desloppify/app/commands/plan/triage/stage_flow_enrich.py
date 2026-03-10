"""Enrich stage command flow."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path

from desloppify.base.output.terminal import colorize
from desloppify.base.output.user_message import print_user_message

from .helpers import print_cascade_clear_feedback
from .services import TriageServices, default_triage_services

ColorizeFn = Callable[[str, str], str]


def run_stage_enrich(
    args: argparse.Namespace,
    *,
    services: TriageServices | None,
    has_triage_in_queue_fn: Callable[[dict], bool],
    require_organize_stage_for_enrich_fn: Callable[[dict], bool],
    underspecified_steps_fn: Callable[[dict], list[tuple[str, int, int]]],
    steps_with_bad_paths_fn: Callable[[dict, Path], list[tuple[str, int, list[str]]]],
    steps_without_effort_fn: Callable[[dict], list[tuple[str, int, int]]],
    enrich_report_or_error_fn: Callable[[str | None], str | None],
    resolve_reusable_report_fn: Callable[[str | None, dict | None], tuple[str | None, bool]],
    record_enrich_stage_fn: Callable[..., list[str]],
    colorize_fn: ColorizeFn = colorize,
    print_user_message_fn: Callable[[str], None] = print_user_message,
    print_cascade_clear_feedback_fn: Callable[[list[str], dict], None] = print_cascade_clear_feedback,
    default_triage_services_fn: Callable[[], TriageServices] = default_triage_services,
    get_project_root_fn: Callable[[], Path] | None = None,
    auto_confirm_organize_for_complete_fn: Callable[..., bool] | None = None,
) -> None:
    """Record the ENRICH stage with validation and optional auto-confirm."""
    report: str | None = getattr(args, "report", None)
    attestation: str | None = getattr(args, "attestation", None)

    resolved_services = services or default_triage_services_fn()
    plan = resolved_services.load_plan()

    if not has_triage_in_queue_fn(plan):
        print(colorize_fn("  No planning stages in the queue — nothing to enrich.", "yellow"))
        return

    meta = plan.get("epic_triage_meta", {})
    stages = meta.get("triage_stages", {})

    existing_stage = stages.get("enrich")
    report, is_reuse = resolve_reusable_report_fn(report, existing_stage)

    if not require_organize_stage_for_enrich_fn(stages):
        return

    if not stages.get("organize", {}).get("confirmed_at"):
        if attestation:
            if auto_confirm_organize_for_complete_fn is None:
                from ._stage_validation import _auto_confirm_organize_for_complete

                auto_confirm_organize_for_complete_fn = _auto_confirm_organize_for_complete
            if not auto_confirm_organize_for_complete_fn(
                plan=plan,
                stages=stages,
                attestation=attestation,
                save_plan_fn=resolved_services.save_plan,
            ):
                return
        else:
            print(colorize_fn("  Cannot enrich: organize stage not confirmed.", "red"))
            print(colorize_fn("  Run: desloppify plan triage --confirm organize", "dim"))
            print(colorize_fn("  Or pass --attestation to auto-confirm organize inline.", "dim"))
            return

    # --- Check cluster_update operations since organize ---
    if not is_reuse:
        organize_ts = stages.get("organize", {}).get("timestamp", "")
        if organize_ts:
            from .helpers import count_log_activity_since

            activity = count_log_activity_since(plan, organize_ts)
            update_ops = activity.get("cluster_update", 0)
            if update_ops == 0:
                if attestation and len(attestation.strip()) >= 40:
                    print(colorize_fn(
                        "  Note: 0 cluster_update ops logged since organize. "
                        "Proceeding with attestation override.",
                        "yellow",
                    ))
                else:
                    print(colorize_fn(
                        "  Cannot enrich: no cluster_update operations logged since organize.",
                        "red",
                    ))
                    print(colorize_fn(
                        "  Enriching steps requires running cluster update commands.\n"
                        '  e.g. desloppify plan cluster update <name> --update-step N --detail "..."',
                        "dim",
                    ))
                    print(colorize_fn(
                        '  Override: pass --attestation "reason why no update ops" (40+ chars).',
                        "dim",
                    ))
                    return

    underspec = underspecified_steps_fn(plan)
    total_bare = sum(n for _, n, _ in underspec)

    if underspec:
        print(
            colorize_fn(
                f"  Cannot enrich: {total_bare} step(s) across {len(underspec)} cluster(s) lack detail or issue_refs:",
                "red",
            )
        )
        for name, bare, total in underspec:
            print(colorize_fn(f"    {name}: {bare}/{total} steps need enrichment", "yellow"))
        print()
        print(
            colorize_fn(
                "  Every step needs --detail (sub-points) or --issue-refs (for auto-completion).",
                "dim",
            )
        )
        print(colorize_fn("  Fix:", "dim"))
        print(
            colorize_fn(
                '    desloppify plan cluster update <name> --update-step N --detail "sub-details"',
                "dim",
            )
        )
        print(
            colorize_fn(
                "  You can also still reorganize: add/remove clusters, reorder, etc.",
                "dim",
            )
        )
        return

    print(colorize_fn("  All steps have detail or issue_refs.", "green"))

    if get_project_root_fn is None:
        from desloppify.base.discovery.paths import get_project_root

        get_project_root_fn = get_project_root

    bad_paths = steps_with_bad_paths_fn(plan, get_project_root_fn())
    if bad_paths:
        total_bad = sum(len(bp) for _, _, bp in bad_paths)
        print(
            colorize_fn(
                f"  Warning: {total_bad} file path(s) in step details don't exist on disk:",
                "yellow",
            )
        )
        for name, step_num, paths in bad_paths[:5]:
            print(colorize_fn(f"    {name} step {step_num}: {', '.join(paths[:3])}", "yellow"))
        print(
            colorize_fn(
                "  Fix paths before confirming enrich (confirmation will block on bad paths).",
                "dim",
            )
        )

    untagged = steps_without_effort_fn(plan)
    if untagged:
        total_missing = sum(n for _, n, _ in untagged)
        print(colorize_fn(f"  Note: {total_missing} step(s) have no effort tag.", "yellow"))
        print(
            colorize_fn(
                "  Consider: desloppify plan cluster update <name> --update-step N --effort small",
                "dim",
            )
        )

    report = enrich_report_or_error_fn(report)
    if report is None:
        return

    stages = meta.setdefault("triage_stages", {})
    cleared = record_enrich_stage_fn(
        stages,
        report=report,
        shallow_count=total_bare,
        existing_stage=existing_stage,
        is_reuse=is_reuse,
    )

    resolved_services.save_plan(plan)

    resolved_services.append_log_entry(
        plan,
        "triage_enrich",
        actor="user",
        detail={"shallow_count": total_bare, "reuse": is_reuse},
    )
    resolved_services.save_plan(plan)

    print(
        colorize_fn(
            f"  Enrich stage recorded: {total_bare} step(s) still without detail.",
            "green",
        )
    )
    if is_reuse:
        print(colorize_fn("  Enrich data preserved (no changes).", "dim"))
        if cleared:
            print_cascade_clear_feedback_fn(cleared, stages)
    else:
        print(colorize_fn("  Now confirm the enrichment.", "yellow"))
        print(colorize_fn("    desloppify plan triage --confirm enrich", "dim"))

    print_user_message_fn(
        "Enrich recorded. Before confirming — check the subagent's"
        " work. Could a developer who has never seen this code"
        " execute every step without asking a question? Every step"
        " needs: file path, specific location, specific action."
        " 'Refactor X' fails. 'Extract lines 45-89 into Y' passes."
    )


__all__ = ["ColorizeFn", "run_stage_enrich"]
