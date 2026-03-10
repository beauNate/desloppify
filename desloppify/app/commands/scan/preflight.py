"""Scan preflight guard: warn and gate scan when queue has unfinished items."""

from __future__ import annotations

import logging

from desloppify import state as state_mod
from desloppify.app.commands.helpers.queue_progress import (
    ScoreDisplayMode,
    plan_aware_queue_breakdown,
    score_display_mode,
)
from desloppify.app.commands.helpers.queue_progress import get_plan_start_strict
from desloppify.app.commands.helpers.state import state_path
from desloppify.base.exception_sets import PLAN_LOAD_EXCEPTIONS, CommandError
from desloppify.base.output.terminal import colorize
from desloppify.engine.plan import load_plan

_logger = logging.getLogger(__name__)


def scan_queue_preflight(args: object) -> None:
    """Warn and gate scan when queue has unfinished items."""
    # CI profile always passes
    if getattr(args, "profile", None) == "ci":
        return

    # --force-rescan with valid attestation bypasses
    if getattr(args, "force_rescan", False):
        attest = getattr(args, "attest", None) or ""
        if "i understand" not in attest.lower():
            raise CommandError(
                '--force-rescan requires --attest "I understand this is not '
                "the intended workflow and I am intentionally skipping queue "
                'completion"'
            )
        print(
            colorize(
                "  --force-rescan: bypassing queue completion check. "
                "Queue-destructive reconciliation steps will be skipped.",
                "yellow",
            )
        )
        return

    # No plan = no gate (first scan, or user never uses plan)
    try:
        plan = load_plan()
    except PLAN_LOAD_EXCEPTIONS:
        _logger.debug("scan preflight plan load skipped", exc_info=True)
        return
    if not plan.get("plan_start_scores"):
        return  # No active cycle

    # Count plan-aware remaining items.  Block scan when ANY queue items
    # remain (objective OR subjective).  Mid-cycle scans regenerate issue
    # IDs which wipes triage state and re-clusters the queue, undoing
    # prioritisation work.
    try:
        state = state_mod.load_state(state_path(args))
        breakdown = plan_aware_queue_breakdown(state, plan)
        plan_start_strict = get_plan_start_strict(plan)
        mode = score_display_mode(breakdown, plan_start_strict)
    except PLAN_LOAD_EXCEPTIONS:
        _logger.debug("scan preflight queue breakdown skipped", exc_info=True)
        return
    if mode is ScoreDisplayMode.LIVE:
        return  # Queue fully clear or no active cycle — scan allowed

    remaining = breakdown.queue_total
    # GATE — block both FROZEN (objective work) and PHASE_TRANSITION
    # (subjective/workflow items remain)
    raise CommandError(
        f"{remaining} item{'s' if remaining != 1 else ''}"
        " remaining in your queue.\n"
        "  Scanning mid-cycle regenerates issue IDs and breaks triage state.\n"
        "  Work through items with `desloppify next`, then scan when clear.\n\n"
        "  To force a rescan (resets your plan-start score):\n"
        '    desloppify scan --force-rescan --attest "I understand this is not '
        "the intended workflow and I am intentionally skipping queue "
        'completion"'
    )


__all__ = ["scan_queue_preflight"]
