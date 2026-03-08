"""Plan sync helpers for review-import flows."""

from __future__ import annotations

from dataclasses import dataclass

from desloppify.engine._plan.schema import PlanModel, ensure_plan_defaults
from desloppify.engine._plan.sync_triage import (
    compute_new_issue_ids,
    sync_triage_needed,
)
from desloppify.engine._state.schema import StateModel


@dataclass
class ReviewImportSyncResult:
    """Summary of plan changes after a review import."""

    new_ids: set[str]
    added_to_queue: list[str]
    triage_injected: bool


def sync_plan_after_review_import(
    plan: PlanModel,
    state: StateModel,
) -> ReviewImportSyncResult | None:
    """Sync plan queue after review import. Pure engine function — no I/O.

    Appends new issue IDs to queue_order and injects triage stages
    if needed. Returns None when there are no new issues to sync.
    """
    ensure_plan_defaults(plan)
    new_ids = compute_new_issue_ids(plan, state)
    if not new_ids:
        return None

    # Add new issue IDs to end of queue_order so they have position
    order: list[str] = plan["queue_order"]
    existing = set(order)
    added: list[str] = []
    for issue_id in sorted(new_ids):
        if issue_id not in existing:
            order.append(issue_id)
            added.append(issue_id)

    # Inject triage stages if needed
    triage_result = sync_triage_needed(plan, state)
    triage_injected = bool(
        triage_result and getattr(triage_result, "injected", False)
    )

    return ReviewImportSyncResult(
        new_ids=new_ids,
        added_to_queue=added,
        triage_injected=triage_injected,
    )


__all__ = ["ReviewImportSyncResult", "sync_plan_after_review_import"]
