"""Plan sync helpers for review-import flows."""

from __future__ import annotations

from dataclasses import dataclass, field

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
    triage_injected_ids: list[str] = field(default_factory=list)
    triage_deferred: bool = False


def sync_plan_after_review_import(
    plan: PlanModel,
    state: StateModel,
    *,
    policy=None,
) -> ReviewImportSyncResult | None:
    """Sync plan queue after review import. Pure engine function — no I/O.

    Appends new issue IDs to queue_order and injects triage stages
    if needed (respects mid-cycle guard — defers when objective work
    remains).  Returns None when there are no new issues to sync.
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

    # Inject triage stages if needed (policy enables mid-cycle guard)
    triage_result = sync_triage_needed(plan, state, policy=policy)
    triage_injected_ids = list(getattr(triage_result, "injected", []) or [])
    triage_injected = bool(triage_injected_ids)
    triage_deferred = bool(triage_result and getattr(triage_result, "deferred", False))

    return ReviewImportSyncResult(
        new_ids=new_ids,
        added_to_queue=added,
        triage_injected=triage_injected,
        triage_injected_ids=triage_injected_ids,
        triage_deferred=triage_deferred,
    )


__all__ = ["ReviewImportSyncResult", "sync_plan_after_review_import"]
