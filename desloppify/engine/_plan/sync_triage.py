"""Triage sync — inject/prune triage stage IDs based on review issue changes."""

from __future__ import annotations

from desloppify.engine._plan import stale_policy as stale_policy_mod
from desloppify.engine._plan._sync_context import has_objective_backlog, is_mid_cycle
from desloppify.engine._plan.constants import (
    TRIAGE_IDS,
    TRIAGE_STAGE_IDS,
    QueueSyncResult,
)
from desloppify.engine._plan.schema import PlanModel, ensure_plan_defaults
from desloppify.engine._plan.subjective_policy import SubjectiveVisibility
from desloppify.engine._state.schema import StateModel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_review_ids_since_triage(
    state: StateModel,
    meta: dict,
) -> set[str]:
    """Return review issue IDs that are new since the last triage."""
    triaged_ids = set(meta.get("triaged_ids", []))
    return stale_policy_mod.open_review_ids(state) - triaged_ids


def _prune_all_triage_stages(order: list[str]) -> None:
    """Remove all ``triage::*`` stage IDs from *order*."""
    for sid in TRIAGE_STAGE_IDS:
        while sid in order:
            order.remove(sid)


def _inject_pending_triage_stages(
    order: list[str],
    confirmed: set[str],
    *,
    skipped: dict[str, object] | None = None,
) -> list[str]:
    """Inject triage stages for pending (unconfirmed) items.

    Always appends to the back — new items never reorder existing queue.
    Returns list of injected stage IDs.
    """
    stage_names = ("observe", "reflect", "organize", "enrich", "sense-check", "commit")
    existing = set(order)
    injected: list[str] = []
    for sid, name in zip(TRIAGE_STAGE_IDS, stage_names, strict=False):
        if name not in confirmed and sid not in existing:
            if skipped is not None:
                skipped.pop(sid, None)
            order.append(sid)
            injected.append(sid)
            existing.add(sid)
    return injected


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def is_triage_stale(plan: PlanModel, state: StateModel) -> bool:
    """Side-effect-free check: is triage needed?

    Returns True when genuinely *new* review issues appeared since the
    last triage.  Triage stage IDs being in the queue alone is not
    sufficient — the new issues that triggered injection may have been
    resolved since then.

    When issues are merely resolved (current IDs are a subset of
    previously triaged IDs), triage is NOT stale — the user is working
    through the plan.
    """
    ensure_plan_defaults(plan)
    return stale_policy_mod.is_triage_stale(plan, state)


def compute_new_issue_ids(plan: PlanModel, state: StateModel) -> set[str]:
    """Return the set of open review/concerns issue IDs added since last triage.

    Returns an empty set when no prior triage has recorded ``triaged_ids``.
    """
    return stale_policy_mod.compute_new_issue_ids(plan, state)


def sync_triage_needed(
    plan: PlanModel,
    state: StateModel,
    *,
    policy: SubjectiveVisibility | None = None,
) -> QueueSyncResult:
    """Append triage stage IDs to back of queue when review issues change.

    Only injects stages not already confirmed in ``epic_triage_meta``.

    **Mid-cycle guard**: when the objective backlog still has work, triage
    stages are NOT injected.  Instead, ``epic_triage_meta["triage_recommended"]``
    is set so the UI can show a non-blocking banner.  Stages are injected
    once the objective backlog drains (or on manual ``plan triage``).

    When stages are already present but all new issues have been resolved
    since injection, auto-prunes the stale stages and updates the hash.

    When issues are *resolved* (current IDs are a subset of previously
    triaged IDs), the snapshot hash is updated silently — no re-triage
    is needed since the user is working through the plan.
    """
    ensure_plan_defaults(plan)
    result = QueueSyncResult()
    order: list[str] = plan["queue_order"]
    meta = plan.get("epic_triage_meta", {})
    confirmed = set(meta.get("triage_stages", {}).keys())

    # Check if any triage stage is already in queue
    already_present = any(sid in order for sid in TRIAGE_IDS)

    current_hash = stale_policy_mod.review_issue_snapshot_hash(state)
    last_hash = meta.get("issue_snapshot_hash", "")

    if already_present:
        # Stages present — check if the reason for injection still applies.
        # Only auto-prune when triage was completed before (hash exists),
        # all new issues have been resolved, and no triage work is in
        # progress.  This avoids pruning the initial triage or a
        # user-started triage session.
        if last_hash and not confirmed:
            new_since_triage = _new_review_ids_since_triage(state, meta)

            if not new_since_triage:
                # No new issues remain — prune stale stages
                _prune_all_triage_stages(order)
                if current_hash:
                    meta["issue_snapshot_hash"] = current_hash
                    plan["epic_triage_meta"] = meta
                result.pruned = list(TRIAGE_STAGE_IDS)
        return result

    # Backfill: partial triage was done (stages confirmed with
    # confirmed_at) but triage was never fully completed — remaining
    # stages were skipped or removed from the queue.  Record current
    # review IDs as triaged so they don't re-trigger injection on the
    # next cycle.
    stages = meta.get("triage_stages", {})
    has_completed_stage = any(
        isinstance(v, dict) and v.get("confirmed_at")
        for v in stages.values()
    )
    if has_completed_stage and not meta.get("triaged_ids") and not last_hash:
        current_review = sorted(stale_policy_mod.open_review_ids(state))
        if current_review:
            meta["triaged_ids"] = current_review
            meta["issue_snapshot_hash"] = current_hash
            plan["epic_triage_meta"] = meta

    if current_hash and current_hash != last_hash:
        # Distinguish "new issues appeared" from "issues were resolved".
        # Only re-triage when genuinely new issues exist.
        new_since_triage = _new_review_ids_since_triage(state, meta)

        if new_since_triage:
            # Mid-cycle guard: defer injection while objective work remains.
            # Triage injection itself is safe (appends to back of queue),
            # but auto-clustering (guarded separately in plan_reconcile.py)
            # would regenerate queue structure if triggered mid-cycle.
            if is_mid_cycle(plan) and has_objective_backlog(state, policy):
                meta["triage_recommended"] = True
                plan["epic_triage_meta"] = meta
                result.deferred = True
            else:
                # Inject: either pre-cycle, end-of-cycle, or queue clear
                meta.pop("triage_recommended", None)
                plan["epic_triage_meta"] = meta
                injected = _inject_pending_triage_stages(
                    order,
                    confirmed,
                    skipped=plan.get("skipped", {}),
                )
                result.injected = injected
        else:
            # Only resolved issues changed the hash — update silently.
            # Also clear triage_recommended: the issues that triggered the
            # recommendation have been resolved, so it's no longer relevant.
            meta["issue_snapshot_hash"] = current_hash
            meta.pop("triage_recommended", None)
            plan["epic_triage_meta"] = meta

    return result


__all__ = [
    "compute_new_issue_ids",
    "is_triage_stale",
    "sync_triage_needed",
]
