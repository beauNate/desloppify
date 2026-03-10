"""Sync subjective dimensions into the plan queue.

Two independent sync functions:

- **sync_unscored_dimensions** — append never-scored (placeholder) dimensions
  to the *back* of the queue unconditionally.
- **sync_stale_dimensions** — append stale (previously-scored) dimensions to
  the *back* of the queue when no objective items remain, and evict them
  again when objective backlog returns.

Invariant: new items are always appended — sync never reorders existing queue.
"""

from __future__ import annotations

from desloppify.base.config import DEFAULT_TARGET_STRICT_SCORE
from desloppify.engine._plan import stale_policy as stale_policy_mod
from desloppify.engine._plan._sync_context import has_objective_backlog, is_mid_cycle
from desloppify.engine._plan.constants import SUBJECTIVE_PREFIX, QueueSyncResult
from desloppify.engine._plan.schema import PlanModel, ensure_plan_defaults
from desloppify.engine._plan.subjective_policy import SubjectiveVisibility
from desloppify.engine._state.schema import StateModel


# ---------------------------------------------------------------------------
# ID helpers
# ---------------------------------------------------------------------------

def current_unscored_ids(state: StateModel) -> set[str]:
    """Return the set of ``subjective::<slug>`` IDs that are currently unscored (placeholder).

    Checks ``subjective_assessments`` first; when that dict is empty
    (common before any reviews have been run), falls through to
    ``dimension_scores`` which carries placeholder metadata from scan.
    """
    return stale_policy_mod.current_unscored_ids(
        state,
        subjective_prefix=SUBJECTIVE_PREFIX,
    )


def current_under_target_ids(
    state: StateModel,
    *,
    target_strict: float = DEFAULT_TARGET_STRICT_SCORE,
) -> set[str]:
    """Return ``subjective::<slug>`` IDs that are under target but not stale or unscored.

    These are dimensions whose assessment is still current (not needing refresh)
    but whose score hasn't reached the target yet.
    """
    return stale_policy_mod.current_under_target_ids(
        state,
        target_strict=target_strict,
        subjective_prefix=SUBJECTIVE_PREFIX,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _prune_subjective_ids(
    order: list[str],
    *,
    keep_ids: set[str],
    pruned: list[str],
) -> None:
    """Remove subjective IDs from *order* that are not in *keep_ids*, appending removed to *pruned*."""
    to_remove = [
        fid for fid in order
        if fid.startswith(SUBJECTIVE_PREFIX)
        and fid not in keep_ids
    ]
    for fid in to_remove:
        order.remove(fid)
        pruned.append(fid)


def _skipped_subjective_ids(plan: PlanModel) -> set[str]:
    """Return subjective IDs that are currently skipped in the plan."""
    skipped = plan.get("skipped", {})
    if not isinstance(skipped, dict):
        return set()
    return {
        str(fid)
        for fid in skipped
        if isinstance(fid, str) and fid.startswith(SUBJECTIVE_PREFIX)
    }


def _prune_skipped_subjective_ids(
    order: list[str],
    *,
    skipped_ids: set[str],
    pruned: list[str],
) -> None:
    """Repair invalid overlap by removing skipped subjective IDs from queue order."""
    if not skipped_ids:
        return
    to_remove = [fid for fid in order if fid in skipped_ids]
    for fid in to_remove:
        order.remove(fid)
        pruned.append(fid)


def _inject_subjective_ids(
    order: list[str],
    *,
    inject_ids: set[str],
    injected: list[str],
) -> None:
    """Inject subjective IDs into *order* if not already present.

    Always appends to the back — new items never reorder existing queue.
    """
    existing = set(order)
    for sid in sorted(inject_ids):
        if sid not in existing:
            order.append(sid)
            injected.append(sid)


# ---------------------------------------------------------------------------
# Unscored dimension sync (back of queue, unconditional)
# ---------------------------------------------------------------------------

def sync_unscored_dimensions(
    plan: PlanModel,
    state: StateModel,
) -> QueueSyncResult:
    """Keep the plan queue in sync with unscored (placeholder) subjective dimensions.

    1. **Prune** — always remove ``subjective::*`` IDs from ``queue_order``
       that are no longer unscored AND not stale (avoids pruning stale IDs —
       that is ``sync_stale_dimensions``' responsibility).  Pruning runs even
       mid-cycle to clean up items that were incorrectly injected by older
       code that lacked the mid-cycle guard.
    2. **Inject** — append currently-unscored IDs to the *back* of
       ``queue_order``.  Never reorders existing items.  Skipped mid-cycle —
       unscored dimensions surface at cycle boundaries only.
    """
    ensure_plan_defaults(plan)
    result = QueueSyncResult()
    mid_cycle = is_mid_cycle(plan)

    unscored_ids = current_unscored_ids(state)
    stale_ids = stale_policy_mod.current_stale_ids(
        state, subjective_prefix=SUBJECTIVE_PREFIX,
    )
    order: list[str] = plan["queue_order"]
    skipped_ids = _skipped_subjective_ids(plan)

    if not mid_cycle:
        # Unscored dimensions have never been reviewed — a permanent skip on a
        # placeholder is premature (the scoring pipeline needs actual scores).
        # Clear any skip entries so they resurface for initial review.
        skipped_dict = plan.get("skipped", {})
        if isinstance(skipped_dict, dict):
            for sid in sorted(unscored_ids & skipped_ids):
                skipped_dict.pop(sid, None)
                result.resurfaced.append(sid)
            skipped_ids -= unscored_ids

    # Keep queue/skipped invariants healthy even if an old plan contains overlap.
    _prune_skipped_subjective_ids(order, skipped_ids=skipped_ids, pruned=result.pruned)

    # --- Cleanup: prune subjective IDs that are no longer unscored --------
    # Only prune IDs that are neither unscored nor stale (stale sync owns those).
    # Mid-cycle: prune ALL subjective IDs (keep_ids empty) since they
    # shouldn't be in the queue mid-cycle at all.
    if mid_cycle:
        _prune_subjective_ids(order, keep_ids=set(), pruned=result.pruned)
    else:
        _prune_subjective_ids(order, keep_ids=unscored_ids | stale_ids, pruned=result.pruned)

    # --- Inject: append unscored IDs to back of queue ---------------------
    # Mid-cycle: don't inject — they'll surface at cycle end.
    if not mid_cycle:
        _inject_subjective_ids(
            order,
            inject_ids=unscored_ids - skipped_ids,
            injected=result.injected,
        )

    return result


# ---------------------------------------------------------------------------
# Stale dimension sync (back of queue, conditional)
# ---------------------------------------------------------------------------

def sync_stale_dimensions(
    plan: PlanModel,
    state: StateModel,
    *,
    policy: SubjectiveVisibility | None = None,
    cycle_just_completed: bool = False,
) -> QueueSyncResult:
    """Keep the plan queue in sync with stale and under-target subjective dimensions.

    1. Remove any ``subjective::*`` IDs from ``queue_order`` that are no
       longer stale/under-target and not unscored (avoids pruning IDs owned
       by ``sync_unscored_dimensions``).
       When objective backlog exists (and this is not a just-completed cycle),
       stale/under-target IDs are also evicted so they do not block objective work.
    2. Append stale and under-target dimension IDs to the *back* when either:
       a. No objective items remain (mid-cycle), OR
       b. A cycle just completed.
       Never reorders existing items.
    """
    ensure_plan_defaults(plan)
    result = QueueSyncResult()
    stale_ids = stale_policy_mod.current_stale_ids(
        state, subjective_prefix=SUBJECTIVE_PREFIX,
    )
    under_target_ids = current_under_target_ids(state)
    skipped_ids = _skipped_subjective_ids(plan)
    injectable_ids = (stale_ids | under_target_ids) - skipped_ids
    unscored_ids = current_unscored_ids(state)
    order: list[str] = plan["queue_order"]

    # Keep queue/skipped invariants healthy even if an old plan contains overlap.
    _prune_skipped_subjective_ids(order, skipped_ids=skipped_ids, pruned=result.pruned)

    objective_backlog = has_objective_backlog(state, policy)

    # --- Cleanup: prune resolved subjective IDs --------------------------
    # Mid-cycle: don't keep unscored IDs — sync_unscored_dimensions owns
    # those and prunes them mid-cycle.  Only keep stale/under-target when
    # objective backlog is clear or just-completed.
    # Non-mid-cycle: keep unscored IDs always (they belong in the queue).
    mid_cycle = is_mid_cycle(plan)
    if mid_cycle:
        keep_ids = injectable_ids if not objective_backlog else set()
    else:
        keep_ids = unscored_ids | injectable_ids
        if objective_backlog and not cycle_just_completed:
            keep_ids = unscored_ids
    _prune_subjective_ids(order, keep_ids=keep_ids, pruned=result.pruned)

    # --- Inject stale + under-target dimensions --------------------------
    should_inject = not objective_backlog or cycle_just_completed

    if should_inject and injectable_ids:
        _inject_subjective_ids(order, inject_ids=injectable_ids, injected=result.injected)

    return result


__all__ = [
    "current_under_target_ids",
    "current_unscored_ids",
    "sync_stale_dimensions",
    "sync_unscored_dimensions",
]
