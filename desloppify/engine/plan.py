"""Public plan API facade.

Plan internals live in ``desloppify.engine._plan``; this module exposes
the stable, non-private API used by commands and rendering helpers.
"""

from __future__ import annotations

# --- auto-clustering --------------------------------------------------------
from desloppify.engine._plan.auto_cluster import (
    AUTO_PREFIX,
    auto_cluster_issues,
)

# --- commit tracking --------------------------------------------------------
from desloppify.engine._plan.commit_tracking import (
    add_uncommitted_issues,
    commit_tracking_summary,
    filter_issue_ids_by_pattern,
    find_commit_for_issue,
    generate_pr_body,
    get_uncommitted_issues,
    purge_uncommitted_ids,
    record_commit,
    suggest_commit_message,
)

# --- epic triage ------------------------------------------------------------
from desloppify.engine._plan.epic_triage import (
    TriageInput,
    build_triage_prompt,
    collect_triage_input,
    detect_recurring_patterns,
    extract_issue_citations,
)
from desloppify.engine._plan.triage_playbook import (
    TRIAGE_CMD_CLUSTER_ADD,
    TRIAGE_CMD_CLUSTER_CREATE,
    TRIAGE_CMD_CLUSTER_ENRICH,
    TRIAGE_CMD_CLUSTER_ENRICH_COMPACT,
    TRIAGE_CMD_CLUSTER_STEPS,
    TRIAGE_CMD_COMPLETE,
    TRIAGE_CMD_COMPLETE_VERBOSE,
    TRIAGE_CMD_CONFIRM_EXISTING,
    TRIAGE_CMD_ENRICH,
    TRIAGE_CMD_OBSERVE,
    TRIAGE_CMD_ORGANIZE,
    TRIAGE_CMD_REFLECT,
    TRIAGE_CMD_RUN_STAGES_CLAUDE,
    TRIAGE_CMD_RUN_STAGES_CODEX,
    TRIAGE_CMD_SENSE_CHECK,
    TRIAGE_STAGE_DEPENDENCIES,
    TRIAGE_STAGE_LABELS,
    triage_manual_stage_command,
    triage_run_stages_command,
    triage_runner_commands,
)

# --- operations -------------------------------------------------------------
from desloppify.engine._plan.annotations import annotation_counts
from desloppify.engine._plan.operations_cluster import (
    add_to_cluster,
    create_cluster,
    delete_cluster,
    merge_clusters,
    move_cluster,
    remove_from_cluster,
)
from desloppify.engine._plan.operations_lifecycle import (
    clear_focus,
    purge_ids,
    reset_plan,
    set_focus,
)
from desloppify.engine._plan.operations_meta import (
    annotate_issue,
    append_log_entry,
    describe_issue,
)
from desloppify.engine._plan.operations_queue import move_items
from desloppify.engine._plan.operations_skip import (
    resurface_stale_skips,
    skip_items,
    unskip_items,
)
from desloppify.engine._plan.project_policy import (
    add_rule,
    load_policy,
    remove_rule,
    render_policy_block,
    save_policy,
)
from desloppify.engine._plan.refresh_lifecycle import (
    clear_postflight_scan_completion,
    mark_postflight_scan_completed,
    postflight_scan_pending,
)
from desloppify.engine._plan.skip_policy import (
    SKIP_KIND_LABELS,
    USER_SKIP_KINDS,
    skip_kind_from_flags,
    skip_kind_requires_attestation,
    skip_kind_requires_note,
    skip_kind_state_status,
)
from desloppify.engine._plan.step_completion import auto_complete_steps
from desloppify.engine._plan.step_parser import (
    format_steps,
    normalize_step,
    parse_steps_file,
    step_summary,
)

# --- persistence ------------------------------------------------------------
from desloppify.engine._plan.persistence import (
    PLAN_FILE,
    get_plan_file,
    has_living_plan,
    load_plan,
    plan_lock,
    plan_path_for_state,
    save_plan,
)

# --- reconcile --------------------------------------------------------------
from desloppify.engine._plan.reconcile import (
    ReconcileResult,
    ReviewImportSyncResult,
    reconcile_plan_after_scan,
    sync_plan_after_review_import,
)

# --- schema -----------------------------------------------------------------
from desloppify.engine._plan.schema import (
    EPIC_PREFIX,
    PLAN_VERSION,
    VALID_EPIC_DIRECTIONS,
    VALID_SKIP_KINDS,
    Cluster,
    CommitRecord,
    ExecutionLogEntry,
    ItemOverride,
    PlanModel,
    SkipEntry,
    SupersededEntry,
    empty_plan,
    ensure_plan_defaults,
    triage_clusters,
    validate_plan,
)

# --- constants --------------------------------------------------------------
from desloppify.engine._plan.constants import (
    SYNTHETIC_PREFIXES,
    TRIAGE_IDS,
    TRIAGE_PREFIX,
    TRIAGE_STAGE_IDS,
    WORKFLOW_CREATE_PLAN_ID,
    WORKFLOW_PREFIX,
    WORKFLOW_SCORE_CHECKPOINT_ID,
    QueueSyncResult,
    confirmed_triage_stage_names,
    recorded_unconfirmed_triage_stage_names,
)

# --- sync: dimensions ------------------------------------------------------
from desloppify.engine._plan.sync_dimensions import (
    current_unscored_ids,
    sync_stale_dimensions,
    sync_unscored_dimensions,
)

# --- sync: triage -----------------------------------------------------------
from desloppify.engine._plan.sync_triage import (
    compute_new_issue_ids,
    is_triage_stale,
    sync_triage_needed,
)

# --- sync: workflow ---------------------------------------------------------
from desloppify.engine._plan.sync_workflow import (
    ScoreSnapshot,
    sync_communicate_score_needed,
    sync_create_plan_needed,
    sync_import_scores_needed,
    sync_score_checkpoint_needed,
)
from desloppify.engine._plan.stale_policy import (
    _REVIEW_DETECTORS,
    open_review_ids,
    review_issue_snapshot_hash,
)

# --- subjective policy ------------------------------------------------------
from desloppify.engine._plan._sync_context import (
    has_objective_backlog as _has_objective_backlog,
)
from desloppify.engine._plan.subjective_policy import compute_subjective_visibility
from desloppify.engine._plan.plan_public_exports import PLAN_PUBLIC_EXPORTS


def triage_phase_banner(plan: PlanModel, state: dict | None = None) -> str:
    """Return a banner string describing triage status.

    Handles three states:
    - Triage stages in queue + objective work → TRIAGE PENDING
    - Triage stages in queue + no objective work → TRIAGE MODE (active)
    - No triage stages but ``triage_recommended`` set → soft recommendation
    """
    ensure_plan_defaults(plan)
    order = set(plan.get("queue_order", []))
    has_triage = any(sid in order for sid in TRIAGE_IDS)
    meta = plan.get("epic_triage_meta", {})
    run_hint = (
        f"Run: {TRIAGE_CMD_RUN_STAGES_CODEX} "
        f"(or {TRIAGE_CMD_RUN_STAGES_CLAUDE})"
    )

    if not has_triage:
        # No stages in queue — check for deferred recommendation
        if meta.get("triage_recommended"):
            return (
                "TRIAGE RECOMMENDED — review issues changed since last triage. "
                f"{run_hint}"
            )
        return ""

    if state and _has_objective_backlog(state, None):
        return (
            "TRIAGE PENDING — queued and will activate after objective work "
            "is complete."
        )
    stages = meta.get("triage_stages", {})
    completed = [s for s in ("observe", "reflect", "organize", "enrich", "sense-check") if s in stages]
    if completed:
        return (
            f"TRIAGE MODE ({len(completed)}/5 stages complete) — "
            f"complete all stages to exit. {run_hint}"
        )
    return (
        "TRIAGE MODE — review issues need analysis before fixing. "
        f"{run_hint}"
    )


__all__ = PLAN_PUBLIC_EXPORTS
