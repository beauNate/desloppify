"""Shared constants and helpers for plan internals."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

AUTO_PREFIX = "auto/"

SUBJECTIVE_PREFIX = "subjective::"
TRIAGE_ID = "triage::pending"  # deprecated, kept for migration

TRIAGE_PREFIX = "triage::"
TRIAGE_STAGE_IDS = (
    "triage::observe",
    "triage::reflect",
    "triage::organize",
    "triage::enrich",
    "triage::sense-check",
    "triage::commit",
)
TRIAGE_IDS = set(TRIAGE_STAGE_IDS)
WORKFLOW_CREATE_PLAN_ID = "workflow::create-plan"
WORKFLOW_SCORE_CHECKPOINT_ID = "workflow::score-checkpoint"
WORKFLOW_IMPORT_SCORES_ID = "workflow::import-scores"
WORKFLOW_COMMUNICATE_SCORE_ID = "workflow::communicate-score"
WORKFLOW_DEFERRED_DISPOSITION_ID = "workflow::deferred-disposition"
WORKFLOW_PREFIX = "workflow::"
SYNTHETIC_PREFIXES = ("triage::", "workflow::", "subjective::")


@dataclass
class QueueSyncResult:
    """Unified result for all queue sync operations."""

    injected: list[str] = field(default_factory=list)
    pruned: list[str] = field(default_factory=list)
    resurfaced: list[str] = field(default_factory=list)
    deferred: bool = False

    @property
    def changes(self) -> int:
        return len(self.injected) + len(self.pruned) + len(self.resurfaced)


def _resolve_triage_stages(meta_or_stages: dict[str, Any] | None) -> dict[str, Any]:
    """Extract the triage stages dict from meta or a raw stages dict."""
    if not isinstance(meta_or_stages, dict):
        return {}
    if "triage_stages" in meta_or_stages:
        raw = meta_or_stages.get("triage_stages")
        return raw if isinstance(raw, dict) else {}
    return meta_or_stages


def confirmed_triage_stage_names(meta_or_stages: dict[str, Any] | None) -> set[str]:
    """Return triage stage names with an explicit ``confirmed_at`` marker."""
    return {
        str(name)
        for name, payload in _resolve_triage_stages(meta_or_stages).items()
        if isinstance(payload, dict) and payload.get("confirmed_at")
    }


def recorded_unconfirmed_triage_stage_names(meta_or_stages: dict[str, Any] | None) -> set[str]:
    """Return recorded triage stage names that still need confirmation."""
    return {
        str(name)
        for name, payload in _resolve_triage_stages(meta_or_stages).items()
        if isinstance(payload, dict) and payload and not payload.get("confirmed_at")
    }


__all__ = [
    "AUTO_PREFIX",
    "QueueSyncResult",
    "confirmed_triage_stage_names",
    "recorded_unconfirmed_triage_stage_names",
    "SUBJECTIVE_PREFIX",
    "SYNTHETIC_PREFIXES",
    "TRIAGE_IDS",
    "TRIAGE_PREFIX",
    "TRIAGE_STAGE_IDS",
    "WORKFLOW_COMMUNICATE_SCORE_ID",
    "WORKFLOW_DEFERRED_DISPOSITION_ID",
    "WORKFLOW_CREATE_PLAN_ID",
    "WORKFLOW_IMPORT_SCORES_ID",
    "WORKFLOW_PREFIX",
    "WORKFLOW_SCORE_CHECKPOINT_ID",
]
