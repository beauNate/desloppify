"""Shared helpers for triage runner orchestrators."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from desloppify.base.output.terminal import colorize
from desloppify.engine._plan.constants import TRIAGE_STAGE_IDS

from ..helpers import has_triage_in_queue, inject_triage_stages
from ..services import TriageServices

STAGES: tuple[str, ...] = ("observe", "reflect", "organize", "enrich", "sense-check")


def parse_only_stages(raw: str | None) -> list[str]:
    """Parse --only-stages comma-separated string into validated stage list."""
    if not raw:
        return list(STAGES)
    stages = [s.strip().lower() for s in raw.split(",") if s.strip()]
    for stage in stages:
        if stage not in STAGES:
            raise ValueError(f"Unknown stage: {stage!r}. Valid: {', '.join(STAGES)}")
    return stages


def run_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def ensure_triage_started(
    plan: dict[str, Any],
    services: TriageServices,
    *,
    runner: str | None = None,
) -> dict[str, Any]:
    """Auto-start triage if not started. Returns updated plan."""
    if not has_triage_in_queue(plan):
        inject_triage_stages(plan)
        meta = plan.setdefault("epic_triage_meta", {})
        meta.setdefault("triage_stages", {})
        services.append_log_entry(
            plan,
            "triage_auto_start",
            actor="system",
            detail={
                "source": "runner_auto_start",
                "runner": runner,
                "injected_stage_ids": list(TRIAGE_STAGE_IDS),
            },
        )
        services.save_plan(plan)
        print(colorize("  Planning mode auto-started.", "cyan"))
    return plan
