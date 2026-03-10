"""Claude orchestrator mode instructions."""

from __future__ import annotations

import argparse

from desloppify.base.discovery.paths import get_project_root
from desloppify.base.output.terminal import colorize

from ..services import TriageServices, default_triage_services
from .orchestrator_common import ensure_triage_started


def run_claude_orchestrator(
    args: argparse.Namespace,
    *,
    services: TriageServices | None = None,
) -> None:
    """Print orchestrator instructions for Claude Code agent."""
    resolved_services = services or default_triage_services()
    _repo_root = get_project_root()
    plan = resolved_services.load_plan()
    ensure_triage_started(plan, resolved_services, runner="claude")

    print(colorize("\n  Claude triage orchestrator mode.", "bold"))
    print(colorize("  " + "─" * 60, "dim"))
    print(colorize("  You are the orchestrator. For each stage, launch a subagent.\n", "cyan"))
    print("  For each stage (observe → reflect → organize → enrich → sense-check):\n")
    print("    1. Get the prompt:")
    print("       desloppify plan triage --stage-prompt <stage>\n")
    print("    2. Launch a subagent (Agent tool) with that prompt.\n")
    print("    3. Verify the stage was recorded:")
    print("       desloppify plan triage\n")
    print("    4. Confirm:")
    print('       desloppify plan triage --confirm <stage> --attestation "..."\n')
    print("    5. Proceed to the next stage.\n")
    print("  After all 5 stages:")
    print('    desloppify plan triage --complete --strategy "..." --attestation "..."\n')
    print(colorize("  Key rules:", "yellow"))
    print("    - ONE subagent per stage. Don't combine stages.")
    print("    - Check the dashboard between stages.")
    print("    - Observe subagent should use sub-subagents (one per dimension group).")
    print("    - Enrich subagent should use sub-subagents (one per cluster).")
    print("    - Sense-check launches TWO parallel subagents (content + structure).")
    print("    - If a stage fails validation, fix and re-record.")


__all__ = ["run_claude_orchestrator"]
