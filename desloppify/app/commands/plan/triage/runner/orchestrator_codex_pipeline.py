"""Codex pipeline orchestration for triage stages."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import desloppify
from desloppify.app.commands.review.batches_runtime import make_run_log_writer
from desloppify.base.discovery.file_paths import safe_write_text
from desloppify.base.discovery.paths import get_project_root
from desloppify.base.exception_sets import CommandError
from desloppify.base.output.terminal import colorize

from .._stage_validation import (
    _analyze_reflect_issue_accounting,
    _validate_reflect_issue_accounting,
)
from ..services import TriageServices, default_triage_services
from .codex_runner import run_triage_stage
from .orchestrator_codex_observe import run_observe
from .orchestrator_codex_sense import run_sense_check
from .orchestrator_common import STAGES, ensure_triage_started, run_stamp
from .stage_prompts import build_stage_prompt
from .stage_prompts_instruction_shared import PromptMode
from .stage_validation import build_auto_attestation, validate_stage


def _is_full_stage_run(stages_to_run: list[str]) -> bool:
    """True when the pipeline was asked to run the full triage stage set."""
    return set(stages_to_run) == set(STAGES)


def _all_stage_results_successful(
    *,
    stages_to_run: list[str],
    stage_results: dict[str, dict],
) -> bool:
    """True when each requested stage is confirmed or already confirmed."""
    for stage in stages_to_run:
        status = str(stage_results.get(stage, {}).get("status", ""))
        if status not in {"confirmed", "skipped"}:
            return False
    return True


def _print_not_finalized_message(reason: str) -> None:
    """Emit a consistent next-step message when auto-completion is skipped/blocked."""
    print(colorize(f"\n  Stages complete, triage not finalized ({reason}).", "yellow"))
    print(
        colorize(
            '  Finalize manually: desloppify plan triage --complete --strategy "<execution plan>"',
            "dim",
        )
    )


def _load_prior_reports_from_plan(plan: dict) -> dict[str, str]:
    """Seed prior stage reports from the existing live triage state."""
    stages = plan.get("epic_triage_meta", {}).get("triage_stages", {})
    prior_reports: dict[str, str] = {}
    for stage in STAGES:
        report = stages.get(stage, {}).get("report", "")
        if report:
            prior_reports[stage] = report
    return prior_reports


@dataclass(frozen=True)
class StageHandler:
    """Per-stage execution/record hooks for the codex triage pipeline."""

    run_parallel: Callable[..., tuple[bool | None, str | None]] | None = None
    record_report: Callable[[str, argparse.Namespace, TriageServices], None] | None = None
    prompt_mode: PromptMode = "output_only"

def _record_observe_report(
    report: str,
    args: argparse.Namespace,
    services: TriageServices,
) -> None:
    from ..stage_flow_commands import cmd_stage_observe

    record_args = argparse.Namespace(
        stage="observe",
        report=report,
        state=getattr(args, "state", None),
    )
    cmd_stage_observe(record_args, services=services)


def _record_reflect_report(
    report: str,
    args: argparse.Namespace,
    services: TriageServices,
) -> None:
    from ..stage_flow_commands import cmd_stage_reflect

    record_args = argparse.Namespace(
        stage="reflect",
        report=report,
        state=getattr(args, "state", None),
    )
    cmd_stage_reflect(record_args, services=services)


def _record_sense_check_report(
    report: str,
    args: argparse.Namespace,
    services: TriageServices,
) -> None:
    from ..stage_flow_commands import cmd_stage_sense_check

    record_args = argparse.Namespace(
        stage="sense-check",
        report=report,
        state=getattr(args, "state", None),
    )
    cmd_stage_sense_check(record_args, services=services)


_STAGE_HANDLERS: dict[str, StageHandler] = {
    "observe": StageHandler(
        run_parallel=lambda **kwargs: run_observe(
            si=kwargs["si"],
            repo_root=kwargs["repo_root"],
            prompts_dir=kwargs["prompts_dir"],
            output_dir=kwargs["output_dir"],
            logs_dir=kwargs["logs_dir"],
            timeout_seconds=kwargs["timeout_seconds"],
            dry_run=kwargs["dry_run"],
            append_run_log=kwargs["append_run_log"],
        ),
        record_report=_record_observe_report,
    ),
    "reflect": StageHandler(
        record_report=_record_reflect_report,
    ),
    "organize": StageHandler(
        prompt_mode="self_record",
    ),
    "enrich": StageHandler(
        prompt_mode="self_record",
    ),
    "sense-check": StageHandler(
        run_parallel=lambda **kwargs: run_sense_check(
            plan=kwargs["plan"],
            repo_root=kwargs["repo_root"],
            prompts_dir=kwargs["prompts_dir"],
            output_dir=kwargs["output_dir"],
            logs_dir=kwargs["logs_dir"],
            timeout_seconds=kwargs["timeout_seconds"],
            dry_run=kwargs["dry_run"],
            append_run_log=kwargs["append_run_log"],
        ),
        record_report=_record_sense_check_report,
    ),
}


def _write_desloppify_cli_helper(run_dir: Path) -> Path:
    """Create an exact CLI wrapper so codex subagents use this checkout + interpreter."""
    package_root = Path(desloppify.__file__).resolve().parent.parent
    script_path = run_dir / "run_desloppify.sh"
    script = (
        "#!/bin/sh\n"
        f"export PYTHONPATH={shlex.quote(str(package_root))}${{PYTHONPATH:+:$PYTHONPATH}}\n"
        f"exec {shlex.quote(sys.executable)} -m desloppify.cli \"$@\"\n"
    )
    safe_write_text(script_path, script)
    os.chmod(script_path, 0o755)
    return script_path


def _read_stage_output(output_file: Path) -> str:
    """Return stripped stage output text, or an empty string when unreadable."""
    try:
        return output_file.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _preflight_stage(
    *,
    stage: str,
    plan: dict,
    si,
    append_run_log,
) -> tuple[bool, str | None]:
    """Fail fast when a requested stage has invalid upstream prerequisites."""
    if stage != "organize":
        return True, None
    reflect_report = str(
        plan.get("epic_triage_meta", {})
        .get("triage_stages", {})
        .get("reflect", {})
        .get("report", "")
    )
    accounting_ok, _cited, missing_ids, duplicate_ids = _validate_reflect_issue_accounting(
        report=reflect_report,
        valid_ids=set(getattr(si, "open_issues", {}).keys()),
    )
    if accounting_ok:
        return True, None
    reason_parts: list[str] = []
    if missing_ids:
        reason_parts.append(f"missing={len(missing_ids)}")
    if duplicate_ids:
        reason_parts.append(f"duplicates={len(duplicate_ids)}")
    reason = "reflect_accounting_invalid"
    if reason_parts:
        reason = f"{reason}({' '.join(reason_parts)})"
    append_run_log(f"stage-preflight-failed stage={stage} reason={reason}")
    return False, reason


def _stage_report_recorded(plan: dict, stage: str) -> bool:
    """True when the plan contains a persisted report for the given stage."""
    return bool(
        plan.get("epic_triage_meta", {})
        .get("triage_stages", {})
        .get(stage, {})
        .get("report", "")
    )


def _build_reflect_repair_prompt(
    *,
    si,
    prior_reports: dict[str, str],
    repo_root: Path,
    cli_command: str,
    original_report: str,
    missing_ids: list[str],
    duplicate_ids: list[str],
    stages_data: dict | None = None,
) -> str:
    """Build a targeted retry prompt for a reflect report that failed accounting."""
    missing_short = ", ".join(issue_id.rsplit("::", 1)[-1] for issue_id in missing_ids) or "none"
    duplicate_short = (
        ", ".join(issue_id.rsplit("::", 1)[-1] for issue_id in duplicate_ids) or "none"
    )
    base_prompt = build_stage_prompt(
        "reflect",
        si,
        prior_reports,
        repo_root=repo_root,
        mode="output_only",
        cli_command=cli_command,
        stages_data=stages_data,
    )
    return "\n\n".join(
        [
            base_prompt,
            "## Repair Pass",
            "Your previous reflect report failed the exact-hash accounting check.",
            f"Missing hashes: {missing_short}",
            f"Duplicated hashes: {duplicate_short}",
            "Rewrite the FULL reflect report so it passes validation.",
            "Requirements for this repair:",
            "- Start with a `## Coverage Ledger` section.",
            '- Use one ledger line per issue hash: `- abcd1234 -> cluster "name"` or `- abcd1234 -> skip "reason"`.',
            "- Mention every required hash exactly once in that ledger.",
            "- Do not mention hashes anywhere else in the report.",
            "- Preserve the same strategy unless fixing the missing/duplicate hashes forces a small adjustment.",
            "- Output only the corrected reflect report.",
            "## Previous Reflect Report",
            original_report,
        ]
    )


def _repair_reflect_report_if_needed(
    *,
    report: str,
    si,
    prior_reports: dict[str, str],
    repo_root: Path,
    prompts_dir: Path,
    output_dir: Path,
    logs_dir: Path,
    cli_command: str,
    timeout_seconds: int,
    append_run_log,
    stages_data: dict | None = None,
) -> tuple[str | None, str | None]:
    """Retry reflect once with a targeted repair prompt when accounting is invalid."""
    _cited, missing_ids, duplicate_ids = _analyze_reflect_issue_accounting(
        report=report,
        valid_ids=set(getattr(si, "open_issues", {}).keys()),
    )
    if not missing_ids and not duplicate_ids:
        return report, None

    print(colorize("  Reflect: repairing missing/duplicate hash accounting...", "yellow"))
    append_run_log(
        "stage-reflect-repair-start "
        f"missing={len(missing_ids)} duplicates={len(duplicate_ids)}"
    )

    repair_prompt = _build_reflect_repair_prompt(
        si=si,
        prior_reports=prior_reports,
        repo_root=repo_root,
        cli_command=cli_command,
        original_report=report,
        missing_ids=missing_ids,
        duplicate_ids=duplicate_ids,
        stages_data=stages_data,
    )
    repair_prompt_file = prompts_dir / "reflect.repair.md"
    repair_output_file = output_dir / "reflect.repair.raw.txt"
    repair_log_file = logs_dir / "reflect.repair.log"
    safe_write_text(repair_prompt_file, repair_prompt)
    exit_code = run_triage_stage(
        prompt=repair_prompt,
        repo_root=repo_root,
        output_file=repair_output_file,
        log_file=repair_log_file,
        timeout_seconds=timeout_seconds,
    )
    append_run_log(f"stage-reflect-repair-done code={exit_code}")
    if exit_code != 0:
        return None, f"reflect_repair_failed_exit_{exit_code}"

    repaired_report = _read_stage_output(repair_output_file)
    if not repaired_report:
        return None, "reflect_repair_empty_output"

    _cited, missing_after, duplicates_after = _analyze_reflect_issue_accounting(
        report=repaired_report,
        valid_ids=set(getattr(si, "open_issues", {}).keys()),
    )
    if missing_after or duplicates_after:
        return None, "reflect_repair_invalid"

    print(colorize("  Reflect: repair pass fixed issue accounting.", "green"))
    append_run_log("stage-reflect-repair-success")
    return repaired_report, None


def _execute_stage(
    *,
    stage: str,
    args: argparse.Namespace,
    services: TriageServices,
    plan: dict,
    si: dict,
    prior_reports: dict[str, str],
    repo_root: Path,
    prompts_dir: Path,
    output_dir: Path,
    logs_dir: Path,
    cli_command: str,
    stage_start: float,
    timeout_seconds: int,
    dry_run: bool,
    append_run_log,
) -> tuple[str, dict]:
    """Execute one stage and return (status, stage_result)."""
    handler = _STAGE_HANDLERS.get(stage)
    used_parallel = False
    prompt_mode = handler.prompt_mode if handler is not None else "output_only"

    preflight_ok, preflight_reason = _preflight_stage(
        stage=stage,
        plan=plan,
        si=si,
        append_run_log=append_run_log,
    )
    if not preflight_ok:
        elapsed = int(time.monotonic() - stage_start)
        print(
            colorize(
                f"  Stage {stage}: blocked before launch ({preflight_reason}).",
                "red",
            )
        )
        return "failed", {
            "status": "failed",
            "elapsed_seconds": elapsed,
            "error": preflight_reason,
        }

    if handler and handler.run_parallel is not None:
        parallel_ok, merged_report = handler.run_parallel(
            si=si,
            plan=plan,
            repo_root=repo_root,
            prompts_dir=prompts_dir,
            output_dir=output_dir,
            logs_dir=logs_dir,
            timeout_seconds=timeout_seconds,
            dry_run=dry_run,
            append_run_log=append_run_log,
        )
        if parallel_ok is True and dry_run:
            return "dry_run", {"status": "dry_run"}
        if parallel_ok is True and merged_report:
            if handler.record_report is not None:
                handler.record_report(merged_report, args, services)
            used_parallel = True
        elif parallel_ok is False:
            elapsed = int(time.monotonic() - stage_start)
            print(
                colorize(f"  {stage.capitalize()}: parallel execution failed. Aborting.", "red")
            )
            append_run_log(
                f"stage-failed stage={stage} elapsed={elapsed}s reason=parallel_execution_failed"
            )
            return "failed", {"status": "failed", "elapsed_seconds": elapsed}

    if not used_parallel:
        stages_data = plan.get("epic_triage_meta", {}).get("triage_stages", {})
        prompt = build_stage_prompt(
            stage,
            si,
            prior_reports,
            repo_root=repo_root,
            mode=prompt_mode,
            cli_command=cli_command,
            stages_data=stages_data,
        )

        prompt_file = prompts_dir / f"{stage}.md"
        safe_write_text(prompt_file, prompt)

        if dry_run:
            print(colorize(f"  Stage {stage}: prompt written to {prompt_file}", "cyan"))
            print(colorize("  [dry-run] Would execute codex subprocess.", "dim"))
            return "dry_run", {"status": "dry_run"}

        print(colorize(f"\n  Stage {stage}: launching codex subprocess...", "bold"))
        append_run_log(f"stage-subprocess-start stage={stage}")

        output_file = output_dir / f"{stage}.raw.txt"
        log_file = logs_dir / f"{stage}.log"

        exit_code = run_triage_stage(
            prompt=prompt,
            repo_root=repo_root,
            output_file=output_file,
            log_file=log_file,
            timeout_seconds=timeout_seconds,
        )

        elapsed = int(time.monotonic() - stage_start)
        append_run_log(
            f"stage-subprocess-done stage={stage} code={exit_code} elapsed={elapsed}s"
        )

        if exit_code != 0:
            print(
                colorize(
                    f"  Stage {stage}: codex subprocess failed (exit {exit_code}).", "red"
                )
            )
            print(colorize(f"  Check log: {log_file}", "dim"))
            print(colorize("  Re-run to resume (confirmed stages are skipped).", "dim"))
            append_run_log(
                f"stage-failed stage={stage} elapsed={elapsed}s code={exit_code}"
            )
            return "failed", {
                "status": "failed",
                "exit_code": exit_code,
                "elapsed_seconds": elapsed,
            }

        if handler and handler.record_report is not None:
            report = _read_stage_output(output_file)
            if report:
                if stage == "reflect":
                    report, repair_error = _repair_reflect_report_if_needed(
                        report=report,
                        si=si,
                        prior_reports=prior_reports,
                        repo_root=repo_root,
                        prompts_dir=prompts_dir,
                        output_dir=output_dir,
                        logs_dir=logs_dir,
                        cli_command=cli_command,
                        timeout_seconds=timeout_seconds,
                        append_run_log=append_run_log,
                        stages_data=stages_data,
                    )
                    if repair_error:
                        print(
                            colorize(
                                f"  Stage {stage}: repair failed ({repair_error}).",
                                "red",
                            )
                        )
                        append_run_log(
                            f"stage-failed stage={stage} elapsed={elapsed}s reason={repair_error}"
                        )
                        return "failed", {
                            "status": "failed",
                            "elapsed_seconds": elapsed,
                            "error": repair_error,
                        }
                    if not report:
                        append_run_log(
                            f"stage-failed stage={stage} elapsed={elapsed}s reason=reflect_repair_no_report"
                        )
                        return "failed", {
                            "status": "failed",
                            "elapsed_seconds": elapsed,
                            "error": "reflect_repair_no_report",
                        }
                handler.record_report(report, args, services)
                plan_after_record = services.load_plan()
                if not _stage_report_recorded(plan_after_record, stage):
                    print(
                        colorize(
                            f"  Stage {stage}: handler completed but did not persist the stage.",
                            "red",
                        )
                    )
                    append_run_log(
                        f"stage-record-failed stage={stage} elapsed={elapsed}s reason=stage_not_recorded"
                    )
                    return "failed", {
                        "status": "failed",
                        "elapsed_seconds": elapsed,
                        "error": "stage_not_recorded",
                    }
                append_run_log(
                    f"stage-recorded stage={stage} elapsed={elapsed}s mode=orchestrator"
                )
            else:
                print(colorize(f"  Stage {stage}: output file was empty after subprocess.", "red"))
                append_run_log(
                    f"stage-failed stage={stage} elapsed={elapsed}s reason=empty_stage_output"
                )
                return "failed", {
                    "status": "failed",
                    "elapsed_seconds": elapsed,
                    "error": "empty_stage_output",
                }

    return "ready", {}


def _validate_and_confirm_stage(
    *,
    stage: str,
    args: argparse.Namespace,
    services: TriageServices,
    si: dict,
    state,
    repo_root: Path,
    stage_start: float,
    append_run_log,
) -> tuple[bool, dict, str]:
    """Run shared stage validation + confirmation flow."""
    plan = services.load_plan()

    ok, error_msg = validate_stage(stage, plan, state, repo_root, triage_input=si)
    if not ok:
        elapsed = int(time.monotonic() - stage_start)
        print(colorize(f"  Stage {stage}: validation failed: {error_msg}", "red"))
        print(colorize("  Re-run to resume.", "dim"))
        append_run_log(
            f"stage-validation-failed stage={stage} elapsed={elapsed}s error={error_msg}"
        )
        return (
            False,
            {
                "status": "validation_failed",
                "elapsed_seconds": elapsed,
                "error": error_msg,
            },
            "",
        )

    attestation = build_auto_attestation(stage, plan, si)
    confirm_args = argparse.Namespace(
        confirm=stage,
        attestation=attestation,
        state=getattr(args, "state", None),
    )

    from ..confirmations_router import cmd_confirm_stage

    cmd_confirm_stage(confirm_args, services=services)

    plan = services.load_plan()
    meta = plan.get("epic_triage_meta", {})
    stages_data = meta.get("triage_stages", {})
    elapsed = int(time.monotonic() - stage_start)
    if stage in stages_data and stages_data[stage].get("confirmed_at"):
        print(colorize(f"  Stage {stage}: confirmed ({elapsed}s).", "green"))
        append_run_log(f"stage-confirmed stage={stage} elapsed={elapsed}s")
        report = stages_data.get(stage, {}).get("report", "")
        return (
            True,
            {"status": "confirmed", "elapsed_seconds": elapsed},
            report,
        )

    print(colorize(f"  Stage {stage}: auto-confirmation did not take effect.", "red"))
    print(colorize("  Re-run to resume.", "dim"))
    append_run_log(f"stage-confirm-failed stage={stage} elapsed={elapsed}s")
    return (
        False,
        {"status": "confirm_failed", "elapsed_seconds": elapsed},
        "",
    )


def _build_completion_strategy(stages_data: dict[str, dict]) -> str:
    strategy_parts: list[str] = []
    for stage in STAGES:
        report = stages_data.get(stage, {}).get("report", "")
        if report:
            strategy_parts.append(f"[{stage}] {report[:200]}")
    strategy = " ".join(strategy_parts)
    if len(strategy) < 200:
        strategy = strategy + " " + "Automated triage via codex subagent pipeline. " * 3
    return strategy


def _complete_pipeline(
    *,
    args: argparse.Namespace,
    services: TriageServices,
    plan: dict,
    strategy: str,
    triage_input: dict,
) -> bool:
    """Run the triage completion coordinator and report success."""
    completed_before = plan.get("epic_triage_meta", {}).get("last_completed_at")

    print(colorize("\n  Completing triage...", "bold"))

    attestation = build_auto_attestation("sense-check", plan, triage_input)
    complete_args = argparse.Namespace(
        complete=True,
        strategy=strategy[:2000],
        attestation=attestation,
        state=getattr(args, "state", None),
    )

    from ..stage_completion_commands import _cmd_triage_complete

    _cmd_triage_complete(complete_args, services=services)

    completed_after = (
        services.load_plan().get("epic_triage_meta", {}).get("last_completed_at")
    )
    return bool(completed_after and completed_after != completed_before)


def run_codex_pipeline(
    args: argparse.Namespace,
    *,
    stages_to_run: list[str],
    services: TriageServices | None = None,
) -> None:
    """Run triage stages via Codex subprocesses (automated pipeline)."""
    resolved_services = services or default_triage_services()
    timeout_seconds = int(getattr(args, "stage_timeout_seconds", 1800) or 1800)
    dry_run = bool(getattr(args, "dry_run", False))

    repo_root = get_project_root()
    plan = resolved_services.load_plan()
    ensure_triage_started(plan, resolved_services, runner="codex")

    stamp = run_stamp()
    desloppify_dir = repo_root / ".desloppify"
    run_dir = desloppify_dir / "triage_runs" / stamp
    prompts_dir = run_dir / "prompts"
    output_dir = run_dir / "output"
    logs_dir = run_dir / "logs"
    for d in (prompts_dir, output_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    run_log_path = run_dir / "run.log"
    append_run_log = make_run_log_writer(run_log_path)
    cli_helper = _write_desloppify_cli_helper(run_dir)
    append_run_log(
        f"run-start runner=codex stages={','.join(stages_to_run)} "
        f"timeout={timeout_seconds}s dry_run={dry_run}"
    )

    print(colorize(f"  Run artifacts: {run_dir}", "dim"))
    print(colorize(f"  Live run log:  {run_log_path}", "dim"))
    print(colorize(f"  CLI helper:    {cli_helper}", "dim"))

    runtime = resolved_services.command_runtime(args)
    state = runtime.state

    prior_reports = _load_prior_reports_from_plan(plan)
    stage_results: dict[str, dict] = {}
    pipeline_start = time.monotonic()

    for stage in stages_to_run:
        plan = resolved_services.load_plan()
        meta = plan.get("epic_triage_meta", {})
        stages = meta.get("triage_stages", {})

        if stage in stages and stages[stage].get("confirmed_at"):
            print(colorize(f"  Stage {stage}: already confirmed, skipping.", "green"))
            append_run_log(f"stage-skip stage={stage} reason=already_confirmed")
            stage_results[stage] = {"status": "skipped"}
            report = stages[stage].get("report", "")
            if report:
                prior_reports[stage] = report
            continue

        stage_start = time.monotonic()
        append_run_log(f"stage-start stage={stage}")

        si = resolved_services.collect_triage_input(plan, state)
        exec_status, exec_result = _execute_stage(
            stage=stage,
            args=args,
            services=resolved_services,
            plan=plan,
            si=si,
            prior_reports=prior_reports,
            repo_root=repo_root,
            prompts_dir=prompts_dir,
            output_dir=output_dir,
            logs_dir=logs_dir,
            cli_command=str(cli_helper),
            stage_start=stage_start,
            timeout_seconds=timeout_seconds,
            dry_run=dry_run,
            append_run_log=append_run_log,
        )
        if exec_status == "dry_run":
            stage_results[stage] = exec_result
            continue
        if exec_status == "failed":
            stage_results[stage] = exec_result
            write_triage_run_summary(
                run_dir, stamp, stages_to_run, stage_results, append_run_log
            )
            raise CommandError(
                f"triage stage failed: {stage}. See {run_dir / 'run_summary.json'}",
                exit_code=1,
            )

        confirmed, confirm_result, report = _validate_and_confirm_stage(
            stage=stage,
            args=args,
            services=resolved_services,
            si=si,
            state=state,
            repo_root=repo_root,
            stage_start=stage_start,
            append_run_log=append_run_log,
        )
        stage_results[stage] = confirm_result
        if not confirmed:
            write_triage_run_summary(
                run_dir, stamp, stages_to_run, stage_results, append_run_log
            )
            raise CommandError(
                f"triage stage validation failed: {stage}. See {run_dir / 'run_summary.json'}",
                exit_code=1,
            )
        if report:
            prior_reports[stage] = report

    if dry_run:
        print(colorize("\n  [dry-run] All prompts generated. No stages executed.", "cyan"))
        write_triage_run_summary(run_dir, stamp, stages_to_run, stage_results, append_run_log)
        return

    plan = resolved_services.load_plan()
    meta = plan.get("epic_triage_meta", {})
    stages_data = meta.get("triage_stages", {})

    strategy = _build_completion_strategy(stages_data)

    should_auto_complete = (
        _is_full_stage_run(stages_to_run)
        and _all_stage_results_successful(
            stages_to_run=stages_to_run,
            stage_results=stage_results,
        )
    )
    if not should_auto_complete:
        total_elapsed = int(time.monotonic() - pipeline_start)
        _print_not_finalized_message("partial stage run")
        append_run_log(f"run-finished elapsed={total_elapsed}s finalized=false reason=partial_stage_run")
        write_triage_run_summary(
            run_dir,
            stamp,
            stages_to_run,
            stage_results,
            append_run_log,
            finalized=False,
            finalization_reason="partial_stage_run",
        )
        return

    completed = _complete_pipeline(
        args=args,
        services=resolved_services,
        plan=plan,
        strategy=strategy,
        triage_input=si,
    )
    total_elapsed = int(time.monotonic() - pipeline_start)
    if not completed:
        _print_not_finalized_message("completion command blocked")
        append_run_log(
            f"run-finished elapsed={total_elapsed}s finalized=false reason=completion_blocked"
        )
        write_triage_run_summary(
            run_dir,
            stamp,
            stages_to_run,
            stage_results,
            append_run_log,
            finalized=False,
            finalization_reason="completion_blocked",
        )
        return

    print(colorize(f"\n  Triage pipeline complete ({total_elapsed}s).", "green"))
    append_run_log(f"run-finished elapsed={total_elapsed}s finalized=true")
    write_triage_run_summary(
        run_dir,
        stamp,
        stages_to_run,
        stage_results,
        append_run_log,
        finalized=True,
    )


def write_triage_run_summary(
    run_dir: Path,
    stamp: str,
    stages: list[str],
    stage_results: dict[str, dict],
    append_run_log,
    *,
    finalized: bool | None = None,
    finalization_reason: str | None = None,
) -> None:
    """Write a run_summary.json with per-stage results."""
    summary = {
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_stamp": stamp,
        "runner": "codex",
        "stages_requested": stages,
        "stage_results": stage_results,
        "run_dir": str(run_dir),
    }
    if finalized is not None:
        summary["finalized"] = finalized
    if finalization_reason:
        summary["finalization_reason"] = finalization_reason
    summary_path = run_dir / "run_summary.json"
    safe_write_text(summary_path, json.dumps(summary, indent=2) + "\n")
    print(colorize(f"  Run summary: {summary_path}", "dim"))
    append_run_log(f"run-summary {summary_path}")


__all__ = ["run_codex_pipeline"]
