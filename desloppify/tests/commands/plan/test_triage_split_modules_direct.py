"""Direct coverage tests for split triage validation/orchestrator modules."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import desloppify.app.commands.plan.triage._stage_validation_completion_policy as completion_policy_mod
import desloppify.app.commands.plan.triage._stage_validation_completion_stages as completion_stages_mod
import desloppify.app.commands.plan.triage._stage_validation_enrich_checks as enrich_checks_mod
import desloppify.app.commands.plan.triage.confirmations_basic as confirmations_basic_mod
import desloppify.app.commands.plan.triage.confirmations_enrich as confirmations_enrich_mod
import desloppify.app.commands.plan.triage.confirmations_organize as confirmations_organize_mod
import desloppify.app.commands.plan.triage.display_layout as display_layout_mod
import desloppify.app.commands.plan.triage.runner.orchestrator_claude as orchestrator_claude_mod
import desloppify.app.commands.plan.triage.runner.orchestrator_codex_observe as orchestrator_observe_mod
import desloppify.app.commands.plan.triage.runner.orchestrator_codex_pipeline as orchestrator_pipeline_mod
import desloppify.app.commands.plan.triage.runner.orchestrator_codex_sense as orchestrator_sense_mod
import desloppify.app.commands.plan.triage.runner.orchestrator_common as orchestrator_common_mod
from desloppify.base.exception_sets import CommandError


def test_completion_policy_helpers_cover_success_and_fail_paths(monkeypatch, capsys) -> None:
    monkeypatch.setattr(completion_policy_mod, "manual_clusters_with_issues", lambda _plan: ["c1"])
    monkeypatch.setattr(completion_policy_mod, "unenriched_clusters", lambda _plan: [])
    monkeypatch.setattr(completion_policy_mod, "unclustered_review_issues", lambda _plan, _state: [])
    assert completion_policy_mod._completion_clusters_valid({"clusters": {}}, state={}) is True

    assert completion_policy_mod._resolve_completion_strategy("keep", meta={}) == "keep"
    assert completion_policy_mod._resolve_completion_strategy(None, meta={}) is None
    assert completion_policy_mod._completion_strategy_valid("same") is True
    assert completion_policy_mod._completion_strategy_valid("x" * 220) is True
    assert completion_policy_mod._completion_strategy_valid("too short") is False

    assert completion_policy_mod._require_prior_strategy_for_confirm({"strategy_summary": "ok"}) is True
    assert completion_policy_mod._require_prior_strategy_for_confirm({}) is False
    assert completion_policy_mod._confirm_note_valid("x" * 100) is True
    assert completion_policy_mod._confirm_note_valid("short") is False

    assert (
        completion_policy_mod._resolve_confirm_existing_strategy(
            "same",
            has_only_additions=False,
            meta={},
        )
        == "same"
    )
    assert (
        completion_policy_mod._resolve_confirm_existing_strategy(
            None,
            has_only_additions=True,
            meta={},
        )
        == "same"
    )
    assert completion_policy_mod._confirm_strategy_valid("x" * 220) is True
    assert completion_policy_mod._confirm_strategy_valid("short") is False

    monkeypatch.setattr(
        completion_policy_mod,
        "extract_issue_citations",
        lambda _note, valid_ids: set(valid_ids),
    )
    si = SimpleNamespace(
        new_since_last={"review::a.py::id1"},
        open_issues={"review::a.py::id1": {}},
    )
    assert completion_policy_mod._note_cites_new_issues_or_error("review::a.py::id1", si) is True
    monkeypatch.setattr(completion_policy_mod, "extract_issue_citations", lambda _note, _ids: set())
    assert completion_policy_mod._note_cites_new_issues_or_error("no citation", si) is False

    out = capsys.readouterr().out
    assert "Strategy too short" in out


def test_completion_stage_helpers_include_gate_and_auto_confirm_defaults(monkeypatch, capsys) -> None:
    plan = {"clusters": {"a": {"issue_ids": ["id1"], "action_steps": []}}}

    assert (
        completion_stages_mod._require_organize_stage_for_complete(
            plan=plan,
            meta={},
            stages={},
        )
        is False
    )
    assert (
        completion_stages_mod._require_enrich_stage_for_complete(
            plan=plan,
            meta={},
            stages={"organize": {}},
        )
        is False
    )
    assert (
        completion_stages_mod._require_sense_check_stage_for_complete(
            plan=plan,
            meta={},
            stages={"enrich": {}},
        )
        is False
    )

    assert (
        completion_stages_mod._auto_confirm_organize_for_complete(
            plan=plan,
            stages={},
            attestation=None,
        )
        is False
    )
    assert (
        completion_stages_mod._auto_confirm_enrich_for_complete(
            plan=plan,
            stages={},
            attestation=None,
        )
        is False
    )
    assert (
        completion_stages_mod._auto_confirm_sense_check_for_complete(
            plan=plan,
            stages={},
            attestation=None,
        )
        is False
    )

    out = capsys.readouterr().out
    assert "Cannot complete" in out


def test_enrich_checks_helpers_cover_main_signals(tmp_path, capsys) -> None:
    plan = {
        "clusters": {
            "manual": {
                "issue_ids": ["i1", "i2", "i3"],
                "action_steps": [
                    {"title": "Fix A", "detail": "short", "issue_refs": []},
                    {
                        "title": "Fix B",
                        "detail": "edit src/missing/file.ts and update behavior",
                        "issue_refs": ["review::a.py::1"],
                    },
                    {
                        "title": "Fix C",
                        "detail": (
                            "update src/a/file.ts and src/b/file.ts and src/c/file.ts and "
                            "src/d/file.ts and src/e/file.ts and src/f/file.ts"
                        ),
                        "issue_refs": ["review::a.py::2"],
                        "effort": "small",
                    },
                ],
            }
        },
        "issues": {"review::a.py::2": {"status": "wontfix"}},
    }

    assert enrich_checks_mod._require_organize_stage_for_enrich({"observe": {}, "reflect": {}}) is False
    assert enrich_checks_mod._underspecified_steps(plan) == [("manual", 1, 3)]
    assert enrich_checks_mod._steps_without_effort(plan) == [("manual", 2, 3)]
    assert enrich_checks_mod._steps_missing_issue_refs(plan) == [("manual", 1, 3)]
    assert enrich_checks_mod._clusters_with_high_step_ratio(plan) == []

    bad_paths = enrich_checks_mod._steps_with_bad_paths(plan, tmp_path)
    assert bad_paths
    vague = enrich_checks_mod._steps_with_vague_detail(plan, tmp_path)
    assert vague
    stale_refs = enrich_checks_mod._steps_referencing_skipped_issues(plan)
    assert stale_refs == [("manual", 3, ["review::a.py::2"])]

    assert enrich_checks_mod._enrich_report_or_error("x" * 120) == "x" * 120
    assert enrich_checks_mod._enrich_report_or_error("short") is None

    out = capsys.readouterr().out
    assert "Report too short" in out


def test_confirmation_modules_stage_presence_guards(capsys) -> None:
    args = argparse.Namespace()
    confirmations_basic_mod.confirm_observe(args, {}, {}, None)
    confirmations_basic_mod.confirm_reflect(args, {}, {}, None)
    confirmations_enrich_mod.confirm_enrich(args, {}, {}, None)
    confirmations_enrich_mod.confirm_sense_check(args, {}, {}, None)
    confirmations_organize_mod.confirm_organize(args, {}, {}, None)
    out = capsys.readouterr().out
    assert "Cannot confirm" in out


def test_validate_attestation_rules() -> None:
    assert confirmations_basic_mod.validate_attestation("mentions naming", "observe", dimensions=["Naming"]) is None
    err = confirmations_basic_mod.validate_attestation(
        "generic text",
        "reflect",
        dimensions=["Naming"],
        cluster_names=["cluster-a"],
    )
    assert err is not None


def test_display_layout_renderers(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        display_layout_mod,
        "print_stage_progress",
        lambda _stages, _plan: print("stage-progress"),
    )

    si = SimpleNamespace(
        open_issues={
            "review::src/a.py::id1": {
                "summary": "Issue one",
                "detail": {"dimension": "naming", "suggestion": "rename value"},
            }
        },
        existing_epics=[],
        new_since_last={"review::src/a.py::id1"},
        resolved_since_last=set(),
    )
    stages = {"observe": {"report": "observe report"}, "reflect": {"report": "reflect report"}}
    meta = {"strategy_summary": "Legacy strategy summary"}
    plan = {
        "clusters": {
            "manual": {
                "issue_ids": ["review::src/a.py::id1"],
                "action_steps": [{"title": "Do thing"}],
                "description": "manual cluster",
            }
        },
        "queue_order": ["review::src/a.py::id1"],
    }
    state = {"issues": si.open_issues}

    display_layout_mod.print_dashboard_header(si, stages, meta, plan)
    display_layout_mod.print_action_guidance(stages, meta, si, plan)
    display_layout_mod.print_prior_stage_reports(stages)
    display_layout_mod.print_issues_by_dimension(si.open_issues)
    display_layout_mod.show_plan_summary(plan, state)

    out = capsys.readouterr().out
    assert "Epic triage" in out
    assert "stage-progress" in out
    assert "Review issues by dimension" in out
    assert "Coverage:" in out
    assert "reusing the current enriched cluster plan" in out


def test_orchestrator_common_helpers(monkeypatch) -> None:
    assert orchestrator_common_mod.parse_only_stages(None) == list(orchestrator_common_mod.STAGES)
    assert orchestrator_common_mod.parse_only_stages("observe,reflect") == ["observe", "reflect"]
    with pytest.raises(ValueError):
        orchestrator_common_mod.parse_only_stages("invalid")

    stamp = orchestrator_common_mod.run_stamp()
    assert len(stamp) == 15

    saved: list[dict] = []
    entries: list[tuple[str, dict]] = []
    monkeypatch.setattr(orchestrator_common_mod, "has_triage_in_queue", lambda _plan: False)
    monkeypatch.setattr(
        orchestrator_common_mod,
        "inject_triage_stages",
        lambda plan: plan.setdefault("queue_order", []).append("triage::observe"),
    )
    plan = {"queue_order": []}
    services = SimpleNamespace(
        save_plan=lambda p: saved.append(p),
        append_log_entry=lambda _plan, action, **kwargs: entries.append((action, kwargs["detail"])),
    )
    updated = orchestrator_common_mod.ensure_triage_started(plan, services, runner="codex")
    assert "triage::observe" in updated["queue_order"]
    assert saved
    assert entries == [
        (
            "triage_auto_start",
            {
                "source": "runner_auto_start",
                "runner": "codex",
                "injected_stage_ids": list(orchestrator_common_mod.TRIAGE_STAGE_IDS),
            },
        )
    ]


def test_orchestrator_claude_prints_instructions(monkeypatch, capsys) -> None:
    monkeypatch.setattr(orchestrator_claude_mod, "ensure_triage_started", lambda *_a, **_k: None)
    services = SimpleNamespace(load_plan=lambda: {}, save_plan=lambda _plan: None)
    orchestrator_claude_mod.run_claude_orchestrator(argparse.Namespace(), services=services)
    out = capsys.readouterr().out
    assert "Claude triage orchestrator mode" in out


def test_orchestrator_observe_helpers_and_dry_run(monkeypatch, tmp_path, capsys) -> None:
    output_file = tmp_path / "observe.txt"
    output_file.write_text("batch output", encoding="utf-8")
    merged = orchestrator_observe_mod._merge_observe_outputs([(["naming"], output_file)])
    assert "Dimensions: naming" in merged

    monkeypatch.setattr(
        orchestrator_observe_mod,
        "group_issues_into_observe_batches",
        lambda _si: [(["naming"], [{"id": "1"}])],
    )
    monkeypatch.setattr(
        orchestrator_observe_mod,
        "build_observe_batch_prompt",
        lambda **_kwargs: "prompt",
    )

    ok, report = orchestrator_observe_mod.run_observe(
        si=SimpleNamespace(),
        repo_root=tmp_path,
        prompts_dir=tmp_path / "prompts",
        output_dir=tmp_path / "out",
        logs_dir=tmp_path / "logs",
        timeout_seconds=60,
        dry_run=True,
    )
    assert ok is True
    assert report == ""
    out = capsys.readouterr().out
    assert "[dry-run]" in out


def test_orchestrator_sense_dry_run(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.setattr(orchestrator_sense_mod, "manual_clusters_with_issues", lambda _plan: ["cluster-a"])
    monkeypatch.setattr(
        orchestrator_sense_mod,
        "build_sense_check_content_prompt",
        lambda **_kwargs: "content prompt",
    )
    monkeypatch.setattr(
        orchestrator_sense_mod,
        "build_sense_check_structure_prompt",
        lambda **_kwargs: "structure prompt",
    )

    ok, report = orchestrator_sense_mod.run_sense_check(
        plan={"clusters": {"cluster-a": {"issue_ids": ["id1"]}}},
        repo_root=tmp_path,
        prompts_dir=tmp_path / "prompts",
        output_dir=tmp_path / "out",
        logs_dir=tmp_path / "logs",
        timeout_seconds=60,
        dry_run=True,
    )
    assert ok is True
    assert report == ""
    out = capsys.readouterr().out
    assert "[dry-run]" in out


def test_orchestrator_pipeline_summary_writer(tmp_path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True)
    messages: list[str] = []

    orchestrator_pipeline_mod.write_triage_run_summary(
        run_dir,
        stamp="20260309_120000",
        stages=["observe"],
        stage_results={"observe": {"status": "confirmed"}},
        append_run_log=messages.append,
    )

    summary_path = run_dir / "run_summary.json"
    assert summary_path.exists()
    text = summary_path.read_text(encoding="utf-8")
    assert '"runner": "codex"' in text
    assert messages


def test_orchestrator_pipeline_completion_guards() -> None:
    assert orchestrator_pipeline_mod._is_full_stage_run(
        ["observe", "reflect", "organize", "enrich", "sense-check"]
    ) is True
    assert orchestrator_pipeline_mod._is_full_stage_run(["observe", "reflect"]) is False

    assert orchestrator_pipeline_mod._all_stage_results_successful(
        stages_to_run=["observe", "reflect"],
        stage_results={
            "observe": {"status": "confirmed"},
            "reflect": {"status": "skipped"},
        },
    ) is True
    assert orchestrator_pipeline_mod._all_stage_results_successful(
        stages_to_run=["observe", "reflect"],
        stage_results={
            "observe": {"status": "confirmed"},
            "reflect": {"status": "failed"},
        },
    ) is False


def test_orchestrator_pipeline_summary_writer_includes_finalization_fields(tmp_path) -> None:
    run_dir = tmp_path / "run2"
    run_dir.mkdir(parents=True)
    messages: list[str] = []

    orchestrator_pipeline_mod.write_triage_run_summary(
        run_dir,
        stamp="20260309_120001",
        stages=["observe", "reflect"],
        stage_results={"observe": {"status": "confirmed"}},
        append_run_log=messages.append,
        finalized=False,
        finalization_reason="partial_stage_run",
    )

    summary_path = run_dir / "run_summary.json"
    text = summary_path.read_text(encoding="utf-8")
    assert '"finalized": false' in text
    assert '"finalization_reason": "partial_stage_run"' in text


def test_orchestrator_pipeline_entrypoint_is_exposed() -> None:
    assert callable(orchestrator_pipeline_mod.run_codex_pipeline)


def test_orchestrator_pipeline_writes_exact_cli_helper(tmp_path: Path) -> None:
    helper = orchestrator_pipeline_mod._write_desloppify_cli_helper(tmp_path)
    text = helper.read_text(encoding="utf-8")
    assert helper.exists()
    assert helper.stat().st_mode & 0o111
    assert "PYTHONPATH=" in text
    assert "-m desloppify.cli" in text


def test_load_prior_reports_from_plan_uses_existing_stage_reports() -> None:
    plan = {
        "epic_triage_meta": {
            "triage_stages": {
                "observe": {"report": "observe report"},
                "reflect": {"report": "reflect report"},
                "organize": {"report": ""},
            }
        }
    }

    prior = orchestrator_pipeline_mod._load_prior_reports_from_plan(plan)
    assert prior == {
        "observe": "observe report",
        "reflect": "reflect report",
    }


def test_execute_stage_records_output_only_reflect_report(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    plan_store = {"epic_triage_meta": {"triage_stages": {}}}
    for dirname in ("prompts", "output", "logs"):
        (tmp_path / dirname).mkdir()

    monkeypatch.setattr(orchestrator_pipeline_mod, "build_stage_prompt", lambda *args, **kwargs: "prompt")

    def fake_run_triage_stage(*, prompt, repo_root, output_file, log_file, timeout_seconds):
        del prompt, repo_root, log_file, timeout_seconds
        output_file.write_text("Reflect analysis report with enough detail.", encoding="utf-8")
        return 0

    monkeypatch.setattr(orchestrator_pipeline_mod, "run_triage_stage", fake_run_triage_stage)
    monkeypatch.setitem(
        orchestrator_pipeline_mod._STAGE_HANDLERS,
        "reflect",
        orchestrator_pipeline_mod.StageHandler(
            record_report=lambda report, _args, _services: (
                captured.setdefault("report", report),
                plan_store["epic_triage_meta"]["triage_stages"].update(
                    {"reflect": {"report": report}}
                ),
            ),
            prompt_mode="output_only",
        ),
    )

    status, result = orchestrator_pipeline_mod._execute_stage(
        stage="reflect",
        args=argparse.Namespace(state=None),
        services=SimpleNamespace(load_plan=lambda: plan_store),
        plan={},
        si={},
        prior_reports={},
        repo_root=tmp_path,
        prompts_dir=tmp_path / "prompts",
        output_dir=tmp_path / "output",
        logs_dir=tmp_path / "logs",
        cli_command="/tmp/run_desloppify.sh",
        stage_start=time.monotonic(),
        timeout_seconds=60,
        dry_run=False,
        append_run_log=lambda _line: None,
    )

    assert status == "ready"
    assert result == {}
    assert captured["report"] == "Reflect analysis report with enough detail."


def test_execute_stage_uses_self_record_mode_for_organize(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    for dirname in ("prompts", "output", "logs"):
        (tmp_path / dirname).mkdir()

    def fake_build_stage_prompt(stage, triage_input, prior_reports, *, repo_root, mode, cli_command, stages_data=None):
        del triage_input, prior_reports, repo_root, stages_data
        captured["stage"] = stage
        captured["mode"] = mode
        captured["cli_command"] = cli_command
        return "prompt"

    def fake_run_triage_stage(*, prompt, repo_root, output_file, log_file, timeout_seconds):
        del prompt, repo_root, log_file, timeout_seconds
        output_file.write_text("Organize summary.", encoding="utf-8")
        return 0

    monkeypatch.setattr(orchestrator_pipeline_mod, "build_stage_prompt", fake_build_stage_prompt)
    monkeypatch.setattr(orchestrator_pipeline_mod, "run_triage_stage", fake_run_triage_stage)

    status, result = orchestrator_pipeline_mod._execute_stage(
        stage="organize",
        args=argparse.Namespace(state=None),
        services=SimpleNamespace(),
        plan={},
        si={},
        prior_reports={"reflect": "report"},
        repo_root=tmp_path,
        prompts_dir=tmp_path / "prompts",
        output_dir=tmp_path / "output",
        logs_dir=tmp_path / "logs",
        cli_command="/tmp/run_desloppify.sh",
        stage_start=time.monotonic(),
        timeout_seconds=60,
        dry_run=False,
        append_run_log=lambda _line: None,
    )

    assert status == "ready"
    assert result == {}
    assert captured["stage"] == "organize"
    assert captured["mode"] == "self_record"
    assert captured["cli_command"] == "/tmp/run_desloppify.sh"


def test_execute_stage_blocks_organize_when_reflect_accounting_is_invalid(
    monkeypatch,
    tmp_path: Path,
) -> None:
    for dirname in ("prompts", "output", "logs"):
        (tmp_path / dirname).mkdir()

    monkeypatch.setattr(
        orchestrator_pipeline_mod,
        "_validate_reflect_issue_accounting",
        lambda **_kwargs: (False, set(), ["review::x::deadbeef"], []),
    )

    status, result = orchestrator_pipeline_mod._execute_stage(
        stage="organize",
        args=argparse.Namespace(state=None),
        services=SimpleNamespace(),
        plan={
            "epic_triage_meta": {
                "triage_stages": {"reflect": {"report": "bad reflect blueprint"}}
            }
        },
        si=SimpleNamespace(open_issues={"review::x::deadbeef": {}}),
        prior_reports={"reflect": "bad reflect blueprint"},
        repo_root=tmp_path,
        prompts_dir=tmp_path / "prompts",
        output_dir=tmp_path / "output",
        logs_dir=tmp_path / "logs",
        cli_command="/tmp/run_desloppify.sh",
        stage_start=time.monotonic(),
        timeout_seconds=60,
        dry_run=False,
        append_run_log=lambda _line: None,
    )

    assert status == "failed"
    assert result["error"].startswith("reflect_accounting_invalid")


def test_repair_reflect_report_if_needed_repairs_missing_hashes(monkeypatch, tmp_path: Path) -> None:
    for dirname in ("prompts", "output", "logs"):
        (tmp_path / dirname).mkdir()

    repaired_report = """
## Coverage Ledger
- aaaabbbb -> cluster "alpha"
- ccccdddd -> skip "false-positive"

## Cluster Blueprint
Cluster "alpha" owns the actual code changes.

## Execution Order
1. alpha
"""

    monkeypatch.setattr(orchestrator_pipeline_mod, "run_triage_stage", lambda **_kwargs: 0)
    monkeypatch.setattr(orchestrator_pipeline_mod, "build_stage_prompt", lambda *a, **k: "repair prompt")
    monkeypatch.setattr(
        orchestrator_pipeline_mod,
        "_read_stage_output",
        lambda _path: repaired_report,
    )

    report, error = orchestrator_pipeline_mod._repair_reflect_report_if_needed(
        report=(
            "## Coverage Ledger\n"
            '- aaaabbbb -> cluster "alpha"\n\n'
            "## Cluster Blueprint\n"
            "Cluster alpha is the main work."
        ),
        si=SimpleNamespace(
            open_issues={
                "review::src/a.ts::alpha::aaaabbbb": {},
                "review::src/b.ts::beta::ccccdddd": {},
            }
        ),
        prior_reports={"observe": "Observed the issues carefully."},
        repo_root=tmp_path,
        prompts_dir=tmp_path / "prompts",
        output_dir=tmp_path / "output",
        logs_dir=tmp_path / "logs",
        cli_command="/tmp/run_desloppify.sh",
        timeout_seconds=30,
        append_run_log=lambda _line: None,
    )

    assert error is None
    assert report == repaired_report


def test_execute_stage_fails_when_handler_does_not_persist_stage(monkeypatch, tmp_path: Path) -> None:
    for dirname in ("prompts", "output", "logs"):
        (tmp_path / dirname).mkdir()

    monkeypatch.setattr(orchestrator_pipeline_mod, "build_stage_prompt", lambda *a, **k: "prompt")
    monkeypatch.setattr(orchestrator_pipeline_mod, "run_triage_stage", lambda **_kwargs: 0)
    monkeypatch.setattr(orchestrator_pipeline_mod, "_read_stage_output", lambda _path: "x" * 120)
    monkeypatch.setitem(
        orchestrator_pipeline_mod._STAGE_HANDLERS,
        "reflect",
        orchestrator_pipeline_mod.StageHandler(record_report=lambda *_a, **_k: None),
    )

    services = SimpleNamespace(load_plan=lambda: {"epic_triage_meta": {"triage_stages": {}}})

    status, result = orchestrator_pipeline_mod._execute_stage(
        stage="reflect",
        args=argparse.Namespace(state=None),
        services=services,
        plan={"epic_triage_meta": {"triage_stages": {"observe": {"report": "ok"}}}},
        si=SimpleNamespace(open_issues={}),
        prior_reports={"observe": "ok"},
        repo_root=tmp_path,
        prompts_dir=tmp_path / "prompts",
        output_dir=tmp_path / "output",
        logs_dir=tmp_path / "logs",
        cli_command="/tmp/run_desloppify.sh",
        stage_start=time.monotonic(),
        timeout_seconds=60,
        dry_run=False,
        append_run_log=lambda _line: None,
    )

    assert status == "failed"
    assert result["error"] == "stage_not_recorded"


def test_run_codex_pipeline_raises_on_stage_failure(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(orchestrator_pipeline_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(orchestrator_pipeline_mod, "run_stamp", lambda: "20260309_151500")
    monkeypatch.setattr(
        orchestrator_pipeline_mod,
        "_write_desloppify_cli_helper",
        lambda run_dir: run_dir / "run_desloppify.sh",
    )
    monkeypatch.setattr(
        orchestrator_pipeline_mod,
        "_execute_stage",
        lambda **_kwargs: ("failed", {"status": "failed", "error": "boom"}),
    )

    services = SimpleNamespace(
        load_plan=lambda: {"epic_triage_meta": {"triage_stages": {}}},
        command_runtime=lambda _args: SimpleNamespace(state={}),
        collect_triage_input=lambda _plan, _state: SimpleNamespace(open_issues={}, resolved_issues={}),
    )
    monkeypatch.setattr(
        orchestrator_pipeline_mod,
        "default_triage_services",
        lambda: services,
    )
    monkeypatch.setattr(orchestrator_pipeline_mod, "ensure_triage_started", lambda *_a, **_k: None)

    with pytest.raises(CommandError) as excinfo:
        orchestrator_pipeline_mod.run_codex_pipeline(
            argparse.Namespace(stage_timeout_seconds=30, dry_run=False, state=None),
            stages_to_run=["organize"],
            services=services,
        )

    assert excinfo.value.exit_code == 1
    assert "triage stage failed: organize" in excinfo.value.message
