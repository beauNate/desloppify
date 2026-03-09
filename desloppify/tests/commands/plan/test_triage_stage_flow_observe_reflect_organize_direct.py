"""Direct tests for observe/reflect/organize stage flow module."""

from __future__ import annotations

import argparse
from types import SimpleNamespace

import desloppify.app.commands.plan.triage.stage_flow_observe_reflect_organize as flow_mod


def _args(**overrides) -> argparse.Namespace:
    base = {"report": None, "attestation": None}
    base.update(overrides)
    return argparse.Namespace(**base)


def _services(plan: dict, *, state: dict | None = None):
    saved: list[dict] = []
    logs: list[tuple[str, dict]] = []
    state_obj = state or {}
    return SimpleNamespace(
        command_runtime=lambda _args: SimpleNamespace(state=state_obj),
        load_plan=lambda: plan,
        save_plan=lambda current: saved.append(current.copy()),
        collect_triage_input=lambda _plan, _state: SimpleNamespace(
            open_issues={},
            resolved_issues={},
        ),
        extract_issue_citations=lambda _report, _valid_ids: set(),
        append_log_entry=lambda _plan, action, **kwargs: logs.append((action, kwargs)),
        detect_recurring_patterns=lambda _open, _resolved: {},
    ), saved, logs


def test_observe_autostarts_planning_and_requires_report(monkeypatch) -> None:
    plan = {"epic_triage_meta": {}}
    services, saved, _logs = _services(plan)
    called: list[str] = []
    monkeypatch.setattr(flow_mod, "_print_observe_report_requirement", lambda: called.append("required"))

    flow_mod._cmd_stage_observe(
        _args(report=None),
        services=services,
        has_triage_in_queue_fn=lambda _plan: False,
        inject_triage_stages_fn=lambda _plan: _plan.setdefault("queue_order", []).append("workflow::observe"),
    )

    assert called == ["required"]
    assert saved  # planning mode auto-start save happened
    assert "workflow::observe" in plan.get("queue_order", [])


def test_observe_zero_issue_path_records_stage_and_saves(monkeypatch) -> None:
    plan = {"epic_triage_meta": {"triage_stages": {}}}
    services, saved, _logs = _services(plan)
    monkeypatch.setattr(flow_mod, "record_observe_stage", lambda stages, **_k: stages.setdefault("observe", {}) or [])

    flow_mod._cmd_stage_observe(
        _args(report="short but valid"),
        services=services,
        has_triage_in_queue_fn=lambda _plan: True,
        inject_triage_stages_fn=lambda _plan: None,
    )

    assert saved
    assert "observe" in plan["epic_triage_meta"]["triage_stages"]


def test_reflect_exits_when_triage_not_in_queue(monkeypatch, capsys) -> None:
    plan = {"epic_triage_meta": {"triage_stages": {}}}
    services, _saved, _logs = _services(plan)
    monkeypatch.setattr(flow_mod, "has_triage_in_queue", lambda _plan: False)

    flow_mod._cmd_stage_reflect(_args(report="r" * 120), services=services)
    out = capsys.readouterr().out
    assert "No planning stages in the queue" in out


def test_organize_exits_when_reflect_requirement_not_met(monkeypatch) -> None:
    plan = {"epic_triage_meta": {"triage_stages": {}}, "clusters": {}}
    services, _saved, _logs = _services(plan, state={"issues": {}})
    monkeypatch.setattr(flow_mod, "has_triage_in_queue", lambda _plan: True)
    monkeypatch.setattr(flow_mod, "_require_reflect_stage_for_organize", lambda _stages: False)

    flow_mod._cmd_stage_organize(_args(report="organized"), services=services)


def test_public_wrappers_delegate_to_private(monkeypatch) -> None:
    called: list[str] = []
    monkeypatch.setattr(flow_mod, "_cmd_stage_observe", lambda *args, **kwargs: called.append("observe"))
    monkeypatch.setattr(flow_mod, "_cmd_stage_reflect", lambda *args, **kwargs: called.append("reflect"))
    monkeypatch.setattr(flow_mod, "_cmd_stage_organize", lambda *args, **kwargs: called.append("organize"))

    args = _args()
    flow_mod.cmd_stage_observe(args)
    flow_mod.cmd_stage_reflect(args)
    flow_mod.cmd_stage_organize(args)

    assert called == ["observe", "reflect", "organize"]
