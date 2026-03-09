"""Direct tests for status command flow helpers."""

from __future__ import annotations

import argparse
from types import SimpleNamespace

import desloppify.app.commands.status.flow as flow_mod


def test_print_score_section_frozen_mode_uses_frozen_renderer(monkeypatch) -> None:
    frozen_calls: list[tuple[float, float]] = []
    breakdown = SimpleNamespace(queue_total=3)
    monkeypatch.setattr(flow_mod, "get_plan_start_strict", lambda _plan: 80.0)
    monkeypatch.setattr(flow_mod, "plan_aware_queue_breakdown", lambda *_a, **_k: breakdown)
    monkeypatch.setattr(flow_mod, "score_display_mode", lambda *_a, **_k: flow_mod.ScoreDisplayMode.FROZEN)
    monkeypatch.setattr(
        flow_mod,
        "print_frozen_score_with_queue_context",
        lambda _breakdown, frozen_strict, live_score: frozen_calls.append((frozen_strict, live_score)),
    )

    result = flow_mod.print_score_section(
        state={},
        scores=SimpleNamespace(overall=90.0, objective=95.0, strict=85.0, verified=84.0),
        plan={},
        target_strict_score=95.0,
        ctx=SimpleNamespace(),
    )

    assert result is breakdown
    assert frozen_calls == [(80.0, 85.0)]


def test_render_terminal_status_writes_query_payload(monkeypatch) -> None:
    written: list[dict] = []
    monkeypatch.setattr(flow_mod, "check_skill_version", lambda: None)
    monkeypatch.setattr(flow_mod, "check_config_staleness", lambda _cfg: None)
    monkeypatch.setattr(
        flow_mod.state_mod,
        "score_snapshot",
        lambda _state: SimpleNamespace(overall=90.0, objective=92.0, strict=88.0, verified=87.5),
    )
    monkeypatch.setattr(flow_mod, "target_strict_score_from_config", lambda _cfg: 95.0)
    monkeypatch.setattr(flow_mod, "resolve_lang", lambda _args: SimpleNamespace(name="python"))
    monkeypatch.setattr(flow_mod, "load_plan", lambda: {"queue_order": ["x"]})
    monkeypatch.setattr(flow_mod, "print_triage_guardrail_info", lambda **_k: None)
    monkeypatch.setattr(flow_mod, "compute_narrative", lambda *_a, **_k: {"headline": ""})
    monkeypatch.setattr(flow_mod, "queue_context", lambda *_a, **_k: SimpleNamespace(policy=SimpleNamespace(objective_count=0)))
    monkeypatch.setattr(flow_mod, "print_score_section", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "print_scan_metrics", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "print_open_scope_breakdown", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "print_scan_completeness", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "show_tier_progress_table", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "show_review_summary", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "show_structural_areas", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "render_uncommitted_reminder", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "show_agent_plan", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "show_ignore_summary", lambda *_a, **_k: None)
    monkeypatch.setattr(flow_mod, "write_status_query", lambda **payload: written.append(payload))

    flow_mod.render_terminal_status(
        argparse.Namespace(),
        state={},
        config={},
        stats={"by_tier": {}},
        dim_scores={},
        scorecard_dims=[],
        subjective_measures=[],
        suppression={},
    )

    assert written
    payload = written[0]
    assert payload["overall_score"] == 90.0
    assert payload["strict_score"] == 88.0
    assert payload["plan"] == {"queue_order": ["x"]}
