"""Direct tests for review importing support modules."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import desloppify.app.commands.review.importing.flags as flags_mod
import desloppify.app.commands.review.importing.plan_sync as plan_sync_mod
import desloppify.app.commands.review.importing.results as results_mod
import desloppify.engine.plan as plan_mod


def test_flags_validation_and_assessment_state_helpers() -> None:
    with pytest.raises(flags_mod.ImportFlagValidationError):
        flags_mod.validate_import_flag_combos(
            attested_external=True,
            allow_partial=False,
            override_enabled=True,
            override_attest="ok",
        )
    with pytest.raises(flags_mod.ImportFlagValidationError):
        flags_mod.validate_import_flag_combos(
            attested_external=False,
            allow_partial=True,
            override_enabled=True,
            override_attest="ok",
        )

    keys = flags_mod.imported_assessment_keys(
        {"assessments": {"Naming Quality": 70, "": 50}}
    )
    assert keys == {"naming_quality"}

    state = {
        "scan_count": 4,
        "subjective_assessments": {"naming_quality": {"source": "holistic"}},
    }
    marked = flags_mod.mark_manual_override_assessments_provisional(
        state,
        assessment_keys={"naming_quality"},
    )
    assert marked == 1
    assert state["subjective_assessments"]["naming_quality"]["provisional_until_scan"] == 5

    cleared = flags_mod.clear_provisional_override_flags(
        state,
        assessment_keys={"naming_quality"},
    )
    assert cleared == 1
    assert state["subjective_assessments"]["naming_quality"]["source"] == "holistic"


def test_sync_plan_after_import_no_living_plan(monkeypatch) -> None:
    monkeypatch.setattr(plan_mod, "has_living_plan", lambda: False)
    plan_sync_mod.sync_plan_after_import(
        state={},
        diff={"new": 1, "reopened": 0},
        assessment_mode="issues_only",
    )


def test_sync_plan_after_import_handles_plan_exceptions(monkeypatch, capsys) -> None:
    monkeypatch.setattr(plan_mod, "has_living_plan", lambda: True)
    monkeypatch.setattr(plan_mod, "load_plan", lambda: (_ for _ in ()).throw(OSError("boom")))
    monkeypatch.setattr(plan_sync_mod, "PLAN_LOAD_EXCEPTIONS", (OSError,))

    plan_sync_mod.sync_plan_after_import(
        state={},
        diff={"new": 1, "reopened": 0},
        assessment_mode="issues_only",
    )
    out = capsys.readouterr().out
    assert "skipped plan sync after review import" in out


def test_sync_plan_after_import_logs_triage_provenance(monkeypatch) -> None:
    plan: dict = {"queue_order": []}
    entries: list[tuple[str, dict]] = []

    monkeypatch.setattr(plan_mod, "has_living_plan", lambda: True)
    monkeypatch.setattr(plan_mod, "load_plan", lambda: plan)
    monkeypatch.setattr(plan_mod, "save_plan", lambda _plan: None)
    monkeypatch.setattr(plan_mod, "current_unscored_ids", lambda _state: set())
    monkeypatch.setattr(plan_mod, "purge_ids", lambda _plan, _ids: None)
    monkeypatch.setattr(
        plan_mod,
        "sync_plan_after_review_import",
        lambda _plan, _state: SimpleNamespace(
            new_ids={"review::x"},
            added_to_queue=["review::x"],
            triage_injected=True,
            triage_injected_ids=["triage::observe", "triage::reflect"],
            triage_deferred=False,
        ),
    )
    monkeypatch.setattr(plan_mod, "sync_score_checkpoint_needed", lambda _plan, _state: SimpleNamespace(changes=False))
    monkeypatch.setattr(plan_mod, "sync_import_scores_needed", lambda _plan, _state, assessment_mode: SimpleNamespace(changes=False))
    monkeypatch.setattr(plan_mod, "sync_create_plan_needed", lambda _plan, _state: SimpleNamespace(changes=False))
    monkeypatch.setattr(
        plan_mod,
        "append_log_entry",
        lambda _plan, action, **kwargs: entries.append((action, kwargs["detail"])),
    )

    plan_sync_mod.sync_plan_after_import(
        state={},
        diff={"new": 1, "reopened": 0},
        assessment_mode="issues_only",
    )

    assert entries
    action, detail = entries[-1]
    assert action == "review_import_sync"
    assert detail["triage_injected"] is True
    assert detail["triage_injected_ids"] == ["triage::observe", "triage::reflect"]
    assert detail["triage_deferred"] is False


def test_print_import_results_writes_query_payload(monkeypatch) -> None:
    captured: list[dict] = []
    monkeypatch.setattr(results_mod.narrative_mod, "compute_narrative", lambda *_a, **_k: {"summary": "ok"})
    monkeypatch.setattr(results_mod.import_helpers_mod, "print_skipped_validation_details", lambda *_a, **_k: None)
    monkeypatch.setattr(results_mod.import_helpers_mod, "print_assessments_summary", lambda *_a, **_k: None)
    monkeypatch.setattr(
        results_mod.import_helpers_mod,
        "print_open_review_summary",
        lambda *_a, **_k: "desloppify next",
    )
    monkeypatch.setattr(
        results_mod.import_helpers_mod,
        "print_review_import_scores_and_integrity",
        lambda *_a, **_k: [{"name": "Design coherence", "score": 95.0}],
    )
    monkeypatch.setattr(results_mod, "show_score_with_plan_context", lambda *_a, **_k: None)
    monkeypatch.setattr(results_mod, "write_query", lambda payload: captured.append(payload))

    results_mod.print_import_results(
        state={"issues": {}},
        lang_name="python",
        config={},
        diff={"new": 2, "auto_resolved": 1, "reopened": 0},
        prev=SimpleNamespace(overall=0),
        label="Holistic review",
        provisional_count=0,
        assessment_policy=SimpleNamespace(mode="issues_only", trusted=False, reason="untrusted"),
        scorecard_subjective_at_target_fn=lambda *_a, **_k: [],
    )

    assert captured
    payload = captured[0]
    assert payload["command"] == "review"
    assert payload["action"] == "import"
    assert payload["next_command"] == "desloppify next"
    assert payload["assessment_import"]["mode"] == "issues_only"
