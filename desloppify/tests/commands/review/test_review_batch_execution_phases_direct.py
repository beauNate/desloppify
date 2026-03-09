"""Direct tests for review batch execution phases module."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import desloppify.app.commands.review.batch.execution_phases as phases_mod


def _policy(**overrides):
    base = {
        "run_parallel": True,
        "max_parallel_batches": 2,
        "heartbeat_seconds": 1.0,
        "batch_timeout_seconds": 60.0,
        "batch_max_retries": 1,
        "batch_retry_backoff_seconds": 1.0,
        "stall_warning_seconds": 10.0,
        "stall_kill_seconds": 30.0,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_prepare_batch_run_returns_none_for_dry_run(monkeypatch, tmp_path: Path) -> None:
    deps = SimpleNamespace(
        colorize_fn=lambda text, _tone=None: text,
        run_stamp_fn=lambda: "stamp",
        load_or_prepare_packet_fn=lambda *_a, **_k: (
            {"dimensions": ["design_coherence"], "batches": [{"dimensions": ["design_coherence"]}]},
            tmp_path / "packet.json",
            tmp_path / "prompt.json",
        ),
        selected_batch_indexes_fn=lambda *_a, **_k: [0],
        prepare_run_artifacts_fn=lambda **_k: (
            tmp_path / "run",
            tmp_path / "logs",
            {0: tmp_path / "prompt-1.txt"},
            {0: tmp_path / "out-1.txt"},
            {0: tmp_path / "log-1.txt"},
        ),
        safe_write_text_fn=lambda *_a, **_k: None,
    )
    args = SimpleNamespace(
        runner="codex",
        allow_partial=False,
        path=".",
        dimensions=None,
        run_log_file=None,
    )
    monkeypatch.setattr(phases_mod, "validate_runner", lambda *_a, **_k: None)
    monkeypatch.setattr(phases_mod, "resolve_batch_run_policy", lambda _args: _policy())
    monkeypatch.setattr(phases_mod, "normalize_dimension_list", lambda dims: list(dims))
    monkeypatch.setattr(phases_mod, "scored_dimensions_for_lang", lambda _lang: ["design_coherence"])
    monkeypatch.setattr(phases_mod, "print_preflight_dimension_scope_notice", lambda **_k: None)
    monkeypatch.setattr(phases_mod, "require_batches", lambda packet, **_k: packet["batches"])
    monkeypatch.setattr(phases_mod, "explode_to_single_dimension", lambda batches, **_k: batches)
    monkeypatch.setattr(phases_mod, "resolve_run_log_path", lambda *_a, **_k: tmp_path / "run.log")
    monkeypatch.setattr(phases_mod, "make_run_log_writer", lambda _p: (lambda *_a, **_k: None))
    monkeypatch.setattr(phases_mod, "log_run_start", lambda **_k: None)
    monkeypatch.setattr(phases_mod, "maybe_handle_dry_run", lambda **_k: True)

    result = phases_mod.prepare_batch_run(
        args=args,
        state={},
        lang=SimpleNamespace(name="python"),
        config={},
        deps=deps,
        project_root=tmp_path,
        subagent_runs_dir=tmp_path / "runs",
    )

    assert result is None


def test_execute_batch_run_partial_path_records_failures(monkeypatch) -> None:
    logs: list[str] = []
    printed_failures: list[list[int]] = []
    prepared = {
        "selected_indexes": [0, 1],
        "prompt_files": {0: Path("p0"), 1: Path("p1")},
        "output_files": {0: Path("o0"), 1: Path("o1")},
        "log_files": {0: Path("l0"), 1: Path("l1")},
        "project_root": Path("."),
        "run_parallel": True,
        "max_parallel_batches": 2,
        "heartbeat_seconds": 1.0,
        "report_progress": lambda *_a, **_k: None,
        "record_issue": lambda *_a, **_k: None,
        "batch_status": {},
        "batch_positions": {0: 1, 1: 2},
        "write_run_summary": lambda **_k: None,
        "allow_partial": True,
        "append_run_log": logs.append,
        "immutable_packet_path": Path("packet"),
        "logs_dir": Path("logs"),
        "packet": {"dimensions": ["d"]},
    }
    deps = SimpleNamespace(
        run_codex_batch_fn=lambda *_a, **_k: 0,
        execute_batches_fn=lambda **_k: [1],
        collect_batch_results_fn=lambda **_k: ({}, []),
        colorize_fn=lambda text, _tone=None: text,
        print_failures_and_raise_fn=lambda **_k: (_ for _ in ()).throw(AssertionError("unexpected")),
        print_failures_fn=lambda failures, **_k: printed_failures.append([idx for idx, _ in failures]),
    )
    monkeypatch.setattr(phases_mod, "build_batch_tasks", lambda **_k: ["task"])
    monkeypatch.setattr(
        phases_mod,
        "collect_and_reconcile_results",
        lambda **_k: (
            [{"issues": []}],
            [0],
            [(1, "failed")],
            {1},
        ),
    )

    result = phases_mod.execute_batch_run(prepared=prepared, deps=deps)

    assert result["successful_indexes"] == [0]
    assert result["failure_set"] == {1}
    assert printed_failures == [[1]]
    assert any("run-partial" in line for line in logs)


def test_merge_and_import_batch_run_calls_all_pipeline_steps(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        phases_mod,
        "merge_and_write_results",
        lambda **_k: (Path("merged.json"), []),
    )
    monkeypatch.setattr(
        phases_mod,
        "enforce_import_coverage",
        lambda **_k: calls.append("enforce"),
    )
    monkeypatch.setattr(
        phases_mod,
        "import_and_finalize",
        lambda **_k: calls.append("import"),
    )
    phases_mod.merge_and_import_batch_run(
        prepared={
            "batches": [{"dimensions": ["d"]}],
            "packet": {"dimensions": ["d"]},
            "packet_dimensions": ["d"],
            "scored_dimensions": ["d"],
            "scan_path": ".",
            "runner": "codex",
            "prompt_packet_path": Path("prompt"),
            "stamp": "stamp",
            "run_dir": Path("run"),
            "state": {},
            "lang": SimpleNamespace(name="python"),
            "config": {},
            "allow_partial": False,
            "append_run_log": lambda *_a, **_k: None,
            "args": SimpleNamespace(),
        },
        executed={
            "batch_results": [{"issues": []}],
            "successful_indexes": [0],
            "failure_set": set(),
        },
        state_file=Path("state.json"),
        deps=SimpleNamespace(
            merge_batch_results_fn=lambda *_a, **_k: {"issues": []},
            build_import_provenance_fn=lambda **_k: {},
            safe_write_text_fn=lambda *_a, **_k: None,
            colorize_fn=lambda text, _tone=None: text,
            do_import_fn=lambda *_a, **_k: None,
            run_followup_scan_fn=lambda **_k: 0,
        ),
    )

    assert calls == ["enforce", "import"]
