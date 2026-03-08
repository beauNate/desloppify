"""Success-path output validation helpers for review batch attempts."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from ._runner_process_types import CodexBatchRunnerDeps, _ExecutionResult


DefValidateFn = Callable[[Path], bool]


def handle_successful_attempt_core(
    *,
    result: _ExecutionResult,
    output_file: Path,
    log_file: Path,
    deps: CodexBatchRunnerDeps,
    log_sections: list[str],
    default_validate_fn: DefValidateFn,
    monotonic_fn: Callable[[], float],
) -> int | None:
    """Validate successful run output and handle delayed/fallback writes."""
    if result.code != 0:
        return None

    validate = deps.validate_output_fn or default_validate_fn
    valid = validate(output_file)
    grace_wait_used = False

    if not valid:
        grace_raw = getattr(deps, "output_validation_grace_seconds", 0.0)
        poll_raw = getattr(deps, "output_validation_poll_seconds", 0.1)
        try:
            grace_seconds = max(0.0, float(grace_raw))
        except (TypeError, ValueError):
            grace_seconds = 0.0
        try:
            poll_seconds = max(0.01, float(poll_raw))
        except (TypeError, ValueError):
            poll_seconds = 0.1

        if grace_seconds > 0:
            grace_wait_used = True
            deadline = monotonic_fn() + grace_seconds
            while monotonic_fn() < deadline:
                remaining = deadline - monotonic_fn()
                sleep_for = min(poll_seconds, max(0.0, remaining))
                if sleep_for <= 0:
                    break
                try:
                    deps.sleep_fn(sleep_for)
                except (OSError, RuntimeError, ValueError, TypeError):
                    break
                if validate(output_file):
                    valid = True
                    break

    if not valid and deps.validate_output_fn is not None:
        # For custom validators (triage/text modes), recover from stdout/stderr
        # when the runner exited successfully but the output file write lagged.
        fallback_text = (result.stdout_text or "").strip() or (result.stderr_text or "").strip()
        if fallback_text:
            try:
                deps.safe_write_text_fn(output_file, fallback_text)
            except (OSError, RuntimeError, ValueError, TypeError):
                pass
            else:
                if validate(output_file):
                    valid = True
                    log_sections.append(
                        "Runner output recovered from stdout/stderr fallback text."
                    )

    if not valid:
        log_sections.append(
            "Runner exited 0 but output file is missing or invalid; "
            "treating as execution failure."
        )
        deps.safe_write_text_fn(log_file, "\n\n".join(log_sections))
        return 1

    if grace_wait_used:
        log_sections.append(
            "Runner output validation passed after grace wait for delayed file write."
        )

    deps.safe_write_text_fn(log_file, "\n\n".join(log_sections))
    return 0


__all__ = ["handle_successful_attempt_core"]
