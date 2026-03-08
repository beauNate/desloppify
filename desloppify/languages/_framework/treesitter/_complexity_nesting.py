"""Nesting-depth and callback-depth complexity metrics for tree-sitter languages.

Also hosts shared helpers (ComputeFn, _ensure_parser) used by sibling modules.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable

from . import PARSE_INIT_ERRORS
from ._cache import _PARSE_CACHE
from ._extractors import _get_parser

if TYPE_CHECKING:
    from desloppify.languages._framework.treesitter import TreeSitterLangSpec

logger = logging.getLogger(__name__)

ComputeFn = Callable[[str, list[str]], tuple[int, str] | None]
"""Signature for complexity signal compute functions.

Each factory returns a closure with this effective call shape:
``(content, lines, *, _filepath="") -> (count, label) | None``.
"""


def _ensure_parser(
    cache: dict[str, Any],
    spec: TreeSitterLangSpec,
    *,
    with_query: bool = False,
) -> bool:
    """Lazily initialise parser (and optionally function query) into *cache*."""
    if "parser" in cache:
        return True
    try:
        parser, lang = _get_parser(spec.grammar)
        cache["parser"] = parser
        cache["language"] = lang
        if with_query:
            from ._extractors import _make_query

            cache["query"] = _make_query(lang, spec.function_query)
    except PARSE_INIT_ERRORS as exc:
        logger.debug("tree-sitter init failed: %s", exc)
        return False
    return True


# ---------------------------------------------------------------------------
# Nesting depth
# ---------------------------------------------------------------------------

_NESTING_NODE_TYPES = frozenset(
    {
        "if_statement",
        "if_expression",
        "if_let_expression",
        "else_clause",
        "elif_clause",
        "for_statement",
        "for_expression",
        "for_in_statement",
        "while_statement",
        "while_expression",
        "do_statement",
        "loop_expression",
        "try_statement",
        "try_expression",
        "catch_clause",
        "rescue",
        "except_clause",
        "switch_statement",
        "switch_expression",
        "match_expression",
        "case_clause",
        "match_arm",
        "with_statement",
        "with_clause",
        "lambda_expression",
        "closure_expression",
    }
)


def compute_nesting_depth_ts(
    filepath: str, spec: TreeSitterLangSpec, parser, language
) -> int | None:
    """Compute max control-flow nesting depth via iterative AST walk."""
    del language
    cached = _PARSE_CACHE.get_or_parse(filepath, parser, spec.grammar)
    if cached is None:
        return None
    _source, tree = cached

    max_depth = 0
    stack: list[tuple[object, int]] = [(tree.root_node, 0)]
    while stack:
        node, depth = stack.pop()
        if node.type in _NESTING_NODE_TYPES:
            depth += 1
            if depth > max_depth:
                max_depth = depth
        for index in range(node.child_count - 1, -1, -1):
            stack.append((node.children[index], depth))
    return max_depth


def make_nesting_depth_compute(spec: TreeSitterLangSpec) -> ComputeFn:
    """Build a complexity compute callback for max nesting depth."""
    _cached_parser: dict[str, Any] = {}

    def compute(content: str, lines: list[str], *, _filepath: str = "") -> tuple[int, str] | None:
        del content, lines
        if not _filepath:
            return None
        if not _ensure_parser(_cached_parser, spec):
            return None

        depth = compute_nesting_depth_ts(
            _filepath,
            spec,
            _cached_parser["parser"],
            _cached_parser["language"],
        )
        if depth is None or depth <= 0:
            return None
        return depth, f"nesting depth {depth}"

    return compute


# ---------------------------------------------------------------------------
# Callback / closure nesting depth
# ---------------------------------------------------------------------------

_CLOSURE_NODE_TYPES = frozenset(
    {
        "arrow_function",
        "function_expression",
        "function",
        "lambda_expression",
        "closure_expression",
        "lambda",
        "anonymous_function",
        "block_argument",
        "func_literal",
    }
)


def make_callback_depth_compute(spec: TreeSitterLangSpec) -> ComputeFn:
    """Build a complexity compute callback for callback/closure nesting depth."""
    _cached_parser: dict[str, Any] = {}

    def compute(content: str, lines: list[str], *, _filepath: str = "") -> tuple[int, str] | None:
        del content, lines
        if not _filepath:
            return None
        if not _ensure_parser(_cached_parser, spec):
            return None

        parser = _cached_parser["parser"]
        cached = _PARSE_CACHE.get_or_parse(_filepath, parser, spec.grammar)
        if cached is None:
            return None
        _source, tree = cached

        max_depth = 0
        stack: list[tuple[object, int]] = [(tree.root_node, 0)]
        while stack:
            node, depth = stack.pop()
            if node.type in _CLOSURE_NODE_TYPES:
                depth += 1
                if depth > max_depth:
                    max_depth = depth
            for index in range(node.child_count - 1, -1, -1):
                stack.append((node.children[index], depth))

        if max_depth <= 1:
            return None
        return max_depth, f"callback depth {max_depth}"

    return compute
