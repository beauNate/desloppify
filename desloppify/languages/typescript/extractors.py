"""TypeScript/React extraction facade.

Re-exports component extraction helpers and function extraction utilities.
"""

from __future__ import annotations

from desloppify.languages.typescript.extractors_components import (
    detect_passthrough_components,
    extract_props,
    extract_ts_components,
    tsx_passthrough_pattern,
)
from desloppify.languages.typescript.extractors_functions import (
    _extract_ts_params,
    _parse_param_names,
    extract_ts_functions,
    normalize_ts_body,
)

__all__ = [
    "_extract_ts_params",
    "_parse_param_names",
    "detect_passthrough_components",
    "extract_props",
    "extract_ts_components",
    "extract_ts_functions",
    "normalize_ts_body",
    "tsx_passthrough_pattern",
]
