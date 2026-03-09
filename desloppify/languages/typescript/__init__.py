"""TypeScript/React language configuration for desloppify."""

from __future__ import annotations

from desloppify.base.discovery.source import find_ts_files
from desloppify.base.discovery.paths import get_area
from desloppify.engine.hook_registry import register_lang_hooks
from desloppify.engine.policy.zones import COMMON_ZONE_RULES, Zone, ZoneRule
from desloppify.languages import register_lang
from desloppify.languages._framework.base.phase_builders import (
    detector_phase_security,
    detector_phase_signature,
    detector_phase_test_coverage,
    shared_subjective_duplicates_tail,
)
from desloppify.languages._framework.base.types import (
    BoundaryRule,
    DetectorPhase,
    LangConfig,
    LangSecurityResult,
)
from desloppify.languages.typescript import commands as ts_commands_mod
from desloppify.languages.typescript import test_coverage as ts_test_coverage_hooks
from desloppify.languages.typescript._detectors import (
    ts_extract_functions,
    ts_treesitter_phases,
)
from desloppify.languages.typescript._fixers import get_ts_fixers
from desloppify.languages.typescript.review import (
    HOLISTIC_REVIEW_DIMENSIONS as TS_HOLISTIC_REVIEW_DIMENSIONS,
    LOW_VALUE_PATTERN as TS_LOW_VALUE_PATTERN,
    MIGRATION_MIXED_EXTENSIONS as TS_MIGRATION_MIXED_EXTENSIONS,
    MIGRATION_PATTERN_PAIRS as TS_MIGRATION_PATTERN_PAIRS,
    REVIEW_GUIDANCE as TS_REVIEW_GUIDANCE,
    api_surface as ts_review_api_surface,
    module_patterns as ts_review_module_patterns,
)
from desloppify.languages.typescript._zones import TS_ZONE_RULES
from desloppify.languages.typescript.detectors import deps as deps_detector_mod
from desloppify.languages.typescript.detectors.security import _detect_ts_security_result
from desloppify.languages.typescript.phases import (
    TS_COMPLEXITY_SIGNALS,
    TS_GOD_RULES,
    TS_SKIP_DIRS,
    TS_SKIP_NAMES,
    phase_coupling,
    phase_deprecated,
    phase_exports,
    phase_logs,
    phase_smells,
    phase_structural,
    phase_unused,
)

register_lang_hooks("typescript", test_coverage=ts_test_coverage_hooks)


@register_lang("typescript")
class TypeScriptConfig(LangConfig):
    def detect_lang_security_detailed(self, files, zone_map):
        result = _detect_ts_security_result(files, zone_map)
        return LangSecurityResult(
            entries=result.entries,
            files_scanned=result.population_size,
        )

    def __init__(self):
        super().__init__(
            name="typescript",
            extensions=[".ts", ".tsx"],
            exclusions=["node_modules", ".d.ts"],
            default_src="src",
            build_dep_graph=deps_detector_mod.build_dep_graph,
            entry_patterns=[
                "/pages/",
                "/main.tsx",
                "/main.ts",
                "/App.tsx",
                "vite.config",
                "tailwind.config",
                "postcss.config",
                ".d.ts",
                "/settings.ts",
                "/__tests__/",
                ".test.",
                ".spec.",
                ".stories.",
            ],
            barrel_names={"index.ts", "index.tsx"},
            phases=[
                DetectorPhase("Logs", phase_logs),
                DetectorPhase("Unused (tsc)", phase_unused),
                DetectorPhase("Dead exports", phase_exports),
                DetectorPhase("Deprecated", phase_deprecated),
                DetectorPhase("Structural analysis", phase_structural),
                DetectorPhase("Coupling + single-use + patterns + naming", phase_coupling),
                *ts_treesitter_phases(),
                detector_phase_signature(),
                detector_phase_test_coverage(),
                DetectorPhase("Code smells", phase_smells),
                detector_phase_security(),
                *shared_subjective_duplicates_tail(),
            ],
            fixers=get_ts_fixers(),
            get_area=get_area,
            detect_commands=ts_commands_mod.get_detect_commands(),
            boundaries=[
                BoundaryRule("shared/", "tools/", "shared→tools"),
            ],
            typecheck_cmd="npx tsc --noEmit",
            file_finder=find_ts_files,
            large_threshold=500,
            complexity_threshold=15,
            default_scan_profile="full",
            detect_markers=["package.json"],
            external_test_dirs=["tests", "test", "__tests__"],
            test_file_extensions=[".ts", ".tsx"],
            review_module_patterns_fn=ts_review_module_patterns,
            review_api_surface_fn=ts_review_api_surface,
            review_guidance=TS_REVIEW_GUIDANCE,
            review_low_value_pattern=TS_LOW_VALUE_PATTERN,
            holistic_review_dimensions=TS_HOLISTIC_REVIEW_DIMENSIONS,
            migration_pattern_pairs=TS_MIGRATION_PATTERN_PAIRS,
            migration_mixed_extensions=TS_MIGRATION_MIXED_EXTENSIONS,
            extract_functions=ts_extract_functions,
            zone_rules=TS_ZONE_RULES,
        )


__all__ = [
    "COMMON_ZONE_RULES",
    "TS_COMPLEXITY_SIGNALS",
    "TS_GOD_RULES",
    "TS_HOLISTIC_REVIEW_DIMENSIONS",
    "TS_LOW_VALUE_PATTERN",
    "TS_MIGRATION_MIXED_EXTENSIONS",
    "TS_MIGRATION_PATTERN_PAIRS",
    "TS_REVIEW_GUIDANCE",
    "TS_SKIP_DIRS",
    "TS_SKIP_NAMES",
    "TS_ZONE_RULES",
    "TypeScriptConfig",
    "Zone",
    "ZoneRule",
]
