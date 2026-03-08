"""Tree-sitter specs for compiled/backend language families."""

from __future__ import annotations

from desloppify.languages._framework.treesitter import TreeSitterLangSpec

from ._import_resolvers_backend import (
    resolve_csharp_import,
    resolve_cxx_include,
    resolve_dart_import,
    resolve_go_import,
    resolve_java_import,
    resolve_kotlin_import,
    resolve_rust_import,
    resolve_scala_import,
)
from ._import_resolvers_scripts import resolve_php_import

GO_SPEC = TreeSitterLangSpec(
    grammar="go",
    function_query="""
        (function_declaration
            name: (identifier) @name
            body: (block) @body) @func
        (method_declaration
            name: (field_identifier) @name
            body: (block) @body) @func
    """,
    comment_node_types=frozenset({"comment"}),
    import_query="""
        (import_spec
            path: (interpreted_string_literal) @path) @import
    """,
    resolve_import=resolve_go_import,
    class_query="""
        (type_declaration
            (type_spec
                name: (type_identifier) @name
                type: (struct_type) @body)) @class
    """,
    log_patterns=(
        r"^\s*(?:fmt\.Print|fmt\.Fprint|log\.)",
    ),
)

RUST_SPEC = TreeSitterLangSpec(
    grammar="rust",
    function_query="""
        (function_item
            name: (identifier) @name
            body: (block) @body) @func
    """,
    comment_node_types=frozenset({"line_comment", "block_comment"}),
    import_query="""
        (use_declaration
            argument: (_) @path) @import
    """,
    resolve_import=resolve_rust_import,
    class_query="""
        (struct_item
            name: (type_identifier) @name
            body: (field_declaration_list) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:println!|eprintln!|dbg!|tracing::)",
    ),
)

JAVA_SPEC = TreeSitterLangSpec(
    grammar="java",
    function_query="""
        (method_declaration
            name: (identifier) @name
            body: (block) @body) @func
        (constructor_declaration
            name: (identifier) @name
            body: (constructor_body) @body) @func
    """,
    comment_node_types=frozenset({"line_comment", "block_comment"}),
    import_query="""
        (import_declaration
            (scoped_identifier) @path) @import
    """,
    resolve_import=resolve_java_import,
    class_query="""
        (class_declaration
            name: (identifier) @name
            body: (class_body) @body) @class
        (interface_declaration
            name: (identifier) @name
            body: (interface_body) @body) @class
        (enum_declaration
            name: (identifier) @name
            body: (enum_body) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:System\.out\.|System\.err\.|Logger\.|log\.)",
    ),
)

KOTLIN_SPEC = TreeSitterLangSpec(
    grammar="kotlin",
    function_query="""
        (function_declaration
            (simple_identifier) @name
            (function_body) @body) @func
    """,
    comment_node_types=frozenset({"line_comment", "multiline_comment"}),
    import_query="""
        (import_header
            (identifier) @path) @import
    """,
    resolve_import=resolve_kotlin_import,
    class_query="""
        (class_declaration
            (type_identifier) @name
            (class_body) @body) @class
        (object_declaration
            (type_identifier) @name
            (class_body) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:println\(|print\(|Logger\.|log\.)",
    ),
)

CSHARP_SPEC = TreeSitterLangSpec(
    grammar="csharp",
    function_query="""
        (method_declaration
            name: (identifier) @name
            body: (block) @body) @func
        (constructor_declaration
            name: (identifier) @name
            body: (block) @body) @func
    """,
    comment_node_types=frozenset({"comment"}),
    import_query="""
        (using_directive
            (identifier) @path) @import
    """,
    resolve_import=resolve_csharp_import,
    class_query="""
        (class_declaration
            name: (identifier) @name
            body: (declaration_list) @body) @class
        (interface_declaration
            name: (identifier) @name
            body: (declaration_list) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:Console\.Write|Debug\.Log|Logger\.)",
    ),
)

SWIFT_SPEC = TreeSitterLangSpec(
    grammar="swift",
    function_query="""
        (function_declaration
            name: (simple_identifier) @name
            body: (function_body) @body) @func
    """,
    comment_node_types=frozenset({"comment", "multiline_comment"}),
    class_query="""
        (class_declaration
            name: (type_identifier) @name
            body: (class_body) @body) @class
        (protocol_declaration
            name: (type_identifier) @name
            body: (protocol_body) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:print\(|NSLog|os_log|Logger\.)",
    ),
)

PHP_SPEC = TreeSitterLangSpec(
    grammar="php",
    function_query="""
        (function_definition
            name: (name) @name
            body: (compound_statement) @body) @func
        (method_declaration
            name: (name) @name
            body: (compound_statement) @body) @func
    """,
    comment_node_types=frozenset({"comment"}),
    import_query="""
        (namespace_use_declaration
            (namespace_use_clause
                (qualified_name) @path)) @import
    """,
    resolve_import=resolve_php_import,
    class_query="""
        (class_declaration
            name: (name) @name
            body: (declaration_list) @body) @class
        (interface_declaration
            name: (name) @name
            body: (declaration_list) @body) @class
        (trait_declaration
            name: (name) @name
            body: (declaration_list) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:echo |print |var_dump|error_log|Log::)",
    ),
)

DART_SPEC = TreeSitterLangSpec(
    grammar="dart",
    function_query="""
        (function_signature
            name: (identifier) @name) @func
        (method_signature
            (function_signature
                name: (identifier) @name)) @func
    """,
    comment_node_types=frozenset({"comment", "documentation_comment"}),
    import_query="""
        (import_or_export
            (library_import
                (import_specification
                    (configurable_uri
                        (uri
                            (string_literal) @path))))) @import
    """,
    resolve_import=resolve_dart_import,
    class_query="""
        (class_definition
            name: (identifier) @name
            body: (class_body) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:print\(|debugPrint|log\.)",
    ),
)

C_SPEC = TreeSitterLangSpec(
    grammar="c",
    function_query="""
        (function_definition
            declarator: (function_declarator
                declarator: (identifier) @name)
            body: (compound_statement) @body) @func
    """,
    comment_node_types=frozenset({"comment"}),
    import_query="""
        (preproc_include
            path: (string_literal) @path) @import
    """,
    resolve_import=resolve_cxx_include,
    class_query="""
        (struct_specifier
            name: (type_identifier) @name
            body: (field_declaration_list) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:printf\(|fprintf\(|perror\()",
    ),
)

CPP_SPEC = TreeSitterLangSpec(
    grammar="cpp",
    function_query="""
        (function_definition
            declarator: (function_declarator
                declarator: (identifier) @name)
            body: (compound_statement) @body) @func
        (function_definition
            declarator: (function_declarator
                declarator: (qualified_identifier) @name)
            body: (compound_statement) @body) @func
    """,
    comment_node_types=frozenset({"comment"}),
    import_query="""
        (preproc_include
            path: (string_literal) @path) @import
    """,
    resolve_import=resolve_cxx_include,
    class_query="""
        (class_specifier
            name: (type_identifier) @name
            body: (field_declaration_list) @body) @class
        (struct_specifier
            name: (type_identifier) @name
            body: (field_declaration_list) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:std::cout|std::cerr|printf\(|fprintf\()",
    ),
)

SCALA_SPEC = TreeSitterLangSpec(
    grammar="scala",
    function_query="""
        (function_definition
            name: (identifier) @name
            body: (_) @body) @func
    """,
    comment_node_types=frozenset({"comment", "block_comment"}),
    import_query="""
        (import_declaration
            path: (identifier) @path) @import
    """,
    resolve_import=resolve_scala_import,
    class_query="""
        (class_definition
            name: (identifier) @name
            body: (template_body) @body) @class
        (object_definition
            name: (identifier) @name
            body: (template_body) @body) @class
        (trait_definition
            name: (identifier) @name
            body: (template_body) @body) @class
    """,
    log_patterns=(
        r"^\s*(?:println\(|print\(|Logger\.|log\.)",
    ),
)
