from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Severity(Enum):
    ERROR = "error"
    WARNING = "warning"


# Diagnostic codes are plain strings passed at each call site (R0xx resolver,
# P0xx parser, L0xx lexer, C0xx/B0xx compiler). Build-language codes are named
# here so there is one discoverable record of them.
R010_UNRESOLVED_REFERENCE = "R010_UNRESOLVED_REFERENCE"
"""A bare identifier in a build-statement expression does not resolve to a
pattern capture or a previously declared `let` name."""

R011_PARSE_TARGET_NOT_CAPTURE = "R011_PARSE_TARGET_NOT_CAPTURE"
"""The target of a `parse X as T` statement is not a pattern capture."""

R012_UNKNOWN_MESSAGE_TYPE = "R012_UNKNOWN_MESSAGE_TYPE"
"""A message type named in `parse … as T` is not present in the host's
message-type registry."""

R013_DUP_LET = "R013_DUP_LET"
"""A `let` name collides with a pattern capture or an earlier `let`."""

R014_DUP_OUT_FIELD = "R014_DUP_OUT_FIELD"
"""The same field name appears more than once in an `out` block."""

R015_BUILD_AMBIGUOUS = "R015_BUILD_AMBIGUOUS"
"""A matcher declares both a `build` directive (registered builder) and build
statements. Use exactly one form."""

R016_BUILD_NO_OUT = "R016_BUILD_NO_OUT"
"""A declarative matcher (no `build` directive) has build statements but no
`out` block, so there is nothing to produce."""

R017_MULTIPLE_OUT = "R017_MULTIPLE_OUT"
"""More than one `out` block in a matcher body; exactly one is allowed."""

R022_UNNORMALIZED_PRODUCES_CLASS = "R022_UNNORMALIZED_PRODUCES_CLASS"
"""A PascalCase (class-name) `produces` token has no class->btype entry in
mch/btypes.py CLASS_TO_BTYPE, so it cannot be normalized to the block's real
btype. Left unresolved it would silently emit the class name itself as the
produced btype, which the serializer and
legacy blocks never key on. Add the entry, or use the bare btype string."""

R023_PARSE_TARGET_COMPOSED_BLOCK = "R023_PARSE_TARGET_COMPOSED_BLOCK"
"""A `parse X as T` statement targets a capture bound by a `btype` node: a
composed block built by an earlier matcher. A produced block's `get_body()`
reads `event_nodes[0]`, whose identity after the
prior matcher's `merge_blocks` is not the anchor protocol message, so the
re-parse does not round-trip and `.body` silently null-propagates. Read the
prior matcher's fields via `X.data.<field>` instead of re-parsing."""

R024_NESTED_COMPREHENSION = "R024_NESTED_COMPREHENSION"
"""A `map|any|all(xs as e => body)` comprehension appears inside another
comprehension (either the outer `xs` or `body`). Nested comprehensions are
forbidden: exactly one bound element var per comprehension, no
comprehension in the iterated list or the body."""

P011_DUPLICATE_DIRECTIVE = "P011_DUPLICATE_DIRECTIVE"
"""A matcher declares the same at-most-once directive more than once
(currently `shape`)."""

P012_STITCH_REMOVED = "P012_STITCH_REMOVED"
"""A `stitch` declaration was encountered. Cross-trace joining is unsupported;
host post-process code merges paired results. The words `stitch partial final
key ttl merge` remain reserved so this error is targeted."""

C011_UNKNOWN_SHAPER = "C011_UNKNOWN_SHAPER"
"""The `shape` directive names a shaper that is not present in the host's
shaper registry."""

C012_CYCLIC_RULE_SHAPE = "C012_CYCLIC_RULE_SHAPE"
"""A `rule NAME cyclic = …` body violates the cyclic-descent shape rules:
captures inside the body, a self-reference that is
not the last atom of its chain, a `maybe`-wrapped self-reference, or a
reference to another recursive rule."""


@dataclass(frozen=True)
class Span:
    path: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int
    start_off: int
    end_off: int


@dataclass(frozen=True)
class Diagnostic:
    severity: Severity
    code: str
    message: str
    span: Span
    suggestion: str | None = None


@dataclass
class DiagnosticBag:
    items: list[Diagnostic] = field(default_factory=list)

    def error(self, code: str, message: str, span: Span, suggestion: str | None = None) -> None:
        self.items.append(Diagnostic(Severity.ERROR, code, message, span, suggestion))

    def warning(self, code: str, message: str, span: Span, suggestion: str | None = None) -> None:
        self.items.append(Diagnostic(Severity.WARNING, code, message, span, suggestion))

    @property
    def has_errors(self) -> bool:
        return any(d.severity is Severity.ERROR for d in self.items)


class CompileError(Exception):
    def __init__(self, bag: DiagnosticBag) -> None:
        self.bag = bag
        super().__init__(f"compilation failed with {len(bag.items)} diagnostics")


def format_diagnostic(d: Diagnostic, source_text: str) -> str:
    lines = source_text.splitlines()
    idx = d.span.start_line - 1
    source_line = lines[idx] if 0 <= idx < len(lines) else ""
    span_len = max(1, d.span.end_col - d.span.start_col)
    caret_indent = " " * (d.span.start_col - 1)
    caret = caret_indent + "^" * span_len
    header = (
        f"{d.span.path}:{d.span.start_line}:{d.span.start_col}: "
        f"{d.severity.value}: {d.message} ({d.code})"
    )
    parts = [header, source_line, caret]
    if d.suggestion:
        parts.append(d.suggestion)
    return "\n".join(parts)
