from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Union

from indexer.events.mch.diagnostics import Span


class Edge(Enum):
    CHILD = "->"
    PARENT = "<-"


# Expression nodes parsed and carried through the frontend. The
# v0 compiler ignores these nodes entirely (no evaluation, no lowering).


class Expr:
    pass


@dataclass(frozen=True)
class IntLit(Expr):
    value: int
    span: Span


@dataclass(frozen=True)
class StrLit(Expr):
    value: str  # decoded string contents (without surrounding quotes)
    span: Span


@dataclass(frozen=True)
class BoolLit(Expr):
    value: bool
    span: Span


@dataclass(frozen=True)
class NullLit(Expr):
    span: Span


@dataclass(frozen=True)
class NameRef(Expr):
    """A bare identifier used as a value: a capture name or a `let` name."""
    name: str
    span: Span


@dataclass(frozen=True)
class FieldRef(Expr):
    """Leading-dot field access `.field`, the current block's data field
    (valid only as a primary inside an inline `where (expr)` clause)."""
    field: str
    span: Span


@dataclass(frozen=True)
class FieldAccess(Expr):
    target: Expr
    field: str
    span: Span


@dataclass(frozen=True)
class Call(Expr):
    callee: Expr
    args: tuple[Expr, ...]
    span: Span


@dataclass(frozen=True)
class LookupExpr(Expr):
    """`lookup KIND(args)` — interface-repository query."""
    kind: str
    args: tuple[Expr, ...]
    span: Span


@dataclass(frozen=True)
class UnaryOp(Expr):
    op: str  # "not" | "-"
    operand: Expr
    span: Span


@dataclass(frozen=True)
class BinaryOp(Expr):
    op: str  # "or" "and" "==" "!=" "<" "<=" ">" ">=" "??"
    left: Expr
    right: Expr
    span: Span


@dataclass(frozen=True)
class Ternary(Expr):
    """Python-style conditional: `then if cond else orelse`."""
    cond: Expr
    then: Expr
    orelse: Expr
    span: Span


@dataclass(frozen=True)
class ListLit(Expr):
    """`[expr, ...]` is a list value whose elements evaluate in order."""
    elements: tuple[Expr, ...]
    span: Span


@dataclass(frozen=True)
class ParseExpr(Expr):
    """`parse <target> as T (| T)*` in expression position.

    Evaluates `target` to a block and parses its body against the message types
    in source order (first success wins). On total failure (no type parses, or a
    null/non-block target) it faults and the build rejects. There is no soft-null
    variant; the result is the parsed message object. Distinct
    from the `parse CAPTURE as T` build STATEMENT (ParseStmt), which records the
    body in a side table for later `.body` access."""
    target: Expr
    msg_types: tuple[str, ...]
    span: Span


@dataclass(frozen=True)
class Comprehension(Expr):
    """`map|any|all(xs as e => body)` lambda comprehension.

    `kind` is "map" (list comprehension), "any" or "all" (quantifiers). `var` is
    the element binder, visible ONLY inside `body` (a new scope frame). No nested
    comprehensions. Distinct from the `map(xs, "field")` builtin call, which the
    parser tells apart by the `as` keyword."""
    kind: str  # "map" | "any" | "all"
    xs: Expr
    var: str
    body: Expr
    span: Span


@dataclass(frozen=True)
class RecordField:
    name: str
    value: Expr
    span: Span


@dataclass(frozen=True)
class RecordLit(Expr):
    """`{key: expr, ...}` is a record value with string keys."""
    fields: tuple[RecordField, ...]
    span: Span


class NodeHead:
    pass


@dataclass(frozen=True)
class OpHead(NodeHead):
    """Concrete opcode: either a registered identifier (str) or literal int."""
    ref: Union[str, int]
    span: Span


@dataclass(frozen=True)
class BTypeHead(NodeHead):
    name: str
    span: Span


@dataclass(frozen=True)
class PredHead(NodeHead):
    name: str
    span: Span


@dataclass(frozen=True)
class AnyHead(NodeHead):
    span: Span


class PatternExpr:
    pass


@dataclass(frozen=True)
class Node(PatternExpr):
    head: NodeHead
    capture: str | None
    where_predicate: str | None
    span: Span
    # Inline `where (expr)` form. Mutually exclusive with the named
    # `where_predicate` form; the compiler ignores it in v0.
    where_expr: Expr | None = None


@dataclass(frozen=True)
class RuleRef(PatternExpr):
    name: str
    span: Span


@dataclass(frozen=True)
class ChildrenBlock(PatternExpr):
    items: tuple[PatternExpr, ...]
    span: Span


@dataclass(frozen=True)
class Maybe(PatternExpr):
    inner: PatternExpr
    span: Span


@dataclass(frozen=True)
class Sequence(PatternExpr):
    """One-or-more atoms connected by edges.

    A bare atom is Sequence(head=atom, tail=()). A linear chain `A -> B -> C` is
    Sequence(head=A, tail=((Edge.CHILD, B), (Edge.CHILD, C))).
    """
    head: PatternExpr
    tail: tuple[tuple[Edge, PatternExpr], ...]
    span: Span


@dataclass(frozen=True)
class Alternative(PatternExpr):
    branches: tuple[PatternExpr, ...]
    span: Span
    exclusive: bool = False


@dataclass(frozen=True)
class OpcodeDecl:
    name: str
    value: int
    span: Span


@dataclass(frozen=True)
class PredicateDecl:
    name: str
    span: Span


@dataclass(frozen=True)
class RuleDecl:
    name: str
    pattern: PatternExpr
    span: Span
    # Recursion strategy for a self-referential rule: "frontier"
    # (default) or "cyclic" (cyclic descent, with self-references at
    # chain ends lower to back-edges in the matcher graph). Parsed from the
    # contextual modifier `rule NAME cyclic = …`; ignored for non-recursive
    # rules.
    strategy: str = "frontier"


# Build statements appear in a matcher body after the pattern.


class BuildStmt:
    pass


@dataclass(frozen=True)
class ParseStmt(BuildStmt):
    """`parse CAPTURE as MessageType (| MessageType)*`.

    Alternatives are tried in source order; the first type whose constructor
    parses the body wins (soft — all failing yields a null body).
    """
    capture: str
    msg_types: tuple[str, ...]
    span: Span

    @property
    def msg_type(self) -> str:
        """First (or only) declared message type. Kept for single-type callers."""
        return self.msg_types[0]


@dataclass(frozen=True)
class LetStmt(BuildStmt):
    """`let NAME = expr`."""
    name: str
    value: Expr
    span: Span


@dataclass(frozen=True)
class RejectStmt(BuildStmt):
    """`reject when expr`."""
    condition: Expr
    span: Span


@dataclass(frozen=True)
class FailedStmt(BuildStmt):
    """`failed when expr`."""
    condition: Expr
    span: Span


@dataclass(frozen=True)
class BrokenStmt(BuildStmt):
    """`broken when expr`."""
    condition: Expr
    span: Span


@dataclass(frozen=True)
class OutField:
    name: str
    value: Expr
    span: Span
    # `name?: expr` — omit this key from the produced data map when the value
    # evaluates to null. Default fields always emit the key.
    optional: bool = False


@dataclass(frozen=True)
class OutStmt(BuildStmt):
    """`out { field: expr <sep> ... }`."""
    fields: tuple[OutField, ...]
    span: Span


@dataclass(frozen=True)
class SwitchBranch:
    """One arm of a `produces switch`. `condition is None` marks the `else`
    arm. `btype` is the produced block type this arm selects; `out` its fields."""
    condition: Expr | None
    btype: str
    out: OutStmt
    span: Span


@dataclass(frozen=True)
class SwitchStmt(BuildStmt):
    """`produces switch { when cond => btype out {...} ... else => btype out {...} }`.
    Branches are tested in order; the first true (or `else`) wins,
    selecting both the produced btype and its `out` fields."""
    branches: tuple[SwitchBranch, ...]
    span: Span


@dataclass(frozen=True)
class MatcherDecl:
    name: str
    produces: tuple[str, ...]
    # None when the matcher is declarative (build statements with an `out`
    # block instead of a registered-builder reference).
    build: str | None
    entry: str | None
    include_excess: bool
    include_bounces: bool
    pattern: PatternExpr
    span: Span
    # Build statements in source order. Empty for builder-based matchers.
    build_stmts: tuple[BuildStmt, ...] = ()
    # `shape IDENT` names a registered shaper invoked by the
    # engine after merge, for post-build tree surgery on the produced block's
    # region. None when absent.
    shape: str | None = None
    # `priority DEC` directive: registration-order weight, lower
    # runs earlier; ties broken by (file name, source position). Default 100.
    priority: int = 100

    @property
    def produces_primary(self) -> str:
        """First declared `produces` name. Convenience accessor for code that
        needs a single type (e.g. `CompiledMatcher.produces_cls_name`); the
        union is documentation plus registry validation, and
        selecting among the declared types at build time is the builder's job."""
        return self.produces[0]


@dataclass(frozen=True)
class File:
    path: str
    opcodes:    tuple[OpcodeDecl, ...]
    predicates: tuple[PredicateDecl, ...]
    rules:      tuple[RuleDecl, ...]
    matchers:   tuple[MatcherDecl, ...]
