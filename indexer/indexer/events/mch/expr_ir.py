"""ast_ expression -> IR expression-tree encoding (k-tagged dicts).

This module is separate from ir_emit.py so the compiler can use it too: ir_emit
imports the compiler, so the compiler cannot import ir_emit back. The compiled
path converts a node's inline `where (expr)` to this encoding at compile time
and evaluates it with the shared sync evaluator (mch/expr_eval.py). These are the exact
artifact bytes the IR engine evaluates, so the two paths cannot drift.

Frontend-only module (it walks ast_ nodes); mch_ir must not import it.
"""
from __future__ import annotations

from indexer.events.mch import ast_

IR_VERSION = "1.1"


def emit_expr(e: ast_.Expr) -> dict:
    if isinstance(e, ast_.IntLit):
        return {"k": "int", "v": e.value}
    if isinstance(e, ast_.StrLit):
        return {"k": "str", "v": e.value}
    if isinstance(e, ast_.BoolLit):
        return {"k": "bool", "v": e.value}
    if isinstance(e, ast_.NullLit):
        return {"k": "null"}
    if isinstance(e, ast_.NameRef):
        return {"k": "name", "id": e.name}
    if isinstance(e, ast_.FieldRef):
        return {"k": "dotfield", "name": e.field}
    if isinstance(e, ast_.FieldAccess):
        return {"k": "attr", "of": emit_expr(e.target), "name": e.field}
    if isinstance(e, ast_.Call):
        # eval.py's evaluator only ever calls a bare builtin name ("only
        # builtin names are callable"); a non-name callee can be *parsed*
        # (postfix chaining is generic in the grammar) but can never evaluate,
        # so it has no faithful IR encoding here.
        if not isinstance(e.callee, ast_.NameRef):
            raise ValueError(
                f"IR emission: Call callee must be a bare name (got "
                f"{type(e.callee).__name__}); non-name callees never evaluate "
                f"(see eval.py Evaluator._eval_call) and are not representable "
                f"in IR {IR_VERSION}"
            )
        return {"k": "call", "fn": e.callee.name, "args": [emit_expr(a) for a in e.args]}
    if isinstance(e, ast_.LookupExpr):
        return {"k": "lookup", "name": e.kind, "args": [emit_expr(a) for a in e.args]}
    if isinstance(e, ast_.UnaryOp):
        return {"k": "unary", "op": e.op, "x": emit_expr(e.operand)}
    if isinstance(e, ast_.BinaryOp):
        return {"k": "bin", "op": e.op, "l": emit_expr(e.left), "r": emit_expr(e.right)}
    if isinstance(e, ast_.Ternary):
        return {
            "k": "ternary",
            "cond": emit_expr(e.cond),
            "then": emit_expr(e.then),
            "else": emit_expr(e.orelse),
        }
    if isinstance(e, ast_.ListLit):
        return {"k": "list", "items": [emit_expr(x) for x in e.elements]}
    if isinstance(e, ast_.RecordLit):
        return {"k": "record", "fields": [
            {"name": f.name, "expr": emit_expr(f.value)} for f in e.fields
        ]}
    if isinstance(e, ast_.ParseExpr):
        return {
            "k": "parse",
            "x": emit_expr(e.target),
            "types": list(e.msg_types),
            "nullable": e.nullable,
        }
    if isinstance(e, ast_.Comprehension):
        # map -> `mapc`; any/all -> `quant` carrying the op.
        if e.kind == "map":
            return {"k": "mapc", "xs": emit_expr(e.xs), "var": e.var,
                    "body": emit_expr(e.body)}
        return {"k": "quant", "op": e.kind, "xs": emit_expr(e.xs), "var": e.var,
                "body": emit_expr(e.body)}
    raise ValueError(f"unsupported expression node {type(e).__name__} in IR emission")
