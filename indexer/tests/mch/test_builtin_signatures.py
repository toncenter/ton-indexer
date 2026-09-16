"""Keep compiler and evaluator builtin metadata on one authoritative table."""

from indexer.events.mch import compiler
from indexer.events.mch.builtin_signatures import BUILTIN_SIGNATURES
from indexer.events.mch.expr_eval import BUILTINS


def test_compiler_and_evaluator_builtin_signatures_agree():
    assert compiler.BUILTIN_SIGNATURES is BUILTIN_SIGNATURES
    assert {name: arity for name, (_, arity) in BUILTINS.items()} == BUILTIN_SIGNATURES
