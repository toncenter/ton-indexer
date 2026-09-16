from __future__ import annotations

from typing import Any, Protocol


class RecursionStrategy(Protocol):
    """Lower a recursive rule reference to an IR node."""

    def lower_recursive_rule(
        self,
        rule_name: str,
        ctx: Any,                  # CompileCtx, typed at the call site
    ) -> Any:                      # compiler.LNode, typed at the call site
        ...


class RecursiveMatcherStrategy:
    """Lower a recursive rule reference to a frontier `recursive` IR node.

    Implementation lives in compiler.py (it needs the compiler's lowering
    helpers). This class is a placeholder that the compiler instantiates; the
    actual behavior is invoked through compiler.CompileCtx.
    """
    pass
