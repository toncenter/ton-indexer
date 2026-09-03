"""`.mch` matcher DSL — Python compiler.

Public surface:
    compile_text(text, path, registries, ...) -> list[CompiledMatcher]
    compile_file(path, registries, ...)       -> list[CompiledMatcher]
    Registries                                 -> host-language registry container
"""
from __future__ import annotations

from indexer.events.mch.compile import compile_dir, compile_file, compile_text
from indexer.events.mch.compiler import CompiledMatcher
from indexer.events.mch.registry import Registries

__all__ = [
    "CompiledMatcher",
    "Registries",
    "compile_dir",
    "compile_file",
    "compile_text",
]
