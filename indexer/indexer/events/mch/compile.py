from __future__ import annotations

from pathlib import Path

from indexer.events.mch.compiler import CompiledMatcher, compile_
from indexer.events.mch.lexer import tokenize
from indexer.events.mch.parser import parse
from indexer.events.mch.recursion import RecursionStrategy
from indexer.events.mch.registry import Registries
from indexer.events.mch.resolver import resolve


def compile_text(
    text: str,
    path: str,
    registries: Registries,
    *,
    recursion: RecursionStrategy | None = None,
) -> list[CompiledMatcher]:
    tokens = list(tokenize(text, path))
    file_ast = parse(tokens, path)
    resolved = resolve(file_ast, registries)
    return compile_(resolved, registries, recursion=recursion)


def compile_file(
    path: Path,
    registries: Registries,
    *,
    recursion: RecursionStrategy | None = None,
) -> list[CompiledMatcher]:
    text = Path(path).read_text(encoding="utf-8")
    return compile_text(text, str(path), registries, recursion=recursion)


def compile_dir(
    dir_path: Path,
    registries: Registries,
    *,
    recursion: RecursionStrategy | None = None,
) -> list[CompiledMatcher]:
    out: list[CompiledMatcher] = []
    for p in sorted(Path(dir_path).glob("*.mch")):
        out.extend(compile_file(p, registries, recursion=recursion))
    return out
