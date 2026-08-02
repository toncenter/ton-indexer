from __future__ import annotations

import re
from hashlib import blake2s
from pathlib import Path

from indexer.events.mch import ast_
from indexer.events.mch.resolver import CaptureInfo, ResolvedFile


_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def emit(resolved: ResolvedFile, out_dir: Path) -> list[Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for rm in resolved.matchers:
        # Defense in depth: the lexer's IDENT pattern already constrains this, but
        # validate explicitly so codegen doesn't become a path-traversal vector if
        # the lexer's regex is ever relaxed.
        if not _SAFE_IDENT.match(rm.decl.name):
            raise ValueError(
                f"refusing to emit codegen for unsafe matcher name {rm.decl.name!r}"
            )
        path = out_dir / f"{rm.decl.name}_match.py"
        content = _render_match_module(rm.decl.name, rm.captures, source_path=resolved.raw.path)
        # Idempotent write: skip if content (excluding the hash header) is unchanged.
        if path.exists() and path.read_text(encoding="utf-8") == content:
            written.append(path)
            continue
        path.write_text(content, encoding="utf-8")
        written.append(path)
    return written


def _render_match_module(name: str, captures: dict[str, CaptureInfo], source_path: str) -> str:
    cls_name = _to_pascal_case(name) + "Match"

    field_defs: list[str] = []
    imports: set[str] = set()
    for cap_name, info in captures.items():
        type_str = _capture_type_str(info, imports)
        field_defs.append(f"    {cap_name}: {type_str}")

    body = "\n".join(field_defs) or "    pass"
    header_hash = blake2s(f"{name}\n{body}".encode("utf-8"), digest_size=6).hexdigest()

    import_lines = sorted(imports) if imports else []
    import_block = "\n".join(import_lines)

    parts = [
        f"# Generated from {source_path}  (do not edit)",
        f"# hash: {header_hash}",
        "from __future__ import annotations",
        "",
        "from dataclasses import dataclass",
    ]
    if import_block:
        parts.append(import_block)
    parts.extend([
        "",
        "@dataclass",
        f"class {cls_name}:",
        body,
        "",
    ])
    return "\n".join(parts)


def _capture_type_str(info: CaptureInfo, imports: set[str]) -> str:
    head = info.head
    if isinstance(head, ast_.OpHead):
        imports.add("from indexer.events.blocks.basic_blocks import CallContractBlock")
        base = "CallContractBlock"
    elif isinstance(head, ast_.BTypeHead):
        # Block type class can't be auto-imported without registry knowledge; fall back to Block.
        imports.add("from indexer.events.blocks.core import Block")
        base = "Block"
    elif isinstance(head, ast_.PredHead) or isinstance(head, ast_.AnyHead):
        imports.add("from indexer.events.blocks.core import Block")
        base = "Block"
    else:
        imports.add("from typing import Any")
        base = "Any"

    if info.list_binding:
        return f"list[{base}]"
    if info.optional:
        return f"{base} | None"
    return base


def _to_pascal_case(snake: str) -> str:
    return "".join(part.capitalize() for part in snake.split("_"))
