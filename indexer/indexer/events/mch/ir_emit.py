"""MCH IR emitter for lowered node graphs.

Walks the ``LNode`` graph the compiler lowers each ``.mch`` matcher into (plain
records, no engine primitives). Node ids are per-matcher DFS pre-order from the
anchor node; the recursion order at each node is: or-branches / recursive step
then exit, then child, then children set items, then parent. Two runs over the
same specs yield byte-identical output.

CLI::

    python -m indexer.events.mch.ir_emit -o <out.json>

The CLI self-provisions permissive Registries: it pre-parses the spec files and
registers always-true predicate stubs, no-op async builder stubs, and dummy
block types for every referenced name, so specs compile without the production
host registries.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from indexer.events.mch import ast_
from indexer.events.mch.btypes import produces_btype
from indexer.events.mch.compile import compile_file
from indexer.events.mch.compiler import CompiledMatcher, LNode
from indexer.events.mch.expr_ir import IR_VERSION, emit_expr as _emit_expr
from indexer.events.mch.lexer import tokenize
from indexer.events.mch.parser import parse
from indexer.events.mch.registry import Registries


# Reproducible source list for build-time IR emission. Paths are relative to
# this module's directory. ORDER IS THE CONTRACT: it equals matcher registration
# order, and equal-priority matchers tie-break on this order.
#
# specs/jetton_transfer.mch and specs/jettons.mch are deliberately omitted.
# They are builder-tier conformance references that duplicate the
# jetton_transfer/burn/mint matchers in specs/jettons_decl.mch. They are
# permanently C++-skipped and inert in Python because the priority-90
# declarative jetton matchers consume the blocks first. Including them only
# produced duplicate/ambiguous name-keyed shadow counters and dead SKIP lines.
# They remain in specs/ for the Python conformance suite, which references them
# by matcher name, so suffixing was not an option.
DEFAULT_SPEC_PATHS = [
    "specs/nft_mint.mch",
    "specs/jettons_decl.mch",
    "specs/pton_transfer.mch",
    "specs/coffee_liquidity.mch",
    "specs/coffee_swap.mch",
    "specs/coffee_withdraw.mch",
    "specs/dedust_deposit.mch",
    "specs/dedust_swap.mch",
    "specs/dedust_withdraw.mch",
    "specs/dedust_v2_claim_fees.mch",
    "specs/dedust_v2_claim_reward.mch",
    "specs/dedust_v2_swap.mch",
    "specs/dedust_v2_withdraw.mch",
    "specs/dedust_v2_deposit.mch",
    "specs/dns.mch",
    "specs/elections.mch",
    "specs/subscriptions.mch",
    "specs/ethena.mch",
    "specs/evaa.mch",
    "specs/nft_transfer.mch",
    "specs/getgems.mch",
    "specs/jvault.mch",
    "specs/layerzero.mch",
    "specs/multisig.mch",
    "specs/nominator.mch",
    "specs/stonfi_v1_swap.mch",
    "specs/stonfi_v2_provide.mch",
    "specs/stonfi_v2_swap.mch",
    "specs/stonfi_v2_withdraw.mch",
    "specs/telegram_nft.mch",
    "specs/teleitem_auction.mch",
    "specs/nft_sale.mch",
    "specs/tgbtc.mch",
    "specs/tonco_liquidity.mch",
    "specs/tonco_swap.mch",
    "specs/tonstakers.mch",
    "specs/vesting.mch",
    "specs/coffee_create_pool.mch",
    "specs/coffee_mev.mch",
    "specs/coffee_staking.mch",
    "specs/cocoon.mch",
    "specs/tgwallet.mch",  # last: claims only a change-key call nothing else wanted
]


def _unsigned32(opcode: int) -> int:
    """Canonical unsigned 32-bit opcode (schema rule: hosts compare unsigned)."""
    return opcode & 0xFFFFFFFF


# Stub registries (CLI self-provisioning)


def _collect_predicate_names(p: ast_.PatternExpr, out: set[str]) -> None:
    if isinstance(p, ast_.Node):
        if p.where_predicate is not None:
            out.add(p.where_predicate)
        if isinstance(p.head, ast_.PredHead):
            out.add(p.head.name)
    elif isinstance(p, (ast_.Maybe, ast_.Peek)):
        _collect_predicate_names(p.inner, out)
    elif isinstance(p, ast_.Sequence):
        _collect_predicate_names(p.head, out)
        for _edge, atom in p.tail:
            _collect_predicate_names(atom, out)
    elif isinstance(p, ast_.Alternative):
        for b in p.branches:
            _collect_predicate_names(b, out)
    elif isinstance(p, ast_.ChildrenBlock):
        for it in p.items:
            _collect_predicate_names(it, out)
    # RuleRef: rule bodies are scanned via File.rules directly.


class _StubMessage:
    """Message-type stand-in for compile-time validation (never parsed here)."""

    def __init__(self, _slice):
        pass


def build_stub_registries(files: list[ast_.File]) -> Registries:
    """Permissive Registries covering every name the given files reference."""
    reg = Registries()
    predicate_names: set[str] = set()

    def stub_expr(e: ast_.Expr) -> None:
        if isinstance(e, ast_.FieldAccess):
            stub_expr(e.target)
        elif isinstance(e, ast_.Call):
            if not isinstance(e.callee, ast_.NameRef):
                stub_expr(e.callee)
            for arg in e.args:
                stub_expr(arg)
        elif isinstance(e, ast_.LookupExpr):
            for arg in e.args:
                stub_expr(arg)
        elif isinstance(e, ast_.UnaryOp):
            stub_expr(e.operand)
        elif isinstance(e, ast_.BinaryOp):
            stub_expr(e.left)
            stub_expr(e.right)
        elif isinstance(e, ast_.Ternary):
            stub_expr(e.cond)
            stub_expr(e.then)
            stub_expr(e.orelse)
        elif isinstance(e, ast_.ListLit):
            for item in e.elements:
                stub_expr(item)
        elif isinstance(e, ast_.RecordLit):
            for field in e.fields:
                stub_expr(field.value)
        elif isinstance(e, ast_.ParseExpr):
            stub_expr(e.target)
            for msg_type in e.msg_types:
                reg.message_types.setdefault(msg_type, _StubMessage)
        elif isinstance(e, ast_.Comprehension):
            stub_expr(e.xs)
            stub_expr(e.body)

    def stub_fields(fields: tuple[ast_.OutField, ...]) -> None:
        for field in fields:
            stub_expr(field.value)

    for f in files:
        for pd in f.predicates:
            predicate_names.add(pd.name)
        for r in f.rules:
            _collect_predicate_names(r.pattern, predicate_names)
        for m in f.matchers:
            _collect_predicate_names(m.pattern, predicate_names)
            for produced in m.produces:
                reg.block_types.setdefault(produced, object)
            if m.build is not None and m.build not in reg.builders:
                async def _stub_builder(match, _name=m.build):
                    return None
                reg.builders[m.build] = _stub_builder
            if m.shape is not None and m.shape not in reg.shapers:
                reg.shapers[m.shape] = lambda produced, match: None
            for stmt in m.build_stmts:
                if isinstance(stmt, ast_.ParseStmt):
                    for t in stmt.msg_types:
                        reg.message_types.setdefault(t, _StubMessage)
                elif isinstance(stmt, ast_.LetStmt):
                    stub_expr(stmt.value)
                elif isinstance(stmt, (ast_.RejectStmt, ast_.FailedStmt, ast_.BrokenStmt)):
                    stub_expr(stmt.condition)
                elif isinstance(stmt, ast_.OutStmt):
                    stub_fields(stmt.fields)
                elif isinstance(stmt, ast_.SwitchStmt):
                    for branch in stmt.branches:
                        if branch.condition is not None:
                            stub_expr(branch.condition)
                        stub_fields(branch.out.fields)
    for name in sorted(predicate_names):
        reg.predicates[name] = lambda _block: True
    return reg


# Emission


def _emit_node(n: LNode, nodes: list[dict], memo: dict[int, int]) -> int:
    """Append `n`'s node record (and its subtree) to `nodes`; return its id.

    Already-seen objects return their existing id (back-reference), which both
    handles shared sub-objects and guarantees termination on cyclic graphs.
    """
    existing = memo.get(id(n))
    if existing is not None:
        return existing

    # An in-body cyclic self-reference resolves to the shared body root.
    if n.kind == "cyclic_ref":
        target_id = _emit_node(n.target, nodes, memo)
        memo[id(n)] = target_id
        return target_id

    node_id = len(nodes)
    memo[id(n)] = node_id
    rec: dict = {}
    nodes.append(rec)

    if n.kind == "recursive" and n.strategy == "cyclic":
        # Outer cyclic reference: a `recursive` node with `strategy: "cyclic"`
        # and the body root as `step` (never an `exit`).
        rec["kind"] = "recursive"
        rec["strategy"] = "cyclic"
        rec["step"] = _emit_node(n.step, nodes, memo)
    elif n.kind == "recursive":
        rec["kind"] = "recursive"
        rec["step"] = _emit_node(n.step, nodes, memo)
        if n.exit_ is not None:
            rec["exit"] = _emit_node(n.exit_, nodes, memo)
    elif n.kind == "or":
        rec["kind"] = "or"
        rec["branches"] = [_emit_node(b, nodes, memo) for b in n.branches]
        rec["exclusive"] = n.exclusive
    elif n.kind == "contract":
        rec["kind"] = "contract"
        rec["opcode"] = _unsigned32(n.opcode)
    elif n.kind == "block_type":
        rec["kind"] = "block_type"
        rec["btype"] = n.btype
    elif n.kind == "pred":
        rec["kind"] = "pred"
        rec["pred"] = n.pred
    elif n.kind == "any":
        # Permissive nodes: `any` heads, recursion sentinels, and opcode-set /
        # predicate anchor roots (whose test lives in the matcher record's
        # anchor, see _anchor_record).
        rec["kind"] = "any"
    else:
        raise ValueError(f"unsupported node kind {n.kind!r} in IR emission")

    if n.optional:
        rec["optional"] = True
    if n.peek:
        rec["peek"] = True
    if n.capture is not None:
        rec["capture"] = n.capture
    if n.where is not None:
        rec["where"] = n.where
    if n.where_expr is not None:
        rec["where_expr"] = _emit_expr(n.where_expr)
    if n.child is not None:
        rec["child"] = _emit_node(n.child, nodes, memo)
    if n.children:
        rec["children"] = [_emit_node(c, nodes, memo) for c in n.children]
    if n.parent is not None:
        rec["parent"] = _emit_node(n.parent, nodes, memo)
    return node_id


def _anchor_record(cm: CompiledMatcher) -> dict:
    # Predicate anchors have no opcode/btype prefilter. The anchor test
    # is the named predicate, full-scanned by the engine.
    # A mixed-kind or per-branch-`where` anchor group uses a `mixed`
    # record carrying one branch object per alternative branch, each `{op}` or
    # `{btype}` with an optional `where` predicate name. Membership is the union
    # of all branch heads; the matching branch's `where` (if any) is tested
    # after membership. Pure single-kind groups without `where` keep the
    # opcode_set/btype forms (compiler records exactly one representation).
    if cm.anchor_pred is not None:
        return {"kind": "pred", "pred": cm.anchor_pred}
    if cm.anchor_branches is not None:
        out_branches: list[dict] = []
        for kind, value, where_name in cm.anchor_branches:
            rec = {"op": _unsigned32(value)} if kind == "op" else {"btype": value}
            if where_name is not None:
                rec["where"] = where_name
            out_branches.append(rec)
        return {"kind": "mixed", "branches": out_branches}
    if cm.anchor_opcodes:
        return {"kind": "opcode_set", "values": [_unsigned32(o) for o in cm.anchor_opcodes]}
    if cm.anchor_btypes:
        return {"kind": "btype", "values": list(cm.anchor_btypes)}
    root = cm.root
    if root.kind == "contract":
        return {"kind": "opcode_set", "values": [_unsigned32(root.opcode)]}
    if root.kind == "block_type":
        return {"kind": "btype", "values": [root.btype]}
    raise ValueError(f"unsupported anchor root kind {root.kind!r}")


# The IR schema requires btype strings; class names are normalized via the
# shared map in btypes.py (also used by the declarative build path).
_produces_btype = produces_btype


# Expressions and build programs.


# _emit_expr = expr_ir.emit_expr. The compiler shares that encoding, and the
# import direction avoids a cycle through ir_emit.


def _emit_stmt(stmt: ast_.BuildStmt) -> dict:
    if isinstance(stmt, ast_.ParseStmt):
        return {"s": "parse", "target": stmt.capture, "types": list(stmt.msg_types)}
    if isinstance(stmt, ast_.LetStmt):
        return {"s": "let", "name": stmt.name, "expr": _emit_expr(stmt.value)}
    if isinstance(stmt, ast_.RejectStmt):
        return {"s": "reject", "when": _emit_expr(stmt.condition)}
    if isinstance(stmt, ast_.FailedStmt):
        return {"s": "failed", "when": _emit_expr(stmt.condition)}
    if isinstance(stmt, ast_.BrokenStmt):
        return {"s": "broken", "when": _emit_expr(stmt.condition)}
    if isinstance(stmt, ast_.OutStmt):
        return {"s": "out", "fields": _emit_out_fields(stmt.fields)}
    if isinstance(stmt, ast_.SwitchStmt):
        return {"s": "switch", "branches": [_emit_switch_branch(b) for b in stmt.branches]}
    raise ValueError(f"unsupported build statement {type(stmt).__name__} in IR emission")


def _emit_out_fields(fields) -> list[dict]:
    out = []
    for f in fields:
        rec = {"name": f.name, "expr": _emit_expr(f.value)}
        # `optional` is emitted only when set, keeping existing artifacts
        # byte-identical (additive-only IR change).
        if f.optional:
            rec["optional"] = True
        out.append(rec)
    return out


def _emit_switch_branch(b: ast_.SwitchBranch) -> dict:
    # Normalize the branch btype the same way `produces` names are (class ->
    # btype string), so a branch may name either form.
    rec: dict = {"btype": _produces_btype(b.btype), "fields": _emit_out_fields(b.out.fields)}
    # The `else` arm (condition is None) omits `when`.
    if b.condition is not None:
        rec["when"] = _emit_expr(b.condition)
    return rec


def emit_ir(
    compiled: list[CompiledMatcher],
    source_files: list[str],
    build_stmts: list[tuple[ast_.BuildStmt, ...]] | None = None,
) -> dict:
    """Build the top-level artifact dict, keys in schema order.

    Build statements come from `CompiledMatcher.build_stmts`; the optional
    `build_stmts` parameter (parallel to `compiled`) overrides them and is kept
    for callers that pre-extracted statements from parsed `ast_.File`s.
    """
    if build_stmts is None:
        build_stmts = [cm.build_stmts for cm in compiled]
    matchers_out: list[dict] = []
    nodes: list[dict] = []
    for cm, stmts in zip(compiled, build_stmts):
        memo: dict[int, int] = {}
        root_id = _emit_node(cm.root, nodes, memo)
        rec: dict = {
            "name": cm.name,
            "anchor": _anchor_record(cm),
            "root": root_id,
            "produces": [_produces_btype(p) for p in cm.produces],
            "captures": [
                {"name": cap_name, "card": card}
                for cap_name, card in cm.captures
            ],
            "include_excess": bool(cm.include_excess),
            "include_bounces": bool(cm.include_bounces),
        }
        # `priority`: emitted only when non-default (100), keeping existing
        # artifacts byte-identical. The loader defaults an absent key to 100.
        if cm.priority != 100:
            rec["priority"] = cm.priority
        # Declarative matchers (build statements, no registered builder) have
        # no builder name; per the container rule (no nulls for absent
        # fields), omit rather than emit "builder": null.
        if cm.build_fn_name is not None:
            rec["builder"] = cm.build_fn_name
        if stmts:
            rec["build_program"] = [_emit_stmt(s) for s in stmts]
        # `shape` contains a registered shaper name. Omit it when absent;
        # omit when absent, never emit null.
        if cm.shape_fn_name is not None:
            rec["shape"] = cm.shape_fn_name
        matchers_out.append(rec)
    return {
        "mch_ir_version": IR_VERSION,
        "frontend": {"generator": "mch-python", "source_files": source_files},
        "matchers": matchers_out,
        "nodes": nodes,
        "registration_order": list(range(len(matchers_out))),
    }


def _emit_paths(paths: list[Path], source_files: list[str]) -> dict:
    """Compile the given .mch files IN THE GIVEN ORDER and emit IR.

    Emission order == matcher registration order (source-position tie-break for
    equal `priority`), so callers control ordering by ordering `paths`.
    `source_files` is the label list written to `frontend.source_files`.
    """
    files = [
        parse(list(tokenize(p.read_text(encoding="utf-8"), str(p))), str(p))
        for p in paths
    ]
    registries = build_stub_registries(files)
    compiled: list[CompiledMatcher] = []
    for p in paths:
        compiled.extend(compile_file(p, registries))
    return emit_ir(compiled, source_files)


def emit_dir(specs_dir: Path) -> dict:
    """Compile every .mch file in `specs_dir` (sorted by file name) and emit IR."""
    paths = sorted(Path(specs_dir).glob("*.mch"), key=lambda p: p.name)
    if not paths:
        raise FileNotFoundError(f"no .mch files found in {specs_dir}")
    return _emit_paths(paths, [p.name for p in paths])


def emit_manifest(manifest_path: Path) -> dict:
    """Compile the spec files listed in a manifest and emit IR.

    Manifest = JSON ``{"specs": ["<rel>", ...]}``; each entry is a spec path
    relative to the mch package dir (this file's parent), resolved so the
    artifact is reproducible from committed sources. The list order IS the
    registration order; `frontend.source_files` = the entries verbatim.
    """
    data = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    rels: list[str] = data["specs"]
    base = Path(__file__).resolve().parent
    paths = [base / r for r in rels]
    missing = [str(p) for p in paths if not p.is_file()]
    if missing:
        raise FileNotFoundError(f"manifest references missing spec files: {missing}")
    return _emit_paths(paths, list(rels))


def emit_default() -> dict:
    """Compile the built-in production spec list and emit IR."""
    base = Path(__file__).resolve().parent
    paths = [base / rel for rel in DEFAULT_SPEC_PATHS]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"built-in manifest references missing spec files: {missing}")
    return _emit_paths(paths, list(DEFAULT_SPEC_PATHS))


def render_json(artifact: dict) -> str:
    return json.dumps(artifact, indent=2, sort_keys=False, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        prog="python -m indexer.events.mch.ir_emit",
        description="Emit MCH IR from the built-in production list or specified .mch specs.",
    )
    ap.add_argument("specs_dir", nargs="?", help="directory containing .mch spec files")
    ap.add_argument(
        "--manifest",
        help="JSON manifest listing spec files in registration order "
        "(paths relative to the mch package dir); reproducible artifact source",
    )
    ap.add_argument("-o", "--output", required=True, help="output JSON path")
    args = ap.parse_args(argv)

    if args.specs_dir and args.manifest:
        ap.error("provide at most one of specs_dir or --manifest")
    if args.manifest:
        artifact = emit_manifest(Path(args.manifest))
    elif args.specs_dir:
        artifact = emit_dir(Path(args.specs_dir))
    else:
        artifact = emit_default()
    with open(args.output, "w", encoding="utf-8", newline="\n") as f:
        f.write(render_json(artifact))
    print(
        f"wrote {args.output}: {len(artifact['matchers'])} matchers, "
        f"{len(artifact['nodes'])} nodes"
    )


if __name__ == "__main__":
    main()
