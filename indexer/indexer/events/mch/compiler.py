from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from indexer.events.mch import ast_
from indexer.events.mch.builtin_signatures import BUILTIN_SIGNATURES
from indexer.events.mch.diagnostics import CompileError, DiagnosticBag, Span
from indexer.events.mch.recursion import RecursionStrategy, RecursiveMatcherStrategy
from indexer.events.mch.registry import Registries
from indexer.events.mch.resolver import (
    CaptureInfo,
    RecursionClass,
    ResolvedFile,
    ResolvedMatcher,
)


@dataclass
class LNode:
    """A lowered IR node.

    The frontend lowers `.mch` patterns straight into a graph of these plain
    records. No engine primitives are constructed. `ir_emit` walks the graph in
    DFS pre-order to assign node ids and produce the JSON artifact; the v2 IR
    engine (`mch_ir/`) is the only runtime that consumes it.

    `kind` ∈ {"contract", "block_type", "pred", "any", "or", "recursive",
    "cyclic_ref"}. Field usage by kind:
      - contract:   `opcode`
      - block_type: `btype`
      - pred:       `pred`
      - any:        (leaf, permissive; anchor-group / predicate-anchor roots,
                    recursion sentinels, `any` heads)
      - or:         `branches`, `exclusive`
      - recursive:  `step`, `exit_` (frontier), or `strategy="cyclic"` + `step`
      - cyclic_ref: `target`, an in-body self-reference back-edge; carries no
                    node of its own (emitter resolves it to `target`'s id).
    Common optional fields on every kind: `optional`, `peek`, `capture`, `where`,
    `where_expr`, `child`, `children`, `parent`.
    """

    kind: str
    opcode: int | None = None
    btype: str | None = None
    pred: str | None = None
    branches: list["LNode"] | None = None
    exclusive: bool = False
    strategy: str | None = None            # "cyclic" or None (frontier)
    step: "LNode | None" = None
    exit_: "LNode | None" = None
    target: "LNode | None" = None          # cyclic_ref back-edge target
    optional: bool = False
    peek: bool = False
    peek_span: Span | None = None
    capture: str | None = None
    where: str | None = None
    where_expr: "ast_.Expr | None" = None
    child: "LNode | None" = None
    children: list["LNode"] | None = None
    parent: "LNode | None" = None


@dataclass
class CompiledMatcher:
    name: str
    root: LNode
    build_fn_name: str | None
    produces_cls_name: str
    source_span: Span
    produces: tuple[str, ...] = ()
    captures: tuple[tuple[str, str], ...] = ()
    include_excess: bool = True
    include_bounces: bool = True
    build_stmts: tuple = ()
    shape_fn_name: str | None = None
    priority: int = 100
    # Anchor record representation. Exactly one of
    # these is set for a group/predicate anchor; a plain single op/btype anchor
    # leaves them all None and the emitter reads the root node's kind.
    anchor_pred: str | None = None
    anchor_branches: tuple | None = None
    anchor_opcodes: tuple[int, ...] | None = None
    anchor_btypes: tuple[str, ...] | None = None


@dataclass
class CompileCtx:
    resolved: ResolvedFile
    registries: Registries
    recursion: RecursionStrategy
    bag: DiagnosticBag
    where_entry_capture: str = ""
    # Names of recursive rules currently being lowered; used to break cycles
    # by treating self-refs inside step/exit branches as a recursion sentinel.
    lowering_recursive: set[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.lowering_recursive is None:
            self.lowering_recursive = set()


def compile_(
    resolved: ResolvedFile,
    registries: Registries,
    recursion: RecursionStrategy | None = None,
) -> list[CompiledMatcher]:
    """Compile a resolved file to a list of CompiledMatcher.

    Function is named `compile_` (trailing underscore) to avoid shadowing the builtin.
    """
    bag = DiagnosticBag()
    ctx = CompileCtx(
        resolved=resolved,
        registries=registries,
        recursion=recursion or RecursiveMatcherStrategy(),
        bag=bag,
    )

    out: list[CompiledMatcher] = []
    for rm in resolved.matchers:
        out.append(_compile_matcher(rm, ctx))

    if bag.has_errors:
        raise CompileError(bag)

    return out


def _compile_matcher(rm: ResolvedMatcher, ctx: CompileCtx) -> CompiledMatcher:
    decl = rm.decl
    ctx.where_entry_capture = rm.anchor_capture

    _check_no_capture_after_recursion(decl.pattern, decl.span, ctx)
    _check_no_bare_children_block(decl.pattern, ctx)
    if ctx.bag.has_errors:
        raise CompileError(ctx.bag)

    if decl.produces_primary not in ctx.registries.block_types:
        ctx.bag.error(
            "C003_UNKNOWN_BLOCK_TYPE",
            f"matcher {decl.name!r}: produces type {decl.produces_primary!r} is not registered",
            decl.span,
        )
    if decl.build is not None and decl.build not in ctx.registries.builders:
        ctx.bag.error(
            "C002_UNKNOWN_BUILDER",
            f"matcher {decl.name!r}: builder {decl.build!r} is not registered",
            decl.span,
        )
    if decl.shape is not None and decl.shape not in ctx.registries.shapers:
        ctx.bag.error(
            "C011_UNKNOWN_SHAPER",
            f"matcher {decl.name!r}: shaper {decl.shape!r} is not registered",
            decl.span,
        )

    # Lower the pattern relative to the anchor atom. For set anchors, the whole
    # exclusive alternative group is the anchor atom,
    # so chain edges around it wire exactly as around a single anchor node.
    anchor_atom: ast_.PatternExpr = (
        rm.anchor_group if rm.anchor_group is not None else rm.anchor_node
    )
    parent_m, child_m, children_ms = _lower_relative_to_anchor(decl.pattern, anchor_atom, ctx)

    head = rm.anchor_node.head
    anchor_pred: str | None = None
    anchor_branches: tuple | None = None
    anchor_opcodes: tuple[int, ...] | None = None
    anchor_btypes: tuple[str, ...] | None = None
    if rm.anchor_group is not None:
        # Membership is recorded separately; the graph root remains permissive.
        if rm.anchor_group.exclusive:
            _check_redundant_exclusive_heads(rm.anchor_group, ctx)
        opcode_set, btype_set = _anchor_group_head_sets(rm.anchor_group, ctx)
        branch_descs = _anchor_group_branch_descs(rm.anchor_group, ctx)
        any_where = any(w is not None for _k, _v, w in branch_descs)
        root = LNode(kind="any", child=child_m, parent=parent_m)
    elif isinstance(head, ast_.OpHead):
        opcode = head.ref if isinstance(head.ref, int) else ctx.resolved.opcodes.get(head.ref) or ctx.registries.opcodes.get(head.ref)
        root = LNode(
            kind="contract",
            opcode=opcode,
            child=child_m,
            parent=parent_m,
            children=children_ms,
        )
    elif isinstance(head, ast_.BTypeHead):
        root = LNode(kind="block_type", btype=head.name, child=child_m, parent=parent_m)
    elif isinstance(head, ast_.PredHead):
        # Predicate anchors have no opcode/btype prefilter and are full-scanned.
        anchor_pred = head.name
        root = LNode(kind="any", child=child_m, parent=parent_m)
    else:
        # Resolver should have rejected this; defensive guard.
        ctx.bag.error(
            "C001_RECURSION_SHAPE_UNSUPPORTED",
            f"matcher {decl.name!r}: unsupported anchor head {type(head).__name__}",
            decl.span,
        )
        raise CompileError(ctx.bag)

    # Some heads carry children_matchers only post-hoc (only `contract` takes
    # them at construction); apply here if not already set.
    if children_ms is not None and root.children is None:
        root.children = children_ms
    # Apply where-predicate on the anchor node, if any. For a set-membership
    # group anchor, the per-branch `where` is already folded into
    # the group's membership record; `anchor_node` is only a representative
    # branch, so its `where` is not re-applied to the root node.
    anchor_pred_name = None if rm.anchor_group is not None else rm.anchor_node.where_predicate
    if anchor_pred_name is not None:
        root.where = anchor_pred_name
    if rm.anchor_capture:
        root.capture = rm.anchor_capture
    if rm.anchor_group is None and rm.anchor_node.where_expr is not None:
        _set_where_expr(root, rm.anchor_node.where_expr, rm.anchor_node.span, ctx)
    if rm.anchor_group is not None:
        # Group-anchor branches carry only named `where` predicates in the IR.
        # Reject an inline expression rather than dropping it.
        for b in rm.anchor_group.branches:
            if isinstance(b, ast_.Node) and b.where_expr is not None:
                ctx.bag.error(
                    "C013_WHERE_EXPR_NOT_SYNC",
                    f"matcher {decl.name!r}: inline `where (expr)` is not "
                    f"supported on opcode-set anchor branches; use a named "
                    f"`where <predicate>`",
                    b.span,
                )
    if rm.anchor_group is not None:
        # A pure single-kind group with no branch `where` keeps the
        # `opcode_set`/`btype` forms.
        # a mixed-kind group or any branch `where` emits the `mixed` branch form
        # (see ir_emit._anchor_record). Record exactly one representation.
        kinds = {k for k, _v, _w in branch_descs}
        if len(kinds) == 1 and not any_where:
            if kinds == {"op"}:
                anchor_opcodes = tuple(sorted(opcode_set))
            else:
                anchor_btypes = tuple(sorted(btype_set))
        else:
            anchor_branches = tuple(branch_descs)

    _check_peek_subtree_closure(root, decl.name, ctx)

    return CompiledMatcher(
        name=decl.name,
        root=root,
        build_fn_name=decl.build,
        produces_cls_name=decl.produces_primary,
        source_span=decl.span,
        produces=tuple(decl.produces),
        captures=tuple(
            (cname, "many" if ci.list_binding else ("opt" if ci.optional else "one"))
            for cname, ci in rm.captures.items()
        ),
        include_excess=bool(decl.include_excess),
        include_bounces=bool(decl.include_bounces),
        build_stmts=tuple(decl.build_stmts),
        shape_fn_name=decl.shape,
        priority=decl.priority,
        anchor_pred=anchor_pred,
        anchor_branches=anchor_branches,
        anchor_opcodes=anchor_opcodes,
        anchor_btypes=anchor_btypes,
    )


def _lower_relative_to_anchor(
    pattern: ast_.PatternExpr,
    anchor: ast_.PatternExpr,
    ctx: CompileCtx,
) -> tuple[LNode | None, LNode | None, list[LNode] | None]:
    """Walk the pattern to figure out (parent_matcher, child_matcher, children_matchers).

    `anchor` is compared by identity: a single anchor Node, or the whole
    exclusive Alternative for opcode-set anchors.
    """
    parents = _find_anchor_path(pattern, anchor)
    if parents is None:
        return None, None, None
    # `parents` ends with the anchor itself; its immediate parent is parents[-2].
    if len(parents) < 2:
        return None, None, None
    immediate_parent = parents[-2]

    child_chain, children_ms = _lower_child_chain_from(immediate_parent, pattern, anchor, ctx)
    parent_chain = _lower_parent_chain_from(immediate_parent, pattern, anchor, ctx)
    return parent_chain, child_chain, children_ms


def _find_anchor_path(pattern: ast_.PatternExpr, anchor: ast_.PatternExpr) -> list[ast_.PatternExpr] | None:
    if pattern is anchor:
        return None

    def search(p: ast_.PatternExpr, path: list[ast_.PatternExpr]) -> list[ast_.PatternExpr] | None:
        if p is anchor:
            return path + [p]
        if isinstance(p, ast_.Sequence):
            r = search(p.head, path + [p])
            if r:
                return r
            for _e, atom in p.tail:
                r = search(atom, path + [p])
                if r:
                    return r
        elif isinstance(p, (ast_.Maybe, ast_.Peek)):
            return search(p.inner, path + [p])
        elif isinstance(p, ast_.Alternative):
            for b in p.branches:
                r = search(b, path + [p])
                if r:
                    return r
        elif isinstance(p, ast_.ChildrenBlock):
            for it in p.items:
                r = search(it, path + [p])
                if r:
                    return r
        return None

    return search(pattern, [])


def _unwrap_modifiers(
    p: ast_.PatternExpr,
) -> tuple[ast_.PatternExpr, bool, bool, Span | None]:
    optional = False
    peek = False
    peek_span: Span | None = None
    while isinstance(p, (ast_.Maybe, ast_.Peek)):
        if isinstance(p, ast_.Maybe):
            optional = True
        else:
            peek = True
            if peek_span is None:
                peek_span = p.span
        p = p.inner
    return p, optional, peek, peek_span


def _is_cb_atom(a: ast_.PatternExpr) -> bool:
    """Whether an atom attaches left without advancing the chain cursor.
    """
    inner, _optional, _peek, _peek_span = _unwrap_modifiers(a)
    return isinstance(inner, ast_.ChildrenBlock)


def _lower_child_chain_from(
    parent: ast_.PatternExpr,
    pattern: ast_.PatternExpr,
    anchor: ast_.PatternExpr,
    ctx: CompileCtx,
) -> tuple[LNode | None, list[LNode] | None]:
    if isinstance(parent, ast_.Sequence):
        # Collect child-edged atoms in the anchor's downstream slice. Stop at
        # the first parent edge; later atoms belong to the parent chain
        # (`_lower_parent_chain_from` consumes them).
        # In `op A <- op B -> {C; D}`, the children belong to B's parent chain,
        # not to anchor A.
        downstream_atoms: list[ast_.PatternExpr] = []
        if parent.head is anchor:
            tail_slice = parent.tail
        else:
            if isinstance(parent.head, ast_.ChildrenBlock):
                ctx.bag.error(
                    "C008_CHILDREN_BLOCK_BARE",
                    "children block '{...}' cannot appear as the first element of a sequence; "
                    "it must follow a node (e.g. 'op A -> {op B; op C}')",
                    parent.head.span,
                )
                raise CompileError(ctx.bag)
            anchor_idx = None
            for i, (_e, atom) in enumerate(parent.tail):
                if atom is anchor:
                    anchor_idx = i
                    break
            if anchor_idx is None:
                return None, None
            tail_slice = parent.tail[anchor_idx + 1:]
        for edge, atom in tail_slice:
            if edge is ast_.Edge.PARENT:
                if downstream_atoms and not all(_is_cb_atom(a) for a in downstream_atoms):
                    # `op A -> op B <- op C`: the parent edge's left side is a
                    # non-anchor node (B) reached through a preceding child edge.
                    # Its only faithful lowering sets B.parent_matcher = C, which
                    # at runtime tests B.previous_block (the anchor A) against
                    # opcode C and can never be satisfied. Reject rather than
                    # silently drop the `<- C` segment.
                    # If everything before the parent edge is a children block,
                    # the cursor never left the anchor. In
                    # `op A -> {B; C} <- op P`, A has both
                    # children_matchers and parent_matcher.
                    ctx.bag.error(
                        "C009_MIXED_CHILD_PARENT_CHAIN_UNSUPPORTED",
                        "a parent edge ('<-') following a child edge in the same "
                        "chain is unsupported: the parent would test the anchor "
                        "block and can never match; split the pattern or re-anchor",
                        atom.span,
                    )
                    raise CompileError(ctx.bag)
                break
            downstream_atoms.append(atom)
        if not downstream_atoms:
            return None, None
        first = downstream_atoms[0]
        first_inner, _optional, _peek, _peek_span = _unwrap_modifiers(first)
        if isinstance(first_inner, ast_.ChildrenBlock):
            # Consume consecutive anchor-adjacent children blocks so
            # `{B;C} {D;E}`
            # all attach to the anchor's children_matchers in source order.
            children_ms: list[LNode] = []
            rest_idx = 0
            while rest_idx < len(downstream_atoms):
                atom = downstream_atoms[rest_idx]
                inner, optional_items, peek_items, peek_span = _unwrap_modifiers(atom)
                if not isinstance(inner, ast_.ChildrenBlock):
                    break
                for it in inner.items:
                    lowered_it = _lower_atom(it, ctx)
                    if optional_items:
                        lowered_it.optional = True
                    if peek_items:
                        lowered_it.peek = True
                        lowered_it.peek_span = peek_span
                    children_ms.append(lowered_it)
                rest_idx += 1
            rest = downstream_atoms[rest_idx:]
            if rest:
                pairs = [(ast_.Edge.CHILD, a) for a in rest]
                child = _lower_chain_with_children_attach(pairs, ctx)
                return child, children_ms
            return None, children_ms
        pairs = [(ast_.Edge.CHILD, a) for a in downstream_atoms]
        child = _lower_chain_with_children_attach(pairs, ctx)
        return child, None

    return None, None


def _lower_parent_chain_from(
    parent: ast_.PatternExpr,
    pattern: ast_.PatternExpr,
    anchor: ast_.PatternExpr,
    ctx: CompileCtx,
) -> LNode | None:
    """Lower consecutive PARENT edges from the anchor outward into a chain of
    parent_matchers. Handles anchor-is-head and both anchor-in-tail variants.
    """
    if not isinstance(parent, ast_.Sequence):
        return None

    prepend_previous = False
    start_idx = 0

    if parent.head is anchor:
        # Skip anchor-attached children blocks. They do not advance the chain
        # cursor, so a parent edge after them still belongs to the
        # anchor: `op A -> {B; C} <- op P`.
        idx = 0
        while (
            idx < len(parent.tail)
            and parent.tail[idx][0] is ast_.Edge.CHILD
            and _is_cb_atom(parent.tail[idx][1])
        ):
            idx += 1
        if idx >= len(parent.tail) or parent.tail[idx][0] is not ast_.Edge.PARENT:
            return None
        start_idx = idx
    else:
        anchor_idx = next(
            (i for i, (_e, a) in enumerate(parent.tail) if a is anchor),
            None,
        )
        if anchor_idx is None:
            return None
        edge_at_anchor, _ = parent.tail[anchor_idx]
        if edge_at_anchor is ast_.Edge.PARENT:
            # When the anchor's incoming edge is PARENT, the previous atom
            # acts as the first parent link.
            prepend_previous = True
            start_idx = anchor_idx + 1
        else:
            # When the anchor's incoming edge is CHILD, an atom upstream
            # of the anchor (`… -> op Anchor`) is the anchor's parent. Lowering
            # only walks downstream from the anchor, so every upstream atom would
            # be silently dropped. Reject
            # rather than misbehave; re-anchor or restructure the pattern.
            upstream_atom = (
                parent.head if anchor_idx == 0 else parent.tail[anchor_idx - 1][1]
            )
            ctx.bag.error(
                "C010_UPSTREAM_ATOMS_UNSUPPORTED",
                "upstream atoms before the anchor are not lowered; re-anchor the "
                "pattern or restructure — see specs/stonfi_v2_swap.mch",
                upstream_atom.span if hasattr(upstream_atom, "span") else anchor.span,
            )
            raise CompileError(ctx.bag)

    # Build the parent chain in one interleaved walk from the anchor's first
    # parent link outward. PARENT edges extend the chain; CHILD-edged children
    # blocks attach to the current chain tail without advancing the cursor, so
    # `<- op X -> {CB} <- Y` gives X both
    # children_matchers and parent_matcher, matching the grouped form
    # `<- ( op X -> {CB} <- Y )`. A CHILD edge with a non-CB atom (a child branch
    # off a parent node) is out of scope in v0 and stops the walk.
    chain_pairs: list[tuple[ast_.Edge, ast_.PatternExpr]] = []
    if prepend_previous:
        anchor_idx = next(i for i, (_e, a) in enumerate(parent.tail) if a is anchor)
        previous_atom = parent.head if anchor_idx == 0 else parent.tail[anchor_idx - 1][1]
        chain_pairs.append((ast_.Edge.PARENT, previous_atom))

    saw_parent_link = prepend_previous
    for edge, atom in parent.tail[start_idx:]:
        if edge is ast_.Edge.PARENT:
            chain_pairs.append((ast_.Edge.PARENT, atom))
            saw_parent_link = True
        elif edge is ast_.Edge.CHILD and saw_parent_link and _is_cb_atom(atom):
            chain_pairs.append((ast_.Edge.CHILD, atom))
        else:
            # A non-PARENT edge before any parent link, or a non-CB child branch
            # off a parent node: out of scope in v0. Stop consuming.
            break

    if not saw_parent_link:
        return None

    return _lower_chain_with_children_attach(chain_pairs, ctx)


def _lower_chain_with_children_attach(
    pairs: list[tuple[ast_.Edge, ast_.PatternExpr]],
    ctx: CompileCtx,
) -> LNode | None:
    """Walk an ordered list of `(edge, atom)` pairs and build a chained node graph.

    ChildrenBlock atoms attach their items to the immediately-preceding node's
    `children`. Modifier-wrapped blocks apply those flags to each item root.
    Bare ChildrenBlock at the head (no left sibling) emits C008. Non-CB atoms
    chain via `_attach_as_child` or `_attach_as_parent`.
    """
    head_matcher: LNode | None = None
    previous_lowered: LNode | None = None

    for edge, atom in pairs:
        # Detect modifier-wrapped ChildrenBlock attach-to-left.
        cb_items: list[ast_.PatternExpr] | None = None
        optional_items = False
        peek_items = False
        peek_span: Span | None = None
        inner, optional_items, peek_items, peek_span = _unwrap_modifiers(atom)
        if isinstance(inner, ast_.ChildrenBlock):
            cb_items = list(inner.items)

        if cb_items is not None:
            if previous_lowered is None:
                # Bare ChildrenBlock with no left sibling.
                ctx.bag.error(
                    "C008_CHILDREN_BLOCK_BARE",
                    "children block '{...}' must follow a node in a sequence "
                    "(e.g. 'op A -> {op B; op C}'); bare children blocks are not allowed",
                    inner.span,
                )
                raise CompileError(ctx.bag)
            if previous_lowered.children is None:
                previous_lowered.children = []
            for item in cb_items:
                lowered_item = _lower_atom(item, ctx)
                if optional_items:
                    lowered_item.optional = True
                if peek_items:
                    lowered_item.peek = True
                    lowered_item.peek_span = peek_span
                previous_lowered.children.append(lowered_item)
            # Do not update previous_lowered. A children block constrains the
            # preceding matcher, not a chain link.
            continue

        lowered = _lower_atom(atom, ctx)
        if head_matcher is None:
            head_matcher = lowered
        else:
            if edge is ast_.Edge.PARENT:
                _attach_as_parent(previous_lowered, lowered)
            else:
                _attach_as_child(previous_lowered, lowered)
        previous_lowered = lowered

    return head_matcher


def _branches_eligible_for_exclusive_heuristic(
    branches: tuple[ast_.PatternExpr, ...],
    ctx: CompileCtx,
) -> bool:
    """Return True iff all branches are bare concrete Nodes with distinct resolved heads and no predicates."""
    seen_opcodes: set[int] = set()
    seen_btypes: set[str] = set()
    for b in branches:
        if not isinstance(b, ast_.Node):
            return False
        head = b.head
        if not isinstance(head, (ast_.OpHead, ast_.BTypeHead)):
            return False
        if b.where_predicate is not None or b.where_expr is not None:
            return False
        if isinstance(head, ast_.OpHead):
            ref = head.ref
            opcode_int = ref if isinstance(ref, int) else (
                ctx.resolved.opcodes.get(ref) or ctx.registries.opcodes.get(ref)
            )
            if opcode_int is None:
                return False
            if opcode_int in seen_opcodes:
                return False
            seen_opcodes.add(opcode_int)
        elif isinstance(head, ast_.BTypeHead):
            if head.name in seen_btypes:
                return False
            seen_btypes.add(head.name)
    return True


def _check_redundant_exclusive_heads(p: ast_.Alternative, ctx: CompileCtx) -> None:
    """Emit W202 if two branches share the same concrete head (non-blocking warning)."""
    seen: dict[object, object] = {}
    for b in p.branches:
        if not isinstance(b, ast_.Node):
            continue
        head = b.head
        if isinstance(head, ast_.OpHead):
            ref = head.ref
            opcode_int = ref if isinstance(ref, int) else (
                ctx.resolved.opcodes.get(ref) or ctx.registries.opcodes.get(ref)
            )
            key = ("op", opcode_int)
        elif isinstance(head, ast_.BTypeHead):
            key = ("btype", head.name)
        else:
            continue
        if key in seen:
            ctx.bag.warning(
                "W202_EXCLUSIVE_OR_REDUNDANT_HEADS",
                f"two branches of this exclusive alternative share the same head "
                f"({'opcode' if key[0] == 'op' else 'btype'} {key[1]!r}); "
                f"ExclusiveOrMatcher's sum-check will never return True for this head",
                b.span,
            )
        else:
            seen[key] = b.span


def _lnode_refs(n: LNode):
    for ref in (n.child, n.parent, n.step, n.exit_, n.target):
        if ref is not None:
            yield ref
    yield from n.children or ()
    yield from n.branches or ()


def _check_peek_subtree_closure(root: LNode, matcher_name: str, ctx: CompileCtx) -> None:
    all_nodes: list[LNode] = []
    seen_all: set[int] = set()
    stack = [root]
    while stack:
        node = stack.pop()
        if id(node) in seen_all:
            continue
        seen_all.add(id(node))
        all_nodes.append(node)
        stack.extend(_lnode_refs(node))

    for origin in all_nodes:
        if not origin.peek:
            continue
        seen = {id(origin)}
        descendants = list(_lnode_refs(origin))
        while descendants:
            node = descendants.pop()
            if id(node) in seen:
                continue
            seen.add(id(node))
            if not node.peek:
                if origin.peek_span is None:
                    raise AssertionError("peek node missing source span")
                ctx.bag.error(
                    "C014_PEEK_SUBTREE_CONSUMES",
                    f"matcher {matcher_name!r}: a `peek` subtree contains a consuming node; "
                    "every reachable node must also be `peek`",
                    origin.peek_span,
                )
                raise CompileError(ctx.bag)
            descendants.extend(_lnode_refs(node))


def _anchor_group_head_sets(
    group: ast_.Alternative,
    ctx: CompileCtx,
) -> tuple[frozenset[int], frozenset[str]]:
    """Collect the branch heads of an opcode-set anchor group.

    The resolver guarantees every branch is a bare Node with a concrete
    `op`/`btype` head and no `where` clause.
    """
    opcode_set: set[int] = set()
    btype_set: set[str] = set()
    for b in group.branches:
        head = b.head
        if isinstance(head, ast_.OpHead):
            opcode = head.ref if isinstance(head.ref, int) else (
                ctx.resolved.opcodes.get(head.ref) or ctx.registries.opcodes.get(head.ref)
            )
            opcode_set.add(opcode)
        else:
            btype_set.add(head.name)
    return frozenset(opcode_set), frozenset(btype_set)


def _anchor_group_branch_descs(
    group: ast_.Alternative,
    ctx: CompileCtx,
) -> list[tuple[str, Any, str | None]]:
    """Per-branch descriptors of a set-membership anchor group, in source order.
    Each is `(kind, value, where_name)` where kind is `"op"`
    (value = resolved opcode int) or `"btype"` (value = btype name), and
    where_name is the branch's `where` predicate name or None. The resolver
    guarantees every branch is a bare concrete `op`/`btype` node."""
    descs: list[tuple[str, Any, str | None]] = []
    for b in group.branches:
        head = b.head
        if isinstance(head, ast_.OpHead):
            opcode = head.ref if isinstance(head.ref, int) else (
                ctx.resolved.opcodes.get(head.ref) or ctx.registries.opcodes.get(head.ref)
            )
            descs.append(("op", opcode, b.where_predicate))
        else:
            descs.append(("btype", head.name, b.where_predicate))
    return descs


def _lower_atom(p: ast_.PatternExpr, ctx: CompileCtx) -> LNode:
    if isinstance(p, ast_.Node):
        return _lower_node(p, ctx)
    if isinstance(p, ast_.Maybe):
        # Bare `maybe {...}` wrapping a children block with no chain context is
        # rejected. `_lower_chain_with_children_attach` handles the in-chain
        # form `op A -> maybe {B; C}` first.
        if isinstance(p.inner, ast_.ChildrenBlock):
            ctx.bag.error(
                "C008_CHILDREN_BLOCK_BARE",
                "'maybe {...}' must follow a node in a sequence "
                "(e.g. 'op A -> maybe {op B; op C}'); bare maybe-children blocks are not allowed",
                p.inner.span,
            )
            raise CompileError(ctx.bag)
        inner = _lower_atom(p.inner, ctx)
        inner.optional = True
        return inner
    if isinstance(p, ast_.Peek):
        if isinstance(p.inner, ast_.ChildrenBlock):
            ctx.bag.error(
                "C008_CHILDREN_BLOCK_BARE",
                "'peek {...}' must follow a node in a sequence "
                "(e.g. 'op A -> peek {op B; op C}'); bare peek-children blocks are not allowed",
                p.inner.span,
            )
            raise CompileError(ctx.bag)
        inner = _lower_atom(p.inner, ctx)
        inner.peek = True
        inner.peek_span = p.span
        return inner
    if isinstance(p, ast_.Alternative):
        lowered_branches = [_lower_atom(b, ctx) for b in p.branches]
        if p.exclusive:
            _check_redundant_exclusive_heads(p, ctx)
            return LNode(kind="or", branches=lowered_branches, exclusive=True)
        if _branches_eligible_for_exclusive_heuristic(p.branches, ctx):
            return LNode(kind="or", branches=lowered_branches, exclusive=True)
        return LNode(kind="or", branches=lowered_branches, exclusive=False)
    if isinstance(p, ast_.Sequence):
        pairs: list[tuple[ast_.Edge, ast_.PatternExpr]] = [(ast_.Edge.CHILD, p.head)]
        pairs.extend(p.tail)
        result = _lower_chain_with_children_attach(pairs, ctx)
        if result is None:
            raise AssertionError("Sequence lowered to None")
        return result
    if isinstance(p, ast_.ChildrenBlock):
        ctx.bag.error(
            "C008_CHILDREN_BLOCK_BARE",
            "children block '{...}' cannot appear as a standalone expression; "
            "it must follow a node in a sequence (e.g. 'op A -> {op B; op C}')",
            p.span,
        )
        raise CompileError(ctx.bag)
    if isinstance(p, ast_.RuleRef):
        target = next((r for r in ctx.resolved.raw.rules if r.name == p.name), None)
        if target is None:
            raise CompileError(ctx.bag)
        # _lower_cyclic_rule patches this back-edge to the shared body root.
        cyclic_pending = getattr(ctx, _CYCLIC_PENDING_KEY, None)
        if cyclic_pending is not None and p.name in cyclic_pending:
            ref = LNode(kind="cyclic_ref")
            cyclic_pending[p.name].append(ref)
            return ref
        # Self-reference inside the body of a recursive rule we're already lowering:
        # the frontier recursion node handles the repetition, so we represent the
        # self-ref as a permissive sentinel.
        if p.name in ctx.lowering_recursive:
            return LNode(kind="any")
        rec_class = ctx.resolved.rule_recursion.get(p.name)
        if rec_class is RecursionClass.INDIRECT_RECURSIVE:
            ctx.bag.error(
                "C005_INDIRECT_RECURSION_UNSUPPORTED",
                f"rule {p.name!r}: indirect (mutual) recursion is not supported in v0; "
                f"flatten the rule cycle or use a single self-referential rule",
                p.span,
            )
            raise CompileError(ctx.bag)
        if rec_class is RecursionClass.DIRECT_RECURSIVE:
            if target.strategy == "cyclic":
                return _lower_cyclic_rule(p.name, ctx)
            return _lower_recursive_rule(p.name, ctx)
        return _lower_atom(target.pattern, ctx)
    raise AssertionError(f"unhandled pattern node: {type(p).__name__}")


def _check_where_expr_sync(e: ast_.Expr, span: Span, ctx: CompileCtx) -> bool:
    """Compile-time mirror of the IR loader's _check_where_expr_sync.

    An inline `where (expr)` composes onto the engine's synchronous test_self,
    so async constructs (lookups, host fns) are compile errors here. The IR
    loader rejects the same artifacts, keeping both paths failing at the same
    compiler instead of silently never matching. Returns True
    when the expression is sync-evaluable."""
    if isinstance(e, ast_.NameRef):
        if e.name != ctx.where_entry_capture:
            ctx.bag.error(
                "C015_WHERE_EXPR_CAPTURE_NOT_ENTRY",
                f"`where (expr)` may reference only entry capture "
                f"@{ctx.where_entry_capture}; name {e.name!r} is not allowed",
                e.span,
            )
            return False
        return True
    if isinstance(e, ast_.LookupExpr):
        ctx.bag.error(
            "C013_WHERE_EXPR_NOT_SYNC",
            "`where (expr)` cannot contain lookups (test_self is sync)",
            span,
        )
        return False
    if isinstance(e, ast_.Comprehension):
        ctx.bag.error(
            "C013_WHERE_EXPR_NOT_SYNC",
            "`where (expr)` cannot contain map/any/all comprehensions "
            "(build-expression only)",
            span,
        )
        return False
    if isinstance(e, ast_.ParseExpr):
        ctx.bag.error(
            "C013_WHERE_EXPR_NOT_SYNC",
            "`where (expr)` cannot contain a `parse` expression "
            "(build-expression only)",
            span,
        )
        return False
    if isinstance(e, ast_.Call):
        callee = e.callee
        if not isinstance(callee, ast_.NameRef) or callee.name not in BUILTIN_SIGNATURES:
            shown = callee.name if isinstance(callee, ast_.NameRef) else type(callee).__name__
            ctx.bag.error(
                "C013_WHERE_EXPR_NOT_SYNC",
                f"`where (expr)` can only call builtins, not host fn {shown!r}",
                span,
            )
            return False
        return all(_check_where_expr_sync(a, span, ctx) for a in e.args)
    if isinstance(e, ast_.FieldAccess):
        return _check_where_expr_sync(e.target, span, ctx)
    if isinstance(e, ast_.UnaryOp):
        return _check_where_expr_sync(e.operand, span, ctx)
    if isinstance(e, ast_.BinaryOp):
        return (_check_where_expr_sync(e.left, span, ctx)
                and _check_where_expr_sync(e.right, span, ctx))
    if isinstance(e, ast_.Ternary):
        return (_check_where_expr_sync(e.cond, span, ctx)
                and _check_where_expr_sync(e.then, span, ctx)
                and _check_where_expr_sync(e.orelse, span, ctx))
    if isinstance(e, ast_.ListLit):
        return all(_check_where_expr_sync(it, span, ctx) for it in e.elements)
    if isinstance(e, ast_.RecordLit):
        return all(_check_where_expr_sync(f.value, span, ctx) for f in e.fields)
    return True


def _set_where_expr(n: LNode, expr: ast_.Expr, span: Span, ctx: CompileCtx) -> None:
    """Store inline `where (expr)` on a node for IR emission.

    The C013 sync check still runs. An async construct
    (lookup/host fn) is a compile error, keeping the frontend and the IR loader
    failing at the same stage. The IR engine evaluates the expression at match
    time via the shared sync evaluator (expr_eval.py)."""
    if not _check_where_expr_sync(expr, span, ctx):
        return
    n.where_expr = expr


def _lower_node(n: ast_.Node, ctx: CompileCtx) -> LNode:
    head = n.head
    if isinstance(head, ast_.OpHead):
        opcode = head.ref if isinstance(head.ref, int) else (
            ctx.resolved.opcodes.get(head.ref) or ctx.registries.opcodes.get(head.ref)
        )
        m = LNode(kind="contract", opcode=opcode)
    elif isinstance(head, ast_.BTypeHead):
        m = LNode(kind="block_type", btype=head.name)
    elif isinstance(head, ast_.PredHead):
        m = LNode(kind="pred", pred=head.name)
    elif isinstance(head, ast_.AnyHead):
        m = LNode(kind="any")
    else:
        raise AssertionError(f"unknown node head: {type(head).__name__}")
    if n.where_predicate is not None:
        m.where = n.where_predicate
    if n.where_expr is not None:
        _set_where_expr(m, n.where_expr, n.span, ctx)
    if n.capture is not None:
        m.capture = n.capture
    return m


def _chain(matchers_iter) -> LNode | None:
    matchers = [m for m in matchers_iter if m is not None]
    if not matchers:
        return None
    first = matchers[0]
    current = first
    for nxt in matchers[1:]:
        _attach_as_child(current, nxt)
        current = nxt
    return first


def _attach_as_child(host: LNode, child: LNode) -> None:
    if host.child is None:
        host.child = child
    else:
        _attach_as_child(host.child, child)


def _attach_as_parent(host: LNode, parent: LNode) -> None:
    if host.parent is None:
        host.parent = parent
    else:
        _attach_as_parent(host.parent, parent)


def _check_no_capture_after_recursion(
    pattern: ast_.PatternExpr,
    decl_span: Span,
    ctx: CompileCtx,
) -> None:
    """Reject captures after a direct-recursive reference in the same sequence.

    The walker cannot reach them after engine deduplication.
    """

    def is_direct_recursive_ref(p: ast_.PatternExpr) -> bool:
        if isinstance(p, ast_.RuleRef):
            return ctx.resolved.rule_recursion.get(p.name) is RecursionClass.DIRECT_RECURSIVE
        if isinstance(p, (ast_.Maybe, ast_.Peek)):
            return is_direct_recursive_ref(p.inner)
        return False

    def contains_capture(p: ast_.PatternExpr) -> bool:
        if isinstance(p, ast_.Node):
            return p.capture is not None
        if isinstance(p, (ast_.Maybe, ast_.Peek)):
            return contains_capture(p.inner)
        if isinstance(p, ast_.Sequence):
            if contains_capture(p.head):
                return True
            return any(contains_capture(a) for _e, a in p.tail)
        if isinstance(p, ast_.Alternative):
            return any(contains_capture(b) for b in p.branches)
        if isinstance(p, ast_.ChildrenBlock):
            return any(contains_capture(it) for it in p.items)
        if isinstance(p, ast_.RuleRef):
            target = next((r for r in ctx.resolved.raw.rules if r.name == p.name), None)
            return target is not None and contains_capture(target.pattern)
        return False

    def walk(p: ast_.PatternExpr) -> None:
        if isinstance(p, ast_.Sequence):
            recursion_seen = False
            atoms = [(None, p.head)] + list(p.tail)
            for _edge, atom in atoms:
                if recursion_seen and contains_capture(atom):
                    ctx.bag.error(
                        "C007_CAPTURE_AFTER_RECURSION",
                        "captures placed after a recursive-rule reference in the "
                        "same sequence are not bound at runtime; restructure the pattern",
                        atom.span if hasattr(atom, "span") else decl_span,
                    )
                if is_direct_recursive_ref(atom):
                    recursion_seen = True
                walk(atom)
        elif isinstance(p, (ast_.Maybe, ast_.Peek)):
            walk(p.inner)
        elif isinstance(p, ast_.Alternative):
            for b in p.branches:
                walk(b)
        elif isinstance(p, ast_.ChildrenBlock):
            for it in p.items:
                walk(it)

    walk(pattern)


def _check_no_bare_children_block(
    pattern: ast_.PatternExpr,
    ctx: CompileCtx,
) -> None:
    """Report C008 for a children block without a node on its left.
    """

    def is_modifier_wrapped_cb(p: ast_.PatternExpr) -> tuple[bool, ast_.Span]:
        inner, _optional, _peek, _peek_span = _unwrap_modifiers(p)
        if isinstance(inner, ast_.ChildrenBlock):
            return True, inner.span
        return False, p.span if hasattr(p, "span") else None  # type: ignore[return-value]

    def emit_c008(span: ast_.Span) -> None:
        ctx.bag.error(
            "C008_CHILDREN_BLOCK_BARE",
            "children block '{...}' must follow a node in a sequence "
            "(e.g. 'op A -> {op B; op C}'); bare children blocks are not allowed",
            span,
        )

    def walk(p: ast_.PatternExpr, in_chain_left: bool) -> None:
        """`in_chain_left` is True when this atom has a non-CB left sibling in its
        enclosing Sequence tail (i.e. it's a valid attach-to-left position).
        """
        is_cb, span = is_modifier_wrapped_cb(p)
        if is_cb and not in_chain_left:
            emit_c008(span)
        if isinstance(p, ast_.Sequence):
            walk(p.head, in_chain_left=False)
            previous_was_atom = not is_modifier_wrapped_cb(p.head)[0]
            for _edge, atom in p.tail:
                walk(atom, in_chain_left=previous_was_atom)
                if not is_modifier_wrapped_cb(atom)[0]:
                    previous_was_atom = True
        elif isinstance(p, (ast_.Maybe, ast_.Peek)):
            inner, _optional, _peek, _peek_span = _unwrap_modifiers(p)
            if not isinstance(inner, ast_.ChildrenBlock):
                walk(p.inner, in_chain_left=False)
        elif isinstance(p, ast_.Alternative):
            for b in p.branches:
                walk(b, in_chain_left=False)
        elif isinstance(p, ast_.ChildrenBlock):
            for it in p.items:
                walk(it, in_chain_left=False)
        elif isinstance(p, ast_.RuleRef):
            # Check the rule body once at its definition site, with no chain
            # context. Its body must be self-contained or referenced from a
            # Sequence (in which case body's own bareness is independent).
            target = next((r for r in ctx.resolved.raw.rules if r.name == p.name), None)
            if target is not None and not _seen_rule(p.name, ctx):
                _mark_rule_seen(p.name, ctx)
                walk(target.pattern, in_chain_left=False)

    walk(pattern, in_chain_left=False)


_RULE_VALIDATED_KEY = "__mch_rule_validated__"


def _seen_rule(name: str, ctx: CompileCtx) -> bool:
    seen: set[str] = getattr(ctx, _RULE_VALIDATED_KEY, set())
    return name in seen


def _mark_rule_seen(name: str, ctx: CompileCtx) -> None:
    seen: set[str] = getattr(ctx, _RULE_VALIDATED_KEY, set())
    seen.add(name)
    setattr(ctx, _RULE_VALIDATED_KEY, seen)


def _is_maybe_self_ref(p: ast_.PatternExpr, rule_name: str) -> bool:
    saw_maybe = False
    while isinstance(p, (ast_.Maybe, ast_.Peek)):
        if isinstance(p, ast_.Maybe):
            saw_maybe = True
        p = p.inner
    return saw_maybe and isinstance(p, ast_.RuleRef) and p.name == rule_name


def _strip_trailing_self_ref(branch: ast_.PatternExpr, rule_name: str) -> ast_.PatternExpr | None:
    """Strip trailing `maybe $rule_name` from a step branch.

    Returns the stripped pattern, or None if the branch is just `maybe $rule_name`
    alone (the branch carries no actual content and should be discarded).
    """
    if _is_maybe_self_ref(branch, rule_name):
        return None
    if isinstance(branch, ast_.Sequence):
        if branch.tail:
            last_edge, last_atom = branch.tail[-1]
            if _is_maybe_self_ref(last_atom, rule_name):
                new_tail = branch.tail[:-1]
                if not new_tail:
                    return branch.head
                return ast_.Sequence(head=branch.head, tail=new_tail, span=branch.span)
    return branch


# Dynamic CompileCtx attributes for cyclic rules avoid changing CompileCtx:
# pending back-references collected while a cyclic body is being lowered, and
# the per-compile cache of lowered body roots (shared across reference sites).
_CYCLIC_PENDING_KEY = "_mch_cyclic_pending"   # dict[str, list[LNode]]  (cyclic_ref back-edges)
_CYCLIC_ROOTS_KEY = "_mch_cyclic_roots"       # dict[str, LNode]  (shared body roots)


def _check_cyclic_rule_body(target: ast_.RuleDecl, ctx: CompileCtx) -> None:
    """Enforce C012 shape rules for cyclic rule bodies.
    """
    name = target.name

    def is_self_ref(a: ast_.PatternExpr) -> bool:
        if isinstance(a, (ast_.Maybe, ast_.Peek)):
            return is_self_ref(a.inner)
        return isinstance(a, ast_.RuleRef) and a.name == name

    def err(msg: str, span: ast_.Span) -> None:
        ctx.bag.error("C012_CYCLIC_RULE_SHAPE", f"rule {name!r}: {msg}", span)
        raise CompileError(ctx.bag)

    def walk(p: ast_.PatternExpr) -> None:
        if isinstance(p, ast_.Node):
            if p.capture is not None:
                err(
                    f"capture @{p.capture} inside a cyclic rule body never binds; "
                    f"drive the builder from Match._mch_consumed",
                    p.span,
                )
        elif isinstance(p, ast_.Sequence):
            atoms = [p.head] + [a for _e, a in p.tail]
            for i, atom in enumerate(atoms):
                if is_self_ref(atom) and i != len(atoms) - 1:
                    err(
                        "a self-reference must be the last atom of its chain "
                        "(atoms after it would be structurally ignored)",
                        atom.span,
                    )
                walk(atom)
        elif isinstance(p, ast_.Alternative):
            for b in p.branches:
                walk(b)
        elif isinstance(p, ast_.ChildrenBlock):
            for it in p.items:
                walk(it)
        elif isinstance(p, ast_.Maybe):
            if is_self_ref(p.inner):
                err(
                    "`maybe` on a self-reference inside a cyclic body is not "
                    "supported (an IR back-edge carries no per-site optionality); "
                    "add a non-recursive alternative branch instead",
                    p.span,
                )
            walk(p.inner)
        elif isinstance(p, ast_.Peek):
            if is_self_ref(p.inner):
                err(
                    "`peek` on a self-reference inside a cyclic body is not "
                    "supported (an IR back-edge carries no per-site modifiers); "
                    "put `peek` on concrete nodes instead",
                    p.span,
                )
            walk(p.inner)
        elif isinstance(p, ast_.RuleRef):
            if p.name == name:
                return
            rc = ctx.resolved.rule_recursion.get(p.name)
            if rc in (RecursionClass.DIRECT_RECURSIVE, RecursionClass.INDIRECT_RECURSIVE):
                err(
                    f"reference to recursive rule {p.name!r} inside a cyclic "
                    f"rule body is not supported (nested recursion)",
                    p.span,
                )
            other = next((r for r in ctx.resolved.raw.rules if r.name == p.name), None)
            if other is not None:
                walk(other.pattern)  # non-recursive rules inline; hold them to the same rules

    walk(target.pattern)


def _lower_cyclic_rule(name: str, ctx: CompileCtx) -> LNode:
    """Lower a cyclic rule reference.

    The body lowers once per compile into a shared node graph; in-body
    self-references become back-edges to the body root (patched here after the
    body finishes lowering). Each reference site emits its own cyclic `recursive`
    node whose `step` is the shared body root; several sites may therefore share
    one body subgraph.
    """
    target = next(r for r in ctx.resolved.raw.rules if r.name == name)
    roots: dict[str, LNode] = getattr(ctx, _CYCLIC_ROOTS_KEY, None) or {}
    if not hasattr(ctx, _CYCLIC_ROOTS_KEY):
        setattr(ctx, _CYCLIC_ROOTS_KEY, roots)
    root = roots.get(name)
    if root is None:
        _check_cyclic_rule_body(target, ctx)
        pending: dict = getattr(ctx, _CYCLIC_PENDING_KEY, None)
        if pending is None:
            pending = {}
            setattr(ctx, _CYCLIC_PENDING_KEY, pending)
        pending[name] = []
        try:
            root = _lower_atom(target.pattern, ctx)
        finally:
            back_refs = pending.pop(name)
        if root.kind == "cyclic_ref":
            ctx.bag.error(
                "C012_CYCLIC_RULE_SHAPE",
                f"rule {name!r}: body reduces to a bare self-reference",
                target.span,
            )
            raise CompileError(ctx.bag)
        for ref in back_refs:
            ref.target = root
        roots[name] = root
    return LNode(kind="recursive", strategy="cyclic", step=root)


def _lower_recursive_rule(name: str, ctx: CompileCtx) -> LNode:
    """Lower a recursive rule to a frontier `recursive` node.

    Strip trailing `maybe $self` from step branches; leave `exit_` None when
    there are no pure-exit branches (avoids the v0 'always-one-iteration' bug
    where an always-true exit short-circuits the loop). Frontier recursion nodes
    always carry `optional=True`.
    """
    target = next(r for r in ctx.resolved.raw.rules if r.name == name)
    pattern = target.pattern

    step_branches, exit_branches = _split_recursion_branches(pattern, name)
    has_pure_exit = bool(exit_branches)

    # Strip trailing self-refs from step branches.
    stripped_step_branches: list[ast_.PatternExpr] = []
    for b in step_branches:
        s = _strip_trailing_self_ref(b, name)
        if s is not None:
            stripped_step_branches.append(s)

    if not stripped_step_branches:
        # Fallback: stripping discarded every step branch (e.g. a rule shape like
        # `op A -> ( maybe $self | op B )` where _split_recursion_branches sees
        # only the Alternative branches and the `maybe $self` is the entire step).
        # Preserve v0 behavior by keeping the original branches; the permissive
        # sentinel will fire on the self-ref. The recursion will be bounded by
        # the engine's per-iteration block exhaustion rather than the strip.
        if not step_branches:
            ctx.bag.error(
                "C001_RECURSION_SHAPE_UNSUPPORTED",
                f"rule {name!r}: no non-recursive step branch found",
                target.span,
            )
            raise CompileError(ctx.bag)
        stripped_step_branches = list(step_branches)

    ctx.lowering_recursive.add(name)
    try:
        if len(stripped_step_branches) == 1:
            step = _lower_atom(stripped_step_branches[0], ctx)
        else:
            step = LNode(
                kind="or",
                branches=[_lower_atom(b, ctx) for b in stripped_step_branches],
                exclusive=False,
            )

        if has_pure_exit:
            if len(exit_branches) == 1:
                exit_m: LNode | None = _lower_atom(exit_branches[0], ctx)
            else:
                exit_m = LNode(
                    kind="or",
                    branches=[_lower_atom(b, ctx) for b in exit_branches],
                    exclusive=False,
                )
        else:
            exit_m = None
    finally:
        ctx.lowering_recursive.discard(name)

    return LNode(kind="recursive", step=step, exit_=exit_m, optional=True)


def _split_recursion_branches(
    pattern: ast_.PatternExpr,
    rule_name: str,
) -> tuple[list[ast_.PatternExpr], list[ast_.PatternExpr]]:
    """Heuristic: separate (step, exit) branches.

    - Branches that *do not* reference `$rule_name` are exits.
    - Branches that reference `$rule_name` outside `maybe` are steps.
    - Branches whose only $rule_name reference is inside `maybe` are also steps
      (the maybe is the termination marker).

    Returns (step_branches, exit_branches). For non-Alternative patterns we return
    one step branch (the whole pattern) and no exits.
    """
    if isinstance(pattern, ast_.Alternative):
        steps: list[ast_.PatternExpr] = []
        exits: list[ast_.PatternExpr] = []
        for b in pattern.branches:
            if _contains_rule_ref(b, rule_name):
                steps.append(b)
            else:
                exits.append(b)
        return steps, exits
    # Sequence with an alternative inside its tail: search recursively.
    if isinstance(pattern, ast_.Sequence):
        # Look for an Alternative within the tail; if present, split it.
        for _e, atom in pattern.tail:
            if isinstance(atom, ast_.Alternative):
                return _split_recursion_branches(atom, rule_name)
    return [pattern], []


def _contains_rule_ref(p: ast_.PatternExpr, name: str) -> bool:
    if isinstance(p, ast_.RuleRef):
        return p.name == name
    if isinstance(p, (ast_.Maybe, ast_.Peek)):
        return _contains_rule_ref(p.inner, name)
    if isinstance(p, ast_.Sequence):
        if _contains_rule_ref(p.head, name):
            return True
        return any(_contains_rule_ref(a, name) for _e, a in p.tail)
    if isinstance(p, ast_.Alternative):
        return any(_contains_rule_ref(b, name) for b in p.branches)
    if isinstance(p, ast_.ChildrenBlock):
        return any(_contains_rule_ref(it, name) for it in p.items)
    return False
