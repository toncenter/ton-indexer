from __future__ import annotations

from typing import Iterable

from indexer.events.mch import ast_
from indexer.events.mch.diagnostics import CompileError, DiagnosticBag, Span
from indexer.events.mch.tokens import Token, TokenKind


# Build/expression keywords accepted as plain identifiers in name
# positions (capture names, let/parse targets). Some existing specs use these as
# capture names, so
# so name positions stay permissive. Structural keywords (op/btype/maybe/…) are
# deliberately NOT softened.
_SOFT_KEYWORDS: frozenset[TokenKind] = frozenset({
    TokenKind.KW_PARSE, TokenKind.KW_AS, TokenKind.KW_LET, TokenKind.KW_OUT,
    TokenKind.KW_REJECT, TokenKind.KW_FAILED, TokenKind.KW_BROKEN,
    TokenKind.KW_WHEN, TokenKind.KW_LOOKUP, TokenKind.KW_NOT,
    TokenKind.KW_AND, TokenKind.KW_OR, TokenKind.KW_NULL,
    TokenKind.KW_IF, TokenKind.KW_ELSE,
    TokenKind.KW_SHAPE,
})

_CMP_OPS: dict[TokenKind, str] = {
    TokenKind.EQEQ: "==",
    TokenKind.NEQ: "!=",
    TokenKind.LT: "<",
    TokenKind.LE: "<=",
    TokenKind.GT: ">",
    TokenKind.GE: ">=",
}

_BUILD_STMT_HEADS: frozenset[TokenKind] = frozenset({
    TokenKind.KW_PARSE, TokenKind.KW_LET, TokenKind.KW_REJECT,
    TokenKind.KW_FAILED, TokenKind.KW_BROKEN, TokenKind.KW_OUT,
    TokenKind.KW_PRODUCES,
})


class _ParserState:
    def __init__(self, tokens: list[Token], path: str, bag: DiagnosticBag):
        self.tokens = tokens
        self.path = path
        self.bag = bag
        self.i = 0

    def peek(self, offset: int = 0) -> Token:
        j = self.i + offset
        return self.tokens[j] if j < len(self.tokens) else self.tokens[-1]

    def at(self, kind: TokenKind) -> bool:
        return self.peek().kind is kind

    def eat(self, kind: TokenKind) -> Token:
        t = self.peek()
        if t.kind is not kind:
            self.bag.error(
                "P001_UNEXPECTED_TOKEN",
                f"expected {kind.name}, found {t.kind.name} ({t.text!r})",
                t.span,
            )
            raise CompileError(self.bag)
        self.i += 1
        return t

    def try_eat(self, kind: TokenKind) -> Token | None:
        if self.at(kind):
            t = self.peek()
            self.i += 1
            return t
        return None

    def skip_newlines(self) -> None:
        while self.at(TokenKind.NEWLINE):
            self.i += 1

    def eat_name(self) -> Token:
        """Eat an identifier, accepting soft keywords as plain names."""
        t = self.peek()
        if t.kind is TokenKind.IDENT or t.kind in _SOFT_KEYWORDS:
            self.i += 1
            return t
        self.bag.error(
            "P001_UNEXPECTED_TOKEN",
            f"expected identifier, found {t.kind.name} ({t.text!r})",
            t.span,
        )
        raise CompileError(self.bag)

    def eat_field_name(self) -> Token:
        """Eat a field name after '.'. The position is unambiguous, so ANY
        keyword is acceptable here (e.g. `x.btype`, `x.failed`)."""
        t = self.peek()
        if t.kind is TokenKind.IDENT or t.kind.name.startswith("KW_"):
            self.i += 1
            return t
        self.bag.error(
            "P001_UNEXPECTED_TOKEN",
            f"expected field name, found {t.kind.name} ({t.text!r})",
            t.span,
        )
        raise CompileError(self.bag)


def parse(tokens: Iterable[Token], path: str) -> ast_.File:
    tokens = list(tokens)
    bag = DiagnosticBag()
    s = _ParserState(tokens, path, bag)

    opcodes:    list[ast_.OpcodeDecl] = []
    predicates: list[ast_.PredicateDecl] = []
    rules:      list[ast_.RuleDecl] = []
    matchers:   list[ast_.MatcherDecl] = []

    s.skip_newlines()
    while not s.at(TokenKind.EOF):
        t = s.peek()
        if t.kind is TokenKind.KW_OPCODE:
            opcodes.append(_parse_opcode_decl(s))
        elif t.kind is TokenKind.KW_PREDICATE:
            predicates.append(_parse_predicate_decl(s))
        elif t.kind is TokenKind.KW_RULE:
            rules.append(_parse_rule_decl(s))
        elif t.kind is TokenKind.KW_MATCHER:
            matchers.append(_parse_matcher_decl(s))
        elif t.kind is TokenKind.KW_STITCH:
            s.bag.error(
                "P012_STITCH_REMOVED",
                "stitch was removed in 0.2; paired results are merged by host post-process code",
                t.span,
            )
            raise CompileError(bag)
        else:
            s.bag.error(
                "P001_UNEXPECTED_TOKEN",
                f"expected top-level declaration, found {t.kind.name} ({t.text!r})",
                t.span,
            )
            raise CompileError(bag)
        s.skip_newlines()

    if bag.has_errors:
        raise CompileError(bag)

    return ast_.File(
        path=path,
        opcodes=tuple(opcodes),
        predicates=tuple(predicates),
        rules=tuple(rules),
        matchers=tuple(matchers),
    )


def _parse_opcode_decl(s: _ParserState) -> ast_.OpcodeDecl:
    start = s.eat(TokenKind.KW_OPCODE).span
    name_tok = s.eat(TokenKind.IDENT)
    s.eat(TokenKind.EQ)
    value_tok = s.peek()
    if value_tok.kind is TokenKind.HEX:
        s.i += 1
        value = int(value_tok.text, 16)
    elif value_tok.kind is TokenKind.DEC:
        s.i += 1
        value = int(value_tok.text, 10)
    else:
        s.bag.error(
            "P001_UNEXPECTED_TOKEN",
            f"expected numeric literal, found {value_tok.kind.name}",
            value_tok.span,
        )
        raise CompileError(s.bag)
    return ast_.OpcodeDecl(name=name_tok.text, value=value, span=_join(start, value_tok.span))


def _parse_predicate_decl(s: _ParserState) -> ast_.PredicateDecl:
    start = s.eat(TokenKind.KW_PREDICATE).span
    name_tok = s.eat(TokenKind.IDENT)
    return ast_.PredicateDecl(name=name_tok.text, span=_join(start, name_tok.span))


def _parse_rule_decl(s: _ParserState) -> ast_.RuleDecl:
    start = s.eat(TokenKind.KW_RULE).span
    name_tok = s.eat(TokenKind.IDENT)
    # Contextual modifier (not a reserved word): `rule NAME cyclic = …` selects
    # the cyclic-descent recursion strategy.
    strategy = "frontier"
    if s.at(TokenKind.IDENT) and s.peek().text == "cyclic":
        s.eat(TokenKind.IDENT)
        strategy = "cyclic"
    s.eat(TokenKind.EQ)
    s.skip_newlines()
    pattern = _parse_pattern_expr(s)
    return ast_.RuleDecl(name=name_tok.text, pattern=pattern,
                         span=_join(start, pattern.span), strategy=strategy)


def _parse_matcher_decl(s: _ParserState) -> ast_.MatcherDecl:
    start = s.eat(TokenKind.KW_MATCHER).span
    name_tok = s.eat(TokenKind.IDENT)
    s.eat(TokenKind.LBRACE)
    s.skip_newlines()

    produces: tuple[str, ...] | None = None
    build:    str | None = None
    entry:    str | None = None
    shape:    str | None = None
    priority: int | None = None
    include_excess  = True
    include_bounces = True
    seen_include_directives: set[str] = set()

    while not s.at(TokenKind.EOF):
        s.skip_newlines()
        t = s.peek()
        if t.kind is TokenKind.KW_PRODUCES:
            if produces is not None:
                s.bag.error(
                    "P011_DUPLICATE_DIRECTIVE",
                    f"matcher {name_tok.text!r} declares more than one `produces` directive",
                    t.span,
                )
                raise CompileError(s.bag)
            s.i += 1
            names = [s.eat(TokenKind.IDENT).text]
            while s.at(TokenKind.PIPE):
                s.i += 1
                names.append(s.eat(TokenKind.IDENT).text)
            produces = tuple(names)
        elif t.kind is TokenKind.KW_BUILD:
            if build is not None:
                s.bag.error(
                    "P011_DUPLICATE_DIRECTIVE",
                    f"matcher {name_tok.text!r} declares more than one `build` directive",
                    t.span,
                )
                raise CompileError(s.bag)
            s.i += 1
            build = s.eat(TokenKind.IDENT).text
        elif t.kind is TokenKind.KW_ENTRY:
            if entry is not None:
                s.bag.error(
                    "P011_DUPLICATE_DIRECTIVE",
                    f"matcher {name_tok.text!r} declares more than one `entry` directive",
                    t.span,
                )
                raise CompileError(s.bag)
            s.i += 1
            s.eat(TokenKind.AT)
            entry = s.eat_name().text
        elif t.kind is TokenKind.KW_SHAPE:
            if shape is not None:
                s.bag.error(
                    "P011_DUPLICATE_DIRECTIVE",
                    f"matcher {name_tok.text!r} declares more than one `shape` directive",
                    t.span,
                )
                raise CompileError(s.bag)
            s.i += 1
            shape = s.eat(TokenKind.IDENT).text
        elif t.kind is TokenKind.KW_PRIORITY:
            if priority is not None:
                s.bag.error(
                    "P011_DUPLICATE_DIRECTIVE",
                    f"matcher {name_tok.text!r} declares more than one `priority` directive",
                    t.span,
                )
                raise CompileError(s.bag)
            s.i += 1
            priority = int(s.eat(TokenKind.DEC).text)
        elif t.kind is TokenKind.KW_INCLUDE_EXCESS:
            if "include_excess" in seen_include_directives:
                s.bag.error(
                    "P011_DUPLICATE_DIRECTIVE",
                    f"matcher {name_tok.text!r} declares more than one `include_excess` directive",
                    t.span,
                )
                raise CompileError(s.bag)
            seen_include_directives.add("include_excess")
            s.i += 1
            include_excess = _parse_bool_literal(s)
        elif t.kind is TokenKind.KW_INCLUDE_BOUNCES:
            if "include_bounces" in seen_include_directives:
                s.bag.error(
                    "P011_DUPLICATE_DIRECTIVE",
                    f"matcher {name_tok.text!r} declares more than one `include_bounces` directive",
                    t.span,
                )
                raise CompileError(s.bag)
            seen_include_directives.add("include_bounces")
            s.i += 1
            include_bounces = _parse_bool_literal(s)
        else:
            break
        s.skip_newlines()

    pattern = _parse_pattern_expr(s)
    build_stmts = _parse_build_stmts(s)
    s.skip_newlines()
    end = s.eat(TokenKind.RBRACE).span

    # `build` is optional when build statements are present in declarative form;
    # the resolver enforces the finer rules (out required, no mixing).
    if produces is None or (build is None and not build_stmts):
        s.bag.error(
            "P001_UNEXPECTED_TOKEN",
            f"matcher {name_tok.text!r} missing required directive "
            f"(produces, and either build or build statements)",
            _join(start, end),
        )
        raise CompileError(s.bag)

    return ast_.MatcherDecl(
        name=name_tok.text,
        produces=produces,
        build=build,
        entry=entry,
        include_excess=include_excess,
        include_bounces=include_bounces,
        pattern=pattern,
        span=_join(start, end),
        build_stmts=build_stmts,
        shape=shape,
        priority=100 if priority is None else priority,
    )


def _parse_bool_literal(s: _ParserState) -> bool:
    t = s.peek()
    if t.kind is TokenKind.KW_TRUE:
        s.i += 1
        return True
    if t.kind is TokenKind.KW_FALSE:
        s.i += 1
        return False
    s.bag.error("P001_UNEXPECTED_TOKEN", f"expected 'true' or 'false', found {t.text!r}", t.span)
    raise CompileError(s.bag)


def _parse_pattern_expr(s: _ParserState) -> ast_.PatternExpr:
    """pattern_expr := sequence (('|' | '^') sequence)*

    The separator kind is decided by the first separator seen.
    Mixing '|' and '^' at the same nesting level emits P010.
    """
    s.skip_newlines()
    branches = [_parse_sequence(s)]
    separator_kind: TokenKind | None = None

    while True:
        save_i = s.i
        s.skip_newlines()
        if s.at(TokenKind.PIPE) or s.at(TokenKind.CARET):
            current_sep = s.tokens[s.i].kind
            if separator_kind is None:
                separator_kind = current_sep
            elif current_sep is not separator_kind:
                offending_tok = s.tokens[s.i]
                s.bag.error(
                    "P010_ALTERNATIVE_SEPARATOR_MIX",
                    "cannot mix '|' and '^' at the same nesting level; "
                    "use parentheses to nest alternatives",
                    offending_tok.span,
                )
                raise CompileError(s.bag)
            s.i += 1  # consume separator
            s.skip_newlines()
            branches.append(_parse_sequence(s))
        else:
            s.i = save_i
            break

    if len(branches) == 1:
        return branches[0]
    span = _join(branches[0].span, branches[-1].span)
    return ast_.Alternative(
        branches=tuple(branches),
        span=span,
        exclusive=(separator_kind is TokenKind.CARET),
    )


def _parse_sequence(s: _ParserState) -> ast_.PatternExpr:
    """sequence := atom ( edge atom )*"""
    head = _parse_atom(s)
    tail: list[tuple[ast_.Edge, ast_.PatternExpr]] = []
    while True:
        save_i = s.i
        s.skip_newlines()
        t = s.peek()
        if t.kind is TokenKind.ARROW_R:
            s.i += 1
            s.skip_newlines()
            tail.append((ast_.Edge.CHILD, _parse_atom(s)))
        elif t.kind is TokenKind.ARROW_L:
            s.i += 1
            s.skip_newlines()
            tail.append((ast_.Edge.PARENT, _parse_atom(s)))
        else:
            s.i = save_i
            break
    if not tail:
        return head
    last_span = tail[-1][1].span
    return ast_.Sequence(head=head, tail=tuple(tail), span=_join(head.span, last_span))


def _parse_atom(s: _ParserState) -> ast_.PatternExpr:
    """atom := node | '(' pattern_expr ')' | rule_ref | children_block
               | ('maybe' | 'peek') atom
    """
    t = s.peek()

    if t.kind is TokenKind.KW_MAYBE:
        s.i += 1
        s.skip_newlines()
        inner = _parse_atom(s)
        return ast_.Maybe(inner=inner, span=_join(t.span, inner.span))

    if t.kind is TokenKind.KW_PEEK:
        s.i += 1
        s.skip_newlines()
        inner = _parse_atom(s)
        return ast_.Peek(inner=inner, span=_join(t.span, inner.span))

    if t.kind is TokenKind.LPAREN:
        s.i += 1
        s.skip_newlines()
        inner = _parse_pattern_expr(s)
        s.skip_newlines()
        end = s.eat(TokenKind.RPAREN).span
        if isinstance(inner, ast_.Alternative):
            return ast_.Alternative(branches=inner.branches, span=_join(t.span, end), exclusive=inner.exclusive)
        if isinstance(inner, ast_.Sequence):
            return ast_.Sequence(head=inner.head, tail=inner.tail, span=_join(t.span, end))
        return inner

    if t.kind is TokenKind.LBRACE:
        return _parse_children_block(s)

    if t.kind is TokenKind.DOLLAR:
        s.i += 1
        name_tok = s.eat(TokenKind.IDENT)
        return ast_.RuleRef(name=name_tok.text, span=_join(t.span, name_tok.span))

    return _parse_node(s)


def _parse_node(s: _ParserState) -> ast_.Node:
    """node := node_head capture? predicate_clause?"""
    t = s.peek()
    head: ast_.NodeHead
    head_start_span = t.span

    if t.kind is TokenKind.KW_OP:
        s.i += 1
        ref_tok = s.peek()
        if ref_tok.kind is TokenKind.IDENT:
            s.i += 1
            head = ast_.OpHead(ref=ref_tok.text, span=_join(t.span, ref_tok.span))
        elif ref_tok.kind is TokenKind.HEX:
            s.i += 1
            head = ast_.OpHead(ref=int(ref_tok.text, 16), span=_join(t.span, ref_tok.span))
        elif ref_tok.kind is TokenKind.DEC:
            s.i += 1
            head = ast_.OpHead(ref=int(ref_tok.text, 10), span=_join(t.span, ref_tok.span))
        else:
            s.bag.error("P001_UNEXPECTED_TOKEN", f"expected identifier or numeric literal after 'op'", ref_tok.span)
            raise CompileError(s.bag)
    elif t.kind is TokenKind.KW_BTYPE:
        s.i += 1
        name_tok = s.eat(TokenKind.IDENT)
        head = ast_.BTypeHead(name=name_tok.text, span=_join(t.span, name_tok.span))
    elif t.kind is TokenKind.KW_PRED:
        s.i += 1
        name_tok = s.eat(TokenKind.IDENT)
        head = ast_.PredHead(name=name_tok.text, span=_join(t.span, name_tok.span))
    elif t.kind is TokenKind.KW_ANY:
        s.i += 1
        head = ast_.AnyHead(span=t.span)
    else:
        s.bag.error("P001_UNEXPECTED_TOKEN", f"expected node head, found {t.kind.name}", t.span)
        raise CompileError(s.bag)

    capture: str | None = None
    if s.at(TokenKind.AT):
        s.i += 1
        cap_tok = s.eat_name()
        capture = cap_tok.text
        head_start_span = _join(head_start_span, cap_tok.span)

    where_predicate: str | None = None
    where_expr: ast_.Expr | None = None
    if s.at(TokenKind.KW_WHERE):
        s.i += 1
        if s.at(TokenKind.LPAREN):
            s.i += 1
            s.skip_newlines()
            where_expr = _parse_expr(s)
            s.skip_newlines()
            rp = s.eat(TokenKind.RPAREN)
            head_start_span = _join(head_start_span, rp.span)
        else:
            pred_tok = s.eat(TokenKind.IDENT)
            where_predicate = pred_tok.text
            head_start_span = _join(head_start_span, pred_tok.span)

    return ast_.Node(
        head=head,
        capture=capture,
        where_predicate=where_predicate,
        span=head_start_span,
        where_expr=where_expr,
    )


def _parse_children_block(s: _ParserState) -> ast_.ChildrenBlock:
    start = s.eat(TokenKind.LBRACE).span
    items: list[ast_.PatternExpr] = []
    while True:
        while s.at(TokenKind.SEMI) or s.at(TokenKind.NEWLINE):
            s.i += 1
        if s.at(TokenKind.RBRACE):
            break
        if s.at(TokenKind.EOF):
            s.bag.error("P001_UNEXPECTED_TOKEN", "unterminated children block", start)
            raise CompileError(s.bag)
        item = _parse_pattern_expr(s)
        items.append(item)
    end = s.eat(TokenKind.RBRACE).span
    if not items:
        s.bag.error("P002_EMPTY_CHILDREN_ITEM", "children block must contain at least one item", _join(start, end))
        raise CompileError(s.bag)
    return ast_.ChildrenBlock(items=tuple(items), span=_join(start, end))


# Build statements


def _parse_build_stmts(s: _ParserState) -> tuple[ast_.BuildStmt, ...]:
    """Zero or more build statements after the pattern, newline/`;`-separated."""
    stmts: list[ast_.BuildStmt] = []
    while True:
        while s.at(TokenKind.NEWLINE) or s.at(TokenKind.SEMI):
            s.i += 1
        t = s.peek()
        if t.kind not in _BUILD_STMT_HEADS:
            break
        if t.kind is TokenKind.KW_PARSE:
            stmts.append(_parse_parse_stmt(s))
        elif t.kind is TokenKind.KW_LET:
            stmts.append(_parse_let_stmt(s))
        elif t.kind is TokenKind.KW_REJECT:
            stmts.append(_parse_guard_stmt(s, TokenKind.KW_REJECT, ast_.RejectStmt))
        elif t.kind is TokenKind.KW_FAILED:
            stmts.append(_parse_guard_stmt(s, TokenKind.KW_FAILED, ast_.FailedStmt))
        elif t.kind is TokenKind.KW_BROKEN:
            stmts.append(_parse_guard_stmt(s, TokenKind.KW_BROKEN, ast_.BrokenStmt))
        elif t.kind is TokenKind.KW_OUT:
            stmts.append(_parse_out_stmt(s))
        elif t.kind is TokenKind.KW_PRODUCES:
            stmts.append(_parse_switch_stmt(s))
    return tuple(stmts)


def _parse_parse_stmt(s: _ParserState) -> ast_.ParseStmt:
    start = s.eat(TokenKind.KW_PARSE).span
    cap = s.eat_name()
    s.eat(TokenKind.KW_AS)
    msg = s.eat(TokenKind.IDENT)
    types = [msg.text]
    end_span = msg.span
    while s.at(TokenKind.PIPE):
        s.i += 1
        alt = s.eat(TokenKind.IDENT)
        types.append(alt.text)
        end_span = alt.span
    return ast_.ParseStmt(capture=cap.text, msg_types=tuple(types), span=_join(start, end_span))


def _parse_let_stmt(s: _ParserState) -> ast_.LetStmt:
    start = s.eat(TokenKind.KW_LET).span
    name = s.eat_name()
    s.eat(TokenKind.EQ)
    value = _parse_expr(s)
    return ast_.LetStmt(name=name.text, value=value, span=_join(start, value.span))


def _parse_guard_stmt(s: _ParserState, kw: TokenKind, cls) -> ast_.BuildStmt:
    """`reject|failed|broken when expr`."""
    start = s.eat(kw).span
    s.eat(TokenKind.KW_WHEN)
    cond = _parse_expr(s)
    return cls(condition=cond, span=_join(start, cond.span))


def _parse_out_stmt(s: _ParserState) -> ast_.OutStmt:
    start = s.eat(TokenKind.KW_OUT).span
    s.eat(TokenKind.LBRACE)
    fields: list[ast_.OutField] = []
    while True:
        while s.at(TokenKind.NEWLINE) or s.at(TokenKind.SEMI) or s.at(TokenKind.COMMA):
            s.i += 1
        if s.at(TokenKind.RBRACE):
            break
        if s.at(TokenKind.EOF):
            s.bag.error("P001_UNEXPECTED_TOKEN", "unterminated out block", start)
            raise CompileError(s.bag)
        fname = s.eat_field_name()
        optional = False
        if s.at(TokenKind.QUESTION):
            s.i += 1
            optional = True
        s.eat(TokenKind.COLON)
        s.skip_newlines()
        value = _parse_expr(s)
        fields.append(ast_.OutField(
            name=fname.text, value=value, optional=optional,
            span=_join(fname.span, value.span),
        ))
    end = s.eat(TokenKind.RBRACE).span
    if not fields:
        s.bag.error("P002_EMPTY_CHILDREN_ITEM", "out block must contain at least one field", _join(start, end))
        raise CompileError(s.bag)
    return ast_.OutStmt(fields=tuple(fields), span=_join(start, end))


def _parse_switch_stmt(s: _ParserState) -> ast_.SwitchStmt:
    """`produces switch { when EXPR => BTYPE out {…} … else => BTYPE out {…} }`."""
    start = s.eat(TokenKind.KW_PRODUCES).span
    s.eat(TokenKind.KW_SWITCH)
    s.eat(TokenKind.LBRACE)
    branches: list[ast_.SwitchBranch] = []
    while True:
        while s.at(TokenKind.NEWLINE) or s.at(TokenKind.SEMI):
            s.i += 1
        if s.at(TokenKind.RBRACE):
            break
        if s.at(TokenKind.EOF):
            s.bag.error("P001_UNEXPECTED_TOKEN", "unterminated produces switch", start)
            raise CompileError(s.bag)
        b_start = s.peek().span
        if s.at(TokenKind.KW_WHEN):
            s.i += 1
            cond: ast_.Expr | None = _parse_expr(s)
        elif s.at(TokenKind.KW_ELSE):
            s.i += 1
            cond = None
        else:
            t = s.peek()
            s.bag.error("P001_UNEXPECTED_TOKEN",
                        f"produces switch: expected `when` or `else`, found {t.kind.name}", t.span)
            raise CompileError(s.bag)
        s.eat(TokenKind.FATARROW)
        btype = s.eat(TokenKind.IDENT)
        out = _parse_out_stmt(s)
        branches.append(ast_.SwitchBranch(
            condition=cond, btype=btype.text, out=out,
            span=_join(b_start, out.span),
        ))
    end = s.eat(TokenKind.RBRACE).span
    if not branches:
        s.bag.error("P002_EMPTY_CHILDREN_ITEM", "produces switch must have at least one branch", _join(start, end))
        raise CompileError(s.bag)
    return ast_.SwitchStmt(branches=tuple(branches), span=_join(start, end))


# Expression language
#
# Precedence, low -> high:
#   ternary ('A if C else B', right-assoc) -> or -> and -> not(prefix)
#      -> comparison(non-assoc) -> ??(coalesce)
#      -> unary '-' -> postfix(.field / call) -> primary


def _parse_expr(s: _ParserState) -> ast_.Expr:
    return _parse_ternary(s)


def _parse_ternary(s: _ParserState) -> ast_.Expr:
    """`then if cond else orelse` (Python-style). Right-associative: the else
    branch may itself be a ternary."""
    then = _parse_or(s)
    if not s.at(TokenKind.KW_IF):
        return then
    s.i += 1
    cond = _parse_or(s)
    s.eat(TokenKind.KW_ELSE)
    orelse = _parse_ternary(s)
    return ast_.Ternary(cond=cond, then=then, orelse=orelse, span=_join(then.span, orelse.span))


def _parse_or(s: _ParserState) -> ast_.Expr:
    left = _parse_and(s)
    while s.at(TokenKind.KW_OR):
        s.i += 1
        right = _parse_and(s)
        left = ast_.BinaryOp(op="or", left=left, right=right, span=_join(left.span, right.span))
    return left


def _parse_and(s: _ParserState) -> ast_.Expr:
    left = _parse_not(s)
    while s.at(TokenKind.KW_AND):
        s.i += 1
        right = _parse_not(s)
        left = ast_.BinaryOp(op="and", left=left, right=right, span=_join(left.span, right.span))
    return left


def _parse_not(s: _ParserState) -> ast_.Expr:
    if s.at(TokenKind.KW_NOT):
        tok = s.peek()
        s.i += 1
        operand = _parse_not(s)
        return ast_.UnaryOp(op="not", operand=operand, span=_join(tok.span, operand.span))
    return _parse_cmp(s)


def _parse_cmp(s: _ParserState) -> ast_.Expr:
    left = _parse_additive(s)
    op = _CMP_OPS.get(s.peek().kind)
    if op is not None:
        s.i += 1
        right = _parse_additive(s)  # non-associative: exactly one comparison
        return ast_.BinaryOp(op=op, left=left, right=right, span=_join(left.span, right.span))
    return left


def _parse_additive(s: _ParserState) -> ast_.Expr:
    """`+` / `-` (left-associative); binds tighter than comparison, looser than
    `*`. A leading `-` is unary and belongs to `_parse_unary`."""
    left = _parse_multiplicative(s)
    while s.at(TokenKind.PLUS) or s.at(TokenKind.MINUS):
        op = "+" if s.at(TokenKind.PLUS) else "-"
        s.i += 1
        right = _parse_multiplicative(s)
        left = ast_.BinaryOp(op=op, left=left, right=right, span=_join(left.span, right.span))
    return left


def _parse_multiplicative(s: _ParserState) -> ast_.Expr:
    """`*` (left-associative); the tightest binary arithmetic level."""
    left = _parse_coalesce(s)
    while s.at(TokenKind.STAR):
        s.i += 1
        right = _parse_coalesce(s)
        left = ast_.BinaryOp(op="*", left=left, right=right, span=_join(left.span, right.span))
    return left


def _parse_coalesce(s: _ParserState) -> ast_.Expr:
    left = _parse_unary(s)
    while s.at(TokenKind.COALESCE):
        s.i += 1
        right = _parse_unary(s)
        left = ast_.BinaryOp(op="??", left=left, right=right, span=_join(left.span, right.span))
    return left


def _parse_unary(s: _ParserState) -> ast_.Expr:
    if s.at(TokenKind.MINUS):
        tok = s.peek()
        s.i += 1
        operand = _parse_unary(s)
        return ast_.UnaryOp(op="-", operand=operand, span=_join(tok.span, operand.span))
    return _parse_postfix(s)


def _parse_postfix(s: _ParserState) -> ast_.Expr:
    e = _parse_primary(s)
    while True:
        if s.at(TokenKind.DOT):
            s.i += 1
            name = s.eat_field_name()
            e = ast_.FieldAccess(target=e, field=name.text, span=_join(e.span, name.span))
        elif s.at(TokenKind.LPAREN):
            s.i += 1
            args, end = _parse_arg_list(s)
            e = ast_.Call(callee=e, args=args, span=_join(e.span, end))
        else:
            break
    return e


def _parse_arg_list(s: _ParserState) -> tuple[tuple[ast_.Expr, ...], Span]:
    """Parse `expr (, expr)*` up to and including the closing ')'."""
    s.skip_newlines()
    args: list[ast_.Expr] = []
    if not s.at(TokenKind.RPAREN):
        args.append(_parse_expr(s))
        while s.at(TokenKind.COMMA):
            s.i += 1
            s.skip_newlines()
            args.append(_parse_expr(s))
    s.skip_newlines()
    end = s.eat(TokenKind.RPAREN).span
    return tuple(args), end


# `any` lexes to KW_ANY (the node-head keyword), `map`/`all` stay plain
# identifiers.
def _at_comprehension(s: _ParserState) -> bool:
    """True at `map|any|all ( … as … => … )`, the lambda comprehension form,
    told apart from the `map(xs, "f")` builtin call by an `as` keyword at the
    argument list's top parenthesis depth."""
    t = s.peek()
    if t.kind is TokenKind.KW_ANY:
        pass
    elif t.kind is TokenKind.IDENT and t.text in ("map", "all"):
        pass
    else:
        return False
    if s.peek(1).kind is not TokenKind.LPAREN:
        return False
    depth = 0
    j = 1
    while True:
        k = s.peek(j).kind
        if k is TokenKind.EOF:
            return False
        if k in (TokenKind.LPAREN, TokenKind.LBRACKET, TokenKind.LBRACE):
            depth += 1
        elif k in (TokenKind.RPAREN, TokenKind.RBRACKET, TokenKind.RBRACE):
            depth -= 1
            if depth == 0:
                return False
        elif k is TokenKind.KW_AS and depth == 1:
            return True
        j += 1


def _parse_comprehension(s: _ParserState) -> ast_.Comprehension:
    """`map|any|all ( xs as VAR => body )`. VAR binds one element,
    visible only in `body`."""
    head = s.peek()
    kind = "any" if head.kind is TokenKind.KW_ANY else head.text  # map | all
    s.i += 1
    s.eat(TokenKind.LPAREN)
    s.skip_newlines()
    xs = _parse_expr(s)
    s.eat(TokenKind.KW_AS)
    var_tok = s.eat(TokenKind.IDENT)
    s.eat(TokenKind.FATARROW)
    s.skip_newlines()
    body = _parse_expr(s)
    s.skip_newlines()
    end = s.eat(TokenKind.RPAREN).span
    return ast_.Comprehension(kind=kind, xs=xs, var=var_tok.text, body=body,
                              span=_join(head.span, end))


def _parse_parse_expr(s: _ParserState, *, nullable: bool = False) -> ast_.ParseExpr:
    """`[try] parse <target> as T (| T)*` in expression position."""
    start = s.eat(TokenKind.KW_PARSE).span
    target = _parse_postfix(s)
    s.eat(TokenKind.KW_AS)
    msg = s.eat(TokenKind.IDENT)
    types = [msg.text]
    end = msg.span
    while s.at(TokenKind.PIPE):
        s.i += 1
        alt = s.eat(TokenKind.IDENT)
        types.append(alt.text)
        end = alt.span
    return ast_.ParseExpr(
        target=target,
        msg_types=tuple(types),
        span=_join(start, end),
        nullable=nullable,
    )


def _parse_primary(s: _ParserState) -> ast_.Expr:
    if _at_comprehension(s):
        return _parse_comprehension(s)
    if s.at(TokenKind.KW_TRY):
        start = s.eat(TokenKind.KW_TRY).span
        parsed = _parse_parse_expr(s, nullable=True)
        return ast_.ParseExpr(
            target=parsed.target,
            msg_types=parsed.msg_types,
            span=_join(start, parsed.span),
            nullable=True,
        )
    if s.at(TokenKind.KW_PARSE):
        return _parse_parse_expr(s)
    t = s.peek()
    if t.kind is TokenKind.HEX:
        s.i += 1
        return ast_.IntLit(value=int(t.text, 16), span=t.span)
    if t.kind is TokenKind.DEC:
        s.i += 1
        return ast_.IntLit(value=int(t.text, 10), span=t.span)
    if t.kind is TokenKind.STRING:
        s.i += 1
        return ast_.StrLit(value=_decode_string(t.text), span=t.span)
    if t.kind is TokenKind.KW_TRUE:
        s.i += 1
        return ast_.BoolLit(value=True, span=t.span)
    if t.kind is TokenKind.KW_FALSE:
        s.i += 1
        return ast_.BoolLit(value=False, span=t.span)
    if t.kind is TokenKind.KW_NULL:
        s.i += 1
        return ast_.NullLit(span=t.span)
    if t.kind is TokenKind.KW_LOOKUP:
        return _parse_lookup(s)
    if t.kind is TokenKind.DOT:
        s.i += 1
        name = s.eat_field_name()
        return ast_.FieldRef(field=name.text, span=_join(t.span, name.span))
    if t.kind is TokenKind.LPAREN:
        s.i += 1
        s.skip_newlines()
        inner = _parse_expr(s)
        s.skip_newlines()
        s.eat(TokenKind.RPAREN)
        return inner
    if t.kind is TokenKind.LBRACKET:
        return _parse_list_lit(s)
    if t.kind is TokenKind.LBRACE:
        return _parse_record_lit(s)
    if t.kind is TokenKind.IDENT:
        s.i += 1
        return ast_.NameRef(name=t.text, span=t.span)
    s.bag.error("P001_UNEXPECTED_TOKEN", f"expected expression, found {t.kind.name} ({t.text!r})", t.span)
    raise CompileError(s.bag)


def _parse_lookup(s: _ParserState) -> ast_.LookupExpr:
    start = s.eat(TokenKind.KW_LOOKUP).span
    kind = s.eat(TokenKind.IDENT)
    s.eat(TokenKind.LPAREN)
    args, end = _parse_arg_list(s)
    return ast_.LookupExpr(kind=kind.text, args=args, span=_join(start, end))


def _parse_list_lit(s: _ParserState) -> ast_.ListLit:
    """Parse a list literal, allowing a trailing comma and newlines."""
    start = s.eat(TokenKind.LBRACKET).span
    elements: list[ast_.Expr] = []
    s.skip_newlines()
    if not s.at(TokenKind.RBRACKET):
        elements.append(_parse_expr(s))
        while True:
            s.skip_newlines()
            if not s.at(TokenKind.COMMA):
                break
            s.i += 1
            s.skip_newlines()
            if s.at(TokenKind.RBRACKET):
                break  # trailing comma
            elements.append(_parse_expr(s))
    s.skip_newlines()
    end = s.eat(TokenKind.RBRACKET).span
    return ast_.ListLit(elements=tuple(elements), span=_join(start, end))


def _parse_record_lit(s: _ParserState) -> ast_.RecordLit:
    """Parse a record literal. Fields are separated by
    comma/newline/`;` (same convention as `out { }`)."""
    start = s.eat(TokenKind.LBRACE).span
    fields: list[ast_.RecordField] = []
    while True:
        while s.at(TokenKind.NEWLINE) or s.at(TokenKind.SEMI) or s.at(TokenKind.COMMA):
            s.i += 1
        if s.at(TokenKind.RBRACE):
            break
        if s.at(TokenKind.EOF):
            s.bag.error("P001_UNEXPECTED_TOKEN", "unterminated record literal", start)
            raise CompileError(s.bag)
        fname = s.eat_field_name()
        s.eat(TokenKind.COLON)
        s.skip_newlines()
        value = _parse_expr(s)
        fields.append(ast_.RecordField(name=fname.text, value=value, span=_join(fname.span, value.span)))
    end = s.eat(TokenKind.RBRACE).span
    return ast_.RecordLit(fields=tuple(fields), span=_join(start, end))


def _decode_string(text: str) -> str:
    """Strip surrounding quotes and unescape `\\"` / `\\\\` from a STRING token."""
    body = text[1:-1] if len(text) >= 2 else text
    out: list[str] = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "\\" and i + 1 < len(body):
            out.append(body[i + 1])
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _join(a: Span, b: Span) -> Span:
    return Span(
        path=a.path,
        start_line=a.start_line,
        start_col=a.start_col,
        end_line=b.end_line,
        end_col=b.end_col,
        start_off=a.start_off,
        end_off=b.end_off,
    )
