from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from indexer.events.mch.diagnostics import Span


class TokenKind(Enum):
    # literals
    IDENT = "IDENT"
    HEX = "HEX"
    DEC = "DEC"
    DURATION = "DURATION"
    STRING = "STRING"
    # declaration keywords
    KW_OPCODE = "opcode"
    KW_PREDICATE = "predicate"
    KW_RULE = "rule"
    KW_MATCHER = "matcher"
    KW_STITCH = "stitch"
    # node-head keywords
    KW_OP = "op"
    KW_BTYPE = "btype"
    KW_PRED = "pred"
    KW_ANY = "any"
    # modifier / directive keywords
    KW_MAYBE = "maybe"
    KW_WHERE = "where"
    KW_PRODUCES = "produces"
    KW_SWITCH = "switch"
    KW_PRIORITY = "priority"
    KW_BUILD = "build"
    KW_ENTRY = "entry"
    KW_INCLUDE_EXCESS = "include_excess"
    KW_INCLUDE_BOUNCES = "include_bounces"
    KW_SHAPE = "shape"
    # Build-language keywords parsed here and ignored by the pattern compiler.
    KW_PARSE = "parse"
    KW_AS = "as"
    KW_LET = "let"
    KW_OUT = "out"
    KW_REJECT = "reject"
    KW_FAILED = "failed"
    KW_BROKEN = "broken"
    KW_WHEN = "when"
    KW_LOOKUP = "lookup"
    KW_IF = "if"
    KW_ELSE = "else"
    # expression operator keywords
    KW_NOT = "not"
    KW_AND = "and"
    KW_OR = "or"
    # Stitch keywords remain reserved so the parser can emit
    # P012_STITCH_REMOVED.
    KW_PARTIAL = "partial"
    KW_FINAL = "final"
    KW_KEY = "key"
    KW_TTL = "ttl"
    KW_MERGE = "merge"
    # boolean / null literals
    KW_TRUE = "true"
    KW_FALSE = "false"
    KW_NULL = "null"
    # punctuation
    ARROW_R = "->"
    ARROW_L = "<-"
    PIPE = "|"
    CARET = "^"
    AT = "@"
    DOLLAR = "$"
    LBRACE = "{"
    RBRACE = "}"
    LBRACKET = "["
    RBRACKET = "]"
    LPAREN = "("
    RPAREN = ")"
    EQ = "="
    DOT = "."
    SEMI = ";"
    COMMA = ","
    COLON = ":"
    # Expression operators.
    EQEQ = "=="
    NEQ = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    COALESCE = "??"
    MINUS = "-"
    PLUS = "+"
    STAR = "*"
    FATARROW = "=>"
    QUESTION = "?"
    # control
    NEWLINE = "NEWLINE"
    EOF = "EOF"


# Mapping from keyword text to TokenKind, for the lexer's identifier-keyword promotion.
KEYWORDS: dict[str, TokenKind] = {
    k.value: k
    for k in TokenKind
    if k.name.startswith("KW_")
}


@dataclass(frozen=True)
class Token:
    kind: TokenKind
    text: str
    span: Span
