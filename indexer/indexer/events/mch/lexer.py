from __future__ import annotations

from typing import Iterator

from indexer.events.mch.diagnostics import Span
from indexer.events.mch.tokens import KEYWORDS, Token, TokenKind


_SINGLE_CHAR: dict[str, TokenKind] = {
    "|": TokenKind.PIPE,
    "^": TokenKind.CARET,
    "@": TokenKind.AT,
    "$": TokenKind.DOLLAR,
    "{": TokenKind.LBRACE,
    "}": TokenKind.RBRACE,
    "[": TokenKind.LBRACKET,
    "]": TokenKind.RBRACKET,
    "+": TokenKind.PLUS,
    "*": TokenKind.STAR,
    "(": TokenKind.LPAREN,
    ")": TokenKind.RPAREN,
    ".": TokenKind.DOT,
    ";": TokenKind.SEMI,
    ",": TokenKind.COMMA,
    ":": TokenKind.COLON,
}

_DURATION_UNITS = set("smhd")


class LexerError(Exception):
    def __init__(self, code: str, message: str, span: Span):
        super().__init__(message)
        self.code = code
        self.message = message
        self.span = span


class _Cursor:
    def __init__(self, text: str, path: str):
        self.text = text
        self.path = path
        self.i = 0
        self.line = 1
        self.col = 1

    def at_end(self) -> bool:
        return self.i >= len(self.text)

    def peek(self, offset: int = 0) -> str:
        j = self.i + offset
        return self.text[j] if j < len(self.text) else ""

    def advance(self) -> str:
        ch = self.text[self.i]
        self.i += 1
        if ch == "\n":
            self.line += 1
            self.col = 1
        else:
            self.col += 1
        return ch

    def span_from(self, start_i: int, start_line: int, start_col: int) -> Span:
        return Span(
            path=self.path,
            start_line=start_line,
            start_col=start_col,
            end_line=self.line,
            end_col=self.col,
            start_off=start_i,
            end_off=self.i,
        )


def tokenize(text: str, path: str) -> Iterator[Token]:
    cur = _Cursor(text, path)

    while not cur.at_end():
        ch = cur.peek()

        # Comment: consume to (but not including) newline; the newline becomes a NEWLINE token below.
        if ch == "#":
            while not cur.at_end() and cur.peek() != "\n":
                cur.advance()
            continue

        # Newlines become NEWLINE tokens.
        if ch == "\n":
            start_i, start_line, start_col = cur.i, cur.line, cur.col
            cur.advance()
            yield Token(TokenKind.NEWLINE, "\n", cur.span_from(start_i, start_line, start_col))
            continue

        # Other whitespace consumed silently.
        if ch in " \t\r":
            cur.advance()
            continue

        # Operators sharing a lead char with a longer form. Longest match wins;
        # `->`/`<-` (edges) stay distinct from `-` (minus) and `<`/`<=` (compare).
        if ch in "-<>=!?":
            yield _read_operator(cur)
            continue

        # Single-char punctuation.
        if ch in _SINGLE_CHAR:
            start_i, start_line, start_col = cur.i, cur.line, cur.col
            cur.advance()
            yield Token(_SINGLE_CHAR[ch], ch, cur.span_from(start_i, start_line, start_col))
            continue

        # Numeric: hex, decimal, or duration.
        if ch.isdigit():
            yield _read_number(cur)
            continue

        # Identifier (and keyword promotion).
        if ch.isalpha() or ch == "_":
            yield _read_identifier_or_keyword(cur)
            continue

        # String literal — reserved, not used in patterns; accept and yield as STRING.
        if ch == '"':
            yield _read_string(cur)
            continue

        start_i, start_line, start_col = cur.i, cur.line, cur.col
        cur.advance()
        raise LexerError(
            code="L002_UNKNOWN_CHAR",
            message=f"unexpected character {ch!r}",
            span=cur.span_from(start_i, start_line, start_col),
        )

    yield Token(TokenKind.EOF, "", cur.span_from(cur.i, cur.line, cur.col))


# Two-char operators keyed by (lead, follow); the follow char must match exactly.
_TWO_CHAR_OPS: dict[tuple[str, str], TokenKind] = {
    ("-", ">"): TokenKind.ARROW_R,
    ("<", "-"): TokenKind.ARROW_L,
    ("<", "="): TokenKind.LE,
    (">", "="): TokenKind.GE,
    ("=", "="): TokenKind.EQEQ,
    ("=", ">"): TokenKind.FATARROW,
    ("!", "="): TokenKind.NEQ,
    ("?", "?"): TokenKind.COALESCE,
}
# Single-char fallbacks; a leading char with no two-char match and no entry here
# is a lexer error (bare `!`, bare `?`).
_ONE_CHAR_OPS: dict[str, TokenKind] = {
    "-": TokenKind.MINUS,
    "<": TokenKind.LT,
    ">": TokenKind.GT,
    "=": TokenKind.EQ,
    "?": TokenKind.QUESTION,
}


def _read_operator(cur: _Cursor) -> Token:
    start_i, start_line, start_col = cur.i, cur.line, cur.col
    lead = cur.peek()
    two = _TWO_CHAR_OPS.get((lead, cur.peek(1)))
    if two is not None:
        cur.advance(); cur.advance()
        return Token(two, lead + cur.text[start_i + 1], cur.span_from(start_i, start_line, start_col))
    one = _ONE_CHAR_OPS.get(lead)
    if one is not None:
        cur.advance()
        return Token(one, lead, cur.span_from(start_i, start_line, start_col))
    cur.advance()
    raise LexerError(
        "L002_UNKNOWN_CHAR",
        f"unexpected character {lead!r}",
        cur.span_from(start_i, start_line, start_col),
    )


def _read_number(cur: _Cursor) -> Token:
    start_i, start_line, start_col = cur.i, cur.line, cur.col

    # Hex.
    if cur.peek() == "0" and cur.peek(1) in ("x", "X"):
        cur.advance(); cur.advance()
        digits_start = cur.i
        while not cur.at_end() and cur.peek() in "0123456789abcdefABCDEF":
            cur.advance()
        if cur.i == digits_start:
            raise LexerError(
                "L001_BAD_NUMERIC",
                "hex literal requires at least one hex digit",
                cur.span_from(start_i, start_line, start_col),
            )
        text = cur.text[start_i:cur.i]
        return Token(TokenKind.HEX, text, cur.span_from(start_i, start_line, start_col))

    # Decimal (possibly followed by a duration unit).
    while not cur.at_end() and cur.peek().isdigit():
        cur.advance()
    if not cur.at_end() and cur.peek() in _DURATION_UNITS:
        # Duration unit must NOT be followed by another ident char (i.e. "6s" vs "6sm").
        if (
            cur.i + 1 < len(cur.text)
            and (cur.text[cur.i + 1].isalnum() or cur.text[cur.i + 1] == "_")
        ):
            text = cur.text[start_i:cur.i]
            return Token(TokenKind.DEC, text, cur.span_from(start_i, start_line, start_col))
        cur.advance()
        text = cur.text[start_i:cur.i]
        return Token(TokenKind.DURATION, text, cur.span_from(start_i, start_line, start_col))

    text = cur.text[start_i:cur.i]
    return Token(TokenKind.DEC, text, cur.span_from(start_i, start_line, start_col))


def _read_identifier_or_keyword(cur: _Cursor) -> Token:
    start_i, start_line, start_col = cur.i, cur.line, cur.col
    while not cur.at_end():
        ch = cur.peek()
        if ch.isalnum() or ch == "_":
            cur.advance()
        else:
            break
    text = cur.text[start_i:cur.i]
    kind = KEYWORDS.get(text, TokenKind.IDENT)
    return Token(kind, text, cur.span_from(start_i, start_line, start_col))


def _read_string(cur: _Cursor) -> Token:
    start_i, start_line, start_col = cur.i, cur.line, cur.col
    cur.advance()  # opening quote
    while not cur.at_end() and cur.peek() != '"':
        if cur.peek() == "\\":
            cur.advance()
            if cur.at_end():
                raise LexerError(
                    "L003_UNTERMINATED_STRING",
                    "unterminated string literal",
                    cur.span_from(start_i, start_line, start_col),
                )
        cur.advance()
    if cur.at_end():
        raise LexerError(
            "L003_UNTERMINATED_STRING",
            "unterminated string literal",
            cur.span_from(start_i, start_line, start_col),
        )
    cur.advance()  # closing quote
    text = cur.text[start_i:cur.i]
    return Token(TokenKind.STRING, text, cur.span_from(start_i, start_line, start_col))
