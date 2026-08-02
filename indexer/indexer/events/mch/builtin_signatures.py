"""Builtin expression names and arities shared by the compiler and evaluator."""

BUILTIN_SIGNATURES: dict[str, int] = {
    "account": 1,
    "amount": 1,
    "asset": 1,
    "ton_asset": 0,
    "addr_none": 0,
    "b64": 1,
    "asset_of": 1,
    "tail_unwrap": 1,
    "bytes_of": 1,
    "first": 1,
    "last": 1,
    "len": 1,
    "sum": 1,
    "zip": 2,
    "concat": 2,
    "map": 2,
    "contains": 2,
}
