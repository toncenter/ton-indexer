from __future__ import annotations

import json
import logging
from typing import Iterable

logger = logging.getLogger(__name__)

def _format_opcode(opcode: int | None) -> str | None:
    if opcode is None:
        return None
    return f"0x{(opcode & 0xFFFFFFFF):08x}"


def _format_msg(msg) -> dict | None:
    if msg is None:
        return None
    return {
        "msg_hash": msg.msg_hash,
        "source": msg.source,
        "destination": msg.destination,
        "opcode": _format_opcode(msg.opcode),
        "created_lt": msg.created_lt,
        "tx_lt": msg.tx_lt,
    }


def _format_tx(tx) -> dict:
    in_msg = None
    out_msgs = []
    for m in tx.messages or ():
        if m.direction == "in" and in_msg is None:
            in_msg = m
        elif m.direction == "out":
            out_msgs.append(m)
    return {
        "tx_hash": tx.hash,
        "account": tx.account,
        "lt": tx.lt,
        "descr": tx.descr,
        "mc_block_seqno": tx.mc_block_seqno,
        "in_msg": _format_msg(in_msg),
        "out_msgs": [_format_msg(m) for m in out_msgs],
    }


def _build_payload(trace, action) -> dict:
    txs = sorted(trace.transactions, key=lambda t: t.lt)
    return {
        "trace_id": trace.trace_id,
        "trace": {
            "nodes_": trace.nodes_,
            "edges_": trace.edges_,
            "state": str(trace.state) if trace.state is not None else None,
            "classification_state": (
                str(trace.classification_state)
                if trace.classification_state is not None else None
            ),
            "mc_seqno_start": trace.mc_seqno_start,
            "mc_seqno_end": trace.mc_seqno_end,
            "start_lt": trace.start_lt,
            "end_lt": trace.end_lt,
            "start_utime": trace.start_utime,
            "end_utime": trace.end_utime,
            "loaded_transactions_count": len(trace.transactions),
        },
        "action": {
            "type": action.type,
            "opcode": _format_opcode(action.opcode),
            "action_id": action.action_id,
            "source": action.source,
            "destination": action.destination,
            "tx_hashes": list(action.tx_hashes) if action.tx_hashes else [],
        },
        "tree": [_format_tx(tx) for tx in txs],
    }


def observe_classification_result(trace, actions: Iterable) -> None:
    try:
        if any(tx.emulated for tx in trace.transactions):
            return
        if (trace.nodes_ or 0) <= 2:
            return
        if (trace.nodes_ or 0) > 20: # Avoid analyzing big traces
            return
        actions = list(actions)
        if len(actions) != 1:
            return
        action = actions[0]
        if action.type not in ("ton_transfer", "call_contract"):
            return
        tx_hashes = action.tx_hashes or []
        if len(tx_hashes) >= trace.nodes_:
            return
        payload = _build_payload(trace, action)
        logger.warning("PARTIAL_TRACE %s", json.dumps(payload))
    except Exception:
        logger.exception("partial-trace observer failed")
