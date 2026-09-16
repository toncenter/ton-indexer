"""Unit tests for the partial-trace observer.

Gating logic + payload builder don't depend on real classification, so we
build minimal stubs via SimpleNamespace and assert log emission via caplog.
"""
from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

from indexer.events.blocks.utils import partial_trace_observer
from indexer.events.blocks.utils.partial_trace_observer import (
    observe_classification_result,
)


def _msg(*, msg_hash="m", direction="in", source="0:src", destination="0:dst",
         opcode=0x7362d09c, value=1, created_lt=100, tx_lt=100):
    return SimpleNamespace(
        msg_hash=msg_hash,
        direction=direction,
        source=source,
        destination=destination,
        opcode=opcode,
        value=value,
        created_lt=created_lt,
        tx_lt=tx_lt,
    )


def _tx(*, tx_hash="t", account="0:acct", lt=100, descr="ord",
        mc_block_seqno=42, emulated=False, in_msg=None, out_msgs=()):
    if in_msg is None and descr != "tick_tock":
        in_msg = _msg(tx_lt=lt)
    messages = []
    if in_msg is not None:
        messages.append(in_msg)
    messages.extend(out_msgs)
    return SimpleNamespace(
        hash=tx_hash,
        account=account,
        lt=lt,
        descr=descr,
        mc_block_seqno=mc_block_seqno,
        emulated=emulated,
        messages=messages,
    )


def _trace(*, trace_id="tr", nodes_=5, edges_=4, transactions=None,
           classification_state="ok"):
    return SimpleNamespace(
        trace_id=trace_id,
        nodes_=nodes_,
        edges_=edges_,
        state="complete",
        classification_state=classification_state,
        mc_seqno_start=100,
        mc_seqno_end=101,
        start_lt=1000,
        end_lt=1004,
        start_utime=10,
        end_utime=14,
        transactions=transactions or [_tx()],
    )


def _action(*, type="call_contract", tx_hashes=("t",), opcode=0x7362d09c):
    return SimpleNamespace(
        type=type,
        opcode=opcode,
        action_id="aid",
        source="0:src",
        destination="0:dst",
        tx_hashes=list(tx_hashes),
    )


def _captured_payload(caplog):
    """Return the JSON-decoded payload from the single PARTIAL_TRACE warning."""
    records = [r for r in caplog.records if "PARTIAL_TRACE" in r.getMessage()]
    assert len(records) == 1, (
        f"expected exactly 1 PARTIAL_TRACE log line, got {len(records)}"
    )
    msg = records[0].getMessage()
    _, _, payload_json = msg.partition("PARTIAL_TRACE ")
    return json.loads(payload_json)


def test_fires_on_kshe_pattern(caplog):
    """5-node trace, 1 loaded tx, single call_contract action → flagged."""
    trace = _trace(nodes_=5, transactions=[_tx()])
    actions = [_action(type="call_contract", tx_hashes=("t",))]

    with caplog.at_level(logging.WARNING, logger=partial_trace_observer.__name__):
        observe_classification_result(trace, actions)

    payload = _captured_payload(caplog)
    assert payload["trace_id"] == "tr"
    assert payload["trace"]["nodes_"] == 5
    assert payload["trace"]["loaded_transactions_count"] == 1
    assert payload["action"]["type"] == "call_contract"
    assert payload["action"]["opcode"] == "0x7362d09c"
    assert payload["action"]["tx_hashes"] == ["t"]
    assert len(payload["tree"]) == 1
    assert payload["tree"][0]["in_msg"]["opcode"] == "0x7362d09c"


def test_skipped_when_action_covers_all_nodes(caplog):
    """Action coverage equals nodes_ → not flagged (rule 5 short-circuits)."""
    trace = _trace(nodes_=5, transactions=[_tx(tx_hash=f"t{i}", lt=100 + i) for i in range(5)])
    actions = [_action(tx_hashes=tuple(f"t{i}" for i in range(5)))]

    with caplog.at_level(logging.WARNING, logger=partial_trace_observer.__name__):
        observe_classification_result(trace, actions)

    assert not any("PARTIAL_TRACE" in r.getMessage() for r in caplog.records)


def test_skipped_when_nodes_small(caplog):
    """nodes_ <= 2 → not flagged (rule 2)."""
    trace = _trace(nodes_=2, transactions=[_tx()])
    actions = [_action()]

    with caplog.at_level(logging.WARNING, logger=partial_trace_observer.__name__):
        observe_classification_result(trace, actions)

    assert not any("PARTIAL_TRACE" in r.getMessage() for r in caplog.records)


def test_skipped_when_emulated(caplog):
    """Any emulated tx → not flagged (rule 1, gates out pendings classifier)."""
    trace = _trace(nodes_=5, transactions=[_tx(emulated=True)])
    actions = [_action()]

    with caplog.at_level(logging.WARNING, logger=partial_trace_observer.__name__):
        observe_classification_result(trace, actions)

    assert not any("PARTIAL_TRACE" in r.getMessage() for r in caplog.records)


def test_skipped_when_action_is_higher_level(caplog):
    """jetton_transfer / nft_transfer / etc. → not flagged (rule 4)."""
    trace = _trace(nodes_=5, transactions=[_tx()])
    actions = [_action(type="jetton_transfer")]

    with caplog.at_level(logging.WARNING, logger=partial_trace_observer.__name__):
        observe_classification_result(trace, actions)

    assert not any("PARTIAL_TRACE" in r.getMessage() for r in caplog.records)


def test_payload_handles_none_opcode_and_ticktock(caplog):
    """A trace with a tick-tock tx and a None-opcode in_msg renders cleanly."""
    ticktock_tx = _tx(tx_hash="tt", descr="tick_tock", lt=99, in_msg=None)
    none_op_msg = _msg(msg_hash="m2", opcode=None, tx_lt=100)
    regular_tx = _tx(tx_hash="t", lt=100, in_msg=none_op_msg)
    trace = _trace(nodes_=5, transactions=[ticktock_tx, regular_tx])
    actions = [_action(tx_hashes=("t",), opcode=None)]

    with caplog.at_level(logging.WARNING, logger=partial_trace_observer.__name__):
        observe_classification_result(trace, actions)

    payload = _captured_payload(caplog)
    # Tree sorted by lt ascending: tick_tock (lt=99) first, regular (lt=100) second.
    assert payload["tree"][0]["descr"] == "tick_tock"
    assert payload["tree"][0]["in_msg"] is None
    assert payload["tree"][1]["descr"] == "ord"
    assert payload["tree"][1]["in_msg"]["opcode"] is None
    # JSON-serializable end-to-end, opcode renders as null not "0x...".
    assert payload["action"]["opcode"] is None


def test_negative_opcode_is_masked():
    """Signed-int opcode (e.g. -2147483648) renders as unsigned hex via &0xFFFFFFFF."""
    # 0x80000000 stored as signed = -2147483648
    assert partial_trace_observer._format_opcode(-2147483648) == "0x80000000"
    # 0x7362d09c stored as positive
    assert partial_trace_observer._format_opcode(0x7362d09c) == "0x7362d09c"
    assert partial_trace_observer._format_opcode(None) is None


def test_observer_swallows_payload_errors(caplog):
    """A malformed trace must never raise out of the observer."""

    class BrokenTrace:
        # Trip the gates so we reach payload building, then explode there.
        nodes_ = 5
        transactions = [_tx()]
        # Missing trace_id, state, etc. — payload builder will hit AttributeError.

    actions = [_action()]
    with caplog.at_level(logging.ERROR, logger=partial_trace_observer.__name__):
        observe_classification_result(BrokenTrace(), actions)

    # Did not raise. An error log line should explain.
    assert any("partial-trace observer failed" in r.getMessage() for r in caplog.records)
