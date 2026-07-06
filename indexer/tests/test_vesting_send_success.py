import pytest
from pytoniq_core import Slice

# Import order matters: importing the classifier first primes the module graph
# and avoids a circular import between context and interface_repository.
from event_classifier import process_trace  # noqa: F401
from indexer.events import context
from indexer.events.blocks.messages.vesting import VestingSendMessage
from indexer.events.blocks.vesting import _vesting_message_was_sent
from tests.utils.repository import TestInterfaceRepository
from tests.utils.trace_deserializer import load_trace_from_file

VESTING_SEND_TRACE = "03RZCW7mu3eJp-FWifVzKsRHafUrqQNu4E6YwZxvXJ0="


def _load_trace(traces_dir):
    trace, interfaces = load_trace_from_file(traces_dir / f"{VESTING_SEND_TRACE}.lz4")
    context.interface_repository.set(TestInterfaceRepository(interfaces))
    return trace


def _find_vesting_send(trace):
    for tx in trace.transactions:
        in_msg = next((m for m in tx.messages if m.direction == "in"), None)
        if in_msg is None or in_msg.opcode is None:
            continue
        if (in_msg.opcode & 0xFFFFFFFF) == VestingSendMessage.opcode:
            return tx, in_msg
    raise AssertionError("Vesting send transaction not found in trace")


class TestVestingSendSuccess:
    def test_message_sent_is_detected_as_success(self, traces_dir):
        trace = _load_trace(traces_dir)
        tx, in_msg = _find_vesting_send(trace)
        request = VestingSendMessage(Slice.one_from_boc(in_msg.message_content.body))

        # The vesting transaction actually forwarded the requested message.
        assert _vesting_message_was_sent(tx, request) is True

    def test_ignored_send_is_detected_as_failure(self, traces_dir):
        trace = _load_trace(traces_dir)
        tx, in_msg = _find_vesting_send(trace)
        request = VestingSendMessage(Slice.one_from_boc(in_msg.message_content.body))

        # Simulate the "send mode 3 / ignore errors" case where the message is
        # silently skipped: drop the forwarded message, keep only the response.
        tx.messages = [
            m
            for m in tx.messages
            if not (
                m.direction == "out"
                and m.opcode is not None
                and (m.opcode & 0xFFFFFFFF) != VestingSendMessage.response_opcode
            )
        ]

        assert _vesting_message_was_sent(tx, request) is False
