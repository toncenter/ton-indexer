import pytest

from event_classifier import process_trace
from indexer.events import context
from tests.utils.generic_yaml_test import BaseGenericActionTest
from tests.utils.repository import TestInterfaceRepository
from tests.utils.trace_deserializer import load_trace_from_file


class TestTonTransfers(BaseGenericActionTest):
    yaml_file = "ton-transfers.yaml"


class TestStonfiSwaps(BaseGenericActionTest):
    yaml_file = "stonfi-swaps.yaml"

class TestStonfiV2Swaps(BaseGenericActionTest):
    yaml_file = "stonfi-v2-swaps.yaml"

class TestJettonMints(BaseGenericActionTest):
    yaml_file = "jetton-mints.yaml"

class TestTonstakersActions(BaseGenericActionTest):
    yaml_file = "tonstakers.yaml"

class TestJvaultActions(BaseGenericActionTest):
    yaml_file = "jvault.yaml"

class TestJettonTransfersActions(BaseGenericActionTest):
    yaml_file = "jetton-transfer.yaml"

class TestEvaaActions(BaseGenericActionTest):
    yaml_file = "evaa.yaml"

class TestVestingActions(BaseGenericActionTest):
    yaml_file = "vesting.yaml"

class TestUnknownAction:

    @pytest.mark.asyncio
    async def test_unknown_action(self, traces_dir):
        trace_id = "Ugmymow0mpGDSuNUKC1YHkd28o7qceVvYtokZ--D-3E="
        filename = traces_dir / f"{trace_id}.lz4"
        assert filename.exists(), f"Trace file not found: {filename}"
        trace, interfaces = load_trace_from_file(filename)
        trace.transactions[0].messages[0].message_content.body = ""
        repository = TestInterfaceRepository(interfaces)
        context.interface_repository.set(repository)

        _, _, actions, _ = await process_trace(trace)
        assert len(actions) == 1
        assert actions[0].type == "unknown"
        assert set(actions[0]._accounts) == {"0:9E53B9A59CC76005E9B00D571D4933D8548361F87608D58BC1A0029FACCEF345"}
        assert set(actions[0].tx_hashes) == {"Ugmymow0mpGDSuNUKC1YHkd28o7qceVvYtokZ++D+3E="}