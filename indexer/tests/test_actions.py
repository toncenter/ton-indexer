import pytest

from event_classifier import process_trace
from indexer.core.database import Trace
from indexer.events import context
from indexer.events.event_processing import trace_post_processors
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

class TestDedustLiquiditiesActions(BaseGenericActionTest):
    yaml_file = "dedust-liquidities.yaml"

class TestToncoActions(BaseGenericActionTest):
    yaml_file = "tonco.yaml"

class TestNftActions(BaseGenericActionTest):
    yaml_file = "nft.yaml"

class TestCoffeeActions(BaseGenericActionTest):
    yaml_file = "coffee.yaml"

class TestTgBTCActions(BaseGenericActionTest):
    yaml_file = "tgbtc.yaml"

class TestLayerZeroActions(BaseGenericActionTest):
    yaml_file = "layerzero.yaml"

class TestEthenaActions(BaseGenericActionTest):
    yaml_file = "ethena.yaml"

class TestClassificationCommon:

    def load_trace(self, trace_id, traces_dir) -> Trace:
        filename = traces_dir / f"{trace_id}.lz4"
        assert filename.exists(), f"Trace file not found: {filename}"
        trace, interfaces = load_trace_from_file(filename)
        trace.transactions[0].messages[0].message_content.body = ""
        repository = TestInterfaceRepository(interfaces)
        context.interface_repository.set(repository)
        return trace

    @pytest.mark.asyncio
    async def test_unknown_action(self, traces_dir):
        trace_id = "Ugmymow0mpGDSuNUKC1YHkd28o7qceVvYtokZ--D-3E="
        trace = self.load_trace(trace_id, traces_dir)

        _, _, actions, _ = await process_trace(trace)
        assert len(actions) == 1
        assert actions[0].type == "unknown"
        assert set(actions[0].accounts) == {"0:9E53B9A59CC76005E9B00D571D4933D8548361F87608D58BC1A0029FACCEF345"}
        assert set(actions[0].tx_hashes) == {"Ugmymow0mpGDSuNUKC1YHkd28o7qceVvYtokZ++D+3E="}

    @pytest.mark.asyncio
    async def test_fallback_classification(self, traces_dir):
        trace_id = "utNOlIUjzWv7MRoSUfkjHkBaIg40RkYMLOy9ljAaykQ="
        trace = self.load_trace(trace_id, traces_dir)
        _, state, actions, _ = await process_trace(trace)
        assert len(actions) > 0 and state == 'ok'

        # Simulate some unexpected error that fires once
        async def unexpected_error(x):
            trace_post_processors.remove(unexpected_error)
            raise Exception("Unexpected error")
        trace_post_processors.append(unexpected_error)

        _, state, actions, _ = await process_trace(trace)
        assert state == 'failed'
        assert len(actions) > 0
        basic_actions = ['ton_transfer', 'call_contract', 'contract_deploy', 'tick_tock']
        for action in actions:
            assert action.type in basic_actions