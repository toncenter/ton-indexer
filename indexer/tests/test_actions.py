from tests.utils.generic_yaml_test import BaseGenericActionTest


class TestTonTransfers(BaseGenericActionTest):
    yaml_file = "ton-transfers.yaml"


class TestStonfiSwaps(BaseGenericActionTest):
    yaml_file = "stonfi-swaps.yaml"

class TestStonfiV2Swaps(BaseGenericActionTest):
    yaml_file = "stonfi-v2-swaps.yaml"

class TestJettonMints(BaseGenericActionTest):
    yaml_file = "jetton-mints.yaml"