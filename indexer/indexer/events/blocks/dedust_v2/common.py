from __future__ import annotations

import logging

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockTypeMatcher,
    ContractMatcher,
    OrMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages import DedustV2PayoutMessage
from indexer.events.blocks.utils import AccountId, Amount, Asset

logger = logging.getLogger(__name__)


def payout_leg_matcher() -> OrMatcher:
    # jetton or TON payout leg; optional so zero-fee/failed flows still match
    return OrMatcher([
        labeled("payout", BlockTypeMatcher(block_type="jetton_transfer")),
        labeled("payout", ContractMatcher(opcode=DedustV2PayoutMessage.opcode, include_excess=False)),
    ], optional=True)


def parse_payout_leg(payout: Block) -> tuple | None:
    # (amount, asset, user_wallet, dex_wallet, dex_jetton_wallet) or None
    if isinstance(payout, JettonTransferBlock):
        return (
            payout.data["amount"],
            payout.data["asset"],
            payout.data["receiver_wallet"],
            payout.data["sender"],
            payout.data["sender_wallet"],
        )
    if (
        isinstance(payout, CallContractBlock)
        and payout.opcode == DedustV2PayoutMessage.opcode
    ):
        try:
            pm = DedustV2PayoutMessage(payout.get_body())
        except Exception as e:
            logger.debug("DeDust V2 payout parse failed: %s", e)
            return None
        return (
            Amount(pm.amount),
            Asset(is_ton=True),
            None,
            AccountId(payout.get_message().source),
            None,
        )
    return None
