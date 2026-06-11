from __future__ import annotations

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    ContractMatcher,
    excess_matcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.dedust_v2.common import parse_payout_leg, payout_leg_matcher
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages import (
    DedustV2ClaimAvailableFees,
    DedustV2ClaimPositionFees,
    DedustV2ClaimPositionReward,
    DedustV2ClaimReward,
    DedustV2PayoutPositionFees,
    DedustV2PayoutReward,
)
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import get_labeled, get_labeled_sorted


class DedustV2ClaimFeesBlockMatcher(BlockMatcher):
    """DeDust CPMM V2 LP trading-fees claim:

        ClaimPositionFees 0x5652f1df (user -> Pool)
          ClaimAvailableFees 0x25f19752 (Pool -> Position)
            PayoutPositionFees 0x29ff1bcf (Position -> Pool)  # exact x/y fees + owner
              payout leg (jetton_transfer | PayoutMessage 0x3216ca09)  # if x_fees > 0
              payout leg (same)                                        # if y_fees > 0
              excess

    A zero-fee side has no payout leg (asset stays None)."""

    def __init__(self):
        payout_position_fees = labeled("payout_fees", ContractMatcher(
            opcode=DedustV2PayoutPositionFees.opcode,
            include_excess=False,
            children_matchers=[
                payout_leg_matcher(),
                payout_leg_matcher(),
                excess_matcher(),
            ],
        ))
        claim_available_fees = ContractMatcher(
            opcode=DedustV2ClaimAvailableFees.opcode,
            include_excess=False,
            child_matcher=payout_position_fees,
        )
        super().__init__(include_excess=False, child_matcher=claim_available_fees)

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2ClaimPositionFees.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        payout_fees_block = get_labeled("payout_fees", other_blocks, CallContractBlock)
        if payout_fees_block is None:
            return []
        try:
            payout_fees = DedustV2PayoutPositionFees(payout_fees_block.get_body())
        except Exception as e:
            return self.bail(block, f"PayoutPositionFees parse failed: {e}")

        sender = AccountId(block.get_message().source)
        pool = AccountId(block.get_message().destination)
        position = AccountId(payout_fees_block.get_message().source)

        # match legs to x/y slots by exact amount against PayoutPositionFees
        legs: dict[str, tuple | None] = {"x": None, "y": None}
        for payout in get_labeled_sorted("payout", other_blocks):
            parsed = parse_payout_leg(payout)
            if parsed is None:
                return self.bail(block, "unparseable payout leg")
            amount, asset, user_wallet, dex_wallet, dex_jetton_wallet = parsed
            leg = (asset, user_wallet, dex_wallet, dex_jetton_wallet)
            if amount.value == payout_fees.x_fees and legs["x"] is None:
                legs["x"] = leg
            elif amount.value == payout_fees.y_fees and legs["y"] is None:
                legs["y"] = leg

        empty = (None, None, None, None)
        asset_x, user_wallet_x, dex_wallet_x, dex_jetton_wallet_x = legs["x"] or empty
        asset_y, user_wallet_y, dex_wallet_y, dex_jetton_wallet_y = legs["y"] or empty

        new_block = Block("dedust_v2_claim_fees", [])
        new_block.data = {
            "dex": "dedust_v2",
            "sender": sender,
            "pool": pool,
            "position": position,
            "owner": AccountId(payout_fees.owner),
            "amount_x": Amount(payout_fees.x_fees),
            "amount_y": Amount(payout_fees.y_fees),
            "asset_x": asset_x,
            "asset_y": asset_y,
            "user_wallet_x": user_wallet_x,
            "user_wallet_y": user_wallet_y,
            "dex_wallet_x": dex_wallet_x,
            "dex_wallet_y": dex_wallet_y,
            "dex_jetton_wallet_x": dex_jetton_wallet_x,
            "dex_jetton_wallet_y": dex_jetton_wallet_y,
        }
        new_block.merge_blocks([block] + other_blocks)
        new_block.failed = False
        return [new_block]


class DedustV2ClaimRewardBlockMatcher(BlockMatcher):
    """DeDust CPMM V2 liquidity-mining reward claim:

        ClaimReward 0x909fdb65 (user -> Pool)
          ClaimPositionReward 0x2e0cccba (Pool -> Position)
            PayoutReward 0x9e17fbbe (Position -> Pool)  # exact amount + owner
              payout leg (jetton_transfer | PayoutMessage 0x3216ca09)
              excess

    A zero-amount claim has no payout leg (asset stays None)."""

    def __init__(self):
        payout_reward = labeled("payout_reward", ContractMatcher(
            opcode=DedustV2PayoutReward.opcode,
            include_excess=False,
            children_matchers=[
                payout_leg_matcher(),
                excess_matcher(),
            ],
        ))
        claim_position_reward = ContractMatcher(
            opcode=DedustV2ClaimPositionReward.opcode,
            include_excess=False,
            child_matcher=payout_reward,
        )
        super().__init__(include_excess=False, child_matcher=claim_position_reward)

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2ClaimReward.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        payout_reward_block = get_labeled("payout_reward", other_blocks, CallContractBlock)
        if payout_reward_block is None:
            return []
        try:
            claim = DedustV2ClaimReward(block.get_body())
            payout_reward = DedustV2PayoutReward(payout_reward_block.get_body())
        except Exception as e:
            return self.bail(block, f"claim/PayoutReward parse failed: {e}")

        sender = AccountId(block.get_message().source)
        pool = AccountId(block.get_message().destination)
        position = AccountId(payout_reward_block.get_message().source)

        asset = user_wallet = dex_wallet = dex_jetton_wallet = None
        payout = get_labeled("payout", other_blocks)
        if payout is not None:
            parsed = parse_payout_leg(payout)
            if parsed is None:
                return self.bail(block, "unparseable payout leg")
            _, asset, user_wallet, dex_wallet, dex_jetton_wallet = parsed

        new_block = Block("dedust_v2_claim_reward", [])
        new_block.data = {
            "dex": "dedust_v2",
            "sender": sender,
            "pool": pool,
            "position": position,
            "owner": AccountId(payout_reward.owner),
            "reward_index": claim.reward_index,
            "amount": Amount(payout_reward.amount),
            "asset": asset,
            "user_wallet": user_wallet,
            "dex_wallet": dex_wallet,
            "dex_jetton_wallet": dex_jetton_wallet,
        }
        new_block.merge_blocks([block] + other_blocks)
        new_block.failed = False
        return [new_block]
