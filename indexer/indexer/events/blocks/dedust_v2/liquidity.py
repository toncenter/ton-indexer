from __future__ import annotations

import base64
import logging
from collections import defaultdict

from pytoniq_core import Cell

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    OrMatcher,
    excess_matcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.dedust_v2.common import parse_payout_leg, payout_leg_matcher
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages import (
    DedustV2CreditAsset,
    DedustV2CreditLiquidity,
    DedustV2DepositEvent,
    DedustV2DepositPayload,
    DedustV2ExitLiquidity,
    DedustV2JoinLiquidity,
    DedustV2PayJetton,
    DedustV2PayNative,
    DedustV2PayoutMessage,
    DedustV2Withdraw,
    DedustV2WithdrawalEvent,
    DedustV2WithdrawLiquidity,
)
from indexer.events.blocks.utils import AccountId, Amount, Asset
from indexer.events.blocks.utils.block_utils import get_labeled, get_labeled_sorted

logger = logging.getLogger(__name__)

_EMPTY_LEG = (None, None, None, None, None)


class DedustV2WithdrawBlockMatcher(BlockMatcher):
    """DeDust CPMM V2 withdraw liquidity:

        Withdraw 0x20b5ef89 (user -> Pool)
          WithdrawLiquidity 0xbc1531a9 (Pool -> Position)   # burns LP share
            ExitLiquidity 0xf6f6a3aa (Position -> Pool)     # realized amounts
              Withdrawal event 0xc0d77b54 (Pool ext-out)    # exact out amounts
              payout leg 1 (jetton_transfer | PayoutMessage 0x3216ca09)
              payout leg 2 (same)
              excess

    LP sits in the Position contract, not a jetton, so asset/sender_wallet stay
    None. No Withdrawal event = failed (slippage)."""

    def __init__(self):
        exit_liquidity = ContractMatcher(
            opcode=DedustV2ExitLiquidity.opcode,
            include_excess=False,
            children_matchers=[
                labeled("event", ContractMatcher(
                    opcode=DedustV2WithdrawalEvent.opcode,
                    optional=True,
                    include_excess=False,
                )),
                payout_leg_matcher(),
                payout_leg_matcher(),
            ],
        )
        withdraw_liquidity = ContractMatcher(
            opcode=DedustV2WithdrawLiquidity.opcode,
            include_excess=False,
            child_matcher=exit_liquidity,
        )
        super().__init__(include_excess=False, child_matcher=withdraw_liquidity)

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2Withdraw.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        try:
            withdraw_msg = DedustV2Withdraw(block.get_body())
        except Exception as e:
            logger.debug("DeDust V2 withdraw parse failed: %s", e)
            return []

        # autoClaimFees mixes fee payouts into the same pool tx and the payout
        # slots can't tell them from withdrawn amounts
        if withdraw_msg.auto_claim_fees:
            return self.bail(block, "autoClaimFees withdraw is unsupported")

        sender = AccountId(block.get_message().source)
        pool = AccountId(block.get_message().destination)

        event_block = get_labeled("event", other_blocks)
        success = event_block is not None

        payout_blocks = get_labeled_sorted("payout", other_blocks)
        payout_blocks = (payout_blocks + [None, None])[:2]

        legs = []
        for payout in payout_blocks:
            if payout is None:
                legs.append(_EMPTY_LEG)
                continue
            leg = parse_payout_leg(payout)
            if leg is None:
                return self.bail(block, "unparseable payout leg")
            legs.append(leg)
        amount1, asset1, wallet1, dex_wallet1, dex_jetton_wallet1 = legs[0]
        amount2, asset2, wallet2, dex_wallet2, dex_jetton_wallet2 = legs[1]

        if success:
            lp_burnt = Amount(DedustV2WithdrawalEvent(event_block.get_body()).liquidity)
        else:
            lp_burnt = Amount(withdraw_msg.liquidity)

        new_block = Block("dex_withdraw_liquidity", [])
        new_block.data = {
            "dex": "dedust_v2",
            "sender": sender,
            "sender_wallet": None,
            "pool": pool,
            "asset": None,  # LP is not a jetton
            "lp_tokens_burnt": lp_burnt,
            "is_refund": not success,
            "amount1_out": amount1,
            "asset1_out": asset1,
            "dex_wallet_1": dex_wallet1,
            "dex_jetton_wallet_1": dex_jetton_wallet1,
            "wallet1": wallet1,
            "amount2_out": amount2,
            "asset2_out": asset2,
            "dex_wallet_2": dex_wallet2,
            "dex_jetton_wallet_2": dex_jetton_wallet2,
            "wallet2": wallet2,
        }
        new_block.merge_blocks([block] + other_blocks)
        new_block.failed = not success
        return [new_block]


class DedustV2DepositLiquidityBlock(Block):
    def __init__(self, data):
        super().__init__("dedust_v2_deposit_liquidity", [], data)

    def __repr__(self):
        return f"dedust_v2_deposit_liquidity {self.data}"


class DedustV2DepositLiquidityPartialBlock(Block):
    def __init__(self, data):
        super().__init__("dedust_v2_deposit_liquidity_partial", [], data)

    def __repr__(self):
        return f"dedust_v2_deposit_liquidity_partial {self.data}"


def _deposit_carrier_matcher() -> OrMatcher:
    # one deposit leg's carrier: jetton transfer or PayNative (TON leg)
    return OrMatcher([
        labeled("carrier", BlockTypeMatcher(block_type="jetton_transfer")),
        labeled("carrier", ContractMatcher(opcode=DedustV2PayNative.opcode, include_excess=False)),
    ])


def _deposit_credit_parent() -> BlockMatcher:
    # CreditAsset -> carrier parent chain, shared by both JoinLiquidity matchers
    return labeled("credit", ContractMatcher(
        opcode=DedustV2CreditAsset.opcode,
        include_excess=False,
        parent_matcher=_deposit_carrier_matcher(),
    ))


def _deposit_payload(carrier: Block) -> DedustV2DepositPayload | None:
    # DepositPayload (desired amountX/amountY) from either carrier kind
    try:
        if isinstance(carrier, JettonTransferBlock):
            fp = carrier.data.get("forward_payload")
            if fp is None:
                return None
            pay_jetton = DedustV2PayJetton(Cell.one_from_boc(base64.b64decode(fp)).begin_parse())
            return DedustV2DepositPayload(pay_jetton.payment_payload.begin_parse())
        pay_native = DedustV2PayNative(carrier.get_body())
        return DedustV2DepositPayload(pay_native.payment_payload.begin_parse())
    except Exception as e:
        logger.debug("DeDust V2 deposit payload parse failed: %s", e)
        return None


def _deposit_leg(carrier: Block, credit: CallContractBlock) -> tuple[int, Asset, AccountId | None, AccountId | None]:
    # returns (slot, asset, user_jetton_wallet, sender), slot 1=X 2=Y
    is_x = DedustV2CreditAsset(credit.get_body()).is_x
    if isinstance(carrier, JettonTransferBlock):
        asset = carrier.data["asset"]
        user_wallet = carrier.data["sender_wallet"]
        sender = carrier.data["sender"]
    else:
        asset = Asset(is_ton=True)
        user_wallet = None
        sender = AccountId(carrier.get_message().source)
    return (1 if is_x else 2), asset, user_wallet, sender


class DedustV2DepositBlockMatcher(BlockMatcher):
    """DeDust CPMM V2 deposit, the completing leg:

        carrier_X -+  (jetton_transfer | PayNative)
                   +-> Pool -> Deposit  CreditAsset 0xdc5ddba1 (is_x)
        carrier_Y -+
        Deposit -> Pool  JoinLiquidity 0x25251ee0   <- anchor
                Pool -> ext  Deposit event 0x35df2e12  (amounts + LP)
                Pool -> Position  CreditLiquidity 0x855afcbd

    Catches the last-credited leg via the parent chain; the other leg becomes a
    partial and the post-processor folds it in by deposit contract address.
    Must run before the partial matcher."""

    def __init__(self):
        super().__init__(
            include_excess=False,
            parent_matcher=_deposit_credit_parent(),
            children_matchers=[
                labeled("event", ContractMatcher(
                    opcode=DedustV2DepositEvent.opcode, optional=True, include_excess=False)),
                labeled("credit_liquidity", ContractMatcher(
                    opcode=DedustV2CreditLiquidity.opcode, optional=True)),
                # unused-input dust refunds, pool -> user, go to vault_excesses
                labeled("refund_jetton", BlockTypeMatcher(block_type="jetton_transfer", optional=True)),
                labeled("refund_jetton", BlockTypeMatcher(block_type="jetton_transfer", optional=True)),
                labeled("refund_ton", ContractMatcher(
                    opcode=DedustV2PayoutMessage.opcode, optional=True, include_excess=False)),
            ],
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2JoinLiquidity.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        try:
            join = DedustV2JoinLiquidity(block.get_body())
        except Exception as e:
            logger.debug("DeDust V2 deposit JoinLiquidity parse failed: %s", e)
            return []

        deposit_contract = block.get_message().source
        pool = AccountId(block.get_message().destination)

        event_block = get_labeled("event", other_blocks)
        if event_block is None:
            return []  # no Deposit event, rejected matcher's case
        event = DedustV2DepositEvent(event_block.get_body())

        carrier = get_labeled("carrier", other_blocks)
        credit = get_labeled("credit", other_blocks)
        if carrier is None or credit is None:
            return self.bail(block, "credit/carrier parent chain not matched")

        try:
            slot, asset, user_wallet, _ = _deposit_leg(carrier, credit)
        except Exception as e:
            return self.bail(block, f"deposit leg parse failed: {e}")

        payload = _deposit_payload(carrier)
        target_amount_1 = Amount(payload.amount_x) if payload is not None else None
        target_amount_2 = Amount(payload.amount_y) if payload is not None else None

        initiator = AccountId(join.initiator)

        # every refund leg must go to the initiator, same as the rejected flow
        vault_excesses: list[tuple[Asset, Amount]] = []
        for rj in get_labeled_sorted("refund_jetton", other_blocks):
            if not isinstance(rj, JettonTransferBlock) or rj.data["receiver"] != initiator:
                return self.bail(block, "jetton refund not addressed to the initiator")
            vault_excesses.append((rj.data["asset"], rj.data["amount"]))
        refund_ton = get_labeled("refund_ton", other_blocks)
        if refund_ton is not None:
            try:
                pm = DedustV2PayoutMessage(refund_ton.get_body())
            except Exception as e:
                return self.bail(block, f"TON refund parse failed: {e}")
            if AccountId(refund_ton.get_message().destination) != initiator:
                return self.bail(block, "TON refund not addressed to the initiator")
            vault_excesses.append((Asset(is_ton=True), Amount(pm.amount)))

        # amounts from the event; the other slot comes from the partial
        data = {
            "dex": "dedust_v2",
            "sender": initiator,
            "pool": pool,
            "deposit_contract": AccountId(deposit_contract),
            "amount_1": Amount(event.amount_x_in),
            "asset_1": None,
            "sender_wallet_1": None,
            "amount_2": Amount(event.amount_y_in),
            "asset_2": None,
            "sender_wallet_2": None,
            "target_amount_1": target_amount_1,
            "target_amount_2": target_amount_2,
            "lp_tokens_minted": Amount(event.liquidity),
            "vault_excesses": vault_excesses,
        }
        data[f"asset_{slot}"] = asset
        data[f"sender_wallet_{slot}"] = user_wallet

        new_block = DedustV2DepositLiquidityBlock(data)
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class DedustV2RejectedDepositBlockMatcher(BlockMatcher):
    """DeDust CPMM V2 deposit rejected at JoinLiquidity (no Deposit event),
    pool refunds every credited leg to the initiator:

        Deposit -> Pool  JoinLiquidity 0x25251ee0   <- anchor
                Pool -> user  jetton_transfer            # refund, per jetton leg
                Pool -> user  PayoutMessage 0x3216ca09   # refund, TON leg
                Pool -> user  excess

    One failed deposit: attempted amounts from the JoinLiquidity body, refunds
    go to vault_excesses, the other leg comes from its partial."""

    def __init__(self):
        super().__init__(
            include_excess=False,
            parent_matcher=_deposit_credit_parent(),
            children_matchers=[
                labeled("refund_jetton", BlockTypeMatcher(block_type="jetton_transfer", optional=True)),
                labeled("refund_jetton", BlockTypeMatcher(block_type="jetton_transfer", optional=True)),
                labeled("refund_ton", ContractMatcher(
                    opcode=DedustV2PayoutMessage.opcode, optional=True, include_excess=False)),
                excess_matcher(),
            ],
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2JoinLiquidity.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        if any(isinstance(b, CallContractBlock) and b.opcode == DedustV2DepositEvent.opcode
               for b in block.next_blocks):
            return []  # join succeeded, completing matcher's case
        try:
            join = DedustV2JoinLiquidity(block.get_body())
        except Exception as e:
            logger.debug("DeDust V2 rejected-join parse failed: %s", e)
            return []
        initiator = AccountId(join.initiator)

        # every refund must go to the initiator and at least one must exist
        vault_excesses: list[tuple[Asset, Amount]] = []
        for rj in get_labeled_sorted("refund_jetton", other_blocks):
            if not isinstance(rj, JettonTransferBlock) or rj.data["receiver"] != initiator:
                return self.bail(block, "jetton refund not addressed to the initiator")
            vault_excesses.append((rj.data["asset"], rj.data["amount"]))
        refund_ton = get_labeled("refund_ton", other_blocks)
        if refund_ton is not None:
            try:
                pm = DedustV2PayoutMessage(refund_ton.get_body())
            except Exception as e:
                return self.bail(block, f"TON refund parse failed: {e}")
            if AccountId(refund_ton.get_message().destination) != initiator:
                return self.bail(block, "TON refund not addressed to the initiator")
            vault_excesses.append((Asset(is_ton=True), Amount(pm.amount)))
        if not vault_excesses:
            return self.bail(block, "no refund legs found")

        carrier = get_labeled("carrier", other_blocks)
        credit = get_labeled("credit", other_blocks)
        if carrier is None or credit is None:
            return self.bail(block, "credit/carrier parent chain not matched")
        try:
            slot, asset, user_wallet, _ = _deposit_leg(carrier, credit)
        except Exception as e:
            return self.bail(block, f"deposit leg parse failed: {e}")

        payload = _deposit_payload(carrier)
        data = {
            "dex": "dedust_v2",
            "sender": initiator,
            "pool": AccountId(block.get_message().destination),
            "deposit_contract": AccountId(block.get_message().source),
            "amount_1": Amount(join.amount_x),
            "asset_1": None,
            "sender_wallet_1": None,
            "amount_2": Amount(join.amount_y),
            "asset_2": None,
            "sender_wallet_2": None,
            "target_amount_1": Amount(payload.amount_x) if payload is not None else None,
            "target_amount_2": Amount(payload.amount_y) if payload is not None else None,
            "lp_tokens_minted": None,
            "vault_excesses": vault_excesses,
        }
        data[f"asset_{slot}"] = asset
        data[f"sender_wallet_{slot}"] = user_wallet

        new_block = DedustV2DepositLiquidityBlock(data)
        new_block.merge_blocks([block] + other_blocks)
        new_block.failed = True
        return [new_block]


class DedustV2DepositPartialBlockMatcher(BlockMatcher):
    """One credited deposit leg: CreditAsset 0xdc5ddba1 with its carrier as
    parent. Runs after the completing matcher so it only claims the remaining
    leg(s); deposit contract address is the merge key for the post-processor."""

    def __init__(self):
        super().__init__(
            include_excess=False,
            parent_matcher=_deposit_carrier_matcher(),
            # escrow's gas-return excess; top-level include_excess doesn't merge it
            child_matcher=excess_matcher(),
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustV2CreditAsset.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        carrier = get_labeled("carrier", other_blocks)
        if carrier is None:
            return self.bail(block, "carrier not matched")
        try:
            credit_msg = DedustV2CreditAsset(block.get_body())
            slot, asset, user_wallet, sender = _deposit_leg(carrier, block)
        except Exception as e:
            return self.bail(block, f"deposit leg parse failed: {e}")

        data = {
            "dex": "dedust_v2",
            "sender": sender,
            "pool": AccountId(block.get_message().source),
            "deposit_contract": AccountId(block.get_message().destination),
            "amount_1": None,
            "asset_1": None,
            "sender_wallet_1": None,
            "amount_2": None,
            "asset_2": None,
            "sender_wallet_2": None,
            "lp_tokens_minted": None,
        }
        data[f"asset_{slot}"] = asset
        data[f"amount_{slot}"] = Amount(credit_msg.amount)
        data[f"sender_wallet_{slot}"] = user_wallet

        new_block = DedustV2DepositLiquidityPartialBlock(data)
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


async def post_process_dedust_v2_liquidity(blocks: list[Block]) -> list[Block]:
    # merge deposit partials into their completing block by deposit contract
    partials = [b for b in blocks if isinstance(b, DedustV2DepositLiquidityPartialBlock)]
    if not partials:
        return blocks
    finals = [b for b in blocks if isinstance(b, DedustV2DepositLiquidityBlock)]

    used_contracts: dict = defaultdict(int)
    for b in partials + finals:
        used_contracts[b.data.get("deposit_contract")] += 1
    for contract, count in used_contracts.items():
        if count > 2:
            logger.warning(
                "DeDust V2: %d deposit legs for contract %s; skip merging", count, contract
            )
            return blocks

    for final in finals:
        deposit_contract = final.data.get("deposit_contract")
        partial = next(
            (p for p in partials
             if p in blocks and p.data.get("deposit_contract") == deposit_contract),
            None,
        )
        if partial is None:
            continue
        for idx in (1, 2):
            if final.data.get(f"asset_{idx}") is None and partial.data.get(f"asset_{idx}") is not None:
                final.data[f"asset_{idx}"] = partial.data[f"asset_{idx}"]
                final.data[f"sender_wallet_{idx}"] = partial.data[f"sender_wallet_{idx}"]
                if final.data.get(f"amount_{idx}") is None:
                    final.data[f"amount_{idx}"] = partial.data.get(f"amount_{idx}")
        blocks.remove(partial)
        final.event_nodes.extend(partial.event_nodes)
        if final.initiating_event_node != partial.initiating_event_node:
            final.event_nodes.append(partial.initiating_event_node)
        final.event_nodes = list(set(final.event_nodes))
        final.children_blocks.extend(partial.children_blocks)
        final.children_blocks = list(set(final.children_blocks))
        final.calculate_min_max_lt()
    return blocks
