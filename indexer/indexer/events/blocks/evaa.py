from __future__ import annotations

import base64
import logging
from dataclasses import dataclass

from pytoniq_core import Cell, Slice

from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    ExclusiveOrMatcher,
    GenericMatcher,
    OrMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages.evaa import (
    EvaaLiquidateFail,
    EvaaLiquidateMaster,
    EvaaLiquidateSatisfied,
    EvaaLiquidateSuccess,
    EvaaLiquidateSuccessReport,
    EvaaLiquidateUnsatisfied,
    EvaaLiquidateUser,
    EvaaLiquidationError,
    EvaaSupplyFail,
    EvaaSupplyMaster,
    EvaaSupplySuccess,
    EvaaSupplyUser,
    EvaaWithdrawCollateralized,
    EvaaWithdrawFail,
    EvaaWithdrawFailExcess,
    EvaaWithdrawMaster,
    EvaaWithdrawNoFundsExcess,
    EvaaWithdrawSuccess,
    EvaaWithdrawUser, EvaaSupplyJettonForwardMessage,
)
from indexer.events.blocks.utils import AccountId, Asset
from indexer.events.blocks.utils.block_utils import get_labeled
from indexer.events.blocks.utils.ton_utils import Amount
from indexer.core.database import Action

logger = logging.getLogger('actions-indexer')
logger.debug('Loading evaa.py')


TON_ASSET_ID = 0x1A4219FE5E60D63AF2A3CC7DCE6FEC69B45C6B5718497A6148E7C232AC87BD8A

evaa_action_comment_matcher = GenericMatcher(
    lambda block: isinstance(block, TonTransferBlock)
    and block.comment in ["EVAA liquidation.", "EVAA supply.", "EVAA withdraw."],
    optional=True,
)

def load_user_header(slice: Slice) -> tuple[int | None, Cell | None, int]:
    user_version = slice.load_coins()
    upgrade_info = slice.load_maybe_ref()
    upgrade_exec = slice.load_uint(2)
    return user_version, upgrade_info, upgrade_exec

class EvaaContractWithHeaderMatcher(ContractMatcher):
    def __init__(self, opcode,
                 header_func = load_user_header,
                 child_matcher=None,
                 parent_matcher=None,
                 optional=False,
                 children_matchers=None,
                 include_excess=True):
        self.header_func = header_func
        super().__init__(opcode, child_matcher, parent_matcher, optional, children_matchers, include_excess)

    def test_self(self, block: Block):
        if not isinstance(block, CallContractBlock):
            return False
        try:
            body = block.get_body()
            self.header_func(body)
            msg_opcode = body.load_uint(32)
            if msg_opcode == self.opcode:
                return True
            else:
                return False
        except Exception as e:
            logger.error(e, exc_info=True)
            return False

# ------------------------- Supply (ton and jetton) -------------------------

@dataclass
class EvaaSupplyData:
    sender: AccountId
    recipient: AccountId
    recipient_contract: AccountId
    amount: int
    is_success: bool
    is_ton: bool
    asset_id: int  # sha256('TON') for ton
    sender_jetton_wallet: AccountId | None = None
    recipient_jetton_wallet: AccountId | None = None
    master_jetton_wallet: AccountId | None = None
    master: AccountId | None = None
    asset: Asset | None = None


class EvaaSupplyBlock(Block):
    data: EvaaSupplyData

    def __init__(self, data: EvaaSupplyData):
        super().__init__("evaa_supply", [], data)

    def __repr__(self):
        return f"evaa_supply {self.data}"


class EvaaSupplyBlockMatcher(BlockMatcher):
    def __init__(self):
        success_matcher = labeled(
            "supply_success",
            ContractMatcher(
                opcode=EvaaSupplySuccess.opcode,
                optional=False,
                child_matcher=evaa_action_comment_matcher,
            ),
        )

        fail_matcher = labeled(
            "supply_fail",
            ContractMatcher(
                opcode=EvaaSupplyFail.opcode,
                optional=False,
            ),
        )

        refund_matcher = labeled(
            "jetton_return",
            BlockTypeMatcher(
                block_type="jetton_transfer",
                optional=False,
            ),
        )

        user_matcher = labeled(
            "supply_user",
            EvaaContractWithHeaderMatcher(
                opcode=EvaaSupplyUser.opcode,
                optional=False,
                child_matcher=OrMatcher(
                    [success_matcher, fail_matcher, refund_matcher]
                ),
            ),
        )
        logger.debug("Trying")

        super().__init__(child_matcher=user_matcher)

    def test_self(self, block: Block):
        # check if this is either supply_master call (for ton)
        # or jetton transfer with supply_master payload (for jetton)

        # ton case
        if (
            isinstance(block, CallContractBlock)
            and block.opcode == EvaaSupplyMaster.opcode
        ):
            return True

        # jetton case
        if isinstance(block, JettonTransferBlock):
            try:
                if not block.data.get("forward_payload"):
                    return False

                # create cell from payload and check its opcode
                payload_cell = Cell.from_boc(block.data["forward_payload"])[0]
                slice = payload_cell.begin_parse()
                opcode = slice.load_uint(32)
                return opcode == EvaaSupplyMaster.opcode
            except:
                return False

        return False

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        is_ton = isinstance(block, CallContractBlock)
        sender = None
        amount = 0
        sender_jetton_wallet = None
        recipient_address = None
        recipient_jetton_wallet = None
        master_jetton_wallet = None
        master = None
        asset = Asset(is_ton=True)
        if is_ton:
            msg = block.get_message()
            sender = AccountId(msg.source)

            supply_master_data = EvaaSupplyMaster(block.get_body())
            amount = supply_master_data.supply_amount
            recipient_address = AccountId(supply_master_data.recipient_address)
            master = AccountId(block.get_message().destination)
        else:
            sender = AccountId(block.data["sender"])
            sender_jetton_wallet = AccountId(block.data["sender_wallet"])
            master_jetton_wallet = AccountId(block.data["receiver_wallet"])
            amount = block.data["amount"].value
            master = block.data["receiver"]
            asset = block.data['asset']
            if not block.data.get("forward_payload"):
                logger.warning("No forward_payload in jetton supply")
                return []

            payload_slice = Slice.one_from_boc(block.data["forward_payload"])
            supply_master_data = EvaaSupplyJettonForwardMessage(payload_slice)
            recipient_address = AccountId(supply_master_data.recipient_address)
            # determine recipient_jetton_wallet
            if sender == recipient_address:
                recipient_jetton_wallet = sender_jetton_wallet
            else:
                # find recipient_jetton_wallet in other blocks when depositing to another wallet
                for other_block in other_blocks:
                    if isinstance(other_block, JettonTransferBlock):
                        if (
                            other_block.data.get("sender_wallet")
                            == master_jetton_wallet
                        ):
                            recipient_jetton_wallet = AccountId(
                                other_block.data["receiver_wallet"]
                            )
                            break

        supply_user_block = get_labeled("supply_user", other_blocks, CallContractBlock)
        if not supply_user_block:
            return []

        recipient_contract = AccountId(supply_user_block.get_message().destination)

        supply_user_body = supply_user_block.get_body()
        load_user_header(supply_user_body)
        supply_user_data = EvaaSupplyUser(supply_user_body)
        asset_id = supply_user_data.asset_id

        success_block = get_labeled("supply_success", other_blocks, CallContractBlock)
        if success_block:
            success_data = EvaaSupplySuccess(success_block.get_body())

            # update amount from success message (more accurate)
            if amount != success_data.amount_supplied:
                logger.warning(
                    f"amount in/out mismatch: {amount} != {success_data.amount_supplied}"
                )
            amount = success_data.amount_supplied

            new_block = EvaaSupplyBlock(
                data=EvaaSupplyData(
                    sender=sender,
                    recipient=recipient_address,
                    recipient_contract=recipient_contract,
                    amount=amount,
                    is_success=True,
                    is_ton=is_ton,
                    asset_id=asset_id,
                    recipient_jetton_wallet=recipient_jetton_wallet,
                    master_jetton_wallet=master_jetton_wallet,
                    sender_jetton_wallet=sender_jetton_wallet,
                    master=master,
                    asset=asset,
                )
            )

            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        # check fail scenario or jetton return
        fail_block = get_labeled("supply_fail", other_blocks, CallContractBlock)
        jetton_return = get_labeled("jetton_return", other_blocks, JettonTransferBlock)

        if fail_block or jetton_return:
            # failed operation
            new_block = EvaaSupplyBlock(
                data=EvaaSupplyData(
                    sender=sender,
                    recipient=recipient_address,
                    recipient_contract=recipient_contract,
                    amount=amount,
                    is_success=False,
                    is_ton=is_ton,
                    asset_id=asset_id,
                    recipient_jetton_wallet=recipient_jetton_wallet,
                    master_jetton_wallet=master_jetton_wallet,
                    sender_jetton_wallet=sender_jetton_wallet,
                    master=master,
                    asset=asset,
                )
            )
            # include possible blocks
            include_blocks = [block] + other_blocks
            if jetton_return:
                include_blocks.append(jetton_return)
            if fail_block:
                include_blocks.append(fail_block)
            new_block.merge_blocks(include_blocks)
            return [new_block]

        # if no success or failure blocks found, operation is not completed yet
        return []


# ------------------------- Withdraw (ton and jetton) -------------------------


@dataclass
class EvaaWithdrawData:
    owner: AccountId
    owner_contract: AccountId
    recipient: AccountId
    asset_id: int
    amount: int
    is_success: bool
    is_ton: bool
    recipient_jetton_wallet: AccountId | None = None
    master_jetton_wallet: AccountId | None = None
    fail_reason: str | None = None
    master: AccountId | None = None
    asset: Asset | None = None

class EvaaWithdrawBlock(Block):
    data: EvaaWithdrawData

    def __init__(self, data: EvaaWithdrawData):
        super().__init__("evaa_withdraw", [], data)

    def __repr__(self):
        return f"evaa_withdraw {self.data}"


class EvaaWithdrawBlockMatcher(BlockMatcher):
    def __init__(self):
        # for successful jetton withdrawals
        jetton_payout_matcher = labeled(
            "jetton_payout",
            BlockTypeMatcher(
                block_type="jetton_transfer",
                optional=False,
            ),
        )

        # for successful ton withdrawals
        ton_payout_matcher = labeled(
            "ton_payout",
            ContractMatcher(
                opcode=EvaaWithdrawSuccess.opcode,
                optional=False,
                child_matcher=evaa_action_comment_matcher,
            ),
        )

        payout_matcher = ExclusiveOrMatcher([jetton_payout_matcher, ton_payout_matcher])

        # opcode used for both payout and user smc update
        success_matcher = labeled(
            "withdraw_success",
            EvaaContractWithHeaderMatcher(
                opcode=EvaaWithdrawSuccess.opcode,
                optional=False,
                child_matcher=BlockTypeMatcher(block_type="ton_transfer")
            ),
        )

        # response from user contract to master may end with success or fail
        # if success, it sends money to user and updates data
        # if fail, it unlocks, reverts data and returns ton
        withdraw_collateralized_success_matcher = labeled(
            "withdraw_collateralized_success",
            ContractMatcher(
                opcode=EvaaWithdrawCollateralized.opcode,
                optional=False,
                children_matchers=[
                    payout_matcher,  # send money
                    success_matcher,  # update data
                ],
            ),
        )

        # master tells user contract to revert data back due to failure
        # the reason should be withdraw_no_funds_excess
        # and excesses with the reason are sent back to the user
        fail_no_funds_matcher = labeled(
            "withdraw_fail_no_funds",
            EvaaContractWithHeaderMatcher(
                opcode=EvaaWithdrawFail.opcode,
                optional=False,
                child_matcher=labeled(
                    "withdraw_no_funds_excess",
                    ContractMatcher(
                        opcode=EvaaWithdrawNoFundsExcess.opcode,
                        optional=False,
                    ),
                ),
            ),
        )

        withdraw_collateralized_fail_matcher = labeled(
            "withdraw_collateralized_fail",
            ContractMatcher(
                opcode=EvaaWithdrawCollateralized.opcode,
                optional=False,
                child_matcher=fail_no_funds_matcher,
            ),
        )

        # matches one of 4 opcodes, each indicating different failure reason
        fail_on_user_excesses_matcher = labeled(
            "withdraw_fail_on_user_excesses",
            GenericMatcher(
                optional=False,
                test_self_func=lambda block: isinstance(block, CallContractBlock)
                and block.opcode in EvaaWithdrawFailExcess.opcodes,
            ),
        )

        user_matcher = labeled(
            "withdraw_on_user",
            EvaaContractWithHeaderMatcher(
                opcode=EvaaWithdrawUser.opcode,
                optional=False,
                child_matcher=ExclusiveOrMatcher(
                    [
                        withdraw_collateralized_success_matcher,
                        withdraw_collateralized_fail_matcher,
                        fail_on_user_excesses_matcher,
                    ]
                ),
            ),
        )

        super().__init__(child_matcher=user_matcher)

    def test_self(self, block: Block):
        # check if this is either withdraw_master call (for ton)
        # or jetton transfer with withdraw_master payload (for jetton)
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == EvaaWithdrawMaster.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        owner = AccountId(msg.source)

        withdraw_master_body = block.get_body()
        master = AccountId(block.get_message().destination)
        withdraw_master_data = EvaaWithdrawMaster(withdraw_master_body)
        asset_id = withdraw_master_data.asset_id
        desired_amount = withdraw_master_data.amount
        recipient = AccountId(withdraw_master_data.recipient_address)

        is_ton = asset_id == TON_ASSET_ID
        recipient_jetton_wallet = None
        master_jetton_wallet = None

        withdraw_user_block = get_labeled(
            "withdraw_on_user", other_blocks, CallContractBlock
        )
        if not withdraw_user_block:
            return []
        owner_contract = AccountId(withdraw_user_block.get_message().destination)

        withdraw_collateralized_success = get_labeled(
            "withdraw_collateralized_success", other_blocks, CallContractBlock
        )
        amount = desired_amount
        if withdraw_collateralized_success:
            collateralized_data = EvaaWithdrawCollateralized(
                withdraw_collateralized_success.get_body()
            )
            amount = collateralized_data.withdraw_amount_current

            if is_ton:
                asset = Asset(is_ton=True)
                ton_payout = get_labeled("ton_payout", other_blocks, CallContractBlock)
                if not ton_payout:
                    return []
            else:
                jetton_payout = get_labeled(
                    "jetton_payout", other_blocks, JettonTransferBlock
                )
                if not jetton_payout:
                    return []
                asset = jetton_payout.data["asset"]
                recipient_jetton_wallet = AccountId(
                    jetton_payout.data["receiver_wallet"]
                )
                master_jetton_wallet = AccountId(jetton_payout.data["sender_wallet"])

                in_transfer_value = jetton_payout.data["amount"].value
                if desired_amount != in_transfer_value:
                    logger.warning(f"amount mismatch: {desired_amount} != {in_transfer_value}")
                    desired_amount = in_transfer_value

            new_block = EvaaWithdrawBlock(
                data=EvaaWithdrawData(
                    owner=owner,
                    owner_contract=owner_contract,
                    recipient=recipient,
                    asset_id=asset_id,
                    amount=amount,
                    is_success=True,
                    is_ton=is_ton,
                    recipient_jetton_wallet=recipient_jetton_wallet,
                    master_jetton_wallet=master_jetton_wallet,
                    master=master,
                    asset=asset,
                )
            )

            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        withdraw_collateralized_fail = get_labeled(
            "withdraw_collateralized_fail", other_blocks, CallContractBlock
        )
        if withdraw_collateralized_fail:
            new_block = EvaaWithdrawBlock(
                data=EvaaWithdrawData(
                    owner=owner,
                    owner_contract=owner_contract,
                    recipient=recipient,
                    asset_id=asset_id,
                    amount=desired_amount,
                    is_success=False,
                    is_ton=is_ton,
                    fail_reason="withdraw_no_funds_excess",
                    master=master
                )
            )
            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        fail_on_user = get_labeled(
            "withdraw_fail_on_user_excesses", other_blocks, CallContractBlock
        )
        if fail_on_user:
            error_data = EvaaWithdrawFailExcess(fail_on_user.get_body())

            new_block = EvaaWithdrawBlock(
                data=EvaaWithdrawData(
                    owner=owner,
                    owner_contract=owner_contract,
                    recipient=recipient,
                    asset_id=asset_id,
                    amount=desired_amount,
                    is_success=False,
                    is_ton=is_ton,
                    fail_reason=error_data.reason,
                    master=master
                )
            )
            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        return []


# ------------------------- Liquidate -------------------------


@dataclass
class EvaaLiquidateData:
    liquidator: AccountId
    borrower: AccountId  # user being liquidated
    borrower_contract: AccountId | None
    collateral_asset_id: int
    collateral_amount: int
    debt_asset_id: int
    debt_amount: int
    is_success: bool
    fail_reason: str | None = None


class EvaaLiquidateBlock(Block):
    data: EvaaLiquidateData

    def __init__(self, data: EvaaLiquidateData):
        super().__init__("evaa_liquidate", [], data)

    def __repr__(self):
        return f"evaa_liquidate {self.data}"


class EvaaLiquidateBlockMatcher(BlockMatcher):
    def __init__(self):
        # matcher for immediate errors (before user contract call)
        immediate_fail_refund_matcher = labeled(
            "immediate_fail_refund",
            ExclusiveOrMatcher(
                [
                    # for ton - just bounce original transaction
                    GenericMatcher(
                        test_self_func=lambda block: isinstance(
                            block, CallContractBlock
                        )
                        and block.get_message().bounced
                    ),
                    # for jetton - transfer
                    BlockTypeMatcher(
                        block_type="jetton_transfer",
                        optional=False,
                    ),
                ]
            ),
        )

        finish_with_tokens_and_report_matcher = labeled(
            "finish_with_tokens_and_report",
            ExclusiveOrMatcher(
                [
                    # for ton - call contract with success_report or fail_report
                    GenericMatcher(
                        test_self_func=lambda block: (
                            isinstance(block, CallContractBlock)
                            and block.opcode
                            in [
                                EvaaLiquidateSuccessReport.opcode,
                                EvaaLiquidateFail.opcode,
                            ]
                        )
                    ),
                    # for jetton - just transfer
                    BlockTypeMatcher(
                        block_type="jetton_transfer",
                        optional=False,
                    ),
                ]
            ),
        )

        liquidate_satisfied_and_success_matcher = labeled(
            "liquidate_satisfied_and_success",
            ContractMatcher(
                opcode=EvaaLiquidateSatisfied.opcode,
                optional=False,
                children_matchers=[
                    EvaaContractWithHeaderMatcher(  # to user contract
                        opcode=EvaaLiquidateSuccess.opcode,
                        optional=False,
                        child_matcher=evaa_action_comment_matcher,
                    ),
                    finish_with_tokens_and_report_matcher,
                ],
            ),
        )
        liquidate_satisfied_and_fail_matcher = labeled(
            "liquidate_satisfied_and_fail",
            ContractMatcher(
                opcode=EvaaLiquidateSatisfied.opcode,
                optional=False,
                children_matchers=[
                    ContractMatcher(  # to user contract
                        opcode=EvaaLiquidateFail.opcode,
                        optional=False,
                    ),
                    finish_with_tokens_and_report_matcher,
                ],
            ),
        )

        # matcher for failed liquidation on user contract
        unsatisfied_matcher = labeled(
            "liquidate_unsatisfied",
            ContractMatcher(
                opcode=EvaaLiquidateUnsatisfied.opcode,
                optional=False,
                child_matcher=finish_with_tokens_and_report_matcher,
            ),
        )

        # path: liquidate_user -> (satisfied or unsatisfied)
        user_matcher = labeled(
            "liquidate_user",
            EvaaContractWithHeaderMatcher(
                opcode=EvaaLiquidateUser.opcode,
                optional=False,
                child_matcher=ExclusiveOrMatcher(
                    [
                        liquidate_satisfied_and_success_matcher,
                        liquidate_satisfied_and_fail_matcher,
                        unsatisfied_matcher,
                    ]
                ),
            ),
        )

        super().__init__(
            child_matcher=ExclusiveOrMatcher(
                [user_matcher, immediate_fail_refund_matcher]
            )
        )

    def test_self(self, block: Block):
        # check if this is either liquidate_master call (for ton)
        # or jetton transfer with liquidate_master payload (for jetton)
        if (
            isinstance(block, CallContractBlock)
            and block.opcode == EvaaLiquidateMaster.opcode
        ):
            return True

        if isinstance(block, JettonTransferBlock):
            try:
                if not block.data.get("forward_payload"):
                    return False

                payload_cell = Cell.from_boc(block.data["forward_payload"])[0]
                slice = payload_cell.begin_parse()
                opcode = slice.load_uint(32)
                return opcode == EvaaLiquidateMaster.opcode
            except:
                return False

        return False

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        is_ton_liquidation = isinstance(block, CallContractBlock)

        liquidator = None
        liquidate_master_data = None
        transferred_asset_id = None
        liquidate_amount = None

        if is_ton_liquidation:
            msg = block.get_message()
            liquidator = AccountId(msg.source)
            liquidate_master_data = EvaaLiquidateMaster(block.get_body())
            transferred_asset_id = TON_ASSET_ID
        else:
            liquidator = AccountId(block.data["sender"])
            liquidate_amount = block.data["amount"].value
            if not block.data.get("forward_payload"):
                logger.warning("No forward_payload in jetton liquidation")
                return []
            payload_cell = Cell.from_boc(block.data["forward_payload"])[0]
            liquidate_master_data = EvaaLiquidateMaster(payload_cell.begin_parse())
            transferred_asset_id = block.data['receiver_wallet'].address

        liquidate_amount = liquidate_master_data.liquidate_incoming_amount
        borrower_address = AccountId(liquidate_master_data.borrower_address)
        collateral_asset_id = liquidate_master_data.collateral_asset_id

        liquidate_user_block = get_labeled(
            "liquidate_user", other_blocks, CallContractBlock
        )

        if not liquidate_user_block:
            # immediate_asset_refund case (early rejection on master)
            fail_report = get_labeled("immediate_fail_refund", other_blocks, Block)
            if not fail_report:
                return []
            new_block = EvaaLiquidateBlock(
                data=EvaaLiquidateData(
                    liquidator=liquidator,
                    borrower=borrower_address,
                    borrower_contract=None,
                    collateral_asset_id=collateral_asset_id,
                    collateral_amount=0,
                    debt_asset_id=transferred_asset_id,
                    debt_amount=liquidate_amount,
                    is_success=False,
                    fail_reason="immediate_rejection",
                )
            )
            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        borrower_contract = AccountId(liquidate_user_block.get_message().destination)

        satisfied_and_success = get_labeled(
            "liquidate_satisfied_and_success", other_blocks
        )
        satisfied_and_fail = get_labeled("liquidate_satisfied_and_fail", other_blocks)
        unsatisfied = get_labeled("liquidate_unsatisfied", other_blocks)

        if satisfied_and_success:
            satisfied_data = EvaaLiquidateSatisfied(satisfied_and_success.get_body())

            new_block = EvaaLiquidateBlock(
                data=EvaaLiquidateData(
                    liquidator=liquidator,
                    borrower=AccountId(satisfied_data.owner_address),
                    borrower_contract=borrower_contract,
                    collateral_asset_id=satisfied_data.collateral_asset_id,
                    collateral_amount=satisfied_data.collateral_reward,
                    debt_asset_id=satisfied_data.transferred_asset_id,
                    debt_amount=satisfied_data.liquidatable_amount,
                    is_success=True,
                )
            )

            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        elif unsatisfied:
            unsatisfied_data = EvaaLiquidateUnsatisfied(unsatisfied.get_body())

            try:
                error_data = unsatisfied_data.get_error()
                fail_reason = error_data.reason
            except Exception as e:
                logger.warning(f"Error parsing liquidation fail reason: {e}")
                fail_reason = "liquidation_error"  # default

            new_block = EvaaLiquidateBlock(
                data=EvaaLiquidateData(
                    liquidator=liquidator,
                    borrower=borrower_address,
                    borrower_contract=borrower_contract,
                    collateral_asset_id=collateral_asset_id,
                    collateral_amount=0,  # nothing received
                    debt_asset_id=transferred_asset_id,
                    debt_amount=liquidate_amount,
                    is_success=False,
                    fail_reason=fail_reason,
                )
            )

            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        elif satisfied_and_fail:
            new_block = EvaaLiquidateBlock(
                data=EvaaLiquidateData(
                    liquidator=liquidator,
                    borrower=borrower_address,
                    borrower_contract=borrower_contract,
                    collateral_asset_id=collateral_asset_id,
                    collateral_amount=0,
                    debt_asset_id=transferred_asset_id,
                    debt_amount=liquidate_amount,
                    is_success=False,
                    fail_reason="master_not_enough_liquidity",
                )
            )

            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        logger.debug(f"Liquidation in progress for borrower {borrower_address}")
        return []
