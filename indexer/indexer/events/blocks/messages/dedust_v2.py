from __future__ import annotations

from pytoniq_core import Slice

class DedustV2PayNative:
    """pay_native#a5a7cbf8 query_id:uint64 amount:Coins
    payment_payload:^Cell payout_config:^ExtendedPayoutConfig

    TON carrier sent directly to the pool. paymentPayload (a ref) holds the
    actual operation payload; its first 32 bits identify the op (swap / deposit /
    fund-reward)."""

    opcode = 0xA5A7CBF8

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.amount = body.load_coins()
        self.payment_payload = body.load_ref()
        self.payout_config = body.load_ref()
        # Peek the operation opcode of the payment payload (do not consume it here).
        self.payment_payload_opcode = self.payment_payload.to_slice().load_uint(32)


class DedustV2PayJetton:
    """pay_jetton#cbc33949 payment_payload:^Cell payout_config:^ExtendedPayoutConfig

    Jetton carrier. Rides as the forwardPayload of a TEP-74 transfer/notification
    received by the pool. paymentPayload (a ref) holds the operation payload."""

    opcode = 0xCBC33949

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.payment_payload = body.load_ref()
        self.payout_config = body.load_ref()
        self.payment_payload_opcode = self.payment_payload.to_slice().load_uint(32)


class DedustV2PayoutMessage:
    """payout#3216ca09 query_id:uint64 amount:Coins exit_code:int32 payload:(Maybe ^Cell)

    Pool -> user. The real TON payout / refund leg. exit_code is the
    success(0) / refund(abort reason) discriminator."""

    opcode = 0x3216CA09

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.amount = body.load_coins()
        self.exit_code = body.load_int(32)
        self.payload = body.load_maybe_ref()


class DedustV2SwapPayload:
    """swap#c442500f minimal_amount_out:Coins deadline:uint40
    next:(Maybe ^SwapStep) partner_config:(Maybe PartnerConfig)
    referrer_config:(Maybe ReferrerConfig)

    Carried inside PayNative.paymentPayload / PayJetton.paymentPayload."""

    opcode = 0xC442500F

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.minimal_amount_out = body.load_coins()
        self.deadline = body.load_uint(40)
        # next names the second pool of a multi-hop route (and so on, recursively).
        # The initiator encodes the whole route here up-front, so every downstream
        # pool is readable from the entry message without walking the trace.
        self.next = body.load_maybe_ref()  # Maybe ^SwapStep
        # partner_config / referrer_config are inline Maybe structs
        self.partner_id = None
        self.partner_fee_bps = None
        if body.load_bit():
            self.partner_id = body.load_uint(256)
            self.partner_fee_bps = body.load_uint(16)
        self.referrer_id = None
        self.referrer_fee_bps = None
        if body.load_bit():
            self.referrer_id = body.load_uint(256)
            self.referrer_fee_bps = body.load_uint(16)


class DedustV2SwapStep:
    """_#_ pool:MsgAddressInt minimal_amount_out:Coins deadline:uint40
    next:(Maybe ^SwapStep) = SwapStep

    Inline routing step of a multi-hop swap (no opcode prefix — it is a struct,
    not a message body). Chained off ``SwapPayload.next``; each step names the next
    pool on the route. Pools don't know about each other, so the full hop list is
    specified by the initiator and lives entirely in the earliest message."""

    def __init__(self, body: Slice):
        self.pool = body.load_address()
        self.minimal_amount_out = body.load_coins()
        self.deadline = body.load_uint(40)
        self.next = body.load_maybe_ref()  # Maybe ^SwapStep


class DedustV2SwapEvent:
    """swap#78e79ba4 x_to_y:Bool amount_in:Coins amount_out:Coins
    initiator_address:MsgAddressInt recipient_address:MsgAddressInt
    next_state:^PoolUpdatedState fees:^SwapFees

    Emitted by the pool as an external-out message after a successful swap.
    The only exact source of amountIn / amountOut / direction."""

    opcode = 0x78E79BA4

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.x_to_y = body.load_bit()
        self.amount_in = body.load_coins()
        self.amount_out = body.load_coins()
        self.initiator = body.load_address()
        self.recipient = body.load_address()
        self.next_state = body.load_ref()  # ^PoolUpdatedState
        self.fees = body.load_ref()  # ^SwapFees


class DedustV2Withdraw:
    """withdraw#20b5ef89 query_id:uint64 liquidity:Coins
    minimal_x_out:Coins minimal_y_out:Coins auto_claim_fees:Bool
    payout_config:^BasicPayoutConfig

    user -> Pool. Entry of a liquidity withdrawal. The LP share lives in the
    Position contract (there is no LP-jetton burn), so `liquidity` is the share
    being burned, not a jetton amount."""

    opcode = 0x20B5EF89

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.liquidity = body.load_coins()
        self.minimal_x_out = body.load_coins()
        self.minimal_y_out = body.load_coins()
        self.auto_claim_fees = body.load_bit()
        self.payout_config = body.load_ref()


class DedustV2WithdrawLiquidity:
    """withdraw_liquidity#bc1531a9 query_id:uint64 liquidity:Coins
    minimal_x_out:Coins minimal_y_out:Coins x_fee_per_token:Q120X120
    y_fee_per_token:Q120X120 rewards:PoolRewards auto_claim_fees:Bool
    payout_config:^BasicPayoutConfig

    Pool -> Position. Q120X120 is varuint32, decoded with load_var_uint(5)
    (5-bit length prefix)."""

    opcode = 0xBC1531A9

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.liquidity = body.load_coins()
        self.minimal_x_out = body.load_coins()
        self.minimal_y_out = body.load_coins()
        self.x_fee_per_token = body.load_var_uint(5)  # Q120X120
        self.y_fee_per_token = body.load_var_uint(5)  # Q120X120
        self.rewards = body.load_maybe_ref()  # PoolRewards = HashmapE 2 PoolReward
        self.auto_claim_fees = body.load_bit()
        self.payout_config = body.load_ref()  # ^BasicPayoutConfig


class DedustV2ExitLiquidity:
    """exit_liquidity#f6f6a3aa query_id:uint64 liquidity:Coins
    x_fees:Coins y_fees:Coins owner:MsgAddressInt
    minimal_amount_x:Coins minimal_amount_y:Coins payout_config:^BasicPayoutConfig

    Position -> Pool. Carries the realized amounts and the LP owner. The Pool tx
    that processes this message emits the Withdrawal event and the payout legs."""

    opcode = 0xF6F6A3AA

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.liquidity = body.load_coins()
        self.x_fees = body.load_coins()
        self.y_fees = body.load_coins()
        self.owner = body.load_address()
        self.minimal_amount_x = body.load_coins()
        self.minimal_amount_y = body.load_coins()
        self.payout_config = body.load_ref()  # ^BasicPayoutConfig


class DedustV2WithdrawalEvent:
    """withdrawal#c0d77b54 liquidity:Coins amount_x_out:Coins amount_y_out:Coins
    initiator_address:MsgAddressInt recipient_address:MsgAddressInt
    next_state:^PoolUpdatedState

    Emitted by the Pool as an external-out message after a successful withdrawal.
    Exact source of the burned liquidity and the two out amounts. Absence of this
    event in the trace means the withdraw failed (e.g. slippage)."""

    opcode = 0xC0D77B54

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.liquidity = body.load_coins()
        self.amount_x_out = body.load_coins()
        self.amount_y_out = body.load_coins()
        self.initiator = body.load_address()
        self.recipient = body.load_address()
        self.next_state = body.load_ref()  # ^PoolUpdatedState


class DedustV2DepositPayload:
    """deposit#c9a015da amount_x:Coins amount_y:Coins minimal_liquidity:Coins
    locked_liquidity_share:uint16

    The deposit operation payload, carried inside a PayNative/PayJetton
    paymentPayload ref. amount_x/amount_y are the *desired* (target) amounts."""

    opcode = 0xC9A015DA

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.amount_x = body.load_coins()
        self.amount_y = body.load_coins()
        self.minimal_liquidity = body.load_coins()
        self.locked_liquidity_share = body.load_uint(16)


class DedustV2CreditAsset:
    """credit_asset#dc5ddba1 query_id:uint64 is_x:Bool amount:Coins

    Pool -> Deposit. One per asset leg credited into the ephemeral Deposit
    escrow contract. `is_x` says whether this leg is pool asset X or Y."""

    opcode = 0xDC5DDBA1

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.is_x = body.load_bit()
        self.amount = body.load_coins()


class DedustV2JoinLiquidity:
    """join_liquidity#25251ee0 query_id:uint64 config:^DepositConfig
    amount_x:Coins amount_y:Coins initiator:MsgAddressInt

    Deposit -> Pool. Fired once both asset legs are credited; unique to deposit
    completion."""

    opcode = 0x25251EE0

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.config = body.load_ref()
        self.amount_x = body.load_coins()
        self.amount_y = body.load_coins()
        self.initiator = body.load_address()


class DedustV2DepositEvent:
    """deposit#35df2e12 amount_x_in:Coins amount_y_in:Coins liquidity:Coins
    initiator_address:MsgAddressInt recipient_address:MsgAddressInt
    next_state:^PoolUpdatedState

    Emitted by the Pool as an external-out message after a successful deposit.
    Exact source of consumed amounts (amount_x_in/amount_y_in) and minted LP."""

    opcode = 0x35DF2E12

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.amount_x_in = body.load_coins()
        self.amount_y_in = body.load_coins()
        self.liquidity = body.load_coins()
        self.initiator = body.load_address()
        self.recipient = body.load_address()
        self.next_state = body.load_ref()  # ^PoolUpdatedState


class DedustV2CreditLiquidity:
    """credit_liquidity#855afcbd query_id:uint64 liquidity:Coins
    locked_liquidity_share:uint16 notify_extra_gas:Coins
    notify_payload:(Maybe ^Cell) excesses_to:MsgAddressInt
    x_fee_per_token:Q120X120 y_fee_per_token:Q120X120 rewards:PoolRewards

    Pool -> Position. Mints the LP share into the Position."""

    opcode = 0x855AFCBD

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.liquidity = body.load_coins()
        self.locked_liquidity_share = body.load_uint(16)
        self.notify_extra_gas = body.load_coins()
        self.notify_payload = body.load_maybe_ref()
        self.excesses_to = body.load_address()
        self.x_fee_per_token = body.load_var_uint(5)  # Q120X120
        self.y_fee_per_token = body.load_var_uint(5)  # Q120X120
        self.rewards = body.load_maybe_ref()  # PoolRewards = HashmapE 2 PoolReward


class DedustV2ClaimPositionFees:
    """claim_position_fees#5652f1df query_id:uint64 excesses_to:MsgAddressInt

    user -> Pool. Entry of an LP trading-fees claim."""

    opcode = 0x5652F1DF

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.excesses_to = body.load_address()


class DedustV2ClaimAvailableFees:
    """claim_available_fees#25f19752 query_id:uint64 excesses_to:MsgAddressInt
    x_fee_per_token:Q120X120 y_fee_per_token:Q120X120

    Pool -> Position. Q120X120 is varuint32, decoded with load_var_uint(5)."""

    opcode = 0x25F19752

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.excesses_to = body.load_address()
        self.x_fee_per_token = body.load_var_uint(5)  # Q120X120
        self.y_fee_per_token = body.load_var_uint(5)  # Q120X120


class DedustV2PayoutPositionFees:
    """payout_position_fees#29ff1bcf query_id:uint64 x_fees:Coins y_fees:Coins
    owner:MsgAddressInt excesses_to:MsgAddressInt

    Position -> Pool. Carries the exact accrued fee amounts and the LP owner.
    The Pool tx that processes this message emits the payout legs."""

    opcode = 0x29FF1BCF

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.x_fees = body.load_coins()
        self.y_fees = body.load_coins()
        self.owner = body.load_address()
        self.excesses_to = body.load_address()


class DedustV2ClaimReward:
    """claim_reward#909fdb65 query_id:uint64 reward_index:uint2
    excesses_to:MsgAddressInt

    user -> Pool. Entry of a liquidity-mining reward claim."""

    opcode = 0x909FDB65

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.reward_index = body.load_uint(2)
        self.excesses_to = body.load_address()


class DedustV2ClaimPositionReward:
    """claim_position_reward#2e0cccba query_id:uint64 reward_index:uint2
    reward:PoolReward excesses_to:MsgAddressInt

    Pool -> Position. PoolReward is inline: remaining_time:uint40
    remaining_budget:Coins rewards_per_token:Q120X120 last_update:uint40."""

    opcode = 0x2E0CCCBA

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.reward_index = body.load_uint(2)
        self.reward_remaining_time = body.load_uint(40)
        self.reward_remaining_budget = body.load_coins()
        self.reward_rewards_per_token = body.load_var_uint(5)  # Q120X120
        self.reward_last_update = body.load_uint(40)
        self.excesses_to = body.load_address()


class DedustV2PayoutReward:
    """payout_reward#9e17fbbe query_id:uint64 amount:Coins reward_index:uint2
    owner:MsgAddressInt excesses_to:MsgAddressInt

    Position -> Pool. Carries the exact reward amount and the LP owner. The
    Pool tx that processes this message emits the reward payout leg."""

    opcode = 0x9E17FBBE

    def __init__(self, body: Slice):
        op = body.load_uint(32)
        assert op == self.opcode
        self.query_id = body.load_uint(64)
        self.amount = body.load_coins()
        self.reward_index = body.load_uint(2)
        self.owner = body.load_address()
        self.excesses_to = body.load_address()
