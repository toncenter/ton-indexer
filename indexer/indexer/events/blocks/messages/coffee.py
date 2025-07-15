from __future__ import annotations

from pytoniq_core import Address, Slice

from indexer.events.blocks.utils import Asset

# --- Types ---


def load_coffee_asset(cell_slice: Slice) -> Asset:
    """
    asset parser for swap.coffee

    native$00 = Asset;
    jetton$01 chain:uint8 hash:uint256 = Asset;
    extra$10 id:uint32 = Asset;
    """
    tag = cell_slice.load_uint(2)
    if tag == 0b00:  # native
        return Asset(True)
    elif tag == 0b01:  # jetton
        chain = cell_slice.load_uint(8)
        hash_part = cell_slice.load_bytes(32)
        return Asset(False, Address((chain, hash_part)))
    elif tag == 0b10:  # extra
        # extra$10 id:uint32 = Asset;
        # not supported by our Asset class.
        raise NotImplementedError("Extra asset type not supported")
    else:
        raise ValueError(f"Invalid asset tag: {tag}")


class NotificationDataSingle:
    """
    notification_data_single receiver:MsgAddressInt fwd_gas:Coins
                             payload:^Cell = NotificationDataSingle;
    """

    def __init__(self, cell_slice: Slice):
        self.receiver = cell_slice.load_address()
        self.fwd_gas = cell_slice.load_coins()
        self.payload = cell_slice.load_ref()


class NotificationData:
    """
    notification_data on_success:(Maybe ^NotificationDataSingle)
                      on_failure:(Maybe ^NotificationDataSingle) = NotificationData;

    Types used:
    notification_data_single receiver:MsgAddressInt fwd_gas:Coins
                             payload:^Cell = NotificationDataSingle;
    """

    def __init__(self, cell_slice: Slice):
        self.on_success = None
        on_success_ref = cell_slice.load_maybe_ref()
        if on_success_ref:
            self.on_success = NotificationDataSingle(on_success_ref.to_slice())

        self.on_failure = None
        on_failure_ref = cell_slice.load_maybe_ref()
        if on_failure_ref:
            self.on_failure = NotificationDataSingle(on_failure_ref.to_slice())


class SwapParams:
    """
    swap_params deadline:uint32 recipient:MsgAddressInt referral:MsgAddressInt
                notification_data:(Maybe ^NotificationData) = SwapParams;

    Types used:
    notification_data on_success:(Maybe ^NotificationDataSingle)
                      on_failure:(Maybe ^NotificationDataSingle) = NotificationData;
    notification_data_single receiver:MsgAddressInt fwd_gas:Coins
                             payload:^Cell = NotificationDataSingle;
    """

    def __init__(self, cell_slice: Slice):
        self.deadline = cell_slice.load_uint(32)
        self.recipient = cell_slice.load_address()
        self.referral = cell_slice.load_address()
        self.notification_data = None
        notification_data_ref = cell_slice.load_maybe_ref()
        if notification_data_ref:
            self.notification_data = NotificationData(notification_data_ref.to_slice())


class SwapStepParams:
    """
    swap_step_params pool_address_hash:uint256 min_output_amount:Coins
                next:(Maybe ^SwapStepParams) = SwapStepParams;
    """

    def __init__(self, cell_slice: Slice):
        self.pool_address_hash = cell_slice.load_uint(256)
        self.min_output_amount = cell_slice.load_coins()
        self.next = None
        next_ref = cell_slice.load_maybe_ref()
        if next_ref:
            self.next = SwapStepParams(next_ref.to_slice())


class SwapStepInternalParams:
    """
    swap_step_internal_params previous_amount:Coins previous_asset_hint:(Maybe Asset)
                              min_output_amount:Coins
                              next:(Maybe ^SwapStepParams) = SwapStepInternalParams;
    """

    def __init__(self, cell_slice: Slice):
        self.previous_amount = cell_slice.load_coins()
        self.previous_asset_hint = None
        ah_ref = cell_slice.load_maybe_ref()
        if ah_ref:
            self.previous_asset_hint = load_coffee_asset(ah_ref.to_slice())
        self.min_output_amount = cell_slice.load_coins()
        self.next = None
        next_ref = cell_slice.load_maybe_ref()
        if next_ref:
            self.next = SwapStepParams(next_ref.to_slice())


class PublicPoolCreationParams:
    """
    public_pool_creation_params recipient:MsgAddressInt
                                use_recipient_on_failure:int1
                                notification_data:(Maybe ^Cell)
    = PublicPoolCreationParams;
    """

    def __init__(self, cell_slice: Slice):
        self.recipient = cell_slice.load_address()
        self.use_recipient_on_failure = cell_slice.load_bit()
        self.notification_data = None
        notification_data_ref = cell_slice.load_maybe_ref()
        if notification_data_ref:
            self.notification_data = NotificationData(notification_data_ref.to_slice())


class PrivatePoolCreationParams:
    """
    private_pool_creation_params is_active:uint1 extra_settings:(Maybe ^Cell)
    = PrivatePoolCreationParams;
    """

    def __init__(self, cell_slice: Slice):
        self.is_active = cell_slice.load_bit()
        self.extra_settings = cell_slice.load_maybe_ref()


class PoolCreationParams:
    """
    pool_creation_params public:PublicPoolCreationParams private:PrivatePoolCreationParams
    = PoolCreationParams;
    """

    def __init__(self, cell_slice: Slice):
        self.public = PublicPoolCreationParams(cell_slice)
        self.private = PrivatePoolCreationParams(cell_slice)


class PoolParams:
    """
    pool_params first:Asset second:Asset amm:AMM amm_settings:(Maybe ^Cell) = PoolParams;

    Types used:
    constant_product$000 = AMM;
    curve_fi_stable$001 = AMM;
    """

    def __init__(self, cell_slice: Slice):
        self.first = load_coffee_asset(cell_slice)
        self.second = load_coffee_asset(cell_slice)
        tag_amm = cell_slice.load_uint(3)
        if tag_amm == 0b000:
            self.amm = "constant_product"
        elif tag_amm == 0b001:
            self.amm = "curve_fi_stable"
        else:
            self.amm = "unknown"
        self.amm_settings = cell_slice.load_maybe_ref()


class DepositLiquidityCondition:
    """
    none$00 = DepositLiquidityCondition;
    lp_quantity$01 min_lp_amount:Coins = DepositLiquidityCondition;
    reserves_ratio$10 denominator:uint16 min_nominator:uint16
                      max_nominator:uint16 = DepositLiquidityCondition;
    complex$11 min_lp_amount:Coins denominator:uint16 min_nominator:uint16
               max_nominator:uint16 = DepositLiquidityCondition;
    """

    def __init__(self, cell_slice: Slice):
        self.tag = cell_slice.load_uint(2)
        if self.tag == 0b00:  # none
            self.kind = "none"
        elif self.tag == 0b01:  # lp_quantity
            self.kind = "lp_quantity"
            self.min_lp_amount = cell_slice.load_coins()
        elif self.tag == 0b10:  # reserves_ratio
            self.kind = "reserves_ratio"
            self.denominator = cell_slice.load_uint(16)
            self.min_nominator = cell_slice.load_uint(16)
            self.max_nominator = cell_slice.load_uint(16)
        elif self.tag == 0b11:  # complex
            self.kind = "complex"
            self.min_lp_amount = cell_slice.load_coins()
            self.denominator = cell_slice.load_uint(16)
            self.min_nominator = cell_slice.load_uint(16)
            self.max_nominator = cell_slice.load_uint(16)


class DepositLiquidityParamsTrimmed:
    """
    deposit_liquidity_params_trimmed recipient:MsgAddressInt
                                     use_recipient_on_failure:int1
    = DepositLiquidityParamsTrimmed;
    """

    def __init__(self, cell_slice: Slice):
        self.recipient = cell_slice.load_address()
        self.use_recipient_on_failure = cell_slice.load_bit()


class DepositLiquidityParams:
    """
    deposit_liquidity_params params:^DepositLiquidityParamsTrimmed
                         pool_params:^PoolParams = DepositLiquidityParams;
    """

    def __init__(self, cell_slice: Slice):
        self.params = DepositLiquidityParamsTrimmed(cell_slice)
        self.pool_params = PoolParams(cell_slice)


class WithdrawLiquidityCondition:
    """
    none$00 = WithdrawLiquidityCondition;
    assets_quantity$01 min_first_amount:Coins min_second_amount:Coins = WithdrawLiquidityCondition;
    reserves_ratio$10 denominator:uint16 min_nominator:uint16
                      max_nominator:uint16 = WithdrawLiquidityCondition;
    complex$11 min_first_amount:Coins min_second_amount:Coins denominator:uint16
               min_nominator:uint16 max_nominator:uint16 = WithdrawLiquidityCondition;
    """

    def __init__(self, cell_slice: Slice):
        self.tag = cell_slice.load_uint(2)
        if self.tag == 0b00:  # none
            self.kind = "none"
        elif self.tag == 0b01:  # assets_quantity
            self.kind = "assets_quantity"
            self.min_first_amount = cell_slice.load_coins()
            self.min_second_amount = cell_slice.load_coins()
        elif self.tag == 0b10:  # reserves_ratio
            self.kind = "reserves_ratio"
            self.denominator = cell_slice.load_uint(16)
            self.min_nominator = cell_slice.load_uint(16)
            self.max_nominator = cell_slice.load_uint(16)
        elif self.tag == 0b11:  # complex
            self.kind = "complex"
            self.min_first_amount = cell_slice.load_coins()
            self.min_second_amount = cell_slice.load_coins()
            self.denominator = cell_slice.load_uint(16)
            self.min_nominator = cell_slice.load_uint(16)
            self.max_nominator = cell_slice.load_uint(16)


class WithdrawLiquidityParams:
    """
    withdraw_liquidity_params use_recipient_on_failure:int1 deadline:uint32
                              condition:WithdrawLiquidityCondition
                              extra_settings:(Maybe ^Cell)
                              on_success:(Maybe ^NotificationDataSingle)
    = WithdrawLiquidityParams;
    """

    def __init__(self, cell_slice: Slice):
        self.use_recipient_on_failure = cell_slice.load_bit()
        self.deadline = cell_slice.load_uint(32)
        self.condition = WithdrawLiquidityCondition(cell_slice)
        self.extra_settings = cell_slice.load_maybe_ref()
        self.on_success = cell_slice.load_maybe_ref()


class PoolReserves:
    """
    pool_reserves input_reserve:Coins output_reserve:Coins = PoolReserves;
    """

    def __init__(self, cell_slice: Slice):
        self.input_reserve = cell_slice.load_coins()
        self.output_reserve = cell_slice.load_coins()


class PoolUpdateParams:
    """
    pool_update_params flags:(## 2)
                       protocol_fee:flags.0?uint16 lp_fee:flags.0?uint16
                       is_active:flags.1?uint1 = PoolUpdateParams;
    """

    def __init__(self, cell_slice: Slice):
        first_flag = cell_slice.load_bit()
        second_flag = cell_slice.load_bit()
        self.protocol_fee = None
        self.lp_fee = None
        if first_flag:
            self.protocol_fee = cell_slice.load_uint(16)
            self.lp_fee = cell_slice.load_uint(16)

        self.is_active = None
        if second_flag:
            self.is_active = cell_slice.load_bit()


class ContractUpdate:
    """
    contract_update code:(Maybe ^Cell) data:(Maybe ^Cell) = ContractUpdate;
    """

    def __init__(self, cell_slice: Slice):
        self.code = cell_slice.load_maybe_ref()
        self.data = cell_slice.load_maybe_ref()


# --- Messages ---


class CoffeeSwapNative:
    """
    TL-B:
    swap_native#c0ffee00 query_id:uint64 amount:Coins _:SwapStepParams
                        params:^SwapParams = SwapNative;
    """

    opcode = 0xC0FFEE00

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.amount = body.load_coins()
        self.params = SwapParams(body.load_ref().to_slice())
        self.step_params = SwapStepParams(body)


class CoffeeSwapJetton:
    """
    TL-B:
    swap_jetton#c0ffee10 _:SwapStepParams params:^SwapParams = SwapJetton;

    Types used:
    swap_params deadline:uint32 recipient:MsgAddressInt referral:MsgAddressInt
                notification_data:(Maybe ^NotificationData) = SwapParams;
    notification_data on_success:(Maybe ^NotificationDataSingle)
                      on_failure:(Maybe ^NotificationDataSingle) = NotificationData;
    notification_data_single receiver:MsgAddressInt fwd_gas:Coins
                             payload:^Cell = NotificationDataSingle;
    """

    opcode = 0xC0FFEE10

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.params = SwapStepParams(body)
        params_ref = body.load_ref()
        self.params = SwapParams(params_ref.to_slice())


class CoffeeSwapExtra:
    """
    TL-B:
    swap_extra#c0ffee01 query_id:uint64 _:SwapStepParams
                    params:^SwapParams = SwapExtra;
    """

    opcode = 0xC0FFEE01

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.step_params = SwapStepParams(body)
        self.params = SwapParams(body.load_ref().to_slice())


class CoffeeSwapSuccessfulEvent:
    """
    TL-B:
    swap_successful_event#c0ffee30 query_id:uint64 input:Asset input_amount:Coins
                                output_amount:Coins
                                reserves:^PoolReserves = SwapSuccessfulEvent;
    """

    opcode = 0xC0FFEE30

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.input = load_coffee_asset(body)
        self.input_amount = body.load_coins()
        self.output_amount = body.load_coins()
        self.reserves = PoolReserves(body)


class CoffeeSwapFailedEvent:
    """
    TL-B:
    swap_failed_event#c0ffee31 query_id:uint64 input:Asset input_amount:Coins
                            reserves:(Maybe ^PoolReserves) = SwapFailedEvent;
    """

    opcode = 0xC0FFEE31

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.input = load_coffee_asset(body)
        self.input_amount = body.load_coins()
        reserves_ref = body.load_maybe_ref()
        self.reserves = None
        if reserves_ref:
            self.reserves = PoolReserves(reserves_ref.to_slice())


class CoffeeCreatePoolNative:
    """
    TL-B:
    create_pool_native#c0ffee02 query_id:uint64 amount:Coins params:PoolParams
                                creation_params:PoolCreationParams = CreatePoolNative;
    """

    opcode = 0xC0FFEE02

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.amount = body.load_coins()
        self.params = PoolParams(body)  # modifies body slice
        self.creation_params = PoolCreationParams(body)


class CoffeeCreatePoolJetton:
    """
    TL-B:
    create_pool_jetton#c0ffee11 params:PoolParams
                                creation_params:PoolCreationParams = CreatePoolJetton;
    """

    opcode = 0xC0FFEE11

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.params = PoolParams(body)
        self.creation_params = PoolCreationParams(body)


class CoffeeCreatePoolExtra:
    """
    TL-B:
    create_pool_extra#c0ffee03 query_id:uint64 params:PoolParams
                            creation_params:PoolCreationParams = CreatePoolExtra;
    """

    opcode = 0xC0FFEE03

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.params = PoolParams(body)
        self.creation_params = PoolCreationParams(body)


class CoffeeDepositLiquidityNative:
    """
    TL-B:
    deposit_liquidity_native#c0ffee04 query_id:uint64 amount:Coins
                                    params:DepositLiquidityParams
    = DepositLiquidityNative;
    """

    opcode = 0xC0FFEE04

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.amount = body.load_coins()
        self.params = DepositLiquidityParams(body)


class CoffeeDepositLiquidityJetton:
    """
    TL-B:
    deposit_liquidity_jetton#c0ffee12 params:DepositLiquidityParams
    = DepositLiquidityJetton;
    """

    opcode = 0xC0FFEE12

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.params = DepositLiquidityParams(body)


class CoffeeDepositLiquidityExtra:
    """
    TL-B:
    deposit_liquidity_extra#c0ffee05 query_id:uint64 params:DepositLiquidityParams
    = DepositLiquidityExtra;
    """

    opcode = 0xC0FFEE05

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.params = DepositLiquidityParams(body)


class CoffeeDepositLiquiditySuccessfulEvent:
    """
    TL-B:
    deposit_liquidity_successful_event#c0ffee33 query_id:uint64 amount1:Coins
                                                amount2:Coins lp_amount:Coins
                                                total_supply:PoolReserves
                                                reserves:PoolReserves
    = DepositLiquiditySuccessfulEvent;
    """

    opcode = 0xC0FFEE33

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.amount1 = body.load_coins()
        self.amount2 = body.load_coins()
        self.lp_amount = body.load_coins()
        self.total_supply = PoolReserves(body)
        self.reserves = PoolReserves(body)


class CoffeeDepositLiquidityFailedEvent:
    """
    TL-B:
    deposit_liquidity_failed_event#c0ffee34 query_id:uint64 amount1:Coins
                                            amount2:Coins min_lp_amount:Coins
                                            total_supply:Coins reserves:PoolReserves
    = DepositLiquidityFailedEvent;
    """

    opcode = 0xC0FFEE34

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.amount1 = body.load_coins()
        self.amount2 = body.load_coins()
        self.min_lp_amount = body.load_coins()
        self.total_supply = body.load_coins()
        self.reserves = PoolReserves(body)


class CoffeeLiquidityWithdrawalEvent:
    """
    TL-B:
    liquidity_withdrawal_event#c0ffee35 query_id:uint64 amount1:Coins amount2:Coins
                                        lp_amount:Coins total_supply:PoolReserves
                                        reserves:PoolReserves
    = LiquidityWithdrawalEvent;
    """

    opcode = 0xC0FFEE35

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.amount1 = body.load_coins()
        self.amount2 = body.load_coins()
        self.lp_amount = body.load_coins()
        self.total_supply = PoolReserves(body)
        self.reserves = PoolReserves(body)


class CoffeeWithdrawDeposit:
    """
    TL-B:
    withdraw_deposit#c0ffee07 query_id:uint64 = WithdrawDeposit;
    """

    opcode = 0xC0FFEE07

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)


class CoffeeBurn:
    """
    TL-B: from jetton standard, used for liquidity withdrawal
    burn#595f07bc query_id:uint64 amount:(VarUInteger 16)
                response_destination:MsgAddress custom_payload:(Maybe ^Cell)
                = InternalMsgBody;
    """

    opcode = 0x595F07BC

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.amount = body.load_uint(16)
        self.response_destination = body.load_address()
        self.custom_payload = body.load_maybe_ref()


class CoffeePayout:
    """
    TL-B:
    payout#c0ffee32 query_id:uint64 = Payout;
    """

    opcode = 0xC0FFEE32

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)


class CoffeeCreateVault:
    """
    TL-B:
    create_vault#c0ffee06 query_id:uint64 asset:Asset = CreateVault;
    """

    opcode = 0xC0FFEE06

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.asset = load_coffee_asset(body)


class CoffeeNotification:
    """
    TL-B:
    notification#c0ffee36 query_id:uint64 body:^Cell = Notification;
    """

    opcode = 0xC0FFEE36

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.body = body.load_ref()


class CoffeeSwapInternal:
    """
    TL-B:
    swap_internal#c0ffee20 query_id:uint64 _:SwapStepInternalParams
                           params:^SwapParams proof:^Cell = SwapInternal;
    """

    opcode = 0xC0FFEE20

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.step_params = SwapStepInternalParams(body)
        self.params = SwapParams(body.load_ref().to_slice())
        self.proof = body.load_ref()


class CoffeePayoutInternal:
    """
    TL-B:
    payout_internal#c0ffee21 query_id:uint64 recipient:MsgAddressInt amount:Coins
                             notification_data:(Maybe ^NotificationDataSingle)
                             proof:(Maybe ^Cell) = PayoutInternal;
    """

    opcode = 0xC0FFEE21

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.recipient = body.load_address()
        self.amount = body.load_coins()
        self.notification_data = None
        notification_data_ref = body.load_maybe_ref()
        if notification_data_ref:
            self.notification_data = NotificationDataSingle(
                notification_data_ref.to_slice()
            )
        self.proof = body.load_maybe_ref()


class CoffeeDeploy:
    """
    TL-B:
    deploy#c0ffee22 code:^Cell data:^Cell action:(Maybe ^Cell) = Deploy;
    """

    opcode = 0xC0FFEE22

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.code = body.load_ref()
        self.data = body.load_ref()
        self.action = body.load_maybe_ref()


class CoffeeCreatePoolCreatorRequest:
    """
    TL-B:
    create_pool_creator_request#c0ffee23 query_id:uint64 amount:Coins params:PoolParams
                                        creation_params:^PoolCreationParams
                                        sender:MsgAddressInt proof:^Cell
    = CreatePoolCreatorRequest;
    """

    opcode = 0xC0FFEE23

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.amount = body.load_coins()
        self.params = PoolParams(body)
        self.creation_params = PoolCreationParams(body)
        self.sender = body.load_address()
        self.proof = body.load_ref()


class CoffeeCreatePoolCreatorInternal:
    """
    TL-B:
    create_pool_creator_internal#c0ffee24 query_id:uint64 asset:Asset amount:Coins
                                         creation_params:^PoolCreationParams
    = CreatePoolCreatorInternal;
    """

    opcode = 0xC0FFEE24

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.asset = load_coffee_asset(body)
        self.amount = body.load_coins()
        self.creation_params = PoolCreationParams(body)


class CoffeeCreatePoolRequest:
    """
    TL-B:
    create_pool_request#c0ffee25 query_id:uint64 amount1:Coins
                                 amount2:Coins tx_initiator:MsgAddressInt
                                 creation_params:^PoolCreationParams proof:^Cell
    = CreatePoolRequest;
    """

    opcode = 0xC0FFEE25

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.amount1 = body.load_coins()
        self.amount2 = body.load_coins()
        self.tx_initiator = body.load_address()
        creation_params_ref = body.load_ref()
        self.creation_params = PoolCreationParams(creation_params_ref.to_slice())
        self.proof = body.load_ref()


class CoffeeCreatePoolInternal:
    """
    TL-B:
    create_pool_internal#c0ffee26 query_id:uint64 amount1:Coins
                                   amount2:Coins tx_initiator:MsgAddressInt
                                   recipient:MsgAddressInt
                                   use_recipient_on_failure:int1
                                   extra_settings:(Maybe ^Cell)
                                   notification_data:(Maybe ^NotificationData)
    = CreatePoolInternal;
    """

    opcode = 0xC0FFEE26

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.amount1 = body.load_coins()
        self.amount2 = body.load_coins()
        self.tx_initiator = body.load_address()
        self.recipient = body.load_address()
        self.use_recipient_on_failure = body.load_bit()
        self.extra_settings = body.load_maybe_ref()
        self.notification_data = None
        notification_data_ref = body.load_maybe_ref()
        if notification_data_ref:
            self.notification_data = NotificationData(notification_data_ref.to_slice())


class CoffeeCreateLiquidityDepositoryRequest:
    """
    TL-B:
    create_liquidity_depository_request#c0ffee27 query_id:uint64 amount:Coins
                                                 params:^DepositLiquidityParamsTrimmed
                                                 pool_params:^PoolParams
                                                 sender:MsgAddressInt
                                                 proof:^Cell
    = CreateLiquidityDepositoryRequest;
    """

    opcode = 0xC0FFEE27

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.amount = body.load_coins()
        self.params = DepositLiquidityParamsTrimmed(body.load_ref().to_slice())
        self.pool_params = PoolParams(body.load_ref().to_slice())
        self.sender = body.load_address()
        self.proof = body.load_ref()


class CoffeeCreateLiquidityDepositoryInternal:
    """
    TL-B:
    create_liquidity_depository_internal#c0ffee28 query_id:uint64 asset:Asset
                                                 amount:Coins
                                                 params:^DepositLiquidityParamsTrimmed
    = CreateLiquidityDepositoryInternal;
    """

    opcode = 0xC0FFEE28

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.asset = load_coffee_asset(body)
        self.amount = body.load_coins()
        params_ref = body.load_ref()
        self.params = DepositLiquidityParamsTrimmed(params_ref.to_slice())


class CoffeeDepositLiquidityInternal:
    """
    TL-B:
    deposit_liquidity_internal#c0ffee29 query_id:uint64 amount1:Coins
                                        amount2:Coins tx_initiator:MsgAddressInt
                                        params:^DepositLiquidityParamsTrimmed
                                        proof:^Cell = DepositLiquidityInternal;
    """

    opcode = 0xC0FFEE29

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.amount1 = body.load_coins()
        self.amount2 = body.load_coins()
        self.tx_initiator = body.load_address()
        params_ref = body.load_ref()
        self.params = DepositLiquidityParamsTrimmed(params_ref.to_slice())
        self.proof = body.load_ref()


class CoffeeCreateVaultInternal:
    """
    TL-B:
    create_vault_internal#c0ffee2a query_id:uint64 = CreateVaultInternal;
    """

    opcode = 0xC0FFEE2A

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)


class CoffeeUpdatePoolInternal:
    """
    TL-B:
    update_pool_internal#c0ffee2b query_id:uint64 excesses_receiver:MsgAddressInt
                                  params:^PoolUpdateParams = UpdatePoolInternal;
    """

    opcode = 0xC0FFEE2B

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.excesses_receiver = body.load_address()
        params_ref = body.load_ref()
        self.params = PoolUpdateParams(params_ref.to_slice())


class CoffeeActivateVaultInternal:
    """
    TL-B:
    activate_vault_internal#c0ffee2c query_id:uint64
                                     wallet:MsgAddressInt = ActivateVaultInternal;
    """

    opcode = 0xC0FFEE2C

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.wallet = body.load_address()


class CoffeeWithdrawInternal:
    """
    TL-B:
    withdraw_internal#c0ffee2d query_id:uint64 asset:Asset amount:Coins
                               receiver:MsgAddressInt = WithdrawInternal;
    """

    opcode = 0xC0FFEE2D

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.asset = load_coffee_asset(body)
        self.amount = body.load_coins()
        self.receiver = body.load_address()


class CoffeeUpdateContractInternal:
    """
    TL-B:
    update_contract_internal#c0ffee2e query_id:uint64 excesses_receiver:MsgAddressInt
                                      _:ContractUpdate = UpdateContractInternal;
    """

    opcode = 0xC0FFEE2E

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.excesses_receiver = body.load_address()
        self.contract_update = ContractUpdate(body)


class CoffeeMevProtectHoldFunds:
    """
    TL-B:
    mev_protect_hold_funds#6bc79e7e query_id:uint64 = MevProtectHoldFunds;
    """

    opcode = 0x6BC79E7E

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)


class CoffeeServiceFee:
    """
    TL-B:
    service_fee#c0ffeea0 = ServiceFee;
    """

    opcode = 0xC0FFEEA0

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode


class CoffeeMevProtectFailedSwap:
    """
    TL-B:
    mev_protect_failed_swap#ee51ce51 query_id:uint64 recipient:MsgAddressInt
    = MevProtectFailedSwap;
    """

    opcode = 0xEE51CE51

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.recipient = body.load_address()


# Staking helper classes


class CoffeeStakingAssetData:
    """
    staking_asset_data#_ wallet:MsgAddressInt amount:Coins = CoffeeStakingAssetData;
    """

    def __init__(self, cell_slice: Slice):
        self.wallet = cell_slice.load_address()
        self.amount = cell_slice.load_coins()


class CoffeeStakingPositionData:
    """
    staking_position_data#_ user_points:Coins additional_points:Coins start_timestamp:uint64
                           end_timestamp:uint64 period_id:uint32 = CoffeeStakingPositionData;
    """

    def __init__(self, cell_slice: Slice):
        self.user_points = cell_slice.load_coins()
        self.additional_points = cell_slice.load_coins()
        self.start_timestamp = cell_slice.load_uint(64)
        self.end_timestamp = cell_slice.load_uint(64)
        self.period_id = cell_slice.load_uint(32)


class CoffeeStakingForwardData:
    """
    staking_forward_data#_ gas:Coins payload:^Cell = CoffeeStakingForwardData;
    """

    def __init__(self, cell_slice: Slice):
        self.gas = cell_slice.load_coins()
        self.payload = cell_slice.load_ref()


# Staking message parsers


class CoffeeStakingLock:
    """
    TL-B:
    staking_lock#c0ffede period_id:uint32 = ForwardPayload;
    """

    opcode = 0xC0FFEDE

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.period_id = body.load_uint(32)


class CoffeeStakingDeposit:
    """
    TL-B:
    staking_deposit#f9471134 query_id:uint64 sender:MsgAddressInt jetton_amount:Coins
                             from_user:MsgAddressInt period_id:uint32 = InMsgBody;
    """

    opcode = 0xF9471134

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.sender = body.load_address()
        self.jetton_amount = body.load_coins()
        self.from_user = body.load_address()
        self.period_id = body.load_uint(32)


class CoffeeStakingInitialize:
    """
    TL-B:
    staking_initialize#be5a7595 query_id:uint64 owner:MsgAddressInt jetton_data:^CoffeeStakingAssetData
                               position_data:^CoffeeStakingPositionData periods:^Cell = InMsgBody;
    """

    opcode = 0xBE5A7595

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.owner = body.load_address()
        self.jetton_data = CoffeeStakingAssetData(body.load_ref().begin_parse())
        self.position_data = CoffeeStakingPositionData(body.load_ref().begin_parse())
        self.periods = body.load_ref()


class CoffeeStakingClaimRewards:
    """
    TL-B:
    staking_claim_rewards#b30c7310 query_id:uint64 jetton_wallet:MsgAddressInt jetton_amount:Coins
                                   receiver:MsgAddressInt payload:(Maybe ^CoffeeStakingForwardData) = InMsgBody;
    """

    opcode = 0xB30C7310

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.jetton_wallet = body.load_address()
        self.jetton_amount = body.load_coins()
        self.receiver = body.load_address()
        self.payload = None
        payload_ref = body.load_maybe_ref()
        if payload_ref:
            self.payload = CoffeeStakingForwardData(payload_ref.begin_parse())


class CoffeeStakingPositionWithdraw1:
    """
    TL-B:
    staking_position_withdraw_1#cb03bfaf query_id:uint64 = InMsgBody;
    """

    opcode = 0xCB03BFAF

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)


class CoffeeStakingPositionWithdraw2:
    """
    TL-B:
    staking_position_withdraw_2#cb03bfaf query_id:uint64 nft_id:uint64 owner:MsgAddressInt
                                         points:Coins jetton_data:^[wallet:MsgAddressInt amount:Coins] = InMsgBody;
    """

    opcode = 0xCB03BFAF

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.nft_id = body.load_uint(64)
        self.owner = body.load_address()
        self.points = body.load_coins()
        jetton_data_ref = body.load_ref()
        jetton_data_slice = jetton_data_ref.begin_parse()
        self.jetton_wallet = jetton_data_slice.load_address()
        self.jetton_amount = jetton_data_slice.load_coins()


class CoffeeStakingPositionWithdraw3:
    """
    TL-B:
    staking_position_withdraw_3#cb03bfaf query_id:uint64 jetton_wallet:MsgAddressInt
                                         jetton_amount:Coins owner:MsgAddressInt = InMsgBody;
    """

    opcode = 0xCB03BFAF

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.jetton_wallet = body.load_address()
        self.jetton_amount = body.load_coins()
        self.owner = body.load_address()


class CoffeeStakingUpdateRewards:
    """
    TL-B:
    staking_update_rewards#0a9577f0 query_id:uint64 jetton_wallet:MsgAddressInt
                                    jetton_amount:Coins duration:uint64 = InMsgBody;
    """

    opcode = 0x0A9577F0

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.jetton_wallet = body.load_address()
        self.jetton_amount = body.load_coins()
        self.duration = body.load_uint(64)


class CoffeeStakingRewardsUpdated:
    """
    TL-B:
    staking_rewards_updated#0a9577f0 query_id:uint64 jetton_wallet:MsgAddressInt duration:uint64
                                     finish_at:uint64 rewards_rate:Coins = ExtOutMsgBody;
    """

    opcode = 0x0A9577F0

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.jetton_wallet = body.load_address()
        self.duration = body.load_uint(64)
        self.finish_at = body.load_uint(64)
        self.rewards_rate = body.load_coins()
